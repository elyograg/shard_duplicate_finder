package org.elyograg.solr.duplicate;

import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

@Command(name = "find_duplicate", sortOptions = false, description = "Multi-threaded "
    + "program to detect situations where the same id value exists in more than one shard.")
public class Main implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Map<String, QueryThread> qtMap = Collections
      .synchronizedMap(new HashMap<>());

  /** Debug option. */
  @Option(names = { "-v" }, arity = "0", description = "Log any available debug messages.")
  private static boolean verbose;

  @ArgGroup(multiplicity = "1")
  private static RequiredOpts requiredOpts;

  private static final class RequiredOpts {
    /** Help option. */
    @Option(names = {
        "-h" }, arity = "0", usageHelp = true, description = "Display this command usage.")
    private static boolean help;

    /** A hidden --exit option. Useful for making sure the program will run. */
    @Option(names = {
        "--exit" }, arity = "0", hidden = true, scope = ScopeType.INHERIT, description = ""
            + "Exit the program as soon as it starts.")
    private static boolean exitFlag;

    @Option(names = { "-s",
        "--solr-url" }, arity = "1", description = "Solr URL.  MUST include a corename. "
            + "Can be specified multiple times. "
            + "Example: https:/server:8443/solr/corename", required = true)
    private static List<String> solrUrls;
  }

  @Option(names = { "-uk",
      "--unique-key" }, arity = "1", defaultValue = "id", description = "UniqueKey field. Default '${DEFAULT-VALUE}'")
  private static String uk;

  @Option(names = { "-D" }, arity = "1", paramLabel = "someProp=\"some value\"", description = ""
      + "System Property. Can be specified multiple times. "
      + "Also works just like the -D option for Java. "
      + "Quotes are required if the value contains spaces or other special characters."
      + "Don't use special characters in the property name.")
  private static List<String> properties;

  @Option(names = { "-u", "--user" }, arity = "1", description = "Username for solr server(s).")
  private static String user;

  @Option(names = { "-p", "--password" }, arity = "1", description = "Password for solr server(s).")
  private static String pass;

  @Option(names = { "-2", "--http2" }, arity = "0", description = "Set the client to use http2.")
  private static boolean h2;

  @Option(names = { "-fq",
      "--filter" }, arity = "1", description = "Filter query.  Can be specified more than once. "
          + "Multiple filters are ANDed together.")
  private static List<String> fq;

  @Option(names = { "-b",
      "--batch-size" }, arity = "1", defaultValue = "25000", description = "Batch size "
          + "for query. Default '${DEFAULT-VALUE}'")
  private static int batchSize;

  public static final void main(final String[] args) {
    new CommandLine(new Main()).setHelpFactory(StaticStuff.createLeftAlignedUsageHelp())
        .execute(args);
  }

  @Override
  public final void run() {
    if (RequiredOpts.exitFlag) {
      System.exit(0);
    }

    Thread.setDefaultUncaughtExceptionHandler(new StaticStuff.MigrateUCE());
    StaticStuff.setVerboseFlag(verbose);

    if (properties != null) {
      for (final String propString : properties) {
        final String[] split = propString.split("=", 2);
        final String prop = split[0];
        final String value = split[1];
        System.getProperties().put(prop, value);
        log.info("Property {} added.", propString);
      }
    }

    if (fq != null && fq.size() > 0) {
      for (final String f : fq) {
        log.info("Filter added: {}", f);
      }
    }

    for (final String url : RequiredOpts.solrUrls) {
      makeThread(url);
    }

    for (final String key : qtMap.keySet()) {
      qtMap.get(key).start();
    }

    boolean done = false;
    while (!done) {
      done = true;
      for (final String key : qtMap.keySet()) {
        if (qtMap.get(key).isAlive()) {
          done = false;
        }
      }
    }

    final Map<String, List<String>> duplicates = Collections.synchronizedMap(new HashMap<>());
    final Set<String> bigSet = Collections.synchronizedSet(new HashSet<>());
    int i = 0;
    final Set<String> coreNames = qtMap.keySet();
    for (final String outerCore : coreNames) {
      final Set<String> shardIdSet = qtMap.get(outerCore).getIdSet();
      writeIdsToFile(shardIdSet, i);
      if (i == 0) {
        bigSet.addAll(shardIdSet);
        log.warn("Adding entire first shard IDs.");
        i++;
        continue;
      }
      for (final String id : shardIdSet) {
        if (bigSet.contains(id)) {
          for (final String innerCore : coreNames) {
            List<String> list = duplicates.get(id);
            if (qtMap.get(innerCore).getIdSet().contains(id)) {
              if (list == null) {
                list = Collections.synchronizedList(new ArrayList<>());
              }
              list.add(innerCore);
              duplicates.put(id, list);
            }
          }
        }
      }
      i++;
    }

    log.info("{} Duplicated IDs:", duplicates.size());
    for (final String id : duplicates.keySet()) {
      log.info("{}:{}", id, duplicates.get(id));
    }

    log.info("Main thread ending!");
  }

  private void writeIdsToFile(final Set<String> shardIdSet, final int i) {
    try (OutputStream os = Files.newOutputStream(Paths.get("idlist_" + i + ".txt"));) {
      for (final String id : shardIdSet) {
        final String idLine = id + "\n";
        os.write(idLine.getBytes(StandardCharsets.UTF_8));
      }
    } catch (final Exception e) {
      log.error("Error writing ID list to file {}", i, e);
    }
  }

  /**
   * Get info from url. Create SolrClient object and thread, populating the thread
   * map.
   * 
   * @param url the URL to process
   */
  private static final void makeThread(final String url) {
    String coreName;

    final Map<String, String> parseMap = parseUrl(url);
    String path = parseMap.get("path");

    // Remove trailing slashes
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    // Split the path by "/"
    final String[] pathComponents = path.split("/");

    // Retrieve the last component
    if (pathComponents.length > 0) {
      coreName = pathComponents[pathComponents.length - 1];
    } else {
      throw new IllegalArgumentException("No path components found.");
    }

    final Http2SolrClient.Builder cb = new Http2SolrClient.Builder(url);
    cb.useHttp1_1(!h2);
    if (user != null && !user.equals("")) {
      cb.withBasicAuthCredentials(user, pass);
    }
    qtMap.put(coreName, new QueryThread(cb.build(), coreName, uk, batchSize));
  }

  public static final List<String> getFilters() {
    return fq;
  }

  private static final Map<String, String> parseUrl(final String urlString) {
    final Map<String, String> parseMap = Collections.synchronizedMap(new HashMap<>());
    try {
      final URL url = new URL(urlString);

      parseMap.put("scheme", url.getProtocol());
      parseMap.put("host", url.getHost());
      parseMap.put("port", Integer.toString(url.getPort()));
      parseMap.put("path", url.getPath());
      parseMap.put("query", url.getQuery());
      parseMap.put("userInfo", url.getUserInfo());
    } catch (final MalformedURLException e) {
      throw new IllegalArgumentException("Invalid URL: " + e.getMessage());
    }
    return parseMap;
  }
}
