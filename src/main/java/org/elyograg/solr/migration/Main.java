package org.elyograg.solr.migration;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
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
  private static final List<QueryThread> queryThreads = new ArrayList<>();

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

    @Option(names = { "-c",
        "--source-collection" }, arity = "1", description = "Name of the source collection.", required = true)
    private static String collection;

  }

  @Option(names = { "-z", "zkhost" }, arity = "1", description = "zkHost string. "
      + "Example: ip1:2181,ip2:2181,ip3:2181/solr", required = true)
  private static String zkHost;

  @Option(names = { "-uk" }, arity = "1", description = "UniqueKey field.")
  private static String uk;

  @Option(names = { "-D" }, arity = "1", paramLabel = "someProp=\"some value\"", description = ""
      + "System Property. Can be specified multiple times. "
      + "Also works just like the -D option for Java. "
      + "Quotes are required if the value contains spaces or other special characters.")
  private static List<String> properties;

  @Option(names = { "-r",
      "--chroot" }, arity = "1", paramLabel = "/chroot", description = "Target ZK chroot. "
          + "Example: '/solr'")
  private static String chroot;

  @Option(names = { "-u", "--user" }, arity = "1", description = "Username for source solr server.")
  private static String user;

  @Option(names = { "-p",
      "--password" }, arity = "1", description = "Password for source solr server.")
  private static String pass;

  @Option(names = { "-2",
      "--http2" }, arity = "0", description = "Set the source server to use http2.")
  private static boolean h2;

  @Option(names = { "-fq",
      "--filter" }, arity = "1", description = "Filter query.  Can be specified more than once. "
          + "Multiple filters are ANDed together.")
  private static List<String> fq;

  @Option(names = { "-b",
      "--batch-size" }, arity = "1", defaultValue = "10000", description = "Batch size "
          + "for query. Default '${DEFAULT-VALUE}'")
  private static int batchSize;

  public static void main(final String[] args) {
    new CommandLine(new Main()).setHelpFactory(StaticStuff.createLeftAlignedUsageHelp())
        .execute(args);
  }

  @Override
  public void run() {
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

    log.info("Collection {} zkHost {} ", RequiredOpts.collection, zkHost);

    final Map<String, String> coreNameToCoreUrl = new HashMap<>();

    final CloudSolrServer solrServer = new CloudSolrServer(zkHost);
    solrServer.setDefaultCollection(RequiredOpts.collection);

    final ClusterState clusterState = solrServer.getZkStateReader().getClusterState();
    final Collection<Slice> slices = clusterState.getActiveSlices(RequiredOpts.collection);

    for (final Slice slice : slices) {
      final Collection<Replica> replicas = slice.getReplicas();
      for (final Replica replica : replicas) {
        final String coreUrl = replica.getStr("base_url") + "/" + replica.getStr("core");
        coreNameToCoreUrl.put(replica.getStr("core"), coreUrl);
      }
    }

    log.warn("cores: {}", coreNameToCoreUrl);
    
    solrServer.shutdown();
    System.exit(1);

    // TODO: rework.
//    queryThread = new QueryThread(sourceClient, sourceCollection, uniqueKeyFieldName,
//        queryBatchSize);
//    final String name = "query_" + sourceCollection;
//    queryThread.setName(name);
//    queryThread.setDaemon(true);
//    log.info("Starting {} thread", name);
//    queryThread.start();
//
//    StaticStuff.logDebug(log, "Query batch size : {}", queryBatchSize);
//
//    // TODO: log this for each thread.
//    log.info("Query thread started.");
////    log.info("Start numFound: {}", QueryThread.getStartNumFound());

    /*
     * No idea why this sleep makes things work. Without it, the program dies saying
     * that the target SolrClient is stopped.
     */
    StaticStuff.sleep(5, TimeUnit.SECONDS);

    /*
     * Begin the main loop where it just waits for all the other threads to do their
     * thing.
     */
    // TODO: Log for all threads
//    log.info("---");
//    log.info("End numFound: {}", QueryThread.getEndNumFound());

    log.info("Main thread ending!");
  }

  public static List<String> getFilters() {
    return fq;
  }
}
