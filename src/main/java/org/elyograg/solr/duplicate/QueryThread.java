package org.elyograg.solr.duplicate;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryThread extends Thread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final AtomicLong totalIndexTimeMillis = new AtomicLong();
  private final AtomicInteger requestCounter = new AtomicInteger();
  private final AtomicInteger avgLatencyMillis = new AtomicInteger();
  private final AtomicInteger lastLatencyMillis = new AtomicInteger();
  private final AtomicLong counter = new AtomicLong(0);
  private final AtomicLong startNumFound = new AtomicLong(0);
  private final AtomicLong endNumFound = new AtomicLong(0);
  private final SolrClient client;
  private final int batchSize;
  private final String core;
  private final String uniqueKey;
  private final Set<String> idSet = Collections.synchronizedSet(new HashSet<>());

  public QueryThread(final SolrClient clientParam, final String coreParam, final String ukParam,
      final int batchParam) {
    client = clientParam;
    batchSize = batchParam;
    core = coreParam;
    uniqueKey = ukParam;
    this.setDaemon(true);
    this.setName("query." + coreParam);
  }

  @Override
  public final void run() {
    String cursorMark = CursorMarkParams.CURSOR_MARK_START;
    QueryResponse rsp;
    boolean done = false;
    while (StaticStuff.getGlobalRunFlag() && !done) {
      final SolrQuery q = new SolrQuery("*:*");
      q.set("distrib", "false");
      q.set("rows", batchSize);
      q.set("sort", uniqueKey + " asc");
      q.set("fl", uniqueKey);
      final List<String> filters = Main.getFilters();
      if (filters != null) {
        for (final String f : filters) {
          q.addFilterQuery(f);
        }
      }
      q.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      final QueryRequest req = new QueryRequest(q);
      try {
        req.setMethod(METHOD.POST);
        final long latencyStartNanos = System.nanoTime();
        rsp = req.process(client);
        addIdstoSet(rsp);
        final long elapsedMillis = TimeUnit.MILLISECONDS
            .convert(System.nanoTime() - latencyStartNanos, TimeUnit.NANOSECONDS);
        totalIndexTimeMillis.addAndGet(elapsedMillis);
        avgLatencyMillis.set((int) (totalIndexTimeMillis.get() / requestCounter.incrementAndGet()));
        lastLatencyMillis.set((int) elapsedMillis);
      } catch (final Exception e) {
        log.error("Core {} cursorMark {} query exception, aborting import", core, cursorMark, e);
        throw new RuntimeException("Problem querying, aborting import.", e);
        /*
         * TODO: This program REALLY needs an uncaught exception handler and a global
         * run flag to quickly make threads die when the handler runs.
         */
      }
      final String nextCursorMark = rsp.getNextCursorMark();
      StaticStuff.logDebug(log, "Query info: cursorMark {}", cursorMark);

      // TODO: Build ID structures for comparison.

      if (cursorMark.equals(nextCursorMark)) {
        done = true;
      }
      cursorMark = nextCursorMark;
    }

    log.info("Closing SolrClient");
    try {
      if (client != null) {
        client.close();
      }
    } catch (final Exception e) {
      log.error("Error closing Solr client!");
    }
    log.info("-=-=-=-=-=-=-=-=- Query thread ended");

  }

  private void addIdstoSet(final QueryResponse rsp) {
    final SolrDocumentList docs = rsp.getResults();
    for (final SolrDocument doc : docs) {
      final String id = (String) doc.getFieldValue(uniqueKey);
      final boolean ok = idSet.add(id);
      if (!ok) {
        log.error("ID {} is already in set. THIS SHOULD NOT HAPPEN.", id);
      }
    }
  }

  public final long getAddedCount() {
    return counter.get();
  }

  public final int getAvgLatencyMillis() {
    return avgLatencyMillis.get();
  }

  public final int getLastLatencyMillis() {
    return lastLatencyMillis.get();
  }

  public final int getRequestCount() {
    return requestCounter.get();
  }

  public long getStartNumFound() {
    return startNumFound.get();
  }

  public long getEndNumFound() {
    return endNumFound.get();
  }

  /**
   * @return null if thread is still alive, or the set of found IDs.
   */
  public Set<String> getIdSet() {
    if (this.isAlive()) {
      return null;
    } else {
      return idSet;
    }
  }
}
