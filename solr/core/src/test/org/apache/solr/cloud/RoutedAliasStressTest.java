/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.DateMathParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SolrTestCaseJ4.SuppressSSL
public class RoutedAliasStressTest extends RoutedAliasTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int THREADS_PER_INTERVAL = 1;
  public static final int INTERVALS = 10; // how many intervals the TRA keeps
  public static final int TOTAL_THREADS = 2 * THREADS_PER_INTERVAL * INTERVALS;
  public static String INTERVAL_DURATION = "+3SECOND";

  @BeforeClass
  public static void setupCluster() throws Exception {
    int nodeCount = Integer.parseInt(System.getProperty("solr.tra.stress.nodes", "4"));
    configureCluster(nodeCount).configure();
  }

  @Override
  @Before
  public void setUp() throws Exception {
//    this.cloudClientProvider = new CloudSolrClientProvider() {
//
//      private volatile CloudSolrClient client;
//
//      @Override
//      public CloudSolrClient getProvidedClient() {
//        CloudSolrClient tmpClient = client;
//        if (tmpClient == null) {
//          synchronized (this) {
//            tmpClient = client;
//            if (tmpClient == null)
//            tmpClient = client = new CloudSolrClient.Builder().withZkHost("localhost:2181").withZkChroot("/solr_stress_test").build();
//          }
//        }
//        return tmpClient;
//      }
//
//    };

    super.setUp();

  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * A test that blasts documents at cloudsolrclient  time allowed until after all collections have been deleted at least once.
   */
  @Test
  public void testForDoubleValidRange() throws InterruptedException, IOException, SolrServerException {

    CollectionAdminRequest.Create template = CollectionAdminRequest.createCollection("ignored", "_default", 2, 2);
    CloudSolrClient providedClient = cloudClientProvider.getProvidedClient();
    providedClient.setDefaultCollection(getTestName());
    CollectionAdminRequest.createTimeRoutedAlias(getTestName(),
        "NOW/MINUTE+1MINUTE+10SECOND", INTERVAL_DURATION, "time_dt", template)
        .process(providedClient);

    Thread[] testerThreads = new Thread[TOTAL_THREADS];

    DateMathParser parser = new DateMathParser();

    // wait a minute to get things settled, make sure all is started up
    Instant beginningAt = DateMathParser.parseMath(new Date(),"NOW/MINUTE+1MINUTE").toInstant();

    Instant[] timesForTesters = new Instant[INTERVALS * 2 + 1];
    Instant temp = beginningAt;
    for (int i = 0; i < timesForTesters.length; i++) {
      timesForTesters[i] = temp;
      parser.setNow(Date.from(temp));
      temp = DateMathParser.parseMath(Date.from(temp), "NOW" + INTERVAL_DURATION).toInstant();
    }

    CountDownLatch allComplete = new CountDownLatch(TOTAL_THREADS);
    List<IntervalTester> testers = new ArrayList<>(TOTAL_THREADS);

    // prophylactic thread to shut things down if errors have caused us to be failing to stop
    // also used to ensure all threads are interrupted and complete at end of test...
    Thread timeoutThread = new Thread(() -> {
      try {
        Thread.sleep( INTERVALS * 60 * 1000); // assuming test should take no more than 1 min per interval
      } catch (InterruptedException e) {
      }
      for (Thread testerThread : testerThreads) {
        if (testerThread != null) {
          testerThread.interrupt();
        }
      }
    });
    timeoutThread.start();

    try {
      for (int i = 0; i < TOTAL_THREADS; i++) {
        int intervalStart = (int) Math.floor(i / (float) THREADS_PER_INTERVAL);
        int intervalEnd = intervalStart + 1;
        Instant atLeast = timesForTesters[intervalStart];
        Instant butAlwaysBefore = timesForTesters[intervalEnd];
        // TODO: add some data to the documents too?
        TemporalDocumentGenerator generator = new TemporalDocumentGenerator("time_dt", atLeast, butAlwaysBefore);
        IntervalTester tester = new IntervalTester(generator, cloudClientProvider.getProvidedClient(), beginningAt, timesForTesters[timesForTesters.length - 1], 100, 100);
        testers.add(tester);
        testerThreads[i] = new Thread(() -> {
          try {
            tester.run();
          } finally {
            allComplete.countDown();
          }
        });
        testerThreads[i].setDaemon(true);
        testerThreads[i].start();
      }
    } catch (Exception e){
      // ensure that the threads don't run long even if there's a bug. This should hopefully avoid issues with
      // stuck threads causing errors with test infrastructure thread tracking by ensuring all threads complete;
      timeoutThread.interrupt();
      Thread.sleep(2000);
      throw e;
    }
    allComplete.await();
    timeoutThread.interrupt(); // will cause timeout thread to complete so it doesn't linger
    timeoutThread.join();      // wait for said completion

    //nocommit : the above doesn't seem to be working as well as expected. Sometimes threads still linger :(

    // some time for possibly overloaded client to recover before test ends and mini cluster shuts down...
    // not sure this is needed...
    Thread.sleep(5000);

    int successCount = 0;
    for (IntervalTester tester : testers) {
      tester.assertContiguousSuccess();
      if (tester.someSuccess()) {
        successCount++;
      }
    }
    System.out.println(successCount);
    assertTrue(successCount > 0);

    // TODO: make sure set of collections is as expected (some should have been deleted)

    // TODO: run through added docs, make sure all are in correct collection
  }


  /**
   * A class to test addition of documents over time. The intended use is that N of these are set up to push
   * documents for a short time before the TRA would accept documents and a short time after. Success is defined
   * by an optional continuous run of failed attempts (before the interval can be created, per the TRA metadata),
   * A period of success (While the TRA will accept that data) and an optional period of failure after the TRA
   * stops accepting data for this interval.
   * <p>
   * Interval Testers should be configured such that their interval matches the TRA intervals.
   * <p>
   * IntervalTester is designed to stop all testing immediately upon receiving and interrupted exception.
   */
  static class IntervalTester implements Runnable {

    private static AtomicInteger instanceNum = new AtomicInteger(0);

    private TemporalDocumentGenerator generator;
    private CloudSolrClient cloudSolrClient;
    private Instant start;
    private Instant stop;
    private int batchSize;
    private int testerNum = instanceNum.incrementAndGet();

    private LinkedHashMap<Long, Boolean> results = new LinkedHashMap<>();
    private LinkedHashMap<Long, Throwable> errors = new LinkedHashMap<>();
    private int interBatchPause;


    IntervalTester(TemporalDocumentGenerator generator, CloudSolrClient cloudSolrClient, Instant start, Instant stop, int batchSize, int interBatchPause) {
      this.generator = generator;
      this.cloudSolrClient = cloudSolrClient;
      this.start = start;
      this.stop = stop;
      this.batchSize = batchSize;
      this.interBatchPause = interBatchPause;
    }

    @Override
    public void run() {
      try {
        while (start.isAfter(Instant.now())) {
          Thread.sleep(100);
        }
        log.info("ACTIVE: {}", this);
        while (stop.isAfter(Instant.now())) {
          List<SolrInputDocument> docs = new ArrayList<>(batchSize);
          for (int i = 0; i < batchSize; i++) {
            docs.add(generator.nextDoc());
          }
          Thread.sleep(interBatchPause);
          try {
            UpdateResponse add = cloudSolrClient.add(docs);
            int status = add.getStatus();
            // todo: we can be smarter here and store just the transitions from success to failure rather
            // than every success/fail which can cause memory probs when the batch delay is reduced
            if (status == 0) {
              for (SolrInputDocument doc : docs) {
                log.info("success for {} in {}", doc, this );
                results.put((Long) doc.get("id").getFirstValue(), true);
              }
            } else {
              log.info(add.getResponseHeader().toString());
              for (SolrInputDocument doc : docs) {
                results.put((Long) doc.get("id").getFirstValue(), false);
              }
            }

          } catch (SolrServerException | SolrException | IOException e) {
            if (!e.getMessage().contains("couldn't be routed")) {  // that exception is expected for docs out of range
              log.info("EXCEPTION:", e);
            }
            for (SolrInputDocument doc : docs) {
              results.put((Long) doc.get("id").getFirstValue(), false);
              errors.put((Long) doc.get("id").getFirstValue(), e);
            }
          }
        }
      } catch (InterruptedException e) {
        errors.put(Long.MAX_VALUE, e);
        e.printStackTrace();
      }
    }

    void assertContiguousSuccess() {
      if (results.size() == 0) {
        fail("no successful results?");
      } else {
        boolean successRegionStart = false;
        boolean successRegionEnd = false;
        for (Boolean result : results.values()) {
          successRegionStart |= result;
          successRegionEnd |= successRegionStart && !result;
          if (result && successRegionEnd) {
            fail("Found failures within the success region!");
          }
        }
      }
    }

    boolean someSuccess() {
      for (Boolean success : results.values()) {
        if (success) {
          return true;
        }
      }
      return false;
    }


    @Override
    public String toString() {
      return "IntervalTester{" +
          "start=" + start +
          ", stop=" + stop +
          ", batchSize=" + batchSize +
          ", testerNum=" + testerNum +
          '}';
    }
  }


  /**
   * A class to encapsulate the generation of randomized documents with a constrained
   * temporal field. Optionally a set of data can be added to ensure that actual indexing/analysis load
   * is simulated. The added data set will be randomly queried to generate documents.
   */
  static class TemporalDocumentGenerator {

    private static AtomicLong idGen = new AtomicLong(0);

    private ListMultimap<String, Object> fieldDataSet = ArrayListMultimap.create();
    private String temporalField;
    private Instant atLeast;
    private Instant butAlwaysBefore;
    private long interval;


    TemporalDocumentGenerator(String temporalField, Instant atLeast, Instant butAlwaysBefore) {
      this.temporalField = temporalField;
      this.atLeast = atLeast;
      this.butAlwaysBefore = butAlwaysBefore;
      newInterval();
    }

    private synchronized void newInterval() {
      this.interval = this.butAlwaysBefore.toEpochMilli() - this.atLeast.toEpochMilli();
    }

    public void addFieldData(String field, Object data) {
      fieldDataSet.put(field, data);
    }

    @SuppressWarnings("unused")
    public void setFieldDataSet(ArrayListMultimap<String, Object> dataSet) {
      this.fieldDataSet = dataSet;
    }

    public SolrInputDocument nextDoc() {
      SolrInputDocument next = new SolrInputDocument();
      next.setField("id", idGen.incrementAndGet());
      next.setField(temporalField, DateTimeFormatter.ISO_INSTANT.format(makeDate()));

      if (fieldDataSet.size() > 0) {
        // don't see value in simulating multivalue fields here...
        for (String field : fieldDataSet.keySet()) {
          List<Object> data = fieldDataSet.get(field);
          Object datum = data.get((int) (data.size() * random().nextFloat()));
          next.setField(field, datum);
        }
      }
      return next;
    }

    Instant makeDate() {
      double rndInterval = random().nextDouble() * interval;
      return atLeast.plusMillis((long) rndInterval);
    }

    public Instant getAtLeast() {
      return atLeast;
    }

    public synchronized void setAtLeast(Instant atLeast) {
      this.atLeast = atLeast;
      newInterval();
    }

    @SuppressWarnings("unused")
    public Instant getButAlwaysBefore() {
      return butAlwaysBefore;
    }

    @SuppressWarnings("unused")
    public synchronized void setButAlwaysBefore(Instant butAlwaysBefore) {
      this.butAlwaysBefore = butAlwaysBefore;
      newInterval();
    }
  }
}
