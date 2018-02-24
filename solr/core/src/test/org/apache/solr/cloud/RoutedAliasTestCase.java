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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoutedAliasTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected CloudSolrClient solrClient;
  protected CloseableHttpClient httpClient;

  // This allows us to plug in a provider that returns a client that talks to an external solr cluster
  //
  // !!! WARNING !!! ONLY point this test at clusters specifically set up for testing. The FIRST
  //                 thing this test does is delete ALL aliases and ALL collections in the cluster provided.
  //
  @SuppressWarnings("WeakerAccess")
  protected CloudSolrClientProvider cloudClientProvider ;
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(solrClient, httpClient, cloudClientProvider);
  }

  @Before
  public void setUp() throws Exception {
    if (cloudClientProvider == null){
      cloudClientProvider = new  CloudSolrClientProvider() {

        Set<CloudSolrClient> clients = new HashSet<>();

        @Override
        public CloudSolrClient getProvidedClient() {
          CloudSolrClient cloudSolrClient = getCloudSolrClient(cluster);
          cloudSolrClient.setDefaultCollection(getTestName()); // test should set this if somethinge else desired
          clients.add(cloudSolrClient);
          return cloudSolrClient;
        }

        @Override
        public boolean iKnowThisTestDeletesTheEntireCluster() {
          return true; // using the test infra cluster so no worries...
        }

        @Override
        public Set<CloudSolrClient> getClients() {
          return Collections.unmodifiableSet(clients);
        }

      };
    }
    super.setUp();
    if (!cloudClientProvider.iKnowThisTestDeletesTheEntireCluster()) {
      fail("Apparently you don't know that this test WILL delete an entire cluster and have provided a custom client. " +
          "We are failing this test to save you from possibly deleting data you care about. To avoid this failure " +
          "implement the iKnowThisTestDeletesTheEntireCluster() method to return true, at which point, " +
          "you HAVE been warned.");
      return;
    }

    solrClient = ensureCloudClient(solrClient);
    httpClient = (CloseableHttpClient) solrClient.getHttpClient();

    // here we make sure we have a clean working environment. Delete at front so that failing tests
    // leave their data behind for inspection if hooked up to a separate cluster.

    // delete aliases first since they refer to the collections
    ZkStateReader zkStateReader = solrClient.getZkStateReader();
    //TODO create an API to delete collections attached to the routed alias when the alias is removed
    zkStateReader.aliasesManager.update();// ensure we're seeing the latest
    zkStateReader.aliasesManager.applyModificationAndExportToZk(aliases -> {
      Aliases a = zkStateReader.aliasesManager.getAliases();
      for (String alias : a.getCollectionAliasMap().keySet()) {
        a = a.cloneWithCollectionAlias(alias,null); // remove
      }
      return a;
    });
    List<String> collections = CollectionAdminRequest.listCollections(solrClient);
    for (String collection : collections) {
      CollectionAdminRequest.deleteCollection(collection).process(solrClient);
    }
  }

  protected CloudSolrClient ensureCloudClient(CloudSolrClient solrClient) {
    if (solrClient== null) {
      solrClient = cloudClientProvider.getProvidedClient();
    }
    int attempts = 0;
    ZkStateReader zkStateReader = null;
    do {
      attempts++;
      // try until we get a client that DOES have a zkStateReader
      try {
        zkStateReader = solrClient.getZkStateReader();
      } catch ( IllegalStateException e) {
        // got a direct to leader client that has no zkStateReader, try again
        if (attempts > 1000) {
          fail("Your provider does not appear to reliably produce a cloud capable client within 1000 requests");
        }
        solrClient = cloudClientProvider.getProvidedClient();
      }
    } while  (zkStateReader == null);

    return solrClient;
  }

  interface CloudSolrClientProvider extends Closeable {
    /**
     * Provide a client. This method must provide a Cloud capable client at least occasionally. It is
     * acceptable to sometimes provide (randomized) clients that include clients with no zkStateReader
     * so long as there is a non-zero frequency with which a cloud capable client will be returned if this
     * method is invoked repeatedly.
     *
     * @return a client tha can be used in tests.
     */
    CloudSolrClient getProvidedClient();

    /**
     * Don't say we didn't tell you...
     */
    default boolean iKnowThisTestDeletesTheEntireCluster() {
      return false;
    }

    /**
     * The provider must retain a handle to and be ready to close any clients distributed.
     */
    default void close(){
      for (CloudSolrClient cloudSolrClient : getClients()) {
        try {
          cloudSolrClient.close();
        } catch (IOException e) {
          log.warn("Unable to close client!", e);
        }
      }
    }

    /**
     * All the clients that have been distributed by {@link #getProvidedClient()}.
     *
     * @return a <tt>Set</tt> of provided clients
     */
    Set<CloudSolrClient> getClients();
  }

}
