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

import java.util.List;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.After;
import org.junit.Before;

public class RoutedAliasTestCase extends SolrCloudTestCase {

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
    IOUtils.close(solrClient, httpClient);
  }

  @Before
  public void setUp() throws Exception {
    cloudClientProvider = new  CloudSolrClientProvider() {
      @Override
      public CloudSolrClient getProvidedClient() {
        CloudSolrClient cloudSolrClient = getCloudSolrClient(cluster);
        cloudSolrClient.setDefaultCollection(getTestName()); // test should set this if somethinge else desired
        return cloudSolrClient;
      }

      @Override
      public boolean iKnowThisTestDeletesTheEntireCluster() {
        return true; // using the test infra cluster so no worries...
      }
    };
    super.setUp();
    if (!cloudClientProvider.iKnowThisTestDeletesTheEntireCluster()) {
      fail("Apparently you don't know that this test WILL delete an entire cluster and have provided a custom client. " +
          "We are failing this test to save you from possibly deleting data you care about. To avoid this failure " +
          "implement the iKnowThisTestDeletesTheEntireCluster() method to return true, at which point, " +
          "you HAVE been warned.");
      return;
    }
    solrClient = cloudClientProvider.getProvidedClient();
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

  interface CloudSolrClientProvider {
    CloudSolrClient getProvidedClient();

    /**
     * Don't say we didn't tell you...
     */
    default boolean iKnowThisTestDeletesTheEntireCluster() {
      return false;
    }
  }

}
