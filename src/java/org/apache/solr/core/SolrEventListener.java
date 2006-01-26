/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import org.apache.solr.util.NamedList;
import org.apache.solr.search.SolrIndexSearcher;

import java.util.logging.Logger;

/**
 * @author yonik
 * @version $Id: SolrEventListener.java,v 1.4 2005/05/25 04:26:47 yonik Exp $
 */
public interface SolrEventListener {
  static final Logger log = Logger.getLogger(SolrCore.class.getName());

  public void init(NamedList args);

  public void postCommit();

  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher);

}