/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.odf.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.inmemory.JPQLInMemoryEvaluator;
import org.datanucleus.store.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * JPQL query for ODF documents.
 * Retrieves all objects in the worksheet of the candidate class, and then applies the
 * generic JPQLEvaluator to apply the filter, result, grouping, ordering etc.
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    private static final long serialVersionUID = 4599206494545227602L;

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, JPQLQuery q)
    {
        super(storeMgr, ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public JPQLQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec, query);
    }

    /**
     * Method to execute the query, specific to ODF datastores.
     * Here we retrieve all objects of the candidate type, and process them using the in-memory evaluator.
     * @param parameters Map of parameter values keyed by name.
     */
    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try
        {
            long startTime = 0;
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                startTime = System.currentTimeMillis();
                NucleusLogger.QUERY.debug(Localiser.msg("021046", QueryLanguage.JPQL.name(), getSingleStringQuery(), null));
            }
            List candidates = null;
            if (candidateCollection == null)
            {
                candidates = new ODFCandidateList(candidateClass, subclasses, ec, (String)getExtension(Query.EXTENSION_RESULT_CACHE_TYPE), mconn, ignoreCache, getFetchPlan());
            }
            else
            {
                candidates = new ArrayList(candidateCollection);
            }

            // Map any result restrictions onto the worksheet results
            JavaQueryInMemoryEvaluator resultMapper = new JPQLInMemoryEvaluator(this, candidates, compilation, 
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", QueryLanguage.JPQL.name(), 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            if (type == QueryType.BULK_DELETE)
            {
                ec.deleteObjects(results.toArray());
                return Long.valueOf(results.size());
            }
            else if (type == QueryType.BULK_UPDATE)
            {
                // TODO Support BULK UPDATE
                throw new NucleusException("Bulk Update is not yet supported");
            }
            else
            {
                return results;
            }
        }
        finally
        {
            mconn.release();
        }
    }
}