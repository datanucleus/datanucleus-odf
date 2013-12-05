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

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.odf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.util.ClassUtils;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;

/**
 * StoreManager for OpenOffice (spreadsheet) ODF docs.
 */
public class ODFStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    public ODFStoreManager(ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super("odf", clr, ctx, props);

        // Check if ODFDOM JAR is in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.odftoolkit.odfdom.doc.OdfDocument", "odfdom.jar");

        persistenceHandler = new ODFPersistenceHandler(this);

        logConfiguration();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getNucleusConnection(org.datanucleus.ExecutionContext)
     */
    @Override
    public NucleusConnection getNucleusConnection(ExecutionContext ec)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("NonDurableIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        set.add("ORM");
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#createSchema(java.util.Set, java.util.Properties)
     */
    public void createSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Find/Create the sheet (table) appropriate for storing objects of this class
                    String sheetName = getNamingFactory().getTableName(cmd);
                    OdfTable table = spreadsheetDoc.getTableByName(sheetName);
                    if (table == null)
                    {
                        // Table for this class doesn't exist yet so create
                        table = ODFUtils.addTableForClass(spreadsheetDoc, cmd, sheetName, this);
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#deleteSchema(java.util.Set)
     */
    public void deleteSchema(Set<String> classNames, Properties props)
    {
        ManagedConnection mconn = getConnection(-1);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Find/Delete the sheet (table) appropriate for storing objects of this class
                    String sheetName = getNamingFactory().getTableName(cmd);
                    OdfTable table = spreadsheetDoc.getTableByName(sheetName);
                    if (table != null)
                    {
                        table.remove();
                    }
                }
            }
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.SchemaAwareStoreManager#validateSchema(java.util.Set)
     */
    public void validateSchema(Set<String> classNames, Properties props)
    {
        // TODO Auto-generated method stub
        
    }
}