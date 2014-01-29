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
import org.datanucleus.PersistenceNucleusContext;
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
    public ODFStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
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

    public void createSchema(String schemaName, Properties props)
    {
        throw new UnsupportedOperationException("Dont support the creation of a schema with ODF since there is no equivalent concept");
    }

    public void deleteSchema(String schemaName, Properties props)
    {
        throw new UnsupportedOperationException("Dont support the deletion of a schema with ODF since there is no equivalent concept");
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
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

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
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

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        // TODO Auto-generated method stub
        
    }
}