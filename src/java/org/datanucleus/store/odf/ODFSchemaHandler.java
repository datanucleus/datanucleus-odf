/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
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

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.AbstractStoreSchemaHandler;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.store.schema.table.Table;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;

/**
 * Handler for schema operations with ODF documents.
 */
public class ODFSchemaHandler extends AbstractStoreSchemaHandler
{
    public ODFSchemaHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#createSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void createSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        OdfSpreadsheetDocument spreadsheet = (OdfSpreadsheetDocument)connection;
        ManagedConnection mconn = null;
        try
        {
            if (spreadsheet == null)
            {
                mconn = storeMgr.getConnection(-1);
                spreadsheet = (OdfSpreadsheetDocument)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Find/Create the sheet (table) appropriate for storing objects of this class
                    StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    Table schemaTable = null;
                    if (storeData != null)
                    {
                        schemaTable = (Table) storeData.getProperties().get("tableObject");
                    }
                    else
                    {
                        schemaTable = new CompleteClassTable(storeMgr, cmd, new SchemaVerifierImpl(storeMgr, cmd, clr));
                    }

                    String sheetName = schemaTable.getIdentifier();
                    OdfTable table = spreadsheet.getTableByName(sheetName);
                    if (table == null)
                    {
                        // Table for this class doesn't exist yet so create
                        table = ODFUtils.addTableForClass(spreadsheet, cmd, schemaTable, storeMgr);
                    }
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#deleteSchemaForClasses(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void deleteSchemaForClasses(Set<String> classNames, Properties props, Object connection)
    {
        OdfSpreadsheetDocument spreadsheet = (OdfSpreadsheetDocument)connection;
        ManagedConnection mconn = null;
        try
        {
            if (spreadsheet == null)
            {
                mconn = storeMgr.getConnection(-1);
                spreadsheet = (OdfSpreadsheetDocument)mconn.getConnection();
            }

            Iterator<String> classIter = classNames.iterator();
            ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
            while (classIter.hasNext())
            {
                String className = classIter.next();
                AbstractClassMetaData cmd = storeMgr.getMetaDataManager().getMetaDataForClass(className, clr);
                if (cmd != null)
                {
                    // Find/Delete the sheet (table) appropriate for storing objects of this class
                    StoreData storeData = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                    Table schemaTable = null;
                    if (storeData != null)
                    {
                        schemaTable = (Table) storeData.getProperties().get("tableObject");
                    }
                    else
                    {
                        schemaTable = new CompleteClassTable(storeMgr, cmd, new SchemaVerifierImpl(storeMgr, cmd, clr));
                    }

                    OdfTable table = spreadsheet.getTableByName(schemaTable.getIdentifier());
                    if (table != null)
                    {
                        table.remove();
                    }
                }
            }
        }
        finally
        {
            if (mconn != null)
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.schema.AbstractStoreSchemaHandler#validateSchema(java.util.Set, java.util.Properties, java.lang.Object)
     */
    @Override
    public void validateSchema(Set<String> classNames, Properties props, Object connection)
    {
        // TODO Implement validation of the ODF document
        super.validateSchema(classNames, props, connection);
    }
}