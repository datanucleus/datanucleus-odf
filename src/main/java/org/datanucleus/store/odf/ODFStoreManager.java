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
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;

/**
 * StoreManager for OpenOffice (spreadsheet) ODF docs.
 */
public class ODFStoreManager extends AbstractStoreManager implements SchemaAwareStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.odf.Localisation", ODFStoreManager.class.getClassLoader());
    }

    public ODFStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext ctx, Map<String, Object> props)
    {
        super("odf", clr, ctx, props);

        // Check if ODFDOM JAR is in CLASSPATH
        ClassUtils.assertClassForJarExistsInClasspath(clr, "org.odftoolkit.odfdom.doc.OdfDocument", "odfdom.jar");

        schemaHandler = new ODFSchemaHandler(this);
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
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_NONDURABLE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        return set;
    }

    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = getConnection(-1);
        try
        {
            OdfSpreadsheetDocument spreadsheet = (OdfSpreadsheetDocument)mconn.getConnection();
            manageClasses(classNames, clr, spreadsheet);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, OdfSpreadsheetDocument spreadsheet)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isEmbeddedOnly())
            {
                if (cmd.isAbstract())
                {
                    continue;
                }

                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }

        // Create schema for classes
        schemaHandler.createSchemaForClasses(clsNameSet, null, spreadsheet);
    }

    public void createSchema(String schemaName, Properties props)
    {
        schemaHandler.createSchema(schemaName, props, null);
    }

    public void deleteSchema(String schemaName, Properties props)
    {
        schemaHandler.deleteSchema(schemaName, props, null);
    }

    public void createSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.createSchemaForClasses(classNames, props, null);
    }

    public void deleteSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.deleteSchemaForClasses(classNames, props, null);
    }

    public void validateSchemaForClasses(Set<String> classNames, Properties props)
    {
        schemaHandler.validateSchema(classNames, props, null);
    }
}