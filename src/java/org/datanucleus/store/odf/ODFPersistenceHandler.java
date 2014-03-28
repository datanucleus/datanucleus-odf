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

import java.sql.Timestamp;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.odf.fieldmanager.FetchFieldManager;
import org.datanucleus.store.odf.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * Persistence Handler for Open Document Format (ODF) datastores.
 * Handles the insert/update/delete/fetch/locate operations by using ODF Toolkit.
 * <h3>Field -> Cell mapping</h3>
 * A field is mapped to a cell. The field metadata can define a cell number (starting at 0).
 * Specifying the cell number means that the user takes responsibility for the cell numbers
 * being consistent. The default cell numbering is alphabetical start in the root class, and working
 * down the inheritance tree to the actual instance class.
 */
public class ODFPersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_ODF = Localiser.getInstance(
        "org.datanucleus.store.odf.Localisation", ODFStoreManager.class.getClassLoader());

    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public ODFPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#close()
     */
    public void close()
    {
        // Nothing to do since we maintain no resources
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#insertObject(org.datanucleus.state.ObjectProvider)
     */
    public void insertObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.Insert.Start", 
                    StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
            }

            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
            }
            Table schemaTable = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");

            // Find the sheet (table) appropriate for storing objects of this class TODO Coordinate this with manageClasses above, maybe not needed here
            String sheetName = schemaTable.getIdentifier();
            OdfTable table = spreadsheetDoc.getTableByName(sheetName);
            if (table == null)
            {
                // Table for this class doesn't exist yet so create
                table = ODFUtils.addTableForClass(spreadsheetDoc, cmd, schemaTable, storeMgr);
            }

            if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Enforce uniqueness of datastore rows
                try
                {
                    locateObject(op);
                    throw new NucleusUserException(LOCALISER_ODF.msg("ODF.Insert.ObjectWithIdAlreadyExists",
                        StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Do nothing since object with this id doesn't exist
                }
            }

            // Add a new row to this table for this object
            OdfTableRow row = table.appendRow();

            // Add cells for the fields to this row
            op.provideFields(cmd.getAllMemberPositions(), new StoreFieldManager(op, row, true, schemaTable));

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                int colIndex = schemaTable.getDatastoreIdColumn().getPosition();
                OdfTableCell cell = row.getCellByIndex(colIndex);
                Object idKey = ((OID)op.getInternalObjectId()).getKeyValue();
                if (idKey instanceof String)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                    cell.setStringValue((String)idKey);
                }
                else
                {
                    long idValue = ((Long)((OID)op.getInternalObjectId()).getKeyValue()).longValue();
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(new Double(idValue));
                }
                cell.getOdfElement().setStyleName("DN_PK");
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), null);
                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                }
                op.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(LOCALISER_ODF.msg("ODF.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId(), "" + nextVersion));
                }

                int colIndex = schemaTable.getVersionColumn().getPosition();
                OdfTableCell cell = row.getCellByIndex(colIndex);
                if (nextVersion instanceof Long)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(((Long)nextVersion).doubleValue());
                }
                else if (nextVersion instanceof Timestamp)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(new Double(((Timestamp)nextVersion).getTime()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug(LOCALISER_ODF.msg("ODF.Insert.ObjectPersisted",
                    StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
            }

            // Use SCO wrappers from this point
            op.replaceAllLoadedSCOFieldsWithWrappers();
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Error inserting object", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#updateObject(org.datanucleus.state.ObjectProvider, int[])
     */
    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            if (!storeMgr.managesClass(cmd.getFullClassName()))
            {
                // Make sure schema exists, using this connection
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
            }
            Table schemaTable = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");

            // TODO Add optimistic checks
            int[] updatedFieldNums = fieldNumbers;
            Object nextVersion = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Version object so calculate version to store with
                Object currentVersion = op.getTransactionalVersion();
                if (currentVersion instanceof Integer)
                {
                    // Cater for Integer-based versions TODO Generalise this
                    currentVersion = Long.valueOf(((Integer)currentVersion).longValue());
                }
                nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);

                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());

                    if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuffer fieldStr = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.Update.Start", 
                    StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId(), fieldStr.toString()));
            }

            // Update the row in the worksheet
            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(op, spreadsheetDoc, true);
            if (row == null)
            {
                String sheetName = storeMgr.getNamingFactory().getTableName(cmd);
                throw new NucleusDataStoreException(LOCALISER_ODF.msg("ODF.RowNotFoundForSheetForWorkbook",
                    sheetName, StringUtils.toJVMIDString(op.getInternalObjectId())));
            }
            op.provideFields(updatedFieldNums, new StoreFieldManager(op, row, false, schemaTable));

            if (vermd != null)
            {
                // Versioned object so set version cell in spreadsheet
                op.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(LOCALISER_ODF.msg("ODF.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId(), "" + nextVersion));
                }

                int verCellIndex = schemaTable.getVersionColumn().getPosition();
                OdfTableCell verCell = row.getCellByIndex(verCellIndex);
                if (nextVersion instanceof Long)
                {
                    verCell.setDoubleValue(new Double((Long)nextVersion));
                }
                else if (nextVersion instanceof Integer)
                {
                    verCell.setDoubleValue(new Double((Integer)nextVersion));
                }
                else if (nextVersion instanceof Timestamp)
                {
                    verCell.setDoubleValue(new Double(((Timestamp)nextVersion).getTime()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Exception updating object", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#deleteObject(org.datanucleus.state.ObjectProvider)
     */
    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            Table schemaTable = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
            // TODO Add optimistic checks

            // Delete all reachable PC objects (due to dependent-field)
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete this object
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.Delete.Start", 
                    StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
            }

            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(op, spreadsheetDoc, false);
            if (row == null)
            {
                throw new NucleusObjectNotFoundException("object not found", op.getObject());
            }
            else
            {
                // Remove the row node
                spreadsheetDoc.getTableByName(schemaTable.getIdentifier()).removeRowsByIndex(row.getRowIndex(), 1);
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER_ODF.msg("ODF.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Exception deleting object", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#fetchObject(org.datanucleus.state.ObjectProvider, int[])
     */
    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = op.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuffer str = new StringBuffer("Fetching object \"");
            str.append(StringUtils.toJVMIDString(op.getObject())).append("\" (id=");
            str.append(op.getInternalObjectId()).append(")").append(" fields [");
            for (int i=0;i<fieldNumbers.length;i++)
            {
                if (i > 0)
                {
                    str.append(",");
                }
                str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
            }
            str.append("]");
            NucleusLogger.PERSISTENCE.debug(str.toString());
        }

        ExecutionContext ec = op.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        boolean notFound = false;
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            Table schemaTable = (Table) ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getProperty("tableObject");
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_ODF.msg("ODF.Fetch.Start", 
                    StringUtils.toJVMIDString(op.getObject()), op.getInternalObjectId()));
            }

            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(op, spreadsheetDoc, false);
            if (row == null)
            {
                notFound = true;
            }
            else
            {
                op.replaceFields(fieldNumbers, new FetchFieldManager(op, row, schemaTable));

                if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER_ODF.msg("ODF.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
                if (ec.getStatistics() != null)
                {
                    ec.getStatistics().incrementNumReads();
                    ec.getStatistics().incrementFetchCount();
                }
                // TODO Version retrieval
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Exception fetching object", e);
        }
        finally
        {
            mconn.release();
        }

        if (notFound)
        {
            throw new NucleusObjectNotFoundException("object not found", op.getObject());
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#findObject(org.datanucleus.ExecutionContext, java.lang.Object)
     */
    public Object findObject(ExecutionContext om, Object id)
    {
        return null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#locateObject(org.datanucleus.state.ObjectProvider)
     */
    public void locateObject(ObjectProvider sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(sm, spreadsheetDoc, false);
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumReads();
            }

            if (row != null)
            {
                return;
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception thrown when querying object", e);
            throw new NucleusDataStoreException("Error when trying to find object with id=" + sm.getInternalObjectId(), e);
        }
        finally
        {
            mconn.release();
        }

        throw new NucleusObjectNotFoundException("Object not found", sm.getInternalObjectId());
    }
}