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
import java.util.HashSet;
import java.util.Set;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.odf.fieldmanager.FetchFieldManager;
import org.datanucleus.store.odf.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.SurrogateColumnType;
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
 * <p>
 * <b>Field to Cell mapping</b>
 * A field is mapped to a cell. The field metadata can define a cell number (starting at 0).
 * Specifying the cell number means that the user takes responsibility for the cell numbers
 * being consistent. The default cell numbering is alphabetical start in the root class, and working
 * down the inheritance tree to the actual instance class.
 */
public class ODFPersistenceHandler extends AbstractPersistenceHandler
{
    /**
     * Constructor.
     * @param storeMgr Manager for the datastore
     */
    public ODFPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    @Override
    public void close()
    {
        // Nothing to do since we maintain no resources
    }

    @Override
    public void insertObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.Insert.Start", StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
            }

            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table schemaTable = sd.getTable();

            // Find the sheet (table) appropriate for storing objects of this class TODO Coordinate this with manageClasses above, maybe not needed here
            String sheetName = schemaTable.getName();
            OdfTable table = spreadsheetDoc.getTableByName(sheetName);

            if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Enforce uniqueness of datastore rows
                try
                {
                    locateObject(sm);
                    throw new NucleusUserException(Localiser.msg("ODF.Insert.ObjectWithIdAlreadyExists",
                        StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Do nothing since object with this id doesn't exist
                }
            }

            // Add a new row to this table for this object
            OdfTableRow row = table.appendRow();

            // Add cells for the fields to this row
            sm.provideFields(cmd.getAllMemberPositions(), new StoreFieldManager(sm, row, true, schemaTable));

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                int colIndex = schemaTable.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                OdfTableCell cell = row.getCellByIndex(colIndex);
                Object idKey = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
                if (idKey instanceof String)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                    cell.setStringValue((String)idKey);
                }
                else
                {
                    long idValue = ((Long)IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId())).longValue();
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(Double.valueOf(idValue));
                }
                cell.getOdfElement().setStyleName("DN_PK");
            }

            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, null);
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
                sm.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("ODF.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId(), "" + nextVersion));
                }

                OdfTableCell verCell = null;
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    MemberColumnMapping mapping = schemaTable.getMemberColumnMappingForMember(verMmd);
                    verCell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                }
                else
                {
                    int colIndex = schemaTable.getSurrogateColumn(SurrogateColumnType.VERSION).getPosition();
                    verCell = row.getCellByIndex(colIndex);
                }
                if (nextVersion instanceof Long)
                {
                    verCell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    verCell.setDoubleValue(((Long)nextVersion).doubleValue());
                }
                else if (nextVersion instanceof Timestamp)
                {
                    verCell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    verCell.setDoubleValue(Double.valueOf(((Timestamp)nextVersion).getTime()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }

            if (NucleusLogger.DATASTORE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE.debug(Localiser.msg("ODF.Insert.ObjectPersisted", StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
            }

            // Use SCO wrappers from this point
            sm.replaceAllLoadedSCOFieldsWithWrappers();
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

    @Override
    public void updateObject(ObjectProvider sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table schemaTable = sd.getTable();

            // TODO Add optimistic checks
            int[] updatedFieldNums = fieldNumbers;
            Object nextVersion = null;
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd != null)
            {
                // Version object so calculate version to store with
                Object currentVersion = sm.getTransactionalVersion();
                if (currentVersion instanceof Integer)
                {
                    // Cater for Integer-based versions TODO Generalise this
                    currentVersion = Long.valueOf(((Integer)currentVersion).longValue());
                }
                nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);

                if (vermd.getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());

                    if (verMmd.getType() == Integer.class || verMmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

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
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.Update.Start", 
                    StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId(), fieldStr.toString()));
            }

            // Update the row in the worksheet
            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(sm, spreadsheetDoc, true);
            if (row == null)
            {
                String sheetName = schemaTable.getName();
                throw new NucleusDataStoreException(Localiser.msg("ODF.RowNotFoundForSheetForWorkbook",
                    sheetName, StringUtils.toJVMIDString(sm.getInternalObjectId())));
            }
            sm.provideFields(updatedFieldNums, new StoreFieldManager(sm, row, false, schemaTable));

            if (vermd != null)
            {
                // Versioned object so set version cell in spreadsheet
                sm.setTransactionalVersion(nextVersion);
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("ODF.Insert.ObjectPersistedWithVersion",
                        StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId(), "" + nextVersion));
                }

                OdfTableCell verCell = null;
                if (vermd.getFieldName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    MemberColumnMapping mapping = schemaTable.getMemberColumnMappingForMember(verMmd);
                    verCell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                }
                else
                {
                    int verCellIndex = schemaTable.getSurrogateColumn(SurrogateColumnType.VERSION).getPosition();
                    verCell = row.getCellByIndex(verCellIndex);
                }
                if (nextVersion instanceof Long)
                {
                    verCell.setDoubleValue(Double.valueOf((Long)nextVersion));
                }
                else if (nextVersion instanceof Integer)
                {
                    verCell.setDoubleValue(Double.valueOf((Integer)nextVersion));
                }
                else if (nextVersion instanceof Timestamp)
                {
                    verCell.setDoubleValue(Double.valueOf(((Timestamp)nextVersion).getTime()));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.ExecutionTime", (System.currentTimeMillis() - startTime)));
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

    @Override
    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table schemaTable = sd.getTable();
            // TODO Add optimistic checks

            // Delete all reachable PC objects (due to dependent-field)
            sm.loadUnloadedFields();
            sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm));

            // Delete this object
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.Delete.Start", StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
            }

            OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(sm, spreadsheetDoc, false);
            if (row == null)
            {
                throw new NucleusObjectNotFoundException("Object not found for id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()), sm.getObject());
            }

            // Remove the row node
            spreadsheetDoc.getTableByName(schemaTable.getName()).removeRowsByIndex(row.getRowIndex(), 1);

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("ODF.ExecutionTime", (System.currentTimeMillis() - startTime)));
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

    @Override
    public void fetchObject(ObjectProvider sm, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
        {
            // Debug information about what we are retrieving
            StringBuilder str = new StringBuilder("Fetching object \"");
            str.append(StringUtils.toJVMIDString(sm.getObject())).append("\" (id=");
            str.append(sm.getInternalObjectId()).append(")").append(" fields [");
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

        // Strip out any non-persistent fields
        Set<Integer> nonpersistableFields = null;
        for (int i = 0; i < fieldNumbers.length; i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
            if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
            {
                if (nonpersistableFields == null)
                {
                    nonpersistableFields = new HashSet<Integer>();
                }
                nonpersistableFields.add(fieldNumbers[i]);
            }
        }
        if (nonpersistableFields != null)
        {
            // Just go through motions for non-persistable fields
            for (Integer fieldNum : nonpersistableFields)
            {
                sm.replaceField(fieldNum, sm.provideField(fieldNum));
            }
        }
        if (nonpersistableFields == null || nonpersistableFields.size() != fieldNumbers.length)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            boolean notFound = false;
            try
            {
                OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                if (sd == null)
                {
                    ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
                    sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                }
                Table schemaTable = sd.getTable();
                long startTime = System.currentTimeMillis();
                if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("ODF.Fetch.Start", StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
                }

                OdfTableRow row = ODFUtils.getTableRowForObjectInSheet(sm, spreadsheetDoc, false);
                if (row == null)
                {
                    notFound = true;
                }
                else
                {
                    if (nonpersistableFields != null)
                    {
                        // Strip out any nonpersistable fields
                        int[] persistableFieldNums = new int[fieldNumbers.length - nonpersistableFields.size()];
                        int pos = 0;
                        for (int i = 0; i < fieldNumbers.length; i++)
                        {
                            if (!nonpersistableFields.contains(fieldNumbers[i]))
                            {
                                persistableFieldNums[pos++] = fieldNumbers[i];
                            }
                        }
                        fieldNumbers = persistableFieldNums;
                    }
                    sm.replaceFields(fieldNumbers, new FetchFieldManager(sm, row, schemaTable));

                    if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("ODF.ExecutionTime", (System.currentTimeMillis() - startTime)));
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
                throw new NucleusObjectNotFoundException("Object not found for id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()), sm.getObject());
            }
        }
    }

    @Override
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }

    @Override
    public void locateObject(ObjectProvider sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
            final AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                ((ODFStoreManager)storeMgr).manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
            }
            
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