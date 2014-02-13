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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.NucleusContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.odf.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.incubator.doc.office.OdfOfficeAutomaticStyles;
import org.odftoolkit.odfdom.incubator.doc.style.OdfStyle;
import org.odftoolkit.odfdom.pkg.OdfFileDom;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;
import org.odftoolkit.odfdom.dom.style.OdfStyleFamily;

/**
 * Utilities to assist in persistence to ODF spreadsheets.
 */
public class ODFUtils
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.odf.Localisation", ODFStoreManager.class.getClassLoader());

    /**
     * Convenience method to find the row of an object in the provided sheet.
     * For application-identity does a search for a row with the specified PK field values.
     * For datastore-identity does a search for the row with the datastore column having the specified value
     * @param sm ObjectProvider for the object
     * @param spreadsheetDoc The spreadsheet document
     * @param originalValue Whether to use the original value (when available) when using non-durable id.
     * @return The row (or null if not found)
     */
    public static OdfTableRow getTableRowForObjectInSheet(ObjectProvider sm, OdfSpreadsheetDocument spreadsheetDoc,
            boolean originalValue)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        String sheetName = sm.getExecutionContext().getStoreManager().getNamingFactory().getTableName(cmd);
        OdfTable table = spreadsheetDoc.getTableByName(sheetName);
        if (table == null)
        {
            return null;
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] pkFieldNumbers = cmd.getPKMemberPositions();

            List<Integer> pkFieldColList = new ArrayList(pkFieldNumbers.length);
            List pkFieldValList = new ArrayList(pkFieldNumbers.length);
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                Object fieldValue = sm.provideField(pkFieldNumbers[i]);
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.newObjectProviderForEmbedded(fieldValue, false, sm, pkFieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = sm.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        pkFieldColList.add(getColumnPositionForFieldOfEmbeddedClass(j, mmd));
                        pkFieldValList.add(embOP.provideField(j));
                    }
                }
                else
                {
                    pkFieldColList.add(getColumnPositionForFieldOfClass(cmd, pkFieldNumbers[i]));
                    pkFieldValList.add(fieldValue);
                }
            }

            List<OdfTableRow> rows = table.getRowList();
            Iterator<OdfTableRow> rowIter = rows.iterator();
            while (rowIter.hasNext())
            {
                OdfTableRow row = rowIter.next();
                boolean isRow = true;

                for (int i=0;i<pkFieldColList.size();i++)
                {
                    int index = pkFieldColList.get(i);
                    Object val = pkFieldValList.get(i);
                    OdfTableCell cellNode = row.getCellByIndex(index);
                    if (!doesCellMatchValue(cellNode, val))
                    {
                        isRow = false;
                        break;
                    }
                }
                if (isRow)
                {
                    return row;
                }
            }
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            OID oid = (OID)sm.getInternalObjectId();
            Object key = oid.getKeyValue();
            int index = getColumnPositionForFieldOfClass(cmd, -1);
            List<OdfTableRow> rows = table.getRowList();
            Iterator<OdfTableRow> rowIter = rows.iterator();
            while (rowIter.hasNext())
            {
                OdfTableRow row = rowIter.next();
                OdfTableCell cell = row.getCellByIndex(index);
                if (doesCellMatchValue(cell, key))
                {
                    return row;
                }
            }
        }
        else
        {
            // Nondurable, comparing all suitable fields
            ExecutionContext ec = sm.getExecutionContext();
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] fieldNumbers = cmd.getAllMemberPositions();

            List<Integer> fieldColList = new ArrayList(fieldNumbers.length);
            List fieldValList = new ArrayList(fieldNumbers.length);
            for (int i=0;i<fieldNumbers.length;i++)
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                Object fieldValue = null;
                if (originalValue)
                {
                    Object oldValue = sm.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumbers[i]);
                    if (oldValue != null)
                    {
                        fieldValue = oldValue;
                    }
                    else
                    {
                        fieldValue = sm.provideField(fieldNumbers[i]);
                    }
                }
                else
                {
                    fieldValue = sm.provideField(fieldNumbers[i]);
                }
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.newObjectProviderForEmbedded(fieldValue, false, sm, fieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = sm.getExecutionContext().getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        fieldColList.add(getColumnPositionForFieldOfEmbeddedClass(j, mmd));
                        fieldValList.add(embOP.provideField(j));
                    }
                }
                else if (relationType == RelationType.NONE)
                {
                    fieldColList.add(getColumnPositionForFieldOfClass(cmd, fieldNumbers[i]));
                    fieldValList.add(fieldValue);
                }
            }

            List<OdfTableRow> rows = table.getRowList();
            Iterator<OdfTableRow> rowIter = rows.iterator();
            while (rowIter.hasNext())
            {
                OdfTableRow row = rowIter.next();
                boolean isRow = true;

                for (int i=0;i<fieldColList.size();i++)
                {
                    int index = fieldColList.get(i);
                    Object val = fieldValList.get(i);
                    OdfTableCell cellNode = row.getCellByIndex(index);
                    if (!doesCellMatchValue(cellNode, val))
                    {
                        isRow = false;
                        break;
                    }
                }
                if (isRow)
                {
                    return row;
                }
            }
        }

        return null;
    }

    /**
     * Convenience method to return if the supplied cell matches the passed value.
     * Compares the type and value.
     * @param cell The cell
     * @param value The value
     * @return Whether they match
     */
    private static boolean doesCellMatchValue(OdfTableCell cell, Object value)
    {
        if (cell == null)
        {
            return false;
        }

        if (value instanceof Long && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.FLOAT) && 
            cell.getDoubleValue().longValue() == ((Long)value).longValue())
        {
            return true;
        }
        else if (value instanceof Integer && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.FLOAT) &&
            cell.getDoubleValue().intValue() == ((Integer)value).intValue())
        {
            return true;
        }
        else if (value instanceof Short && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.FLOAT) &&
            cell.getDoubleValue().shortValue() == ((Short)value).shortValue())
        {
            return true;
        }
        else if (value instanceof String && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.STRING) && 
            cell.getStringValue().equals((String)value))
        {
            return true;
        }
        else if (value instanceof Date && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.DATE) && 
            cell.getDateValue().getTimeInMillis() == ((Date)value).getTime())
        {
            return true;
        }
        else if (value instanceof Date && isOfficeValueTypeConsistent(cell, OfficeValueTypeAttribute.Value.TIME) && 
            cell.getTimeValue().getTimeInMillis() == ((java.sql.Time)value).getTime())
        {
            return true;
        }
        // TODO Cater for other types
        return false;
    }

    /**
     * Convenience method to get the position where a field of a class is persisted.
     * Uses the column "position" attribute if defined, otherwise uses the column name (as an integer).
     * The field number is the absolute number (0 or higher); a value of -1 implies surrogate identity column,
     * and -2 implies surrogate version column
     * @param acmd MetaData for the class
     * @param inputFieldNumber Absolute field number that we are interested in (-1 = datastore-id, -2=version)
     */
    public static int getColumnPositionForFieldOfClass(AbstractClassMetaData acmd, int inputFieldNumber)
    {
        int fieldNumber = inputFieldNumber;
        if (inputFieldNumber == -1)
        {
            // Datastore-identity, so allocate next column after normal fields
            fieldNumber = acmd.getNoOfManagedMembers() + acmd.getNoOfInheritedManagedMembers();
        }
        else if (inputFieldNumber == -2)
        {
            // Version, so allocate next column after normal fields (and optionally datastore-identity)
            if (acmd.getIdentityType() == IdentityType.DATASTORE)
            {
                fieldNumber = acmd.getNoOfManagedMembers() + acmd.getNoOfInheritedManagedMembers() + 1;
            }
            else
            {
                fieldNumber = acmd.getNoOfManagedMembers() + acmd.getNoOfInheritedManagedMembers();
            }
        }

        if (inputFieldNumber >= 0)
        {
            // Field of the class
            AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            Integer colPos = (ammd.getColumnMetaData() == null || ammd.getColumnMetaData().length == 0 ? 
                    null : ammd.getColumnMetaData()[0].getPosition());
            if (colPos == null)
            {
                ColumnMetaData[] colmds = ammd.getColumnMetaData();
                if (colmds != null && colmds.length > 0)
                {
                    String colName = colmds[0].getName();
                    try
                    {
                        return new Integer(colName).intValue();
                    }
                    catch (NumberFormatException nfe)
                    {
                        return (int)fieldNumber;
                    }
                }
                else
                {
                    return (int)fieldNumber;
                }
            }
            else
            {
                return colPos.intValue();
            }
        }
        else if (inputFieldNumber == -1)
        {
            // Surrogate datastore identity column
            IdentityMetaData imd = acmd.getIdentityMetaData();
            if (imd != null)
            {
                Integer colPos = (imd.getColumnMetaData() == null ? null : imd.getColumnMetaData().getPosition());
                if (colPos == null)
                {
                    if (imd.getColumnMetaData() != null)
                    {
                        String colName = imd.getColumnMetaData().getName();
                        try
                        {
                            return new Integer(colName).intValue();
                        }
                        catch (NumberFormatException nfe)
                        {
                            return (int)fieldNumber;
                        }
                    }
                    else
                    {
                        return (int)fieldNumber;
                    }
                }
                else
                {
                    return colPos.intValue();
                }
            }
            return -1;
        }
        else if (inputFieldNumber == -2)
        {
            // Surrogate version column
            VersionMetaData vmd = acmd.getVersionMetaDataForClass();
            if (vmd != null)
            {
                Integer colPos = (vmd.getColumnMetaData() == null ? null : vmd.getColumnMetaData().getPosition());
                if (colPos == null)
                {
                    if (vmd.getColumnMetaData() != null)
                    {
                        String colName = vmd.getColumnMetaData().getName();
                        try
                        {
                            return new Integer(colName).intValue();
                        }
                        catch (NumberFormatException nfe)
                        {
                            return (int)fieldNumber;
                        }
                    }
                    else
                    {
                        return (int)fieldNumber;
                    }
                }
                else
                {
                    return colPos.intValue();
                }
            }
            return -1;
        }
        else
        {
            throw new NucleusException("Unsupported field number " + fieldNumber);
        }
    }

    /**
     * Return the column position for the specified field of an embedded object stored in the specified owner field.
     * Uses the column "position" attribute if defined, otherwise uses the column name (as an integer).
     * @param fieldNumber Number of the field in the embedded class.
     * @param ownerMmd the owner field
     * @return The column position
     */
    public static int getColumnPositionForFieldOfEmbeddedClass(int fieldNumber, 
            AbstractMemberMetaData ownerMmd)
    {
        if (fieldNumber >= 0)
        {
            EmbeddedMetaData emd = ownerMmd.getEmbeddedMetaData();
            AbstractMemberMetaData[] emb_mmd = emd.getMemberMetaData();
            AbstractMemberMetaData ammd = emb_mmd[fieldNumber];
            Integer colPos = (ammd.getColumnMetaData() == null || ammd.getColumnMetaData().length == 0 ?
                    null : ammd.getColumnMetaData()[0].getPosition());
            if (colPos == null)
            {
                ColumnMetaData[] colmds = ammd.getColumnMetaData();
                if (colmds != null && colmds.length > 0)
                {
                    String colName = colmds[0].getName();
                    try
                    {
                        return Integer.valueOf(colName).intValue();
                    }
                    catch (NumberFormatException nfe)
                    {
                        return (int)fieldNumber;
                    }
                }
                else
                {
                    return (int)fieldNumber;
                }
            }
            else
            {
                return colPos;
            }
        }
        else
        {
            throw new NucleusException("Unsupported field number " + fieldNumber + " of owner "+ ownerMmd.getFullFieldName());
        }
    }

    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses) from the 
     * specified workbook connection.
     * @param ec execution context
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @return List of objects of the candidate type
     */
    public static List getObjectsOfCandidateType(ExecutionContext ec, ManagedConnection mconn, 
            Class candidateClass, boolean subclasses, boolean ignoreCache)
    {
        OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
        List results = null;
        try
        {
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            final AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
            results = getObjectsOfCandidateType(ec, spreadsheetDoc, acmd, ignoreCache);
            if (subclasses)
            {
                // Add on any subclass objects
                String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(candidateClass.getName(), true);
                if (subclassNames != null)
                {
                    for (int i=0;i<subclassNames.length;i++)
                    {
                        AbstractClassMetaData cmd =
                            ec.getMetaDataManager().getMetaDataForClass(subclassNames[i], clr);
                        results.addAll(getObjectsOfCandidateType(ec, spreadsheetDoc, cmd, ignoreCache));
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Error extracting results from ODF document", e);
        }
        return results;
    }

    /**
     * Method to extract the actual objects of a particular class from the Excel spreadsheet.
     * @param ec execution context
     * @param spreadsheetDoc Spreadsheet
     * @param acmd MetaData for the class
     * @param ignoreCache Whether to ignore the cache
     * @return List of objects (connected to ObjectProviders as required)
     */
    private static List getObjectsOfCandidateType(ExecutionContext ec, OdfSpreadsheetDocument spreadsheetDoc,
            final AbstractClassMetaData acmd, boolean ignoreCache)
    {
        List results = new ArrayList();

        String sheetName = ec.getStoreManager().getNamingFactory().getTableName(acmd);
        final OdfTable table = spreadsheetDoc.getTableByName(sheetName);
        if (table != null)
        {
            List<OdfTableRow> rows = table.getRowList();
            Iterator<OdfTableRow> rowIter = rows.iterator();
            while (rowIter.hasNext())
            {
                final OdfTableRow row = rowIter.next();
                final FetchFieldManager fm = new FetchFieldManager(ec, acmd, row);
                OdfStyle style = row.getDefaultCellStyle();
                String styleName = (style != null ? style.getStyleNameAttribute() : null);
                if (styleName != null && styleName.equals("DN_Headers"))
                {
                    // Skip headers
                    continue;
                }

                if (acmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, null, false, 
                        new FetchFieldManager(ec, acmd, row));
                    results.add(ec.findObject(id, new FieldValues()
                    {
                        // ObjectProvider calls the fetchFields method
                        public void fetchFields(ObjectProvider op)
                        {
                            op.replaceFields(acmd.getAllMemberPositions(), fm);
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                            op.replaceNonLoadedFields(acmd.getAllMemberPositions(), fm);
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                    }, null, ignoreCache, false));
                }
                else if (acmd.getIdentityType() == IdentityType.DATASTORE)
                {
                    int idIndex = ODFUtils.getColumnPositionForFieldOfClass(acmd, -1);
                    OdfTableCell idCell = row.getCellByIndex(idIndex);
                    Object idKey = null;
                    if (isOfficeValueTypeConsistent(idCell, OfficeValueTypeAttribute.Value.STRING))
                    {
                        idKey = idCell.getStringValue();
                    }
                    else
                    {
                        idKey = new Long(idCell.getDoubleValue().longValue());
                    }
                    OID oid = OIDFactory.getInstance(ec.getNucleusContext(), acmd.getFullClassName(), idKey);
                    results.add(ec.findObject(oid, new FieldValues()
                    {
                        // ObjectProvider calls the fetchFields method
                        public void fetchFields(ObjectProvider op)
                        {
                            op.replaceFields(acmd.getAllMemberPositions(), fm);
                        }
                        public void fetchNonLoadedFields(ObjectProvider op)
                        {
                            op.replaceNonLoadedFields(acmd.getAllMemberPositions(), fm);
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                    }, null, ignoreCache, false));
                }
                else
                {
                    Object id = new SCOID(acmd.getFullClassName());
                    results.add(ec.findObject(id, new FieldValues()
                    {
                        public void fetchFields(ObjectProvider sm)
                        {
                            sm.replaceFields(acmd.getAllMemberPositions(), new FetchFieldManager(sm, row));
                        }
                        public void fetchNonLoadedFields(ObjectProvider sm)
                        {
                            sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), new FetchFieldManager(sm, row));
                        }
                        public FetchPlan getFetchPlanForLoading()
                        {
                            return null;
                        }
                    }, null, ignoreCache, false));
                }
            }
        }
        return results;
    }

    public static boolean isOfficeValueTypeConsistent(OdfTableCell cell, OfficeValueTypeAttribute.Value type)
    {
        String cellTypeStr = cell.getValueType();
        String typeStr = type.toString();
        if (cellTypeStr != null && typeStr != null)
        {
            return cellTypeStr.equals(typeStr);
        }
        return false;
    }

    /**
     * Convenience method to add a table (worksheet) to represent a class.
     * @param doc The spreadsheet doc
     * @param cmd Metadata for the class
     * @param sheetName Name of the sheet
     * @param storeMgr StoreManager being used
     * @return The table
     */
    public static OdfTable addTableForClass(OdfSpreadsheetDocument doc, AbstractClassMetaData cmd, String sheetName, 
            StoreManager storeMgr)
    {
        OdfFileDom contentDoc;
        try
        {
            contentDoc = doc.getContentDom();
        }
        catch (Exception e)
        {
            throw new NucleusDataStoreException("Exception thrown adding worksheet " + sheetName, e);
        }
        OdfOfficeAutomaticStyles styles = contentDoc.getAutomaticStyles();
        OdfStyle headerStyle = styles.getStyle("DN_Headers", OdfStyleFamily.TableCell);

        Map<Integer, String> colNameByPosition = new HashMap<Integer, String>();
        getColumnInformationForClass(colNameByPosition, cmd, storeMgr);
        int numCols = colNameByPosition.size();

        OdfTable table = OdfTable.newTable(doc, 1, numCols);
        table.setTableName(sheetName);

        // Set the header row if required TODO Make this optional when ODFDOM allows tables with no rows/columns
        if (true)
        {
            OdfTableRow headerRow = table.getRowByIndex(0);
            headerRow.setDefaultCellStyle(headerStyle);
            Iterator<Map.Entry<Integer, String>> colIter = colNameByPosition.entrySet().iterator();
            while (colIter.hasNext())
            {
                Map.Entry<Integer, String> entry = colIter.next();
                int position = entry.getKey();
                if (position >= numCols)
                {
                    throw new NucleusUserException("Error in specification of column positions." +
                        " Class " + cmd.getFullClassName() + " has " + numCols + " in total" +
                        " yet there is a column defined at position=" + position);
                }
                String colName = entry.getValue();
                OdfTableCell cell = headerRow.getCellByIndex(position);
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(colName);
            }
        }

        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("ODF.Insert.SheetCreated", sheetName));
        }

        return table;
    }

    protected static void getColumnInformationForClass(Map<Integer, String> colNameByPosition, 
            AbstractClassMetaData cmd, StoreManager storeMgr)
    {
        NucleusContext nucCtx = storeMgr.getNucleusContext();
        MetaDataManager mmgr = nucCtx.getMetaDataManager();
        ClassLoaderResolver clr = nucCtx.getClassLoaderResolver(null);
        int numFields = cmd.getAllMemberPositions().length;
        for (int i=0;i<numFields;i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(i);
            RelationType relationType = mmd.getRelationType(clr);
            if (RelationType.isRelationSingleValued(relationType) && MetaDataUtils.getInstance().isMemberEmbedded(mmgr, clr, mmd, relationType, null))
            {
                AbstractClassMetaData relCmd = mmgr.getMetaDataForClass(mmd.getType(), clr);
                getColumnInformationForEmbeddedClass(colNameByPosition, relCmd, mmd, clr, mmgr, storeMgr);
            }
            else
            {
                int index = ODFUtils.getColumnPositionForFieldOfClass(cmd, i);
                String name = storeMgr.getNamingFactory().getColumnName(mmd, ColumnType.COLUMN);
                String oldName = colNameByPosition.put(index, name);
                if (oldName != null)
                {
                    throw new NucleusUserException("Error assigning positions of columns in ODF." +
                        " Position=" + index + " was previously assigned to column with name=" + oldName +
                        " but now is for name=" + name +
                        " Check your metadata specification of positions");
                }
            }
        }
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            int index = ODFUtils.getColumnPositionForFieldOfClass(cmd, -1);
            String name = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.DATASTOREID_COLUMN);
            String oldName = colNameByPosition.put(index, name);
            if (oldName != null)
            {
                throw new NucleusUserException("Error assigning positions of columns in ODF." +
                    " Position=" + index + " was previously assigned to column with name=" + oldName +
                    " but now is for name=" + name +
                    " Check your metadata specification of positions");
            }
        }
        if (cmd.isVersioned())
        {
            int index = ODFUtils.getColumnPositionForFieldOfClass(cmd, -2);
            String name = storeMgr.getNamingFactory().getColumnName(cmd, ColumnType.VERSION_COLUMN);
            String oldName = colNameByPosition.put(index, name);
            if (oldName != null)
            {
                throw new NucleusUserException("Error assigning positions of columns in ODF." +
                    " Position=" + index + " was previously assigned to column with name=" + oldName +
                    " but now is for name=" + name +
                    " Check your metadata specification of positions");
            }
        }
    }

    protected static void getColumnInformationForEmbeddedClass(Map<Integer, String> colNameByPosition, 
            AbstractClassMetaData cmd, AbstractMemberMetaData ownerMmd, ClassLoaderResolver clr, MetaDataManager mmgr,
            StoreManager storeMgr)
    {
        EmbeddedMetaData emd = ownerMmd.getEmbeddedMetaData();
        AbstractMemberMetaData[] emb_mmd = emd.getMemberMetaData();
        for (int i=0;i<emb_mmd.length;i++)
        {
            AbstractMemberMetaData mmd = cmd.getMetaDataForMember(emb_mmd[i].getName());
            if (emb_mmd[i].getEmbeddedMetaData() == null)
            {
                int index = ODFUtils.getColumnPositionForFieldOfEmbeddedClass(mmd.getAbsoluteFieldNumber(), ownerMmd);
                String name = storeMgr.getNamingFactory().getColumnName(emb_mmd[i], ColumnType.COLUMN);
                String oldName = colNameByPosition.put(index, name);
                if (oldName != null)
                {
                    throw new NucleusUserException("Error assigning positions of columns in ODF." +
                        " Position=" + index + " was previously assigned to column with name=" + oldName +
                        " but now is for name=" + name +
                        " Check your metadata specification of positions");
                }
            }
            else
            {
                RelationType relationType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType)) // TODO Use MetaDataUtils.isEmbedded(...)?
                {
                    AbstractClassMetaData relCmd = mmgr.getMetaDataForClass(emb_mmd[i].getType(), clr);
                    getColumnInformationForEmbeddedClass(colNameByPosition, relCmd, emb_mmd[i], clr, mmgr, storeMgr);
                }
            }
        }
    }
}