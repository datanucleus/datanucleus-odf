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
import java.util.Iterator;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.odf.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.table.Table;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.incubator.doc.style.OdfStyle;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * Utilities to assist in persistence to ODF spreadsheets.
 */
public class ODFUtils
{
    /**
     * Convenience method to find the row of an object in the provided sheet.
     * For application-identity does a search for a row with the specified PK field values.
     * For datastore-identity does a search for the row with the datastore column having the specified value
     * @param op ObjectProvider for the object
     * @param spreadsheetDoc The spreadsheet document
     * @param originalValue Whether to use the original value (when available) when using non-durable id.
     * @return The row (or null if not found)
     */
    public static OdfTableRow getTableRowForObjectInSheet(ObjectProvider op, OdfSpreadsheetDocument spreadsheetDoc, boolean originalValue)
    {
        ExecutionContext ec = op.getExecutionContext();
        final AbstractClassMetaData cmd = op.getClassMetaData();
        Table schemaTable = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
        String sheetName = schemaTable.getIdentifier();
        OdfTable table = spreadsheetDoc.getTableByName(sheetName);
        if (table == null)
        {
            return null;
        }

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            ClassLoaderResolver clr = ec.getClassLoaderResolver();
            int[] pkFieldNumbers = cmd.getPKMemberPositions();

            List<Integer> pkFieldColList = new ArrayList(pkFieldNumbers.length);
            List pkFieldValList = new ArrayList(pkFieldNumbers.length);
            for (int i=0;i<pkFieldNumbers.length;i++)
            {
                Object fieldValue = op.provideField(pkFieldNumbers[i]);
                AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                RelationType relationType = mmd.getRelationType(clr);
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, fieldValue, false, op, pkFieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        List<AbstractMemberMetaData> embMmds = new ArrayList();
                        embMmds.add(mmd);
                        embMmds.add(embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j));
                        pkFieldColList.add(schemaTable.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0).getPosition());
                        pkFieldValList.add(embOP.provideField(j));
                    }
                }
                else
                {
                    pkFieldColList.add(schemaTable.getMemberColumnMappingForMember(mmd).getColumn(0).getPosition());
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
            Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(op.getInternalObjectId());
            int index = schemaTable.getDatastoreIdColumn().getPosition();
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
                    Object oldValue = op.getAssociatedValue(ObjectProvider.ORIGINAL_FIELD_VALUE_KEY_PREFIX + fieldNumbers[i]);
                    if (oldValue != null)
                    {
                        fieldValue = oldValue;
                    }
                    else
                    {
                        fieldValue = op.provideField(fieldNumbers[i]);
                    }
                }
                else
                {
                    fieldValue = op.provideField(fieldNumbers[i]);
                }
                if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
                {
                    // Embedded PC is part of PK (e.g JPA EmbeddedId)
                    ObjectProvider embOP = ec.findObjectProvider(fieldValue);
                    if (embOP == null)
                    {
                        embOP = ec.getNucleusContext().getObjectProviderFactory().newForEmbedded(ec, fieldValue, false, op, fieldNumbers[i]);
                    }
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    for (int j=0;j<embCmd.getNoOfManagedMembers();j++)
                    {
                        List<AbstractMemberMetaData> embMmds = new ArrayList();
                        embMmds.add(mmd);
                        embMmds.add(embCmd.getMetaDataForManagedMemberAtAbsolutePosition(j));
                        fieldColList.add(schemaTable.getMemberColumnMappingForEmbeddedMember(embMmds).getColumn(0).getPosition());
                        fieldValList.add(embOP.provideField(j));
                    }
                }
                else if (relationType == RelationType.NONE)
                {
                    fieldColList.add(schemaTable.getMemberColumnMappingForMember(mmd).getColumn(0).getPosition());
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
    private static List getObjectsOfCandidateType(ExecutionContext ec, OdfSpreadsheetDocument spreadsheetDoc, final AbstractClassMetaData acmd, boolean ignoreCache)
    {
        List results = new ArrayList();

        final Table schemaTable = ec.getStoreManager().getStoreDataForClass(acmd.getFullClassName()).getTable();
        String sheetName = schemaTable.getIdentifier();
        final OdfTable table = spreadsheetDoc.getTableByName(sheetName);
        if (table != null)
        {
            List<OdfTableRow> rows = table.getRowList();
            Iterator<OdfTableRow> rowIter = rows.iterator();
            while (rowIter.hasNext())
            {
                final OdfTableRow row = rowIter.next();
                final FetchFieldManager fm = new FetchFieldManager(ec, acmd, row, schemaTable);
                OdfStyle style = row.getDefaultCellStyle();
                String styleName = (style != null ? style.getStyleNameAttribute() : null);
                if (styleName != null && styleName.equals("DN_Headers"))
                {
                    // Skip headers
                    continue;
                }

                if (acmd.getIdentityType() == IdentityType.APPLICATION)
                {
                    Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, null, false, // TODO Use "fm" from above
                        new FetchFieldManager(ec, acmd, row, schemaTable));
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
                    int idIndex = schemaTable.getDatastoreIdColumn().getPosition();
                    OdfTableCell idCell = row.getCellByIndex(idIndex);
                    Object idKey = null;
                    if (isOfficeValueTypeConsistent(idCell, OfficeValueTypeAttribute.Value.STRING))
                    {
                        idKey = idCell.getStringValue();
                    }
                    else
                    {
                        idKey = Long.valueOf(idCell.getDoubleValue().longValue());
                    }
                    Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(acmd.getFullClassName(), idKey);
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
                else
                {
                    Object id = new SCOID(acmd.getFullClassName());
                    results.add(ec.findObject(id, new FieldValues()
                    {
                        public void fetchFields(ObjectProvider sm)
                        {
                            sm.replaceFields(acmd.getAllMemberPositions(), new FetchFieldManager(sm, row, schemaTable));
                        }
                        public void fetchNonLoadedFields(ObjectProvider sm)
                        {
                            sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), new FetchFieldManager(sm, row, schemaTable));
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
}