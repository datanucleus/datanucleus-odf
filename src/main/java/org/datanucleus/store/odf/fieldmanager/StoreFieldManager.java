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
package org.datanucleus.store.odf.fieldmanager;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * FieldManager to handle the insert/update of information into an ODF spreadsheet row using an object.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    protected final Table table;

    protected final OdfTableRow row;

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, OdfTableRow row, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.row = row;
        this.table = table;
    }

    public StoreFieldManager(ObjectProvider op, OdfTableRow row, boolean insert, Table table)
    {
        super(op, insert);
        this.table = table;
        this.row = row;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.BOOLEAN.toString());
        cell.setBooleanValue(value);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(Double.valueOf(value));
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
        cell.setStringValue("" + value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(Double.valueOf(value));
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(Double.valueOf(value));
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(Double.valueOf(value));
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(Double.valueOf(value));
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
        cell.setStringValue(value);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        // Special cases
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                    {
                        // Related PC object not persistent, but cant do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }
                }

                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                int[] embMmdPosns = embCmd.getAllMemberPositions();
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                if (value == null)
                {
                    // Store null in all columns for the embedded (and nested embedded) object(s)
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, row, insert, embMmds, table);
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Store a null for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            for (int j=0;j<mapping.getNumberOfColumns();j++)
                            {
                                // TODO Put null in this column
                            }
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(embOP, row, insert, embMmds, table);
                embOP.provideFields(embMmdPosns, storeEmbFM);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with ODF");
            }
        }

        storeObjectFieldInternal(fieldNumber, value, mmd, clr, relationType);
    }

    protected void setNullInCell(OdfTableCell cell, Class type)
    {
        if (Number.class.isAssignableFrom(type))
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(0.0); // No other way of saying null ?
        }
        else if (java.sql.Time.class.isAssignableFrom(type))
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.TIME.toString());
            // TODO How to set empty cell?
            cell.setTimeValue(getCalendarForTime(null)); // TODO This is dumped in to avoid NPEs in ODFDOM 0.8.7
        }
        else if (Date.class.isAssignableFrom(type) || Calendar.class.isAssignableFrom(type))
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
            // TODO How to set empty cell?
        }
        else if (String.class.isAssignableFrom(type))
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue(null);
        }
        else
        {
            // Assume it's a String type TODO Set the type based on the field type
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
        }
    }

    protected void storeObjectFieldInternal(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    {
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        Class type = mmd.getType();
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }

            type = clr.classForName(mmd.getCollection().getElementType());
            if (value != null)
            {
                Optional opt = (Optional)value;
                if (opt.isPresent())
                {
                    value = opt.get();
                }
                else
                {
                    value = null;
                }
            }
        }

        if (relationType == RelationType.NONE)
        {
            if (value == null)
            {
                if (mapping.getNumberOfColumns() > 1)
                {
                    Class[] colTypes = ((MultiColumnConverter)mapping.getTypeConverter()).getDatastoreColumnTypes();
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        Column col = mapping.getColumn(i);
                        OdfTableCell theCell = row.getCellByIndex(col.getPosition());
                        setNullInCell(theCell, colTypes[i]);
                    }
                }
                else
                {
                    OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                    setNullInCell(cell, type);
                }
                return;
            }

            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                Class datastoreType = ec.getTypeManager().getDatastoreTypeForTypeConverter(mapping.getTypeConverter(), mmd.getType());
                if (mapping.getNumberOfColumns() > 1)
                {
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        // Set each component cell
                        OdfTableCell cell = row.getCellByIndex(mapping.getColumn(i).getPosition());
                        Object colValue = Array.get(datastoreValue, i);
                        storeValueInCell(mapping, i, cell, colValue);
                    }
                }
                else
                {
                    OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                    if (datastoreType == String.class)
                    {
                        cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                        cell.setStringValue((String)datastoreValue);
                        return;
                    }
                    else if (Number.class.isAssignableFrom(datastoreType))
                    {
                        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                        cell.setDoubleValue(Double.valueOf((Double)datastoreValue));
                        return;
                    }
                    else if (Boolean.class.isAssignableFrom(datastoreType))
                    {
                        cell.setValueType(OfficeValueTypeAttribute.Value.BOOLEAN.toString());
                        cell.setBooleanValue(Boolean.valueOf((Boolean)datastoreValue));
                        return;
                    }
                    else if (java.sql.Time.class.isAssignableFrom(datastoreType))
                    {
                        cell.setValueType(OfficeValueTypeAttribute.Value.TIME.toString());
                        cell.setTimeValue(getCalendarForTime((java.sql.Time)datastoreValue));
                        return;
                    }
                    else if (Date.class.isAssignableFrom(datastoreType))
                    {
                        cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
                        Calendar cal = Calendar.getInstance();
                        cal.setTime((Date)datastoreValue);
                        cell.setDateValue(cal);
                        return;
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_PERSIST.warn("TypeConverter for member " + mmd.getFullFieldName() + " converts to " + datastoreType.getName() + " - not yet supported");
                    }
                }
            }
            else
            {
                OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                storeValueInCell(mapping, 0, cell, value);
            }
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
            {
                if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                {
                    // Related PC object not persistent, but cant do cascade-persist so throw exception
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }
                    throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                }
            }

            // Persistable object - persist the related object and store the identity in the cell
            OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
            if (value == null)
            {
                setNullInCell(cell, type);
                return;
            }

            Object valuePC = op.getExecutionContext().persistObjectInternal(value, op, fieldNumber, -1);
            Object valueId = op.getExecutionContext().getApiAdapter().getIdForObject(valuePC);
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue("[" + IdentityUtils.getPersistableIdentityForId(valueId) + "]");
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
            if (value == null)
            {
                setNullInCell(cell, mmd.getType());
                return;
            }

            if (mmd.hasCollection())
            {
                Collection coll = (Collection) value;
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    // Field doesnt support cascade-persist so no reachability
                    if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                    {
                        NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                    }

                    // Check for any persistable elements that aren't persistent
                    for (Object element : coll)
                    {
                        if (!ec.getApiAdapter().isDetached(element) && !ec.getApiAdapter().isPersistent(element))
                        {
                            // Element is not persistent so throw exception
                            throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), element);
                        }
                    }
                }

                StringBuilder cellValue = new StringBuilder("[");
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(elementID));
                    if (collIter.hasNext())
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(cellValue.toString());
                return;
            }
            else if (mmd.hasMap())
            {
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);

                StringBuilder cellValue = new StringBuilder("[");
                Map map = (Map)value;
                Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                while (mapIter.hasNext())
                {
                    Map.Entry entry = mapIter.next();
                    cellValue.append("[");
                    if (keyCmd != null)
                    {
                        Object keyPC = op.getExecutionContext().persistObjectInternal(entry.getKey(), op, fieldNumber, -1);
                        Object keyID = op.getExecutionContext().getApiAdapter().getIdForObject(keyPC);
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(keyID));
                    }
                    else
                    {
                        cellValue.append(entry.getKey());
                    }
                    cellValue.append("],[");
                    if (valCmd != null)
                    {
                        Object valPC = op.getExecutionContext().persistObjectInternal(entry.getValue(), op, fieldNumber, -1);
                        Object valID = op.getExecutionContext().getApiAdapter().getIdForObject(valPC);
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(valID));
                    }
                    else
                    {
                        cellValue.append(entry.getValue());
                    }
                    cellValue.append("]");
                    if (mapIter.hasNext())
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(cellValue.toString());
                return;
            }
            else if (mmd.hasArray())
            {
                StringBuilder cellValue = new StringBuilder("[");
                for (int i=0;i<Array.getLength(value);i++)
                {
                    Object element = Array.get(value, i);
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(elementID));
                    if (i < (Array.getLength(value)-1))
                    {
                        cellValue.append(",");
                    }
                }
                cellValue.append("]");
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(cellValue.toString());
                return;
            }
        }
    }

    protected void storeValueInCell(MemberColumnMapping mapping, int pos, OdfTableCell cell, Object value)
    {
        Column col = mapping.getColumn(pos);
        AbstractMemberMetaData mmd = mapping.getMemberMetaData();
        if (value instanceof java.sql.Time)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.TIME.toString());
            cell.setTimeValue(getCalendarForTime((java.sql.Time)value));
            return;
        }
        else if (value instanceof Calendar)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
            cell.setDateValue((Calendar)value);
            return;
        }
        else if (value instanceof Date)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date)value);
            cell.setDateValue(cal);
            return;
        }
        else if (value instanceof Boolean)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.BOOLEAN.toString());
            cell.setBooleanValue((Boolean)value);
            return;
        }
        else if (value instanceof Byte)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(Double.valueOf((Byte)value));
            return;
        }
        else if (value instanceof String)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue("" + value);
            return;
        }
        else if (value instanceof Character)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue("" + value);
            return;
        }
        else if (value instanceof Double)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue((Double)value);
            return;
        }
        else if (value instanceof Float)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(Double.valueOf((Float)value));
            return;
        }
        else if (value instanceof Integer)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(Double.valueOf((Integer)value));
            return;
        }
        else if (value instanceof Long)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(Double.valueOf((Long)value));
            return;
        }
        else if (value instanceof Short)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
            cell.setDoubleValue(Double.valueOf((Short)value));
            return;
        }
        else if (value instanceof Currency)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.CURRENCY.toString());
            TypeConverter conv = 
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            cell.setStringValue((String)conv.toDatastoreType(value));
            return;
        }
        else if (value instanceof Enum)
        {
            Object datastoreValue = EnumConversionHelper.getStoredValueFromEnum(mmd, FieldRole.ROLE_FIELD, (Enum) value);
            if (datastoreValue instanceof Number)
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                cell.setDoubleValue(((Number)datastoreValue).doubleValue());
            }
            else
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue((String)datastoreValue);
            }
            return;
        }
        else if (value.getClass() == byte[].class)
        {
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue(Base64.getEncoder().encodeToString((byte[])value));
            return;
        }
        else
        {
            // See if we can persist it as a Long/String using built-in converters
            boolean useLong = MetaDataUtils.isJdbcTypeNumeric(col.getJdbcType());

            TypeConverter longConv = op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
            if (useLong)
            {
                if (longConv != null)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(Double.valueOf((Long)longConv.toDatastoreType(value)));
                    return;
                }
            }
            else
            {
                TypeConverter strConv = op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                if (strConv != null)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                    cell.setStringValue((String)strConv.toDatastoreType(value));
                    return;
                }
                else if (longConv != null)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(Double.valueOf((Long)longConv.toDatastoreType(value)));
                    return;
                }
            }

            NucleusLogger.PERSISTENCE.warn("Dont currently support persistence of field=" + mmd.getFullFieldName() +
                " type=" + value.getClass().getName() + " to ODF");
        }
    }

    protected static Calendar getCalendarForTime(Date date)
    {
        Calendar cal = Calendar.getInstance();
        if (date != null)
        {
            // Base the Time on the provided value
            cal.setTimeInMillis(date.getTime());
        }
        else
        {
            // Set the Time to 00:00:00
            cal.set(Calendar.HOUR, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
        }
        cal.set(Calendar.DAY_OF_MONTH, 0);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.YEAR, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal;
    }
}