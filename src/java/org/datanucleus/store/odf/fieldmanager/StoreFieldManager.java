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
import java.util.Calendar;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.odf.ODFUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.types.converters.TypeConverterHelper;
import org.datanucleus.util.Base64;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * FieldManager to handle the insert/update of information into an ODF spreadsheet row using an object.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    /** Row being inserted/updated. */
    protected final OdfTableRow row;

    public StoreFieldManager(ObjectProvider op, OdfTableRow row, boolean insert)
    {
        super(op, insert);
        this.row = row;
    }

    protected int getColumnIndexForMember(int memberNumber)
    {
        return ODFUtils.getColumnPositionForFieldOfClass(op.getClassMetaData(), memberNumber);
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.BOOLEAN.toString());
        cell.setBooleanValue(value);
    }

    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(new Double(value));
    }

    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
        cell.setStringValue("" + value);
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(value);
    }

    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(new Double(value));
    }

    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(new Double(value));
    }

    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(new Double(value));
    }

    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
        cell.setDoubleValue(new Double(value));
    }

    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
        cell.setStringValue(value);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd =
            op.getClassMetaData().getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        // Special cases
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE)
        {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // Persistable object embedded into this table
                    Class embcls = mmd.getType();
                    AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
                    if (embcmd != null) 
                    {
                        ObjectProvider embSM = null;
                        if (value != null)
                        {
                            embSM = ec.findObjectProviderForEmbedded(value, op, mmd);
                        }
                        else
                        {
                            embSM = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                        }

                        embSM.provideFields(embcmd.getAllMemberPositions(), new StoreEmbeddedFieldManager(embSM, row, mmd, insert));
                        return;
                    }
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with ODF");
                }
            }
        }

        storeObjectFieldInCell(fieldNumber, value, mmd, clr);
    }

    protected void storeObjectFieldInCell(int fieldNumber, Object value, AbstractMemberMetaData mmd, ClassLoaderResolver clr)
    {
        RelationType relationType = mmd.getRelationType(clr);
        int colIndex = getColumnIndexForMember(fieldNumber);
        OdfTableCell cell = row.getCellByIndex(colIndex);
        if (value == null)
        {
            if (Number.class.isAssignableFrom(mmd.getType()))
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                cell.setDoubleValue(0.0); // No other way of saying null ?
            }
            else if (java.sql.Time.class.isAssignableFrom(mmd.getType()))
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.TIME.toString());
                // TODO How to set empty cell?
                cell.setTimeValue(getCalendarForTime(null)); // TODO This is dumped in to avoid NPEs in ODFDOM 0.8.7
            }
            else if (Date.class.isAssignableFrom(mmd.getType()) || Calendar.class.isAssignableFrom(mmd.getType()))
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
                // TODO How to set empty cell?
            }
            else if (String.class.isAssignableFrom(mmd.getType()))
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(null);
            }
            else
            {
                // Assume it's a String type TODO Set the type based on the field type
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            }
            return;
        }
        else if (relationType == RelationType.NONE)
        {
            if (mmd.getTypeConverterName() != null)
            {
                // User-defined type converter
                TypeConverter conv =
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                Class datastoreType = TypeConverterHelper.getDatastoreTypeForTypeConverter(conv, mmd.getType());
                if (datastoreType == String.class)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                    cell.setStringValue((String)conv.toDatastoreType(value));
                    return;
                }
                else if (Number.class.isAssignableFrom(datastoreType))
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue(Double.valueOf((Double)conv.toDatastoreType(value)));
                    return;
                }
                else if (Boolean.class.isAssignableFrom(datastoreType))
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.BOOLEAN.toString());
                    cell.setBooleanValue(Boolean.valueOf((Boolean)conv.toDatastoreType(value)));
                    return;
                }
                else if (java.sql.Time.class.isAssignableFrom(datastoreType))
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.TIME.toString());
                    cell.setTimeValue(getCalendarForTime((java.sql.Time)conv.toDatastoreType(value)));
                    return;
                }
                else if (Date.class.isAssignableFrom(datastoreType))
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.DATE.toString());
                    Calendar cal = Calendar.getInstance();
                    cal.setTime((Date)conv.toDatastoreType(value));
                    cell.setDateValue(cal);
                    return;
                }
            }
            else if (value instanceof java.sql.Time)
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
                cell.setDoubleValue(new Double((Byte)value));
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
                cell.setDoubleValue(new Double((Float)value));
                return;
            }
            else if (value instanceof Integer)
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                cell.setDoubleValue(new Double((Integer)value));
                return;
            }
            else if (value instanceof Long)
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                cell.setDoubleValue(new Double((Long)value));
                return;
            }
            else if (value instanceof Short)
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                cell.setDoubleValue(new Double((Short)value));
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
                ColumnMetaData colmd = null;
                if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
                {
                    colmd = mmd.getColumnMetaData()[0];
                }
                boolean useNumeric = MetaDataUtils.persistColumnAsNumeric(colmd);
                if (useNumeric)
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.FLOAT.toString());
                    cell.setDoubleValue((double) ((Enum)value).ordinal());
                }
                else
                {
                    cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                    cell.setStringValue(((Enum)value).name());
                }
                return;
            }
            else if (value.getClass() == byte[].class)
            {
                cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
                cell.setStringValue(new String(Base64.encode((byte[])value)));
                return;
            }
            else
            {
                // See if we can persist it as a Long/String using built-in converters
                boolean useLong = false;
                ColumnMetaData[] colmds = mmd.getColumnMetaData();
                if (colmds != null && colmds.length == 1)
                {
                    String jdbc = colmds[0].getJdbcType();
                    if (jdbc != null && (jdbc.equalsIgnoreCase("int") || jdbc.equalsIgnoreCase("integer")))
                    {
                        useLong = true;
                    }
                }

                TypeConverter strConv = 
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
                TypeConverter longConv = 
                    op.getExecutionContext().getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
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
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object - persist the related object and store the identity in the cell
            Object valuePC = op.getExecutionContext().persistObjectInternal(value, op, fieldNumber, -1);
            Object valueId = op.getExecutionContext().getApiAdapter().getIdForObject(valuePC);
            cell.setValueType(OfficeValueTypeAttribute.Value.STRING.toString());
            cell.setStringValue("[" + IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valueId) + "]");
            return;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            if (mmd.hasCollection())
            {
                StringBuffer cellValue = new StringBuffer("[");
                Collection coll = (Collection)value;
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    Object element = collIter.next();
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
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
                AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, op.getExecutionContext().getMetaDataManager());
                AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, op.getExecutionContext().getMetaDataManager());

                StringBuffer cellValue = new StringBuffer("[");
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
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), keyID));
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
                        cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), valID));
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
                StringBuffer cellValue = new StringBuffer("[");
                for (int i=0;i<Array.getLength(value);i++)
                {
                    Object element = Array.get(value, i);
                    Object elementPC = op.getExecutionContext().persistObjectInternal(element, op, fieldNumber, -1);
                    Object elementID = op.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                    cellValue.append(IdentityUtils.getPersistableIdentityForId(ec.getApiAdapter(), elementID));
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