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
package org.datanucleus.store.odf.fieldmanager;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Currency;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.util.Base64;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * FieldManager for the fetch of fields from ODF.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected final Table table;
    protected final OdfTableRow row;

    public FetchFieldManager(ObjectProvider op, OdfTableRow row, Table table)
    {
        super(op);
        this.table = table;
        this.row = row;
    }

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, OdfTableRow row, Table table)
    {
        super(ec, cmd);
        this.table = table;
        this.row = row;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        return cell.getBooleanValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0;
        }
        return cell.getDoubleValue().byteValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        return cell.getStringValue().charAt(0);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0.0;
        }
        return val.doubleValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0.0f;
        }
        return val.floatValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0;
        }
        return val.intValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0;
        }
        return val.longValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        Double val = cell.getDoubleValue();
        if (val == null)
        {
            return 0;
        }
        return val.shortValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        OdfTableCell cell = row.getCellByIndex(getColumnMapping(fieldNumber).getColumn(0).getPosition());
        return cell.getStringValue();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        RelationType relationType = mmd.getRelationType(clr);

        // Special cases
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // TODO Null detection
                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                ObjectProvider embOP = ec.newObjectProviderForEmbedded(embCmd, op, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embOP, row, embMmds, table);
                embOP.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embOP.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                throw new NucleusUserException("Dont support embedded multi-valued field at " + mmd.getFullFieldName() + " with ODF");
            }
        }

        return fetchObjectFieldInternal(fieldNumber, mmd, clr, relationType);
    }

    protected Object fetchObjectFieldInternal(int fieldNumber, AbstractMemberMetaData mmd, ClassLoaderResolver clr, RelationType relationType)
    {
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        if (relationType == RelationType.NONE)
        {
            Object value = null;
            if (mapping.getTypeConverter() != null)
            {
                TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
                if (mapping.getNumberOfColumns() > 1)
                {
                    // TODO Cater for int array etc
                    Object[] values = new Object[mapping.getNumberOfColumns()];
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        OdfTableCell cell = row.getCellByIndex(mapping.getColumn(i).getPosition());
                        String cellValueType = cell.getValueType();
                        // TODO Cater for other types (in the datastore we only have these types, but they may need updating as per getMemberValueFromCell
                        if (cellValueType.equals(OfficeValueTypeAttribute.Value.BOOLEAN.toString()))
                        {
                            values[i] = conv.toMemberType(cell.getBooleanValue());
                        }
                        else if (cellValueType.equals(OfficeValueTypeAttribute.Value.STRING.toString()))
                        {
                            values[i] = conv.toMemberType(cell.getStringValue());
                        }
                        else if (cellValueType.equals(OfficeValueTypeAttribute.Value.FLOAT.toString()))
                        {
                            values[i] = conv.toMemberType(cell.getDoubleValue());
                        }
                        else if (cellValueType.equals(OfficeValueTypeAttribute.Value.DATE.toString()))
                        {
                            values[i] = conv.toMemberType(cell.getDateValue());
                        }
                        else if (cellValueType.equals(OfficeValueTypeAttribute.Value.TIME.toString()))
                        {
                            values[i] = conv.toMemberType(cell.getTimeValue());
                        }
                    }
                    value = conv.toMemberType(values);
                }
                else
                {
                    OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                    String cellValueType = cell.getValueType();
                    if (cellValueType.equals(OfficeValueTypeAttribute.Value.BOOLEAN.toString()))
                    {
                        value = conv.toMemberType(cell.getBooleanValue());
                    }
                    else if (cellValueType.equals(OfficeValueTypeAttribute.Value.STRING.toString()))
                    {
                        value = conv.toMemberType(cell.getStringValue());
                    }
                    else if (cellValueType.equals(OfficeValueTypeAttribute.Value.FLOAT.toString()))
                    {
                        value = conv.toMemberType(cell.getDoubleValue());
                    }
                    else if (cellValueType.equals(OfficeValueTypeAttribute.Value.DATE.toString()))
                    {
                        value = conv.toMemberType(cell.getDateValue());
                    }
                    else if (cellValueType.equals(OfficeValueTypeAttribute.Value.TIME.toString()))
                    {
                        value = conv.toMemberType(cell.getTimeValue());
                    }
                }
            }
            else
            {
                OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
                value = getMemberValueFromCell(mapping, 0, cell);
            }
            if (op != null && value != null)
            {
                return op.wrapSCOField(fieldNumber, value, false, false, true);
            }
            return value;
        }
        else if (RelationType.isRelationSingleValued(relationType))
        {
            // Persistable object - retrieve the string form of the identity, and find the object
            OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
            String idStr = cell.getStringValue();
            if (idStr == null)
            {
                return null;
            }

            if (idStr.startsWith("[") && idStr.endsWith("]"))
            {
                idStr = idStr.substring(1, idStr.length()-1);
                Object obj = null;
                AbstractClassMetaData memberCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (memberCmd.usesSingleFieldIdentityClass() && idStr.indexOf(':') > 0)
                {
                    // Uses persistent identity
                    obj = IdentityUtils.getObjectFromPersistableIdentity(idStr, memberCmd, ec);
                }
                else
                {
                    // Uses legacy identity
                    obj = IdentityUtils.getObjectFromIdString(idStr, memberCmd, ec, true);
                }
                return obj;
            }
            else
            {
                return null;
            }
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Collection/Map/Array
            OdfTableCell cell = row.getCellByIndex(mapping.getColumn(0).getPosition());
            String cellStr = cell.getStringValue();
            if (cellStr == null)
            {
                return null;
            }

            if (cellStr.startsWith("[") && cellStr.endsWith("]"))
            {
                cellStr = cellStr.substring(1, cellStr.length()-1);
                String[] components = MetaDataUtils.getInstance().getValuesForCommaSeparatedAttribute(cellStr);
                if (Collection.class.isAssignableFrom(mmd.getType()))
                {
                    Collection<Object> coll;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    if (components != null)
                    {
                        AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                            ec.getClassLoaderResolver(), ec.getMetaDataManager());
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle Collection<interface>
                            Object element = null;
                            if (elementCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                            {
                                // Uses persistent identity
                                element = IdentityUtils.getObjectFromPersistableIdentity(components[i], elementCmd, ec);
                            }
                            else
                            {
                                // Uses legacy identity
                                element = IdentityUtils.getObjectFromIdString(components[i], elementCmd, ec, true);
                            }
                            coll.add(element);
                        }
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, coll, false, false, true);
                    }
                    return coll;
                }
                else if (Map.class.isAssignableFrom(mmd.getType()))
                {
                    AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
                    AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());

                    Map map;
                    try
                    {
                        Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                        map = (Map) instanceType.newInstance();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    if (components != null)
                    {
                        for (int i=0;i<components.length;i++)
                        {
                            String keyCmpt = components[i];
                            i++;
                            String valCmpt = components[i];

                            // Strip square brackets from entry bounds
                            String keyStr = keyCmpt.substring(1, keyCmpt.length()-1);
                            String valStr = valCmpt.substring(1, valCmpt.length()-1);

                            Object key = null;
                            if (keyCmd != null)
                            {
                                // TODO handle Map<interface, ?>
                                if (keyCmd.usesSingleFieldIdentityClass() && keyStr.indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    key = IdentityUtils.getObjectFromPersistableIdentity(keyStr, keyCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    key = IdentityUtils.getObjectFromIdString(keyStr, keyCmd, ec, true);
                                }
                            }
                            else
                            {
                                String keyTypeName = mmd.getMap().getKeyType();
                                Class keyType = ec.getClassLoaderResolver().classForName(keyTypeName);
                                if (Enum.class.isAssignableFrom(keyType))
                                {
                                    key = Enum.valueOf(keyType, keyStr);
                                }
                                else if (keyType == String.class)
                                {
                                    key = keyStr;
                                }
                                else
                                {
                                    // TODO Support other map key types
                                    throw new NucleusException("Don't currently support retrieval of Maps with keys of type " + keyTypeName + " (field="+mmd.getFullFieldName() + ")");
                                }
                            }

                            Object val = null;
                            if (valCmd != null)
                            {
                                // TODO handle Map<?, interface>
                                if (valCmd.usesSingleFieldIdentityClass() && valStr.indexOf(':') > 0)
                                {
                                    // Uses persistent identity
                                    val = IdentityUtils.getObjectFromPersistableIdentity(valStr, valCmd, ec);
                                }
                                else
                                {
                                    // Uses legacy identity
                                    val = IdentityUtils.getObjectFromIdString(valStr, valCmd, ec, true);
                                }
                            }
                            else
                            {
                                String valTypeName = mmd.getMap().getValueType();
                                Class valType = ec.getClassLoaderResolver().classForName(valTypeName);
                                if (Enum.class.isAssignableFrom(valType))
                                {
                                    val = Enum.valueOf(valType, valStr);
                                }
                                else if (valType == String.class)
                                {
                                    val = valStr;
                                }
                                else
                                {
                                    // TODO Support other map value types
                                    throw new NucleusException("Don't currently support retrieval of Maps with values of type " + valTypeName + " (field="+mmd.getFullFieldName() + ")");
                                }
                            }

                            map.put(key, val);
                        }
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, map, false, false, true);
                    }
                    return map;
                }
                else if (mmd.getType().isArray())
                {
                    Object array = null;
                    if (components != null)
                    {
                        AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(
                            ec.getClassLoaderResolver(), ec.getMetaDataManager());
                        array = Array.newInstance(mmd.getType().getComponentType(), components.length);
                        for (int i=0;i<components.length;i++)
                        {
                            // TODO handle interface[]
                            Object element = null;
                            if (elementCmd.usesSingleFieldIdentityClass() && components[i].indexOf(':') > 0)
                            {
                                // Uses persistent identity
                                element = IdentityUtils.getObjectFromPersistableIdentity(components[i], elementCmd, ec);
                            }
                            else
                            {
                                // Uses legacy identity
                                element = IdentityUtils.getObjectFromIdString(components[i], elementCmd, ec, true);
                            }
                            Array.set(array, i, element);
                        }
                    }
                    else
                    {
                        array = Array.newInstance(mmd.getType().getComponentType(), 0);
                    }
                    if (op != null)
                    {
                        return op.wrapSCOField(fieldNumber, array, false, false, true);
                    }
                    return array;
                }
            }
            throw new NucleusException("Dont currently support retrieval of collection/map/array types from ODF");
        }
        else
        {
            throw new NucleusException("Dont currently support retrieval of type " + mmd.getTypeName() + " from ODF");
        }
    }

    protected Object getMemberValueFromCell(MemberColumnMapping mapping, int pos, OdfTableCell cell)
    {
        Object value = null;
        Class type = mapping.getMemberMetaData().getType();
        AbstractMemberMetaData mmd = mapping.getMemberMetaData();
        if (type == Boolean.class)
        {
            if (cell.getBooleanValue() == null)
            {
                return null;
            }
            value = cell.getBooleanValue();
        }
        else if (type == Byte.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = cell.getDoubleValue().byteValue();
        }
        else if (type == Character.class)
        {
            if (cell.getStringValue() == null)
            {
                return null;
            }
            value = cell.getStringValue().charAt(0);
        }
        else if (type == Double.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = cell.getDoubleValue();
        }
        else if (type == Float.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = Float.valueOf(cell.getDoubleValue().floatValue());
        }
        else if (type == Integer.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = Integer.valueOf(cell.getDoubleValue().intValue());
        }
        else if (type == Long.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = Long.valueOf(cell.getDoubleValue().longValue());
        }
        else if (type == Short.class)
        {
            if (cell.getDoubleValue() == null)
            {
                return null;
            }
            value = Short.valueOf(cell.getDoubleValue().shortValue());
        }
        else if (type == Calendar.class)
        {
            if (cell.getDateValue() == null)
            {
                return null;
            }
            value = cell.getDateValue();
        }
        else if (java.sql.Date.class.isAssignableFrom(type))
        {
            if (cell.getDateValue() == null)
            {
                return null;
            }
            value = new java.sql.Date(cell.getDateValue().getTime().getTime());
        }
        else if (java.sql.Time.class.isAssignableFrom(type))
        {
            if (cell.getTimeValue() == null)
            {
                return null;
            }
            value = new java.sql.Time(cell.getTimeValue().getTime().getTime());
        }
        else if (java.sql.Timestamp.class.isAssignableFrom(type))
        {
            if (cell.getDateValue() == null)
            {
                return null;
            }
            value = new java.sql.Timestamp(cell.getDateValue().getTime().getTime());
        }
        else if (Date.class.isAssignableFrom(type))
        {
            if (cell.getDateValue() == null)
            {
                return null;
            }
            Calendar cal = cell.getDateValue();
            value = cal.getTime();
        }
        else if (type == Currency.class)
        {
            if (cell.getStringValue() == null)
            {
                return null;
            }
            TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            value = conv.toMemberType(cell.getStringValue());
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            ColumnMetaData colmd = null;
            if (mmd.getColumnMetaData() != null && mmd.getColumnMetaData().length > 0)
            {
                colmd = mmd.getColumnMetaData()[0];
            }
            boolean useLong = MetaDataUtils.persistColumnAsNumeric(colmd);
            if (useLong)
            {
                Double cellValue = cell.getDoubleValue();
                if (cellValue != null)
                {
                    value = mmd.getType().getEnumConstants()[(int)cellValue.longValue()];
                }
                else
                {
                    return null;
                }
            }
            else
            {
                String cellValue = cell.getStringValue();
                if (cellValue != null && cellValue.length() > 0)
                {
                    value = Enum.valueOf(type, cell.getStringValue());
                }
                else
                {
                    return null;
                }
            }
        }
        else if (byte[].class == type)
        {
            String cellValue = cell.getStringValue();
            if (cellValue != null && cellValue.length() > 0)
            {
                value = Base64.decode(cellValue);
            }
        }
        else
        {
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

            // TODO Make use of default TypeConverter for a type before falling back to String/Long
            TypeConverter strConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), String.class);
            TypeConverter longConv = ec.getNucleusContext().getTypeManager().getTypeConverterForType(mmd.getType(), Long.class);
            if (useLong && longConv != null)
            {
                value = longConv.toMemberType(cell.getDoubleValue().longValue());
            }
            else if (!useLong && strConv != null)
            {
                String cellValue = cell.getStringValue();
                if (cellValue != null && cellValue.length() > 0)
                {
                    value = strConv.toMemberType(cell.getStringValue());
                }
                else
                {
                    return null;
                }
            }
            else if (!useLong && longConv != null)
            {
                value = longConv.toMemberType(cell.getDoubleValue().longValue());
            }
            else
            {
                // Not supported as String so just set to null
                NucleusLogger.PERSISTENCE.warn("Field " + mmd.getFullFieldName() + 
                " could not be set in the object since it is not persistable to ODF");
                return null;
            }
        }
        return value;
    }
}