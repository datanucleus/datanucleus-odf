/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.odf.valuegenerator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractDatastoreGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.store.valuegenerator.ValueGenerator;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;

/**
 * Generator that uses a collection in ODF to store and allocate identity values.
 */
public class IncrementGenerator extends AbstractDatastoreGenerator<Long>
{
    static final String INCREMENT_COL_NAME = "increment";

    /** Key used in the Table to access the increment count */
    private String key;

    private String worksheetName = null;

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param name Symbolic name for this generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(String name, Properties props)
    {
        super(name, props);
        this.key = properties.getProperty(ValueGenerator.PROPERTY_FIELD_NAME, name);
        this.worksheetName = properties.getProperty(ValueGenerator.PROPERTY_SEQUENCETABLE_TABLE);
        if (this.worksheetName == null)
        {
            this.worksheetName = "IncrementTable";
        }
        if (properties.containsKey(ValueGenerator.PROPERTY_KEY_CACHE_SIZE))
        {
            allocationSize = Integer.valueOf(properties.getProperty(ValueGenerator.PROPERTY_KEY_CACHE_SIZE));
        }
        else
        {
            allocationSize = 1;
        }
    }

    public String getName()
    {
        return this.name;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.valuegenerator.AbstractGenerator#reserveBlock(long)
     */
    protected ValueGenerationBlock<Long> reserveBlock(long size)
    {
        if (size < 1)
        {
            return null;
        }

        // Allocate value(s)
        ManagedConnection mconn = connectionProvider.retrieveConnection();
        List<Long> oids = new ArrayList<Long>();
        try
        {
            // Create the worksheet if not existing
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
            OdfTable table = spreadsheetDoc.getTableByName(worksheetName);
            OdfTableRow row = null;
            if (table == null)
            {
                if (!storeMgr.getSchemaHandler().isAutoCreateTables())
                {
                    throw new NucleusUserException(Localiser.msg("040011", worksheetName));
                }

                table = OdfTable.newTable(spreadsheetDoc, 1, 2);
                table.setTableName(worksheetName);
                row = table.getRowByIndex(0);
                OdfTableCell cell = row.getCellByIndex(0);
                cell.setStringValue(key);
                cell = row.getCellByIndex(1);
                cell.setDoubleValue(Double.valueOf(0));
            }
            else
            {
                List<OdfTableRow> rows = table.getRowList();
                Iterator<OdfTableRow> rowIter = rows.iterator();
                while (rowIter.hasNext())
                {
                    OdfTableRow tblRow = rowIter.next();
                    OdfTableCell tblCell = tblRow.getCellByIndex(0);
                    if (tblCell.getStringValue().equals(key))
                    {
                        row = tblRow;
                        break;
                    }
                }
                if (row == null)
                {
                    row = table.appendRow();
                    OdfTableCell cell = row.getCellByIndex(0);
                    cell.setStringValue(key);
                    cell = row.getCellByIndex(1);
                    cell.setDoubleValue(Double.valueOf(0));
                }
            }

            // Update the row
            NucleusLogger.VALUEGENERATION.debug("Allowing " + size + " values for increment generator for "+key);
            OdfTableCell valueCell = row.getCellByIndex(1);
            long currentVal = valueCell.getDoubleValue().longValue();
            valueCell.setDoubleValue(Double.valueOf(currentVal+size));
            for (int i=0;i<size;i++)
            {
                oids.add(currentVal+1);
                currentVal++;
            }
        }
        finally
        {
            connectionProvider.releaseConnection();
        }
        return new ValueGenerationBlock<Long>(oids);
    }
}