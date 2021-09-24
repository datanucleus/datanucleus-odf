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
package org.datanucleus.store.odf.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.odf.ODFStoreManager;
import org.datanucleus.store.odf.ODFUtils;
import org.datanucleus.store.odf.fieldmanager.FetchFieldManager;
import org.datanucleus.store.query.AbstractCandidateLazyLoadList;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.incubator.doc.style.OdfStyle;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;
import org.odftoolkit.odfdom.dom.attribute.office.OfficeValueTypeAttribute;

/**
 * Wrapper for a List of candidate instances from ODF. Loads the instances from the workbook lazily.
 */
public class ODFCandidateList extends AbstractCandidateLazyLoadList
{
    ManagedConnection mconn;

    boolean ignoreCache;

    FetchPlan fetchPlan;

    /** Number of objects per class, in same order as class meta-data. */
    List<Integer> numberInstancesPerClass = null;

    /**
     * Constructor for the lazy loaded ODF candidate list.
     * @param cls The candidate class
     * @param subclasses Whether to include subclasses
     * @param ec execution context
     * @param cacheType Type of caching
     * @param mconn Connection to the datastore
     * @param ignoreCache Whether to ignore the cache on object retrieval
     * @param fp FetchPlan for retrieval
     */
    public ODFCandidateList(Class cls, boolean subclasses, ExecutionContext ec, String cacheType, ManagedConnection mconn, boolean ignoreCache, FetchPlan fp)
    {
        super(cls, subclasses, ec, cacheType);
        this.mconn = mconn;
        this.ignoreCache = ignoreCache;
        this.fetchPlan = fp;

        // Count the instances per class by scanning the associated worksheets
        numberInstancesPerClass = new ArrayList<Integer>();
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        ODFStoreManager storeMgr = (ODFStoreManager)ec.getStoreManager();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData cmd = cmdIter.next();
            OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(new String[] {cmd.getFullClassName()}, ec.getClassLoaderResolver(), spreadsheetDoc);
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();
            String sheetName = table.getName();
            OdfTable worksheet = spreadsheetDoc.getTableByName(sheetName);
            int size = 0;
            if (worksheet != null)
            {
                List<OdfTableRow> rows = worksheet.getRowList();
                Iterator<OdfTableRow> rowIter = rows.iterator();
                while (rowIter.hasNext())
                {
                    OdfTableRow row = rowIter.next();
                    OdfStyle style = row.getDefaultCellStyle();
                    String styleName = (style != null ? style.getStyleNameAttribute() : null);
                    if (styleName != null && styleName.equals("DN_Headers"))
                    {
                        // Skip header row(s)
                    }
                    else
                    {
                        size++;
                    }
                }
            }
            numberInstancesPerClass.add(size);
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#getSize()
     */
    @Override
    protected int getSize()
    {
        int size = 0;

        Iterator<Integer> numberIter = numberInstancesPerClass.iterator();
        while (numberIter.hasNext())
        {
            size += numberIter.next();
        }

        return size;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractLazyLoadList#retrieveObjectForIndex(int)
     */
    @Override
    protected Object retrieveObjectForIndex(int index)
    {
        if (index < 0 || index >= getSize())
        {
            throw new NoSuchElementException();
        }

        OdfSpreadsheetDocument spreadsheetDoc = (OdfSpreadsheetDocument)mconn.getConnection();
        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        Iterator<Integer> numIter = numberInstancesPerClass.iterator();
        int first = 0;
        int last = -1;
        while (cmdIter.hasNext())
        {
            final AbstractClassMetaData cmd = cmdIter.next();
            int number = numIter.next();
            last = first+number;

            if (index >= first && index < last)
            {
                // Object is of this candidate type, so find the object
                Table table = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName()).getTable();
                String sheetName = table.getName();
                OdfTable worksheet = spreadsheetDoc.getTableByName(sheetName);
                List<OdfTableRow> rows = worksheet.getRowList();
                int current = first;
                Iterator<OdfTableRow> rowIter = rows.iterator();
                while (rowIter.hasNext())
                {
                    final OdfTableRow row = rowIter.next();
                    OdfStyle style = row.getDefaultCellStyle();
                    String styleName = (style != null ? style.getStyleNameAttribute() : null);
                    if (styleName != null && styleName.equals("DN_Headers"))
                    {
                        // Skip header row(s)
                    }
                    else
                    {
                        if (current == index)
                        {
                            // This row equates to the required index
                            final FieldManager fm = new FetchFieldManager(ec, cmd, row, table);
                            int[] fpFieldNums = fetchPlan.getFetchPlanForClass(cmd).getMemberNumbers();
                            if (cmd.getIdentityType() == IdentityType.APPLICATION)
                            {
                                Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);
                                return ec.findObject(id, new FieldValues()
                                {
                                    public void fetchFields(DNStateManager sm)
                                    {
                                        sm.replaceFields(fpFieldNums, fm);
                                    }
                                    public void fetchNonLoadedFields(DNStateManager sm)
                                    {
                                        sm.replaceNonLoadedFields(fpFieldNums, fm);
                                    }
                                    public FetchPlan getFetchPlanForLoading()
                                    {
                                        return null;
                                    }
                                }, null, ignoreCache, false);
                            }
                            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
                            {
                                int idIndex = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getPosition();
                                OdfTableCell idCell = row.getCellByIndex(idIndex);
                                Object idKey = null;
                                if (ODFUtils.isOfficeValueTypeConsistent(idCell, OfficeValueTypeAttribute.Value.STRING))
                                {
                                    idKey = idCell.getStringValue();
                                }
                                else
                                {
                                    idKey = Long.valueOf(idCell.getDoubleValue().longValue());
                                }
                                Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);
                                return ec.findObject(id, new FieldValues()
                                {
                                    public void fetchFields(DNStateManager sm)
                                    {
                                        sm.replaceFields(fpFieldNums, fm);
                                    }
                                    public void fetchNonLoadedFields(DNStateManager sm)
                                    {
                                        sm.replaceNonLoadedFields(fpFieldNums, fm);
                                    }
                                    public FetchPlan getFetchPlanForLoading()
                                    {
                                        return null;
                                    }
                                }, null, ignoreCache, false);
                            }
                            else
                            {
                                Object id = new SCOID(cmd.getFullClassName());
                                return ec.findObject(id, new FieldValues()
                                {
                                    public void fetchFields(DNStateManager sm)
                                    {
                                        sm.replaceFields(fpFieldNums, fm);
                                    }
                                    public void fetchNonLoadedFields(DNStateManager sm)
                                    {
                                        sm.replaceNonLoadedFields(fpFieldNums, fm);
                                    }
                                    public FetchPlan getFetchPlanForLoading()
                                    {
                                        return null;
                                    }
                                }, null, ignoreCache, false);
                            }
                        }

                        current++;
                    }
                }
            }
            else
            {
                first += number;
            }
        }
        return null;
    }
}