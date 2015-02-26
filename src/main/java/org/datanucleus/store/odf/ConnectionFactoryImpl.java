/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.OdfDocument;
import org.odftoolkit.odfdom.pkg.OdfFileDom;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.incubator.doc.office.OdfOfficeAutomaticStyles;
import org.odftoolkit.odfdom.incubator.doc.style.OdfStyle;
import org.odftoolkit.odfdom.dom.style.OdfStyleFamily;
import org.odftoolkit.odfdom.dom.style.props.OdfTableCellProperties;

/**
 * Implementation of a ConnectionFactory for ODF.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    String filename = null;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);
        // "odf:file:{filename}"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("you haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }
        if (!url.startsWith("odf:"))
        {
            throw new NucleusException("invalid URL: "+url);
        }

        // Split the URL into filename
        String str = url.substring("odf:".length()); // Omit "odf:"
        if (str.indexOf("file:") != 0)
        {
            throw new NucleusException("invalid URL: "+url);
        }

        filename = str.substring("file:".length()); // Omit "file:"
    }

    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options)
    {
        return new ManagedConnectionImpl();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        /** The ODF file. */
        File file;

        public ManagedConnectionImpl()
        {
        }

        public Object getConnection()
        {
            if (conn == null)
            {
                try
                {
                    file = new File(filename);
                    if (!file.exists())
                    {
                        // ODF spreadsheet doesn't exist, so create
                        OdfSpreadsheetDocument doc = OdfSpreadsheetDocument.newSpreadsheetDocument();

                        // Remove the default table(s) added in construction
                        List<OdfTable> tables = doc.getTableList();
                        if (tables != null && !tables.isEmpty())
                        {
                            Iterator<OdfTable> tblIter = tables.iterator();
                            while (tblIter.hasNext())
                            {
                                OdfTable tbl = tblIter.next();
                                tbl.remove();
                            }
                        }

                        // Make sure we have all required styles
                        OdfFileDom contentDoc = doc.getContentDom();
                        OdfOfficeAutomaticStyles styles = contentDoc.getOrCreateAutomaticStyles();

                        // ColumnHeader colouring
                        OdfStyle style = styles.getStyle("DN_Headers", OdfStyleFamily.TableRow);
                        if (style == null)
                        {
                            style = new OdfStyle(contentDoc);
                            style.setStyleNameAttribute("DN_Headers");
                            style.setStyleFamilyAttribute(OdfStyleFamily.TableCell.getName());
                            style.setProperty(OdfTableCellProperties.BackgroundColor, "#74a3db");
                            styles.appendChild(style);
                        }

                        // Primary-Key colouring
                        style = styles.getStyle("DN_PK", OdfStyleFamily.TableCell);
                        if (style == null)
                        {
                            style = new OdfStyle(contentDoc);
                            style.setStyleNameAttribute("DN_PK");
                            style.setStyleFamilyAttribute(OdfStyleFamily.TableCell.getName());
                            style.setProperty(OdfTableCellProperties.BackgroundColor, "#c2d9e0");
                            styles.appendChild(style);
                        }

                        // Relation colouring
                        style = styles.getStyle("DN_Relation", OdfStyleFamily.TableCell);
                        if (style == null)
                        {
                            style = new OdfStyle(contentDoc);
                            style.setStyleNameAttribute("DN_Relation");
                            style.setStyleFamilyAttribute(OdfStyleFamily.TableCell.getName());
                            style.setProperty(OdfTableCellProperties.BackgroundColor, "#DDDDDD");
                            styles.appendChild(style);
                        }

                        doc.save(file);
                        conn = doc;
                    }

                    NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is starting for file=" + file);
                    conn = OdfDocument.loadDocument(file);
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }
            return conn;
        }

        public void release()
        {
            if (commitOnRelease)
            {
                // Non-transactional operation end : Write to file and close connection
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is committing");

                // Note that if we have one operation which does a get() then that calls another method to do a get() and then release() this will close the connection before
                // the second release comes in.
                try
                {
                    ((OdfDocument)conn).save(file);
//                    ((OdfDocument)conn).close();
//                    file = null;
//                    conn = null;
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(),e);
                }
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " committed connection");
            }
            super.release();
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }

            try
            {
                for (int i=0; i<listeners.size(); i++)
                {
                    listeners.get(i).managedConnectionPreClose();
                }

                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " is committing");
                ((OdfDocument)conn).save(file);
                ((OdfDocument)conn).close();
                NucleusLogger.CONNECTION.debug("ManagedConnection " + this.toString() + " committed connection");
                file = null;
                conn = null;
            }
            catch (Exception e)
            {
                throw new NucleusException(e.getMessage(),e);
            }
            finally
            {
                for (int i=0; i<listeners.size(); i++)
                {
                    listeners.get(i).managedConnectionPostClose();
                }
            }
        }

        public XAResource getXAResource()
        {
            return null;
        }
    }
}