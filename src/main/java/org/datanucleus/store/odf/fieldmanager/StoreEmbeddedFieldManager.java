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

Contributors:
   ...
**********************************************************************/
package org.datanucleus.store.odf.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;

/**
 * FieldManager to handle the store information for an embedded persistable object into ODF.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, OdfTableRow row, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, row, insert, table);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(DNStateManager sm, OdfTableRow row, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, row, insert, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and their owners
            if (sm != null)
            {
                DNStateManager[] ownerSMs = ec.getOwnersForEmbeddedStateManager(sm);
                if (ownerSMs != null && ownerSMs.length == 1 && value != ownerSMs[0].getObject())
                {
                    // Make sure the owner field is set
                    sm.replaceField(fieldNumber, ownerSMs[0].getObject());
                }
            }
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Persistable object embedded into this table
                Class embcls = mmd.getType();
                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
                if (embcmd != null)
                {
                    DNStateManager embSM = null;
                    if (value != null)
                    {
                        embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
                    }
                    else
                    {
                        embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embcmd, sm, fieldNumber, null);
                    }

                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                    embMmds.add(mmd);
                    embSM.provideFields(embcmd.getAllMemberPositions(), new StoreEmbeddedFieldManager(embSM, row, insert, embMmds, table));
                    return;
                }
            }
            else
            {
                // TODO Embedded Collection
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported (embedded), storing as null");
                return;
            }
        }

        if (sm == null)
        {
            // Null the column
            MemberColumnMapping mapping = getColumnMapping(fieldNumber);
            for (int i=0;i<mapping.getNumberOfColumns();i++)
            {
                // TODO Null the column(s)
            }
        }

        storeObjectFieldInternal(fieldNumber, value, mmd, clr, relationType);
    }
}