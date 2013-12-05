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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.odf.ODFUtils;
import org.odftoolkit.odfdom.doc.table.OdfTableRow;

/**
 * FieldManager to handle the retrieval of information for an embedded persistable object from a row of ODF.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    AbstractMemberMetaData embeddedMetaData;

    public FetchEmbeddedFieldManager(ObjectProvider sm, OdfTableRow row, AbstractMemberMetaData mmd)
    {
        super(sm, row);
        embeddedMetaData = mmd;
    }

    protected int getColumnIndexForMember(int memberNumber)
    {
        return ODFUtils.getColumnPositionForFieldOfEmbeddedClass(memberNumber, embeddedMetaData);
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        EmbeddedMetaData emd = embeddedMetaData.getEmbeddedMetaData();
        AbstractMemberMetaData[] emb_mmd = emd.getMemberMetaData();
        AbstractMemberMetaData mmd = emb_mmd[fieldNumber];
        RelationType relationType = mmd.getRelationType(clr);

        // Special cases
        if (RelationType.isRelationSingleValued(relationType) && mmd.isEmbedded())
        {
            // Persistable object embedded into table of this object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                ObjectProvider embSM = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                embSM.replaceFields(embcmd.getAllMemberPositions(), new FetchEmbeddedFieldManager(embSM, row, mmd));
                return embSM.getObject();
            }
        }

        return fetchObjectFieldFromCell(fieldNumber, mmd, clr);
    }
}