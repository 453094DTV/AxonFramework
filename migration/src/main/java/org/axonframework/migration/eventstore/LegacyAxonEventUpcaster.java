/*
 * Copyright (c) 2010-2012. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.migration.eventstore;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.ContentType;
import org.axonframework.serializer.IntermediateRepresentation;
import org.axonframework.serializer.SerializedType;
import org.axonframework.serializer.Upcaster;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.tree.DefaultDocument;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Event preprocessor that upcasts events serialized by the XStreamEventSerializer in versions 0.6 and prior of
 * AxonFramework, to the event format supported since 0.7.
 * <p/>
 * This upcaster uses dom4j Document as event representation, which is supported by the {@link
 * org.axonframework.serializer.XStreamSerializer}.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class LegacyAxonEventUpcaster implements Upcaster {

    @Override
    public boolean canUpcast(SerializedType serializedType) {
        return serializedType.getRevision() < 0;
    }

    @Override
    public ContentType expectedContentType() {
        return ContentType.DOM4J;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public IntermediateRepresentation upcast(IntermediateRepresentation event) {
        Element rootNode = ((Document) event.getContents()).getRootElement();
        if (rootNode.attribute("eventRevision") == null) {
            rootNode.addAttribute("eventRevision", "0");
            Element metaData = rootNode.addElement("metaData").addElement("values");
            Iterator<Element> children = rootNode.elementIterator();
            while (children.hasNext()) {
                Element childNode = children.next();
                String childName = childNode.getName();
                if ("eventIdentifier".equals(childName)) {
                    addMetaDataEntry(metaData, "_identifier", childNode.getTextTrim(), "uuid");
                    rootNode.remove(childNode);
                } else if ("timestamp".equals(childName)) {
                    addMetaDataEntry(metaData, "_timestamp", childNode.getTextTrim(), "localDateTime");
                    rootNode.remove(childNode);
                }
            }
        }
        Document document = new DefaultDocument();
        Element newRoot = document.addElement("domain-event");
        Element payload = newRoot.addElement("payload");
        String objectType = rootNode.getName().replaceAll("\\_\\-", "\\$");
        payload.addAttribute("class", objectType);
        Set<String> forbiddenPayloadElements = new HashSet<String>(Arrays.asList("metaData",
                                                                                 "aggregateIdentifier",
                                                                                 "sequenceNumber",
                                                                                 "timestamp"));
        for (Object node : rootNode.elements()) {
            Element element = (Element) node;
            if (!forbiddenPayloadElements.contains(element.getName())) {
                payload.add(element.createCopy());
            } else {
                newRoot.add(element.createCopy());
            }
        }
        newRoot.addElement("timestamp").addText(extractMetaDataValue(newRoot, "_timestamp"));
        newRoot.addElement("eventIdentifier").addText(extractMetaDataValue(newRoot, "_identifier"));
        String eventRevision = rootNode.attribute("eventRevision").getValue();
        payload.addAttribute("eventRevision", eventRevision);
        return new Dom4jRepresentation(document, DomainEventMessage.class.getName(), Integer.parseInt(eventRevision));
    }

    private String extractMetaDataValue(Element newRoot, String metaDataKey) {
        for (Object entry : newRoot.element("metaData").element("values").elements()) {
            Element element = (Element) entry;
            String key = element.elementTextTrim("string");
            if (metaDataKey.equals(key)) {
                element.detach();
                return (element.node(1).getText());
            }
        }
        return null;
    }

    private void addMetaDataEntry(Element metaData, String key, String value, String keyType) {
        Element entry = metaData.addElement("entry");
        entry.addElement("string").setText(key);
        entry.addElement(keyType).setText(value);
    }
}
