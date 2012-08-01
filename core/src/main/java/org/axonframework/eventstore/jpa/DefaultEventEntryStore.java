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

package org.axonframework.eventstore.jpa;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.serializer.SerializedObject;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.Query;

/**
 * Implementation of the EventEntryStore that stores events in DomainEventEntry entities and snapshot events in
 * SnapshotEventEntry entities.
 * <p/>
 * This implementation requires that the aforementioned instances are available in the current persistence context.
 *
 * @author Allard Buijze
 * @since 1.2
 */
public class DefaultEventEntryStore implements EventEntryStore {

    @Override
    @SuppressWarnings({"unchecked"})
    public void persistEvent(String aggregateType, DomainEventMessage event, SerializedObject serializedPayload,
                             SerializedObject serializedMetaData, EntityManager entityManager) {
        entityManager.persist(new DomainEventEntry(aggregateType, event, serializedPayload, serializedMetaData));
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public SimpleSerializedDomainEventData loadLastSnapshotEvent(String aggregateType, Object identifier,
                                                                 EntityManager entityManager) {
        List<SimpleSerializedDomainEventData> entries = entityManager
                .createQuery("SELECT new org.axonframework.eventstore.jpa.SimpleSerializedDomainEventData("
                                     + "e.eventIdentifier, e.aggregateIdentifier, e.sequenceNumber, "
                                     + "e.timeStamp, e.payloadType, e.payloadRevision, e.payload, e.metaData) "
                                     + "FROM SnapshotEventEntry e "
                                     + "WHERE e.aggregateIdentifier = :id AND e.type = :type "
                                     + "ORDER BY e.sequenceNumber DESC")
                .setParameter("id", identifier.toString())
                .setParameter("type", aggregateType)
                .setMaxResults(1)
                .setFirstResult(0)
                .getResultList();
        if (entries.size() < 1) {
            return null;
        }
        return entries.get(0);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public List<DomainEventEntry> fetchFilteredBatch(String whereClause, Map<String, Object> parameters,
                                                     int startPosition, int batchSize,
                                                     EntityManager entityManager) {
//        String eventIdentifier, String aggregateIdentifier, long sequenceNumber,
//        String timeStamp, String payloadType, String payloadRevision, byte[] payload,
//        byte[] metaData
        Query query = entityManager.createQuery(
                String.format("SELECT new org.axonframework.eventstore.jpa.SimpleSerializedDomainEventData("
                                      + "e.eventIdentifier, e.aggregateIdentifier, e.sequenceNumber, "
                                      + "e.timeStamp, e.payloadType, e.payloadRevision, e.payload, e.metaData) "
                                      + "FROM DomainEventEntry e %s ORDER BY e.timeStamp ASC, e.sequenceNumber ASC",
                              whereClause != null && whereClause.length() > 0 ? "WHERE " + whereClause : ""))
                                   .setFirstResult(startPosition)
                                   .setMaxResults(batchSize);
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof DateTime) {
                value = entry.getValue().toString();
            }
            query.setParameter(entry.getKey(), value);
        }
        return query.getResultList();
    }

    @Override
    public void persistSnapshot(String aggregateType, DomainEventMessage snapshotEvent,
                                SerializedObject serializedPayload, SerializedObject serializedMetaData,
                                EntityManager entityManager) {
        entityManager.persist(new SnapshotEventEntry(aggregateType, snapshotEvent, serializedPayload,
                                                     serializedMetaData));
    }

    @Override
    public void pruneSnapshots(String type, DomainEventMessage mostRecentSnapshotEvent, int maxSnapshotsArchived,
                               EntityManager entityManager) {
        Iterator<Long> redundantSnapshots = findRedundantSnapshots(type, mostRecentSnapshotEvent,
                                                                   maxSnapshotsArchived, entityManager);
        if (redundantSnapshots.hasNext()) {
            Long sequenceOfFirstSnapshotToPrune = redundantSnapshots.next();
            entityManager.createQuery("DELETE FROM SnapshotEventEntry e "
                                              + "WHERE e.type = :type "
                                              + "AND e.aggregateIdentifier = :aggregateIdentifier "
                                              + "AND e.sequenceNumber <= :sequenceOfFirstSnapshotToPrune")
                         .setParameter("type", type)
                         .setParameter("aggregateIdentifier",
                                       mostRecentSnapshotEvent.getAggregateIdentifier().toString())
                         .setParameter("sequenceOfFirstSnapshotToPrune", sequenceOfFirstSnapshotToPrune)
                         .executeUpdate();
        }
    }

    /**
     * Finds the first of redundant snapshots, returned as an iterator for convenience purposes.
     *
     * @param type                 the type of the aggregate for which to find redundant snapshots
     * @param snapshotEvent        the last appended snapshot event
     * @param maxSnapshotsArchived the number of snapshots that may remain archived
     * @param entityManager        the entityManager providing access to the data store
     * @return an iterator over the snapshots found
     */
    @SuppressWarnings({"unchecked"})
    private Iterator<Long> findRedundantSnapshots(String type, DomainEventMessage snapshotEvent,
                                                  int maxSnapshotsArchived,
                                                  EntityManager entityManager) {
        return entityManager.createQuery(
                "SELECT e.sequenceNumber FROM SnapshotEventEntry e "
                        + "WHERE e.type = :type AND e.aggregateIdentifier = :aggregateIdentifier "
                        + "ORDER BY e.sequenceNumber DESC")
                            .setParameter("type", type)
                            .setParameter("aggregateIdentifier", snapshotEvent.getAggregateIdentifier().toString())
                            .setFirstResult(maxSnapshotsArchived)
                            .setMaxResults(1)
                            .getResultList().iterator();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public List<DomainEventEntry> fetchBatch(String aggregateType, Object identifier, long firstSequenceNumber,
                                             int batchSize, EntityManager entityManager) {
        return (List<DomainEventEntry>) entityManager
                .createQuery("SELECT new org.axonframework.eventstore.jpa.SimpleSerializedDomainEventData("
                                     + "e.eventIdentifier, e.aggregateIdentifier, e.sequenceNumber, "
                                     + "e.timeStamp, e.payloadType, e.payloadRevision, e.payload, e.metaData) "
                                     + "FROM DomainEventEntry e "
                                     + "WHERE e.aggregateIdentifier = :id AND e.type = :type "
                                     + "AND e.sequenceNumber >= :seq "
                                     + "ORDER BY e.sequenceNumber ASC")
                .setParameter("id", identifier.toString())
                .setParameter("type", aggregateType)
                .setParameter("seq", firstSequenceNumber)
                .setMaxResults(batchSize)
                .getResultList();
    }
}
