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

package org.axonframework.saga.repository.jpa;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.saga.AssociationValue;
import org.axonframework.saga.NoSuchSagaException;
import org.axonframework.saga.ResourceInjector;
import org.axonframework.saga.Saga;
import org.axonframework.saga.repository.AbstractSagaRepository;
import org.axonframework.serializer.JavaSerializer;
import org.axonframework.serializer.SerializedType;
import org.axonframework.serializer.Serializer;
import org.axonframework.serializer.SimpleSerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.EntityNotFoundException;

/**
 * JPA implementation of the Saga Repository. It uses an {@link EntityManager} to persist the actual saga in a backing
 * store.
 * <p/>
 * After each operations that modified the backing store, {@link javax.persistence.EntityManager#flush()} is invoked to
 * ensure the store contains the last modifications. To override this behavior, see {@link }
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class JpaSagaRepository extends AbstractSagaRepository {

    private static final Logger logger = LoggerFactory.getLogger(JpaSagaRepository.class);

    private final EntityManagerProvider entityManagerProvider;
    private ResourceInjector injector;
    private Serializer serializer;
    private volatile boolean useExplicitFlush = true;
    private volatile boolean initialized = false;

    /**
     * Initializes a Saga Repository with a <code>JavaSerializer</code>.
     *
     * @param entityManagerProvider The EntityManagerProvider providing the EntityManager instance for this repository
     */
    public JpaSagaRepository(EntityManagerProvider entityManagerProvider) {
        this.entityManagerProvider = entityManagerProvider;
        serializer = new JavaSerializer();
    }

    @Override
    public <T extends Saga> Set<T> find(Class<T> type, AssociationValue associationValue) {
        if (!initialized) {
            initialize();
        }
        return super.find(type, associationValue);
    }

    @Override
    public void add(Saga saga) {
        if (!initialized) {
            initialize();
        }
        super.add(saga);
    }

    @Override
    public void commit(Saga saga) {
        if (!initialized) {
            initialize();
        }
        super.commit(saga);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected void removeAssociationValue(AssociationValue associationValue, String sagaType, String sagaIdentifier) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        int updateCount = entityManager.createQuery(
                "DELETE FROM AssociationValueEntry ae "
                        + "WHERE ae.associationKey = :associationKey "
                        + "AND ae.associationValue = :associationValue "
                        + "AND ae.sagaType = :sagaType "
                        + "AND ae.sagaId = :sagaId")
                                       .setParameter("associationKey", associationValue.getKey())
                                       .setParameter("associationValue", associationValue.getValue())
                                       .setParameter("sagaType", sagaType)
                                       .setParameter("sagaId", sagaIdentifier)
                                       .executeUpdate();
        if (updateCount == 0 && logger.isWarnEnabled()) {
            logger.warn("Wanted to remove association value, but it was already gone: sagaId= {}, key={}, value={}",
                        new Object[]{sagaIdentifier,
                                associationValue.getKey(),
                                associationValue.getValue()});
        }
    }

    @Override
    protected String typeOf(Class<? extends Saga> sagaClass) {
        return serializer.typeForClass(sagaClass).getName();
    }

    @Override
    protected void storeAssociationValue(AssociationValue associationValue, String sagaType, String sagaIdentifier) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        entityManager.persist(new AssociationValueEntry(sagaType, sagaIdentifier, associationValue));
        if (useExplicitFlush) {
            entityManager.flush();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends Saga> T loadSaga(Class<T> type, String sagaId) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        List<byte[]> serializedSagaList = (List<byte[]>) entityManager
                .createQuery("SELECT se.serializedSaga FROM SagaEntry se WHERE se.sagaId = :sagaId")
                .setParameter("sagaId", sagaId)
                .setMaxResults(1)
                .getResultList();
        if (serializedSagaList == null || serializedSagaList.isEmpty()) {
            throw new NoSuchSagaException(type, sagaId);
        }
        byte[] serializedSaga = serializedSagaList.get(0);
        SerializedType serializedType = serializer.typeForClass(type);
        Saga loadedSaga = serializer.deserialize(new SimpleSerializedObject<byte[]>(serializedSaga,
                                                                                    byte[].class,
                                                                                    serializedType));
        if (!type.isInstance(loadedSaga)) {
            logger.debug("Saga with id [{}] was of another type than expected. It is ignored.", sagaId);
            return null;
        }
        T storedSaga = type.cast(loadedSaga);
        if (injector != null) {
            injector.injectResources(storedSaga);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded saga id [{}] of type [{}]", sagaId, loadedSaga.getClass().getName());
        }
        return storedSaga;
    }

    @Override
    protected void deleteSaga(Saga saga) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        try {
            entityManager.createQuery("DELETE FROM AssociationValueEntry ae WHERE ae.sagaId = :sagaId")
                         .setParameter("sagaId", saga.getSagaIdentifier())
                         .executeUpdate();
            entityManager.remove(entityManager.getReference(SagaEntry.class, saga.getSagaIdentifier()));
        } catch (EntityNotFoundException e) {
            logger.info("Could not delete SagaEntry {}, it appears to have already been deleted.",
                        saga.getSagaIdentifier());
        }
        entityManager.flush();
    }

    @Override
    protected void updateSaga(Saga saga) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        SagaEntry entry = new SagaEntry(saga, serializer);
        if (logger.isDebugEnabled()) {
            logger.debug("Updating saga id {} as {}", saga.getSagaIdentifier(), new String(entry.getSerializedSaga(),
                                                                                           Charset.forName("UTF-8")));
        }
        if (useExplicitFlush) {
            entityManager.flush();
        }
        int updateCount = entityManager.createQuery(
                "UPDATE SagaEntry s SET s.serializedSaga = :serializedSaga, s.revision = :revision "
                        + "WHERE s.sagaId = :sagaId AND s.sagaType = :sagaType")
                                       .setParameter("serializedSaga", entry.getSerializedSaga())

                                       .setParameter("revision", entry.getRevision())
                                       .setParameter("sagaId", entry.getSagaId())
                                       .setParameter("sagaType", entry.getSagaType())
                                       .executeUpdate();
        if (updateCount == 0) {
            logger.warn("Expected to be able to update a Saga instance, but no rows were found. Inserting instead.");
            entityManager.persist(entry);
            if (useExplicitFlush) {
                entityManager.flush();
            }
        }
    }

    @Override
    protected void storeSaga(Saga saga) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        SagaEntry entry = new SagaEntry(saga, serializer);
        entityManager.persist(entry);
        if (logger.isDebugEnabled()) {
            logger.debug("Storing saga id {} as {}", saga.getSagaIdentifier(), new String(entry.getSerializedSaga(),
                                                                                          Charset.forName("UTF-8")));
        }
        if (useExplicitFlush) {
            entityManager.flush();
        }
    }

    /**
     * Initializes the repository by loading all AssociationValues in memory. Failure to initialize properly might
     * result in Saga instance not being found based on their <code>AssociationValue</code>s.
     */
    @SuppressWarnings({"unchecked"})
    private synchronized void initialize() {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        if (!initialized) {
            List<AssociationValueEntry> entries =
                    entityManager.createQuery("SELECT ae FROM AssociationValueEntry ae").getResultList();
            getAssociationValueMap().clear();
            for (AssociationValueEntry entry : entries) {
                AssociationValue associationValue = entry.getAssociationValue();
                getAssociationValueMap().add(associationValue, entry.getSagaType(), entry.getSagaIdentifier());
            }
            initialized = true;
        }
    }

    /**
     * Sets the ResourceInjector to use to inject Saga instances with any (temporary) resources they might need. These
     * are typically the resources that could not be persisted with the Saga.
     *
     * @param resourceInjector The resource injector
     */
    @Resource
    public void setResourceInjector(ResourceInjector resourceInjector) {
        this.injector = resourceInjector;
    }

    /**
     * Sets the Serializer instance to serialize Sagas with. Defaults to the XStream Serializer.
     *
     * @param serializer the Serializer instance to serialize Sagas with
     */
    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Sets whether or not to do an explicit {@link javax.persistence.EntityManager#flush()} after each data modifying
     * operation on the backing storage. Default to <code>true</code>
     *
     * @param useExplicitFlush <code>true</code> to force flush, <code>false</code> otherwise.
     */
    public void setUseExplicitFlush(boolean useExplicitFlush) {
        this.useExplicitFlush = useExplicitFlush;
    }
}
