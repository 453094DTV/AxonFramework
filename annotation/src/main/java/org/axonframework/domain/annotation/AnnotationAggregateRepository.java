package org.axonframework.domain.annotation;

import org.axonframework.domain.DomainEventMessage;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.eventsourcing.AggregateFactory;
import org.axonframework.eventsourcing.EventSourcingRepository;
import org.axonframework.eventstore.EventStore;
import org.axonframework.repository.Repository;

import java.lang.reflect.Constructor;

import static org.axonframework.common.ReflectionUtils.ensureAccessible;

/**
 * @author Allard Buijze
 */
public class AnnotationAggregateRepository<T> implements Repository<T> {

    private final Class<T> type;
    private final EventStore eventStore;
    private final EventBus eventBus;
    private final Repository<EventSourcedAggregateGuard<T>> backingRepository;

    public AnnotationAggregateRepository(Class<T> type, EventStore eventStore, EventBus eventBus) {
        this.type = type;
        backingRepository = initializeBackingRepository(eventStore, eventBus, new GuardedAggregateFactory<T>(type));
        this.eventStore = eventStore;
        this.eventBus = eventBus;
    }

    @Override
    public T load(Object aggregateIdentifier, Long expectedVersion) {
        return backingRepository.load(aggregateIdentifier, expectedVersion).getAggregateRoot();
    }

    @Override
    public T load(Object aggregateIdentifier) {
        return backingRepository.load(aggregateIdentifier).getAggregateRoot();
    }

    @Override
    public void add(T aggregate) {
        backingRepository.add(Aggregate.getGuardFor(aggregate));
    }

    private Repository<EventSourcedAggregateGuard<T>> initializeBackingRepository(
            EventStore eventStore, EventBus eventBus, GuardedAggregateFactory<T> aggregateFactory) {
        EventSourcingRepository<EventSourcedAggregateGuard<T>> repo =
                new EventSourcingRepository<EventSourcedAggregateGuard<T>>(aggregateFactory);
        repo.setEventStore(eventStore);
        repo.setEventBus(eventBus);
        return repo;
    }

    private static class GuardedAggregateFactory<T> implements AggregateFactory<EventSourcedAggregateGuard<T>> {

        private final Class<T> aggregateType;
        private final AggregateConfiguration aggregateConfiguration;

        private GuardedAggregateFactory(Class<T> aggregateType) {
            this.aggregateType = aggregateType;
            aggregateConfiguration = AggregateConfiguration.forAggregate(this.aggregateType);
        }

        @Override
        public EventSourcedAggregateGuard<T> createAggregate(Object aggregateIdentifier,
                                                             DomainEventMessage firstEvent) {
            T newInstance;
            try {
                Constructor<T> constructor = aggregateType.getDeclaredConstructor();
                ensureAccessible(constructor);
                newInstance = constructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Aggregate.getGuardFor(newInstance);
        }

        @Override
        public String getTypeIdentifier() {
            return aggregateConfiguration.getTypeIdentifier();
        }

        @Override
        public Class getAggregateType() {
            return EventSourcedAggregateGuard.class;
        }
    }
}
