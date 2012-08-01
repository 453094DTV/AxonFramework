package org.axonframework.domain.annotation;

import org.axonframework.domain.AbstractAggregateRoot;
import org.axonframework.domain.DomainEventMessage;
import org.axonframework.domain.DomainEventStream;
import org.axonframework.domain.GenericDomainEventMessage;
import org.axonframework.domain.MetaData;
import org.axonframework.eventhandling.annotation.AnnotationEventHandlerInvoker;
import org.axonframework.eventsourcing.EventSourcedAggregateRoot;
import org.axonframework.eventsourcing.IncompatibleAggregateException;

/**
 * @author Allard Buijze
 */
public class EventSourcedAggregateGuard<T> extends AbstractAggregateRoot implements EventSourcedAggregateRoot {

    private static final long serialVersionUID = -2689775300916970418L;

    private final AnnotationEventHandlerInvoker invoker;
    private final T aggregateRoot;
    private final AggregateConfiguration configuration;

    public EventSourcedAggregateGuard(T aggregateRoot, AggregateConfiguration configuration) {
        this.aggregateRoot = aggregateRoot;
        this.configuration = configuration;
        invoker = new AnnotationEventHandlerInvoker(aggregateRoot);
    }

    @Override
    public void markDeleted() {
        super.markDeleted();
    }

    public void apply(Object event) {
        apply(event, MetaData.emptyInstance());
    }

    /**
     * Apply the provided event. Applying events means they are added to the uncommitted event queue and forwarded to
     * the {@link #handleRecursively(org.axonframework.domain.DomainEventMessage)} event handler method} for processing.
     * <p/>
     * The event is applied on all entities part of this aggregate.
     *
     * @param eventPayload The payload of the event to apply
     * @param metaData     any meta-data that must be registered with the Event
     */
    public void apply(Object eventPayload, MetaData metaData) {
        if (getIdentifier() == null) {
            // workaround for aggregates that set the aggregate identifier in an Event Handler
            if (getUncommittedEventCount() > 0 || getVersion() != null) {
                throw new IncompatibleAggregateException("The Aggregate Identifier has not been initialized. "
                                                                 + "It must be initialized at the latest when the "
                                                                 + "first event is applied.");
            }
            handleRecursively(new GenericDomainEventMessage<Object>(null, 0, eventPayload, metaData));
            registerEvent(metaData, eventPayload);
        } else {
            DomainEventMessage event = registerEvent(metaData, eventPayload);
            handleRecursively(event);
        }
    }

    @Override
    public void initializeState(DomainEventStream domainEventStream) {
        DomainEventMessage lastEvent = null;
        while (domainEventStream.hasNext()) {
            lastEvent = domainEventStream.next();
            handleRecursively(lastEvent);
        }
        if (lastEvent != null) {
            initializeEventStream(lastEvent.getSequenceNumber());
        }
    }

    private void handleRecursively(DomainEventMessage<?> eventMessage) {
        invoker.invokeEventHandlerMethod(eventMessage);
    }

    @Override
    public Object getIdentifier() {
        return configuration.getIdentifier(aggregateRoot);
    }

    public T getAggregateRoot() {
        return aggregateRoot;
    }

    /**
     * When serializing, we actually want to serialize the aggregate itself
     */
    protected Object writeReplace() {
        // TODO: Serialization of this object needs to be smarter (writeObject, readObject)
        return aggregateRoot;
    }
}
