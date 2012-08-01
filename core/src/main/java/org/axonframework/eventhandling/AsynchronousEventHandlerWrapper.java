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

package org.axonframework.eventhandling;

import org.axonframework.domain.EventMessage;

import java.util.concurrent.Executor;

/**
 * The AsynchronousEventHandlerWrapper can wrap any event listener to give it asynchronous behavior. The wrapper will
 * schedule all incoming events for processing, making the calling thread return immediately.
 *
 * @author Allard Buijze
 * @since 0.3
 */
public class AsynchronousEventHandlerWrapper extends AsynchronousExecutionWrapper<EventMessage<?>>
        implements EventListenerProxy {

    private final EventListener eventListener;
    private final Class<?> listenerType;

    /**
     * Initialize the AsynchronousEventHandlerWrapper for the given <code>eventListener</code> using the given
     * <code>executor</code> and <code>transactionManager</code>. The transaction manager is used to start and stop any
     * underlying transactions necessary for event processing.
     *
     * @param eventListener      The event listener this instance manages
     * @param transactionManager The transaction manager that will manage underlying transactions for this event
     * @param sequencingPolicy   The sequencing policy for concurrent execution of events
     * @param executor           The executor that processes the events
     */
    public AsynchronousEventHandlerWrapper(EventListener eventListener, TransactionManager transactionManager,
                                           SequencingPolicy<? super EventMessage<?>> sequencingPolicy,
                                           Executor executor) {
        super(executor, transactionManager, sequencingPolicy);
        this.eventListener = eventListener;
        if (eventListener instanceof EventListenerProxy) {
            this.listenerType = ((EventListenerProxy) eventListener).getTargetType(); // NOSONAR - False alarm
        } else {
            this.listenerType = eventListener.getClass();
        }
    }

    /**
     * Initialize the AsynchronousEventHandlerWrapper for the given <code>eventListener</code> using the given
     * <code>executor</code>.
     * <p/>
     * Note that the underlying bean will not be notified of any transactions.
     *
     * @param eventListener    The event listener this instance manages
     * @param sequencingPolicy The sequencing policy for concurrent execution of events
     * @param executor         The executor that processes the events
     * @see #AsynchronousEventHandlerWrapper(EventListener, TransactionManager, SequencingPolicy,
     *      java.util.concurrent.Executor)
     */
    public AsynchronousEventHandlerWrapper(EventListener eventListener,
                                           SequencingPolicy<? super EventMessage> sequencingPolicy, Executor executor) {
        super(executor, sequencingPolicy);
        this.eventListener = eventListener;
        this.listenerType = eventListener.getClass();
    }

    /**
     * Handles the event by scheduling it for execution by the target event handler.
     *
     * @param event The event to schedule
     */
    @Override
    public void handle(EventMessage event) {
        schedule(event);
    }

    @Override
    public Class<?> getTargetType() {
        return listenerType;
    }

    @Override
    protected void doHandle(EventMessage<?> event) {
        eventListener.handle(event);
    }
}
