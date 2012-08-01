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

package org.axonframework.commandhandling.annotation;

import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.callbacks.VoidCallback;
import org.axonframework.domain.IdentifierFactory;
import org.axonframework.domain.MetaData;
import org.axonframework.domain.StubDomainEvent;
import org.axonframework.eventsourcing.annotation.AbstractAnnotatedAggregateRoot;
import org.axonframework.repository.Repository;
import org.axonframework.unitofwork.UnitOfWork;
import org.junit.*;

import static org.axonframework.commandhandling.GenericCommandMessage.asCommandMessage;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 */
@SuppressWarnings({"unchecked"})
public class AggregateAnnotationCommandHandlerTest {

    private AggregateAnnotationCommandHandler<StubCommandAnnotatedAggregate> testSubject;
    private SimpleCommandBus commandBus;
    private Repository<StubCommandAnnotatedAggregate> mockRepository;

    @Before
    public void setUp() throws Exception {
        commandBus = spy(new SimpleCommandBus());
        mockRepository = mock(Repository.class);
        testSubject = AggregateAnnotationCommandHandler.subscribe(StubCommandAnnotatedAggregate.class,
                                                                  mockRepository,
                                                                  commandBus);
    }

    @Test
    public void testAggregateConstructorThrowsException() {
        commandBus.dispatch(asCommandMessage(new FailingCreateCommand("parameter")), new VoidCallback() {
            @Override
            protected void onSuccess() {
                fail("Expected exception");
            }

            @Override
            public void onFailure(Throwable cause) {
                assertEquals("parameter", cause.getMessage());
            }
        });
    }

    @Test
    public void testAggregateCommandHandlerThrowsException() {
        String aggregateIdentifier = "abc123";
        when(mockRepository.load(eq(aggregateIdentifier), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(asCommandMessage(new FailingUpdateCommand(aggregateIdentifier, "parameter")),
                            new VoidCallback() {
            @Override
            protected void onSuccess() {
                fail("Expected exception");
            }

            @Override
            public void onFailure(Throwable cause) {
                assertEquals("parameter", cause.getMessage());
            }
        });
    }

    @Test
    public void testCommandHandlerSubscribesToCommands() {
        verify(commandBus).subscribe(eq(CreateCommand.class),
                                     any(org.axonframework.commandhandling.CommandHandler.class));
        verify(commandBus).subscribe(eq(UpdateCommandWithAnnotatedMethod.class),
                                     any(org.axonframework.commandhandling.CommandHandler.class));
    }

    @Test
    public void testCommandHandlerCreatesAggregateInstance() {

        commandBus.dispatch(GenericCommandMessage.asCommandMessage(new CreateCommand("Hi")));
        verify(mockRepository).add(isA(StubCommandAnnotatedAggregate.class));
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstance_AnnotatedMethod() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(new UpdateCommandWithAnnotatedMethod("abc123")),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Method works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, null);
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstanceWithCorrectVersion_AnnotatedMethod() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(
                new UpdateCommandWithAnnotatedMethodAndVersion("abc123", 12L)),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Method with version works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, 12L);
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstanceWithNullVersion_AnnotatedMethod() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(
                new UpdateCommandWithAnnotatedMethodAndVersion("abc123", null)),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Method with version works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, null);
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstance_AnnotatedField() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(new UpdateCommandWithAnnotatedField("abc123")),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Field works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, null);
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstanceWithCorrectVersion_AnnotatedField() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(
                new UpdateCommandWithAnnotatedFieldAndVersion("abc123", 321L)),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Field with version works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, 321L);
    }

    @Test
    public void testCommandHandlerUpdatesAggregateInstanceWithCorrectVersion_AnnotatedIntegerField() {
        Object aggregateIdentifier = "abc123";
        when(mockRepository.load(any(Object.class), anyLong()))
                .thenReturn(new StubCommandAnnotatedAggregate(aggregateIdentifier));
        commandBus.dispatch(GenericCommandMessage.asCommandMessage(
                new UpdateCommandWithAnnotatedFieldAndIntegerVersion("abc123", 321)),
                            new CommandCallback<Object>() {
                                @Override
                                public void onSuccess(Object result) {
                                    assertEquals("Field with integer version works fine", result);
                                }

                                @Override
                                public void onFailure(Throwable cause) {
                                    cause.printStackTrace();
                                    fail("Did not expect exception");
                                }
                            });

        verify(mockRepository).load(aggregateIdentifier, 321L);
    }

    private abstract static class AbstractStubCommandAnnotatedAggregate extends AbstractAnnotatedAggregateRoot {

        private final Object identifier;

        public AbstractStubCommandAnnotatedAggregate(Object identifier) {
            this.identifier = identifier;
        }

        @Override
        public Object getIdentifier() {
            return identifier;
        }

        @CommandHandler
        public abstract String handleUpdate(UpdateCommandWithAnnotatedMethod updateCommand);
    }

    private static class StubCommandAnnotatedAggregate extends AbstractStubCommandAnnotatedAggregate {

        @CommandHandler
        public StubCommandAnnotatedAggregate(CreateCommand createCommand, MetaData metaData, UnitOfWork unitOfWork,
                                             @org.axonframework.common.annotation.MetaData("notExist") String value) {
            super(IdentifierFactory.getInstance().generateIdentifier());
            Assert.assertNotNull(unitOfWork);
            Assert.assertNull(value);
            apply(new StubDomainEvent());
        }

        @CommandHandler
        public StubCommandAnnotatedAggregate(FailingCreateCommand createCommand) {
            super(IdentifierFactory.getInstance().generateIdentifier());
            throw new RuntimeException(createCommand.getParameter());
        }

        public StubCommandAnnotatedAggregate(Object aggregateIdentifier) {
            super(aggregateIdentifier);
        }

        @Override
        public String handleUpdate(UpdateCommandWithAnnotatedMethod updateCommand) {
            return "Method works fine";
        }

        @CommandHandler
        public String handleUpdate(UpdateCommandWithAnnotatedMethodAndVersion updateCommand) {
            return "Method with version works fine";
        }

        @CommandHandler
        public String handleUpdate(UpdateCommandWithAnnotatedField updateCommand) {
            return "Field works fine";
        }

        @CommandHandler
        public String handleUpdate(UpdateCommandWithAnnotatedFieldAndVersion updateCommand) {
            return "Field with version works fine";
        }

        @CommandHandler
        public String handleUpdate(UpdateCommandWithAnnotatedFieldAndIntegerVersion updateCommand) {
            return "Field with integer version works fine";
        }

        @CommandHandler
        public void handleFailingUpdate(FailingUpdateCommand updateCommand) {
            throw new RuntimeException(updateCommand.getMessage());
        }
    }

    private static class CreateCommand {

        private String parameter;

        private CreateCommand(String parameter) {
            this.parameter = parameter;
        }

        public String getParameter() {
            return parameter;
        }
    }

    private static class FailingCreateCommand extends CreateCommand {

        private FailingCreateCommand(String parameter) {
            super(parameter);
        }
    }

    private static class UpdateCommandWithAnnotatedMethod {

        private String aggregateIdentifier;

        private UpdateCommandWithAnnotatedMethod(String aggregateIdentifier) {
            this.aggregateIdentifier = aggregateIdentifier;
        }

        @TargetAggregateIdentifier
        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }
    }

    private static class FailingUpdateCommand extends UpdateCommandWithAnnotatedField {

        private final String message;

        private FailingUpdateCommand(String aggregateIdentifier, String message) {
            super(aggregateIdentifier);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    private static class UpdateCommandWithAnnotatedMethodAndVersion {

        private final String aggregateIdentifier;
        private final Long expectedVersion;

        private UpdateCommandWithAnnotatedMethodAndVersion(String aggregateIdentifier, Long expectedVersion) {
            this.aggregateIdentifier = aggregateIdentifier;
            this.expectedVersion = expectedVersion;
        }

        @TargetAggregateIdentifier
        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }

        @TargetAggregateVersion
        public Long getExpectedVersion() {
            return expectedVersion;
        }
    }

    private static class UpdateCommandWithAnnotatedField {

        @TargetAggregateIdentifier
        private final String aggregateIdentifier;

        private UpdateCommandWithAnnotatedField(String aggregateIdentifier) {
            this.aggregateIdentifier = aggregateIdentifier;
        }

        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }
    }

    private static class UpdateCommandWithAnnotatedFieldAndVersion {

        @TargetAggregateIdentifier
        private final String aggregateIdentifier;

        @TargetAggregateVersion
        private final Long expectedVersion;

        private UpdateCommandWithAnnotatedFieldAndVersion(String aggregateIdentifier, Long expectedVersion) {
            this.aggregateIdentifier = aggregateIdentifier;
            this.expectedVersion = expectedVersion;
        }
    }

    private static class UpdateCommandWithAnnotatedFieldAndIntegerVersion {

        @TargetAggregateIdentifier
        private final String aggregateIdentifier;

        @TargetAggregateVersion
        private final int expectedVersion;

        private UpdateCommandWithAnnotatedFieldAndIntegerVersion(String aggregateIdentifier, int expectedVersion) {
            this.aggregateIdentifier = aggregateIdentifier;
            this.expectedVersion = expectedVersion;
        }
    }
}
