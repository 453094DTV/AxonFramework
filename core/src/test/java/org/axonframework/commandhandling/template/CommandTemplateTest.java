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

package org.axonframework.commandhandling.template;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandExecutionException;
import org.axonframework.commandhandling.CommandMessage;
import org.junit.*;
import org.mockito.invocation.*;
import org.mockito.stubbing.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 */
public class CommandTemplateTest {

    private CommandBus mockCommandBus;
    private CommandTemplate testSubject;

    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        executorService = Executors.newSingleThreadExecutor();
        mockCommandBus = mock(CommandBus.class);
        testSubject = new CommandTemplate(mockCommandBus);
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void testSendAndWait_ReturnsValue() throws Exception {
        onCommandReturn("returnValue", 10);
        String actual = testSubject.sendAndWait(new Object());
        assertEquals("returnValue", actual);
    }

    @Test
    public void testSendAndWait_ThrowsException() throws Exception {
        onCommandThrow(new StubException(), 0);
        try {
            testSubject.sendAndWait(new Object());
        } catch (CommandExecutionException e) {
            assertTrue(e.getCause() instanceof StubException);
        }
    }

    @Test
    public void testSendAndWaitWithTimeout_ReturnsValue() throws Exception {
        onCommandReturn("returnValue", 0);
        String actual = testSubject.sendAndWait(new Object(), 1000, TimeUnit.MILLISECONDS);
        assertEquals("returnValue", actual);
    }

    @Test(expected = StubRuntimeException.class)
    public void testSendAndWaitWithTimeout_ThrowsException() throws Exception {
        onCommandThrow(new StubRuntimeException(), 0);
        testSubject.sendAndWait(new Object(), 1000, TimeUnit.MILLISECONDS);
    }

    @Test(expected = TimeoutException.class)
    public void testSendAndWaitWithTimeout_Timeout() throws Exception {
        onCommandReturn("returnValue", 1000);
        testSubject.sendAndWait(new Object(), 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSend() throws Exception {
        onCommandReturn("returnValue", 100);
        Future<String> actual = testSubject.send(new Object());
        try {
            actual.get(10, TimeUnit.MILLISECONDS);
            fail("Expected timeout");
        } catch (TimeoutException e) {
            // expected
        }
        assertEquals("returnValue", actual.get());
    }

    @SuppressWarnings("unchecked")
    private void onCommandReturn(final String returnValue, final long timeOutMillis) {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final CommandCallback callback = (CommandCallback) invocation.getArguments()[1];
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(timeOutMillis);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            fail("Unexpected exception");
                        }
                        callback.onSuccess(returnValue);
                    }
                });
                return null;
            }
        }).when(mockCommandBus).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    @SuppressWarnings("unchecked")
    private void onCommandThrow(final Throwable exception, final long timeOutMillis) {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final CommandCallback callback = (CommandCallback) invocation.getArguments()[1];
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(timeOutMillis);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            fail("Unexpected exception");
                        }
                        callback.onFailure(exception);
                    }
                });
                return null;
            }
        }).when(mockCommandBus).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    private class StubException extends Exception {

    }

    private class StubRuntimeException extends RuntimeException {

    }
}
