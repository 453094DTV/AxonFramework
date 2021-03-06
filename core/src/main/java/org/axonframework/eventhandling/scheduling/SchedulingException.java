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

package org.axonframework.eventhandling.scheduling;

import org.axonframework.common.AxonTransientException;

/**
 * Exception indicating a problem in the Event Scheduling mechanism.
 *
 * @author Allard Buijze
 * @since 0.7
 */
public class SchedulingException extends AxonTransientException {

    private static final long serialVersionUID = -3633716643792480973L;

    /**
     * Initialize a SchedulingException with the given <code>message</code>.
     *
     * @param message The message describing the exception
     */
    public SchedulingException(String message) {
        super(message);
    }

    /**
     * Initialize a SchedulingException with the given <code>message</code> and <code>cause</code>.
     *
     * @param message The message describing the exception
     * @param cause   The cause of this exception
     */
    public SchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
