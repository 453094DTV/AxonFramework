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

package org.axonframework.common.annotation;

import org.axonframework.common.Assert;
import org.axonframework.domain.Message;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

/**
 * Abstract superclass for annotation based Message handlers. Handlers can be compared with on another to decide upon
 * their priority. Handlers that deal with unrelated payloads (i.e. have no parent-child relationship) are ordered
 * based on their payload type's class name.
 * <p/>
 * Handler invokers should always evaluate the first (smallest) suitable handler before evaluating the next.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public abstract class AbstractMessageHandler implements Comparable<AbstractMessageHandler> {

    private final Score score;
    private final Class payloadType;
    private final ParameterResolver[] parameterValueResolvers;

    /**
     * Initializes the MessageHandler to handle messages with the given <code>payloadType</code>, declared in the given
     * <code>declaringClass</code> and using the given <code>parameterValueResolvers</code>.
     *
     * @param payloadType             The type of payload this handlers deals with
     * @param declaringClass          The class on which the handler is declared
     * @param parameterValueResolvers The resolvers for each of the handlers' parameters
     */
    protected AbstractMessageHandler(Class<?> payloadType, Class<?> declaringClass,
                                     ParameterResolver... parameterValueResolvers) {
        score = new Score(payloadType, declaringClass);
        this.payloadType = payloadType;
        this.parameterValueResolvers = Arrays.copyOf(parameterValueResolvers, parameterValueResolvers.length);
    }

    /**
     * Indicates whether this Handler is suitable for the given <code>message</code>.
     *
     * @param message The message to inspect
     * @return <code>true</code> if this handler can handle the message, otherwise <code>false</code>.
     */
    public boolean matches(Message message) {
        Assert.notNull(message, "Event may not be null");
        for (ParameterResolver parameterResolver : parameterValueResolvers) {
            if (!parameterResolver.matches(message)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Invokes this handler for the given <code>target</code> instance, using the given <code>message</code> as
     * source object to provide parameter values.
     *
     * @param target  The target instance to invoke the Handler on.
     * @param message The message providing parameter values
     * @return The result of the handler invocation
     *
     * @throws InvocationTargetException when the handler throws a checked exception
     * @throws IllegalAccessException    if the SecurityManager refuses the handler invocation
     */
    public abstract Object invoke(Object target, Message message)
            throws InvocationTargetException, IllegalAccessException;

    /**
     * Returns the type of payload this handler expects.
     *
     * @return the type of payload this handler expects
     */
    public Class getPayloadType() {
        return payloadType;
    }

    @Override
    public int compareTo(AbstractMessageHandler o) {
        return score.compareTo(o.score);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof AbstractMessageHandler)
                && ((AbstractMessageHandler) obj).score.equals(score);
    }

    @Override
    public int hashCode() {
        return score.hashCode();
    }

    /**
     * Finds ParameterResolvers for the given Member details. The returning array contains as many elements as the
     * given <code>parameterTypes</code>, where each ParameterResolver corresponds with the parameter type at the same
     * location.
     *
     * @param memberAnnotations    The annotations on the member (e.g. method)
     * @param parameterTypes       The parameter type of the member
     * @param parameterAnnotations The annotations on each of the parameters
     * @return the parameter resolvers for the given Member details
     *
     * @see java.lang.reflect.Method
     * @see java.lang.reflect.Constructor
     */
    protected static ParameterResolver[] findResolvers(Annotation[] memberAnnotations, Class<?>[] parameterTypes,
                                                       Annotation[][] parameterAnnotations) {
        int parameters = parameterTypes.length;
        ParameterResolver[] parameterValueResolvers = new ParameterResolver[parameters];
        for (int i = 0; i < parameters; i++) {
            // currently, the first parameter is considered the payload parameter
            final boolean isPayloadParameter = i == 0;
            parameterValueResolvers[i] = ParameterResolverFactory.findParameterResolver(memberAnnotations,
                                                                                        parameterTypes[i],
                                                                                        parameterAnnotations[i],
                                                                                        isPayloadParameter);
        }
        if (parameterValueResolvers[0] == null) {
            parameterValueResolvers[0] = ParameterResolverFactory.createPayloadResolver(parameterTypes[0]);
        }

        return parameterValueResolvers;
    }

    /**
     * Returns the parameter resolvers provided at construction time.
     *
     * @return the parameter resolvers provided at construction time
     */
    protected ParameterResolver[] getParameterValueResolvers() {
        return parameterValueResolvers;
    }

    private static final class Score implements Comparable<Score> {

        private final int declarationDepth;
        private final int payloadDepth;
        private final String payloadName;

        private Score(Class payloadType, Class<?> declaringClass) {
            declarationDepth = superClassCount(declaringClass, 0);
            payloadDepth = superClassCount(payloadType, -255);
            payloadName = payloadType.getName();
        }

        private int superClassCount(Class<?> declaringClass, int interfaceScore) {
            if (declaringClass.isInterface()) {
                return interfaceScore;
            }
            int superClasses = 0;

            while (declaringClass != null) {
                superClasses++;
                declaringClass = declaringClass.getSuperclass();
            }
            return superClasses;
        }

        @Override
        public int compareTo(Score o) {
            if (declarationDepth != o.declarationDepth) {
                return o.declarationDepth - declarationDepth;
            } else if (payloadDepth != o.payloadDepth) {
                return o.payloadDepth - payloadDepth;
            } else {
                return payloadName.compareTo(o.payloadName);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Score score = (Score) o;

            if (declarationDepth != score.declarationDepth) {
                return false;
            }
            if (payloadDepth != score.payloadDepth) {
                return false;
            }
            if (!payloadName.equals(score.payloadName)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = declarationDepth;
            result = 31 * result + payloadDepth;
            result = 31 * result + payloadName.hashCode();
            return result;
        }
    }
}
