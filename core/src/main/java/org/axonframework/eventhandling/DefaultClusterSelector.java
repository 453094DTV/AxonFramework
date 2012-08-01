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

import org.axonframework.eventhandling.Cluster;
import org.axonframework.eventhandling.ClusterSelector;
import org.axonframework.eventhandling.EventListener;
import org.axonframework.eventhandling.SimpleCluster;

/**
 * ClusterSelector implementation that always selects the same cluster. This implementation
 * can serve as delegate for other cluster selectors for event listeners that do not belong to a specific cluster.
 *
 * @author Allard Buijze
 * @since 1.2
 */
public class DefaultClusterSelector implements ClusterSelector {

    private final Cluster defaultCluster;

    /**
     * Initializes the DefaultClusterSelector using a {@link org.axonframework.eventhandling.SimpleCluster}, to which this instance will assign all
     * Event Listeners.
     */
    public DefaultClusterSelector() {
        this.defaultCluster = new SimpleCluster();
    }

    /**
     * Initializes the DefaultClusterSelector to assign the given <code>defaultCluster</code> to each listener.
     *
     * @param defaultCluster The Cluster to assign to each listener
     */
    public DefaultClusterSelector(Cluster defaultCluster) {
        this.defaultCluster = defaultCluster;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation always returns the same instance of {@link SimpleCluster}.
     */
    @Override
    public Cluster selectCluster(EventListener eventListener) {
        return defaultCluster;
    }
}
