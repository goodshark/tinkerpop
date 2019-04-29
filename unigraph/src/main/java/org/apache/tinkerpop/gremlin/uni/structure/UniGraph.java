/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.uni.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.uni.structure.utils.UniEdgeIterator;
import org.apache.tinkerpop.gremlin.uni.structure.utils.UniVertexIterator;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Iterator;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public class UniGraph implements Graph {
    public static String REDIS_HOST = "unigraph.redis.host";
    public static String REDIS_PORT = "unigraph.redis.port";
    private Jedis baseStore;

    public static String RECV_BUFFER = "unigraph.redis.buffer";

    protected UniGraphVariables variables = null;
    private UniGraphFeatures features = new UniGraphFeatures();

    private final Configuration configuration;

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, UniGraph.class.getName());
        this.setProperty(REDIS_HOST, "172.17.171.43");
        this.setProperty(REDIS_PORT, 6379);
        this.setProperty(RECV_BUFFER, 200);
    }};

    private UniGraph(final Configuration configuration)  {
        this.configuration = configuration;
        initRedis(configuration);
    }

    private void initRedis(Configuration conf) {
        baseStore = new Jedis(conf.getString(REDIS_HOST), conf.getInt(REDIS_PORT));
    }

    public static UniGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    public static UniGraph open(Configuration configuration) {
        return new UniGraph(configuration);
    }

    public static UniGraph open(String redisHost, int redisPort, int bufSize) {
        EMPTY_CONFIGURATION.setProperty(REDIS_HOST, redisHost);
        EMPTY_CONFIGURATION.setProperty(REDIS_PORT, redisPort);
        EMPTY_CONFIGURATION.setProperty(RECV_BUFFER, bufSize);
        return new UniGraph(EMPTY_CONFIGURATION);
    }

    public Jedis getBaseStore() {
        return baseStore;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        long vId = UniHelper.getVertexId(this);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        Vertex vertex = new UniVertex(vId, label, this);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        UniHelper.createVertex(this, vertex);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Object[] ids = transformId(vertexIds);
        return UniVertexIterator.build(this, ids);
//        return new UniVertexIterator(this, ids, configuration.getInt(RECV_BUFFER));
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Object[] ids = transformId(edgeIds);
        String[] idStrs = Arrays.stream(ids).toArray(String[]::new);
        return UniEdgeIterator.bulid(this, idStrs);
    }

    private Object[] transformId(Object... ids) {
        Object[] transformIds = new Object[ids.length];
        if (ids.length != 0) {
            if (Vertex.class.isAssignableFrom(ids[0].getClass())) {
                for (int i = 0; i < ids.length; i++) {
                    transformIds[i] = Vertex.class.cast(ids[i]).id();
                }
            } else if (Edge.class.isAssignableFrom(ids[0].getClass())) {
                for (int i = 0; i < ids.length; i++) {
                    transformIds[i] = Edge.class.cast(ids[i]).id();
                }
            } else
                transformIds = ids;
        }
        return transformIds;
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new UniGraphVariables();
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "uni vertices:" +
                UniHelper.vertexCount(this) +
                " uni edges:" + UniHelper.edgeCount(this));
    }

    @Override
    public Features features() {
        return features;
    }

    public class UniGraphFeatures implements Features {

        private final UniGraphGraphFeatures graphFeatures = new UniGraphGraphFeatures();
        private final UniGraphEdgeFeatures edgeFeatures = new UniGraphEdgeFeatures();
        private final UniGraphVertexFeatures vertexFeatures = new UniGraphVertexFeatures();

        private UniGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public class UniGraphVertexFeatures implements Features.VertexFeatures {

        private final UniGraphVertexPropertyFeatures vertexPropertyFeatures = new UniGraphVertexPropertyFeatures();

        private UniGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            if (id instanceof String || id instanceof Integer)
                return true;
            else
                return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    public class UniGraphEdgeFeatures implements Features.EdgeFeatures {

        private UniGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            if (id instanceof String || id instanceof Integer)
                return true;
            else
                return false;
        }
    }

    public class UniGraphGraphFeatures implements Features.GraphFeatures {

        private UniGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }

    public class UniGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private UniGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            if (id instanceof String || id instanceof Integer)
                return true;
            else
                return false;
        }
    }
}
