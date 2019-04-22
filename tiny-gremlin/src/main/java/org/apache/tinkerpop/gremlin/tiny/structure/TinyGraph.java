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
package org.apache.tinkerpop.gremlin.tiny.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public final class TinyGraph implements Graph {
    public static final Logger LOGGER = LoggerFactory.getLogger(TinyGraph.class);

    public static String REDIS_HOST = "tinygraph.redis.host";
    public static String REDIS_PORT = "tinygraph.redis.port";

    private Jedis baseStore;

    protected TinyGraphVariables variables = null;
    private final Configuration configuration;

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, TinyGraph.class.getName());
        this.setProperty(REDIS_HOST, "localhost");
        this.setProperty(REDIS_PORT, 6379);
    }};

    private TinyGraph(final Configuration configuration) {
        this.configuration = configuration;
        initRedis(configuration);
    }

    private void initRedis(Configuration conf) {
        baseStore = new Jedis(conf.getString(REDIS_HOST), conf.getInt(REDIS_PORT));
    }

    public static TinyGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    public static TinyGraph open(Configuration configuration) {
        return new TinyGraph(configuration);
    }

    public Jedis getBaseStore() {
        return baseStore;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        long vId = TinyHelper.getVertexId(this);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        Vertex vertex = new TinyVertex(vId, label, this);
        TinyHelper.createVertex(this, vertex, label, keyValues);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
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
        Object[] transformIds = new Object[vertexIds.length];
        if (vertexIds.length != 0) {
            final List<Object> idList = Arrays.asList(vertexIds);
            validateHomogenousIds(idList);
            // vertexIds may be the Vertex itself
            if (Vertex.class.isAssignableFrom(vertexIds[0].getClass())) {
                for (int i = 0; i < vertexIds.length; i++) {
                    transformIds[i] = Vertex.class.cast(vertexIds[i]).id();
                }
            } else
                transformIds = vertexIds;
        }
        List<Vertex> vertices = new ArrayList<>();
        List<String> vertexStrs = TinyHelper.vertices(this, transformIds);
        for (String vName: vertexStrs) {
            String[] strs = vName.split("-");
            long vId = Long.parseLong(strs[1]);
            String vLabel = strs[2];
            vertices.add(new TinyVertex(vId, vLabel, this));
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        List<Edge> edges = new ArrayList<>();
        List<String> edgeFullnameList = TinyHelper.edges(this, edgeIds);
        for (String eName: edgeFullnameList) {
            String[] strs = eName.split("-");
            String outVname = TinyHelper.getEdgeVertex(this, strs[1], Direction.OUT);
            String[] outVstr = outVname.split("-");
            Vertex oV = new TinyVertex(Long.parseLong(outVstr[1]), outVstr[2], this);
            String inVname = TinyHelper.getEdgeVertex(this, strs[1], Direction.IN);
            String[] inVstr = inVname.split("-");
            Vertex iV = new TinyVertex(Long.parseLong(inVstr[1]), inVstr[2], this);
            edges.add(new TinyEdge(strs[1], oV, strs[2], iV));
        }
        return edges.iterator();
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("close TinyGraph");
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new TinyGraphVariables();
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    public class TinyGraphFeatures implements Features {

        private final TinyGraphGraphFeatures graphFeatures = new TinyGraphGraphFeatures();
        private final TinyGraphEdgeFeatures edgeFeatures = new TinyGraphEdgeFeatures();
        private final TinyGraphVertexFeatures vertexFeatures = new TinyGraphVertexFeatures();

        private TinyGraphFeatures() {
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

    public class TinyGraphVertexFeatures implements Features.VertexFeatures {

        private final TinyGraphVertexPropertyFeatures vertexPropertyFeatures = new TinyGraphVertexPropertyFeatures();

        private TinyGraphVertexFeatures() {
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

    public class TinyGraphEdgeFeatures implements Features.EdgeFeatures {

        private TinyGraphEdgeFeatures() {
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

    public class TinyGraphGraphFeatures implements Features.GraphFeatures {

        private TinyGraphGraphFeatures() {
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

    public class TinyGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private TinyGraphVertexPropertyFeatures() {
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

    // TODO cache
    @Override
    public String toString() {
        return StringFactory.graphString(this, "tiny vertices:" +
                TinyHelper.getVerticesSize(this) +
                " tiny edges:" + TinyHelper.getEdgesSize(this));
    }
}
