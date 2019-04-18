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
package org.apache.tinkerpop.gremlin.mygraph.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public final class MyGraph implements Graph {
    public static final Logger LOGGER = LoggerFactory.getLogger(MyGraph.class);

    public static final String GREMLIN_MYGRAPH_VERTEX_ID_MANAGER = "gremlin.mygraph.vertexIdManager";
    public static final String GREMLIN_MYGRAPH_EDGE_ID_MANAGER = "gremlin.mygraph.edgeIdManager";
    public static final String GREMLIN_MYGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.mygraph.vertexPropertyIdManager";
    public static final String GREMLIN_MYGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.mygraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_MYGRAPH_GRAPH_LOCATION = "gremlin.mygraph.graphLocation";
    public static final String GREMLIN_MYGRAPH_GRAPH_FORMAT = "gremlin.mygraph.graphFormat";

    private final Configuration configuration;


    protected AtomicLong currentId = new AtomicLong(-1L);
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected MyGraphVariables variables = null;
    protected MyIndex<MyVertex> vertexIndex = null;
    protected MyIndex<MyEdge> edgeIndex = null;

    protected final IdManager<?> vertexIdManager;
    protected final IdManager<?> edgeIdManager;
    protected final IdManager<?> vertexPropertyIdManager;
    protected final VertexProperty.Cardinality defaultVertexPropertyCardinality;

    private final String graphLocation;
    private final String graphFormat;

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, MyGraph.class.getName());
    }};

    private MyGraph(final Configuration configuration) {
        this.configuration = configuration;

        this.vertexIdManager = selectIdManager(configuration, GREMLIN_MYGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        this.edgeIdManager = selectIdManager(configuration, GREMLIN_MYGRAPH_EDGE_ID_MANAGER, Edge.class);
        this.vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_MYGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);

        this.defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
                configuration.getString(GREMLIN_MYGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single.name()));

        this.graphLocation = configuration.getString(GREMLIN_MYGRAPH_GRAPH_LOCATION, null);
        this.graphFormat = configuration.getString(GREMLIN_MYGRAPH_GRAPH_FORMAT, null);
    }

    public static MyGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    public static MyGraph open(Configuration configuration) {
        return new MyGraph(configuration);
    }


    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }

        final Vertex vertex = new MyVertex(idValue, label, this);
        this.vertices.put(vertex.id(), vertex);

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
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            return elements.values().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(idManager.convert(id))).iterator(), Objects::nonNull);
        }
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
        LOGGER.info("close graph");
    }

    @Override
    public Variables variables() {
        if (null == this.variables)
            this.variables = new MyGraphVariables();
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "own vertices:" + this.vertices.size() + " own edges:" + this.edges.size());
    }

    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        final String vertexIdManagerConfigValue = config.getString(configKey, DefaultIdManager.ANY.name());
        try {
            return DefaultIdManager.valueOf(vertexIdManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(vertexIdManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure TinkerGraph %s id manager with %s", clazz.getSimpleName(), vertexIdManagerConfigValue));
            }
        }
    }

    public interface IdManager<T> {
        T getNextId(final MyGraph graph);

        T convert(final Object id);

        boolean allow(final Object id);
    }

    public enum DefaultIdManager implements IdManager {

        LONG {
            @Override
            public Long getNextId(final MyGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String)
                    return Long.parseLong((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        INTEGER {
            @Override
            public Integer getNextId(final MyGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String)
                    return Integer.parseInt((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Integer but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        UUID {
            @Override
            public java.util.UUID getNextId(final MyGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else if (id instanceof String)
                    return java.util.UUID.fromString((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof UUID || id instanceof String;
            }
        },

        ANY {
            @Override
            public Long getNextId(final MyGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }

            @Override
            public boolean allow(final Object id) {
                return true;
            }
        }
    }

    public class MyGraphFeatures implements Features {

        private final MyGraphGraphFeatures graphFeatures = new MyGraphGraphFeatures();
        private final MyGraphEdgeFeatures edgeFeatures = new MyGraphEdgeFeatures();
        private final MyGraphVertexFeatures vertexFeatures = new MyGraphVertexFeatures();

        private MyGraphFeatures() {
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

    public class MyGraphVertexFeatures implements Features.VertexFeatures {

        private final MyGraphVertexPropertyFeatures vertexPropertyFeatures = new MyGraphVertexPropertyFeatures();

        private MyGraphVertexFeatures() {
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
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class MyGraphEdgeFeatures implements Features.EdgeFeatures {

        private MyGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class MyGraphGraphFeatures implements Features.GraphFeatures {

        private MyGraphGraphFeatures() {
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

    public class MyGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private MyGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }


}
