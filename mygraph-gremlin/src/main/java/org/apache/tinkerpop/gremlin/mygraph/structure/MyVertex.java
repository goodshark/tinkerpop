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

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

public final class MyVertex extends MyElement implements Vertex {
    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<Edge>> outEdges;
    protected Map<String, Set<Edge>> inEdges;
    private final MyGraph graph;

    protected MyVertex(final Object id, final String label, final MyGraph graph) {
        super(id, label);
        this.graph = graph;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        return MyHelper.addEdge(this.graph, this, (MyVertex) inVertex, label, keyValues);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        final Iterator<Edge> edgeIterator = (Iterator) MyHelper.getEdges(this, direction, edgeLabels);
        return edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return (Iterator) MyHelper.getVertices(this, direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        if (null == this.properties) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList()).iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) return VertexProperty.empty();
        if (this.properties != null && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

            final Object idValue = optionalId.isPresent() ?
                    graph.vertexPropertyIdManager.convert(optionalId.get()) :
                    graph.vertexPropertyIdManager.getNextId(graph);

            final VertexProperty<V> vertexProperty = new MyVertexProperty<>(idValue, this, key, value);

            if (null == this.properties) this.properties = new HashMap<>();
            final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());
            list.add(vertexProperty);
            this.properties.put(key, list);
            MyHelper.autoUpdateIndex(this, key, value, null);
            ElementHelper.attachProperties(vertexProperty, keyValues);
            return vertexProperty;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((MyEdge) edge).removed).forEach(Edge::remove);
        this.properties = null;
        MyHelper.removeElementIndex(this);
        this.graph.vertices.remove(this.id);
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties) return Collections.emptySet();
        return this.properties.keySet();
    }
}
