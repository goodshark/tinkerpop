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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.uni.structure.utils.UniEdgeIterator;
import org.apache.tinkerpop.gremlin.uni.structure.utils.UniVertexIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UniVertex extends UniElement implements Vertex {
    private long id;
    private String label;
    private Map<String, List<VertexProperty>> properties = new ConcurrentHashMap<>();

    private UniGraph graph;

    public UniVertex() {
    }

    protected UniVertex(final Object id, final String label, final UniGraph graph) {
        super(id, label);
        this.id = (long)id;
        this.label = label;
        this.graph = graph;
    }

    public long getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, List<VertexProperty>> getProperties() {
        return properties;
    }

    public void bondGraph(UniGraph g) {
        graph = g;
        super.id = id;
        super.label = label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        long eId = UniHelper.getEdgeId(graph);
        UniEdge edge = new UniEdge(eId, this, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        UniHelper.createEdge(graph, edge);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        Long idValue = UniHelper.getVertexPropertyId(this.graph);
        final VertexProperty<V> vertexProperty = new UniVertexProperty<>(idValue, this, key, value);
        List<VertexProperty> list = properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        properties.put(key, list);
        return vertexProperty;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return UniHelper.getVertexEdges(graph, id().toString(), direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return UniVertexIterator.build(graph, Long.toString(getId()), direction, edgeLabels);
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (this.removed) return VertexProperty.empty();
        if (this.properties != null && this.properties.containsKey(key)) {
            final List<VertexProperty> list = this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
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
    public String toString() {
        return StringFactory.vertexString(this);
    }

    public static void main(String[] args) throws Exception {
        UniGraph graph = UniGraph.open();
        Vertex v = new UniVertex(123L, "person", graph);
        v.property(VertexProperty.Cardinality.list, "name", "marko");
        ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping();
        String res = om.writeValueAsString(v);
        System.out.println("res: " + res);
        UniVertex fv = om.readValue(res, UniVertex.class);
        for (List<VertexProperty> vps: fv.getProperties().values()) {
            for (VertexProperty vp: vps) {
                UniVertexProperty uvp = (UniVertexProperty) vp;
                uvp.bondInfo(fv);
            }
        }
        System.out.println("good");
    }
}
