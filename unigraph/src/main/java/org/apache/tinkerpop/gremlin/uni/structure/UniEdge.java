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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UniEdge extends UniElement implements Edge {
    protected long id;
    protected String label;
    protected Map<String, Property> properties = new ConcurrentHashMap<>();

    protected Vertex outVertex;
    protected Vertex inVertex;

    public UniEdge() {
    }

    protected UniEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
        super(id, label);
        this.id = (long)id;
        this.label = label;
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    public long getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public Vertex getOutVertex() {
        return outVertex;
    }

    public Vertex getInVertex() {
        return inVertex;
    }

    public void bondGraph(UniGraph g) {
        ((UniVertex)inVertex).bondGraph(g);
        ((UniVertex)outVertex).bondGraph(g);
        super.id = id;
        super.label = label;
    }

    public long outVertexId() {
        return (long)outVertex.id();
    }

    public long inVertexId() {
        return (long)inVertex.id();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        if (null == this.properties || properties.size() == 0) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final Property<V> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }

    @Override
    public Graph graph() {
        return inVertex.graph();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
        final Property<V> property = new UniProperty<>(this, key, value);
        properties.put(key, property);
        return property;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return (null == this.properties || this.properties.size() == 0) ? Property.<V>empty() : this.properties.getOrDefault(key, Property.<V>empty());
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static void main(String[] args) throws Exception {
        UniGraph graph = UniGraph.open();
        Vertex ov = new UniVertex(123L, "person", graph);
        Vertex iv = new UniVertex(200L, "softwate", graph);
        ov.property(VertexProperty.Cardinality.list, "name", "marko");
        iv.property(VertexProperty.Cardinality.list, "brand", "hoho");
        Edge e = new UniEdge(100L, ov, "create", iv);
        e.property("time", 1990);
        ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping();
        String res = om.writeValueAsString(e);
        System.out.println("res: " + res);
        UniEdge fe = om.readValue(res, UniEdge.class);
        /*UniVertex fv = om.readValue(res, UniVertex.class);
        for (List<VertexProperty> vps: fv.getProperties().values()) {
            for (VertexProperty vp: vps) {
                UniVertexProperty uvp = (UniVertexProperty) vp;
                uvp.bondInfo(fv);
            }
        }*/
        System.out.println("good");
    }
}
