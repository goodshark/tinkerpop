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

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TinyVertex extends TinyElement implements Vertex {

    private final TinyGraph graph;

    protected TinyVertex(final Object id, final String label, final TinyGraph graph) {
        super(id, label);
        this.graph = graph;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        long eId = TinyHelper.getEdgeId(graph);
        TinyEdge edge = new TinyEdge(eId, this, label, inVertex);
        TinyHelper.createEdge(graph, eId, label, this, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        Long idValue = TinyHelper.getVertexPropertyId(this.graph);
        final VertexProperty<V> vertexProperty = new TinyVertextProperty<>(idValue, this, key, value);
        TinyHelper.createVertexProperty((TinyGraph) this.graph(), this, idValue, key, value.toString());
        return vertexProperty;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        List<Edge> list = new ArrayList<>();
        List<String> edgeFullnames = TinyHelper.getVertexEdges(graph(), (Long)id(), direction, edgeLabels);
        for (String edgeFullname: edgeFullnames) {
            String eId = TinyHelper.extractEid(edgeFullname);
            String outVfullname = TinyHelper.getEdgeVertex(graph(), eId, Direction.OUT);
            Vertex outV = new TinyVertex(Long.parseLong(TinyHelper.extractVid(outVfullname)),
                    TinyHelper.extractVlabel(outVfullname),
                    (TinyGraph) graph());
            String inVfullname = TinyHelper.getEdgeVertex(graph(), eId, Direction.IN);
            Vertex inV = new TinyVertex(Long.parseLong(TinyHelper.extractVid(inVfullname)),
                    TinyHelper.extractVlabel(inVfullname),
                    (TinyGraph)graph());
            list.add(new TinyEdge(eId, outV, TinyHelper.extractElabel(edgeFullname), inV));
        }
        return list.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        List<Vertex> list = new ArrayList<>();
        List<String> vStrs = TinyHelper.getVertexVertices(graph(), (Long)id(), direction, edgeLabels);
        for (String vFullname: vStrs) {
            list.add(new TinyVertex(Long.parseLong(TinyHelper.extractVid(vFullname)),
                    TinyHelper.extractVlabel(vFullname),
                    (TinyGraph) graph())
            );
        }
        return list.iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) return VertexProperty.empty();
        List<VertexProperty> list = new ArrayList<>();
        List<String> strs = TinyHelper.getVertexProperty(this, id(), key);
        if (strs.size() == 0)
            return VertexProperty.<V>empty();
        if (strs.size() > 1)
            throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        String vp = strs.get(0);
        String[] vpStrs = vp.split("-");
        list.add(new TinyVertextProperty<>(vpStrs[0], this, vpStrs[1], vpStrs[2]));
        return list.get(0);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();
        List<VertexProperty> list = new ArrayList<>();
        List<String> strs = new ArrayList<>();
        if (propertyKeys.length != 0) {
            for (String propKey : propertyKeys)
                strs.addAll(TinyHelper.getVertexProperty(this, id(), propKey));
        } else {
            // get all VertexProperty belongs to the Vertex
            strs = TinyHelper.getVertexProperty(this, id());
        }
        for (String pkStr : strs) {
            String[] vpStrs = pkStr.split("-");
            list.add(new TinyVertextProperty(vpStrs[0], this, vpStrs[1], vpStrs[2]));
        }
        if (list.size() == 0)
            return Collections.emptyIterator();
        return (Iterator)list.iterator();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
