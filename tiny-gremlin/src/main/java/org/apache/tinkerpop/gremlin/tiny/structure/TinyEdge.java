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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

public class TinyEdge extends TinyElement implements Edge {
    protected final Vertex inVertex;
    protected final Vertex outVertex;

    protected TinyEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
        super(id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        String vFullname;
        String[] vStrs;
        switch (direction) {
            case OUT:
                vFullname = TinyHelper.getEdgeVertex(graph(), id().toString(), Direction.OUT);
                vStrs = vFullname.split("-");
                return IteratorUtils.of(new TinyVertex(Long.parseLong(vStrs[1]), vStrs[2], (TinyGraph) graph()));
            case IN:
                vFullname = TinyHelper.getEdgeVertex(graph(), id().toString(), Direction.IN);
                vStrs = vFullname.split("-");
                return IteratorUtils.of(new TinyVertex(Long.parseLong(vStrs[1]), vStrs[2], (TinyGraph) graph()));
            default:
                vFullname = TinyHelper.getEdgeVertex(graph(), id().toString(), Direction.OUT);
                vStrs = vFullname.split("-");
                Vertex outV = new TinyVertex(Long.parseLong(vStrs[1]), vStrs[2], (TinyGraph) graph());
                vFullname = TinyHelper.getEdgeVertex(graph(), id().toString(), Direction.IN);
                vStrs = vFullname.split("-");
                Vertex inV = new TinyVertex(Long.parseLong(vStrs[1]), vStrs[2], (TinyGraph) graph());
                return IteratorUtils.of(outV, inV);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Property<V>> list = new ArrayList<>();
        Map<String, String> edgeKvs = TinyHelper.getProperty(this, id(), propertyKeys);
        for (String k: edgeKvs.keySet()) {
            V value = (V) edgeKvs.get(k);
            list.add(new TinyProperty<>(this, k, value));
        }
        /*List<String> edgeValList = TinyHelper.getProperty(this, id(), propertyKeys);
        for (int i = 0; i < propertyKeys.length; i++) {
            V value = (V)edgeValList.get(i);
            list.add(new TinyProperty<>(this, propertyKeys[i], value));
        }*/
        if (list.size() == 0)
            return Collections.emptyIterator();
        return list.iterator();
    }

    @Override
    public Graph graph() {
        return inVertex.graph();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        ElementHelper.validateProperty(key, value);
        final Property<V> property = new TinyProperty(this, key, value);
        TinyHelper.createProperty((TinyGraph) graph(), (Long)id(), key, value);
        return property;
    }

    // TODO cache
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public void remove() {
    }
}
