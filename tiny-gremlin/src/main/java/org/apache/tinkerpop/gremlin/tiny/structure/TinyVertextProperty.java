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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public class TinyVertextProperty<V> extends TinyElement implements VertexProperty<V> {
//    protected Map<String, Property> properties;
    private final TinyVertex vertex;
    private final String key;
    private final V value;

    public TinyVertextProperty(final TinyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(TinyHelper.getVertexPropertyId((TinyGraph) vertex.graph()), key);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public TinyVertextProperty(final Object id, final TinyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
//        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
//        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /*@Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }*/

    @Override
    public <U> Property<U> property(final String key) {
        List<String> vpStrs = TinyHelper.getVertexProperty(vertex, vertex.id(), key);
        for (String vpStr: vpStrs) {
            long vpId = Long.parseLong(TinyHelper.extractVPid(vpStr));
            if (vpId == (long)id()) {
                U val = (U)TinyHelper.extractVPval(vpStr);
                return new TinyProperty<>(this, TinyHelper.extractVPkey(vpStr), val);
            }
        }
        return Property.<U>empty();
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);

        // TODO store in base graph
        final Property<U> property = new TinyProperty<>(this, key, value);
        return property;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        List<Property<U>> list = new ArrayList<>();
        List<String> vpStrs = new ArrayList<>();
        if (propertyKeys.length == 0)
            vpStrs = TinyHelper.getVertexProperty(vertex, vertex.id());
        else {
            for (String pKey: propertyKeys)
                vpStrs.addAll(TinyHelper.getVertexProperty(vertex, vertex.id(), pKey));
        }
        if (vpStrs.size() == 0)
            return Collections.emptyIterator();
        for (String vpStr: vpStrs) {
            String key = TinyHelper.extractVPkey(vpStr);
            U val = (U)TinyHelper.extractVPval(vpStr);
            // TODO type ?
            VertexProperty<U> vp = new TinyVertextProperty<>(TinyHelper.extractVPid(vpStr), vertex, key, val);
            list.add(new TinyProperty<>(vp, key, val));
        }
        return list.iterator();
    }
}
