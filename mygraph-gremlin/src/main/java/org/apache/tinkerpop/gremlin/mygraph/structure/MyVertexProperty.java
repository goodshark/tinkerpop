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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class MyVertexProperty<V> extends MyElement implements VertexProperty<V> {
    protected Map<String, Property> properties;
    private final MyVertex vertex;
    private final String key;
    private final V value;

    public MyVertexProperty(final MyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(((MyGraph) vertex.graph()).vertexPropertyIdManager.getNextId((MyGraph) vertex.graph()), key);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public MyVertexProperty(final Object id, final MyVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
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

    @Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return null == this.properties ? Property.<U>empty() : this.properties.getOrDefault(key, Property.<U>empty());
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);
        final Property<U> property = new MyProperty<>(this, key, value);
        if (this.properties == null) this.properties = new HashMap<>();
        this.properties.put(key, property);
        return property;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        if (null != this.vertex.properties && this.vertex.properties.containsKey(this.key)) {
            this.vertex.properties.get(this.key).remove(this);
            if (this.vertex.properties.get(this.key).size() == 0) {
                this.vertex.properties.remove(this.key);
                MyHelper.removeIndex(this.vertex, this.key, this.value);
            }
            final AtomicBoolean delete = new AtomicBoolean(true);
            this.vertex.properties(this.key).forEachRemaining(property -> {
                if (property.value().equals(this.value))
                    delete.set(false);
            });
            if (delete.get()) MyHelper.removeIndex(this.vertex, this.key, this.value);
            this.properties = null;
            this.removed = true;
        }
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        if (null == this.properties) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final Property<U> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }
}
