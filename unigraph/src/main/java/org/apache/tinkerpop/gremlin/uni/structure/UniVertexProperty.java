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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UniVertexProperty<V> extends UniElement implements VertexProperty<V> {
    private long id;
    private String key;
    private V value;
    // property on VertexProperty
    private Map<String, Property> properties = new ConcurrentHashMap<>();

    private UniVertex vertex;

    public UniVertexProperty() {
    }

    public UniVertexProperty(final UniVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(UniHelper.getVertexPropertyId((UniGraph) vertex.graph()), key);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public UniVertexProperty(final Object id, final UniVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        this.id = (long)id;
        this.vertex = vertex;
        this.key = key;
        this.value = value;
//        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
//        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public long getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public void bondInfo(Vertex v) {
        super.id = id;
        super.label = key;
        vertex = (UniVertex) v;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public <U> Property<U> property(final String key) {
        return null == this.properties ? Property.<U>empty() : this.properties.getOrDefault(key, Property.<U>empty());
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        if (null == this.properties || properties.size() == 0) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final Property<U> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);
        final Property<V> property = new UniProperty<>(this, key, value);
        this.properties.put(key, property);
        return property;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @JsonIgnore
    @Override
    public boolean isPresent() {
        return true;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
