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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;
import java.util.stream.Stream;

public final class MyHelper {
    public static boolean inComputerMode() {
        return false;
    }

    public static Map<Object, Edge> getEdges(final MyGraph graph) {
        return graph.edges;
    }

    public static Iterator<MyEdge> getEdges(final MyVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        return (Iterator) edges.iterator();
    }

    protected static Edge addEdge(final MyGraph graph, final MyVertex outVertex, final MyVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        edge = new MyEdge(idValue, outVertex, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        MyHelper.addOutEdge(outVertex, label, edge);
        MyHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final MyVertex vertex, final String label, final Edge edge) {
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final MyVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static void autoUpdateIndex(final MyVertex vertex, final String key, final Object newValue, final Object oldValue) {
        final MyGraph graph = (MyGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.autoUpdate(key, newValue, oldValue, vertex);
    }

    public static void autoUpdateIndex(final MyEdge edge, final String key, final Object newValue, final Object oldValue) {
        final MyGraph graph = (MyGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
    }

    public static void removeElementIndex(final MyVertex vertex) {
        final MyGraph graph = (MyGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.removeElement(vertex);
    }

    public static void removeElementIndex(final MyEdge edge) {
        final MyGraph graph = (MyGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.removeElement(edge);
    }

    public static void removeIndex(final MyVertex vertex, final String key, final Object value) {
        final MyGraph graph = (MyGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.remove(key, value, vertex);
    }

    public static void removeIndex(final MyEdge edge, final String key, final Object value) {
        final MyGraph graph = (MyGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.remove(key, value, edge);
    }

    public static Iterator<MyVertex> getVertices(final MyVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((MyEdge) edge).inVertex)));
                else if (edgeLabels.length == 1)
                    vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((MyEdge) edge).inVertex));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((MyEdge) edge).inVertex));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((MyEdge) edge).outVertex)));
                else if (edgeLabels.length == 1)
                    vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((MyEdge) edge).outVertex));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((MyEdge) edge).outVertex));
            }
        }
        return (Iterator) vertices.iterator();
    }
}
