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
package org.apache.tinkerpop.gremlin.uni.structure.utils;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.uni.structure.UniGraph;
import org.apache.tinkerpop.gremlin.uni.structure.UniHelper;
import org.apache.tinkerpop.gremlin.uni.structure.UniVertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UniVertexIterator implements Iterator<Vertex> {
    private UniGraph graph;

    private Object[] ids;
    private int bufSize;

    private String vId;
    private Direction direction;
    private String[] edgeLabels;

    private int i = 0;
    private long acc = 0;
    private long total;
    private int curStart;
    private int curLabelIndex = 0;
    // buffer
    private List<Vertex> vertices = new ArrayList<>();

    public UniVertexIterator(UniGraph g, int bSize, Object[] ids) {
        this.graph = g;
        this.ids = ids;
        this.bufSize = bSize;
        initTotal();
    }

    public UniVertexIterator(UniGraph g, int bSize, String vId, Direction direction, String... eLabels) {
        this.graph = g;
        this.bufSize = bSize;
        this.vId = vId;
        this.direction = direction;
        if (eLabels.length == 0)
            this.edgeLabels = UniHelper.getVertexEdgeLabels(graph, vId, direction).stream().toArray(String[]::new);
        else
            this.edgeLabels = eLabels;
    }

    public static UniVertexIterator build(UniGraph g, Object... ids) {
        return new UniVertexIterator(g, g.getConfiguration().getInt(g.RECV_BUFFER), ids);
    }

    public static UniVertexIterator build(UniGraph g, String vId, Direction direction, String... eLabels) {
        UniVertexIterator iterator = new UniVertexIterator(g, g.getConfiguration().getInt(g.RECV_BUFFER), vId, direction, eLabels);
        iterator.initTotal();
        return iterator;
    }

    private void initTotal() {
        if (ids != null)
            total = ids.length != 0 ? ids.length : UniHelper.vertexCount(graph);
        else {
            total = UniHelper.getVertexVerticesCount(graph, vId, direction, edgeLabels);
        }
        loadData();
    }

    private void loadData() {
        i = 0;
        vertices.clear();
        List<String> vStrs = new ArrayList<>();
        if (ids != null) {
            if (ids.length == 0) {
                vStrs = UniHelper.rangeVertices(graph, curStart, curStart + bufSize);
            } else {
                vStrs = UniHelper.multiVertices(graph, ids);
            }
            fillVertex(vStrs);
            curStart = curStart + bufSize + 1;
        } else {
            while (curLabelIndex < edgeLabels.length) {
                vStrs = UniHelper.getVertexVertices(graph, vId, direction, edgeLabels[curLabelIndex]);
                fillVertex(vStrs);
                curLabelIndex++;
                if (vertices.size() >= bufSize)
                    break;
            }
        }
    }

    private void fillVertex(List<String> vStrs) {
        for (String vStr : vStrs) {
            UniVertex v = UniHelper.deserializeVertex(vStr);
            if (v != null)
                v.bondGraph(graph);
                vertices.add(v);
        }
    }

    @Override
    public boolean hasNext() {
        return acc < total;
    }

    @Override
    public Vertex next() {
        if (i >= vertices.size())
            loadData();
        acc++;
        return vertices.get(i++);
    }
}
