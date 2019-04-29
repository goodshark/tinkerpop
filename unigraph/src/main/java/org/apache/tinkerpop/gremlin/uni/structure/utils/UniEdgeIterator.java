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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.uni.structure.UniEdge;
import org.apache.tinkerpop.gremlin.uni.structure.UniGraph;
import org.apache.tinkerpop.gremlin.uni.structure.UniHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO tx need lock
public class UniEdgeIterator implements Iterator<Edge> {
    private UniGraph graph;
    private String[] ids;
    private String vId;
    private String[] labels;
    private int bufSize;
    private Direction direction;

    private int i;
    private long acc;
    private long total;
    private List<Edge> edges = new ArrayList<>();

    private int curLabelIndex = 0;
    private int curStart = 0;

    public UniEdgeIterator(UniGraph g, int bufSize) {
        this.graph = g;
        this.bufSize = bufSize;
    }

    // find edges from vertex based by edge label or not
    public static UniEdgeIterator bulid(UniGraph g, String vId, Direction direction, String... eLabels) {
        UniEdgeIterator iterator = new UniEdgeIterator(g, g.getConfiguration().getInt(g.RECV_BUFFER));
        iterator.initLabels(vId, direction, eLabels);
        return iterator;
    }

    // find edges through edge id
    public static UniEdgeIterator bulid(UniGraph g, String... ids) {
        UniEdgeIterator iterator = new UniEdgeIterator(g, g.getConfiguration().getInt(g.RECV_BUFFER));
        iterator.initIds(ids);
        return iterator;
    }

    private void initLabels(String vId, Direction direction, String... labels) {
        this.vId = vId;
        this.direction = direction;
        if (labels.length != 0)
            this.labels = labels;
        else {
            List<String> list = UniHelper.getVertexEdgeLabels(graph, vId, direction);
            this.labels = list.stream().toArray(String[]::new);
        }
        this.total = UniHelper.edgeCountBasedLabel(graph, vId, direction, labels);
        loadData();
    }

    private void initIds(String... ids) {
        this.ids = ids;
        this.total = ids.length != 0 ? ids.length : UniHelper.edgeCount(graph);
        loadData();
    }

    private void loadData() {
        i = 0;
        edges.clear();
        if (vId != null) {
            while (curLabelIndex < labels.length) {
                List<String> edgeStrs = UniHelper.getVertexLabeledEdges(graph, vId, direction, labels[curLabelIndex]);
                fillEdges(edgeStrs);
                curLabelIndex++;
                if (edges.size() >= bufSize)
                    break;
            }
        } else {
            List<String> idEdgeStrs = new ArrayList<>();
            if (ids.length != 0) {
                idEdgeStrs = UniHelper.multiEdges(graph, ids);
            } else {
                idEdgeStrs = UniHelper.rangeEdges(graph, curStart, curStart+bufSize);
            }
            fillEdges(idEdgeStrs);
            curStart += bufSize;
        }
    }

    private void fillEdges(List<String> edgeStrs) {
        for (String edgeStr: edgeStrs) {
            UniEdge edge = UniHelper.deserializeEdge(edgeStr);
            if (edge != null)
                edge.bondGraph(graph);
                edges.add(edge);
        }
    }

    @Override
    public boolean hasNext() {
        return acc < total;
    }

    @Override
    public Edge next() {
        if (i >= edges.size())
            loadData();
        acc++;
        return edges.get(i++);
    }
}
