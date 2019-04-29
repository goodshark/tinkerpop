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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.uni.structure.utils.UniEdgeIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;

public class UniHelper {
    private final static String VERTEX_ID_PERFIX = "uni-vertex-id";
    private final static String EDGE_ID_PERFIX = "uni-edge-id";
    private final static String VERTEX_PROPERTY_ID_PERFIX = "uni-vertex-property-id";

    private final static String VERTEX_MAP = "uni-vertices";
    private final static String EDGE_MAP = "uni-edges";
    private final static String VERTEX_IT_LIST = "uni-vertices-list";
    private final static String EDGE_IT_LIST = "uni-edges-list";

    private final static ObjectMapper om = new ObjectMapper();

    // generate ID
    public static long getVertexId(UniGraph graph) {
        return graph.getBaseStore().incr(VERTEX_ID_PERFIX);
    }

    public static long getEdgeId(UniGraph graph) {
        return graph.getBaseStore().incr(EDGE_ID_PERFIX);
    }

    public static long getVertexPropertyId(UniGraph graph) {
        return graph.getBaseStore().incr(VERTEX_PROPERTY_ID_PERFIX);
    }

    // serialize
    public static String serializeVertex(Vertex v) {
        String vStr = "";
        try {
            om.enableDefaultTyping();
            vStr = om.writeValueAsString(v);
        } catch (JsonProcessingException e) {
            System.err.println(e);
        }
        return vStr;
    }

    public static UniVertex deserializeVertex(String vStr) {
        UniVertex v = null;
        try {
            // TODO restore some info ?
            v = om.readValue(vStr, UniVertex.class);
        } catch (IOException e) {
            System.err.println(e);
        }
        return v;
    }

    public static String serializeEdge(Edge edge) {
        String eStr = "";
        try {
            om.enableDefaultTyping();
            eStr = om.writeValueAsString(edge);
        } catch (JsonProcessingException e) {
            System.err.println(e);
        }
        return eStr;
    }

    public static UniEdge deserializeEdge(String eStr) {
        UniEdge edge = null;
        try {
            edge = om.readValue(eStr, UniEdge.class);
        } catch (IOException e) {
            System.err.println(e);
        }
        return edge;
    }

    // store Vertex & Edge
    public static boolean createVertex(UniGraph graph, Vertex v) {
        final String vertexId = String.valueOf(v.id());
        String vStr = UniHelper.serializeVertex(v);
        if (vStr.isEmpty())
            return false;
        graph.getBaseStore().hset(VERTEX_MAP, vertexId, vStr);
        graph.getBaseStore().rpush(VERTEX_IT_LIST, vertexId);
        return true;
    }

    public static void createVertexOutEdge(UniGraph graph, String outVertexId, String eLabel, String eId, String inVertexId) {
        long res = graph.getBaseStore().sadd("v-"+outVertexId+"-out", eLabel);
        if (res == 1)
            graph.getBaseStore().rpush("v-"+outVertexId+"-out-list", eLabel);
        graph.getBaseStore().rpush("v-"+outVertexId+"-out-"+eLabel, eId);
        // fast locate vertex out adjacent the vertices
        graph.getBaseStore().rpush("vv-"+outVertexId+"-out", inVertexId);
        graph.getBaseStore().rpush("vv-"+outVertexId+"-out-"+eLabel, inVertexId);
    }

    public static void createVertexInEdge(UniGraph graph, String inVertexId, String eLabel, String eId, String outVertexId) {
        long res = graph.getBaseStore().sadd("v-"+inVertexId+"-in", eLabel);
        if (res == 1)
            graph.getBaseStore().rpush("v-"+inVertexId+"-in-list", eLabel);
        graph.getBaseStore().rpush("v-"+inVertexId+"-in-"+eLabel, eId);
        // fast locate vertex in adjacent the vertices
        graph.getBaseStore().rpush("vv-"+inVertexId+"-in", outVertexId);
        graph.getBaseStore().rpush("vv-"+inVertexId+"-in-"+eLabel, outVertexId);
    }

    public static boolean createEdge(UniGraph graph, Edge e) {
        String eStr = UniHelper.serializeEdge(e);
        if (eStr.isEmpty())
            return false;
        graph.getBaseStore().hset(EDGE_MAP, e.id().toString(), eStr);
        graph.getBaseStore().rpush(EDGE_IT_LIST, e.id().toString());
        String edgeOutKey = "e-" + e.id() + "-out";
        String outVertexId = Long.toString(((UniEdge)e).outVertexId());
        graph.getBaseStore().set(edgeOutKey, outVertexId);
        String edgeInKey = "e-" + e.id() + "-in";
        String inVertexId = Long.toString(((UniEdge)e).inVertexId());
        graph.getBaseStore().set(edgeInKey, inVertexId);
        createVertexOutEdge(graph, outVertexId, e.label(), e.id().toString(), inVertexId);
        createVertexInEdge(graph, inVertexId, e.label(), e.id().toString(), outVertexId);
        return true;
    }

    // read Vertex & Edge
    public static long vertexCount(UniGraph graph) {
        return graph.getBaseStore().hlen(VERTEX_MAP);
    }

    public static long getVertexVerticesCount(UniGraph graph, String vId, Direction direction, String... eLabels) {
        long cnt = 0;
        if (direction.equals(Direction.BOTH)) {
            if (eLabels.length == 0) {
                cnt += graph.getBaseStore().llen("vv-"+vId+"-in");
                cnt += graph.getBaseStore().llen("vv-"+vId+"-out");
            } else {
                for (String label: eLabels) {
                    cnt += graph.getBaseStore().llen("vv-"+vId+"-in-"+label);
                    cnt += graph.getBaseStore().llen("vv-"+vId+"-out-"+label);
                }
            }
        } else {
            if (eLabels.length == 0)
                cnt = graph.getBaseStore().llen("vv-"+vId+"-"+direction.toString().toLowerCase());
            else {
                for (String label : eLabels) {
                    cnt += graph.getBaseStore().llen("vv-"+vId+"-"+direction.toString().toLowerCase()+"-"+label);
                }
            }
        }
        return cnt;
    }

    public static List<String> getVertexVertices(UniGraph graph, String vId, Direction direction, String edgeLabel) {
        List<String> vIds = new ArrayList<>();
        List<String> vStrs = new ArrayList<>();
        // get vIds
        if (direction.equals(Direction.BOTH)) {
            vIds.addAll(graph.getBaseStore().lrange("vv-"+vId+"-in-"+edgeLabel, 0, -1));
            vIds.addAll(graph.getBaseStore().lrange("vv-"+vId+"-out-"+edgeLabel, 0, -1));
        } else {
            vIds.addAll(graph.getBaseStore().lrange("vv-"+vId+"-"+direction.toString().toLowerCase()+"-"+edgeLabel, 0, -1));
        }
        // get vStrs
        String[] tmp = vIds.stream().toArray(String[]::new);
        vStrs.addAll(graph.getBaseStore().hmget(VERTEX_MAP, tmp));
        return vStrs;
    }


    public static long edgeCount(UniGraph graph) {
        return graph.getBaseStore().hlen(EDGE_MAP);
    }

    public static long edgeCountBasedLabel(UniGraph graph, String vId, Direction direction, String... eLabels) {
        long cnt = 0;
        List<String> labelList = eLabels.length == 0 ? UniHelper.getVertexEdgeLabels(graph, vId, direction) : Arrays.asList(eLabels);
        for (String label: labelList) {
            switch (direction) {
                case IN:
                    cnt += graph.getBaseStore().llen("v-"+vId+"-in-"+label);
                    break;
                case OUT:
                    cnt += graph.getBaseStore().llen("v-"+vId+"-out-"+label);
                    break;
                case BOTH:
                    cnt += graph.getBaseStore().llen("v-"+vId+"-in-"+label);
                    cnt += graph.getBaseStore().llen("v-"+vId+"-out-"+label);
                    break;
            }
        }
        return cnt;
    }

    public static List<String> rangeVertices(UniGraph graph, int start, int end) {
        List<String> ids = graph.getBaseStore().lrange(VERTEX_IT_LIST, start, end);
        String[] tmp = ids.stream().toArray(String[]::new);
        return graph.getBaseStore().hmget(VERTEX_MAP, tmp);
    }

    public static List<String> multiVertices(UniGraph graph, Object... ids) {
//        String[] tmp = Arrays.stream(ids).toArray(String[]::new);
        String[] tmp = Arrays.stream(ids).map(id -> id.toString()).toArray(String[]::new);
        return graph.getBaseStore().hmget(VERTEX_MAP, tmp);
    }

    public static List<String> rangeEdges(UniGraph graph, int start, int end) {
        List<String> ids = graph.getBaseStore().lrange(EDGE_IT_LIST, start, end);
        String[] tmp = ids.stream().toArray(String[]::new);
        return graph.getBaseStore().hmget(EDGE_MAP, tmp);
    }

    public static List<String> multiEdges(UniGraph graph, String... ids) {
        String[] tmp = Arrays.stream(ids).toArray(String[]::new);
        return graph.getBaseStore().hmget(EDGE_MAP, tmp);
    }

    public static Iterator<Edge> getVertexEdges(UniGraph graph, String vId, Direction direction, String... edgeLabels) {
        Iterator<Edge> iterator = UniEdgeIterator.bulid(graph, vId, direction, edgeLabels);
        return iterator;
    }

    public static List<String> getVertexEdgeLabels(UniGraph graph, String vId, Direction direction) {
        List<String> list = new ArrayList<>();
        if (direction.equals(Direction.BOTH)) {
            list.addAll(graph.getBaseStore().smembers("v-"+vId+"-in"));
            list.addAll(graph.getBaseStore().smembers("v-"+vId+"-out"));
        } else {
            list.addAll(graph.getBaseStore().smembers("v-"+vId+"-"+direction.toString().toLowerCase()));
        }
        return list;
    }

    public static List<String> getVertexLabeledEdges(UniGraph graph, String vId, Direction direction, String label) {
        List<String> list = new ArrayList<>();
        switch (direction) {
            case IN:
                return getInEdges(graph, vId, label);
            case OUT:
                return getOutEdges(graph, vId, label);
            case BOTH:
                list.addAll(getInEdges(graph, vId, label));
                list.addAll(getOutEdges(graph, vId, label));
                return list;
        }
        return list;
    }

    public static List<String> getOutEdges(UniGraph graph, String vId, String label) {
        List<String> edgeIds = graph.getBaseStore().lrange("v-"+vId+"-out-"+label, 0, -1);
        if (edgeIds == null || edgeIds.size() == 0)
            return new ArrayList<>();
        String[] tmp = edgeIds.stream().toArray(String[]::new);
        return graph.getBaseStore().hmget(EDGE_MAP, tmp);
    }

    public static List<String> getInEdges(UniGraph graph, String vId, String label) {
        List<String> edgeIds = graph.getBaseStore().lrange("v-"+vId+"-in-"+label, 0, -1);
        if (edgeIds == null || edgeIds.size() == 0)
            return new ArrayList<>();
        String[] tmp =  edgeIds.stream().toArray(String[]::new);
        return graph.getBaseStore().hmget(EDGE_MAP, tmp);
    }


}
