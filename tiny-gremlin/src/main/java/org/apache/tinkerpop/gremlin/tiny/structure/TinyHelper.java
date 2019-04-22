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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.stream.Stream;

public class TinyHelper {
    private final static String VERTEX_ID_PERFIX = "vertex-id";
    private final static String EDGE_ID_PERFIX = "edge-id";
    private final static String VERTEX_PROPERTY_ID_PERFIX = "vertex-property-id";

    private final static String VERTEX_MAP = "vertices";
    private final static String EDGE_MAP = "edges";

    private final static String VP_LIST_SUFFIX = "vplist";

    public static String extractVid(String vName) {
        return vName.split("-")[1];
    }

    public static String extractVlabel(String vName) {
        return vName.split("-")[2];
    }

    public static String extractEid(String eName) {
        return eName.split("-")[1];
    }

    public static String extractElabel(String eName) {
        return eName.split("-")[2];
    }

    public static String extractVPid(String vpName) {
        return vpName.split("-")[0];
    }

    public static String extractVPkey(String vpName) {
        return vpName.split("-")[1];
    }

    public static String extractVPval(String vpName) {
        return vpName.split("-")[2];
    }

    public static long getVerticesSize(Graph g) {
        TinyGraph graph = (TinyGraph) g;
        return graph.getBaseStore().hlen(VERTEX_MAP);
    }

    public static long getEdgesSize(Graph g) {
        TinyGraph graph = (TinyGraph) g;
        return graph.getBaseStore().hlen(EDGE_MAP);
    }

    // generate ID
    public static long getVertexId(TinyGraph graph) {
        return graph.getBaseStore().incr(VERTEX_ID_PERFIX);
    }

    public static long getEdgeId(TinyGraph graph) {
        return graph.getBaseStore().incr(EDGE_ID_PERFIX);
    }

    public static long getVertexPropertyId(TinyGraph graph) {
        return graph.getBaseStore().incr(VERTEX_PROPERTY_ID_PERFIX);
    }

    // get vertex & edge
    public static List<String> vertices(TinyGraph graph, Object... ids) {
        if (ids.length == 0)
            return new ArrayList<>(graph.getBaseStore().hgetAll(VERTEX_MAP).values());
        String[] idStrs = new String[ids.length];
        for (int i = 0; i < ids.length; i++)
            idStrs[i] = ids[i].toString();
        return graph.getBaseStore().hmget(VERTEX_MAP, idStrs);
    }

    public static List<String> edges(Graph g, Object... eIds) {
        TinyGraph graph = (TinyGraph) g;
        if (eIds.length == 0)
            return new ArrayList<>(graph.getBaseStore().hgetAll(EDGE_MAP).values());
        String[] eIdStrs = new String[eIds.length];
        for (int i = 0; i < eIds.length; i++)
            eIdStrs[i] = eIds[i].toString();
        return graph.getBaseStore().hmget(EDGE_MAP, eIdStrs);
    }

    public static List<String> getVertexProperty(Vertex v, Object id, String key) {
        TinyGraph graph = (TinyGraph) v.graph();
        String vertextFullName = graph.getBaseStore().hget(VERTEX_MAP, id.toString());
        String[] strs = vertextFullName.split("-");
        String vertexPropertiesKey = "v-" + strs[1] + "-properties";
        String vpListName = graph.getBaseStore().hget(vertexPropertiesKey, key);
        List<String> vPstrList = new ArrayList<>();
        if (vpListName != null)
            vPstrList = graph.getBaseStore().lrange(vpListName, 0, -1);
        return vPstrList;
    }

    public static List<String> getVertexProperty(Vertex v, Object id) {
        List<String> allVps = new ArrayList<>();
        TinyGraph graph = (TinyGraph) v.graph();
        String vertextFullName = graph.getBaseStore().hget(VERTEX_MAP, id.toString());
        String vId = TinyHelper.extractVid(vertextFullName);
        String vertexPropertiesKey = "v-" + vId + "-properties";
        Map<String, String> vpKeyList = graph.getBaseStore().hgetAll(vertexPropertiesKey);
        for (String redList: vpKeyList.values()) {
            List<String> vpStrList = graph.getBaseStore().lrange(redList, 0, -1);
            allVps.addAll(vpStrList);
        }
        return allVps;
    }

    public static Map<String, String> getProperty(Edge e, Object id, String... keys) {
        TinyGraph graph = (TinyGraph) e.graph();
        if (keys.length != 0) {
            Map<String, String> res = new HashMap<>();
            List<String> valList = graph.getBaseStore().hmget("e-" + id.toString() + "-properties", keys);
            for (int i = 0; i < keys.length; i++) {
                res.put(keys[i], valList.get(i));
            }
            return res;
        } else
            return graph.getBaseStore().hgetAll("e-"+id.toString()+"-properties");
    }

    public static List<String> getVertexEdges(Graph g, Long vId, Direction dir, String... labels) {
        List<String> edges = new ArrayList<>();
        TinyGraph graph = (TinyGraph)g;
        if (dir.equals(Direction.BOTH)) {
            edges.addAll(TinyHelper.getVertexDirEdges(graph, vId, Direction.IN.toString().toLowerCase(), labels));
            edges.addAll(TinyHelper.getVertexDirEdges(graph, vId, Direction.OUT.toString().toLowerCase(), labels));
        } else
            edges.addAll(TinyHelper.getVertexDirEdges(graph, vId, dir.toString().toLowerCase(), labels));
        return edges;
    }

    public static List<String> getVertexDirEdges(TinyGraph g, Long vId, String dir, String... labels) {
        List<String> edges = new ArrayList<>();
        String vDirName = "v-" + vId.toString() + "-" + dir;
        Set<String> eLabels;
        if (labels.length == 0) {
            eLabels = g.getBaseStore().smembers(vDirName);
        } else {
            eLabels = new HashSet<>(Arrays.asList(labels));
        }
        for (String eLabel : eLabels) {
            Set<String> eIds = g.getBaseStore().smembers(vDirName + "-" + eLabel);
            for (String eId : eIds) {
                edges.add("e-" + eId + "-" + eLabel);
            }
        }
        return edges;
    }

    public static List<String> getVertexVertices(Graph g, Long vId, Direction dir, String... edgeLabels) {
        TinyGraph graph = (TinyGraph) g;
        List<String> vertices = new ArrayList<>();
        if (dir.equals(Direction.OUT)) {
            vertices = TinyHelper.getOutVertices(graph, vId, edgeLabels);
        } else if (dir.equals(Direction.IN)) {
            vertices = TinyHelper.getInVertices(graph, vId, edgeLabels);
        } else {
            vertices.addAll(TinyHelper.getOutVertices(graph, vId, edgeLabels));
            vertices.addAll(TinyHelper.getInVertices(graph, vId, edgeLabels));
        }
        return vertices;
    }

    public static List<String> getOutVertices(TinyGraph g, Long vId, String... edgeLabels) {
        List<String> vertices = new ArrayList<>();
        List<String> edgeIds = new ArrayList<>();
        List<String> tmpEdgeLabels = new ArrayList<>(Arrays.asList(edgeLabels));
        if (edgeLabels.length == 0) {
            Set<String> eLabels = g.getBaseStore().smembers("v-"+vId+"-out");
            tmpEdgeLabels = new ArrayList<>(eLabels);
        }
        for (String eLabel: tmpEdgeLabels) {
            Set<String> eIds = g.getBaseStore().smembers("v-"+vId+"-"+"out-"+eLabel);
            edgeIds.addAll(eIds);
        }
        for (String eId: edgeIds) {
            String eOutVid = g.getBaseStore().get("e-"+eId+"-in");
            vertices.add(g.getBaseStore().hget(VERTEX_MAP, eOutVid));
        }
        return vertices;
    }

    public static List<String> getInVertices(TinyGraph g, Long vId, String... edgeLabels) {
        List<String> vertices = new ArrayList<>();
        List<String> edgeIds = new ArrayList<>();
        List<String> tmpEdgeLabels = new ArrayList<>(Arrays.asList(edgeLabels));
        if (edgeLabels.length == 0) {
            Set<String> eLabels = g.getBaseStore().smembers("v-"+vId+"-in");
            tmpEdgeLabels = new ArrayList<>(eLabels);
        }
        for (String eLabel: tmpEdgeLabels) {
            Set<String> eIds = g.getBaseStore().smembers("v-"+vId+"-"+"in-"+eLabel);
            edgeIds.addAll(eIds);
        }
        for (String eId: edgeIds) {
            String eInVid = g.getBaseStore().get("e-"+eId+"-out");
            vertices.add(g.getBaseStore().hget(VERTEX_MAP, eInVid));
        }
        return vertices;
    }

    public static String getEdgeVertex(Graph g, String eId, Direction dir) {
        TinyGraph graph = (TinyGraph) g;
        String vId = graph.getBaseStore().get("e-"+eId+"-"+dir.toString().toLowerCase());
        return graph.getBaseStore().hget(VERTEX_MAP, vId);
    }

    // store vertex & edge
    public static void createVertex(TinyGraph graph, Vertex v, String label, Object... kv) {
        final String vertexId = String.valueOf((Long)v.id());
        final String vertexName = "v-" + vertexId + "-" + label;
        graph.getBaseStore().hset(VERTEX_MAP, vertexId, vertexName);
    }

    public static void createVertexProperty(TinyGraph graph, Vertex v, Long vpId, String key, String val) {
        final String vertexName = "v-" + v.id();
        final String vProperties = vertexName + "-properties";
        final String vpName = vertexName + "-" + key + "-" + VP_LIST_SUFFIX;
        graph.getBaseStore().hset(vProperties, key, vpName);
        final String vpVal = vpId.toString() + "-" + key + "-" +  val;
        graph.getBaseStore().rpush(vpName, vpVal);
    }


    public static void createEdge(TinyGraph graph, Long eId, String label, Vertex outV, Vertex inV) {
        final String edgeName = "e-" + eId + "-" + label;
        graph.getBaseStore().hset(EDGE_MAP, eId.toString(), edgeName);
        final String outVertexId = outV.id().toString();
        graph.getBaseStore().set("e-"+eId+"-out", outVertexId);
        final String inVertexId = inV.id().toString();
        graph.getBaseStore().set("e-"+eId+"-in", inVertexId);
        TinyHelper.createOutEdge(graph, outV, eId, label);
        TinyHelper.createInEdge(graph, inV, eId, label);
    }

    public static void createProperty(TinyGraph graph, Long eId, String key, Object value) {
        graph.getBaseStore().hset("e-"+eId+"-properties", key, value.toString());
    }

    public static void createOutEdge(TinyGraph graph, Vertex v, Long eId, String eLabel) {
        graph.getBaseStore().sadd("v-"+v.id().toString()+"-out", eLabel);
        graph.getBaseStore().sadd("v-"+v.id().toString()+"-out-"+eLabel, eId.toString());
    }

    public static void createInEdge(TinyGraph graph, Vertex v, Long eId, String eLabel) {
        graph.getBaseStore().sadd("v-"+v.id().toString()+"-in", eLabel);
        graph.getBaseStore().sadd("v-"+v.id().toString()+"-in-"+eLabel, eId.toString());
    }
}
