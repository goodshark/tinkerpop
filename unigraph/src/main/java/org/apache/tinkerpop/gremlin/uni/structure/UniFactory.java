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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class UniFactory {
    private UniFactory() {
    }

    public static UniGraph getGraph() {
        return getUniGraphWithConf(null, null);
    }

    public static UniGraph getGraph(String host, int port) {
        return getUniGraphWithConf(host, port);
    }

    public static UniGraph createModern() {
        final UniGraph g = getUniGraphWithConf(null, null);
        generateModern(g);
        return g;
    }

    public static UniGraph createModern(String host, int port) {
        final UniGraph g = getUniGraphWithConf(host, port);
        generateModern(g);
        return g;
    }

    public static void generateModern(final UniGraph g) {
        final Vertex marko = g.addVertex(T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, "weight", 0.5d);
        marko.addEdge("knows", josh, "weight", 1.0d);
        marko.addEdge("created", lop, "weight", 0.4d);
        josh.addEdge("created", ripple, "weight", 1.0d);
        josh.addEdge("created", lop, "weight", 0.4d);
        peter.addEdge("created", lop, "weight", 0.2d);
    }

    private static UniGraph getUniGraphWithConf(String host, Integer port) {
        Configuration conf;
        if (host != null && port != null)
            conf = getConfiguration(host, port);
        else
            conf = getEmptyConfiguration();
        return UniGraph.open(conf);
    }

    private static Configuration getEmptyConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty(UniGraph.REDIS_HOST, "localhost");
        conf.setProperty(UniGraph.REDIS_PORT, 6379);
        conf.setProperty(UniGraph.RECV_BUFFER, 500);
        return conf;
    }

    private static Configuration getConfiguration(String host, Integer port) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, UniGraph.class.getName());
        conf.setProperty(UniGraph.REDIS_HOST, host);
        conf.setProperty(UniGraph.REDIS_PORT, port);
        conf.setProperty(UniGraph.RECV_BUFFER, 500);
        return conf;
    }
}
