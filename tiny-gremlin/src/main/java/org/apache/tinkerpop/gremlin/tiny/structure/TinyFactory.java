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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class TinyFactory {
    private TinyFactory() {
    }

    public static TinyGraph getGraph() {
        return getTinyGraphWithConf(null, null);
    }

    public static TinyGraph getGraph(String host, int port) {
        return getTinyGraphWithConf(host, port);
    }

    public static TinyGraph createModern() {
        final TinyGraph g = getTinyGraphWithConf(null, null);
        generateModern(g);
        return g;
    }

    public static TinyGraph createModern(String host, int port) {
        final TinyGraph g = getTinyGraphWithConf(host, port);
        generateModern(g);
        return g;
    }

    public static void generateModern(final TinyGraph g) {
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

    private static TinyGraph getTinyGraphWithConf(String host, Integer port) {
        Configuration conf;
        if (host != null && port != null)
            conf = getConfiguration(host, port);
        else
            conf = getEmptyConfiguration();
        return TinyGraph.open(conf);
    }

    private static Configuration getEmptyConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinyGraph.class.getName());
        conf.setProperty(TinyGraph.REDIS_HOST, "localhost");
        conf.setProperty(TinyGraph.REDIS_PORT, 6379);
        return conf;
    }

    private static Configuration getConfiguration(String host, Integer port) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinyGraph.class.getName());
        conf.setProperty(TinyGraph.REDIS_HOST, host);
        conf.setProperty(TinyGraph.REDIS_PORT, port);
        return conf;
    }
}
