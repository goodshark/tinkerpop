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

    public static TinyGraph createModern() {
        final TinyGraph g = getMyGraphWithNumberManager();
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

    private static TinyGraph getMyGraphWithNumberManager() {
        final Configuration conf = getEmptyConfiguration();
        return TinyGraph.open(conf);
    }

    private static Configuration getEmptyConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinyGraph.class.getName());
        return conf;
    }
}
