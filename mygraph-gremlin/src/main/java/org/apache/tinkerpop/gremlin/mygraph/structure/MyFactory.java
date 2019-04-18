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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public final class MyFactory {
    private MyFactory() {
    }

    public static MyGraph createModern() {
        final MyGraph g = getMyGraphWithNumberManager();
        generateModern(g);
        return g;
    }

    public static void generateModern(final MyGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
        final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
        final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
        final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);
    }

    private static MyGraph getMyGraphWithNumberManager() {
        final Configuration conf = getNumberIdManagerConfiguration();
        return MyGraph.open(conf);
    }

    private static Configuration getNumberIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(MyGraph.GREMLIN_MYGRAPH_VERTEX_ID_MANAGER, MyGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(MyGraph.GREMLIN_MYGRAPH_EDGE_ID_MANAGER, MyGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(MyGraph.GREMLIN_MYGRAPH_VERTEX_PROPERTY_ID_MANAGER, MyGraph.DefaultIdManager.LONG.name());
        return conf;
    }
}
