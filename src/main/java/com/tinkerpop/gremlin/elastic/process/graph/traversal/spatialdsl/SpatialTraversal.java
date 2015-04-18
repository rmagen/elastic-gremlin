package com.tinkerpop.gremlin.elastic.process.graph.traversal.spatialdsl;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by Eliran on 18/4/2015.
 */
public interface SpatialTraversal<S,E> extends GraphTraversal<S,E> {

    public SpatialTraversal<Vertex, Vertex> V(final Object... vertexIds);
    public SpatialTraversal<Edge, Edge> E(final Object... edgeIds) ;

    public default SpatialTraversal<S, String> convexHull(String field) {
        ConvexhullStep convexhullStep = new ConvexhullStep(this, field);
        return (SpatialTraversal) this.asAdmin().addStep(convexhullStep);
    }
    public static <S> SpatialTraversal<S, S> of(final Graph graph) {
        return new DefaultSpatialTraversal<S,S>(graph);
    }



    public class DefaultSpatialTraversal<S, E> extends DefaultGraphTraversal<S, E> implements SpatialTraversal<S, E> {
        private final Graph graph;

        public DefaultSpatialTraversal(final Graph graph) {
            super(Graph.class);
            this.graph = graph;
        }

        public  SpatialTraversal<Vertex, Vertex> V(final Object... vertexIds){
            Traversal t = this;
            return (SpatialTraversal) this.asAdmin().addStep(new GraphStep<>(this, this.graph, Vertex.class, vertexIds));
        }

        public  SpatialTraversal<Edge, Edge> E(final Object... edgeIds) {
            Traversal t = this;
            return (SpatialTraversal) this.asAdmin().addStep(new GraphStep<>(t, this.graph, Edge.class, edgeIds));
        }


    }
}
