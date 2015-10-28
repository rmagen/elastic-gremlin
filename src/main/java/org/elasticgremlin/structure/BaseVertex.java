package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.ElasticMutations;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;

/**
 * The base vertex implementation.
 */
public abstract class BaseVertex extends BaseElement implements Vertex {

    ////////////////////////////////////////////////////////////////////////////
    /// Fields
    /**
     * The elastic mutation for the graph.
     */
    private final ElasticMutations elasticMutations;

    /**
     * A map containing edge query info as the key and a set of found edge as the value.
     */
    private HashMap<EdgeQueryInfo, Set<Edge>> queriedEdges = new HashMap<>();

    /**
     * A list of vertex siblings.
     */
    protected List<BaseVertex> siblings;

    ////////////////////////////////////////////////////////////////////////////
    /// Constructors

    /**
     * Constructs the vertex.
     * @param id the id.
     * @param label the label.
     * @param graph the graph.
     * @param keyValues the key value pair of properties.
     * @param elasticMutations the elastic mutation.
     */
    protected BaseVertex(Object id, String label, ElasticGraph graph, Object[] keyValues, ElasticMutations elasticMutations) {
        super(id, label, graph, keyValues);
        this.elasticMutations = elasticMutations;
    }

    ////////////////////////////////////////////////////////////////////////////
    /// Methods
    @Override
    public Property createProperty(String key, Object value) {
        return new BaseVertexProperty(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... propertyKeys) {
        checkRemoved();
        if(propertyKeys != null && propertyKeys.length > 0) throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        return this.property(key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return edges(direction, edgeLabels, new Predicates());
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return vertices(direction, edgeLabels, new Predicates());
    }

    /**
     * Adds vertices to the other end of the edge defined by the direction, label and predicates.
     * @param direction the direction of the edge.
     * @param edgeLabels the label of the edge.
     * @param predicates the predicates for the edge.
     * @return iterator of the added vertices.
     */
    public Iterator<Vertex> vertices(Direction direction, String[] edgeLabels, Predicates predicates) {
        checkRemoved();
        Iterator<Edge> edgeIterator = edges(direction, edgeLabels, predicates);
        ArrayList<Vertex> vertices = new ArrayList<>();
        if (edgeIterator != null) {
            edgeIterator.forEachRemaining(edge ->
                    vertices.add(vertexToVertex(this, edge, direction)));
        }
        return vertices.iterator();
    }

    /**
     * Sets the siblings.
     *
     * @param siblings the vertex siblings.
     */
    public void setSiblings(List<BaseVertex> siblings) {
        this.siblings = siblings;
    }

    /**
     * Applies lazy fields.
     *
     * @param response
     */
    public void applyLazyFields(MultiGetItemResponse response) {
        setLabel(response.getType());
        response.getResponse().getSource().entrySet().forEach((field) ->
                addPropertyLocal(field.getKey(), field.getValue()));
    }

    /**
     * Gets the vertex of the edge according to edge direction.
     *
     * @param originalVertex the vertex where the edge originates from.
     * @param edge the edge.
     * @param direction the direction.
     * @return the vertex.
     */
    public static Vertex vertexToVertex(Vertex originalVertex, Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if(outV.id().equals(inV.id()))
                    return originalVertex; //points to self
                if(originalVertex.id().equals(inV.id()))
                    return outV;
                if(originalVertex.id().equals(outV.id()))
                    return inV;
            default:
                throw new IllegalArgumentException(direction.toString());
        }
    }


    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        BaseVertexProperty vertexProperty = (BaseVertexProperty) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    /**
     * TBA
     * @param vertexProperty the inner property to be added.
     */
    protected abstract void innerAddProperty(BaseVertexProperty vertexProperty);

    @Override
    public <V> VertexProperty<V> property(final String key) {
        checkRemoved();
        if (this.properties.containsKey(key)) {
            return (VertexProperty<V>) this.properties.get(key);
        }
        else return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        checkRemoved();
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        return graph.getQueryHandler().addEdge(idValue, label, this, vertex, keyValues);
    }

    @Override
    public void remove() {
        super.remove();
        Iterator<Edge> edges = edges(Direction.BOTH);
        edges.forEachRemaining(edge-> {
            edge.remove();
        });
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
    }

    /**
     * Gets the iterator of edges surrounding the vertex defined by the direction, label and predicates.
     * @param direction the direction.
     * @param edgeLabels the labels.
     * @param predicates the predicates.
     * @return the edge iterator.
     */
    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        EdgeQueryInfo queryInfo = new EdgeQueryInfo(direction, edgeLabels, predicates, elasticMutations.getRevision());
        Set<Edge> edges = queriedEdges.get(queryInfo);
        if (edges != null)  return edges.iterator();

        List<BaseVertex> vertices = siblings == null ? IteratorUtils.asList(this) : siblings;

        Map<Object, Set<Edge>> vertexToEdge = graph.getQueryHandler().edges(vertices.iterator(), direction, edgeLabels, predicates);
        vertices.forEach( vertex -> vertex.addQueriedEdges(queryInfo, vertexToEdge.get(vertex.id())));

        Set<Edge> thisEdges = vertexToEdge.get(this.id());
        return thisEdges != null ? thisEdges.iterator() : Collections.emptyIterator();
    }

    /**
     * Adds queried edges to the queried edges map.
     * @param queryInfo as the key.
     * @param edges as the value.
     */
    private void addQueriedEdges(EdgeQueryInfo queryInfo, Set<Edge> edges) {
        queriedEdges.put(queryInfo, edges);
    }

    ////////////////////////////////////////////////////////////////////////////
    /// Inner classes

    /**
     * Edge query info.
     */
    private static class EdgeQueryInfo {

        ////////////////////////////////////////////////////////////////////////////
        /// Fields
        /**
         * The direction of the edge.
         */
        private Direction direction;

        /**
         * The label of the edge.
         */
        private String[] edgeLabels;

        /**
         * The predicates used to filter the edge.
         */
        private Predicates predicates;

        /**
         * The revision of the elastic mutation.
         */
        private int revision;

        ////////////////////////////////////////////////////////////////////////////
        /// Constructors

        /**
         * Constructs the edge query info object.
         * @param direction the direction of the edge.
         * @param edgeLabels the label of the edge.
         * @param predicates the predicates for filtering the edge.
         * @param revision the elastic mutation revision.
         */
        public EdgeQueryInfo(Direction direction, String[] edgeLabels, Predicates predicates, int revision) {
            this.direction = direction;
            this.edgeLabels = edgeLabels;
            this.predicates = predicates;
            this.revision = revision;
        }

        /**
         * Gets the direction.
         * @return the direction.
         */
        public Direction getDirection() {
            return direction;
        }

        /**
         * Gets the edge labels.
         * @return the edge labels.
         */
        public String[] getEdgeLabels() {
            return edgeLabels;
        }

        /**
         * Gets the predicates.
         * @return the predicates.
         */
        public Predicates getPredicates() {
            return predicates;
        }

        // region equals and hashCode

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            EdgeQueryInfo that = (EdgeQueryInfo) o;

            if (revision != that.revision) return false;
            if (direction != that.direction) return false;
            if (!Arrays.equals(edgeLabels, that.edgeLabels)) return false;
            return predicates.equals(that.predicates);

        }

        @Override
        public int hashCode() {
            int result = direction.hashCode();
            result = 31 * result + Arrays.hashCode(edgeLabels);
            result = 31 * result + predicates.hashCode();
            result = 31 * result + revision;
            return result;
        }


        // endregion
    }
}
