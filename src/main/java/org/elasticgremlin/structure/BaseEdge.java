package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

import java.util.*;

/**
 * The base implementation of the edge.
 */
public abstract class BaseEdge extends BaseElement implements Edge {

    ////////////////////////////////////////////////////////////////////////////
    /// Fields
    /**
     * The in-vertex, where the edge directs to.
     */
    protected Vertex inVertex;

    /**
     * The out-vertex, where the edge originates from.
     */
    protected Vertex outVertex;

    ////////////////////////////////////////////////////////////////////////////
    /// Constructors

    /**
     * Constructs the base edge.
     * @param id the id.
     * @param label the label.
     * @param keyValues the key-value pair properties.
     * @param outVertex the out-vertex, where the edge originates from.
     * @param inVertex the in-vertex, where the edge directs to.
     * @param graph the graph.
     */
    public BaseEdge(final Object id, final String label, Object[] keyValues, Vertex outVertex, Vertex inVertex, final ElasticGraph graph) {
        super(id, label, graph, keyValues);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
        ElementHelper.validateLabel(label);
    }

    ////////////////////////////////////////////////////////////////////////////
    /// Methods
    @Override
    public  Property createProperty(String key, Object value) {
        return new BaseProperty<>(this, key, value);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        checkRemoved();
        ElementHelper.validateProperty(key, value);
        BaseProperty<V> vertexProperty = (BaseProperty<V>) addPropertyLocal(key, value);
        innerAddProperty(vertexProperty);
        return vertexProperty;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        checkRemoved();
        ArrayList<Vertex> vertices = new ArrayList<>();
        if(direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(outVertex);
        if(direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(inVertex);
        return vertices.iterator();
    }

    /**
     * TBA
     * @param vertexProperty the vertex property.
     */
    protected abstract void innerAddProperty(BaseProperty vertexProperty);

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        checkRemoved();
        return innerPropertyIterator(propertyKeys);
    }

    @Override
    protected void checkRemoved() {
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
