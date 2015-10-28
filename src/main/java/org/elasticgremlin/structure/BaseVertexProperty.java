package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

import java.util.Iterator;

/**
 * The base implementation of the vertex property.
 * @param <V>
 */
public class BaseVertexProperty<V> implements VertexProperty<V> {

    ////////////////////////////////////////////////////////////////////////////
    /// Fields
    /**
     * The vertex this vertex property belongs to.
     */
    private final BaseVertex vertex;

    /**
     * The property key.
     */
    private final String key;

    /**
     * The property value.
     */
    private final V value;

    ////////////////////////////////////////////////////////////////////////////
    /// Constructors

    /**
     * Constructs the base vertex property.
     * @param vertex the vertex.
     * @param key the property key.
     * @param value the property value.
     */
    public BaseVertexProperty(final BaseVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    ////////////////////////////////////////////////////////////////////////////
    /// Methods
    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    }

    @Override
    public void remove() {
        vertex.removeProperty(this);
    }
}
