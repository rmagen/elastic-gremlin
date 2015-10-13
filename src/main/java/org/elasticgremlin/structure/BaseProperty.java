package org.elasticgremlin.structure;


import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;

/**
 * The base implementation of the property.
 * @param <V>
 */
public class BaseProperty<V> implements Property<V> {
    ////////////////////////////////////////////////////////////////////////////
    /// Fields
    /**
     * The element the property belongs to.
     */
    protected final BaseElement element;

    /**
     * The property key.
     */
    protected final String key;

    /**
     * The property value.
     */
    protected V value;

    /**
     * The graph.
     */
    protected final ElasticGraph graph;

    ////////////////////////////////////////////////////////////////////////////
    /// Constructors

    /**
     * Constructs the base property.
     * @param element the element the property belongs to.
     * @param key the property key.
     * @param value the property value.
     */
    public BaseProperty(BaseElement element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = this.element.graph;
    }

    ////////////////////////////////////////////////////////////////////////////
    /// Methods
    @Override
    public Element element() {
        return this.element;
    }

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
        return null != this.value;
    }

    /**
     * Returns the string representation of the property.
     * @return the string representation of the property.
     */
    public String toString() {
        return StringFactory.propertyString(this);
    }

    /**
     * The equals method to compare with another property.
     * @param object the property to compare with.
     * @return comparison result. True if equal, false otherwise.
     */
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * The hashcode method for the base property implementation.
     * @return the hashcode of the property.
     */
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        element.removeProperty(this);
    }
}
