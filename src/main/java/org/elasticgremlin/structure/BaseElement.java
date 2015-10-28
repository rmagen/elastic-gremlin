package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * The base elastic-gremlin implementation of the Tinkerpop {@link Element} interface.
 * This is the base class for both {@link BaseVertex} and {@link BaseEdge}.
 */
public abstract class BaseElement implements Element{

    ////////////////////////////////////////////////////////////////////////////
    /// Fields
    /**
     * Properties of the element.
     */
    protected HashMap<String, Property> properties = new HashMap<>();

    /**
     * Element Id.
     */
    protected final Object id;

    /**
     * Element label.
     */
    protected String label;

    /**
     * Graph the element belongs to.
     */
    protected final ElasticGraph graph;

    /**
     * Removed flag.
     */
    protected boolean removed = false;

    ////////////////////////////////////////////////////////////////////////////
    /// Constructors

    /**
     * Constructs the element.
     *
     * @param id the element Id.
     * @param label the element label.
     * @param graph the graph.
     * @param keyValues the property key-value pairs.
     */
    public BaseElement(final Object id, final String label, ElasticGraph graph, Object[] keyValues) {
        this.graph = graph;
        this.id = id != null ? id.toString() : new com.eaio.uuid.UUID().toString();
        this.label = label;
        if(keyValues != null) ElementHelper.legalPropertyKeyValueArray(keyValues);

        if (keyValues != null) {
            if(keyValues.length % 2 == 1) throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
            for (int i = 0; i < keyValues.length; i = i + 2) {
                String key = keyValues[i].toString();
                Object value = keyValues[i + 1];

                addPropertyLocal(key, value);
            }
        }

    }

    ////////////////////////////////////////////////////////////////////////////
    /// Methods

    /**
     * Adds the property to the local properties map.
     *
     * @param key the key.
     * @param value the value.
     * @return the property.
     */
    public Property addPropertyLocal(String key, Object value) {
        checkRemoved();
        if (shouldAddProperty(key)) {
            ElementHelper.validateProperty(key, value);
            Property property = createProperty(key, value);
            properties.put(key, property);
            return property;
        }
        return null;
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Set<String> keys() {
        return this.properties.keySet();
    }

    @Override
    public <V> Property<V> property(final String key) {
        checkRemoved();
        return this.properties.containsKey(key) ? this.properties.get(key) : Property.<V>empty();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Gets the iterator of inner property by property keys.
     *
     * @param propertyKeys the property keys.
     * @return the iterator.
     */
    protected Iterator innerPropertyIterator(String[] propertyKeys) {
        HashMap<String, Property> properties = (HashMap<String, Property>) this.properties.clone();

        if (propertyKeys.length > 0)
            return properties.entrySet().stream()
                    .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                    .map(Map.Entry::getValue)
                    .iterator();

        return properties.values().iterator();
    }

    /**
     * Removes a property.
     *
     * @param property the property to be removed.
     */
    public void removeProperty(Property property) {
        properties.remove(property.key());
        this.innerRemoveProperty(property);
    }

    /**
     * Removes inner property.
     *
     * @param property the inner property to be removed.
     */
    protected abstract void innerRemoveProperty(Property property);

    /**
     * Creates a property based on the input key and value.
     *
     * @param key the key.
     * @param value the value.
     * @return the property created.
     */
    protected abstract Property createProperty(String key, Object value);

    /**
     * Determines whether the property with certain key can be added or not.
     *
     * @param key the key to be checked against.
     * @return true if the key is neither label nor id else false.
     */
    protected boolean shouldAddProperty(String key) {
        return !key.equals("label") && !key.equals("id");
    }

    /**
     * Checks whether the element is removed or not.
     */
    protected abstract void checkRemoved();

    /**
     * Removes inner property.
     */
    protected abstract void innerRemove();

    @Override
    public void remove() {
        checkRemoved();
        innerRemove();
        this.removed = true;
    }

    /**
     * Copies of all the properties in a new map.
     *
     * @return the new map copy.
     */
    public Map<String, Object> allFields() {
        Map<String, Object> map = new HashMap<>();
        properties.forEach((key, value) -> map.put(key, value.value()));
        return map;
    }

    /**
     * Sets the label.
     *
     * @param label the element label.
     */
    public void setLabel(String label) {
        this.label = label;
    }
}
