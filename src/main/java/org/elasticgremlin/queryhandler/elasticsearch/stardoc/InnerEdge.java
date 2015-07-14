package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.elasticgremlin.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

public class InnerEdge extends BaseEdge {

    private final EdgeMapping mapping;
    private final StarVertex holdingVertex;
    private final Vertex externalVertex;

    public InnerEdge(EdgeMapping mapping, StarVertex holdingVertex, Vertex externalVertex, Object[] keyValues, ElasticGraph graph) {
        this(externalVertex.toString() + holdingVertex.id().toString(),
                mapping, holdingVertex, externalVertex, keyValues, graph);
    }

    public InnerEdge(Object edgeId, EdgeMapping mapping, StarVertex holdingVertex, Vertex externalVertex,
                     Object[] keyValues, ElasticGraph graph) {
        super(edgeId, mapping.getLabel(), null, graph);
        this.mapping = mapping;
        this.inVertex = mapping.getDirection().equals(Direction.IN) ? holdingVertex : externalVertex;
        this.outVertex = mapping.getDirection().equals(Direction.OUT) ? holdingVertex : externalVertex;
        this.holdingVertex = holdingVertex;
        this.externalVertex = externalVertex;

        // See comment in StarVertex's constructor
        validateAndAddKeyValues(keyValues);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        holdingVertex.innerRemoveProperty(property);
    }

    @Override
    protected void innerRemove() {
        throw new NotImplementedException();
    }

    @Override
    public Property createProperty(String key, Object value) {
        return super.createProperty(mapping.fromDocProperty(key), value);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        return mapping.isEdgeProperty(key) && !mapping.fromDocProperty(key).equals(T.id.getAccessor());
    }

    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {
        BaseVertexProperty property;
        if (vertexProperty == null) {
            property = new BaseVertexProperty(holdingVertex, null, null);
        }
        else {
            property = new BaseVertexProperty(holdingVertex, vertexProperty.key(), vertexProperty.value());
        }
        holdingVertex.innerAddProperty(property);
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = new HashMap<>();
        super.allFields().forEach((key, value) -> map.put(mapping.toDocProperty(key), value));
        map.put(mapping.getExternalVertexField(), externalVertex.id());
        map.put(mapping.toDocProperty(T.id.getAccessor()), id);
        return map;
    }

    public EdgeMapping getMapping() {
        return mapping;
    }
}
