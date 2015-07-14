package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.lang.Object;import java.lang.Override;import java.lang.String;import java.util.Map;

public class BasicEdgeMapping implements EdgeMapping {
    private final String edgeLabel;
    private final String externalVertexLabel;
    private final Direction direction;
    private final String externalVertexField;
    private String prefix;

    public BasicEdgeMapping(String edgeLabel, String externalVertexLabel,
                            Direction direction, String externalVertexField,
                            String prefix) {
        this.edgeLabel = edgeLabel;
        this.externalVertexLabel = externalVertexLabel;
        this.direction = direction;
        this.externalVertexField = externalVertexField;
        this.prefix = prefix;
    }

    @Override
    public String getExternalVertexField() {
        return externalVertexField;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    @Override
    public String getLabel() {
        return edgeLabel;
    }

    @Override
    public String getExternalVertexLabel() {
        return externalVertexLabel;
    }

    @Override
    public boolean isEdgeProperty(String key) {
        return key.startsWith(prefix);
    }

    @Override
    public boolean isVertexProperty(String key) {
        if (getExternalVertexField().equals(key)) {
            return false;
        }
        return !isEdgeProperty(key);
    }

    @Override
    public String fromDocProperty(String key) {
        return key.substring(prefix.length());
    }

    @Override
    public String toDocProperty(String key) {
        return prefix + key;
    }

    @Override
    public Object getExternalVertexId(Map<String, Object> entries) {
        return entries.get(externalVertexField);
    }
}
