package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Map;

public interface EdgeMapping {

    public String getExternalVertexField();

    public Direction getDirection();

    public String getLabel();

    public String getExternalVertexLabel();

    public boolean isEdgeProperty(String key);

    public boolean isVertexProperty(String key);

    public String toDocProperty(String key);

    public String fromDocProperty(String key);

    public Object getExternalVertexId(Map<String, Object> entries);
}
