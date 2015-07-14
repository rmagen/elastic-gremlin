package org.elasticgremlin.queryhandler.elasticsearch.stardoc;

import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.ElasticMutations;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.LazyGetter;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class StarVertex extends BaseVertex {
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final EdgeMapping[] edgeMappings;
    private LazyGetter lazyGetter;
    private Set<InnerEdge> innerEdges;

    public StarVertex(final Object id, final String label, Object[] keyValues, ElasticGraph graph,
                      LazyGetter lazyGetter, ElasticMutations elasticMutations, String indexName,
                      EdgeMapping[] edgeMappings) {
        super(id, label, graph, null);
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.edgeMappings = edgeMappings;
        this.innerEdges = new HashSet<>();
        if(lazyGetter != null) {
            this.lazyGetter = lazyGetter;
            lazyGetter.register(this, this.indexName);
        }
        // Call this function here and don't use super() because we need edgeMappings to be initialized for shouldAddProperty
        validateAndAddKeyValues(keyValues);
    }

    @Override
    public String label() {
        if(this.label == null && lazyGetter != null) lazyGetter.execute();
        return super.label();
    }

    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {
        updateVertex();
    }

    @Override
    protected void innerRemoveProperty(Property property) {
        updateVertex();
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.property(key);
    }

    @Override
    protected boolean shouldAddProperty(String key) {
        if (!super.shouldAddProperty(key)) {
            return false;
        }

        for (EdgeMapping mapping : edgeMappings) {
            if (mapping.isEdgeProperty(key) || !mapping.isVertexProperty(key)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, Object> allFields() {
        Map<String, Object> map = new HashMap<>();
        properties.forEach((key, value) -> map.put(key, value.value()));
        allInnerEdgeFields().forEach(map::put);
        return map;
    }

    @Override
    protected void innerRemove() {
        elasticMutations.deleteElement(this, indexName, null);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if(lazyGetter != null) lazyGetter.execute();
        return super.properties(propertyKeys);
    }

    @Override
    public void applyLazyFields(MultiGetItemResponse response) {
        GetResponse getResponse = response.getResponse();
        if(getResponse.isSourceEmpty()) return;
        setFields(getResponse.getSource());
    }

    @Override
    public void addQueriedEdges(List<Edge> edges, Direction direction, String[] edgeLabels, Predicates predicates) {
        List<HasContainer> toRemove = new ArrayList<>();
        for (HasContainer has : predicates.hasContainers) {
            for (InnerEdge edge : innerEdges) {
                if (has.getKey().equals(edge.getMapping().toDocProperty(T.id.getAccessor())) &&
                        has.getBiPredicate() == Contains.without) {
                    toRemove.add(has);
                }
            }
        }
        predicates.hasContainers.removeAll(toRemove);

        super.addQueriedEdges(edges, direction, edgeLabels, predicates);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String[] edgeLabels, Predicates predicates) {
        List<Edge> queriedEdges = queriedEdges(direction, edgeLabels, predicates);
        if (queriedEdges != null) {
            for (Direction current : directions(direction)) {
                addInnerEdgesToList(current, edgeLabels, predicates, queriedEdges);
            }
            return queriedEdges.iterator();
        }

        List<Edge> edges = new ArrayList<>();
        for (Direction current : directions(direction)) {
            edgesInner(current, edgeLabels, predicates).forEachRemaining(edges::add);
        }
        return edges.iterator();
    }

    private Iterator<Edge> edgesInner(Direction direction, String[] edgeLabels, Predicates predicates) {
        if (edgeLabels == null || edgeLabels.length == 0) {
            return edgesInner(direction, predicates);
        }

        List<Edge> allEdges = new ArrayList<>();
        List<String> labelsLeft = addInnerEdgesToList(direction, edgeLabels, predicates, allEdges);

        // Check if there are labels which had no correct mapping - so they are external
        if (!labelsLeft.isEmpty()) {
            Iterator<Edge> externalEdges = super.edges(direction, labelsLeft.toArray(new String[labelsLeft.size()]), predicates);
            externalEdges.forEachRemaining(allEdges::add);
        }
        return allEdges.iterator();
    }

    private Iterator<Edge> edgesInner(Direction direction, Predicates predicates) {
        List<Edge> allEdges = new ArrayList<>();
        ArrayList<InnerEdge> retrievedEdges = new ArrayList<>();
        addInnerEdgesToList(direction, null, predicates, retrievedEdges);
        allEdges.addAll(retrievedEdges);

        Map<EdgeMapping, List<Object>> edgeMappingToIds = new HashMap<>();
        retrievedEdges.forEach(edge -> {
            List<Object> ids = edgeMappingToIds.get(edge.getMapping());
            if (ids == null) {
                ids = new ArrayList<>();
                edgeMappingToIds.put(edge.getMapping(), ids);
            }
            ids.add(edge.id());
        });
        // Make sure we don't query the edges we already have
        edgeMappingToIds.forEach((mapping, ids) -> {
            predicates.hasContainers.add(new HasContainer(mapping.toDocProperty(T.id.getAccessor()), P.without(ids)));
        });

        Iterator<Edge> externalEdges = super.edges(direction, null, predicates);
        externalEdges.forEachRemaining(allEdges::add);
        return allEdges.iterator();
    }

    /**
     * Adds the inner edges of the direction, labels and predicates to the provided list
     * @param direction The direction of the edges to be added
     * @param edgeLabels The labels of the edges to be added
     * @param predicates The predicates of the edges to be added must pass
     * @param edges The list to add the edges to
     * @return Labels from edgeLabels which are external
     */
    public List<String> addInnerEdgesToList(Direction direction, String[] edgeLabels, Predicates predicates, List<? super InnerEdge> edges) {
        if (edgeLabels == null || edgeLabels.length == 0) {
            addInnerEdgesToListForLabel(direction, null, predicates, edges);
            return new ArrayList<>(0);
        }
        List<String> externalLabels = new ArrayList<>();
        for (String label : edgeLabels) {
            boolean internal = addInnerEdgesToListForLabel(direction, label, predicates, edges);
            if (!internal) {
                externalLabels.add(label);
            }
        }
        return externalLabels;
    }

    /**
     * Adds the inner edges of the label, direction and predicates to the list of edges
     * @param direction The direction of the edges
     * @param edgeLabel The label of the edges
     * @param predicates The predicates the edges have to apply
     * @param edges The list to which the edges will be added
     * @return True if the label had a relevant mapping (mapping with the same label and direction), otherwise false
     */
    private boolean addInnerEdgesToListForLabel(Direction direction, String edgeLabel, Predicates predicates, List<? super InnerEdge> edges) {
        boolean hasMapping = false;
        for (InnerEdge edge : innerEdges) {
            if (isCorrectMapping(edge.getMapping(), direction, edgeLabel)) {
                hasMapping = true;
                // Test predicates on inner edge
                boolean passed = true;
                for (HasContainer hasContainer : predicates.hasContainers) {
                    if (!hasContainer.test(edge)) {
                        passed = false;
                    }
                }
                if (passed) {
                    edges.add(edge);
                }
            }
        }
        if (!hasMapping) {
            // Check if there are mappings which have no edges which are relevant
            for (EdgeMapping mapping : edgeMappings) {
                if (isCorrectMapping(mapping, direction, edgeLabel)) {
                    hasMapping = true;
                }
            }
        }
        return hasMapping;
    }

    private boolean isCorrectMapping(EdgeMapping mapping, Direction direction, String edgeLabel) {
        return mapping.getDirection().equals(direction) &&(edgeLabel == null || mapping.getLabel().equals(edgeLabel));
    }

    private Direction[] directions(Direction direction) {
        if (direction == Direction.IN) {
            return new Direction[] { Direction.IN };
        }
        if (direction == Direction.OUT) {
            return new Direction[] { Direction.OUT };
        }
        return new Direction[] { Direction.IN, Direction.OUT };
    }

    public void setFields(Map<String, Object> entries){
        // IMPORTANT! keyValues will be added both to the vertex and to the edge using addPropertyLocal -
        // which will call shouldAddProperty -
        // which will determine which properties are of the edge and which are of the vertex, using EdgeMapping.

        List<Object> keyValues = new ArrayList<>();
        entries.entrySet().forEach(field -> {
            if(field.getValue() != null) {
                addPropertyLocal(field.getKey(), field.getValue());
                keyValues.add(field.getKey());
                keyValues.add(field.getValue());
            }
        });
        for(EdgeMapping mapping : edgeMappings){
            Object externalVertexId = mapping.getExternalVertexId(entries);
            if(externalVertexId == null) continue;
            Vertex externalVertex = graph.getQueryHandler().vertex(externalVertexId,
                    mapping.getExternalVertexLabel(), null, mapping.getDirection().opposite());
            Object edgeId = entries.get(mapping.toDocProperty(T.id.getAccessor()));
            InnerEdge innerEdge = new InnerEdge(edgeId, mapping, this, externalVertex, keyValues.toArray(), graph);
            this.innerEdges.add(innerEdge);
        }
    }

    public InnerEdge addInnerEdge(EdgeMapping mapping, Object edgeId, String label, Vertex externalVertex,
                                  Object[] properties) {
        boolean mappingExists = false;
        for (EdgeMapping edgeMapping : edgeMappings) {
            if (mapping.equals(edgeMapping)) {
                mappingExists = true;
                break;
            }
        }
        if (!mappingExists) {
            throw new IllegalStateException("Received mapping that is not in the vertex");
        }

        InnerEdge edge = new InnerEdge(edgeId, mapping, this, externalVertex, properties, graph);
        this.innerEdges.add(edge);
        // Make sure edge fields are updated
        updateVertex();
        return edge;
    }

    public EdgeMapping[] getEdgeMappings() {
        return edgeMappings;
    }

    protected Map<String, Object> allInnerEdgeFields() {
        Map<String, Object> innerFieldMap = new HashMap<>();
        this.innerEdges.forEach(edge -> edge.allFields().forEach(innerFieldMap::put));
        return innerFieldMap;
    }

    private void updateVertex() {
        try {
            elasticMutations.updateElement(this, indexName, null, false);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
