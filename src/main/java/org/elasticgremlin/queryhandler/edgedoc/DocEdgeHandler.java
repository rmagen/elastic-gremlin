package org.elasticgremlin.queryhandler.edgedoc;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.*;
import org.elasticgremlin.queryhandler.EdgeHandler;
import org.elasticgremlin.queryhandler.Predicates;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class DocEdgeHandler implements EdgeHandler {
    private ElasticGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private final boolean refresh;

    public DocEdgeHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, boolean refresh) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
    }

    @Override
    public Iterator<Edge> edges() {
        return new QueryIterator<>(FilterBuilders.existsFilter(DocEdge.InId), 0, scrollSize, Integer.MAX_VALUE, client, this::createEdge, null, refresh, indexName
        );
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        MultiGetRequest request = new MultiGetRequest().refresh(refresh);
        for (int i = 0; i < ids.length; i++) request.add(indexName, null, ids[i].toString());
        MultiGetResponse responses = client.multiGet(request).actionGet();

        ArrayList<Edge> elements = new ArrayList<>(ids.length);
        for (MultiGetItemResponse getResponse : responses) {
            GetResponse response = getResponse.getResponse();
            if (!response.isExists()) throw Graph.Exceptions.elementNotFound(Edge.class, response.getId());
            elements.add(createEdge(response));
        }
        return elements.iterator();
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createEdge, null, refresh, indexName
        );
    }

    @Override
    public Iterator<Edge> edges(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        List<Vertex> vertices = ElasticHelper.getVerticesBulk(vertex);
        List<Object> vertexIds = new ArrayList<>(vertices.size());
        vertices.forEach(singleVertex -> vertexIds.add(singleVertex.id()));

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(DocEdge.InId, vertexIds.toArray()));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(DocEdge.OutId, vertexIds.toArray()));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(DocEdge.InId, vertexIds.toArray()),
                    FilterBuilders.termsFilter(DocEdge.OutId, vertexIds.toArray())));

        QueryIterator<Edge> edgeSearchQuery = new QueryIterator<>(boolFilter, 0, scrollSize,
                predicates.limitHigh - predicates.limitLow, client,
                this::createEdge, null, refresh, indexName
        );

        Map<Object, List<Edge>> idToEdges = ElasticHelper.handleBulkEdgeResults(edgeSearchQuery,
                vertices, direction, edgeLabels, predicates);

        return idToEdges.get(vertex.id()).iterator();
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        DocEdge elasticEdge = new DocEdge(edgeId, label, outV.id(), outV.label(), inV.id(), inV.label(), properties, graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private Edge createEdge(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();
        BaseEdge edge = new DocEdge(hit.id(), hit.type(), fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }

    private Edge createEdge(GetResponse hit) {
        Map<String, Object> fields = hit.getSource();
        BaseEdge edge = new DocEdge(hit.getId(), hit.getType(), fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}