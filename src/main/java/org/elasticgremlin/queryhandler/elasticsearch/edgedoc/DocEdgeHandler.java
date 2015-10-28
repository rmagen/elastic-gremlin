package org.elasticgremlin.queryhandler.elasticsearch.edgedoc;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.*;

/**
 * The handler for handling document as an edge.
 */
public class DocEdgeHandler implements EdgeHandler {

    ////////////////////////////////////////////////////////////////////////////
    // Fields
    /**
     * The elastic graph.
     */
    private ElasticGraph graph;

    /**
     * The client.
     */
    private final Client client;

    /**
     * The elastic mutations.
     */
    private final ElasticMutations elasticMutations;

    /**
     * The index name.
     */
    private final String indexName;

    /**
     * The scroll size.
     */
    private final int scrollSize;

    /**
     * The refresh flag.
     */
    private final boolean refresh;

    /**
     * The timing accessor.
     */
    private TimingAccessor timing;


    ////////////////////////////////////////////////////////////////////////////
    // Constructors

    /**
     * Constructs DocEdgeHandler.
     *
     * @param graph the graph.
     * @param client the client.
     * @param elasticMutations the elastic mutations.
     * @param indexName the index name.
     * @param scrollSize the scroll size.
     * @param refresh the refresh flag.
     * @param timing the timing accessor.
     */
    public DocEdgeHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                          int scrollSize, boolean refresh, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
    }


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    @Override
    public Iterator<Edge> edges() {
        return new QueryIterator<>(QueryBuilders.existsQuery(DocEdge.InId), 0, scrollSize, Integer.MAX_VALUE,
                client, this::createEdge, refresh, timing, indexName);
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        MultiGetRequest request = new MultiGetRequest().refresh(refresh);
        for (Object id : ids) request.add(indexName, null, id.toString());
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
        BoolQueryBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(QueryBuilders.existsQuery(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createEdge, refresh, timing, indexName);
    }

    @Override
    public Map<Object, Set<Edge>> edges(Iterator<BaseVertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        Map<Object, Vertex> idToVertex = new HashMap<>();
        vertices.forEachRemaining(singleVertex -> idToVertex.put(singleVertex.id(), singleVertex));

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        Object[] vertexIds = idToVertex.keySet().toArray();
        BoolQueryBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        if (direction == Direction.IN)
            boolFilter.must(QueryBuilders.termsQuery(DocEdge.InId, vertexIds));
        else if (direction == Direction.OUT)
            boolFilter.must(QueryBuilders.termsQuery(DocEdge.OutId, vertexIds));
        else if (direction == Direction.BOTH)
            boolFilter.must(QueryBuilders.orQuery(
                    QueryBuilders.termsQuery(DocEdge.InId, vertexIds),
                    QueryBuilders.termsQuery(DocEdge.OutId, vertexIds)));

        QueryIterator<Edge> edgeQueryIterator = new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow, client, this::createEdge , refresh, timing, indexName);

        Map<Object, Set<Edge>> results = new HashMap<>();
        edgeQueryIterator.forEachRemaining(edge -> edge.vertices(direction).forEachRemaining(vertex -> {
            Set<Edge> resultEdges = results.get(vertex.id());
            if (resultEdges == null) {
                resultEdges = new HashSet<>();
                results.put(vertex.id(), resultEdges);
            }
            resultEdges.add(edge);
        }));

        return results;
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        DocEdge elasticEdge = new DocEdge(edgeId, label, properties, outV, inV,graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    /**
     * Creates the edge.
     *
     * @param hits search hits.
     * @return iterator of edge created.
     */
    private Iterator<Edge> createEdge(Iterator<SearchHit> hits) {
        ArrayList<Edge> edges = new ArrayList<>();
        hits.forEachRemaining(hit -> {
            Map<String, Object> fields = hit.getSource();
            BaseVertex outVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), null, Direction.OUT);
            BaseVertex inVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, Direction.IN);
            BaseEdge edge = new DocEdge(hit.getId(), hit.getType(), null, outVertex, inVertex, graph, elasticMutations, indexName);
            fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
            edges.add(edge);
        });
        return edges.iterator();
    }

    /**
     * Creates the edge.
     *
     * @param hit a hit.
     * @return the edge
     */
    private Edge createEdge(GetResponse hit) {
        Map<String, Object> fields = hit.getSource();
        BaseVertex outVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.OutId), fields.get(DocEdge.OutLabel).toString(), null, Direction.OUT);
        BaseVertex inVertex = graph.getQueryHandler().vertex(fields.get(DocEdge.InId), fields.get(DocEdge.InLabel).toString(), null, Direction.IN);
        BaseEdge edge = new DocEdge(hit.getId(), hit.getType(), null, outVertex, inVertex, graph, elasticMutations, indexName);
        fields.entrySet().forEach((field) -> edge.addPropertyLocal(field.getKey(), field.getValue()));
        return edge;
    }
}
