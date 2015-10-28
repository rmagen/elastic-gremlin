package org.elasticgremlin.queryhandler.elasticsearch.vertexdoc;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.queryhandler.elasticsearch.edgedoc.DocEdge;
import org.elasticgremlin.queryhandler.elasticsearch.helpers.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

/**
 * The handler for handling document as a vertex.
 */
public class DocVertexHandler implements VertexHandler {


    ////////////////////////////////////////////////////////////////////////////
    // Fields
    /**
     * The elastic graph.
     */
    private ElasticGraph graph;

    /**
     * The client.
     */
    private Client client;

    /**
     * The elastic mutations.
     */
    private ElasticMutations elasticMutations;

    /**
     * The index name.
     */
    private String indexName;

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

    /**
     * The lazy getters.
     */
    private Map<Direction, LazyGetter> lazyGetters;

    /**
     * The default lazy getter.
     */
    private LazyGetter defaultLazyGetter;


    ////////////////////////////////////////////////////////////////////////////
    // Constructors

    /**
     * Constructs DocVertexHandler.
     *
     * @param graph the graph.
     * @param client the client.
     * @param elasticMutations the elastic mutations.
     * @param indexName the index name.
     * @param scrollSize the scroll size.
     * @param refresh the refresh flag.
     * @param timing the timing accessor.
     */
    public DocVertexHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                            int scrollSize, boolean refresh, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        this.timing = timing;
        this.lazyGetters = new HashMap<>();
    }


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    @Override
    public Iterator<Vertex> vertices() {
        return new QueryIterator<>(QueryBuilders.missingQuery(DocEdge.InId), 0, scrollSize,
                Integer.MAX_VALUE, client, this::createVertex, refresh, timing, indexName);
    }

    @Override
    public Iterator<? extends Vertex> vertices(Object[] vertexIds) {
        List<BaseVertex> vertices = new ArrayList<>();
        for(Object id : vertexIds){
            DocVertex vertex = new DocVertex(id.toString(), null, null, graph, getLazyGetter(), elasticMutations, indexName);
            vertex.setSiblings(vertices);
            vertices.add(vertex);
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        BoolQueryBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(QueryBuilders.missingQuery(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, refresh, timing, indexName);
    }

    @Override
    public BaseVertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new DocVertex(vertexId,vertexLabel, null ,graph,getLazyGetter(direction), elasticMutations, indexName);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        BaseVertex v = new DocVertex(id, label, properties, graph, null, elasticMutations, indexName);

        try {
            elasticMutations.addElement(v, indexName, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    /**
     * Gets lazy getter.
     *
     * @return lazy getter
     */
    private LazyGetter getLazyGetter() {
        if (defaultLazyGetter == null || !defaultLazyGetter.canRegister()) {
            defaultLazyGetter = new LazyGetter(client, timing);
        }
        return defaultLazyGetter;
    }

    /**
     * Gets lazy getter.
     *
     * @param direction the direction.
     * @return lazy getter.
     */
    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, timing);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    /**
     * Creates vertex.
     *
     * @param hits search hit result.
     * @return iterator of vertex created.
     */
    private Iterator<? extends Vertex> createVertex(Iterator<SearchHit> hits) {
        ArrayList<BaseVertex> vertices = new ArrayList<>();
        hits.forEachRemaining(hit -> {
            BaseVertex vertex = new DocVertex(hit.id(), hit.getType(), null, graph, null, elasticMutations, indexName);
            vertex.setSiblings(vertices);
            hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
            vertices.add(vertex);
        });
        return vertices.iterator();
    }
}
