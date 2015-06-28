package org.elasticgremlin.queryhandler.vertexdoc;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.elasticsearch.*;
import org.elasticgremlin.elasticsearch.LazyGetter;
import org.elasticgremlin.queryhandler.edgedoc.DocEdge;
import org.elasticgremlin.queryhandler.*;
import org.elasticgremlin.structure.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;

public class DocVertexHandler implements VertexHandler {

    private ElasticGraph graph;
    private Client client;
    private ElasticMutations elasticMutations;
    private String indexName;
    private final int scrollSize;
    private final boolean refresh;
    private Map<Direction, LazyGetter> lazyGetters;
    private LazyGetter defaultLazyGetter;

    public DocVertexHandler(ElasticGraph graph, Client client, ElasticMutations elasticMutations, String indexName, int scrollSize, boolean refresh) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.refresh = refresh;
        lazyGetters = new HashMap<>();
    }

    @Override
    public Iterator<Vertex> vertices() {
        return new QueryIterator<>(FilterBuilders.missingFilter(DocEdge.InId), 0, scrollSize,
                Integer.MAX_VALUE, client, this::createVertex, BaseVertex::setVertexSiblings, refresh, indexName
        );
    }

    @Override
    public Iterator<Vertex> vertices(Object[] vertexIds) {
        List<Vertex> vertices = new ArrayList<>();
        for(Object id : vertexIds)
            vertices.add(new DocVertex(id, null, null, graph, getLazyGetter(), elasticMutations, indexName));
        vertices.forEach(vertex -> BaseVertex.setVertexSiblings(vertices));
        return vertices.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates) {
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.missingFilter(DocEdge.InId));
        return new QueryIterator<>(boolFilter, 0, scrollSize, predicates.limitHigh - predicates.limitLow,
                client, this::createVertex, BaseVertex::setVertexSiblings, refresh, indexName
        );
    }

    @Override
    public Vertex vertex(Object vertexId, String vertexLabel, Edge edge, Direction direction) {
        return new DocVertex(vertexId,vertexLabel, null ,graph,getLazyGetter(direction), elasticMutations, indexName);
    }

    @Override
    public Vertex addVertex(Object id, String label, Object[] properties) {
        Vertex v = new DocVertex(id, label, properties, graph, null, elasticMutations, indexName);

        try {
            elasticMutations.addElement(v, indexName, null, true);
        } catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
        }
        return v;
    }

    private LazyGetter getLazyGetter() {
        if (defaultLazyGetter == null || !defaultLazyGetter.canRegister()) {
            defaultLazyGetter = new LazyGetter(client, BaseVertex::setVertexSiblings);
        }
        return defaultLazyGetter;
    }

    private LazyGetter getLazyGetter(Direction direction) {
        LazyGetter lazyGetter = lazyGetters.get(direction);
        if (lazyGetter == null || !lazyGetter.canRegister()) {
            lazyGetter = new LazyGetter(client, BaseVertex::setVertexSiblings);
            lazyGetters.put(direction,
                    lazyGetter);
        }
        return lazyGetter;
    }

    private Vertex createVertex(SearchHit hit) {
        BaseVertex vertex = new DocVertex(hit.id(), hit.getType(), null, graph, null, elasticMutations, indexName);
        hit.getSource().entrySet().forEach((field) -> vertex.addPropertyLocal(field.getKey(), field.getValue()));
        return vertex;
    }
}