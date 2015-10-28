package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.elasticgremlin.queryhandler.elasticsearch.Geo;
import org.elasticsearch.action.admin.cluster.health.*;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;

import java.io.IOException;
import java.util.*;
import java.util.function.BiPredicate;

/**
 * Elastic helper class.
 */
public class ElasticHelper {


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    /**
     * Creates index.
     *
     * @param indexName the index name.
     * @param client the client.
     * @throws IOException when times out.
     */
    public static void createIndex(String indexName, Client client) throws IOException {
        IndicesExistsRequest request = new IndicesExistsRequest(indexName);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (!response.isExists()) {
            Settings settings = Settings.settingsBuilder().put("index.analysis.analyzer.default.type", "keyword").build();
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName).setSettings(settings);
            client.admin().indices().create(createIndexRequestBuilder.request()).actionGet();
        }

        final ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexName).timeout(TimeValue.timeValueSeconds(10)).waitForYellowStatus();
        final ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealthRequest).actionGet();
        if (clusterHealth.isTimedOut()) {
            throw new IOException(clusterHealth.getStatus() +
                    " status returned from cluster '" + client.admin().cluster().toString() +
                    "', index '" + indexName + "'");

        }
    }

    /**
     * Clears the index.
     *
     * @param client the client.
     * @param indexName the index name to be cleared.
     * @return Deleted by query response.
     */
    public static DeleteIndexResponse clearIndex(Client client, String indexName){
        return client.admin().indices()
                .delete(new DeleteIndexRequest(indexName))
                .actionGet();
    }

    /**
     * Creates a bool filter builder.
     * @param hasContainers the list of has container
     * @return bool filter builder
     */
    public static BoolQueryBuilder createFilterBuilder(List<HasContainer> hasContainers) {
        BoolQueryBuilder boolFilter = QueryBuilders.boolQuery();
        if(hasContainers != null) hasContainers.forEach(has -> addFilter(boolFilter, has));
        return boolFilter;
    }

    /**
     * Adds filter to the filter builder.
     *
     * @param boolFilterBuilder the bool filter builder.
     * @param has the has container.
     */
    private static void addFilter(BoolQueryBuilder boolFilterBuilder, HasContainer has){
        String key = has.getKey();
        Object value = has.getValue();
        BiPredicate<?, ?> predicate = has.getBiPredicate();

        if(key.equals("~id")) {
            IdsQueryBuilder idsFilterBuilder = QueryBuilders.idsQuery();
            if(value.getClass().isArray()) {
                for(Object id : (Object[])value)
                    idsFilterBuilder.addIds(id.toString());
            }
            else idsFilterBuilder.addIds(value.toString());
            boolFilterBuilder.must(idsFilterBuilder);
        }
        else if(key.equals("~label")) {
            if(value instanceof List){
                List labels = (List) value;
                if(labels.size() == 1)
                    boolFilterBuilder.must(QueryBuilders.typeQuery(labels.get(0).toString()));
                else {
                    QueryBuilder[] filters = new QueryBuilder[labels.size()];
                    for(int i = 0; i < labels.size(); i++)
                        filters[i] = QueryBuilders.typeQuery(labels.get(i).toString());
                    boolFilterBuilder.must(QueryBuilders.orQuery(filters));
                }
            }
            else boolFilterBuilder.must(QueryBuilders.typeQuery(value.toString()));
        }
        else if (predicate instanceof Compare) {
            String predicateString = predicate.toString();
            switch (predicateString) {
                case ("eq"):
                    boolFilterBuilder.must(QueryBuilders.termQuery(key, value));
                    break;
                case ("neq"):
                    boolFilterBuilder.mustNot(QueryBuilders.termQuery(key, value));
                    break;
                case ("gt"):
                    boolFilterBuilder.must(QueryBuilders.rangeQuery(key).gt(value));
                    break;
                case ("gte"):
                    boolFilterBuilder.must(QueryBuilders.rangeQuery(key).gte(value));
                    break;
                case ("lt"):
                    boolFilterBuilder.must(QueryBuilders.rangeQuery(key).lt(value));
                    break;
                case ("lte"):
                    boolFilterBuilder.must(QueryBuilders.rangeQuery(key).lte(value));
                    break;
                case("inside"):
                    List items =(List) value;
                    Object firstItem = items.get(0);
                    Object secondItem = items.get(1);
                    boolFilterBuilder.must(QueryBuilders.rangeQuery(key).from(firstItem).to(secondItem));
                    break;
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + predicate.toString());
            }
        } else if (predicate instanceof Contains) {
            if (predicate == Contains.without) boolFilterBuilder.must(QueryBuilders.missingQuery(key));
            else if (predicate == Contains.within){
                if(value == null) boolFilterBuilder.must(QueryBuilders.existsQuery(key));
                else  boolFilterBuilder.must(QueryBuilders.termsQuery(key, value));
            }
        } else if (predicate instanceof Geo) boolFilterBuilder.must(new GeoShapeQueryBuilder(key, GetShapeBuilder(value), ((Geo) predicate).getRelation()));
        else throw new IllegalArgumentException("predicate not supported by elastic-gremlin: " + predicate.toString());
    }

    /**
     * Gets shape builder.
     *
     * @param object the geo object in JSON format.
     * @return shape builder.
     */
    private static ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }
}
