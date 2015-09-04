package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.*;
import java.util.function.Function;

/**
 * The Query iterator.
 * @param <E>
 */
public class QueryIterator<E extends Element> implements Iterator<E> {

    ////////////////////////////////////////////////////////////////////////////
    // Fields
    /**
     * The scroll search response.
     */
    private SearchResponse scrollResponse;

    /**
     * Allowed remaining counter.
     */
    private long allowedRemaining;

    /**
     * Conversion function.
     */
    private final Function<Iterator<SearchHit>, Iterator<? extends E>> convertFunc;

    /**
     * Timing accessor.
     */
    private TimingAccessor timing;

    /**
     * The client.
     */
    private Client client;

    /**
     * Iterator of hits.
     */
    private Iterator<? extends E> hits;


    ////////////////////////////////////////////////////////////////////////////
    // Constructors

    /**
     * Constructs QueryIterator.
     *
     * @param filter the filter.
     * @param startFrom start from identifier.
     * @param scrollSize scroll size.
     * @param maxSize maximum size to scroll.
     * @param client the client.
     * @param convertFunc the conversion function.
     * @param refresh the refresh flag.
     * @param timing the timing.
     * @param indices the indices.
     */
    public QueryIterator(QueryBuilder filter, int startFrom, int scrollSize, long maxSize, Client client,
                         Function<Iterator<SearchHit>, Iterator<? extends E>> convertFunc,
                         Boolean refresh, TimingAccessor timing, String... indices) {
        this.client = client;
        this.allowedRemaining = maxSize;
        this.convertFunc = convertFunc;
        this.timing = timing;

        if (refresh) client.admin().indices().prepareRefresh(indices).execute().actionGet();
        this.timing.start("scroll");
        scrollResponse = client.prepareSearch(indices)
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(filter))
                .setFrom(startFrom)
                .setScroll(new TimeValue(60000))
                .setSize(maxSize < scrollSize ? (int) maxSize : scrollSize)
                .execute().actionGet();
        this.timing.stop("scroll");

        hits = convertFunc.apply(scrollResponse.getHits().iterator());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Methods
    @Override
    public boolean hasNext() {
        if(allowedRemaining <= 0) return false;
        if(hits.hasNext()) return true;

        timing.start("scroll");
        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        timing.stop("scroll");

        hits = convertFunc.apply(scrollResponse.getHits().iterator());

        return hits.hasNext();
    }

    @Override
    public E next() {
        allowedRemaining--;
        return hits.next();
    }
}
