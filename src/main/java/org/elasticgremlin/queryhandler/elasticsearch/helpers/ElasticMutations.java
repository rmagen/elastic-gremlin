package org.elasticgremlin.queryhandler.elasticsearch.helpers;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticgremlin.structure.BaseElement;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * The elastic mutations is where the actual elasticsearch request is build and send off.
 */
public class ElasticMutations {


    ////////////////////////////////////////////////////////////////////////////
    // Fields
    /**
     * The timing accessor. Like a stop watch.
     */
    private final TimingAccessor timing;

    /**
     * The client.
     */
    private Client client;

    /**
     * The bulk request bulder.
     */
    private BulkRequestBuilder bulkRequest;

    /**
     * The revision.
     */
    private int revision = 0;


    ////////////////////////////////////////////////////////////////////////////
    // Constructors

    /**
     * Constructs elastic mutation.
     *
     * @param bulk the bulk request flag
     * @param client the client
     * @param timing the timing
     */
    public ElasticMutations(Boolean bulk, Client client, TimingAccessor timing) {
        if(bulk) bulkRequest = client.prepareBulk();
        this.timing = timing;
        this.client = client;
    }


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    /**
     * Adds the element.
     *
     * @param element the element to be added.
     * @param index the index where the element is to be added to.
     * @param routing the routing of the request.
     * @param create the create flag.
     */
    public void addElement(Element element, String index, String routing,  boolean create) {
        IndexRequestBuilder indexRequest = client.prepareIndex(index, element.label(), element.id().toString())
                .setSource(propertiesMap(element)).setRouting(routing).setCreate(create);
        if(bulkRequest != null) bulkRequest.add(indexRequest);
        else indexRequest.execute().actionGet();
        revision++;
    }

    /**
     * Populates the properties of the element into a map and returns it.
     *
     * @param element the element
     * @return the map of properties of the element.
     */
    private Map propertiesMap(Element element) {
        if(element instanceof BaseElement)
            return ((BaseElement)element).allFields();

        Map<String, Object> map = new HashMap<>();
        element.properties().forEachRemaining(property -> map.put(property.key(), property.value()));
        return map;
    }

    /**
     * Updates the element.
     *
     * @param element the element to be updated.
     * @param index the index where the element is located.
     * @param routing the routing of the request.
     * @param upsert the upsert flag.
     * @throws ExecutionException if an error occurs during request execution.
     * @throws InterruptedException if an interruption happened during request execution.
     */
    public void updateElement(Element element, String index, String routing, boolean upsert) throws ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(index, element.label(), element.id().toString())
                .doc(propertiesMap(element)).routing(routing);
        if(upsert)
            updateRequest.detectNoop(true).docAsUpsert(true);
        if(bulkRequest != null) bulkRequest.add(updateRequest);
        else client.update(updateRequest).actionGet();
        revision++;
    }

    /**
     * Deletes an element.
     *
     * @param element the element to be deleted.
     * @param index the index where the element is located.
     * @param routing the routing.
     */
    public void deleteElement(Element element, String index, String routing) {
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(index, element.label(), element.id().toString()).setRouting(routing);
        if(bulkRequest != null) bulkRequest.add(deleteRequestBuilder);
        else deleteRequestBuilder.execute().actionGet();
        revision++;
    }

    /**
     * Commits the bulk request changes.
     */
    public void commit() {
        if (bulkRequest == null || bulkRequest.numberOfActions() == 0) return;
        timing.start("bulk");
        bulkRequest.execute().actionGet();
        timing.stop("bulk");
        bulkRequest = client.prepareBulk();
    }

    /**
     * Gets the revision.
     *
     * @return the revision.
     */
    public int getRevision() {
        return revision;
    }
}
