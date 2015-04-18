package com.tinkerpop.gremlin.elastic.process.graph.traversal.spatialdsl;

import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.shape.Shape;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy.Geo;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Element;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Eliran on 18/4/2015.
 */
public class ConvexhullStep<S extends Element> extends AbstractStep<S, Long> implements SideEffectCapable{
    public static final String CONVEX_KEY =  "CONVEXHULL";
    private String geoshapeProperty;
    public ConvexhullStep(Traversal traversal,final String geoshapeProperty) {
            super(traversal);
        this.geoshapeProperty = geoshapeProperty;

    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().asAdmin().getSideEffects().remove(CONVEX_KEY);
    }

    @Override
    public String getSideEffectKey() {
        return CONVEX_KEY;
    }


    @Override
    protected Traverser<Long> processNextStart() throws NoSuchElementException {
        this.getTraversal().asAdmin().getSideEffects().getOrCreate(CONVEX_KEY, () -> "");
        List<Geometry> geometries = new ArrayList<Geometry>();

        while (this.starts.hasNext()) {
            Object objectShape = this.starts.next().get().value(geoshapeProperty);
            Shape shape = null;
            try {
                shape = Geo.convertObjectToShape(objectShape);
            } catch (IOException e) {
                e.printStackTrace();
                throw FastNoSuchElementException.instance();
            }
            Geometry geom = JtsSpatialContext.GEO.getGeometryFrom(shape);
            geometries.add(geom);
        }
        if(geometries.size()>0) {
            GeometryCollection collection = new GeometryCollection(geometries.toArray(new Geometry[geometries.size()]), new GeometryFactory());
            Geometry convexHull = collection.convexHull();
            this.getTraversal().asAdmin().getSideEffects().set(CONVEX_KEY, convexHull.toString());
        }
        throw FastNoSuchElementException.instance();
    }
}
