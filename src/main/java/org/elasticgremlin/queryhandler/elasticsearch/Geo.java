package org.elasticgremlin.queryhandler.elasticsearch;

import com.spatial4j.core.shape.*;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.json.JSONObject;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.*;

/**
 * Geo ENUM class.
 */
public enum Geo implements BiPredicate {


    ////////////////////////////////////////////////////////////////////////////
    // Fields
    /**
     * Whether the intersection between two geographic regions is non-empty
     */

    INTERSECTS(ShapeRelation.INTERSECTS, (geometry1, geometry2) -> {
        SpatialRelation relation = geometry1.relate(geometry2);
        return relation == SpatialRelation.INTERSECTS || relation == SpatialRelation.CONTAINS || relation == SpatialRelation.WITHIN;
    }
    ),

    /**
     * Whether the intersection between two geographic regions is empty
     */
    DISJOINT(ShapeRelation.DISJOINT, (geometry1, geometry2) -> geometry1.relate(geometry2) == SpatialRelation.DISJOINT),

    /**
     * Whether one geographic region is completely contains within another
     */
    WITHIN(ShapeRelation.WITHIN, (geometry1, geometry2) -> geometry1.relate(geometry2) == SpatialRelation.WITHIN);

    /**
     * The shape relation.
     */
    private ShapeRelation relation;

    /**
     * The test function for the shape.
     */
    private BiFunction<Shape, Shape, Boolean> testFunc;


    ////////////////////////////////////////////////////////////////////////////
    // Constructors

    /**
     * Constructs Geo.
     *
     * @param relation the relation.
     * @param testFunc the shape test function.
     */
    Geo(ShapeRelation relation, BiFunction<Shape, Shape, Boolean> testFunc) {

        this.relation = relation;
        this.testFunc = testFunc;
    }


    ////////////////////////////////////////////////////////////////////////////
    // Methods

    /**
     * Gets the relation.
     *
     * @return the shape relation.
     */
    public ShapeRelation getRelation() {
        return relation;
    }

    @Override
    public boolean test(Object o, Object o2) {
        Shape s1 = null;
        Shape s2 = null;
        try {
            s1 = convertObjectToShapeIfPossible(o);
            s2 = convertObjectToShapeIfPossible(o2);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (s1 == null) return false;
        return testFunc.apply(s1,s2);
    }

    /**
     * Converts object to shape if applicable.
     *
     * @param o the object to convert.
     * @return the shape.
     * @throws IOException if error occurs during conversion.
     */
    private Shape convertObjectToShapeIfPossible(Object o) throws IOException {

        if(o instanceof Shape) return (Shape) o;
        String geoShapeStringValue = null;
        if(o instanceof HashMap) {
            HashMap map = (HashMap) o;
            Preconditions.checkArgument(map.containsKey("coordinates") && map.containsKey("type"));
            geoShapeStringValue = (new JSONObject(map)).toString();
        }
        else if (o instanceof String){
            geoShapeStringValue = (String) o;
        }
        Preconditions.checkNotNull(geoShapeStringValue);
        XContentParser parser = JsonXContent.jsonXContent.createParser(geoShapeStringValue);
        parser.nextToken();

        return ShapeBuilder.parse(parser).build();


    }

    public static <V> P<V> intersercts(final V value) { return new P(Geo.INTERSECTS, value); }
    public static <V> P<V> disjoint(final V value) { return new P(Geo.DISJOINT, value); }
    public static <V> P<V> within(final V value) { return new P(Geo.WITHIN, value); }
}




