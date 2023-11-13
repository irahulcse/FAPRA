package thesis.context.predicates;

import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import thesis.context.VehicleContext;
import thesis.context.data.Data;
import thesis.context.data.LocationData;
import thesis.context.data.ScalarData;

import javax.annotation.Nonnull;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ComplexPredicate {

    private enum Logic {AND, OR}

    private Logic logic = null;
    private final List<ScalarPredicate> scalarPredicates = new ArrayList<>();
    private final List<LocationPredicate> locationPredicates = new ArrayList<>();
    private final List<ComplexPredicate> combinedPredicates = new ArrayList<>();

    /**
     * @param logic              logical "AND" and "OR"
     * @param scalarPredicates   predicate for scalar values, such as speed, time
     * @param locationPredicates predicate for location values of class {@link java.awt.geom.Point2D}
     * @param combinedPredicates other constructed predicates
     * @throws IllegalArgumentException if there are fewer than 2 predicates
     */
    public ComplexPredicate(@Nonnull String logic, Collection<ScalarPredicate> scalarPredicates, Collection<LocationPredicate> locationPredicates,
                            Collection<ComplexPredicate> combinedPredicates) throws IllegalArgumentException {
        if (logic.equalsIgnoreCase("AND") || logic.equalsIgnoreCase("OR")) {
            this.logic = Logic.valueOf(logic.toUpperCase());
        } else {
            throw new IllegalArgumentException("The logic should be either AND or OR");
        }

        if (scalarPredicates != null) {
            this.scalarPredicates.addAll(scalarPredicates);
        }
        if (locationPredicates != null) {
            this.locationPredicates.addAll(locationPredicates);
        }
        if (combinedPredicates != null) {
            this.combinedPredicates.addAll(combinedPredicates);
        }
        if (this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() < 2) {
            throw new IllegalArgumentException("There should be at least 2 predicates defined.");
        }
    }

    public ComplexPredicate(ScalarPredicate sp) {
        scalarPredicates.add(sp);
    }

    public ComplexPredicate(LocationPredicate lp) {
        locationPredicates.add(lp);
    }

    public ComplexPredicate(String json) {
        JSONObject o = new JSONObject(json);
        try {
            if (o.getString("logic") != null &&
                    (o.getString("logic").equalsIgnoreCase("AND") ||
                            o.getString("logic").equalsIgnoreCase("OR"))) {
                logic = Logic.valueOf(o.getString("logic"));
            }
        } catch (JSONException ignored) {
        }

        try {
            JSONArray scalarPredicateArray = o.getJSONArray("scalar");
            scalarPredicateArray.forEach(entry -> {
                this.scalarPredicates.add(new ScalarPredicate((JSONObject) entry));
            });
        } catch (JSONException ignored) {
        }

        try {
            JSONArray locationPredicateArray = o.getJSONArray("location");
            locationPredicateArray.forEach(entry -> {
                this.locationPredicates.add(new LocationPredicate((JSONObject) entry));
            });
        } catch (JSONException ignored) {
        }

        if ((this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() > 1) && this.logic == null) {
            throw new IllegalArgumentException("Only one predicate is allows since there's no logical relation defined.");
        }
        if ((this.scalarPredicates.size() + this.locationPredicates.size() + this.combinedPredicates.size() < 2) && this.logic != null) {
            throw new IllegalArgumentException("There should be at least 2 predicates defined with a logic of " + this.logic);
        }

    }

    public boolean evaluate(Tuple2<ScalarData, LocationData> contextTuple) {
        if (contextTuple == null) return false;
        if (logic == Logic.AND) {
            // if one of the predicate return false, then return false and break;
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = contextTuple.f0.getData();
                    if (!sp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = contextTuple.f1.getData();
                    if (!lp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (!cp.evaluate(contextTuple)) {
                        return false;
                    }
                    ;
                }
            }
            return true;
        }
        if (logic == Logic.OR) {
            // if one of the predicate returns true, then break and return true
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = contextTuple.f0.getData();
                    if (sp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = contextTuple.f1.getData();
                    if (lp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (cp.evaluate(contextTuple)) {
                        return true;
                    }
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unsupported logical operation: " + logic);
    }

    public boolean evaluate(VehicleContext vc){
        if (vc == null) return false;
        if (logic == Logic.AND) {
            // if one of the predicate return false, then return false and break;
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = vc.scalarData.getData();
                    if (!sp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = vc.locationData.getData();
                    if (!lp.evaluate(d)) {
                        return false;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (!cp.evaluate(vc)) {
                        return false;
                    }
                    ;
                }
            }
            return true;
        }
        if (logic == Logic.OR) {
            // if one of the predicate returns true, then break and return true
            if (!scalarPredicates.isEmpty()) {
                for (ScalarPredicate sp : scalarPredicates) {
                    Double d = vc.scalarData.getData();
                    if (sp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!locationPredicates.isEmpty()) {
                for (LocationPredicate lp : locationPredicates) {
                    Point2D d = vc.locationData.getData();
                    if (lp.evaluate(d)) {
                        return true;
                    }
                }
            }
            if (!combinedPredicates.isEmpty()) {
                for (ComplexPredicate cp : combinedPredicates) {
                    if (cp.evaluate(vc)) {
                        return true;
                    }
                }
            }
            return false;
        }
        if (logic == null){
            if (!scalarPredicates.isEmpty()){
                ScalarPredicate sp = scalarPredicates.get(0);
                Double d = vc.scalarData.getData();
                return sp.evaluate(d);
            }
            if (!locationPredicates.isEmpty()){
                LocationPredicate lp = locationPredicates.get(0);
                Point2D p = vc.locationData.getData();
                return lp.evaluate(p);
            }
            if (!combinedPredicates.isEmpty()){
                ComplexPredicate cp = combinedPredicates.get(0);
                return cp.evaluate(vc);
            }
        }
        throw new IllegalArgumentException("Unsupported logical operation: " + logic);
    }
}
