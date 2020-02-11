package se.liu.ida.rspqlstar.function;

import org.apache.jena.sparql.function.FunctionRegistry;

public class RSPUFunctions {
    // Namespace
    public static final String ns = "http://w3id.org/rsp/rspu#";

    public static void register(){
        // Probability distribution
        FunctionRegistry.get().put(ns + "lt", Probability.lessThan);
        FunctionRegistry.get().put(ns + "lte", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(ns + "gt", Probability.greaterThan);
        FunctionRegistry.get().put(ns + "gte", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(ns + "between", Probability.between);
        FunctionRegistry.get().put(ns + "add", Probability.add);
        FunctionRegistry.get().put(ns + "subtract", Probability.subtract);
    }
}
