package extendop;

import org.apache.jena.sparql.function.FunctionRegistry;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.lang.RSPQLStar;

public class OverloadOp {
    public static void main(String[] args){
        String rspuNs = "http://w3id.org/rsp/rspu#";
        RSPQLStar.init();
        FunctionRegistry.get().put(rspuNs + "add", Probability.add);

    }
}
