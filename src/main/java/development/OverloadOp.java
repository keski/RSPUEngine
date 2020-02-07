package development;

import org.apache.jena.base.Sys;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.lang.RSPQLStar;

import java.util.HashMap;

public class OverloadOp {
    public static void main(String[] args){
        String rspuNs = "http://w3id.org/rsp/rspu#";
        RSPQLStar.init();
        FunctionRegistry.get().put(rspuNs + "add", Probability.add);

    }
}
