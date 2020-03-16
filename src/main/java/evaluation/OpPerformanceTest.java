package evaluation;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.expr.NodeValue;
import se.liu.ida.rspqlstar.datatypes.ProbabilityDistribution;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.function.Probability;

public class OpPerformanceTest {
    public static void main(String[] args){
        BayesianNetwork.init();
        Probability.init();

        NodeValue v1 = asNodeValue("11", XSDDatatype.XSDfloat);
        NodeValue d1 = asNodeValue("Normal(0,10)", ProbabilityDistribution.type);
        NodeValue d2 = asNodeValue("Normal(10,10)", ProbabilityDistribution.type);

        // Warm up
        for(int i=0; i<100_000; i++){
            NodeValue x = Probability.greaterThan(d1, v1, true);
        }

        float n = 100_000f;
        long t0;

        t0 = System.nanoTime();
        for(int i=0; i<n; i++) Probability.greaterThan(d1, v1, true);
        System.out.printf("greaterThan: %.2f μs", (System.nanoTime()-t0)/(n*1000));

        t0 = System.nanoTime();
        for(int i=0; i<n; i++) Probability.lessThan(d1, v1, true);
        System.out.printf("lessThan: %.2f μs", (System.nanoTime()-t0)/(n*1000));

        t0 = System.nanoTime();
        System.out.printf("add: %.2f μs", (System.nanoTime()-t0)/(n*1000));
    }

    public static NodeValue asNodeValue(String lex, RDFDatatype type){
        Node n = ResourceFactory.createTypedLiteral(lex, type).asNode();
        return NodeValue.makeNode(n);
    }
}
