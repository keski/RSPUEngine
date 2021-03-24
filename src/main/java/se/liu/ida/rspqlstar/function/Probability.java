package se.liu.ida.rspqlstar.function;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.function.FunctionBase2;
import org.apache.jena.sparql.function.FunctionBase3;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.datatypes.ProbabilityDistribution;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Lazy_Node_Concrete_WithID;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.util.Date;
import java.util.function.Consumer;

public class Probability {
    public static boolean USE_CACHE = true;
    public static boolean USE_LAZY_VAR = true;

    private static final double MIN_VALUE = 0.0000001; // Double.MIN_VALUE?
    public static final String ns = "http://w3id.org/rsp/rspu#";

    public static void main(String[] args){
        RSPQLStarEngineManager.init();
        RSPQLStarEngineManager manager = new RSPQLStarEngineManager(new Date().getTime());
        RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = false;

        for(int i=0; i<1000; i++){
            Node g = NodeFactory.createURI("http://g1");
            Node s = NodeFactory.createURI("http://s" + i);
            Node p = NodeFactory.createLiteral("http://p", XSDDatatype.XSDdecimal);
            Node o = NodeFactory.createLiteralByValue(1, XSDDatatype.XSDdecimal);
            manager.getSdg().add(new Quad(g,s,p,o));
        }

        ResultSet rs = manager.runOnce("" +
                "PREFIX rspu: <http://w3id.org/rsp/rspu#> " +
                "REGISTER STREAM <s> COMPUTED EVERY PT4S AS " +
                "SELECT ?s ?p ?x " +
                "WHERE {" +
                "    GRAPH <http://g1> { ?s ?p ?o } " +
                "    FILTER(rspu:lessThan(\"N(0,1)\"^^rspu:distribution, rspu:add(\"N(0,1)\"^^rspu:distribution, ?o)) > 0) " + // avg. no cache: 1.9ms, avg: cache: 0.48ms
                //"    FILTER(rspu:lessThan(\"N(0,1)\"^^rspu:distribution, ?o) > 0) " + // avg: 0.375ms (if all cached), avg: 0.35ms. Simple value, no difference in using cache!
                //"    FILTER(?o > 0) " + // avg: 0.3ms
                "}");
        long t0 = new Date().getTime();
        double counter = 0;
        while(rs.hasNext()){
            for(String var: rs.getResultVars()){
                System.err.print(var + "\t");
            }
            String line = "";
            while(rs.hasNext()) {
                QuerySolution qs = rs.next();
                for (String var : rs.getResultVars()) {
                    RDFNode n = qs.get(var);
                    if(n != null) {
                        line += n.toString() + "\t";
                    } else {
                        line += "null\t";
                    }
                }
                counter++;
            }
        }
        System.err.println(counter + " results");
        System.err.println("unresolvedLazyNodes: " + Lazy_Node_Concrete_WithID.unresolvedLazyNodes);
        System.err.println("resolvedLazyNodes: " + Lazy_Node_Concrete_WithID.resolvedLazyNodes);
        System.err.println("cachedLazyNodes: " + Lazy_Node_Concrete_WithID.cachedLazyNodes);
        System.err.println("cacheHits: " + LazyNodeCache.cacheHits);

        long t1 = new Date().getTime();
        System.err.println(t1-t0 + " ms");
        System.err.println((t1-t0)/counter + " ms (avg)");
    }

    public static void init(){
        // Probability distribution
        FunctionRegistry.get().put(ns + "lessThan", Probability.fnLessThan);
        FunctionRegistry.get().put(ns + "greaterThan", Probability.fnGreaterThan);
        FunctionRegistry.get().put(ns + "lessThanOrEqual", Probability.fnLessThanOrEqual);
        FunctionRegistry.get().put(ns + "greaterThanOrEqual", Probability.fnGreaterThanOrEqual);
        FunctionRegistry.get().put(ns + "between", Probability.fnBetween);
        FunctionRegistry.get().put(ns + "add", Probability.fnAdd);
        FunctionRegistry.get().put(ns + "subtract", Probability.fnSubtract);
    }

    public static FunctionFactory fnLessThan = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("lessThan", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.lessThan(args[0], args[1], false).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.lessThan(arg1, arg2, false);
        }
    };

    public static FunctionFactory fnLessThanOrEqual = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("lessThanOrEqual", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.lessThan(args[0], args[1], true).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.lessThan(arg1, arg2, true);
        }
    };

    public static FunctionFactory fnGreaterThan = s -> new FunctionBase2() { // greater than
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("greaterThan", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.greaterThan(args[0], args[1], false).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.greaterThan(arg1, arg2, false);
        }
    };

    public static FunctionFactory fnGreaterThanOrEqual = s -> new FunctionBase2() { // greater than or equal
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("greaterThanOrEqual", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.lessThan(args[0], args[1], true).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.lessThan(arg1, arg2, true);
        }
    };

    public static FunctionFactory fnBetween = s -> new FunctionBase3() {
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2, NodeValue arg3) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("between", new NodeValue[]{arg1, arg2, arg3});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.between(args[0], args[1], args[2]).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.between(arg1, arg2, arg3);
        }
    };


    public static FunctionFactory fnAdd = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("add", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.add(args[0], args[1]).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.add(arg1, arg2);
        }
    };

    public static FunctionFactory fnSubtract = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue arg1, NodeValue arg2) {
            if(USE_LAZY_VAR){
                final LazyNodeValue lnv = new LazyNodeValue("subtract", new NodeValue[]{arg1, arg2});
                final Consumer<NodeValue[]> fn = (args) -> {
                    lnv.node.resolvedNode = Probability.subtract(args[0], args[1]).getNode();
                };
                lnv.node.setConsumer(fn);
                return lnv;
            }
            return Probability.subtract(arg1, arg2);
        }
    };

    /**
     * Return the distribution resulting from subtracting nv2 from nv1.
     * @param nv1
     * @param nv2
     * @return
     */
    private static NodeValue subtract(NodeValue nv1, NodeValue nv2){
        final Object v1 = getLiteral(nv1);
        final Object v2 = getLiteral(nv2);

        final NodeValue nv;
        if(v1 instanceof Double && v2 instanceof Double){
            nv = NodeValue.makeDecimal((double) v1 - (double) v2);
        } else if(v1 instanceof Double){
            throw new ExprEvalException("subtract: invalid order of arguments");
        } else if(v2 instanceof RealDistribution){
            nv = subtract((RealDistribution) v1, (RealDistribution) v2);
        } else {
            nv = subtract((RealDistribution) v1, (double) v2);
        }
        return nv;
    }

    /**
     * Return the distribution resulting from adding nv1 to nv2.
     * @param nv1
     * @param nv2
     * @return
     */
    public static NodeValue add(NodeValue nv1, NodeValue nv2) {
        final Object v1 = getLiteral(nv1);
        final Object v2 = getLiteral(nv2);

        final NodeValue nv;
        if(v1 instanceof Double && v2 instanceof Double){
            nv = NodeValue.makeDecimal((double) v1 + (double) v2);
        } else if(v1 instanceof Double){
            throw new ExprEvalException("add: invalid order of arguments");
        } else if(v2 instanceof RealDistribution){
            nv = add((RealDistribution) v1, (RealDistribution) v2);
        } else {
            nv = add((RealDistribution) v1, (double) v2);
        }
        return nv;
    }

    /**
     * Return the probability that nv1 is between nv2 and nv3 (inclusive).
     * @param nv1
     * @param nv2
     * @param nv3
     * @return
     */
    private static NodeValue between(NodeValue nv1, NodeValue nv2, NodeValue nv3) {
        final Object d = getLiteral(nv1);
        final Object lower = getLiteral(nv2);
        final Object upper = getLiteral(nv3);

        final NodeValue nv;
        if (!(lower instanceof Double) || !(upper instanceof Double)) {
            throw new ExprEvalException("between: invalid bounds");
        } else if (d instanceof Double) {
            if((double) d > (double) lower && (double) d <= (double) upper){
                nv = NodeValue.makeDecimal(1);
            } else {
                nv = NodeValue.makeDecimal(0);
            }
        } else {
            final double prob = ((AbstractRealDistribution)d).probability((double) lower, (double) upper);
            nv = NodeValue.makeDecimal(prob);
        }
        return nv;
    }

    /**
     * Return the probability that nv1 is greater than nv2.
     * @param nv1
     * @param nv2
     * @param inclusive
     * @return
     */
    public static NodeValue greaterThan(NodeValue nv1, NodeValue nv2, boolean inclusive){
        final Object o1 = getLiteral(nv1);
        final Object o2 = getLiteral(nv2);
        final double diff = inclusive ? 0 : MIN_VALUE;

        final NodeValue nv;
        if (o2 instanceof Double) {
            final double prob = ((RealDistribution) o1).cumulativeProbability((double) o2 + diff);
            nv = NodeValueNode.makeNode(NodeFactory.createLiteralByValue(1 - prob, XSDDatatype.XSDdouble));
        } else if (o1 instanceof Double) {
            throw new ExprEvalException("greaterThan/greaterThanOrEqual: invalid order of arguments");
        } else {
            RealDistribution d1, d2;
            if(o1 instanceof UniformRealDistribution){
                UniformRealDistribution urd1 = ((UniformRealDistribution) o1);
                d1 = new UniformRealDistribution(new JDKRandomGenerator(), urd1.getSupportLowerBound(), urd1.getSupportUpperBound());
            } else if(o1 instanceof NormalDistribution){
                NormalDistribution n1 = ((NormalDistribution) o1);
                d1 = new NormalDistribution(new JDKRandomGenerator(), n1.getMean(), n1.getStandardDeviation());
            } else {
                throw new ExprEvalException("greaterThan: The distribution is not yet supported");
            }

            if(o2 instanceof UniformRealDistribution){
                UniformRealDistribution d = ((UniformRealDistribution) o2);
                d2 = new UniformRealDistribution(new JDKRandomGenerator(), d.getSupportLowerBound(), d.getSupportUpperBound());
            } else if(o2 instanceof NormalDistribution){
                NormalDistribution d = ((NormalDistribution) o2);
                d2 = new NormalDistribution(new JDKRandomGenerator(), d.getMean(), d.getStandardDeviation());
            } else {
                throw new ExprEvalException("greaterThan: The distributions is not yet supported");
            }

            int sampleSize = 10000;
            DescriptiveStatistics ds = new DescriptiveStatistics();
            for(int i=0; i<sampleSize; i++){
                ds.addValue(d1.sample() > d2.sample() ? 1 : 0);
            }
            nv = NodeValueNode.makeNode(NodeFactory.createLiteralByValue(ds.getMean(), XSDDatatype.XSDdouble));
        }
        return nv;
    }

    /**
     * Return the probability that nv1 is less than nv2.
     * @param nv1
     * @param nv2
     * @param inclusive
     * @return
     */
    public static NodeValue lessThan(NodeValue nv1, NodeValue nv2, boolean inclusive){
        final Object o1 = getLiteral(nv1);
        final Object o2 = getLiteral(nv2);
        final double diff = inclusive ? 0 : MIN_VALUE;
        final NodeValue nv;
        if (o2 instanceof Double) {
            final double prob = ((RealDistribution) o1).cumulativeProbability((double) o2 - diff);
            nv = NodeValueNode.makeNode(NodeFactory.createLiteralByValue(prob, XSDDatatype.XSDdouble));
        } else if (o1 instanceof Double) {
            throw new ExprEvalException("lessThan/lessThanOrEqual: invalid order of arguments");
        } else {
            RealDistribution d1, d2;
            if(o1 instanceof UniformRealDistribution){
                UniformRealDistribution urd1 = ((UniformRealDistribution) o1);
                d1 = new UniformRealDistribution(new JDKRandomGenerator(), urd1.getSupportLowerBound(), urd1.getSupportUpperBound());
            } else if(o1 instanceof NormalDistribution){
                NormalDistribution n1 = ((NormalDistribution) o1);
                d1 = new NormalDistribution(new JDKRandomGenerator(), n1.getMean(), n1.getStandardDeviation());
            } else {
                throw new ExprEvalException("lessThan: The distribution is not yet supported");
            }
            if(o1 instanceof UniformRealDistribution){
                UniformRealDistribution d = ((UniformRealDistribution) o1);
                d2 = new UniformRealDistribution(new JDKRandomGenerator(), d.getSupportLowerBound(), d.getSupportUpperBound());
            } else if(o2 instanceof NormalDistribution){
                NormalDistribution d = ((NormalDistribution) o2);
                d2 = new NormalDistribution(new JDKRandomGenerator(), d.getMean(), d.getStandardDeviation());
            } else {
                throw new ExprEvalException("lessThan: The distributions is not yet supported");
            }

            int sampleSize = 10000;
            DescriptiveStatistics ds = new DescriptiveStatistics();
            for(int i=0; i<sampleSize; i++){
                ds.addValue(d1.sample() < d2.sample() ? 1 : 0);
            }
            nv = NodeValueNode.makeNode(NodeFactory.createLiteralByValue(ds.getMean(), XSDDatatype.XSDdouble));
        }
        return nv;
    }

    /**
     * Parse distribution from node value.
     * @param nodeValue
     * @return
     */
    public static RealDistribution getDistribution(NodeValue nodeValue){
        return ProbabilityDistribution.type.parse(nodeValue.asNode().getLiteralLexicalForm());
    }

    /**
     * Returns the distribution resulting from adding a constant to a distribution.
     * @param x
     * @param k
     * @return
     */
    public static NodeValue add(RealDistribution x, double k) {
        final NodeValue nv;
        if(x instanceof NormalDistribution){
            final double mean = x.getNumericalMean() + k;
            final double variance = x.getNumericalVariance();
            nv = NodeValue.makeNode(String.format("N(%s, %s)", mean, variance), ProbabilityDistribution.type);
        } else if(x instanceof UniformRealDistribution){
            final double lower = x.getSupportLowerBound() + k;
            final double upper = x.getSupportUpperBound() + k;
            final String lexicalForm = String.format("U(%s, %s)", lower, upper);
            nv = NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            throw new ExprEvalException("unsupported distribution type");
        }
        return nv;
    }

    /**
     * Returns the distribution resulting from adding two distributions.
     * @param d1
     * @param d2
     * @return
     */
    public static NodeValue add(RealDistribution d1, RealDistribution d2) {
        final NodeValue nv;
        if(d1 instanceof NormalDistribution && d2 instanceof NormalDistribution){
            final double mean = d1.getNumericalMean() + d2.getNumericalMean();
            final double variance = d1.getNumericalVariance() + d2.getNumericalVariance();
            final String lexicalForm = String.format("N(%s, %s)", mean, variance);
            nv = NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            throw new ExprEvalException("unsupported distribution type");
        }
        return nv;
    }

    /**
     * Returns the distribution resulting from subtracting a constant from distribution.
     * @param x
     * @param k
     * @return
     */
    public static NodeValue subtract(RealDistribution x, double k) {
        final NodeValue nv;
        if(x instanceof NormalDistribution){
            final double mean = x.getNumericalMean() - k;
            final double variance = x.getNumericalVariance();
            nv = NodeValue.makeNode(String.format("N(%s, %s)", mean, variance), ProbabilityDistribution.type);
        } else if(x instanceof UniformRealDistribution){
            final double lower = x.getSupportLowerBound() - k;
            final double upper = x.getSupportUpperBound() - k;
            final String lexicalForm = String.format("U(%s, %s)", lower, upper);
            nv = NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            throw new ExprEvalException("unsupported distribution type");
        }
        return nv;
    }

    /**
     * Returns the distribution resulting from subtracting a distribution from another.
     * @param d1
     * @param d2
     * @return
     */
    public static NodeValue subtract(RealDistribution d1, RealDistribution d2) {
        final NodeValue nv;
        if(d1 instanceof NormalDistribution && d2 instanceof NormalDistribution){
            final double mean = d1.getNumericalMean() - d2.getNumericalMean();
            final double variance = d1.getNumericalVariance() + d2.getNumericalVariance();
            final String lexicalForm = String.format("N(%s, %s)", mean, variance);
            nv = NodeValue.makeNode(lexicalForm, ProbabilityDistribution.type);
        } else {
            throw new ExprEvalException("unsupported distribution type");
        }
        return nv;
    }

    /**
     * Get literal object from node value or null.
     * @param nv
     * @return
     */
    public static Object getLiteral(NodeValue nv){
        if(nv instanceof LazyNodeValue){
            nv = ((LazyNodeValue) nv).getNodeValue();
        }
        final Object value;
        if(!nv.isLiteral()){
            throw new ExprEvalException(nv + " is not a literal value");
        } else if(nv.isNumber()){
            value = nv.getDouble();
        } else if(nv.getDatatypeURI().equals(ProbabilityDistribution.datatypeUri)){
            value = ProbabilityDistribution.type.parse(nv.asNode().getLiteralLexicalForm());
        } else {
            throw new ExprEvalException(nv + " is not a supported distribution type");
        }
        return value;
    }
}
