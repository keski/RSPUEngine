package se.liu.ida.rspqlstar.function;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.function.FunctionBase2;
import org.apache.jena.sparql.function.FunctionBase3;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.datatypes.ProbabilityDistribution;

import java.util.function.Consumer;

public class Probability {
    private static final double MIN_VALUE = 0.0000001; // Double.MIN_VALUE?
    public static final String ns = "http://w3id.org/rsp/rspu#";

    public static void init(){
        // Probability distribution
        FunctionRegistry.get().put(ns + "lessThan", Probability.lessThan);
        FunctionRegistry.get().put(ns + "greaterThan", Probability.greaterThan);
        FunctionRegistry.get().put(ns + "lessThanOrEqual", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(ns + "greaterThanOrEqual", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(ns + "between", Probability.between);
        FunctionRegistry.get().put(ns + "add", Probability.add);
        FunctionRegistry.get().put(ns + "subtract", Probability.subtract);
    }

    public static FunctionFactory lessThan = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("lessThan", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.lessThan(args[0], args[1], false));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.lessThan(nv1, nv2, false);
            }
        }
    };

    public static FunctionFactory lessThanOrEqual = s -> new FunctionBase2() { // less than or equal
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("lessThanOrEqual", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.lessThan(args[0], args[1], true));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.lessThan(nv1, nv2, true);
            }
        }
    };

    public static FunctionFactory greaterThan = s -> new FunctionBase2() { // greater than
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("greaterThan", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.greaterThan(args[0], args[1], false));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.greaterThan(nv1, nv2, false);
            }
        }
    };

    public static FunctionFactory greaterThanOrEqual = s -> new FunctionBase2() { // greater than or equal
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("greaterThanOrEqual", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.greaterThan(args[0], args[1], true));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.greaterThan(nv1, nv2, true);
            }
        }
    };

    public static FunctionFactory between = s -> new FunctionBase3() {
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2, NodeValue nv3) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("between", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.between(args[0], args[1], args[2]));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.between(nv1, nv2, nv3);
            }
        }
    };


    public static FunctionFactory add = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("add", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.add(args[0], args[1]));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.add(nv1, nv2);
            }
        }
    };

    public static FunctionFactory subtract = s -> new FunctionBase2() {
        @Override
        public NodeValue exec(NodeValue nv1, NodeValue nv2) {
            if(RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE) {
                LazyNodeValue value = new LazyNodeValue("subtract", new NodeValue[]{nv1, nv2});
                final Consumer<NodeValue[]> f = (args) -> {
                    LazyNodeValue.cache.put(value.toString(), Probability.subtract(args[0], args[1]));
                };
                value.setConsumer(f);
                return value;
            } else {
                return Probability.subtract(nv1, nv2);
            }
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
            if(o1 instanceof UniformRealDistribution){
                UniformRealDistribution d = ((UniformRealDistribution) o1);
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
