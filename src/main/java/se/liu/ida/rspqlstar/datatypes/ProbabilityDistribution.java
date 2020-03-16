package se.liu.ida.rspqlstar.datatypes;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ProbabilityDistribution extends BaseDatatype {
    public static Logger logger = Logger.getLogger(ProbabilityDistribution.class);
    public static final String datatypeUri = "http://w3id.org/rsp/rspu#distribution";

    public static final ProbabilityDistribution type = new ProbabilityDistribution();
    public static Map<String, MyFunctionalInterface> distributionMap = new HashMap<>();
    private String lexicalForm;

    static {
        distributionMap.put("U", ProbabilityDistribution::getUniformDistribution);
        distributionMap.put("N", ProbabilityDistribution::getNormalDistribution);
    }

    @FunctionalInterface
    public interface MyFunctionalInterface {
        RealDistribution create(double... args);
    }

    private static NormalDistribution getNormalDistribution(double... args) {
        final double mean = args[0];
        final double variance = args[1];
        return new NormalDistribution(null, mean, Math.sqrt(variance));
    }

    private static UniformRealDistribution getUniformDistribution(double... args) {
        final double lower = args[0];
        final double upper = args[1];
        return new UniformRealDistribution(null, lower, upper);
    }

    public ProbabilityDistribution() {
        super(datatypeUri);
    }

    public ProbabilityDistribution(String lexicalForm) {
        super(datatypeUri);
        this.lexicalForm = lexicalForm;
    }

    /**
     * Parse a lexical form of this datatype to a value. Assumes that distribution name is represented by a single
     * char.
     * @throws DatatypeFormatException if the lexical form is not legal
     */
    public RealDistribution parse(String lexicalForm) throws DatatypeFormatException {
        try {
            final String type = lexicalForm.substring(0,1);
            final String[] args = lexicalForm.substring(2,lexicalForm.length()-1).split(",");
            final double[] doubleValues = new double[args.length];
            for(int i=0; i<args.length; i++) doubleValues[i] = Double.parseDouble(args[i]);
            return distributionMap.get(type).create(doubleValues);
        } catch(Exception e){
            logger.error(e);
            return null;
        }
    }

    /**
     * Compares two instances of values of the given datatype.
     * This does not allow rationals to be compared to other number
     * formats, Lang tag is not significant.
     */
    public boolean isEqual(LiteralLabel value1, LiteralLabel value2) {
        return value1.getDatatype() == value2.getDatatype() && value1.getLexicalForm().equals(value2.getLexicalForm());
    }

    public String toString() {
        return String.format("\"%s\"^^<%s>", lexicalForm, uri);
    }


}
