package evaluation;

import generator.Generator;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Exp1 {
    private static Logger logger = Logger.getLogger(Exp1.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws FileNotFoundException {
        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "data/experiments/medical.bn");

        // Experiment parameters
        final int rate = 1000;
        final double[] occUncList = new double[]{.01, .05, .10, .25};
        final double[] attrUncList = new double[]{.001, .01, .05, .10, .25};
        final double[] thresholds = new double[]{0, .001, .01,
                .05, .10, .15, .20, .25, .30, .35, .40, .45, .50,
                .55, .60, .65, .70, .75, .80, .85, .90, .95, 0.99, 0.999};
        final int duration = 1000;
        final int skip = 5;

        // Activate/de-active runs
        boolean attribute = true;
        boolean pattern = true;
        boolean combined = true;

        // Files and listeners
        ResultWriterStream attributeListener = null;
        if(attribute) {
            final String attrFile = root + "data/experiments/results/attribute_correctness.csv";
            attributeListener = new ResultWriterStream(new PrintStream(new File(attrFile)));
        }

        ResultWriterStream patternListener = null;
        if(pattern){
            final String occFile = root + "data/experiments/results/pattern_correctness.csv";
            patternListener = new ResultWriterStream(new PrintStream(new File (occFile)));
        }

        ResultWriterStream combinedListener = null;
        if(combined){
            final String combinedFile = root + "data/experiments/results/combined_correctness.csv";
            combinedListener = new ResultWriterStream(new PrintStream(new File (combinedFile)));
        }


        // Attribute
        if(attribute) {
            for (double attrUnc : attrUncList) {
                for (double threshold : thresholds) {
                    correctness("attribute", 0, attrUnc, threshold, rate, attributeListener, duration, skip);
                }
                //correctness("attribute-baseline", 0, attrUnc, 0, rate, attributeListener, duration, skip);
            }
        }
        // Pattern
        if(pattern) {
            for (double occUnc : occUncList) {
                for (double threshold : thresholds) {
                    correctness("pattern", occUnc, 0, threshold, rate, patternListener, duration, skip);
                }
                //correctness("pattern-baseline", occUnc, 0, 0, rate, attributeListener, duration, skip);
            }
        }
        // Combined
        if(combined) {
            for (double attrUnc : attrUncList) {
                for (double threshold : thresholds) {
                    correctness("combined", 0, attrUnc, threshold, rate, combinedListener, duration, skip);
                }
                //correctness("combined-baseline", 0, attrUnc, 0, rate, combinedListener, duration, skip);
            }
        }
        if(attribute) attributeListener.close();
        if(pattern) patternListener.close();
        if(combined) combinedListener.close();
    }

    /**
     * Correctness test for a given uncertainty config, using a specified threshold.
     *
     * @param type Query type: "occurrence", "attribute", or "occurrence-attribute"
     * @param occUnc Degree of occurrence uncertainty. Expressed as the likelihood that the virtual evidence does not
     *               correspond to the value of its parent.
     * @param attrUnc Degree of attribute uncertainty. Expressed as number the percentile of values outside the value
     *                limits.
     * @param threshold Confidence threshold. Use to filter results.
     * @param rate Stream rate. Expressed as number of events per second per stream.
     * @param duration Duration of test.
     * @param skip The number of results to skip before logging results.
     */
    public static void correctness(String type, double occUnc, double attrUnc, double threshold, int rate,
                                   ResultWriterStream listener, long duration, int skip){
        final long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
        listener.setSkip(skip);

        final String base = String.format("data/experiments/streams/%.3f-%.3f-%s-", occUnc, attrUnc, rate);
        logger.info("Type: " + type + " Threshold: " + threshold);
        logger.info("Using streams with base: " + base);

        // Register streams
        manager.registerStreamFromFile(root + base + "ce.trigs", "http://base/stream/ce");
        manager.registerStreamFromFile(root + base + "hr.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + "br.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + "ox.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "data/experiments/queries/" + type + "_correctness.rspqlstar";

        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$THRESHOLD$", Double.toString(threshold));
        qString = qString.replace("$OCCURRENCE$", Double.toString(occUnc));
        qString = qString.replace("$ATTRIBUTE$", Double.toString(attrUnc));
        //logger.info(qString);
        final RSPQLStarQueryExecution q = manager.registerQuery(qString);

        // listen
        q.addContinuousListener(listener);

        // stop after
        TimeUtil.silentSleep(duration);
        listener.flush();
        manager.stop();
        reset();
    }

    /**
     * Reset all engine dictionaries.
     */
    public static void reset(){
        NodeDictionaryFactory.get().clear();
        ReferenceDictionaryFactory.get().clear();
        VarDictionary.get().clear();
    }
}
