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
import java.io.IOException;

public class CorrectnessEvaluation {
    private static Logger logger = Logger.getLogger(CorrectnessEvaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws IOException {
        // Experiment parameters
        final int rate = 1000;
        final int ratio = 1;
        final double[] occUncList = {.01, .05, .10, .25};
        final double[] attrUncList = {.01, .05, .10, .25};
        final int duration = 10000;
        final int skip = 5;
        final double[] thresholds = {0, .01, .05, .10, .15, .20, .25, .30, .70, .75, .80, .85, .90, .95, 0.99, 0.999};

        if(false){
            Generator.run(duration + 5000, new int[]{rate}, occUncList, attrUncList);
            return;
        }

        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "data/experiments/medical.bn");

        // Activate/de-active runs
        boolean attribute = false;
        boolean pattern = false;
        boolean combined = false;
        boolean baseline = true;

        // Files and listeners
        final String attributeFile = root + "data/experiments/results/attribute_correctness.csv";
        final ResultWriterStream attributeListener = attribute ? new ResultWriterStream(attributeFile) : null;
        final String patternFile = root + "data/experiments/results/pattern_correctness.csv";
        final ResultWriterStream patternListener = pattern ? new ResultWriterStream(patternFile) : null;
        final String combinedFile = root + "data/experiments/results/combined_correctness.csv";
        final ResultWriterStream combinedListener = combined ? new ResultWriterStream(combinedFile) : null;

        final String baselineFile = root + "data/experiments/results/baseline_correctness.csv";
        final ResultWriterStream baselineListener = baseline ? new ResultWriterStream(baselineFile) : null;

        int attributeCounter = 0;
        int patternCounter = 0;
        int combinedCounter = 0;
        for (double threshold : thresholds) {
            // Uncertainty
            if (attribute) {
                for (double attrUnc : attrUncList) {
                    attributeCounter++;
                    logger.info("Attribute: " + attributeCounter + " of " + attrUncList.length * thresholds.length);
                    correctness("attribute", 0, attrUnc, threshold, rate, ratio, attributeListener, duration, skip);
                }
            }
            if (pattern) {
                for (double occUnc : occUncList) {
                    patternCounter++;
                    logger.info("Pattern: " + patternCounter + " of " + occUncList.length * thresholds.length);
                    correctness("pattern", occUnc, 0, threshold, rate, ratio, patternListener, duration, skip);
                }
            }
            if (combined) {
                for (double attrUnc : attrUncList) {
                    combinedCounter++;
                    logger.info("Combined: " + combinedCounter + " of " + attrUncList.length * thresholds.length);
                    correctness("combined", 0, attrUnc, threshold, rate, ratio, combinedListener, duration, skip);
                }
            }
        }
        if(baseline) {
            for (double attrUnc : attrUncList) {
                correctness("attribute-baseline", 0, attrUnc, 0, rate, ratio, baselineListener, duration, skip);
                correctness("combined-baseline", 0, attrUnc, 0, rate, ratio, baselineListener, duration, skip);
            }
            for (double occUnc : occUncList) {
                correctness("pattern-baseline", occUnc, 0, 0, rate, ratio, baselineListener, duration, skip);
            }
        }


        if(attribute) attributeListener.close();
        if(pattern) patternListener.close();
        if(combined) combinedListener.close();
        if(baseline) baselineListener.close();

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
     * @param ratio Ratio between SDEs and CEs.
     * @param duration Duration of test.
     * @param skip The number of results to skip before logging results.
     */
    public static void correctness(String type, double occUnc, double attrUnc, double threshold, int rate, int ratio,
                                   ResultWriterStream listener, long duration, int skip){
        final long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
        listener.setSkip(skip);
        final String base = String.format("data/experiments/streams/%.2f-%.2f-%s_", occUnc, attrUnc, rate);

        // Register streams
        manager.registerStreamFromFile(root + base + ratio + "_ce.trigs", "http://base/stream/ce");
        manager.registerStreamFromFile(root + base + ratio + "_hr.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + ratio + "_br.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + ratio + "_ox.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "data/experiments/queries/" + type + "_correctness.rspqlstar";

        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$THRESHOLD$", Double.toString(threshold));
        qString = qString.replace("$OCCURRENCE$", Double.toString(occUnc));
        qString = qString.replace("$ATTRIBUTE$", Double.toString(attrUnc));
        //logger.info(qString);
        final RSPQLStarQueryExecution q = manager.registerQuery(qString);

        // listen
        q.setListener(listener);

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
