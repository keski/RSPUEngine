package evaluation;

import generator.Generator;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ContinuousListener;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.stream.TimeWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger(PerformanceEvaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws IOException {
        // Experiment parameters
        final int[] rates = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
        final int[] ratios = {1};
        final double occUnc =  .05;
        final double attrUnc = .05;
        final int duration = 10000;
        final int skip = 5;
        final double[] thresholds = {.05};

        if(false){
            Generator.run(duration + 5000, rates, ratios, new double[]{occUnc}, new double[]{attrUnc});
            return;
        }
        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "data/experiments/medical.bn");

        // Activate/de-active runs
        boolean attribute = true;
        boolean pattern = true;
        boolean combined = true;
        boolean baseline = true;

        // Files and listeners
        final String attributeFile = root + "data/experiments/results/attribute_performance.csv";
        final TimeWriterStream attributeListener = attribute ? new TimeWriterStream(attributeFile) : null;
        final String patternFile = root + "data/experiments/results/pattern_performance.csv";
        final TimeWriterStream patternListener = pattern ? new TimeWriterStream(patternFile) : null;
        final String combinedFile = root + "data/experiments/results/combined_performance.csv";
        final TimeWriterStream combinedListener = combined ? new TimeWriterStream(combinedFile) : null;
        final String baselineFile = root + "data/experiments/results/baseline_performance.csv";
        final TimeWriterStream baselineListener = combined ? new TimeWriterStream(baselineFile) : null;

        int attributeCounter = 0;
        int patternCounter = 0;
        int combinedCounter = 0;
        int baselineCounter = 0;
        int total = ratios.length * rates.length * thresholds.length;
        for (int ratio : ratios) {
            for (int rate : rates) {
                for(double threshold: thresholds) {
                    if(attribute) {
                        attributeCounter++;
                        logger.info("Attribute: " + attributeCounter + " of " + total);
                        performance("attribute", 0, attrUnc, threshold, rate, ratio, attributeListener, duration, skip);
                        performance("attribute2", 0, attrUnc, threshold, rate, ratio, attributeListener, duration, skip);
                    }
                    if(pattern) {
                        patternCounter++;
                        logger.info("Pattern: " + patternCounter + " of " + total);
                        performance("pattern", occUnc, 0, threshold, rate, ratio, patternListener, duration, skip);
                    }
                    if(combined) {
                        combinedCounter++;
                        logger.info("Combined: " + combinedCounter + " of " + total);
                        performance("combined", 0, attrUnc, threshold, rate, ratio, combinedListener, duration, skip);
                    }
                    if(baseline){
                        baselineCounter++;
                        logger.info("Baseline: " + baselineCounter + " of " + total);
                        // Uncertainty has no effect on performance, pick any unc
                        performance("baseline", occUnc, 0, threshold, rate, ratio, baselineListener, duration, skip);
                    }
                }
            }
        }

        if(attribute) attributeListener.close();
        if(pattern) patternListener.close();
        if(combined) combinedListener.close();
        if(baseline) baselineListener.close();
    }

    /**
     * Performance test for a given uncertainty config.
     *
     * @param type Query type: "occurrence", "attribute", or "combination"
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
    public static void performance(String type, double occUnc, double attrUnc, double threshold, int rate, int ratio,
                                   TimeWriterStream listener, long duration, int skip){
        final long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime + 1000);
        listener.setSkip(skip);
        final String base = String.format("data/experiments/streams/%.2f-%.2f-%s_", occUnc, attrUnc, rate);

        // Register streams
        manager.registerStreamFromFile(root + base + ratio + "_ce.trigs", "http://base/stream/ce");
        manager.registerStreamFromFile(root + base + ratio + "_hr.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + ratio + "_br.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + ratio + "_ox.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "data/experiments/queries/" + type + "_performance.rspqlstar";

        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$RATE$", Double.toString(rate));
        qString = qString.replace("$THRESHOLD$", Double.toString(threshold));
        qString = qString.replace("$RATIO$", Double.toString(ratio));
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
