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
import se.liu.ida.rspqlstar.stream.PerformanceWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;
import java.io.IOException;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger(PerformanceEvaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws IOException {
        final long t0 = System.currentTimeMillis();

        // Experiment parameters
        final int[] rates = {10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
        final double occUnc =  .05;
        final double attrUnc = .05;
        final int duration = 15000;
        final int skip = 5;

        if(false){
            Generator.run(duration + 7000, rates, new double[]{occUnc}, new double[]{attrUnc});
            return;
        }
        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "experiments/medical.bn");

        // Activate/de-active runs
        boolean attribute = false;
        boolean pattern = false;
        boolean combined = false;
        boolean baseline = true;

        // Files and listeners
        final String attributeFile = root + "experiments/results/attribute_performance.csv";
        final PerformanceWriterStream attributeListener = attribute ? new PerformanceWriterStream(attributeFile) : null;
        final String patternFile = root + "experiments/results/pattern_performance.csv";
        final PerformanceWriterStream patternListener = pattern ? new PerformanceWriterStream(patternFile) : null;
        final String combinedFile = root + "experiments/results/combined_performance.csv";
        final PerformanceWriterStream combinedListener = combined ? new PerformanceWriterStream(combinedFile) : null;
        final String baselineFile = root + "experiments/results/baseline_performance.csv";
        final PerformanceWriterStream baselineListener = baseline ? new PerformanceWriterStream(baselineFile) : null;

        // Warm up. Allocate sufficient VM memory to prevent reallocation during run.
        final boolean runWarmUp = true;
        if(runWarmUp) {
            final PerformanceWriterStream listener = new PerformanceWriterStream("experiments/results/warmUp.csv");
            if (attribute) performance("attribute", 0, attrUnc, 900, listener, duration, skip);
            if (pattern) performance("pattern", occUnc, 0, 900, listener, duration, skip);
            if (combined) performance("combined", 0, attrUnc, 900, listener, duration, skip);
            if (baseline) performance("baseline", occUnc, 0, 100, listener, duration, skip);
            listener.close();
        }
        if(true){
            return;
        }

        // Real run
        int attributeCounter = 0;
        int patternCounter = 0;
        int combinedCounter = 0;
        int baselineCounter = 0;
        final int total = rates.length;

        for (int rate : rates) {
            if(attribute) {
                attributeCounter++;
                logger.info("Attribute: " + attributeCounter + " of " + total);
                performance("attribute", 0, attrUnc, rate, attributeListener, duration, skip);
            }
            if(pattern) {
                patternCounter++;
                logger.info("Pattern: " + patternCounter + " of " + total);
                performance("pattern", occUnc, 0, rate, patternListener, duration, skip);
            }
            if(combined) {
                combinedCounter++;
                logger.info("Combined: " + combinedCounter + " of " + total);
                performance("combined", 0, attrUnc, rate, combinedListener, duration, skip);
            }
            if(baseline){
                baselineCounter++;
                logger.info("Baseline: " + baselineCounter + " of " + total);
                // Uncertainty has no effect on performance, pick any unc
                performance("baseline", occUnc, 0, rate, baselineListener, duration, skip);
            }
        }


        if(attribute) attributeListener.close();
        if(pattern) patternListener.close();
        if(combined) combinedListener.close();
        if(baseline) baselineListener.close();

        logger.info("Finished in " + (System.currentTimeMillis() - t0)/1000 + " seconds");
    }

    /**
     * Performance test for a given uncertainty config.
     *
     * @param type Query type: "occurrence", "attribute", or "combination"
     * @param occUnc Degree of occurrence uncertainty. Expressed as the likelihood that the virtual evidence does not
     *               correspond to the value of its parent.
     * @param attrUnc Degree of attribute uncertainty. Expressed as number the percentile of values outside the value
     *                limits.
     * @param rate Stream rate. Expressed as number of events per second per stream.
     * @param duration Duration of test.
     * @param skip The number of results to skip before logging results.
     */
    public static void performance(String type, double occUnc, double attrUnc, int rate,
                                   ContinuousListener listener, long duration, int skip){
        final long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
        listener.setSkip(skip);
        final String base = String.format("experiments/streams/%.2f-%.2f-%s_%s", occUnc, attrUnc, rate, 1);

        // Register streams
        manager.registerStreamFromFile(root + base + "_hr.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + "_br.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + "_ox.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "experiments/queries/performance/" + type + ".rspqlstar";

        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$RATE$", Double.toString(rate));
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
