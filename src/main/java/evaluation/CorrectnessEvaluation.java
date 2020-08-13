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
import java.util.HashMap;

public class CorrectnessEvaluation {
    private static Logger logger = Logger.getLogger(CorrectnessEvaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws IOException {
        // Experiment parameters
        final int rate = 1000;
        final int ratio = 1;
        final double[] occUncList = {.01, .05, .10, .25};
        final double[] attrUncList = {.01, .05, .10, .25};
        final int duration = 30000;
        final int skip = 5;
        final double[] thresholds = {
                0, 0.00001, .0001, .001, .01,
                .05, .10, .15, .20, .25, .30, .35, .40, .45, .50,
                .55, .60, .65, .70, .75, .80, .85, .90, .95, 0.99
        };


        if(false){
            Generator.run(duration + (skip + 5) * 1000, new int[]{rate}, occUncList, attrUncList);
            return;
        }

        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "experiments/medical.bn");

        // Activate/de-active runs
        boolean opt1 = true;
        boolean opt2 = false;
        boolean opt3 = false;
        boolean opt4 = false;
        boolean opt5 = false;

        // Files and listeners
        final String f1 = root + "experiments/results/correctness/option1.csv";
        final ResultWriterStream option1Listener = opt1 ? new ResultWriterStream(f1) : null;
        final String f2 = root + "experiments/results/correctness/option2.csv";
        final ResultWriterStream option2Listener = opt2 ? new ResultWriterStream(f2) : null;
        final String f3 = root + "experiments/results/correctness/option3.csv";
        final ResultWriterStream option3Listener = opt3 ? new ResultWriterStream(f3) : null;
        final String f4 = root + "experiments/results/correctness/option4.csv";
        final ResultWriterStream option4Listener = opt4 ? new ResultWriterStream(f4) : null;
        final String f5 = root + "experiments/results/correctness/option5.csv";
        final ResultWriterStream option5Listener = opt5 ? new ResultWriterStream(f5) : null;

        int opt1Remaining = attrUncList.length * thresholds.length;
        int opt2Remaining = occUncList.length * thresholds.length;
        int opt3Remaining = occUncList.length * thresholds.length;
        int opt4Remaining = occUncList.length * thresholds.length;
        int opt5Remaining = attrUncList.length * thresholds.length;


        for (double threshold : thresholds) {
            // Uncertainty
            for (double attrUnc : attrUncList) {
                if (opt1) {
                    logger.info("################# Option 1: Remaining " + opt1Remaining--);
                    correctness("option1", 0, attrUnc, threshold, rate, ratio, option1Listener, duration, skip);
                }
                if (opt5) {
                    logger.info("################# Option 5: Remaining " + opt5Remaining--);
                    correctness("option5", 0, attrUnc, threshold, rate, ratio, option5Listener, duration, skip);
                }
            }
            for (double occUnc : occUncList) {
                if (opt2) {
                    logger.info("################# Option 2: Remaining " + opt2Remaining--);
                    correctness("option2", occUnc, 0, threshold, rate, ratio, option2Listener, duration, skip);
                }
                if (opt3) {
                    logger.info("################# Option 3: Remaining " + opt3Remaining--);
                    correctness("option3", occUnc, 0, threshold, rate, ratio, option3Listener, duration, skip);
                }
                if (opt4) {
                    logger.info("################# Option 4: Remaining " + opt4Remaining--);
                    correctness("option4", occUnc, 0, threshold, rate, ratio, option4Listener, duration, skip);
                }
            }

        }

        if(opt1) option1Listener.close();
        if(opt2) option2Listener.close();
        if(opt3) option3Listener.close();
        if(opt4) option4Listener.close();
        if(opt5) option5Listener.close();
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
        final String base = String.format("experiments/streams/%.2f-%.2f-%s_", occUnc, attrUnc, rate);

        // Register streams
        manager.registerStreamFromFile(root + base + ratio + "_ce.trigs", "http://base/stream/ce");
        manager.registerStreamFromFile(root + base + ratio + "_hr.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + ratio + "_br.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + ratio + "_ox.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "experiments/queries/correctness/" + type + ".rspqlstar";

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
