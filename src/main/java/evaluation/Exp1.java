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
        BayesianNetwork.loadNetwork("http://ecare/bn#heart-attack", root + "data/experiments/heart-attack.bn");

        final String resultFile = root + "data/experiments/results/correctness.csv";
        final ResultWriterStream listener = new ResultWriterStream(new PrintStream(new File (resultFile)));
        //final ResultWriterStream listener = new ResultWriterStream(System.out);

        // Params
        final int[] rates = new int[]{1000};
        final double[] occUncList = new double[]{.001, .01, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5};
        final double[] attrUncList = new double[]{.001, .01, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5};
        final double[] thresholds = new double[]{ 0, .001, .05, .1, .15, .2, .25, .3, .35, .4, .45, .5, .55, .6, .65, .7, .75, .8, .85, .9, .95, .99, .999};
        final int total = (occUncList.length + attrUncList.length*2) * thresholds.length;

        int progress = 0;
        for (int rate : rates) {
            for (double occUnc : occUncList) {
                for (double threshold : thresholds) {
                    logger.info(String.format("Progress: %s/%s", progress, total));
                    progress++;
                    listener.setSkip(0);
                    correctness("occurrence", occUnc, 0, threshold, rate, listener, 2000);
                    TimeUtil.silentSleep(100);
                }
            }
            for (double attrUnc : attrUncList) {
                for (double threshold : thresholds) {
                    logger.info(String.format("Progress: %s/%s", progress, total));
                    progress++;
                    listener.setSkip(0);
                    correctness("attribute", 0, attrUnc, threshold, rate, listener, 2000);
                    TimeUtil.silentSleep(100);
                }
            }
            for (double attrUnc : attrUncList) {
                for (double threshold : thresholds) {
                    logger.info(String.format("Progress: %s/%s", progress, total));
                    progress++;
                    listener.setSkip(0);
                    correctness("occurrence-attribute", 0, attrUnc, threshold, rate, listener, 2000);
                    TimeUtil.silentSleep(100);
                }
            }
        }
        listener.close();
    }

    /**
     * Correctness test for a given uncertainty config, using a specified threshold.
     *
     * @param type Query type: "occurrence", "attribute", or "occurrence-attribute"
     * @param occUnc Degree of occurrence uncertainty. Expressed as the likelihood that the virtual evidence does not
     *               correspond to the value of its parent.
     * @param attrUnc Degree of attribute uncertainty. Expressed as number the percentile of values outside the value
     *                thresholds.
     * @param threshold Confidence threshold. Use to filter results.
     * @param rate Stream rate. Expressed as number of events per second per stream.
     * @param duration Duration of test.
     */
    public static void correctness(String type, double occUnc, double attrUnc, double threshold, int rate,
                                   ResultWriterStream listener, long duration){
        logger.info(String.format("Running: %.3f-%.3f-%s", occUnc, attrUnc, rate));

        final long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime + 100);

        final String base = String.format("data/experiments/streams/%.3f-%.3f-%s-", occUnc, attrUnc, rate);
        // Register streams
        manager.registerStreamFromFile(root + base + "HA.trigs", "http://base/stream/ha");
        manager.registerStreamFromFile(root + base + "HR.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(root + base + "BR.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(root + base + "OX.trigs", "http://base/stream/ox");

        // Register query
        final String queryFile  = root + "data/experiments/queries/query-" + type + ".rspqlstar";

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
        manager.stop();
        listener.flush();
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
