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
        //final ResultWriterStream listener = new ResultWriterStream(new PrintStream(new File (resultFile)));
        final ResultWriterStream listener = new ResultWriterStream(System.out);

        final double[] occList = new double[]{.0};
        final int[] attrList = new int[]{1};
        final float[] thresholds = new float[]{ 0f, .5f, .9f};
        int total = occList.length * attrList.length * thresholds.length;
        int progress = 0;
        for(double o : occList){
            for(int a: attrList) {
                for (float threshold : thresholds) {
                    logger.info(String.format("Progress: %s/%s", progress, total));
                    progress++;
                    listener.setSkip(0);

                    if(o > 0 && a > 0){
                        logger.info("Skip combo");
                        continue;
                    }

                    correctness(o, a, threshold, 1, listener, 5000);
                    TimeUtil.silentSleep(100);
                }
            }
        }
        listener.close();
    }

    /**
     * Correctness test for a given uncertainty config, using the specified threshold. TP, TN, FP and FN are recorded.
     * @param occUnc Uncertainty degree for occurrence. Expressed as likelihood ratio between virtual evidence and
     *               the corresponding node in the BN.
     * @param attrUnc Uncertainty degree for attributes. Expressed as number of SDEs from reference value
     * @param threshold Confidence threshold
     * @param rate Stream rate (events/s)
     * @param duration Stop after this many milliseconds
     */
    public static void correctness(double occUnc, int attrUnc, float threshold, int rate,
                                   ResultWriterStream listener, long duration){

        long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime - 100);

        String filePrefix = String.format(root + "data/experiments/streams/%s-%s-%s-", occUnc, attrUnc, rate);
        // Register streams
        manager.registerStreamFromFile(filePrefix + "HA.trigs", "http://base/stream/ha");
        manager.registerStreamFromFile(filePrefix + "HR.trigs", "http://base/stream/hr");
        manager.registerStreamFromFile(filePrefix + "BR.trigs", "http://base/stream/br");
        manager.registerStreamFromFile(filePrefix + "OX.trigs", "http://base/stream/ox");

        // Register query
        String queryFile  = root + "data/experiments/queries/query";
        queryFile += occUnc > 0 ? "-occurrence" : "";
        queryFile += attrUnc > 0 ? "-attribute" : "";
        queryFile += ".rspqlstar";
        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$THRESHOLD$", Float.toString(threshold));
        qString = qString.replace("$OCCURRENCE$", Double.toString(occUnc));
        qString = qString.replace("$ATTRIBUTE$", Integer.toString(attrUnc));

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
