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
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger(PerformanceEvaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main) throws IOException {
        final long t0 = System.currentTimeMillis();

        final double occUnc =  .05;
        final double attrUnc = .05;

        // Test setup parameters
        //final int[] rates = {100};
        //final int duration = 10 * 1000; // 5 minutes
        //final int skip = 5;

        // Experiment parameters
        final int[] rates = {3000, 2500, 2000, 1500, 1000, 500, 100};
        final int duration = 3 * 60 * 1000; // 3 minutes
        final int skip = 30;

        if(false){
            Generator.run(duration + (skip + 5) * 1000, rates, new double[]{occUnc}, new double[]{attrUnc});
            return;
        }
        RSPQLStarEngineManager.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#medical", root + "experiments/medical.bn");

        // Activate/de-active runs
        boolean baseline = false; // done
        boolean option1 = false; // done
        boolean option2 = false; // done
        boolean option3 = false; // done
        boolean option4 = false; // done
        boolean option5 = false; //done

        // Files and listeners
        final String f0 = root + "experiments/results/performance/baseline.csv";
        final PerformanceWriterStream baselineListener = baseline ? new PerformanceWriterStream(f0) : null;
        final String f1 = root + "experiments/results/performance/option1.csv";
        final PerformanceWriterStream option1Listener = option1 ? new PerformanceWriterStream(f1) : null;
        final String f2 = root + "experiments/results/performance/option2.csv";
        final PerformanceWriterStream option2Listener = option2 ? new PerformanceWriterStream(f2) : null;
        final String f3 = root + "experiments/results/performance/option3.csv";
        final PerformanceWriterStream option3Listener = option3 ? new PerformanceWriterStream(f3) : null;
        final String f4 = root + "experiments/results/performance/option4.csv";
        final PerformanceWriterStream option4Listener = option4 ? new PerformanceWriterStream(f4) : null;
        final String f5 = root + "experiments/results/performance/option5.csv";
        final PerformanceWriterStream option5Listener = option5 ? new PerformanceWriterStream(f5) : null;

        /*
        // Warm up. Allocate sufficient VM memory to prevent reallocation during run.
        final boolean runWarmUp = false;
        if(runWarmUp) {
            final int maxRate = 3000;
            final ContinuousListener listener = new ResultWriterStream(System.out);

            if (baseline) performance("baseline", 0, attrUnc, maxRate, listener, duration, skip);
            if (option1) performance("option1", 0, attrUnc, maxRate, listener, duration, skip);
            if (option2) performance("option2", occUnc, 0, maxRate, listener, duration, skip);
            if (option3) performance("option3", occUnc, 0, maxRate, listener, duration, skip);
            if (option4) performance("option4", occUnc, 0, maxRate, listener, duration, skip);
            if (option5) performance("option5", 0, attrUnc, maxRate, listener, duration, skip);

            logger.info("------------ Warm up finished, waiting 20 seconds ------------ ");
            TimeUtil.silentSleep(20000);
        }*/

        // Real run
        for (int rate : rates) {
            if (baseline) performance("baseline", 0, attrUnc, rate, baselineListener, duration, skip);
            if (option1) performance("option1", 0, attrUnc, rate, option1Listener, duration, skip);
            if (option2) performance("option2", occUnc, 0, rate, option2Listener, duration, skip);
            if (option3) performance("option3", occUnc, 0, rate, option3Listener, duration, skip);
            if (option4) performance("option4", occUnc, 0, rate, option4Listener, duration, skip);
            if (option5) performance("option5", 0, attrUnc, rate, option5Listener, duration, skip);
        }

        if(baseline) baselineListener.close();
        if(option1) option1Listener.close();
        if(option2) option2Listener.close();
        if(option3) option3Listener.close();
        if(option4) option4Listener.close();
        if(option5) option5Listener.close();

        logger.info("Finished in " + (System.currentTimeMillis() - t0)/1000 + " seconds");
        Utils.beep();
    }

    /**
     * Performance test for a given uncertainty config.
     *
     * @param option Query option: "occurrence", "attribute", or "combination"
     * @param occUnc Degree of occurrence uncertainty. Expressed as the likelihood that the virtual evidence does not
     *               correspond to the value of its parent.
     * @param attrUnc Degree of attribute uncertainty. Expressed as number the percentile of values outside the value
     *                limits.
     * @param rate Stream rate. Expressed as number of events per second per stream.
     * @param duration Duration of test.
     * @param skip The number of results to skip before logging results.
     */
    public static void performance(String option, double occUnc, double attrUnc, int rate,
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
        final String queryFile  = root + "experiments/queries/performance/" + option + ".rspqlstar";

        String qString = Utils.readFile(queryFile);
        qString = qString.replace("$RATE$", Double.toString(rate));
        final RSPQLStarQueryExecution q = manager.registerQuery(qString);

        // listen
        q.setListener(listener);
        // stop after
        TimeUtil.silentSleep(duration);
        listener.flush();
        manager.stop();
        // wait 2 seconds before reset to allow system to write out active results
        TimeUtil.silentSleep(2000);
        reset();
        logger.info("Execution stopped.");
        Utils.beep();
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
