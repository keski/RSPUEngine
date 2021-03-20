package evaluation;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.LazyNodeValue;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class EvaluationPart2 {
    private static Logger logger = Logger.getLogger(EvaluationPart2.class);
    public final static String root = new File("").getAbsolutePath() + "/";
    public PrintStream results;
    public PrintStream executions;
    public static String refTime = "2021-03-01T10:00:00";
    public static int numberOfResults = 3;
    public static int warmUp = 2;


    public static void main(String[] args) throws ParseException, IOException {
        RSPQLStarEngineManager.init();
        EvaluationPart2 evaluation = new EvaluationPart2();
        //evaluation.runWarmup();
        //evaluation.run();
        evaluation.test();
    }
    public void test() throws IOException, ParseException {
        int computedEvery = 10000;

        results = new PrintStream(new FileOutputStream("results/part2-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/part2-summaries.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ 1 };
        double[] thresholds = new double[]{    1 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/part2-query1.rspqlstar", // query file
                numberOfResults, // number of results
                1, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                50, // throttle execution
                false, // ox stream 1
                false, // ox stream 2
                false, // temp stream 1
                false // temp stream 2
        );

        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;
        for(String join: new String[]{"join_001"}){
            config.join = join;
            run(config, duration);
        }

        results.close();
        executions.close();
    }

    public void run() throws IOException, ParseException {
        int computedEvery = 10000;

        results = new PrintStream(new FileOutputStream("results/part2-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/part2-summaries.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ 1, .9, .8, .7, .6, .5, .4, .3, .2, .1, 0 };
        double[] thresholds = new double[]{    1, .9, .8, .7, .6, .5, .4, .3, .2, .1, 0 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/part2-query1.rspqlstar", // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                50, // throttle execution
                false, // ox stream 1
                false, // ox stream 2
                false, // temp stream 1
                false // temp stream 2
        );

        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;

        // Join partners RHS
        // RHS = 0.01       1 to .001
        // RHS = 0.1        1 to .01
        // RHS = 1          1 to 1
        // RHS = 10         1 to 100
        // RHS = 100        1 to 1000
        for(String join: new String[]{"join_001", "join_01", "join_10", "join_100"}){
            config.join = join;
            run(config, duration);
        }

        results.close();
        executions.close();
    }

    public void run(ExperimentConfiguration config, long duration) throws ParseException, IOException {
        for (boolean h5 : new boolean[]{false, true}) {
            for(int i=0; i < config.thresholds.length; i++) {
                config.rspuFilterPull = h5;
                config.load();
                logger.info("# THRESHOLD: " + config.thresholds.length + " : h5: " + config.rspuFilterPull);

                final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
                final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);

                manager.registerStreamFromFile("resources/data/events1.trigstar", "http://example.org/events1");
                manager.registerStreamFromFile("resources/data/events2.trigstar", "http://example.org/events2");


                double threshold = config.thresholds[i];
                String query = new String(Files.readAllBytes(Paths.get(config.queryFile)));
                query = query.replaceAll("\\$THRESHOLD", "" + threshold);
                query = query.replaceAll("\\$JOIN", "" + config.join);
                query = query.replaceAll("\\$QUERY_NAME", config.join + "_" + threshold + "_" + config.rspuFilterPull);

                System.out.println(query);

                // register query
                final RSPQLStarQueryExecution q = manager.registerQuery(query);
                // start listening
                final ResultWriterStream listener = new ResultWriterStream(results, executions, config, i);
                q.setListener(listener);

                System.out.println("Run duration: " + duration + " ms");
                TimeUtil.silentSleep(duration);
                System.out.println("Stop execution...");
                manager.stop();
                //System.err.println("Cache hits: " + LazyNodeValue.cacheHits);
                //System.err.println("Cache size:" + LazyNodeValue.cache.size());
                //System.err.println("Cache resolved lazy vars: " + LazyNodeValue.resolvedCounter);
                //System.err.println("Cache lazy vars: " + LazyNodeValue.lazyVarCounter);
            }
        }
    }

    public void printExecutionsHeader(){
        String sep = "\t";
        executions.print("avg_ms");
        executions.print(sep);
        executions.print("min_ms");
        executions.print(sep);
        executions.print("max_ms");
        executions.print(sep);
        executions.print("avg_results");
        executions.print(sep);
        executions.print("rspu_pull");
        executions.print(sep);
        executions.print("use_lazy_vars");
        executions.print(sep);
        executions.print("use_cache");
        executions.print(sep);
        executions.print("threshold");
        executions.print(sep);
        executions.print("selectivity");
        executions.print(sep);
        executions.print("join_param");
        executions.print("\n");
    }
}