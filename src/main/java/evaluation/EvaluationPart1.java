package evaluation;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.LazyNodeCache;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Lazy_Node_Concrete_WithID;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class EvaluationPart1 {
    private static Logger logger = Logger.getLogger(EvaluationPart1.class);
    public final static String root = new File("").getAbsolutePath() + "/";
    public PrintStream results;
    public PrintStream executions;
    public PrintStream times;
    public static String refTime = "2021-03-01T10:00:00";
    public static int numberOfResults = 20;
    public static int warmUp = 5;


    public static void main(String[] args) throws ParseException, IOException {
        RSPQLStarEngineManager.init();
        EvaluationPart1 evaluation = new EvaluationPart1();

        // testing
        //warmUp = 4;
        //numberOfResults = 2;

        // FILTER
        //evaluation.query("Q1a.rspqlstar", false, true, false, true, 4000); // tests work
        //evaluation.query("Q1b.rspqlstar", false, true, false, true, 4000); // tests work
        //evaluation.query("Q2a.rspqlstar", true, true, true, true, 8000); // tests work
        //evaluation.query("Q2b.rspqlstar", true, true, true, true, 8000); // tests work
        //evaluation.query("Q3a.rspqlstar", true, true, true, false, 4000); // tests work
        //evaluation.query("Q3b.rspqlstar", true, true, true, false, 4000); // tests work

        // BIND
        //evaluation.query_bind("Q4a.rspqlstar", false, true, false, true, 4000); // tests work
        //evaluation.query_bind("Q4b.rspqlstar", false, true, false, true, 4000); // tests work
        //evaluation.query_bind("Q5a.rspqlstar", true, true, true, true, 8000); // tests work
        //evaluation.query_bind("Q5b.rspqlstar", true, true, true, true, 8000); // tests work
        //evaluation.query_bind("Q6a.rspqlstar", true, true, true, false, 4000); // tests work
        //evaluation.query_bind("Q6b.rspqlstar", true, true, true, false, 4000); // tests work
    }

    public void query(String fileName, boolean stream1, boolean stream2, boolean stream3, boolean stream4, long computedEvery) throws IOException, ParseException {
        String prefix = fileName.split("\\.")[0];
        results = new PrintStream(new FileOutputStream("resources/results/" + prefix + "-results.csv", true));
        executions = new PrintStream(new FileOutputStream("resources/results/" + prefix + "-summaries.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ -1, -1, -1, -1, -1, -1 };
        double[] thresholds = new double[]{     0, .2, .4, .6, .8,  1 };

        for (boolean h5 : new boolean[]{true, false}) { // change order
            // Basic configuration, modify between runs
            ExperimentConfiguration config = new ExperimentConfiguration(
                    "resources/queries/" + fileName, // query file
                    numberOfResults, // number of results
                    warmUp, // warm up
                    true, // cache
                    false, // lazy vars
                    h5, // RSPU filter pull
                    thresholds, // thresholds
                    selectivities, // equivalent selectivities
                    -1, // throttle execution
                    stream1, // ox stream 1
                    stream2, // ox stream 2
                    stream3, // temp stream 1
                    stream4 // temp stream 2
            );
            run(config, computedEvery);
        }
        results.close();
        executions.close();
    }

    public void query_bind(String fileName, boolean stream1, boolean stream2, boolean stream3, boolean stream4, long computedEvery) throws IOException, ParseException {
        String prefix = fileName.split("\\.")[0];
        results = new PrintStream(new FileOutputStream("resources/results/" + prefix + "-results.csv", true));
        executions = new PrintStream(new FileOutputStream("resources/results/" + prefix + "-summaries.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ -1 };
        double[] thresholds = new double[]{    -1 };

        for (boolean lazyVars : new boolean[]{true, false}) {
            for(boolean cache: new boolean[]{true, false}){
                // Basic configuration, modify between runs
                ExperimentConfiguration config = new ExperimentConfiguration(
                        "resources/queries/" + fileName, // query file
                        numberOfResults, // number of results
                        warmUp, // warm up
                        cache, // cache
                        lazyVars, // lazy vars
                        false, // RSPU filter pull
                        thresholds, // thresholds
                        selectivities, // equivalent selectivities
                        -1, // throttle execution
                        stream1, // ox stream 1
                        stream2, // ox stream 2
                        stream3, // temp stream 1
                        stream4 // temp stream 2
                );
                run(config, computedEvery);
            }
        }
        results.close();
        executions.close();
    }

    public void run(ExperimentConfiguration config, long computedEvery) throws ParseException, IOException {
        for (int i = 0; i < config.thresholds.length; i++) {
            config.load();
            long duration = (config.warmUp + config.numberOfResults) * computedEvery + 5000;
            double threshold = config.thresholds[i];

            logger.info("# Threshold: " + threshold + ", H5: " + config.rspuFilterPull);

            final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
            final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
            manager.loadData("resources/data/static.trigstar");
            if (config.oxStream1) {
                manager.registerStreamFromFile("resources/data/ox1.trigstar", "http://example.org/ox1");
            }
            if (config.oxStream2) {
                manager.registerStreamFromFile("resources/data/ox2.trigstar", "http://example.org/ox2");
            }
            if (config.tempStream1) {
                manager.registerStreamFromFile("resources/data/temp1.trigstar", "http://example.org/temp1");
            }
            if (config.tempStream2) {
                manager.registerStreamFromFile("resources/data/temp2.trigstar", "http://example.org/temp2");
            }

            String query = new String(Files.readAllBytes(Paths.get(config.queryFile)));
            query = query.replaceAll("\\$P", "" + config.thresholds[i]);
            query = query.replaceAll("\\$QUERY_NAME",
                    threshold + "_" +
                    config.rspuFilterPull + "_" +
                    config.useCache + "_" +
                    config.useLazyVars + "_" +
                    config.queryFile);

            System.out.println(query);

            // register query
            final RSPQLStarQueryExecution q = manager.registerQuery(query);
            // start listening
            final ResultWriterStream listener = new ResultWriterStream(results, executions, config, i);
            q.setListener(listener);

            System.out.println("Run duration: " + duration + " ms");
            TimeUtil.silentSleep(duration);
            System.out.println("Stop execution...");
            System.err.println("Cache size:" + LazyNodeCache.size());
            System.err.println("Registered lazy nodes: " + Lazy_Node_Concrete_WithID.registeredLazyNodes);
            System.err.println("Resolved lazy nodes: " + Lazy_Node_Concrete_WithID.resolvedLazyNodes);
            System.err.println("Cache hits: " + Lazy_Node_Concrete_WithID.cacheHits);
            manager.stop(10000);
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
        executions.print("\n");
    }
}