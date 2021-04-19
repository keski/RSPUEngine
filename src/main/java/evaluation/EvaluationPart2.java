package evaluation;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.LazyNodeCache;
import se.liu.ida.rspqlstar.function.LazyNodeValue;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Lazy_Node_Concrete_WithID;
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
import java.rmi.server.ExportException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class EvaluationPart2 {
    private static Logger logger = Logger.getLogger(EvaluationPart2.class);
    public final static String root = new File("").getAbsolutePath() + "/";
    public PrintStream results;
    public PrintStream executions;
    public static String refTime = "2021-03-01T10:00:00";
    public static int numberOfResults = 20;
    public static int warmUp = 5;
    public static double[] selectivities = new double[]{  1, .9, .8, .7, .6, .5, .4, .3, .2, .1, 0 };
    public static double[] thresholds = new double[]{     1, .9, .8, .7, .6, .5, .4, .3, .2, .1, 0 };
    public static String[] joinProps = new String[]{"join_100", "join_10", "join_01", "join_001"};

    // 11*4*25*2*2

    public static void main(String[] args) throws Exception {
        RSPQLStarEngineManager.init();
        EvaluationPart2 evaluation = new EvaluationPart2();

        // re-run
        // Q2: join_10
        // Q1: join_001

        joinProps = new String[]{"join_001", "join_01", "join_10", "join_100" };
        evaluation.run("Q-part2.rspqlstar");
    }

    public void run(String queryFile) throws Exception {
        String name = queryFile.split("\\.")[0];
        results = new PrintStream(new FileOutputStream("resources/results/" + name + "-results.csv", true));
        executions = new PrintStream(new FileOutputStream("resources/results/" + name + "-summaries1.csv", true));
        printExecutionsHeader();

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/" + queryFile, // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                -1, // throttle execution. Throttling should be handled with care, realistic simulations take approximately 2 ms only!
                false, // ox stream 1
                false, // ox stream 2
                false, // temp stream 1
                false // temp stream 2
        );

        // Join partners RHS to LHS
        // RHS = 0.01       1 to .01
        // RHS = 0.1        1 to .01
        // RHS = 10         1 to 10
        // RHS = 100        1 to 100
        for(String join: joinProps){
            config.join = join;
            run(config);
        }
        results.flush();
        executions.flush();
    }

    public void run(ExperimentConfiguration config) throws ParseException, IOException {
        for (boolean h5 : new boolean[]{true, false}) {
            for(int i=0; i < config.thresholds.length; i++) {
                int computedEvery = 4000;
                long duration = (config.warmUp + config.numberOfResults) * computedEvery + 5000;

                config.rspuFilterPull = h5;
                config.load();

                final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
                final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);

                manager.registerStreamFromFile("resources/data/events1.trigstar", "http://example.org/events1");
                manager.registerStreamFromFile("resources/data/events2.trigstar", "http://example.org/events2");

                double threshold = config.thresholds[i];
                // Query name must be unique!
                String[] path = config.queryFile.split("/");
                String queryName = path[path.length-1] + "_" + config.join + "_" + threshold + "_" + config.rspuFilterPull;
                logger.info("Running: " + queryName);
                String query = new String(Files.readAllBytes(Paths.get(config.queryFile)));
                query = query.replaceAll("\\$THRESHOLD", "" + threshold);
                query = query.replaceAll("\\$JOIN", "" + config.join);
                query = query.replaceAll("\\$QUERY_NAME", queryName);

                System.out.println(query);

                // register query
                final RSPQLStarQueryExecution q = manager.registerQuery(query);
                // start listening
                final ResultWriterStream listener = new ResultWriterStream(results, executions, config, i);
                q.setListener(listener);

                System.out.println("Run duration: " + duration + " ms");
                TimeUtil.silentSleep(duration);
                System.out.println("Stop execution...");
                manager.stop(5000);
                listener.close();
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