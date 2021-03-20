package evaluation;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.function.LazyNodeValue;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Evaluation {
    private static Logger logger = Logger.getLogger(Evaluation.class);
    public final static String root = new File("").getAbsolutePath() + "/";
    public PrintStream results;
    public PrintStream executions;
    public static String refTime = "2021-03-01T10:00:00";
    public static int numberOfResults = 10;
    public static int warmUp = 5;


    public static void main(String[] args) throws ParseException, IOException {
        RSPQLStarEngineManager.init();
        Evaluation evaluation = new Evaluation();
        //evaluation.query1();
        //evaluation.query1_inverse();

        evaluation.query2();
        //evaluation.query2_inverse();

        //evaluation.runQuery2(); // extend window to two seconds, double time
        //evaluation.runQuery3();
        //evaluation.runQuery4(); // extend window to two seconds, double time (before 2!)

        // need to be updated!
        //evaluation.runQuery5();
        //evaluation.runQuery6();
        //evaluation.runQuery7();
        //evaluation.runQuery8();
    }
    public void query1() throws IOException, ParseException {
        int computedEvery = 1000;
        results = new PrintStream(new FileOutputStream("results/q1-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q1-executions.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ 0.251, 0.248, 0.226, 0.121, 0.026 };
        double[] thresholds = new double[]{ 0, 0.01, 0.1, 0.5, 0.9 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/q1.rspqlstar", // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                -1, // throttle execution
                true, // ox stream 1
                false, // ox stream 2
                true, // temp stream 1
                false // temp stream 2
        );

        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;

        // RSPU filter push
        config.rspuFilterPull = false;
        run(config, duration);

        // RSPU filter pull
        config.rspuFilterPull = true;
        run(config, duration);

        results.close();
        executions.close();
    }

    public void query1_inverse() throws IOException, ParseException {
        int computedEvery = 1000;
        results = new PrintStream(new FileOutputStream("results/q1_inverse-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q1_inverse-executions.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{ 0.251, 0.248, 0.226, 0.121, 0.026 };
        double[] thresholds = new double[]{ 0, 0.01, 0.1, 0.5, 0.9 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/q1_inverse.rspqlstar", // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                -1, // throttle execution
                true, // ox stream 1
                false, // ox stream 2
                true, // temp stream 1
                false // temp stream 2
        );
        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;

        // RSPU filter push
        config.rspuFilterPull = false;
        run(config, duration);

        // RSPU filter pull
        config.rspuFilterPull = true;
        run(config, duration);

        results.close();
        executions.close();
    }

    public void query2() throws IOException, ParseException {
        int computedEvery = 4000;
        results = new PrintStream(new FileOutputStream("results/q2-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q2-executions.csv", true));
        printExecutionsHeader();
        double[] selectivities = new double[]{ 0.251, 0.248, 0.226, 0.121, 0.026 };
        double[] thresholds = new double[]{ 0, 0.01, 0.1, 0.5, 0.9 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/q2.rspqlstar", // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                -1, // throttle execution
                false, // ox stream 1
                true, // ox stream 2
                true, // temp stream 1
                false // temp stream 2
        );

        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;

        // RSPU filter push
        config.rspuFilterPull = false;
        run(config, duration);

        // RSPU filter pull
        config.rspuFilterPull = true;
        run(config, duration);

        results.close();
        executions.close();
    }

    public void query2_inverse() throws IOException, ParseException {
        int computedEvery = 4000;
        results = new PrintStream(new FileOutputStream("results/q2_inverse-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q2_inverse-executions.csv", true));
        printExecutionsHeader();
        double[] selectivities = new double[]{ 0.251, 0.248, 0.226, 0.121, 0.026 };
        double[] thresholds = new double[]{ 0, 0.01, 0.1, 0.5, 0.9 };

        // Basic configuration, modify between runs
        ExperimentConfiguration config = new ExperimentConfiguration(
                "resources/queries/q2_inverse.rspqlstar", // query file
                numberOfResults, // number of results
                warmUp, // warm up
                true, // cache
                true, // lazy vars
                true, // RSPU filter pull
                thresholds, // thresholds
                selectivities, // equivalent selectivities
                -1, // throttle execution
                true, // ox stream 1
                false, // ox stream 2
                false, // temp stream 1
                true // temp stream 2
        );

        long duration = (config.warmUp+config.numberOfResults) * computedEvery + 5000;

        // RSPU filter push
        config.rspuFilterPull = false;
        run(config, duration);

        // RSPU filter pull
        config.rspuFilterPull = true;
        run(config, duration);

        // RSPU filter push, cache off
        config.rspuFilterPull = false;
        config.useCache = false;
        run(config, duration);

        // RSPU filter pull, cache off
        config.rspuFilterPull = true;
        config.useCache = false;
        run(config, duration);

        results.close();
        executions.close();
    }

    public void run(ExperimentConfiguration config, long duration) throws ParseException, IOException {
        config.load();

        for(int i=0; i < config.thresholds.length; i++) {
            logger.info("# " + config.queryFile + " " + (i+1) + " of " + config.thresholds.length +
                    " : rspuPull=" + config.rspuFilterPull + " lazyVars=" + config.useLazyVars);
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
            // set threshold

            String query = new String(Files.readAllBytes(Paths.get(config.queryFile)));
            query = query.replaceAll("\\$P", "" + config.thresholds[i]);
            // register query
            final RSPQLStarQueryExecution q = manager.registerQuery(query);
            // start listening
            final ResultWriterStream listener = new ResultWriterStream(results, executions, config, i);
            q.setListener(listener);

            TimeUtil.silentSleep(duration);
            System.out.println("Stop execution...");
            manager.stop();
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