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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TestLoadData {
    private static Logger logger = Logger.getLogger(TestLoadData.class);
    public final static String root = new File("").getAbsolutePath() + "/";
    public PrintStream results;
    public PrintStream executions;

    public static void main(String[] args) throws ParseException, IOException {
        RSPQLStarEngineManager.init();
        TestLoadData testLoadData = new TestLoadData();
        testLoadData.runQuery1();
    }
    public void runQuery1() throws IOException, ParseException {
        results = new PrintStream(new FileOutputStream("results/q1-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q1-executions.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{0, 0.1, 0.5, 0.9, 1};
        ExperimentConfiguration config1 = new ExperimentConfiguration(false, true, selectivities);
        ExperimentConfiguration config2 = new ExperimentConfiguration(true, true, selectivities);
        ExperimentConfiguration config3 = new ExperimentConfiguration(true, false, selectivities);
        ExperimentConfiguration config4 = new ExperimentConfiguration(false, false, selectivities);
        query1(config1);
        query1(config2);
        query1(config3);
        query1(config4);
        results.close();
        executions.close();
    }

    public void runQuery2() throws IOException, ParseException {
        results = new PrintStream(new FileOutputStream("results/q2-results.csv", true));
        executions = new PrintStream(new FileOutputStream("results/q2-executions.csv", true));
        printExecutionsHeader();

        double[] selectivities = new double[]{0, 0.1, 0.5, 0.9, 1};
        ExperimentConfiguration config1 = new ExperimentConfiguration(false, true, selectivities);
        ExperimentConfiguration config2 = new ExperimentConfiguration(true, true, selectivities);
        ExperimentConfiguration config3 = new ExperimentConfiguration(true, false, selectivities);
        ExperimentConfiguration config4 = new ExperimentConfiguration(false, false, selectivities);
        query2(config1);
        query2(config2);
        query2(config3);
        query2(config4);
        results.close();
        executions.close();
    }

    public void query1(ExperimentConfiguration config) throws IOException, ParseException {
        for(double selectivity: config.selectivities) {
            reset();
            RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = config.rspuFilterPull;
            RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE = config.optimization;

            String refTime = "2021-03-01T10:00:00";
            final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
            final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
            manager.loadData("resources/data/static.trigstar");
            manager.registerStreamFromFile("resources/data/ox1.trigstar", "http://example.org/ox1");
            manager.registerStreamFromFile("resources/data/ox2.trigstar", "http://example.org/ox2");

            String query = new String(Files.readAllBytes(Paths.get("resources/queries/q1.rspqlstar")));
            query = query.replaceAll("\\$P", "" + selectivity);

            // register query
            final RSPQLStarQueryExecution q = manager.registerQuery(query);

            // start listening
            final ResultWriterStream listener = new ResultWriterStream(this.results, this.executions, config, Double.toString(selectivity), 20);
            q.setListener(listener);

            TimeUtil.silentSleep(80000);
            System.out.println("Stopping!");
            manager.stop();
        }
    }

    public void query2(ExperimentConfiguration config) throws IOException, ParseException {
        for(double selectivity: config.selectivities) {
            reset();
            RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = config.rspuFilterPull;
            RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE = config.optimization;

            String refTime = "2021-03-01T10:00:00";
            final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
            final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
            manager.loadData("resources/data/static.trigstar");
            manager.registerStreamFromFile("resources/data/ox1.trigstar", "http://example.org/ox1");
            manager.registerStreamFromFile("resources/data/ox2.trigstar", "http://example.org/ox2");

            String query = new String(Files.readAllBytes(Paths.get("resources/queries/q2.rspqlstar")));
            query = query.replaceAll("\\$P", "" + selectivity);

            // register query
            final RSPQLStarQueryExecution q = manager.registerQuery(query);

            // start listening
            final ResultWriterStream listener = new ResultWriterStream(this.results, this.executions, config, Double.toString(selectivity), 20);
            q.setListener(listener);

            TimeUtil.silentSleep(80000);
            System.out.println("Stopping!");
            manager.stop();
        }
    }

    public void query3(ExperimentConfiguration config) throws IOException, ParseException {
        for(double selectivity: config.selectivities) {
            reset();
            RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = config.rspuFilterPull;
            RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE = config.optimization;

            String refTime = "2021-03-01T10:00:00";
            final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
            final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
            manager.loadData("resources/data/static.trigstar");
            manager.registerStreamFromFile("resources/data/ox1.trigstar", "http://example.org/ox1");
            manager.registerStreamFromFile("resources/data/ox2.trigstar", "http://example.org/ox2");
            manager.registerStreamFromFile("resources/data/temp1.trigstar", "http://example.org/temp1");
            manager.registerStreamFromFile("resources/data/temp2.trigstar", "http://example.org/temp2");

            String query = new String(Files.readAllBytes(Paths.get("resources/queries/q3.rspqlstar")));
            query = query.replaceAll("\\$P", "" + selectivity);

            // register query
            final RSPQLStarQueryExecution q = manager.registerQuery(query);

            // start listening
            final ResultWriterStream listener = new ResultWriterStream(this.results, this.executions, config, Double.toString(selectivity), 20);
            q.setListener(listener);

            TimeUtil.silentSleep(80000);
            System.out.println("Stopping!");
            manager.stop();
        }
    }


    public static void reset(){
        RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE = true;
        RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = false;
        LazyNodeValue.THROTTLE_EXECUTION = -1;
        RSPQLStarQueryExecution.CLEAR_CACHE_BETWEEN_EXECUTIONS = true;
        System.gc();
        TimeUtil.silentSleep(3000);
    }

    public void printExecutionsHeader(){
        String sep = "\t";
        executions.print("execution_time_ms");
        executions.print(sep);
        executions.print("number_of_results");
        executions.print(sep);
        executions.print("h5");
        executions.print(sep);
        executions.print("optimization");
        executions.print(sep);
        executions.print("params");
        executions.print("\n");
    }
}