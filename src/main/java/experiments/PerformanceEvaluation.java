package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.function.ZadehLogic;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;
import smile.Network;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static se.liu.ida.rspqlstar.util.Utils.*;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger("ResultLogger");
    public static String root = "/Users/robke04/Documents/Git_projects/RSPQLStar/RSPU/RSPUEngine/";

    public static void main(String[] args) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();

        // Namespaces
        String rspuNs = "http://w3id.org/rsp/rspu#";
        String bnNs = "http://www.example.org/ecare#";

        // Bayes
        //BayesianNetwork.loadNetwork("http://example.org/bn/medical-small", bnNs, root + "data/performance-eval/medical-small.xdsl");
        //BayesianNetwork.loadNetwork("http://example.org/bn/medical-large", bnNs, root + "data/performance-eval/medical-large.xdsl", Network.BayesianAlgorithmType.HENRION);
        FunctionRegistry.get().put(rspuNs + "belief", BayesianNetwork.belief);
        FunctionRegistry.get().put(rspuNs + "map", BayesianNetwork.map);
        //x100g


        // Fuzzy logic
        FunctionRegistry.get().put(rspuNs + "conjunction", ZadehLogic.conjunction);
        FunctionRegistry.get().put(rspuNs + "disjunction", ZadehLogic.disjunction);
        FunctionRegistry.get().put(rspuNs + "negation", ZadehLogic.negation);
        FunctionRegistry.get().put(rspuNs + "implication", ZadehLogic.implication);

        // Probability distribution
        FunctionRegistry.get().put(rspuNs + "lessThan", Probability.lessThan);
        FunctionRegistry.get().put(rspuNs + "lessThanOrEqual", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(rspuNs + "greaterThan", Probability.greaterThan);
        FunctionRegistry.get().put(rspuNs + "greaterThanOrEqual", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(rspuNs + "between", Probability.between);
        FunctionRegistry.get().put(rspuNs + "add", Probability.add);
        FunctionRegistry.get().put(rspuNs + "subtract", Probability.subtract);

        // Time offset should match the reference time used in the stream generator
        long ref_time = 1574881152000L;

        logger.info("unc_type\trate\ttime_avg\ttime_stddev\tmemory\n");
        long timeOutAfter = 1000 * 5 * 3;

        // Probability distribution query
        int[] rates = new int[]{100, 500, 1000, 1500, 2000, 2500, 3000, 3500};
        String[] unc_types = new String[]{
                "all",
                "baseline",
                "fuzzy",
                "probability",
                "bayes",
                "bayes-large"
        };

        for (int rate : rates) {
            for(String unc_type : unc_types){
                logger.info(String.format("%s\t%s\t", unc_type, rate));
                run(rate, unc_type, timeOutAfter, ref_time);
                reset();
            }
        }
    }

    public static void run(int rate, String unc_type, long timeOutAfter, long ref_time) throws IOException {
        TimeUtil.setOffset(new Date().getTime() - ref_time);

        String queryFile = String.format(root + "data/performance-eval/query-%s.rspqlstar", unc_type);
        final String qString = readFile(queryFile);
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();

        // Add stream
        final RDFStarStream rdfStream = new RDFStarStream("http://ecareathome/stream/heartrate");
        sdg.registerStream(rdfStream);

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // Start stream
        final String fileName = String.format(root + "data/performance-eval/streams/heart-%s.trigs", rate);
        final StreamFromFile stream = new StreamFromFile(rdfStream, fileName, 0);
        stream.start();

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(timeOutAfter);
            stream.stop();
            qexec.stop();

            long[] results = asArray(qexec.executionTimes);
            // log the last minute
            results = Arrays.copyOfRange(results, results.length-61, results.length-1);
            logger.info(calculateMean(results)/1000_000.0);
            logger.info("\t");
            logger.info(calculateStandardDeviation(results)/1000_000.0);
            logger.info("\t");
            logger.info(getReallyUsedMemory() + " MB");
            logger.info("\n");
        }).start();

        // Start query
        PrintStream ps = new PrintStream(new FileOutputStream(new File(String.format("output/%s-%s.txt", unc_type, rate))));
        //ps = System.out;
        System.out.println("# Running " + unc_type + " with stream rate " + rate + " event/s");

        ps.println(unc_type + " with stream rate " + rate + " event/s");
        qexec.execContinuousSelect();//, ref_time);
        ps.flush();
        ps.close();
    }

    /**
     * Reset storage to original state (clear everything).
     */
    public static void reset(){
        NodeDictionaryFactory.get().clear();
        ReferenceDictionaryFactory.get().clear();
        IdFactory.reset();
        VarDictionary.reset();
        // used make sure gc has been run
        getReallyUsedMemory();
        TimeUtil.silentSleep(5000);
    }

    public static long[] asArray(ArrayList<Long> list){
        long[] array = new long[list.size()];
        for(int i=0; i<list.size(); i++){
            array[i] = list.get(i);
        }
        return array;
    }
}
