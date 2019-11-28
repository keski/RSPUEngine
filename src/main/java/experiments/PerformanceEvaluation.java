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
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static se.liu.ida.rspqlstar.util.Utils.*;

public class PerformanceEvaluation {
    private static Logger logger = Logger.getLogger(PerformanceEvaluation.class);
    private static boolean log = false;

    public static void main(String[] args) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();

        // Namespaces
        String rspuNS = "http://w3id.org/rsp/rspu#";
        String rspuFnNs = "http://w3id.org/rsp/rspu/fn#";
        String bnNs = "http://w3id.org/rsp/rspu/bn#";

        // Bayes
        BayesianNetwork.loadNetwork("http://example.org/bn/farida", bnNs, "src/main/resources/use-case/farida.xdsl");
        FunctionRegistry.get().put(rspuFnNs + "belief", BayesianNetwork.belief);
        FunctionRegistry.get().put(rspuFnNs + "map", BayesianNetwork.map);

        // Fuzzy logic
        FunctionRegistry.get().put(rspuFnNs + "conjunction", ZadehLogic.conjunction);
        FunctionRegistry.get().put(rspuFnNs + "disjunction", ZadehLogic.disjunction);
        FunctionRegistry.get().put(rspuFnNs + "negation", ZadehLogic.negation);
        FunctionRegistry.get().put(rspuFnNs + "implication", ZadehLogic.implication);

        // Probability distribution
        FunctionRegistry.get().put(rspuFnNs + "lessThan", Probability.lessThan);
        FunctionRegistry.get().put(rspuFnNs + "lessThanOrEqual", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(rspuFnNs + "greaterThan", Probability.greaterThan);
        FunctionRegistry.get().put(rspuFnNs + "greaterThanOrEqual", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(rspuFnNs + "between", Probability.between);
        FunctionRegistry.get().put(rspuFnNs + "add", Probability.add);
        FunctionRegistry.get().put(rspuFnNs + "subtract", Probability.subtract);

        // Time offset should match the reference time used in the stream generator
        long offset = 1574881152000L;
        log = true;
        logger.info("type;stream rate;avg execution time;stddev;memory;#results\n");
        long timeOutAfter = 1000 * 60 * 5; // 30 seconds

        // Probability distribution query
        for(int rate=1; rate <= 1; rate++) { // events/sec
            reset();
            logger.info(String.format("PD;%s;", rate));
            TimeUtil.setOffset(new Date().getTime() - offset);
            run(rate, "performance-eval/query.rspqlstar", timeOutAfter);
        }

        /*
        for(int i=1; i < 11; i++) {
            reset();
            logger.info(String.format("Reification (naive);%s;", i));
            TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
            run(i, "reification", timeOutAfter, false);
        }

        for(int i=1; i < 11; i++) {
            reset();
            logger.info(String.format("RDFStar;%s;", i));
            TimeUtil.setOffset(new Date().getTime() - 1556617861000L);
            run(i, "rdfstar", timeOutAfter, false);
        }
        */
    }

    public static void run(int streamRate, String queryFile, long timeOutAfter) throws IOException {
        final String qString = readFile(queryFile);
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        // Stream
        final RDFStarStream rdfStream = new RDFStarStream("http://ecareathome/stream/heartrate");
        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();
        sdg.registerStream(rdfStream);

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // Start stream
        final String fileName = "performance-eval/heart.trigs";
        final StreamFromFile stream = new StreamFromFile(rdfStream, fileName, 1000, 1000/streamRate);
        stream.start();

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(timeOutAfter);
            stream.stop();
            qexec.stopContinuousSelect();

            if(log) {
                long[] results = asArray(qexec.executionTimes);
                results = Arrays.copyOfRange(results, 10, results.length);
                logger.info(calculateMean(results)/1000_000.0);
                logger.info(";");
                logger.info(calculateStandardDeviation(results)/1000_000.0);
                logger.info("\n");
            }
        }).start();

        // Start query
        PrintStream ps = new PrintStream(new ByteArrayOutputStream());
        ps = System.out;
        qexec.execContinuousSelect(ps);
    }

    /**
     * Reset storage to original state (clear everything).
     */
    public static void reset(){
        NodeDictionaryFactory.get().clear();
        IdFactory.reset();
        VarDictionary.reset();
        // used make sure gc has been run
        getReallyUsedMemory();
    }

    public static long[] asArray(ArrayList<Long> list){
        long[] array = new long[list.size()];
        for(int i=0; i<list.size(); i++){
            array[i] = list.get(i);
        }
        return array;
    }
}
