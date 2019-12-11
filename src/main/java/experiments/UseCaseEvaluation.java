package experiments;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.log4j.Logger;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static se.liu.ida.rspqlstar.util.Utils.*;

public class UseCaseEvaluation {
    private static Logger logger = Logger.getLogger("ResultLogger");
    public static String root = new File("").getAbsolutePath();

    public static void main(String[] args) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();

        // Namespaces
        String rspuNs = "http://w3id.org/rsp/rspu#";
        String bnNs = "http://www.example.org/ecare#";

        // Bayes
        BayesianNetwork.loadNetwork("http://example.org/bn/farida", bnNs, root + "/data/use-case/farida.xdsl");
        FunctionRegistry.get().put(rspuNs + "belief", BayesianNetwork.belief);
        FunctionRegistry.get().put(rspuNs + "map", BayesianNetwork.map);
        FunctionRegistry.get().put(rspuNs + "mle", BayesianNetwork.mle);
        //x100g

        // Fuzzy logic
        FunctionRegistry.get().put(rspuNs + "zadehConjunction", ZadehLogic.conjunction);
        FunctionRegistry.get().put(rspuNs + "zadehDisjunction", ZadehLogic.disjunction);
        FunctionRegistry.get().put(rspuNs + "zadehNegation", ZadehLogic.negation);
        FunctionRegistry.get().put(rspuNs + "zadehImplication", ZadehLogic.implication);

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

        logger.info("unc_type\ttime_avg\ttime_stddev\tmemory\n");
        long timeOutAfter = 1000 * 60 * 3; // calc only by the last minute
        // Note: For high stream rates performance will deteriorate over time.

        // Probability distribution query
        String[] types = new String[]{
                "baseline",
                "probability",
                "bayes",
                "fuzzy"
        };

        for (String type : types) {
            logger.info(String.format("%s\t", type));
            run(root + "/data/use-case/query-" + type + ".rspqlstar", type, timeOutAfter, ref_time);
            reset();
        }
    }

    public static void run(String queryFile, String type, long timeOutAfter, long ref_time) throws IOException {
        TimeUtil.setOffset(new Date().getTime() - ref_time);
        final String qString = readFile(queryFile);
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();

        // Load base data
        System.err.println("Loading base data: " + root + "/data/use-case/base-data.ttl");
        RDFParser.create()
                .base("http://base/")
                .source(root + "/data/use-case/base-data.ttl")
                .checking(false)
                .lang(LangTrigStar.TRIGSTAR)
                .parse(sdg.getBaseDataset());


        // Add stream
        final RDFStarStream rdfStream1 = new RDFStarStream("http://ecareathome/stream/activity");
        final RDFStarStream rdfStream2 = new RDFStarStream("http://ecareathome/stream/heartrate");
        final RDFStarStream rdfStream3 = new RDFStarStream("http://ecareathome/stream/breathing");
        final RDFStarStream rdfStream4 = new RDFStarStream("http://ecareathome/stream/oxygen");
        final RDFStarStream rdfStream5 = new RDFStarStream("http://ecareathome/stream/temperature");
        sdg.registerStream(rdfStream1);
        sdg.registerStream(rdfStream2);
        sdg.registerStream(rdfStream3);
        sdg.registerStream(rdfStream4);
        sdg.registerStream(rdfStream5);

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // Start streams
        StreamFromFile s1 = new StreamFromFile(rdfStream1, root + "/data/use-case/streams/activity.trigs", 0);
        StreamFromFile s2 = new StreamFromFile(rdfStream2, root + "/data/use-case/streams/heart.trigs", 0);
        StreamFromFile s3 = new StreamFromFile(rdfStream3, root + "/data/use-case/streams/breathing.trigs", 0);
        StreamFromFile s4 = new StreamFromFile(rdfStream4, root + "/data/use-case/streams/oxygen.trigs", 0);
        StreamFromFile s5 = new StreamFromFile(rdfStream5, root + "/data/use-case/streams/temperature.trigs", 0);
        s1.start();
        s2.start();
        s3.start();
        s4.start();
        s5.start();

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(timeOutAfter);
            s1.stop();
            s2.stop();
            s3.stop();
            s4.stop();
            s5.stop();
            qexec.stopContinuousSelect();

            long[] results = asArray(qexec.executionTimes);
            results = Arrays.copyOfRange(results, 10, results.length-1);
            logger.info(calculateMean(results)/1000_000.0);
            logger.info("\t");
            logger.info(calculateStandardDeviation(results)/1000_000.0);
            logger.info("\t");
            logger.info(getReallyUsedMemory() + " MB");
            logger.info("\n");
        }).start();

        // Start query
        PrintStream ps = new PrintStream(new FileOutputStream(new File(String.format("output/use-cae-%s.txt", type))));
        //ps = System.out;

        ps.println(type + " with stream rate 1 event/s");
        qexec.execContinuousSelect(ps, ref_time);
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
