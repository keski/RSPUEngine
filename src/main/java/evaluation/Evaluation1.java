package evaluation;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import static se.liu.ida.rspqlstar.util.Utils.*;

public class Evaluation1 {
    public static String root = "/Users/robke04/Documents/Git_projects/RSPQLStar/RSPU/RSPUEngine/";

    public static void main(String[] args) throws IOException {
        RSPQLStarEngine.register();
        ARQ.init();


    }

    public void run1(){

        // Namespaces
        String rspuNs = "http://w3id.org/rsp/rspu#";

        // Probability distribution
        FunctionRegistry.get().put(rspuNs + "lessThan", Probability.lessThan);
        FunctionRegistry.get().put(rspuNs + "lessThanOrEqual", Probability.lessThanOrEqual);
        FunctionRegistry.get().put(rspuNs + "greaterThan", Probability.greaterThan);
        FunctionRegistry.get().put(rspuNs + "greaterThanOrEqual", Probability.greaterThanOrEqual);
        FunctionRegistry.get().put(rspuNs + "between", Probability.between);
        FunctionRegistry.get().put(rspuNs + "add", Probability.add);
        FunctionRegistry.get().put(rspuNs + "subtract", Probability.subtract);

        long ref_time = 1574881152000L;
        TimeUtil.setOffset(new Date().getTime() - ref_time);

        final String qString = readFile(root + "data/query-simple.rspqlstar");
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);

        // Create streaming dataset
        final StreamingDatasetGraph sdg = new StreamingDatasetGraph();

        // Add stream
        final RDFStarStream rdfStream = new RDFStarStream("http://ecareathome/stream/heartrate");
        sdg.registerStream(rdfStream);

        // Register query
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);

        // Start stream
        final String fileName = String.format(root + "data/performance-eval/streams/heart-100.trigs");
        final StreamFromFile stream = new StreamFromFile(rdfStream, fileName, 0);
        stream.start();

        // stop gracefully
        new Thread(() -> {
            TimeUtil.silentSleep(5000);
            stream.stop();
            qexec.stopContinuousSelect();
        }).start();

        // Start query
        qexec.execContinuousSelect(System.out, ref_time);
    }


}
