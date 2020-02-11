package evaluation;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryFactory;
import se.liu.ida.rspqlstar.function.RSPUFunctions;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.util.*;

public class Evaluation1 {
    public static String root = "/Users/robke04/Documents/Git_projects/RSPQLStar/RSPU/RSPUEngine/";

    public static void main(String[] args) {
        RSPQLStarEngine.register();
        ARQ.init();
        RSPUFunctions.register();

        Evaluation1.run1();
    }

    public static void run1() {
        long ref_time = 1574881152000L;
        TimeUtil.setOffset(new Date().getTime() - ref_time);

        String qString = Utils.readFile(root + "data/query-simple.rspqlstar");
        qString = qString.replaceAll("\\$threshold", "0.6");
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
            qexec.stop();
        }).start();

        // Start query
        //qexec.execContinuousSelect(System.out, ref_time)
        Dataset ds = DatasetFactory.create();
        qexec.execContinuousConstruct(System.out, ref_time);
    }


}
