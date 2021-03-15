package evaluation;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.ResultWriterStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TestLoadData {
    private static Logger logger = Logger.getLogger(TestLoadData.class);

    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {
        RSPQLStarEngineManager.init();

        String refTime = "2021-03-01T10:00:00";
        final long applicationTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(refTime).getTime();
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);
        manager.loadData("resources/data/static.trigstar");
        manager.registerStreamFromFile("resources/data/ox2.trigstar", "http://example.org/ox");

        // Activate H5?
        RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = true;
        RSPQLStarAlgebraGenerator.USE_LAZY_VARS_AND_CACHE = true;

        String query = new String(Files.readAllBytes(Paths.get("resources/test.rspqlstar")));

        final RSPQLStarQueryExecution q = manager.registerQuery(query);

        final ResultWriterStream listener = new ResultWriterStream(System.out);
        q.setListener(listener);
    }
}

/*
    final RSPQLStarQueryExecution q = manager.registerQuery(
            "BASE <http://example.org/data/> " +
                    "PREFIX : <http://example.org/ontology/> " +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                    "PREFIX sosa: <http://www.w3.org/ns/sosa/> " +
                    "PREFIX rspu: <http://w3id.org/rsp/rspu#> " +
                    "REGISTER STREAM <test> COMPUTED EVERY PT5S AS " +
                    "SELECT ?obs1 ?sensor1 ?mu1 " +
                    "FROM NAMED WINDOW <w1> ON <http://example.org/ox1> [RANGE PT1S STEP PT1S] " +
                    "WHERE { " +
                    "   ?sensor1 rspu:measurementUncertainty ?mu1 ;" +
                    "            a :OxygenSensor1 ; " +
                    "            sosa:hasFeatureOfInterest <location/0> . " +
                    "   FILTER(?location = <location/0> ) " +
                    "   WINDOW <w1> { " +
                    "       GRAPH ?g {" +
                    "           ?obs1 a sosa:Observation ; " +
                    "              sosa:madeBySensor ?sensor1 ; " +
                    "              sosa:hasSimpleResult ?value1 . " +
                    "       }" +
                    "   }" +
                    //"   BIND(rspu:add(?mu1, 1) AS ?pdf1) " +
                    "}"
    );
*/
