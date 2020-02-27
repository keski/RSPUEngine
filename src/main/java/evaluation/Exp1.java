package evaluation;

import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.WriterStream;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;

public class Exp1 {


    public static void main(String [] main){
        final String root = new File("").getAbsolutePath() + "/";
        BayesianNetwork.init();
        BayesianNetwork.loadNetwork("http://ecareathome/bn#heart", root + "data/heart-attack.bn");

        long applicationTime = 1574881152000L;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime);

        // Register streams
        final String s1 = root + "data/performance-eval/streams/heart-1.trigs";
        manager.registerStreamFromFile(s1, "http://stream/heartrate");

        // Register query
        final String q1 = Utils.readFile(root + "data/exp/q1.rspqlstar");
        final RSPQLStarQueryExecution q = manager.registerQuery(q1);

        q.addContinuousListener(new WriterStream());
        //manager.getStreams().get("http://base/ce1").addListener(new WriterStream());
    }
}
