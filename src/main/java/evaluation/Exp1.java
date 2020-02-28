package evaluation;

import generator.Generator;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngineManager;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarQueryExecution;
import se.liu.ida.rspqlstar.stream.WriterStream;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.File;

public class Exp1 {
    public final static String root = new File("").getAbsolutePath() + "/";

    public static void main(String [] main){
        BayesianNetwork.init();
        BayesianNetwork.loadNetwork("http://ecare/bn#heart-attack", root + "data/experiments/heart-attack.bn");

        long applicationTime = Generator.referenceTime;
        final RSPQLStarEngineManager manager = new RSPQLStarEngineManager(applicationTime - 1);

        // Register streams
        double occUnc = 0.75;
        double attUnc = 0.0;
        int streamRate = 1;
        String filePrefix = String.format(root + "data/experiments/streams/%s-%s-%s-", occUnc, attUnc, streamRate);
        final String s0 = filePrefix + "HA.trigs";
        manager.registerStreamFromFile(s0, "http://base/stream/ha");
        final String s1 = filePrefix + "HR.trigs";
        manager.registerStreamFromFile(s1, "http://base/stream/hr");
        final String s2 = filePrefix + "BR.trigs";
        manager.registerStreamFromFile(s2, "http://base/stream/br");
        final String s3 = filePrefix + "OX.trigs";
        manager.registerStreamFromFile(s3, "http://base/stream/ox");

        // Register query
        final String q1 = Utils.readFile(root + "data/experiments/queries/query-occUnc.rspqlstar");
        final RSPQLStarQueryExecution q = manager.registerQuery(q1);

        q.addContinuousListener(new WriterStream());
        //manager.getStreams().get("http://base/ce1").addListener(new WriterStream());
    }
}
