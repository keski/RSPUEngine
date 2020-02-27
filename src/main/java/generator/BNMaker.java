package generator;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.jena.sparql.function.Function;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.util.Utils;
import smile.Network;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BNMaker {
    final static String root = new File("").getAbsolutePath() + "/";
    Map<String, Integer> eventHandleMap = new HashMap<>();

    /**
     * Notes: You can only read values from target node if target is set.
     * @param args
     */
    public static void main(String[] args){
        // Init bayesian network for a given licence (resources/smmile-license.txt)
        BayesianNetwork.init();
        BayesianNetwork.loadNetwork("http://this/test/bn", root + "data/heart-attack.bn");




        //Network net = parse(Utils.readFile(root + "data/heart-attack.bn"));
        //net = parse(Utils.readFile("bn.txt"));
        //net.setTarget("HeartAttack", true);
        //net.setEvidence("v_LowOxygenSaturation", "T");
        //net.setEvidence("v_HighHeartRate", "T");
        //net.setEvidence("v_HighBreathingRate", "T");

        //net.setEvidence("v_HighHeartRate", "T");
        //net.setEvidence("v_HighBreathingRate", "T");

        //net.updateBeliefs();
        //String id = "HeartAttackEvent";
        //System.err.println(id + ": " + net.getNodeValue(id)[0]);
        //net.setVirtualEvidence("LowOxygenSaturationEvent", new double[]{0.95,0.1});
        //net.setVirtualEvidence("HighHeartRateEvent", new double[]{0.95,0.1});
        //net.setVirtualEvidence("HighBreathingRateEvent", new double[]{0.95,0.1});
        //net.updateBeliefs();
        //System.err.println(id + ": " + net.getNodeValue(id)[0]);
    }

    private static Network parse(String s){
        JsonObject ob = (JsonObject) JsonParser.parseString(s);
        Network net = new Network();
        JsonObject nodes = ob.get("nodes").getAsJsonObject();
        JsonObject edges = ob.get("edges").getAsJsonObject();
        // add nodes
        for(String node: nodes.keySet()){
            net.addNode(Network.NodeType.CPT, node);
            net.addOutcome(node, "T"); // default has two states T/F
            net.addOutcome(node, "F");

            // adjust for strange behavior (bug?) which adds two extra states
            net.deleteOutcome(node, "State0");
            net.deleteOutcome(node, "State1");

        }
        // add edges
        for(String from: edges.keySet()){
            Iterator<JsonElement> iter = edges.get(from).getAsJsonArray().iterator();
            while(iter.hasNext()) {
                String to = iter.next().getAsString();
                net.addArc(from, to);
            }
        }
        // add CPTs
        for(String node: nodes.keySet()){
            JsonArray arr = nodes.get(node).getAsJsonArray();
            double[] outcomes = new double[arr.size()];
            for(int i=0; i<outcomes.length; i++){
                outcomes[i] = arr.get(i).getAsDouble();
            }
            net.setNodeDefinition(node, outcomes);
        }
        return net;
    }
}
