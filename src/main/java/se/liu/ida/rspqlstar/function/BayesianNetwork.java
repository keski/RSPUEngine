package se.liu.ida.rspqlstar.function;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionBase;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.util.Utils;
import smile.Network;

import java.io.File;
import java.util.*;

public class BayesianNetwork {
    private static final Logger logger = Logger.getLogger(BayesianNetwork.class);
    public static Map<String, Network> bnMap = new HashMap<>();

    /**
     * Initiate the SMILE library with current user credentials.
     * Note: Currently assumes that the program is running on Mac OS.
     */
    public static void init() {
        final String absPath = new File("").getAbsolutePath() + "/libs/jsmile-1.4.0-academic/";
        final String[] licence = Utils.readFile("smile-license.txt").split("\n");
        System.setProperty("jsmile.native.library", absPath + "libjsmile.jnilib");
        new smile.License(licence[0], Utils.stringToIntegerArray(licence[1]));

        String rspuNs = "http://w3id.org/rsp/rspu#";
        FunctionRegistry.get().put(rspuNs + "belief", BayesianNetwork.belief);
        FunctionRegistry.get().put(rspuNs + "map", BayesianNetwork.map);
        FunctionRegistry.get().put(rspuNs + "mle", BayesianNetwork.mle);
    }

    /**
     * Loads a network from file. The file should be either an .xdsl or .bn format. The .bn format is a custom syntax
     * representing a BN as a JSON object.
     *
     * @param bnUri URI identifier for the BN
     * @param filePath Full path to file
     * @return
     */
    public static Network loadNetwork(String bnUri,  String filePath) {
        logger.info("Loading " + filePath + " into " + bnUri);
        if(!bnMap.containsKey(bnUri)){
            if(filePath.endsWith(".bn")){
                bnMap.put(bnUri, parse(filePath));
            } else {
                final Network net = new Network();
                net.readFile(filePath);
                bnMap.put(bnUri, net);
            }
        }
        return bnMap.get(bnUri);
    }

    public static FunctionFactory belief = s -> new FunctionBase() {
        @Override
        public NodeValue exec(List<NodeValue> params) {
            try {
                final Network net = bnMap.get(params.get(0).getNode().getURI());
                final String nodeId = stripNamespace(params.get(1));
                final int outcome = params.get(2).asString() == "true" ? 0 : 1;
                final List<Pair> evidence = getEvidencePairList(params.subList(3, params.size()));
                double d = belief(net, nodeId, outcome, evidence);
                return NodeValue.makeDecimal(d);
            } catch (Exception e){
                throw new ExprEvalException("Error:" + e.getMessage());
            }
        }

        @Override
        public void checkBuild(String s, ExprList exprList) {}
    };

    public static FunctionFactory mle = s -> new FunctionBase() {
        @Override
        public NodeValue exec(List<NodeValue> params) {
            final Network bn = bnMap.get(params.get(0).getNode().getURI());
            int targetNodeId = 0;
            final List<NodeValue> evidence = params.subList(2, params.size());
            final HashMap<NodeValue, Double> outcomes = getOutcomes(bn, targetNodeId, evidence);

            NodeValue map = null;
            double max = 0;
            for(NodeValue outcome : outcomes.keySet()){
                if(outcomes.get(outcome) > max){
                    max = outcomes.get(outcome);
                    map = outcome;
                }
            }
            return map;
        }

        @Override
        public void checkBuild(String s, ExprList exprList) {}
    };

    public static FunctionFactory map = s -> new FunctionBase() {
        @Override
        public NodeValue exec(List<NodeValue> params) {
            final Network bn = bnMap.get(params.get(0).getNode().getURI());
            int targetNodeId = 0; //bn.getNode(encode(params.get(1), nsMap.get(bn)));
            final List<NodeValue> evidence = params.subList(2, params.size());
            final HashMap<NodeValue, Double> outcomes = getOutcomes(bn, targetNodeId, evidence);

            NodeValue map = null;
            double max = 0;
            for(NodeValue outcome : outcomes.keySet()){
                if(outcomes.get(outcome) > max){
                    max = outcomes.get(outcome);
                    map = outcome;
                }
            }
            return map;
        }

        @Override
        public void checkBuild(String s, ExprList exprList) {}
    };

    /**
     * Get evidence pair list from a list of node values.
     * @param params
     * @return
     */
    public static List<Pair> getEvidencePairList(List<NodeValue> params){
        final List<Pair> evidenceList = new ArrayList<>();
        for (int i = 0; i < params.size(); i = i + 2) {
            final String nodeId = stripNamespace(params.get(i));
            final double value = params.get(i + 1).getDouble();
            evidenceList.add(new Pair(nodeId, value));
        }
        return evidenceList;
    }

    public static double getOutcome(Network bn, int targetNodeId, NodeValue targetNodeState, List<NodeValue> evidence){
        return getOutcomes(bn, targetNodeId, evidence).get(targetNodeState);
    }

    public static HashMap<NodeValue, Double> getOutcomes(Network bn, int targetNodeId, List<NodeValue> evidence){
        /*
        final String ns = nsMap.get(bn);
        for(int i=0; i < evidence.size(); i = i + 2){
            bn.setEvidence(encode(evidence.get(i), ns), encode(evidence.get(i + 1), ns));
        }

        bn.setTarget(targetNodeId, true);

        // Approx. algorithms:
        // 1: Network.BayesianAlgorithmType.HENRION
        // 4: Network.BayesianAlgorithmType.SELF_IMPORTANCE;
        // 5: Network.BayesianAlgorithmType.HEURISTIC_IMPORTANCE;
        bn.updateBeliefs();

        final HashMap<NodeValue, Double> outcomeMap = new HashMap<>();
        double[] outcomes = bn.getNodeValue(targetNodeId);
        for(int i=0; i < outcomes.length; i++){
            String outcomeId = bn.getOutcomeId(targetNodeId, i);
            outcomeMap.put(decode(outcomeId, ns), outcomes[i]);
        }

        bn.clearAllTargets();
        bn.clearAllEvidence();
        return outcomeMap;
         */
        return null;
    }

    /**
     * Set evidence in a network based on a list of evidence pairs.
     * @param net
     * @param evidence
     */
    public static void setEvidence(Network net, List<Pair> evidence){
        for(Pair pair : evidence) {
            net.getNode(pair.id);
            if (0 < pair.value && pair.value < 1) {
                //logger.debug("Setting virtual evidence: Pr(" + pair.id + ") = " + pair.value);
                net.setVirtualEvidence(pair.id, new double[]{pair.value, 1 - pair.value});
            } else {
                final String outcome = pair.value == 1 ? "T" : "F";
                //logger.debug("Setting hard evidence: Pr(" + pair.id + ") = " + outcome);
                net.setEvidence(pair.id, outcome);
            }
        }
    }

    /**
     * Strip the namespace.
     * @param nodeValue
     * @return
     */
    public static String stripNamespace(NodeValue nodeValue){
        return stripNamespace(nodeValue.asString());
    }

    /**
     * Strip the namespace.
     * @param string
     * @return
     */
    public static String stripNamespace(String string){
        return string.replaceAll(".+[#/]", "");
    }

    /**
     * Parse a .bn file.
     * @param path
     * @return
     */
    public static Network parse(String path){
        String s = Utils.readFile(path);
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

    public static double belief(Network net, String nodeId, int outcome, List<Pair> evidence){
        setEvidence(net, evidence);
        net.setTarget(nodeId, true);
        net.updateBeliefs();
        final double d = net.getNodeValue(nodeId)[outcome];
        net.clearAllTargets();
        net.clearAllEvidence();
        return d;
    }
}

class Pair {
    String id;
    double value;
    public Pair(String id, double value){
        this.id = id;
        this.value = value;
    }
}
