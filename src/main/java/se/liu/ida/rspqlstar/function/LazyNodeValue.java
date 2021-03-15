package se.liu.ida.rspqlstar.function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueVisitor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class LazyNodeValue extends NodeValue {
    public static Map<String, NodeValue> cache = new HashMap<>();

    private Consumer<NodeValue[]> f;
    private String keyString;
    private NodeValue[] args;
    private Node node = null;

    public LazyNodeValue(String fString, NodeValue[] args) {
        this.args = args;
        keyString = fString;
        for(NodeValue nv: args){
            keyString += nv.toString();
        }
    }

    public void setConsumer(Consumer<NodeValue[]> f){
        this.f = f;
        getNodeValue();
    }

    public int hashCode() {
        return keyString.hashCode();
    }

    public String toString(){
        return keyString;
    }

    protected Node makeNode(){
        if(node == null) {
            getNodeValue();
        }
        return node;
    }

    public NodeValue getNodeValue(){
        if(!cache.containsKey(keyString)){
            f.accept(args);
        }
        NodeValue nv = cache.get(keyString);
        node = nv.getNode();
        return nv;
    }

    public Node getNode() {
        if(node == null) {
            getNodeValue();
        }
        return node;
    }

    @Override
    public void visit(NodeValueVisitor visitor) {
        System.err.println("Not implemented!");
    }
}
