package se.liu.ida.rspqlstar.function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueVisitor;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Lazy_Node_Concrete_WithID;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LazyNodeValue extends NodeValue {
    public Lazy_Node_Concrete_WithID node;
    public String fnName;
    public NodeValue[] args;

    public LazyNodeValue(String fnName, NodeValue[] args) {
        this.fnName = fnName;
        this.args = args;
        String label = toString();
        node = new Lazy_Node_Concrete_WithID(label, args, -1L);
    }

    public Node asNode(){
        node.trigger();
        return node.resolvedNode;
    }

    public String toString(){
        return fnName + "(" + Arrays.asList(args)
                .stream()
                .map( nv -> nv.toString() )
                .collect( Collectors.joining( "," ) ) + ")";
    }

    public NodeValue getNodeValue(){
        node.trigger();
        return NodeValue.makeNode(node.resolvedNode);
    }

    @Override
    protected Node makeNode() {
        return node;
    }

    @Override
    public void visit(NodeValueVisitor visitor) {
        System.err.println("Not implemented!");
    }
}
