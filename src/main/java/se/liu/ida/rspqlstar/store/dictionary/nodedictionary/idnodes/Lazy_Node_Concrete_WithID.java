package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.expr.NodeValue;
import se.liu.ida.rspqlstar.function.LazyNodeCache;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.util.function.Consumer;

/**
 * Lazy node implementation.
 */

public class Lazy_Node_Concrete_WithID extends Node_Concrete_WithID {
    public final String key;
    public Node resolvedNode = null;
    public NodeValue[] args;
    private Consumer<NodeValue[]> fn;
    public static int unresolvedLazyNodes = 0;
    public static int resolvedLazyNodes = 0;
    public static int cachedLazyNodes = 0;
    public static long THROTTLE_EXECUTION = -1;

    public Lazy_Node_Concrete_WithID(String label,  NodeValue[] args, long id) {
        super(label, id);
        this.args = args;
        key = label;
        unresolvedLazyNodes++;
    }

    /**
     * Set the consumer that ultimately resolves the value of this node. Needs to be triggered somehow,
     * otherwise the node will be limited to its string representation.
     * @param fn
     */
    public void setConsumer(Consumer<NodeValue[]> fn){
        this.fn = fn;
    }

    @Override
    public LiteralLabel getLiteral() {
        return null;
    }

    @Override
    public Object getLiteralValue() {
        return null;
    }

    @Override
    public String getLiteralLexicalForm() { return null; }

    @Override
    public String getLiteralLanguage() {
        return null;
    }

    @Override
    public String getLiteralDatatypeURI() {
        return null;
    }

    @Override
    public RDFDatatype getLiteralDatatype() {
        return null;
    }

    @Override
    public boolean getLiteralIsXML() {
        return false;
    }

    @Override
    public String toString(PrefixMapping pm, boolean quoting) {
        if(resolvedNode == null){
            trigger();
        }
        return resolvedNode.toString();
    }

    public void trigger(){
        if(resolvedNode == null) {
            unresolvedLazyNodes--;
            Node n = LazyNodeCache.get(key);
            if(n != null){
                cachedLazyNodes++;
                resolvedNode = n;
            } else {
                TimeUtil.silentSleep(THROTTLE_EXECUTION);
                resolvedLazyNodes++;
                fn.accept(args);
                LazyNodeCache.add(key, resolvedNode);
            }
        }
    }

    @Override
    public boolean isLiteral() {
        return false;
    }

    @Override
    public Object getIndexingValue() {
        return null;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else  {
            trigger();
            return resolvedNode.equals(other);
        }
    }

    /**
     * Test that two nodes are semantically equivalent.
     * In some cases this may be the same as equals, in others
     * equals is stricter. For example, two xsd:int literals with
     * the same value but different language tag are semantically
     * equivalent but distinguished by the java equality function
     * in order to support round tripping.
     */
    @Override
    public boolean sameValueAs(Object o) {
        return asJenaNode().sameValueAs(o);
    }

    @Override
    public boolean matches(Node x) {
        return sameValueAs(x);
    }

    @Override
    public Node asJenaNode() {
        System.err.println("lazyNode as asJenaNode");
        return null;
    }
}
