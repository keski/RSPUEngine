package org.apache.jena.sparql.expr;

import se.liu.ida.rspqlstar.function.LazyNodeValue;

public class E_GreaterThan extends ExprFunction2 {
    private static final String functionName = "gt";
    private static final String symbol = ">";

    public E_GreaterThan(Expr left, Expr right) {
        super(left, right, "gt", ">");
    }

    public NodeValue eval(NodeValue x, NodeValue y) {
        if(x instanceof LazyNodeValue){
            x = ((LazyNodeValue) x).getNodeValue();
        }
        if(y instanceof LazyNodeValue){
            y = ((LazyNodeValue) y).getNodeValue();
        }

        int r = NodeValue.compare(x, y);
        return NodeValue.booleanReturn(r == 1);
    }

    public Expr copy(Expr e1, Expr e2) {
        return new E_GreaterThan(e1, e2);
    }
}