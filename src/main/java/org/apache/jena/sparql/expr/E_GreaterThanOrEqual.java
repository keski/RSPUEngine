//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.jena.sparql.expr;

import se.liu.ida.rspqlstar.function.LazyNodeValue;

public class E_GreaterThanOrEqual extends ExprFunction2 {
    private static final String functionName = "ge";
    private static final String symbol = ">=";

    public E_GreaterThanOrEqual(Expr left, Expr right) {
        super(left, right, "ge", ">=");
    }

    public NodeValue eval(NodeValue x, NodeValue y) {
        if(x instanceof LazyNodeValue){
            x = ((LazyNodeValue) x).getNodeValue();
        }
        if(y instanceof LazyNodeValue){
            y = ((LazyNodeValue) y).getNodeValue();
        }
        int r = NodeValue.compare(x, y);
        return NodeValue.booleanReturn(r == 1 || r == 0);
    }

    public Expr copy(Expr e1, Expr e2) {
        return new E_GreaterThanOrEqual(e1, e2);
    }
}
