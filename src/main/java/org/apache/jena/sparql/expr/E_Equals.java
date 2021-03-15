//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.jena.sparql.expr;

import se.liu.ida.rspqlstar.function.LazyNodeValue;

public class E_Equals extends ExprFunction2 {
    private static final String functionName = "eq";
    private static final String symbol = "=";

    public E_Equals(Expr left, Expr right) {
        super(left, right, "eq", "=");
    }

    public NodeValue eval(NodeValue x, NodeValue y) {
        if(x instanceof LazyNodeValue){
            x = ((LazyNodeValue) x).getNodeValue();
        }
        if(y instanceof LazyNodeValue){
            y = ((LazyNodeValue) y).getNodeValue();
        }
        boolean b = NodeValue.sameAs(x, y);
        return NodeValue.booleanReturn(b);
    }

    public Expr copy(Expr e1, Expr e2) {
        return new E_Equals(e1, e2);
    }
}
