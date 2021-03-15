//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.jena.sparql.expr;

import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.expr.nodevalue.NodeValueOps;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;
import se.liu.ida.rspqlstar.function.LazyNodeValue;

public class E_Subtract extends ExprFunction2 {
    private static final String functionName = "subtract";
    private static final String symbol = "-";

    public E_Subtract(Expr left, Expr right) {
        super(left, right, "subtract", "-");
    }

    public NodeValue eval(NodeValue x, NodeValue y) {
        if(x instanceof LazyNodeValue){
            x = ((LazyNodeValue) x).getNodeValue();
        }
        if(y instanceof LazyNodeValue){
            y = ((LazyNodeValue) y).getNodeValue();
        }
        return ARQ.isStrictMode() ? XSDFuncOp.numSubtract(x, y) : NodeValueOps.subtractionNV(x, y);
    }

    public Expr copy(Expr e1, Expr e2) {
        return new E_Subtract(e1, e2);
    }
}
