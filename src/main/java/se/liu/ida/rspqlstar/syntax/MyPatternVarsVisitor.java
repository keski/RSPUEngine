package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.syntax.ElementPathBlock;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.PatternVarsVisitor;

import java.util.Collection;
import java.util.Iterator;

public class MyPatternVarsVisitor extends PatternVarsVisitor {

    public MyPatternVarsVisitor(Collection<Var> s) {
        super(s);
    }

    public void visit(ElementTriplesBlock el) {
        Iterator iter = el.patternElts();

        while(iter.hasNext()) {
            Triple t = (Triple)iter.next();
            MyVarUtils.addVarsFromTriple(this.acc, t);
        }
    }

    public void visit(ElementPathBlock el) {
        Iterator iter = el.patternElts();

        while(iter.hasNext()) {
            TriplePath tp = (TriplePath)iter.next();
            if (tp.isTriple()) {
                MyVarUtils.addVarsFromTriple(this.acc, tp.asTriple());
            } else {
                MyVarUtils.addVarsFromTriplePath(this.acc, tp);
            }
        }
    }

    public void visit(ElementSubQuery el) {
        if(el instanceof ElementSubRSPQLStarQuery){
            System.err.println("seeeeeeeeeeen!");
        }
        el.getQuery().setResultVars();
        VarExprList x = el.getQuery().getProject();
        this.acc.addAll(x.getVars());
    }

    public void visit(ElementSubRSPQLStarQuery el) {
        System.err.println("Seen?");
        el.getQuery().setResultVars();
        VarExprList x = el.getQuery().getProject();
        this.acc.addAll(x.getVars());
    }
}
