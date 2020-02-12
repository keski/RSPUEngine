package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.PatternVars;

import java.util.Collection;

public class MyPatternVars extends PatternVars {
    public static Collection<Var> embeddedVars(Collection<Var> s, Element element) {
        MyPatternVarsVisitor v = new MyPatternVarsVisitor(s);
        vars(element, v);

        ((ElementGroup) element).getElements().forEach(el -> {
            if(el instanceof ElementNamedWindow){
                vars(((ElementNamedWindow) el).getElement(), v);
            }
        });
        return s;
    }
}
