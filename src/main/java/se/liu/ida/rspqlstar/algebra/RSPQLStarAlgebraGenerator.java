package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.*;
import org.apache.jena.sparql.util.Context;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.syntax.ElementNamedWindow;
import se.liu.ida.rspqlstar.syntax.ElementSubRSPQLStarQuery;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class RSPQLStarAlgebraGenerator extends AlgebraGenerator {
    private final static Logger logger = Logger.getLogger(RSPQLStarAlgebraGenerator.class);
    private Context context;
    private int subQueryDepth;
    public static boolean PULL_RSPU_FILTERS = false;
    public static boolean USE_LAZY_VARS_AND_CACHE = false;

    public RSPQLStarAlgebraGenerator(){
        super();
    }

    public RSPQLStarAlgebraGenerator(Context context, int depth) {
        this.context = context;
        this.subQueryDepth = depth;
    }

    public Op compile(Query query) {
        Op op = compile(query.getQueryPattern());
        op = this.compileModifiers(query, op);
        //logger.debug("\n" + op);
        return op;
    }

    public Op compile(Element elt) {
        Op op = compileElement(elt);
        op = Algebra.toQuadForm(op);
        return  optimize(op);
    }

    protected static Op optimize(Op op) {
        try {
            op = Transformer.transform(new TransformStar(), op);
            // This does not follow the ARQ standard procedure but instead iterates in a top down manner
            op = TransformFlatten.apply(op);
            op = TransformHeuristics.createJoinTree((OpSequence) op);
            logger.debug(op);
        } catch (Exception e){
            logger.error(e);
        }
        return op;
    }

    @Override
    protected Op compileElementGroup(ElementGroup groupElt) {
        Pair<List<Expr>, List<Element>> pair = this.prepareGroup(groupElt);
        List<Expr> filters = pair.getLeft();
        List<Element> groupElts = pair.getRight();
        Op current = OpTable.unit();
        Deque<Op> acc = new ArrayDeque();
        Iterator var7 = groupElts.iterator();

        while(var7.hasNext()) {
            Element elt = (Element)var7.next();
            if (elt != null) {
                current = compileOneInGroup(elt, current, acc);
            }
        }

        Expr expr;
        if (filters != null) {
            for(var7 = filters.iterator(); var7.hasNext(); current = OpFilter.filter(expr, current)) {
                expr = (Expr)var7.next();
            }
        }
        return current;
    }

    @Override
    protected Op compileOneInGroup(Element elt, Op current, Deque<Op> acc) {
        if (elt instanceof ElementAssign) {
            ElementAssign assign = (ElementAssign)elt;
            return OpAssign.assign(current, assign.getVar(), assign.getExpr());
        } else if (elt instanceof ElementBind) {
            ElementBind bind = (ElementBind)elt;
            return OpExtend.create(current, bind.getVar(), bind.getExpr());
        } else if (elt instanceof ElementOptional) {
            ElementOptional eltOpt = (ElementOptional)elt;
            return this.compileElementOptional(eltOpt, current);
        } else {
            Op op;
            if (elt instanceof ElementMinus) {
                ElementMinus elt2 = (ElementMinus)elt;
                op = this.compileElementMinus(current, elt2);
                return op;
            } else if (!(elt instanceof ElementGroup) && !(elt instanceof ElementNamedGraph) && !(elt instanceof ElementService) && !(elt instanceof ElementUnion) && !(elt instanceof ElementSubQuery) && !(elt instanceof ElementData) && !(elt instanceof ElementTriplesBlock) && !(elt instanceof ElementPathBlock) && !(elt instanceof ElementNamedWindow)) {
                if (elt instanceof ElementExists) {
                    ElementExists elt2 = (ElementExists)elt;
                    op = this.compileElementExists(current, elt2);
                    return op;
                } else if (elt instanceof ElementNotExists) {
                    ElementNotExists elt2 = (ElementNotExists)elt;
                    op = this.compileElementNotExists(current, elt2);
                    return op;
                } else if (elt instanceof ElementFilter) {
                    ElementFilter f = (ElementFilter)elt;
                    return OpFilter.filter(f.getExpr(), current);
                } else {
                    return compileUnknownElement(elt, "compile/Element not recognized: " + Lib.className(elt));
                }
            } else {
                op = compileElement(elt);
                return sequence(current, op);
                //return join(current, op);
            }
        }
    }

    private Op compileElementNamedWindow(ElementNamedWindow elt) {
        final Node windowNode = elt.getWindowNameNode();
        final Op sub = compileElement(elt.getElement());
        return new OpWindow(windowNode, sub);
    }

    @Override
    protected Op compileElement(Element elt) {
        if (elt instanceof ElementNamedWindow) {
            return compileElementNamedWindow((ElementNamedWindow)elt);
        } else if (elt instanceof ElementGroup) {
            return compileElementGroup((ElementGroup)elt);
        } else if (elt instanceof ElementUnion) {
            return compileElementUnion((ElementUnion)elt);
        } else if (elt instanceof ElementNamedGraph) {
            return compileElementGraph((ElementNamedGraph)elt);
        } else if (elt instanceof ElementService) {
            return compileElementService((ElementService)elt);
        } else if (elt instanceof ElementTriplesBlock) {
            return compileBasicPattern(((ElementTriplesBlock)elt).getPattern());
        } else if (elt instanceof ElementPathBlock) {
            return compilePathBlock(((ElementPathBlock)elt).getPattern());
        } else if (elt instanceof ElementSubRSPQLStarQuery) {
            return compileElementSubquery((ElementSubRSPQLStarQuery)elt);
        } else if (elt instanceof ElementData) {
            return compileElementData((ElementData)elt);
        } else {
            return elt == null ? OpNull.create() : this.compileUnknownElement(elt, "compile(Element)/Not a structural element: " + Lib.className(elt));
        }
    }

    protected Op compileElementSubquery(ElementSubRSPQLStarQuery eltSubQuery) {
        final RSPQLStarAlgebraGenerator gen = new RSPQLStarAlgebraGenerator(context, subQueryDepth + 1);
        return gen.compile(eltSubQuery.getQuery());
    }

}
