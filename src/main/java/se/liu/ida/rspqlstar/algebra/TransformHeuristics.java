package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprLib;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This transformer reorders the query and creates a join tree based using a heuristics based on selectivity.
 * The heuristics is as follows:
 * H1: Presence of time property in quad in window.
 * H2: Filter joining time prop var.
 * H3*: Extension of od Tsialiamanis et al. (2012): (less is better)
 *      (s,p,o) ≺ (s,?,o) ≺ (?,p,o) ≺ (s,p,?) ≺ (?,?,o) ≺ (s,?,?) ≺ (?,p,?) ≺ (?,?,?)
 *      extended for quads:
 *      (g,s,p,o) ≺ (g,s,?,o) ≺ (g,?,p,o) ≺ (g,s,p,?) ≺ (g,?,?,o) ≺ (g,s,?,?) ≺ (g,?,p,?) ≺ (g,?,?,?) <
 *      (?,s,p,o) ≺ (?,s,?,o) ≺ (?,?,p,o) ≺ (?,s,p,?) ≺ (?,?,?,o) ≺ (?,s,?,?) ≺ (?,?,p,?) ≺ (?,?,?,?)
 *      and window clauses such that:
 *      window < (g,s,p,o)
 */

public class TransformHeuristics extends TransformCopy {
    private Node timePredicate;

    public TransformHeuristics(Node timePredicate){
        this.timePredicate = timePredicate;
    }

    @Override
    public Op transform(OpFilter opFilter, Op subOp){
        return opFilter;
    }

    @Override
    public Op transform(OpSequence opSequence, List<Op> elts) {
        return createJoinTree(opSequence);
    }

    public Op createJoinTree(OpSequence opSequence){
        Op head = null;
        List<Op> unusedOps = opSequence.getElements();
        Set<String> vars = new HashSet<>();
        while(unusedOps.size() > 0){
            int index = indexWithHighestSelectivity(unusedOps, vars);
            Op op = unusedOps.remove(index);
            vars.addAll(extractVariables(op));
            if(head == null){
                head = op;
            } else {
                head = OpJoin.create(head, op);
            }
        }
        return head;
    }

    public int indexWithHighestSelectivity(List<Op> ops, Set<String> vars){
        // selectivity is defined on quad pattern level, using the quad with highest selectivity
        int index = 0;
        int high = 0;
        for(int i=0; i<ops.size(); i++) {
            int selectivity = getMaxSelectivity(ops.get(i), vars);
            if(selectivity > high){
                index = i;
                high = selectivity;
            }
        }
        return index;
    }

    public int getMaxSelectivity(Op op, Set<String> vars){
        int score = 0;
        if(op instanceof OpWindow){
            Op op2 = ((OpWindow) op).getSubOp();
            score = getMaxSelectivity(op2, vars);
            score += 16; // windows have priority
        } else if(op instanceof OpFilter){
            Op op2 = ((OpFilter) op).getSubOp();
            score = getMaxSelectivity(op2, vars);
        } else if(op instanceof OpQuadPattern){
            for(Quad quad : ((OpQuadPattern) op).getPattern().getList()){
                score = Math.max(getSelectivity(quad, vars), score);
            }
        }
        return score;
    }

    public int getSelectivity(final Quad q, Set<String> vars){
        int[] selectivity = new int[]{
                // no graph defined
                1, // 0000
                4, // 0001
                2, // 0010
                6, // 0011
                3, // 0100
                7, // 0101
                5, // 0110
                8, // 0111
                // graph defined
                9,  // 1000
                12, // 1001
                10, // 1010
                14, // 1011
                11, // 1100
                15, // 1101
                13, // 1110
                16, // 1111
        };

        int sel = 0;
        if(q.getGraph().isConcrete() || vars.contains(q.getGraph().getName())){
            sel += 8;
        }
        if(q.getSubject().isConcrete() || vars.contains(q.getSubject().getName())){
            sel += 4;
        }
        if(q.getPredicate().isConcrete() || vars.contains(q.getPredicate().getName())){
            sel += 2;
        }
        if(q.getObject().isConcrete() || vars.contains(q.getObject().getName())){
            sel += 1;
        }
        int score = selectivity[sel];

        if(q.getPredicate().isConcrete() && q.getPredicate().equals(timePredicate)){
            score += 16;
        }
        return score;
    }

    /**
     * Extract variables from Op.
     * @param op
     */
    private Set<String> extractVariables(Op op) {
        Set<String> vars = new HashSet<>();
        if(op instanceof OpWindow){
            return extractVariables(((OpWindow) op).getSubOp());
        } else if(op instanceof OpQuad){
            vars = extractVariables(((OpQuad) op).getQuad());
        } else if(op instanceof OpQuadPattern){
            QuadPattern quadPattern = ((OpQuadPattern) op).getPattern();
            for(Quad quad : quadPattern.getList()){
                vars.addAll(extractVariables(quad));
            }
        }
        return vars;
    }

    /**
     * Extract vars from quad.
     * @param quad
     * @return
     */
    private Set<String> extractVariables(Quad quad){
        Set<String> vars = new HashSet<>();
        if(quad.getGraph().isVariable()){
            vars.add(quad.getGraph().getName());
        }
        if(quad.getSubject().isVariable()){
            vars.add(quad.getSubject().getName());
        }
        if(quad.getPredicate().isVariable()){
            vars.add(quad.getPredicate().getName());
        }
        if(quad.getObject().isVariable()){
            vars.add(quad.getObject().getName());
        }
        return vars;
    }
}
