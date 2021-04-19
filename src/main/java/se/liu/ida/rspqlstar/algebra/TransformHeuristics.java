package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.QuadPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import se.liu.ida.rdfstar.tools.sparqlstar.lang.Node_TripleStarPattern;
import se.liu.ida.rspqlstar.algebra.op.OpExtendQuad;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.function.Probability;
import sun.tools.jconsole.JConsole;

import javax.xml.soap.Node;
import java.util.*;

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

    /**
     * Create a join tree (represented as a sequence of operators executed top down.
     * @param opSequence
     * @return
     * @throws Exception
     */
    public static Op createJoinTree(OpSequence opSequence) throws Exception {
        List<Op> unusedOps = opSequence.getElements();
        List<OpFilter> opFilters = getOpFilters(unusedOps);
        List<OpExtend> opExtends = getOpExtends(unusedOps);
        Set<String> vars = new HashSet<>();

        OpSequence joinTree = createJoinTree(unusedOps, opFilters, opExtends, vars);
        applyFilters(joinTree, opFilters, vars, false);

        if(unusedOps.size() > 0 || opFilters.size() > 0 || opExtends.size() > 0){
            System.err.println(joinTree);
            System.err.println("Missing");
            System.err.println("unusedOps: " + unusedOps);
            System.err.println("opFilters: " + opFilters);
            System.err.println("opExtends " + opExtends);
            throw new Exception("Failed to create a consistent JOIN tree!");
        }

        return OpJoin.create(OpTable.unit(), joinTree);
    }

    public static List<OpFilter> getOpFilters(List<Op> ops){
        List<OpFilter> opFilters = new ArrayList<>();
        int i = 0;
        while(i < ops.size()){
            if(ops.get(i) instanceof OpFilter){
                opFilters.add((OpFilter) ops.get(i));
                ops.remove(ops.get(i));
            } else {
                i++;
            }
        }
        return opFilters;
    }

    public static void applyFilters(OpSequence head, List<OpFilter> opFilters, Set<String> vars, boolean skipRSPU){
        int i = 0;
        while(i < opFilters.size()){
            OpFilter opFilter = opFilters.get(i);
            if(skipRSPU && containsRSPUFunction(opFilter)){
                i++;
                continue;
            }

            if(vars.containsAll(extractVariables(opFilter))){
                head.add(opFilter);
                opFilters.remove(opFilter);
            } else {
                i++;
            }
        }
    }

    public static List<OpExtend> getOpExtends(List<Op> ops){
        List<OpExtend> opExtends = new ArrayList<>();
        int i = 0;
        while(i < ops.size()){
            if(ops.get(i) instanceof OpExtend){
                opExtends.add((OpExtend) ops.get(i));
                ops.remove(ops.get(i));
            } else {
                i++;
            }
        }
        return opExtends;
    }

    public static void applyExtends(OpSequence head, List<OpExtend> opExtends, Set<String> vars){
        int i = 0;
        while(i < opExtends.size()){
            OpExtend opExtend = opExtends.get(i);
            if(vars.containsAll(extractVariables(opExtend))){
                head.add(opExtend);
                opExtends.remove(opExtend);
                Var v = opExtend.getVarExprList().getVars().get(0);
                vars.add(v.getVarName());
            } else {
                i++;
            }
        }
    }

    public static void applyFiltersAndExtends(OpSequence head, List<OpFilter> opFilters, List<OpExtend> opExtends, Set<String> vars){
        boolean updated = true;
        while(updated) {
            int size = opFilters.size() + opExtends.size();
            applyFilters(head, opFilters, vars, RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS);
            applyExtends(head, opExtends, vars);
            updated = size < opFilters.size() + opExtends.size();
        }
    }

    public static OpSequence createJoinTree(List<Op> unusedOps, List<OpFilter> opFilters, List<OpExtend> opExtends, Set<String> vars){
        OpSequence head = OpSequence.create();
        while(true){
            Op op = opWithHighestSelectivity(unusedOps, vars);
            unusedOps.remove(op);

            if(op instanceof OpWindow) {
                OpWindow opWindow = (OpWindow) op;
                List<Op> windowOpSequence = ((OpSequence) opWindow.getSubOp()).getElements();
                op = opWindow.copy(createJoinTree(windowOpSequence, opFilters, opExtends, vars));
                head.add(op);
                continue;
            }

            // apply all filters and extends to the op
            applyFiltersAndExtends(head, opFilters, opExtends, vars);

            if(op == null){
                break;
            }

            if(op instanceof OpTable){
                continue;
            } else if(op instanceof OpExtendQuad){
                head.add(op);
                Var v = ((OpExtendQuad) op).getSubOp().getVarExprList().getVars().get(0);
                vars.add(v.getName());
            } else {
                head.add(op);
                vars.addAll(extractVariables(op));
            }
        }
        return head;
    }

    public static Op opWithHighestSelectivity(List<Op> ops, Set<String> vars){
        // selectivity is defined on quad pattern level, using the quad with highest selectivity
        Op highOp = null;
        int maxSelectivity = -1;
        for(Op op: ops) {
            int selectivity = getSelectivity(op, vars);
            if(selectivity > maxSelectivity){
                highOp = op;
                maxSelectivity = selectivity;
            }
        }
        return highOp;
    }

    public static int getSelectivity(Op op, Set<String> vars){
        int selectivity = 0;
        if(op instanceof OpQuad){
            selectivity = getSelectivity(((OpQuad) op).getQuad(), vars);
        } else if(op instanceof OpExtendQuad){
            OpExtendQuad opExtendQuad = (OpExtendQuad) op;
            if(vars.containsAll(extractVariables(opExtendQuad))){
                selectivity = 1000;
            }
        }

        // Increase selectivity by a factor 10 if the op joins with previous vars (i.e., avoid unions)
        for(String var : extractVariables(op)){
            if(vars.contains(var)){
                selectivity *= 10;
                break;
            }
        }

        return selectivity;
    }


    public static int getSelectivity(final Quad q, Set<String> vars){
        // pattern is binary gspo (0=not defined, 1=defined)
        int[] selectivity = new int[]{
                1, // 0000
                4, // 0001
                2, // 0010
                6, // 0011
                3, // 0100
                7, // 0101
                5, // 0110
                8, // 0111
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

        // A nested expression (i.e., an expression split up into extend quad and quad)
        // Should be treated as a single statement and should appear next to each other.
        // This is accomplished by simply adding 1 for every defined variable in a quad that matches the ?_ patterns.
        if(!q.getSubject().isConcrete()){
            if(vars.contains(q.getSubject().getName()) && q.getSubject().getName().startsWith("_")) {
                sel += 1;
            }
        }
        if(!q.getObject().isConcrete()){
            if(vars.contains(q.getObject().getName()) && q.getObject().getName().startsWith("_")){
                sel += 1;
            }
        }

        int score = selectivity[sel];
        return score;
    }

    /**
     * Extract variables from Op.
     * @param op
     */
    public static Set<String> extractVariables(Op op) {
        Set<String> vars = new HashSet<>();
        if(op instanceof OpJoin){
            vars.addAll(extractVariables(((OpJoin) op).getLeft()));
            vars.addAll(extractVariables(((OpJoin) op).getLeft()));
        } else if(op instanceof OpWindow){
            vars = extractVariables(((OpWindow) op).getSubOp());
        } else if(op instanceof OpSequence){
            OpSequence opSequence = (OpSequence) op;
            for(Op op2: opSequence.getElements()){
                vars.addAll(extractVariables(op2));
            }
        } else if(op instanceof OpQuad){
            vars = extractVariables(((OpQuad) op).getQuad());
        } else if(op instanceof OpQuadPattern){
            QuadPattern quadPattern = ((OpQuadPattern) op).getPattern();
            for(Quad quad : quadPattern.getList()){
                vars.addAll(extractVariables(quad));
            }
        } else if(op instanceof OpFilter){
            OpFilter opFilter = (OpFilter) op;
            for(Var v: opFilter.getExprs().getVarsMentioned()){
                vars.add(v.getVarName());
            }
        } else if(op instanceof OpExtend){
            OpExtend opExtend = (OpExtend) op;
            Var v = opExtend.getVarExprList().getVars().get(0);
            //vars.add(v.getVarName());
            for(Var var: opExtend.getVarExprList().getExpr(v).getVarsMentioned()){
                vars.add(var.getVarName());
            }
        }  else if(op instanceof OpExtendQuad){
            // Note: Not recursive at this point.
            OpExtendQuad opExtendQuad = (OpExtendQuad) op;
            Var v = opExtendQuad.getSubOp().getVarExprList().getVars().get(0);
            NodeValueNode n = (NodeValueNode) opExtendQuad.getSubOp().getVarExprList().getExpr(v);
            Node_TripleStarPattern tsp = (Node_TripleStarPattern) n.asNode();
            vars.addAll(extractVariables(new Quad(null, tsp.get())));
        } else if(op instanceof OpTable) {
            // pass
        } else {
            System.err.println("Cant extract vars from " + op.getClass());
        }
        return vars;
    }

    /**
     * Extract vars from quad.
     * @param quad
     * @return
     */
    public static Set<String> extractVariables(Quad quad){
        Set<String> vars = new HashSet<>();
        if(quad.getGraph() != null && quad.getGraph().isVariable()){
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

    public static boolean containsRSPUFunction(OpFilter opFilter){
        for(Expr expr: opFilter.getExprs()){
            if(containsRSPUFunction(expr)){
                return true;
            }
        }
        return false;
    }

    public static boolean containsRSPUFunction(Expr expr){
        if(expr.isFunction()){
            ExprFunction exprFunction = expr.getFunction();
            // check function
            if(exprFunction.getFunctionIRI() != null && exprFunction.getFunctionIRI().startsWith(Probability.ns)){
                return true;
            }

            // check args
            for(Expr arg: exprFunction.getArgs()){
                if(containsRSPUFunction(arg)){
                    return true;
                }
            }
        }
        return false;
    }

}
