package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.rspqlstar.algebra.op.OpExtendQuad;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;

import java.util.ArrayList;
import java.util.List;

public class TransformFlatten {
    public static OpSequence apply(Op op) throws Exception {
        ArrayList<Op> listOfOps = new ArrayList<>();
        flatten(op, listOfOps);

        OpSequence opSequence = OpSequence.create();
        opSequence.add(OpTable.unit());
        for(Op op2: listOfOps){
            opSequence.add(op2);
        }
        return opSequence;
    }

    /**
     * Return a list of flattened ops. Filters are added to the beginning of the list.
     * @param op
     * @param listOfOps
     */
    public static void flatten(Op op, ArrayList<Op> listOfOps) throws Exception {
        if (op instanceof OpQuad) {
            listOfOps.add(op);
        } else if (op instanceof OpSequence) {
            for(Op op2: ((OpSequence) op).getElements()){
                flatten(op2, listOfOps);
            }
        } else if (op instanceof OpFilter) {
            OpFilter opFilter = (OpFilter) op;
            for(Expr expr: opFilter.getExprs()){
                listOfOps.add(0, opFilter.filterBy(new ExprList(expr), OpTable.unit())); // split compiled
            }
            //listOfOps.add(0, opFilter.filterBy(opFilter.getExprs(), OpTable.unit()));
            flatten(opFilter.getSubOp(), listOfOps);
        } else if (op instanceof OpWindow) {
            OpWindow opWindow = (OpWindow) op;
            OpSequence subOpSequence = apply(opWindow.getSubOp());
            // pull the filters out
            for(int i=0; i < subOpSequence.getElements().size();){
                Op op2 = subOpSequence.getElements().get(i);
                if(op2 instanceof OpFilter){
                    listOfOps.add(0, op2);
                    subOpSequence.getElements().remove(op2);
                } else {
                    i++;
                }
            }
            listOfOps.add(opWindow.copy(subOpSequence));
        } else if (op instanceof OpExtend) {
            OpExtend opExtend = (OpExtend) op;
            listOfOps.add(opExtend.copy(OpTable.empty()));
            for(Op op2: apply(opExtend.getSubOp()).getElements()){
                if(op2 instanceof OpTable){
                    continue;
                }
                listOfOps.add(op2);
            }
        }  else if (op instanceof OpExtendQuad) {
            listOfOps.add(op);
        } else if(op instanceof OpTable) {
            // skip
        } else {
            throw new Exception(op.getClass() + " is not yet supported");
        }
    }
}
