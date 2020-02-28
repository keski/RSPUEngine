package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;

import java.util.List;

public class TransformFlatten extends TransformCopy {
    @Override
    public Op transform(final OpExt opExt) {
        if(opExt instanceof OpWindow){
            final OpWindow opWindow = (OpWindow) opExt;
            final Op op = opWindow.getSubOp();
            Op op2 = op;
            if(op instanceof OpSequence){
                op2 = transform((OpSequence) op);
            }
            return opWindow.copy(op2);
        } else {
            return opExt;
        }
    }

    public Op transform(OpSequence op){
        OpSequence opSequence = OpSequence.create();
        opSequence.add(OpTable.unit());
        flatten(opSequence, op);
        return opSequence;
    }

    /**
     * Recursive flattening of op.
     *
     * @param opSequence OpSequence populated by flattening
     * @param op Incoming op.
     * @return
     */
    public OpSequence flatten(OpSequence opSequence, Op op){
        if(op instanceof OpSequence){
            for(Op op2 : ((OpSequence) op).getElements()){
                flatten(opSequence, op2);
            }
        } else if(op instanceof OpJoin){
            flatten(opSequence, ((OpJoin) op).getLeft());
            flatten(opSequence, ((OpJoin) op).getRight());
        } else if(op instanceof OpTable){
            return opSequence;
        } else {
            //System.err.println("add op: " + op.getClass());
            opSequence.add(op);
        }
        return opSequence;
    }
}
