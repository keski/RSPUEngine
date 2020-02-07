package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpTable;

import java.util.List;

public class TransformFlatten extends TransformCopy {
    public Op transform(OpSequence op, List<Op> elts){
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
            opSequence.add(op);
        }
        return opSequence;
    }
}
