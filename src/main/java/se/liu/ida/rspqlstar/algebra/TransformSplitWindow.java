package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;

import java.util.List;

public class TransformSplitWindow extends TransformCopy {
    /**
     * Split quad patterns in window clauses into separate window clause declarations. This sets the stage for
     * reordering based on temporal filters.
     * @param opExt
     * @return
     */
    @Override
    public Op transform(final OpExt opExt) {
        if(opExt instanceof OpWindow){
            final OpWindow opWindow = (OpWindow) opExt;
            return splitWindow(opWindow);
        } else {
            return opExt;
        }
    }

    public Op splitWindow(OpWindow opWindow){
        OpSequence opSequence = OpSequence.create();
        Op op = opWindow.getSubOp();
        if(op instanceof OpSequence){
            for(Op el : ((OpSequence) op).getElements()){
                opSequence.add(new OpWindow(opWindow.getNode(), el));
            }
            return opSequence;
        } else {
            return opWindow;
        }
    }
}
