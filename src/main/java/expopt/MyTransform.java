package expopt;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpQuadPattern;
import org.apache.jena.sparql.core.Quad;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MyTransform extends TransformCopy {
    public Node timePredicate;

    public MyTransform(){
        this(null);
    }

    public MyTransform(Node timePredicate){
        this.timePredicate = timePredicate;
    }

    public Op transform(OpQuadPattern opQuadPattern) {
        List<Quad> l =  opQuadPattern.getPattern().getList();
        l.sort((o1, o2) -> getSelectivity(o2) - getSelectivity(o1));
        for(Quad q : l){
            System.out.println(getSelectivity(q));
        }
        return opQuadPattern;
    }

    public int getSelectivity(final Quad q){
        int[] selectivity = new int[]{
                1, // 0000
                4, // 0001
                2, // 0010
                6, // 0011
                3, // 0100
                7, // 0101
                5, // 0110
                8, // 0111

                9, // 1000
                12, // 1001
                10, // 1010
                14, // 1011
                11, // 1100
                15, // 1101
                13, // 1110
                16, // 1111
        };

        int sel = 0;
        if(q.getSubject().isConcrete()) sel += 4;
        if(q.getPredicate().isConcrete()) sel += 2;
        if(q.getObject().isConcrete()) sel += 1;
        int score = selectivity[sel];

        if(q.getPredicate().equals(timePredicate)){
            score += 16;
        }
        return score;
    }
}
