package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.stream.ContinuousListener;
import se.liu.ida.rspqlstar.util.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RDFStarStream {
    final public String uri;
    final private List<RDFStarStreamElement> timestampedGraphs = new ArrayList<>();
    final private List<ContinuousListener> listeners = new ArrayList<>();

    public RDFStarStream(String uri){
        if(!Utils.isValidUri(uri)){
            throw new IllegalStateException("Invalid URI: " + uri);
        }
        this.uri = uri;
    }

    public void push(RDFStarStreamElement tg){
        timestampedGraphs.add(tg);
        for(ContinuousListener listener: listeners){
            listener.push(tg);
        }
    }

    public void addListener(ContinuousListener listener){
        listeners.add(listener);
    }

    public void removeListener(ContinuousListener listener){
        listeners.remove(listener);
    }

    public void clearListeners(){
        for(ContinuousListener listener: listeners){
            listener.flush();
            listener.close();
        }
        listeners.clear();
    }

    public Iterator<IdBasedQuad> iterator(long lowerBound, long upperBound){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        for(int i=0; i < timestampedGraphs.size(); i++){
            final RDFStarStreamElement tg = timestampedGraphs.get(i);
            if(lowerBound > tg.getTime()) continue;
            if(upperBound <= tg.getTime()) break;
            iteratorChain.addIterator(tg.iterateAll());
        }
        return iteratorChain;
    }

    public List<RDFStarStreamElement> iterateElements(long lowerBound, long upperBound){
        final List<RDFStarStreamElement> tgs = new ArrayList<>();
        for(RDFStarStreamElement tg : timestampedGraphs){
            if(lowerBound > tg.getTime()) continue;
            if(upperBound <= tg.getTime()) break;
            tgs.add(tg);
        }
        return tgs;
    }

    public int size(){
        return timestampedGraphs.size();
    }
}
