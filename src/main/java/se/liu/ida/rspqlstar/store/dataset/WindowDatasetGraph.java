package se.liu.ida.rspqlstar.store.dataset;

import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.util.Utils;

import java.time.Duration;
import java.util.Iterator;

/**
 * The WindowDatasetGraph generates a dataset from an underlying stream. The class implements a basic
 * caching mechanism.
 */

public class WindowDatasetGraph extends DatasetGraphStar {
    private Logger logger = Logger.getLogger(WindowDatasetGraph.class);

    public String getName() {
        return name;
    }

    public long getWidth() {
        return width;
    }

    public long getStep() {
        return step;
    }

    public RDFStarStream getRdfStream() {
        return rdfStream;
    }

    private String name;
    private long width;
    private long step;
    private long referenceTime;
    private RDFStarStream rdfStream;

    public long cachedUpperBound = -1;
    public DatasetGraphStar cachedDatasetGraph;

    public WindowDatasetGraph(String name, Duration width, Duration step, long referenceTime, RDFStarStream rdfStream){
        if(!Utils.isValidUri(name)){
            throw new IllegalStateException("Invalid URI: " + name);
        }
        this.name = name;
        this.width = width.toMillis();
        this.step = step.toMillis();
        this.referenceTime = referenceTime;
        this.rdfStream = rdfStream;
    }

    public DatasetGraphStar getDataset(long executionTime){
        final long upperBound = getUpperBound(executionTime);
        // use cached dataset
        if(cachedUpperBound == upperBound) {
            //logger.debug("Using cached dataset for window: " + name);
            return cachedDatasetGraph;
        }
        //logger.debug("Not using cached dataset for window: " + name);

        final DatasetGraphStar ds = new DatasetGraphStar();

        final Iterator<IdBasedQuad> iter = rdfStream.iterator(upperBound - width, upperBound);
        while(iter.hasNext()) {
            ds.addToIndex(iter.next());
        }
        cachedDatasetGraph = ds;
        cachedUpperBound = upperBound;

        return cachedDatasetGraph;
    }

    public long getUpperBound(long executionTime){
        return executionTime - ((executionTime - referenceTime) % width);
    }

    public long getLowerBound(long executionTime){
        return getUpperBound(executionTime) - width;
    }

    public Iterator<IdBasedQuad> iterate(long time){
        return getDataset(time).iterateAll();
    }

    public String toString(){
        return String.format("WindowDatasetGraph(size: %s)", size());
    }
}
