package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.*;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.engine.*;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.stream.ContinuousListener;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    private final Logger logger = Logger.getLogger(RSPQLStarQuery.class);
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;
    private boolean stop = false;
    private ContinuousListener listener = null;

    public RSPQLStarQueryExecution(RSPQLStarQuery query, StreamingDatasetGraph sdg){
        this(query, DatasetFactory.wrap(sdg));
        this.query = query;
        this.sdg = sdg;
        if(!sdg.isPreparedForQuery(query)){
            sdg.initForQuery(query);
        }

    }

    public RSPQLStarQueryExecution(RSPQLStarQuery query, Dataset dataset) {
        super(query, dataset, ARQ.getContext(), QueryEngineRegistry.get().find(query, dataset.asDatasetGraph()));
    }

    public ResultSet asResultSet(QueryIterator qIter) {
        ResultSetStream rStream = new ResultSetStream(query.getResultVars(), ModelFactory.createDefaultModel(), qIter);
        return rStream;
    }

    public ResultSet execSelect() {
        checkNotClosed();
        if (!query.isSelectType()) {
            throw new QueryExecException("Wrong query type: " + query);
        } else {
            final ResultSet rs = execResultSet();
            return new ResultSetCheckCondition(rs, this);
        }
    }

    public Runnable execContinuousConstruct(long firstExecution) {
        final Runnable r = () -> {
            long nextExecution = firstExecution;
            while(!stop) {
                final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
                delay(nextExecution);
                sdg.setTime(nextExecution);
                final long t0 = System.currentTimeMillis();

                // Execute
                final Dataset ds = DatasetFactory.create();
                exec.execConstructDataset(ds);
                logger.debug(query.getOutputStream() + " empty? " + ds.isEmpty());
                listener.push(ds, t0);
                exec.close();
                nextExecution += query.getComputedEvery().toMillis();
            }
        };
        return r;
    }

    private ResultSet execResultSet() {
        startQueryIterator();
        return asResultSet(queryIterator);
    }

    private void checkNotClosed() {
        if (closed) {
            throw new QueryExecException("HTTP QueryExecution has been closed");
        }
    }

    /**
     * Does not support timeout.
     */
    private void startQueryIterator() {
        execInit();
        queryIterator = getPlan().iterator();
    }

    protected void execInit() {
        // reset iterator?
    }

    /**
     * Run periodic execution of query.
     */
    public Runnable execContinuousSelect(long firstExecution) {
        final Runnable r = () -> {
            long nextExecution = firstExecution;
            while(!stop) {
                delay(nextExecution);
                logger.info("Executing at: " + nextExecution);
                final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
                sdg.setTime(nextExecution);

                final long t0 = System.currentTimeMillis();
                listener.push(exec.execSelect(), t0);
                exec.close();

                nextExecution += query.getComputedEvery().toMillis();
            }
        };
        return r;
    }

    public void stop(){
        stop = true;
    }

    /**
     * Pause execution until next_execution time.
     *
     * @param next_execution
     */
    public void delay(long next_execution){
        long sleep = next_execution - TimeUtil.getTime().getTime();
        if(sleep > 0){
            TimeUtil.silentSleep(sleep);
        } else {
            logger.warn("Overload! No time to execute!");
        }
    }

    public void setListener(ContinuousListener listener){
        this.listener = listener;
    }
}
