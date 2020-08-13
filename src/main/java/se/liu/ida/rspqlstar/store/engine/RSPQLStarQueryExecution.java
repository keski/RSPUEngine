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
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    private final Logger logger = Logger.getLogger(RSPQLStarQueryExecution.class);
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;
    private boolean stop = false;
    public boolean isRunning = true;
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
            try {
                final ResultSet rs = execResultSet();
                return new ResultSetCheckCondition(rs, this);
            } catch (Exception e){
                logger.error(e.getMessage());
                throw e;
            }
        }
    }

    /**
     * TODO: Update according to execContinuousSelect
     */
    public Runnable execContinuousConstruct() {
        final Runnable r = () -> {
            long nextExecution = sdg.getTime();
            while(!stop) {
                final long sleep = nextExecution - TimeUtil.getTime();
                TimeUtil.silentSleep(sleep);
                logger.info(String.format("Executing at: %s (sleep: %s)", nextExecution, sleep));
                final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
                // execute
                sdg.setTime(nextExecution);
                final Dataset ds = DatasetFactory.create();
                exec.execConstructDataset(ds);
                // push
                listener.push(ds, TimeUtil.getNanoTime());
                exec.close();
                nextExecution += query.getComputedEvery();
            }
            isRunning = false;
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

    protected void execInit() { }

    /**
     * Run periodic execution of query.
     */
    public Runnable execContinuousSelect() {
        final Runnable r = () -> {
            try {
                long nextExecution = sdg.getTime();
                while (!stop) {
                    final long sleep = nextExecution - TimeUtil.getTime();
                    TimeUtil.silentSleep(sleep);
                    logger.info(String.format("Wait for execution: %s ms", sleep));
                    sdg.setTime(nextExecution);
                    final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
                    listener.push(exec.execSelect(), TimeUtil.getNanoTime());
                    exec.close();
                    nextExecution += query.getComputedEvery();
                }
                isRunning = false;
            } catch (Exception e){
                logger.error(e.getMessage());
            }
        };
        return r;
    }

    public void stop(){
        stop = true;
    }

    public void setListener(ContinuousListener listener){
        this.listener = listener;
    }
}
