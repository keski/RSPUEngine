package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.binding.BindingUtils;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebra;
import se.liu.ida.rspqlstar.algebra.TransformFlatten;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;

public class RSPQLStarQueryExecution extends QueryExecutionBase {
    private final Logger logger = Logger.getLogger(RSPQLStarQuery.class);
    protected RSPQLStarQuery query;
    private QueryIterator queryIterator = null;
    public StreamingDatasetGraph sdg;
    private boolean closed;
    private boolean stop = false;

    // Collected execution times
    public ArrayList<Long> executionTimes = new ArrayList<>();

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
     * @param out
     * @param ref_time: The time for the first query execution.
     */
    public void execContinuousSelect(PrintStream out, long ref_time) {
        long next_execution = ref_time;
        stop = false;
        Op op = RSPQLStarAlgebra.compile(query);
        while(!stop) {
            final RSPQLStarQueryExecution exec = new RSPQLStarQueryExecution(query, sdg);
            delay(next_execution);
            sdg.setTime(next_execution);
            //TimeUtil.silentSleep(10); // delay 10 ms, prevent most borderline cases

            final long t0 = System.currentTimeMillis();
            final ResultSet rs = exec.execSelect();

            String line = "";
            if(rs.hasNext()){
                if(rs.getResultVars().size() == 1 && rs.getResultVars().contains("count")) {
                    String count = rs.next().get("count").asLiteral().getLexicalForm();
                    line += "Results: " + count + "\t";
                }
            }

            ResultSetMgr.write(out, rs, ResultSetLang.SPARQLResultSetText);
            exec.close();

            final long execTime = System.currentTimeMillis() - t0;
            line += execTime + " ms";
            executionTimes.add(execTime);
            out.println(line);
            System.out.println("Query executed in: " + execTime + " ms");
            next_execution += query.getComputedEvery().toMillis();
        }
    }

    public void stopContinuousSelect(){
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
            System.out.println("Overload! No time to execute!");
        }
    }
}
