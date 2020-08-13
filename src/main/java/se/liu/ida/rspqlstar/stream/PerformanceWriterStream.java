package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class PerformanceWriterStream implements ContinuousListener {
    private Logger logger = Logger.getLogger(PerformanceWriterStream.class);
    private boolean printHeader = true;
    private final PrintStream ps;
    private int skip = 0;
    private int skipTotal = 0;
    private String sep = ",";
    private String[] cols = {"rate", "type", "count"};

    /**
     * Log timing information from a continuous SELECT query.
     * @param ps Print stream
     */
    public PerformanceWriterStream(PrintStream ps){
        this.ps = ps;
    }

    /**
     * Log timing information from a continuous SELECT query.
     * @param fileName
     */
    public PerformanceWriterStream(String fileName) throws FileNotFoundException {
        this(new PrintStream(new File(fileName)));
    }

    /**
     * Use this function to skip the next x results.
     * @param skip
     */
    public void setSkip(int skip){
        this.skip = skip;
        skipTotal = skip;
    }


    @Override
    public void push(ResultSet rs, long startedAt) {
        try {
            if (printHeader) {
                printHeader = false;
                ps.print(String.join(sep, cols));
                ps.println(sep + "exec_time");
            }
            if (!rs.hasNext()) {
                logger.warn("Empty result");
            }

            final QuerySolution qs = rs.hasNext() ? rs.next() : null;
            logger.debug(qs);

            final long executionTime = System.nanoTime() - startedAt;
            logger.debug("Executed in " + executionTime/1_000_000.0 + " ms");

            // skip
            if(skip == 0) {
                for (String col : cols) {
                    ps.print(qs.getLiteral(col).getLexicalForm());
                    ps.print(sep);
                }
                ps.print(executionTime);
                ps.print("\n");
            } else {
                skip--;
                logger.debug(qs);
                logger.debug("Skipped " + (skipTotal-skip) + " of " + skipTotal);
            }
        } catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    @Override
    public void push(Dataset ds, long startedAt) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void push(RDFStarStreamElement tg) {
        throw new IllegalStateException("Not implemented");
    }

    public void flush(){
        ps.flush();
    }

    public void close(){
        flush();
        ps.close();
    }
}