package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Iterator;

public class ResultWriterStream implements ContinuousListener {
    private Logger logger = Logger.getLogger(ResultWriterStream.class);
    private boolean printHeader = true;
    private final PrintStream ps;
    private int skip = 0;
    private String sep = "\t";
    public boolean dropLiteralDatatype = true;

    /**
     * Log output from a continuous SELECT query.
     * @param ps Print stream
     */
    public ResultWriterStream(PrintStream ps){
        this.ps = ps;
    }
    /**
     * Log output from a continuous SELECT query.
     * @param fileName
     */
    public ResultWriterStream(String fileName) throws FileNotFoundException {
        this(new PrintStream(new File(fileName)));
    }

    @Override
    public void push(Dataset ds, long executionTime) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void push(ResultSet rs, long executionTime) {
        int counter = 0;
        while(rs.hasNext()){
            counter++;
            final QuerySolution qs = rs.next();
            final Iterator<String> iter = rs.getResultVars().iterator();
            while(iter.hasNext()){
                final String s = getString(qs.get(iter.next()));
                ps.print(s);
                if(iter.hasNext()) {
                    ps.print(sep);
                }
            }
            //logger.debug(qs);
            ps.println();
        }
        final long duration = TimeUtil.getTime() - executionTime;
        logger.debug(String.format("Wrote %s results, executed in %s ms", counter, duration));
    }

    public String getString(RDFNode node){
        if(node == null){
            return "null";
        }

        if(dropLiteralDatatype){
            if(node.isLiteral()){
                return  node.asLiteral().getLexicalForm();
            }
        }
        return node.toString();
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