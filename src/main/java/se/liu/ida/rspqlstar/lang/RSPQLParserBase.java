package se.liu.ida.rspqlstar.lang;

import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.lang.SPARQLParserBase;
import org.apache.jena.sparql.syntax.*;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;

public class RSPQLParserBase extends SPARQLParserBase {
    private RSPQLStarQuery query;
    private Deque<RSPQLStarQuery> stack = new ArrayDeque();

    public RSPQLParserBase(){}

    @Override
    public void setQuery(Query q) {
        setQuery(new RSPQLStarQuery(q));
    }

    public void setQuery(RSPQLStarQuery q) {
        query = q ;
        setPrologue(q) ;
    }

    public RSPQLStarQuery getQuery() {
        return query;
    }

    protected void popQuery() {
        query = stack.pop();
    }

    protected void pushQuery() {
        System.err.println("Pushing: " + query.getQueryType());
        if (query == null) {
            throw new ARQInternalErrorException("Parser query object is null");
        } else {
            stack.push(query);
        }
    }

    protected void startSubSelect(int line, int col){
        pushQuery();
        query = newSubQuery(getPrologue()) ;
    }

    protected RSPQLStarQuery newSubQuery(Prologue prologue) {
        return new RSPQLStarQuery(prologue);
    }

    protected ElementGroup createQueryPattern(Template t){
        ElementGroup elg = new ElementGroup();
        Map<Node, BasicPattern> graphs = t.getGraphPattern();
        for(Node n: graphs.keySet()){
            Element el = new ElementPathBlock(graphs.get(n));
            if(! Quad.defaultGraphNodeGenerated.equals(n) ){
                ElementGroup e = new ElementGroup();
                e.addElement(el);
                el = new ElementNamedGraph(n, e);
            }
            elg.addElement(el);
        }
        return elg;
    }

    protected RSPQLStarQuery endSubSelect(int line, int column) {
        final RSPQLStarQuery subQuery = query;
        if (!subQuery.isSelectType()) {
            throwParseException("Subquery not a SELECT query", line, column);
        }

        popQuery();
        return subQuery;
    }
}
