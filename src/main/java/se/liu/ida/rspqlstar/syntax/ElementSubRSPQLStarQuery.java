package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementSubQuery;
import org.apache.jena.sparql.syntax.ElementVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;

public class ElementSubRSPQLStarQuery extends ElementSubQuery {
    private RSPQLStarQuery query;

    public ElementSubRSPQLStarQuery(Query query){
        super(query);
        this.query = (RSPQLStarQuery) query;
    }


    public Query getQuery() {
        return query;
    }

    public boolean equalTo(Element other, NodeIsomorphismMap isoMap) {
        if (!(other instanceof ElementSubRSPQLStarQuery)) {
            return false;
        } else {
            ElementSubRSPQLStarQuery el = (ElementSubRSPQLStarQuery) other;
            return query.equals(el.query);
        }
    }

    public int hashCode() {
        return query.hashCode();
    }

    public void visit(ElementVisitor v) {
        //v.visit(this);
    }
}