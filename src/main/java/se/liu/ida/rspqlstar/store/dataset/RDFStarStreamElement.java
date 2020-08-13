package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.index.*;

import java.io.PrintStream;
import java.util.Date;
import java.util.Iterator;


public class RDFStarStreamElement implements StreamRDF {
    private Index index;
    private long time;
    public static Node timeProperty = NodeFactory.createURI("http://www.w3.org/ns/prov#generatedAtTime");
    private NodeDictionary nd = NodeDictionaryFactory.get();
    private ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public RDFStarStreamElement(){
        this(0);
    }

    public RDFStarStreamElement(Date date){
        this(date.getTime());
    }

    public RDFStarStreamElement(long time){
        this.time = time;
        index = new HashIndex(Field.G, Field.S, Field.P, Field.O);
    }

    public void setTime(long time){
        this.time = time;
    }

    public long getTime(){
        return time;
    }

    @Override
    public void start() {}

    @Override
    public void triple(Triple triple) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void quad(Quad quad){
        addQuad(quad);
    }

    private IdBasedQuad addQuad(Quad quad) {
        final Node graph = quad.getGraph() == null ? Quad.defaultGraphNodeGenerated : quad.getGraph();
        if(graph == Quad.defaultGraphNodeGenerated && quad.getPredicate().equals(timeProperty)) {
            time = ((XSDDateTime) quad.getObject().getLiteral().getValue()).asCalendar().getTimeInMillis();
        }
        final long g = nd.addNodeIfNecessary(graph);
        final long s = addIfNecessary(quad.getSubject(), graph);
        final long p = nd.addNodeIfNecessary(quad.getPredicate());
        final long o = addIfNecessary(quad.getObject(), graph);
        final IdBasedQuad idBasedQuad = new IdBasedQuad(g, s, p, o);
        addToIndex(idBasedQuad);
        return idBasedQuad;
    }

    private long addIfNecessary(Node node, Node graph){
        if (node instanceof Node_Triple) {
            final IdBasedQuad idBasedQuad = addQuad(new Quad(graph, ((Node_Triple) node).get()));
            return refT.addIfNecessary(idBasedQuad.getIdBasedTriple());
        } else {
            return nd.addNodeIfNecessary(node);
        }
    }

    public void addToIndex(IdBasedQuad idBasedQuad) {
        index.add(idBasedQuad);
    }


    @Override
    public void base(String s) {}

    @Override
    public void prefix(String s, String s1) {}

    @Override
    public void finish() {}

    public void print(PrintStream out){
        out.println("TG: " + time);
        index.iterateAll().forEachRemaining(x -> { out.println(x); });
    }

    public Iterator<IdBasedQuad> iterateAll(){
        return index.iterateAll();
    }

    public String toString(){
        return "Time: " + time + ", Payload: " + index.size();
    }
}
