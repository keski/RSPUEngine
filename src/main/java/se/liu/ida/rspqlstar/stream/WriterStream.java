package se.liu.ida.rspqlstar.stream;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.reasoner.rulesys.builtins.Print;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Node_Concrete_WithID;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.iterator.NodeWrapperKey;
import se.liu.ida.rspqlstar.store.engine.main.iterator.TripleWrapperKey;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.io.PrintStream;

public class WriterStream implements ContinuousListener {
    private final PrintStream ps;

    public WriterStream(){
        this(System.out);
    }

    public WriterStream(PrintStream ps){
        this.ps = ps;
    }

    @Override
    public void push(ResultSet rs) {
        ResultSetMgr.write(ps, rs, ResultSetLang.SPARQLResultSetText);
    }

    @Override
    public void push(Dataset ds) {
        RDFDataMgr.write(ps, ds, RDFFormat.TRIG);
    }

    @Override
    public void push(RDFStarStreamElement tg) {
        tg.iterateAll().forEachRemaining(idBasedQuad -> {
            ps.println(decode(idBasedQuad));
        });
    }

    private Quad decode(IdBasedQuad q){
        Quad quad = new Quad(decode(new Key(q.graph)),
                decode(new Key(q.subject)),
                decode(new Key(q.predicate)),
                decode(new Key(q.object)));
        return quad;
    }

    private Node decode(Key key) {
        final Node node;
        try {
            if (key instanceof TripleWrapperKey) {
                // We ignore the graph node for the TripleWrapperKey, since this will
                // be implicit from the context of the Node_Triple
                final IdBasedTriple idBasedQuad = ((TripleWrapperKey) key).idBasedTriple;
                final Node s = decode(new Key(idBasedQuad.subject));
                final Node p = decode(new Key(idBasedQuad.predicate));
                final Node o = decode(new Key(idBasedQuad.object));
                node = new Node_Triple(new Triple(s, p, o));
            } else if (key instanceof NodeWrapperKey) {
                node = ((NodeWrapperKey) key).node;
            } else {
                if (IdFactory.isReferenceId(key.id)) {
                    final IdBasedTriple idBasedTriple = ReferenceDictionaryFactory.get().getIdBasedTriple(key.id);
                    final Node s = decode(new Key(idBasedTriple.subject));
                    final Node p = decode(new Key(idBasedTriple.predicate));
                    final Node o = decode(new Key(idBasedTriple.object));
                    node = new Node_Triple(new Triple(s, p, o));
                } else {
                    node = NodeDictionaryFactory.get().getNode(key.id);
                }
            }

            // If a StarNode is retrieved, convert to Jena node
            if (node instanceof Node_Concrete_WithID) {
                return ((Node_Concrete_WithID) node).asJenaNode();
            }
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return node;
    }
}