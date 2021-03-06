package se.liu.ida.rspqlstar.store.engine.main.pattern;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;

/**
 * Class for creating triple patterns. If a subject, predicate, or object is not set it considered a variable.
 */
public class QuadPatternBuilder {

    private Element graph = null;
    private Element subject = null;
    private Element predicate = null;
    private Element object = null;

    final private VarDictionary varDict = VarDictionary.get();
    final private NodeDictionary nd = NodeDictionaryFactory.get();

    public QuadStarPattern createQuadPattern() {
        return new QuadStarPattern(graph, subject, predicate, object);
    }

    public static QuadStarPattern createQuadPattern(Quad quad){
        return createQuadPattern(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }

    public static QuadStarPattern createQuadPattern(Node graph, Node subject, Node predicate, Node object) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(graph);
        builder.setSubject(subject);
        builder.setPredicate(predicate);
        builder.setObject(object);
        return builder.createQuadPattern();
    }

    private QuadStarPattern createEmbeddedPattern(Node_Triple node) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        final Triple t = node.get();
        builder.setGraph(graph);
        builder.setSubject(t.getSubject());
        builder.setPredicate(t.getPredicate());
        builder.setObject(t.getObject());
        return builder.createQuadPattern();
    }

    public void setGraph(Element graph) {
        this.graph = graph;
    }

    public void setGraph(Node node) {
        if (node.isConcrete()) {
            final Long id = nd.getId(node);
            graph = id != null ? new Key(id) : null;
        } else {
            graph = varDict.createVariable((Var) node);
        }
    }

    public void setSubject(Node node) {
        if (node.isConcrete()) {
            if (node instanceof Node_Triple) {
                subject = createEmbeddedPattern((Node_Triple) node);
            } else {
                final Long id = nd.getId(node);
                subject = id != null ? new Key(id) : null;
            }
        } else {
            subject = varDict.createVariable((Var) node);
        }
    }

    public void setPredicate(Node node) {
        if (node.isConcrete()) {
            final Long id = nd.getId(node);
            predicate = id != null ? new Key(id) : null;
        } else {
            predicate = varDict.createVariable((Var) node);
        }
    }

    public void setObject(Node node) {
        if (node.isConcrete()) {
            if (node instanceof Node_Triple) {
                object = createEmbeddedPattern((Node_Triple) node);
            } else {
                final Long id = nd.getId(node);
                object = id != null ? new Key(id) : null;
            }
        } else {
            object = varDict.createVariable((Var) node);
        }
    }
}
