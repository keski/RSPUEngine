package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.util.TimeUtil;

public class ConstructStream implements ContinuousListener {
    private RDFStarStream stream;
    private final org.apache.log4j.Logger logger = Logger.getLogger(RSPQLStarQuery.class);

    public ConstructStream(RDFStarStream stream){
        this.stream = stream;
    }

    @Override
    public void push(ResultSet rs) {
        throw new IllegalStateException("ConstructStream supports only construct queries");
    }

    @Override
    public void push(Dataset ds) {
        ds.listNames().forEachRemaining(g -> {
            final RDFStarStreamElement tg = new RDFStarStreamElement();
            final Resource graph = ResourceFactory.createResource(g);

            ds.getNamedModel(g).listStatements().forEachRemaining(stmt -> {
                tg.quad(new Quad(graph.asNode(), stmt.asTriple()));
            });
            ds.getDefaultModel().listStatements(graph, null, (Resource) null).forEachRemaining(stmt -> {
                tg.quad(new Quad(null, stmt.asTriple()));
            });
            stream.push(tg);
        });
    }

    @Override
    public void push(RDFStarStreamElement tg) {
        stream.push(tg);
    }
}
