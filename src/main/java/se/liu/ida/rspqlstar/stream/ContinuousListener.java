package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

public interface ContinuousListener {
    void push(ResultSet rs);
    void push(Dataset ds);
    void push(RDFStarStreamElement tg);
}
