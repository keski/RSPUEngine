package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

public interface ContinuousListener {
    void push(ResultSet rs, long executionTime);
    void push(Dataset ds, long executionTime);
    void push(RDFStarStreamElement tg);
    default void setSkip(int skip){};
    default void flush(){};
    default void close(){};
}
