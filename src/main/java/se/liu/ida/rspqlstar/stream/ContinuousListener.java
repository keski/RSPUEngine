package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ResultSet;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

public interface ContinuousListener {
    void push(ResultSet rs, long startedAt);
    void push(Dataset ds, long startedAt);
    default void push(ResultSet rs){
        push(rs, -1);
    }
    default void push(Dataset ds) {
        push(ds, -1);
    };
    void push(RDFStarStreamElement tg);
    default void setSkip(int skip){};
    default void flush(){};
    default void close(){};
}
