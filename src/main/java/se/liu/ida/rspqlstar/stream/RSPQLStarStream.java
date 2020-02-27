package se.liu.ida.rspqlstar.stream;

import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

public interface RSPQLStarStream {
    void push(RDFStarStreamElement tg);
}
