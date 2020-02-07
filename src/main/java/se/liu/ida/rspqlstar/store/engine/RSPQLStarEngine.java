package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.iterator.QueryIteratorCheck;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMainQuad;
import org.apache.jena.sparql.util.Context;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebra;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.engine.main.OpRSPQLStarExecutor;

import java.util.HashMap;
import java.util.Map;

public class RSPQLStarEngine extends QueryEngineMainQuad {
    private static final Logger logger = Logger.getLogger(RSPQLStarEngine.class);
    private Op queryOp;

    static final private QueryEngineFactory factory = new QueryEngineFactory() {
        private final Map<String, Op> cachedQueries = new HashMap<>();

        @Override
        public Plan create(Query q, DatasetGraph dsg, Binding binding, Context context) {
            final RSPQLStarQuery query = (RSPQLStarQuery) q;
            return create(getOp(query), dsg, binding, context);
        }

        @Override
        public Plan create(Op op, DatasetGraph dsg, Binding binding, Context context) {
            final RSPQLStarEngine engine = new RSPQLStarEngine(op, dsg, binding, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph datasetGraph, Context context) {
            return datasetGraph instanceof DatasetGraphStar;
        }

        @Override
        public boolean accept(Query query, DatasetGraph dsg, Context context) {
            return dsg instanceof DatasetGraph;
        }

        /**
         * Returned cached Op for query.
         * @param query
         * @return
         */
        public Op getOp(RSPQLStarQuery query){
            if(!cachedQueries.containsKey(query.toString())){
                cachedQueries.put(query.toString(), RSPQLStarAlgebra.compile(query));
            }
            return cachedQueries.get(query.toString());
        }
    };

    public RSPQLStarEngine(Op op, DatasetGraph dsg, Binding input, Context context) {
        super(op, dsg, input, context);
        queryOp = op;
        QC.setFactory(context, OpRSPQLStarExecutor.factory);
    }

    public RSPQLStarEngine(Query query, DatasetGraph dsg, Binding input, Context context) {
        super(query, dsg, input, context);
        createOp(query);
        QC.setFactory(context, OpRSPQLStarExecutor.factory);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    protected Op createOp(Query query) {
        queryOp = RSPQLStarAlgebra.compile(query);
        return queryOp;
    }

    protected Plan createPlan() {
        Op op = queryOp;
        if (!getStartBinding().isEmpty()) {
            op = Substitute.substitute(op, getStartBinding());
            this.context.put(ARQConstants.sysCurrentAlgebra, op);
        }

        //op = this.modifyOp(op);
        QueryIterator queryIterator = null;
        if (dataset != null) {
            queryIterator = evaluate(op, dataset, getStartBinding(), context);
        } else {
            throw new IllegalStateException("No dataset specified");
        }

        return new PlanOp(op, this, queryIterator);
    }


    public QueryIterator eval(Op op, DatasetGraph datasetGraph, Binding input, Context context) {
        final ExecutionContext execCxt = new ExecutionContext(context, null, datasetGraph, QC.getFactory(context));
        final QueryIterator iter1 = QueryIterRoot.create(input, execCxt);
        final QueryIterator iter2 = QC.execute(op, iter1, execCxt);
        return QueryIteratorCheck.check(iter2, execCxt); // check for closed iterators
    }
}
