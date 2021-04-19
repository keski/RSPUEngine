package evaluation;

import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.function.LazyNodeCache;
import se.liu.ida.rspqlstar.function.LazyNodeValue;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Lazy_Node_Concrete_WithID;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.IOException;

public class ExperimentConfiguration {
    public String queryFile;
    public int numberOfResults;
    public int warmUp;
    public boolean rspuFilterPull;
    public boolean useLazyVars;
    public boolean useCache;
    public double[] thresholds;
    public double[] selectivities;
    public long uncertaintyFunctionThrottle;
    public boolean oxStream1;
    public boolean oxStream2;
    public boolean tempStream1;
    public boolean tempStream2;
    public String join = null;

    public ExperimentConfiguration(
            String queryFile,
            int numberOfResults,
            int warmUp,
            boolean useCache,
            boolean useLazyVars,
            boolean rspuFilterPull,
            double[] thresholds,
            double[] selectivities,
            long uncertaintyFunctionThrottle,
            boolean oxStream1,
            boolean oxStream2,
            boolean tempStream1,
            boolean tempStream2) throws IOException {
        this.queryFile = queryFile;
        this.numberOfResults = numberOfResults;
        this.warmUp = warmUp;
        this.useCache = useCache;
        this.useLazyVars = useLazyVars;
        this.rspuFilterPull = rspuFilterPull;
        this.thresholds = thresholds;
        this.selectivities = selectivities;
        this.uncertaintyFunctionThrottle = uncertaintyFunctionThrottle;
        this.oxStream1 = oxStream1;
        this.oxStream2 = oxStream2;
        this.tempStream1 = tempStream1;
        this.tempStream2 = tempStream2;

        if(selectivities.length != thresholds.length){
            throw new IllegalStateException("Differing lengths!");
        }
    }

    public void load(){
        // reset all
        Lazy_Node_Concrete_WithID.CACHE_ENABLED = useCache;
        RSPQLStarAlgebraGenerator.PULL_RSPU_FILTERS = rspuFilterPull;
        Probability.USE_LAZY_VAR = useLazyVars;
        Lazy_Node_Concrete_WithID.THROTTLE_EXECUTION = uncertaintyFunctionThrottle;
        LazyNodeCache.reset();
        // clear gc and sleep for 3 seconds
        System.gc();
        TimeUtil.silentSleep(15000);
    }
}
