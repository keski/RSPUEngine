package se.liu.ida.rspqlstar.store.engine.main.iterator;

import jdk.nashorn.internal.runtime.JSONListAdapter;
import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;

import java.util.Iterator;

public class IdBasedVarToVarExtendIterator implements Iterator<SolutionMapping> {

    private final int var1;
    private final int var2;
    private final Iterator<SolutionMapping> input;

    /**
     * Bind value of var2 to var1.
     * @param var1 The var to be bound
     * @param var2 The var that has already been bound
     * @param input
     * @param execCxt
     */
    public IdBasedVarToVarExtendIterator(int var1, int var2, Iterator<SolutionMapping> input, ExecutionContext execCxt) {
        this.var1 = var1;
        this.var2 = var2;
        this.input = input;
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public SolutionMapping next() {
        final SolutionMapping solMap = input.next();

        for(int i = 0; i < solMap.size(); i++){
            System.err.println(i + " = " + solMap.get(i));
        }

        System.err.println(var1 + " = " + solMap.get(var1));
        System.err.println(var2 + " = " + solMap.get(var2));
        System.err.println("Setting " + var1 + " to " + solMap.get(var2));
        solMap.set(var1, solMap.get(var2));
        return solMap;
    }
}
