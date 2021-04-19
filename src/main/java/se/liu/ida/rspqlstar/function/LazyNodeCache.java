package se.liu.ida.rspqlstar.function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;

import java.util.HashMap;
import java.util.Map;

public class LazyNodeCache {
    private static Map<String, Node> cache = new HashMap<>();
    public static int cacheHits = 0;

    public static Node get(String key){
        Node node = cache.get(key);
        if (node != null) {
            cacheHits++;
        }
        return node;
    }

    public static void add(String key, Node node){
        cache.put(key, node);
    }

    public static void reset(){
        cache.clear();
        cacheHits = 0;
    }

    public static int size(){
        return cache.size();
    }
}
