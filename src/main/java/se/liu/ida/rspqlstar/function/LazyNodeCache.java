package se.liu.ida.rspqlstar.function;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.expr.NodeValue;

import java.util.HashMap;
import java.util.Map;

public class LazyNodeCache {
    public static boolean CACHE_ENABLED = true;
    private static Map<String, Node> cache = new HashMap<>();
    public static int cacheHits = 0;

    public static Node get(String key){
        if(CACHE_ENABLED) {
            Node node = cache.get(key);
            if (node != null) {
                cacheHits++;
            }
            return node;
        } else {
            return null;
        }
    }

    public static void add(String key, Node node){
        if(CACHE_ENABLED) {
            cache.put(key, node);
        }
    }

    public static void reset(){
        cache.clear();
        cacheHits = 0;
    }

    public static int size(){
        return cache.size();
    }
}
