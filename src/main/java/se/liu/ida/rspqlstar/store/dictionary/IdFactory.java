package se.liu.ida.rspqlstar.store.dictionary;

import java.util.concurrent.atomic.AtomicInteger;

public class IdFactory {
    static final public long REFERENCE_BIT =
            IdFactory.makeLong("1000000000000000000000000000000000000000000000000000000000000000");
    // Note: Although we don't use embedded nodes here, we could embed datatypes and values as in CQELS

    static private AtomicInteger nodeIdCounter = new AtomicInteger();
    static private AtomicInteger referenceIdCounter = new AtomicInteger();;

    /**
     * Create the next node ID for the node dictionary.
     *
     * @return
     */
    public static long nextNodeId() {
        return nodeIdCounter.incrementAndGet();
    }

    /**
     * Get current node ID.
     *
     * @return
     */
    public static long getNodeId() {
        return nodeIdCounter.get();
    }

    /**
     * Create the next reference ID for the node dictionary.
     *
     * @return
     */
    public static long nextReferenceKeyId() {
        return REFERENCE_BIT + referenceIdCounter.incrementAndGet();
    }

    /**
     * Returns true if the provided id is a reference triple id.
     *
     * @param id
     * @return
     */
    public static boolean isReferenceId(long id) {
        return (id >>> 62) == 2;
    }

    /**
     * Produces a long for a 64 bit string.
     * Note: Java encodes strings starting with 1 as negative values. This means that all 64 bits strings need to be
     * parsed as negative 63 bit values.
     *
     * @param input
     * @return
     */
    public static long makeLong(String input) {
        if (input.substring(0, 1).equals("1")) {
            return -1 * (Long.MAX_VALUE - Long.parseLong(input.substring(1), 2) + 1);
        } else {
            return Long.parseLong(input, 2);
        }
    }

    public static long getReferenceIdBody(long id) {
        return id & ~REFERENCE_BIT;
    }

    public static void reset(){
        nodeIdCounter = new AtomicInteger();
        referenceIdCounter = new AtomicInteger();
    }
}
