package io.github.radarroark.xitdb;

/**
 * maps source offsets to target offsets during compaction.
 * mappings must remain available until reset to preserve sharing and cycles.
 * each map requires exclusive use during compaction.
 */
public interface OffsetMap {
    /** clears mappings from the previous compaction. */
    void reset() throws Exception;

    /** returns the target offset, or null if the source offset is absent. */
    Long get(long sourceOffset) throws Exception;

    /** records where the source object was copied. */
    void put(long sourceOffset, long targetOffset) throws Exception;
}
