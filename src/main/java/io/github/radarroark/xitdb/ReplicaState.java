package io.github.radarroark.xitdb;

/**
 * what a streaming replica sends its source, so the source knows which
 * bytes to send back (see {@link Database#replicationStart})
 */
public record ReplicaState(
    long committedSize,
    // the checksum of the trailer that ends the committed bytes. it covers the
    // file ID and the commit's random ID, so it identifies both the file and the
    // history that led to the commit.
    long checksum
) {}
