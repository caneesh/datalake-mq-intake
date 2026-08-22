package com.hcsc.datalake.mqintake.core.audit;

import java.io.IOException;
import java.util.Set;

/**
 * Extracts identity values from a sequence file.
 *
 * <p>From DESIGN.md §10: The identity field is payload_guid with mq_message_id
 * as fallback. Identity values are used for message-ID set comparison to
 * classify files with no audit record.
 *
 * <p>Implementations read only the identity field where the format permits,
 * avoiding full record deserialization for efficiency (§12).
 */
public interface IdentityExtractor {

    /**
     * Extracts all identity values from a file.
     *
     * <p>The identity value is payload_guid if present, otherwise mq_message_id.
     * Returns an empty set if no identities could be extracted.
     *
     * @param filePath path to the sequence file
     * @return set of identity values found in the file
     * @throws IOException if the file cannot be read
     */
    Set<String> extractIdentities(String filePath) throws IOException;

    /**
     * Returns the number of records in the file without extracting identities.
     * Useful for quick count checks before full extraction.
     *
     * @param filePath path to the sequence file
     * @return number of records in the file
     * @throws IOException if the file cannot be read
     */
    int countRecords(String filePath) throws IOException;
}
