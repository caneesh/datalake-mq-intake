package com.hcsc.datalake.mqintake.core.audit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for FileClassification enum.
 *
 * From DESIGN.md §10: Classification determines what action is safe
 * for a file with no audit record.
 */
class FileClassificationTest {

    @Test
    void duplicateIsSafeToQuarantine() {
        assertThat(FileClassification.DUPLICATE.isSafeToQuarantine()).isTrue();
        assertThat(FileClassification.DUPLICATE.mustKeep()).isFalse();
    }

    @Test
    void soleCopyMustBeKept() {
        assertThat(FileClassification.SOLE_COPY.isSafeToQuarantine()).isFalse();
        assertThat(FileClassification.SOLE_COPY.mustKeep()).isTrue();
    }

    @Test
    void inconclusiveMustBeKept() {
        assertThat(FileClassification.INCONCLUSIVE.isSafeToQuarantine()).isFalse();
        assertThat(FileClassification.INCONCLUSIVE.mustKeep()).isTrue();
    }

    @Test
    void onlyDuplicateIsSafeToQuarantine() {
        for (FileClassification classification : FileClassification.values()) {
            if (classification == FileClassification.DUPLICATE) {
                assertThat(classification.isSafeToQuarantine())
                        .as("DUPLICATE should be safe to quarantine")
                        .isTrue();
            } else {
                assertThat(classification.isSafeToQuarantine())
                        .as(classification + " should NOT be safe to quarantine")
                        .isFalse();
            }
        }
    }

    @Test
    void allClassificationsHaveDescriptions() {
        for (FileClassification classification : FileClassification.values()) {
            assertThat(classification.getDescription())
                    .as(classification + " should have a description")
                    .isNotBlank();
        }
    }
}
