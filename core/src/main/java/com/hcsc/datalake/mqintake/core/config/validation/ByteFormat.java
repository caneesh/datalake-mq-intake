package com.hcsc.datalake.mqintake.core.config.validation;

/**
 * Human-readable byte sizes for configuration error messages.
 *
 * <p>An operator reading "134217728 exceeds 1073741824" has to do arithmetic
 * before they can act; "128.00 MB exceeds 1.00 GB" is immediately actionable.
 */
public final class ByteFormat {

    private ByteFormat() {
    }

    public static String format(long bytes) {
        if (bytes >= 1_073_741_824) {
            return String.format("%.2f GB", bytes / 1_073_741_824.0);
        } else if (bytes >= 1_048_576) {
            return String.format("%.2f MB", bytes / 1_048_576.0);
        } else if (bytes >= 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        }
        return bytes + " bytes";
    }
}
