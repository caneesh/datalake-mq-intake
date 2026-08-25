package com.hcsc.datalake.mqintake.core.failure.classification;

import com.hcsc.datalake.mqintake.core.failure.FailureClass;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;

import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.anyType;
import static com.hcsc.datalake.mqintake.core.failure.classification.ThrowableMatcher.messageContains;

/**
 * The service is stopping, so the failure is expected and means nothing about
 * the data or the infrastructure.
 *
 * <p>Marked as relying on message text: interruption is often wrapped in a
 * type we cannot name, so the fallback searches for "interrupted"/"shutdown".
 * That is a blunt test — any payload containing those words would match — which
 * is why this rule must never sit above a type-based one.
 */
public final class ShutdownRule extends MatcherFailureRule {

    public ShutdownRule() {
        super(FailureClass.SHUTDOWN,
                anyType(InterruptedException.class,
                        InterruptedIOException.class,
                        ClosedByInterruptException.class)
                .or(messageContains("interrupted", "shutdown")),
                true);
    }
}
