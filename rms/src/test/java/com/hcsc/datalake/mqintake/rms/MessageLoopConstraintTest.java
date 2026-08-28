package com.hcsc.datalake.mqintake.rms;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CLAUDE.md standing constraint #1: the message loop is hand-rolled —
 * {@code @JmsListener}, {@code DefaultMessageListenerContainer} and
 * {@code JmsTemplate} impose the per-message transaction boundary this
 * project exists to remove. Today that rule is enforced by spring-jms simply
 * not being a dependency; this test turns that accident into a guard, so the
 * dependency arriving (directly or transitively) fails the build instead of
 * silently making the forbidden types available.
 */
class MessageLoopConstraintTest {

    @Test
    void springJmsMustNotBeOnTheClasspath() {
        for (String forbidden : List.of(
                "org.springframework.jms.annotation.JmsListener",
                "org.springframework.jms.listener.DefaultMessageListenerContainer",
                "org.springframework.jms.core.JmsTemplate")) {
            assertThatThrownBy(() -> Class.forName(forbidden))
                    .as(forbidden + " must not be reachable — the receive loop is "
                            + "hand-rolled by design (CLAUDE.md standing constraint #1)")
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }
}
