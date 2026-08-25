package com.hcsc.datalake.mqintake.core.poison;

import javax.jms.Message;
import javax.jms.Session;
import java.util.List;

/**
 * Removes messages that have failed too often from a batch, so the rest can
 * proceed.
 *
 * <p>Screening and routing are one operation, not two. A poison message must
 * be put to the backout queue on the <em>same transacted session</em> that
 * received it, so the put and the get commit or roll back together. Splitting
 * this into "identify" then "route" would invite a caller to commit between
 * them and lose the message.
 *
 * <p>An interface so the receive loop depends on the behaviour rather than on
 * IBM MQ's delivery-count mechanism — a binding whose queue manager exposes
 * poison differently, or one that should not screen at all, becomes a
 * different implementation instead of a branch in the loop.
 */
public interface PoisonScreen {

    /**
     * Routes any message past its backout threshold to the backout queue and
     * returns the rest.
     *
     * @param session  the transacted session the batch was received on; the
     *                 backout put must join its transaction
     * @param messages the batch to screen
     * @return the messages that should still be landed
     * @throws PoisonMessageHandler.BackoutFailureException if a message could
     *         not be routed. The caller MUST NOT commit: rolling back is what
     *         keeps the message on the queue instead of dropping it.
     */
    PoisonMessageHandler.BatchPoisonCheckResult screen(Session session, List<Message> messages)
            throws PoisonMessageHandler.BackoutFailureException;
}
