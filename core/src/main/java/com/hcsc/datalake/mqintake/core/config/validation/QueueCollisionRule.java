package com.hcsc.datalake.mqintake.core.config.validation;

import com.hcsc.datalake.mqintake.core.config.BindingConfig;
import com.hcsc.datalake.mqintake.core.config.IntakeProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Queues that must not collide, within and across bindings.
 *
 * <p>The sharpest case: a backout queue equal to a source queue creates a
 * poison feedback loop — the "isolated" message is routed straight back into
 * consumption, delivery count climbing forever. Nothing validated this;
 * a copy-paste slip in YAML would have produced a self-sustaining loop at
 * runtime with no startup complaint.
 *
 * <p>Tracker queues must also be unique per connection: two bindings sending
 * trackers to one queue would interleave notification streams no consumer
 * could tell apart.
 */
public class QueueCollisionRule implements BindingConfigRule {

    @Override
    public List<String> validate(IntakeProperties properties) {
        List<String> errors = new ArrayList<>();

        // Everything consumed, keyed by (connection, queue)
        Set<String> sourceKeys = new HashSet<>();
        for (BindingConfig binding : properties.getBindings()) {
            if (binding.getSourceQueue() != null) {
                sourceKeys.add(key(binding, binding.getSourceQueue()));
            }
        }

        Map<String, String> trackerOwners = new HashMap<>();

        for (BindingConfig binding : properties.getBindings()) {
            String id = binding.getId();

            String backout = binding.getBackoutQueue();
            if (backout != null && !backout.isBlank()
                    && sourceKeys.contains(key(binding, backout))) {
                errors.add("Binding '" + id + "' backout-queue '" + backout + "' is also a "
                        + "source queue on the same connection — poison messages would be "
                        + "routed straight back into consumption, a feedback loop");
            }

            String tracker = binding.getTrackerQueue();
            if (tracker != null && !tracker.isBlank()) {
                if (sourceKeys.contains(key(binding, tracker))) {
                    errors.add("Binding '" + id + "' tracker-queue '" + tracker
                            + "' is also a source queue on the same connection — the service "
                            + "would consume its own tracker notifications");
                }
                String previous = trackerOwners.putIfAbsent(key(binding, tracker), id);
                if (previous != null) {
                    errors.add("Tracker queue '" + tracker + "' is shared by bindings '"
                            + previous + "' and '" + id + "' on the same connection — their "
                            + "notification streams would interleave indistinguishably");
                }
            }
        }
        return errors;
    }

    private String key(BindingConfig binding, String queue) {
        return binding.getMqConnection() + "::" + queue;
    }
}
