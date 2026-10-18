package ua.kpi.comsys.streams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Gatherer;

/** Custom stream gatherers. */
public final class BatchGatherers {

    private BatchGatherers() {
    }

    /**
     * Groups elements into consecutive batches of {@code batchSize} and stops the stream
     * right after {@code maxBatches} batches are emitted, so it is safe on infinite streams.
     * If a finite stream ends early, the remaining elements form a shorter last batch.
     */
    public static <T> Gatherer<T, ?, List<T>> fixedBatches(int batchSize, int maxBatches) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
        }
        if (maxBatches <= 0) {
            throw new IllegalArgumentException("maxBatches must be greater than 0");
        }

        return Gatherer.<T, BatchState<T>, List<T>>ofSequential(
                BatchState::new,
                (state, element, downstream) -> {
                    state.buffer.add(element);
                    if (state.buffer.size() < batchSize) {
                        return true;
                    }
                    state.emitted++;
                    boolean accepted = downstream.push(state.drain());
                    return accepted && state.emitted < maxBatches;
                },
                (state, downstream) -> {
                    if (!state.buffer.isEmpty() && !downstream.isRejecting()) {
                        downstream.push(state.drain());
                    }
                });
    }

    private static final class BatchState<T> {

        private List<T> buffer = new ArrayList<>();
        private int emitted;

        /** Returns the current batch as an unmodifiable list and starts a new one. */
        List<T> drain() {
            List<T> batch = Collections.unmodifiableList(buffer);
            buffer = new ArrayList<>();
            return batch;
        }
    }
}
