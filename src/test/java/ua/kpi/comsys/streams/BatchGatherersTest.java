package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(2)
class BatchGatherersTest {

    private static Stream<Integer> naturals() {
        return Stream.iterate(1, n -> n + 1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void nonPositiveBatchSizeThrows(int batchSize) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> BatchGatherers.fixedBatches(batchSize, 1));
        assertEquals("batchSize must be greater than 0", e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void nonPositiveMaxBatchesThrows(int maxBatches) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> BatchGatherers.fixedBatches(1, maxBatches));
        assertEquals("maxBatches must be greater than 0", e.getMessage());
    }

    @Test
    void infiniteStreamCutAfterMaxBatches() {
        List<List<Integer>> batches = naturals().gather(BatchGatherers.fixedBatches(3, 2)).toList();
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6)), batches);
    }

    @Test
    void pullsExactlyBatchSizeTimesMaxBatchesElements() {
        AtomicInteger pulled = new AtomicInteger();
        List<List<Integer>> batches = naturals()
                .peek(n -> pulled.incrementAndGet())
                .gather(BatchGatherers.fixedBatches(4, 5))
                .toList();

        assertEquals(5, batches.size());
        assertEquals(20, pulled.get());
    }

    @Test
    void generatedStreamWithSingleElementBatches() {
        List<List<String>> batches = Stream.generate(() -> "x")
                .gather(BatchGatherers.fixedBatches(1, 3))
                .toList();
        assertEquals(List.of(List.of("x"), List.of("x"), List.of("x")), batches);
    }

    @Test
    void finiteStreamEndingOnBatchBoundary() {
        List<List<Integer>> batches = Stream.of(1, 2, 3, 4).gather(BatchGatherers.fixedBatches(2, 10)).toList();
        assertEquals(List.of(List.of(1, 2), List.of(3, 4)), batches);
    }

    @Test
    void finiteStreamLeavesShorterLastBatch() {
        List<List<Integer>> batches = Stream.of(1, 2, 3, 4, 5).gather(BatchGatherers.fixedBatches(2, 10)).toList();
        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5)), batches);
    }

    @Test
    void shortFiniteStreamGivesSinglePartialBatch() {
        List<List<Integer>> batches = Stream.of(1, 2).gather(BatchGatherers.fixedBatches(5, 3)).toList();
        assertEquals(List.of(List.of(1, 2)), batches);
    }

    @Test
    void emptyStreamGivesNoBatches() {
        assertEquals(List.of(), Stream.<Integer>empty().gather(BatchGatherers.fixedBatches(2, 2)).toList());
    }

    @Test
    void hugeBatchSizeDoesNotPreallocate() {
        List<List<Integer>> batches = Stream.of(1, 2).gather(BatchGatherers.fixedBatches(Integer.MAX_VALUE, 1)).toList();
        assertEquals(List.of(List.of(1, 2)), batches);
    }

    @Test
    void downstreamLimitStopsGatherer() {
        AtomicInteger pulled = new AtomicInteger();
        List<List<Integer>> batches = naturals()
                .peek(n -> pulled.incrementAndGet())
                .gather(BatchGatherers.fixedBatches(2, 100))
                .limit(3)
                .toList();

        assertEquals(List.of(List.of(1, 2), List.of(3, 4), List.of(5, 6)), batches);
        assertEquals(6, pulled.get());
    }

    @Test
    void partialBatchNotPushedWhenDownstreamRejects() {
        // limit(1) rejects after the first batch, the leftover 3 must not be pushed
        List<List<Integer>> batches = Stream.of(1, 2, 3).gather(BatchGatherers.fixedBatches(2, 5)).limit(1).toList();
        assertEquals(List.of(List.of(1, 2)), batches);
    }

    @Test
    void nullElementsKept() {
        List<List<String>> batches = Stream.of("a", null, null, "b")
                .gather(BatchGatherers.fixedBatches(2, 2))
                .toList();
        assertEquals(List.of(Arrays.asList("a", null), Arrays.asList(null, "b")), batches);
    }

    @Test
    void batchesAreUnmodifiable() {
        List<List<Integer>> batches = Stream.of(1, 2).gather(BatchGatherers.fixedBatches(2, 1)).toList();
        assertThrows(UnsupportedOperationException.class, () -> batches.getFirst().add(3));
    }

    @Test
    void parallelInfiniteStreamKeepsOrder() {
        List<List<Integer>> batches = naturals().parallel().gather(BatchGatherers.fixedBatches(3, 3)).toList();
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6), List.of(7, 8, 9)), batches);
    }
}
