package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ua.kpi.comsys.streams.Transactions.infinite;
import static ua.kpi.comsys.streams.Transactions.tx;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(2)
class TransactionStreamAnalyzerTest {

    private final TransactionStreamAnalyzer analyzer = new TransactionStreamAnalyzer();

    private static List<String> ids(List<Transaction> transactions) {
        return transactions.stream().map(Transaction::transactionId).toList();
    }

    @Nested
    class FindFirstHighValueTransaction {

        @Test
        void nullStreamThrows() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(null, BigDecimal.TEN));
            assertEquals("stream cannot be null", e.getMessage());
        }

        @Test
        void nullThresholdThrows() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.findFirstHighValueTransaction(Stream.empty(), null));
            assertEquals("threshold cannot be null", e.getMessage());
        }

        @Test
        void emptyStreamGivesEmptyOptional() {
            assertEquals(Optional.empty(), analyzer.findFirstHighValueTransaction(Stream.empty(), BigDecimal.ONE));
        }

        @Test
        void finiteStreamWithoutMatchGivesEmptyOptional() {
            Stream<Transaction> stream = Stream.of(tx(1, 10), tx(2, 50), tx(3, 99));
            assertTrue(analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100")).isEmpty());
        }

        @Test
        void returnsFirstOfSeveralMatches() {
            Stream<Transaction> stream = Stream.of(tx(1, 10), tx(2, 500), tx(3, 900));
            Optional<Transaction> found = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));
            assertEquals(Optional.of("tx-2"), found.map(Transaction::transactionId));
        }

        @Test
        void amountEqualToThresholdIsNotHighValue() {
            Stream<Transaction> stream = Stream.of(tx(1, "100.00"), tx(2, "100.01"));
            Optional<Transaction> found = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("100"));
            assertEquals(Optional.of("tx-2"), found.map(Transaction::transactionId));
        }

        @Test
        void nullTransactionsAndAmountsSkipped() {
            Transaction noAmount = new Transaction("tx-0", "books", null, null);
            Stream<Transaction> stream = Stream.of(null, noAmount, tx(1, 1000));
            Optional<Transaction> found = analyzer.findFirstHighValueTransaction(stream, BigDecimal.ONE);
            assertEquals(Optional.of("tx-1"), found.map(Transaction::transactionId));
        }

        @Test
        void infiniteStreamStopsAtFirstMatch() {
            AtomicInteger pulled = new AtomicInteger();
            Stream<Transaction> stream = infinite(n -> n == 1000 ? "5000" : "1", pulled);

            Optional<Transaction> found = analyzer.findFirstHighValueTransaction(stream, new BigDecimal("4999.99"));

            assertEquals(Optional.of("tx-1000"), found.map(Transaction::transactionId));
            assertEquals(1000, pulled.get());
        }

        @Test
        void infiniteGeneratedStreamWithGrowingAmounts() {
            Optional<BigDecimal> amount = analyzer
                    .findFirstHighValueTransaction(infinite(n -> String.valueOf(n * 3)), new BigDecimal("100"))
                    .map(Transaction::amount);
            assertEquals(Optional.of(new BigDecimal("102")), amount);
        }
    }

    @Nested
    class TakeTransactionsUntilBudgetExceeded {

        @Test
        void nullStreamThrows() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(null, BigDecimal.TEN));
            assertEquals("stream cannot be null", e.getMessage());
        }

        @Test
        void nullBudgetThrows() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), null));
            assertEquals("maxBudget cannot be null", e.getMessage());
        }

        @Test
        void negativeBudgetThrows() {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), new BigDecimal("-0.01")));
            assertEquals("maxBudget cannot be negative", e.getMessage());
        }

        @Test
        void emptyStreamGivesEmptyList() {
            assertEquals(List.of(), analyzer.takeTransactionsUntilBudgetExceeded(Stream.empty(), BigDecimal.TEN));
        }

        @Test
        void finiteStreamWithinBudgetTakenWhole() {
            Stream<Transaction> stream = Stream.of(tx(1, 10), tx(2, 20), tx(3, 30));
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));
            assertEquals(List.of("tx-1", "tx-2", "tx-3"), ids(taken));
        }

        @Test
        void stopsBeforeTransactionThatExceedsBudget() {
            Stream<Transaction> stream = Stream.of(tx(1, 40), tx(2, 40), tx(3, 40), tx(4, 1));
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));
            // tx-4 alone would fit, but takeWhile stops at the first violation
            assertEquals(List.of("tx-1", "tx-2"), ids(taken));
        }

        @Test
        void totalEqualToBudgetIsAllowed() {
            Stream<Transaction> stream = Stream.of(tx(1, "60.50"), tx(2, "39.50"), tx(3, "0.01"));
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100"));
            assertEquals(List.of("tx-1", "tx-2"), ids(taken));
        }

        @Test
        void firstTransactionOverBudgetGivesEmptyList() {
            Stream<Transaction> stream = Stream.of(tx(1, 500), tx(2, 1));
            assertEquals(List.of(), analyzer.takeTransactionsUntilBudgetExceeded(stream, new BigDecimal("100")));
        }

        @Test
        void zeroBudgetTakesOnlyZeroAmounts() {
            Stream<Transaction> stream = Stream.of(tx(1, 0), tx(2, 0), tx(3, 1), tx(4, 0));
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(stream, BigDecimal.ZERO);
            assertEquals(List.of("tx-1", "tx-2"), ids(taken));
        }

        @Test
        void nullTransactionsAndAmountsSkipped() {
            Transaction noAmount = new Transaction("tx-0", "books", null, null);
            List<Transaction> source = new ArrayList<>(Arrays.asList(tx(1, 30), null, noAmount, tx(2, 30)));
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(source.stream(), new BigDecimal("50"));
            assertEquals(List.of("tx-1"), ids(taken));
        }

        @Test
        void infiniteStreamStopsWhenBudgetExceeded() {
            AtomicInteger pulled = new AtomicInteger();
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(
                    infinite(n -> "25", pulled), new BigDecimal("1000"));

            assertEquals(40, taken.size());
            assertEquals("tx-40", taken.getLast().transactionId());
            assertEquals(41, pulled.get());
        }

        @Test
        void parallelStreamStillRespectsEncounterOrder() {
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(
                    infinite(n -> String.valueOf(n)).parallel(), new BigDecimal("55"));
            // 1 + 2 + ... + 10 = 55
            assertEquals(10, taken.size());
        }

        @Test
        void resultIsUnmodifiable() {
            List<Transaction> taken = analyzer.takeTransactionsUntilBudgetExceeded(
                    Stream.of(tx(1, 1)), BigDecimal.TEN);
            assertThrows(UnsupportedOperationException.class, () -> taken.add(tx(2, 1)));
        }
    }
}
