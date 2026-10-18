package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

/**
 * Analyzes possibly infinite transaction streams using lazy, short-circuiting operations.
 * Null transactions and transactions without an amount are skipped.
 */
public class TransactionStreamAnalyzer {

    /**
     * Returns the first transaction whose amount is strictly greater than {@code threshold}.
     * Short-circuits on the first match, so infinite streams are safe as long as a match exists.
     */
    public Optional<Transaction> findFirstHighValueTransaction(Stream<Transaction> stream, BigDecimal threshold) {
        requireNonNull(stream, "stream");
        requireNonNull(threshold, "threshold");
        return stream
                .filter(TransactionStreamAnalyzer::hasAmount)
                .filter(t -> t.amount().compareTo(threshold) > 0)
                .findFirst();
    }

    /**
     * Takes transactions while their running total stays within {@code maxBudget}.
     * The first transaction that would push the total over the budget ends the stream and is not included.
     */
    public List<Transaction> takeTransactionsUntilBudgetExceeded(Stream<Transaction> stream, BigDecimal maxBudget) {
        requireNonNull(stream, "stream");
        requireNonNull(maxBudget, "maxBudget");
        if (maxBudget.signum() < 0) {
            throw new IllegalArgumentException("maxBudget cannot be negative");
        }
        return stream
                .sequential()
                .filter(TransactionStreamAnalyzer::hasAmount)
                .takeWhile(new WithinBudget(maxBudget))
                .toList();
    }

    /**
     * Splits the stream into batches of {@code batchSize} using {@link BatchGatherers#fixedBatches}
     * and stops after {@code maxBatches} batches.
     */
    public List<List<Transaction>> processBatches(Stream<Transaction> stream, int batchSize, int maxBatches) {
        requireNonNull(stream, "stream");
        Gatherer<Transaction, ?, List<Transaction>> batches = BatchGatherers.fixedBatches(batchSize, maxBatches);
        return stream
                .filter(TransactionStreamAnalyzer::hasAmount)
                .gather(batches)
                .toList();
    }

    private static boolean hasAmount(Transaction transaction) {
        return Optional.ofNullable(transaction)
                .map(Transaction::amount)
                .isPresent();
    }

    private static void requireNonNull(Object value, String name) {
        Optional.ofNullable(value)
                .orElseThrow(() -> new IllegalArgumentException(name + " cannot be null"));
    }

    /** Stateful predicate that accumulates amounts; valid only for one sequential stream. */
    private static final class WithinBudget implements Predicate<Transaction> {

        private final BigDecimal maxBudget;
        private BigDecimal spent = BigDecimal.ZERO;

        WithinBudget(BigDecimal maxBudget) {
            this.maxBudget = maxBudget;
        }

        @Override
        public boolean test(Transaction transaction) {
            BigDecimal total = spent.add(transaction.amount());
            if (total.compareTo(maxBudget) > 0) {
                return false;
            }
            spent = total;
            return true;
        }
    }
}
