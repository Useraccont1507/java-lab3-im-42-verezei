package ua.kpi.comsys.streams;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Stream;

/** Test data: finite and infinite transaction streams. */
final class Transactions {

    private static final LocalDateTime START = LocalDateTime.of(2026, 10, 1, 12, 0);

    private Transactions() {
    }

    static Transaction tx(int n, String amount) {
        return new Transaction("tx-" + n, "books", new BigDecimal(amount), START.plusMinutes(n));
    }

    static Transaction tx(int n, int amount) {
        return tx(n, String.valueOf(amount));
    }

    /** Infinite stream tx-1, tx-2, ... with amounts given by {@code amountOf(n)}. */
    static Stream<Transaction> infinite(IntFunction<String> amountOf) {
        return Stream.iterate(1, n -> n + 1).map(n -> tx(n, amountOf.apply(n)));
    }

    /** Same as {@link #infinite(IntFunction)}, counting how many elements were pulled from the source. */
    static Stream<Transaction> infinite(IntFunction<String> amountOf, AtomicInteger pulled) {
        return infinite(amountOf).peek(t -> pulled.incrementAndGet());
    }
}
