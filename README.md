# Лабораторна робота 3 - Stream API, Optional, Stream Gatherers

Студент: Illia Verezei, група ІМ-42

Сервіс `ua.kpi.comsys.streams.TransactionStreamAnalyzer` для аналізу нескінченних стрімів транзакцій: ледачі операції, short-circuiting, `Optional` та власний `Gatherer`.

## Варіант

N = 9 (номер у списку групи), V = 9 mod 3 = **0** - електронна комерція та стрім транзакцій.

## Реалізація

- `Transaction` - record (id, категорія, `BigDecimal` сума, час).
- `findFirstHighValueTransaction` - `filter` + `findFirst`, сума строго більша за поріг; немає збігу - `Optional.empty()`.
- `takeTransactionsUntilBudgetExceeded` - `takeWhile` зі stateful предикатом накопиченої суми; транзакція, що перевищує бюджет, не входить у результат.
- `processBatches` - `BatchGatherers.fixedBatches` на `Gatherer.ofSequential`: батчі по `batchSize`, після `maxBatches` інтегратор повертає `false` і стрім зупиняється. На скінченному стрімі залишок стає коротшим останнім батчем.
- `null` аргументи, від'ємний бюджет, `batchSize <= 0`, `maxBatches <= 0` - `IllegalArgumentException`.
- `null` транзакції та транзакції без суми пропускаються.

## Тести

JUnit 5, `@Timeout(2)` на всіх класах тестів. Джерела - `Stream.iterate` / `Stream.generate`; лічильник через `peek` перевіряє, що з нескінченного стріму забирається рівно стільки елементів, скільки потрібно. Покриття JaCoCo - 100% рядків і гілок.

## Запуск

Gradle 9.1 (wrapper), Java 25 (потрібна для `Gatherer`). JDK 25 Gradle завантажує сам через toolchain, достатньо будь-якої JDK 17+ для запуску wrapper.

```bash
./gradlew build     # збірка + тести
./gradlew test      # лише тести
```

Звіт покриття: `build/reports/jacoco/test/html/index.html`.
