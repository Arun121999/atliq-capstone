# Phase 2 Write-up

## 1. Speed lane vs nightly batch

AtliQ needs the speed lane when someone has to act within minutes. For example, the operations team wants to know how many orders are coming in right now, or whether revenue dropped in the last ten minutes. The nightly batch cannot answer this because it only shows yesterday's numbers. My `gold_revenue_5min` table does it: one 5-minute window showed 22 paid orders and revenue 63506.

The nightly batch is enough when numbers are only reviewed later. For example, leadership's morning sales report in Fabric shows yesterday's sales. Nobody acts on it minute by minute, so a nightly load is simpler and cheaper.

## 2. Why the Gold windows appeared late

One row from `gold_revenue_5min`: window 16:35 to 16:40, 22 orders, revenue 63506.

This row was not in the first batch of the run (table version 1 was empty). It appeared in the second batch (version 2), together with the other 7 windows. The latest window written was 19:10 to 19:15, and nothing after it.

A window is written only after the watermark passes its end. The first batch moved the watermark forward, and only the next batch could write the windows that had closed. Windows that had not closed yet stayed unwritten. This is the cost of waiting for late events, not a bug.

## 3. Why events are keyed by order_id

Kafka keeps order only inside one partition, and the same key always goes to the same partition. With `order_id` as the key, all events of one order (placed, paid, cancelled) stay together and are read in order.

If keys were random, one order's events could land in different partitions and be read out of order, for example payment before placed. That would break de-duplication, order-level counts and any logic that depends on the order lifecycle.

## 4. What Silver dropped

Bronze had 1948 rows and Silver had 1888, so Silver removed 60 records:

- 42 replays: the same `event_id` seen twice, removed by `dropDuplicates(["event_id", "event_ts"])`.
- 18 broken records: a missing `event_ts` or an `order_amount` of 'N/A'. `from_json` turns these into NULL, and the NULL filter removed them.
- 0 very late events: none arrived later than the 10-minute watermark.

How I checked: I compared the two table counts (1948 vs 1888, a difference of 60) and looked for repeated `event_id` values in Bronze to count the replays. The remaining difference is the broken records.
