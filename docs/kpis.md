# KPI definitions

The reporting cohort is the **UTC promised-delivery date**, including its assigned station and driver.
On-time is inclusive of the exact promised instant. Durations use elapsed milliseconds / 60,000;
regional daylight saving does not alter elapsed time. UTC reporting avoids mixing station calendars;
local-day reporting can be added explicitly with the dimension's IANA timezone.

| KPI | Formula | Denominator / exclusions |
|---|---|---|
| Total shipments | count of accepted unique shipments | Includes cancelled and in-transit; quarantine excluded |
| On-time delivery % | 100 × delivered at/before promised / delivered | Non-delivered excluded |
| Average delivery delay (minutes) | sum(max(0, delivered − promised)) / delivered | Includes on-time deliveries as zero; non-delivered excluded |
| First-attempt success % | 100 × shipments with successful attempt #1 / shipments with accepted attempt #1 | Pending/cancelled and missing first-attempt evidence excluded |
| Failed delivery % | 100 × FAILED shipments / (DELIVERED + FAILED shipments) | Terminal delivery outcomes only; cancellation is not a failed delivery |
| Failed attempt % | 100 × failed accepted attempts / all accepted attempts | A failed attempt followed by successful redelivery still counts as a failed attempt |
| Station/driver performance | Same formulas grouped by station/assigned driver | Compare workload and conditions before ranking |

Zero denominators produce **NULL**, not zero. Average delay is not a signed earliness/lateness measure
and is not “average among late shipments only.” The query set can calculate that separate metric.
A redelivery shipment counts once in shipment metrics, but all its accepted attempts contribute to
attempt metrics. Global/weekly rates must be recomputed from additive counters, never averaged from
daily/station percentages. Delivery evidence gaps are visible in the KPI tables.

The tests contain an independently calculated Python oracle plus a hand-worked boundary cohort:
one delivery exactly on deadline, one 60 minutes late after a failed first attempt, one terminal failure,
one pending shipment and one cancellation. Completed-cohort on-time is 50%, mean delay 30 minutes,
first-attempt success 1/3, failed delivery 1/3 and failed attempts 2/4. Pending-only rates are NULL.

These definitions are portfolio business choices, not claims about Amazon's internal metrics.
All source entities, conditions and results are synthetic. Weather/traffic analyses show associations
created by the generator; they do not establish causal findings about real delivery operations.
