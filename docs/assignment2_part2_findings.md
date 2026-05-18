# Assignment 2 – Part 2 Findings: TF-IDF + `ChiSqSelector` vs Assignment 1

## Setup

The Part 2 DataFrame pipeline mirrors the spec:

```
RegexTokenizer → StopWordsRemover → CountVectorizer → IDF → StringIndexer → ChiSqSelector(numTopFeatures=2000)
```

The selected vocabulary (2000 terms) is written, alphabetically sorted, to `output_ds.txt`.

For the comparison we use the **joined-dictionary line** of `output_rdd.txt` – the union of the per-category top-75 terms from Assignment 1 / Part 1 RDD on the same `reviews_devset.json` (78 829 reviews, 22 categories). The two RDD/MRJob runs are bit-identical (max chi² diff = 0.000000 across all 22 × 75 entries), so either can serve as the Assignment 1 reference.

## Headline numbers

| Set | Size |
|---|---|
| Part 2 `output_ds.txt` (TF-IDF + `ChiSqSelector`) | **2000** terms |
| Part 1 joined dictionary (∪ per-category top-75) | **1464** terms |
| Intersection | **751** terms |
| Part 2 only (in DS, never in any per-category top-75) | **1249** terms |
| Part 1 only (selected per-category but missed by `ChiSqSelector`) | **713** terms |

So the overlap is ~51 % of the Assignment 1 dictionary – the two methods agree on roughly half of their picks. The Part 2 spec explicitly anticipates this: *"do not expect to obtain identical results."*

## Examples of disagreement

**Strong per-category discriminators that Assignment 1 picks but `ChiSqSelector` skips** (chi² shown is from the Part 1 score in the nominating category):

| Term | Part 1 category | Part 1 chi² | In `output_ds.txt`? |
|---|---|---:|---|
| `crib` | Baby | 2411.5 | no |
| `acne` | Beauty | 1022.6 | no |
| `dewalt` | Tools_and_Home_Improvement | 1000.4 | no |
| `medela` | Baby | 856.0 | no |
| `aquarium` | Pet_Supplies | 450.6 | no |
| `airsoft` | Sports_and_Outdoor | 378.7 | no |
| `glock` | Sports_and_Outdoor | 324.8 | no |
| `appetite` | Health_and_Personal_Care | 315.8 | no |
| `acdelco` | Automotive | 281.9 | no |
| `ammo` | Sports_and_Outdoor | 208.3 | no |

These are exactly the "obvious" category labels a human would expect.

**Generic terms that `ChiSqSelector` keeps but Assignment 1 never selects** (sample of `output_ds.txt` head):

`access`, `accessories`, `account`, `acid`, `act`, `actions`, `adapters`, `adjust`, `adjustable`, `admit`, `adult`, `adults`, `adventures`, `advertised`, `advice`, `age`, `ages`, `agree`, `air`, `alive`, `amazing`, `amazon`, `america`, `american`, `amusing`, `analysis`, `ancient`, `angle`, `animals`, `answers`, …

These are common English words with mediocre per-category specificity.

## Why the two methods disagree

Three concrete pipeline differences explain the gap:

1. **Feature representation.** Part 1 builds a binary document-presence contingency table (a term either occurs in a review or it doesn't), so its chi² is a direct test of the *(term-present, category)* association. Part 2 feeds **TF-IDF** floats into `ChiSqSelector`. Spark's `ChiSqSelector` treats each distinct feature value as a category in the chi² test, which on continuous TF-IDF values produces a very different statistic – it rewards features whose *value distribution* differs across labels rather than features whose *presence* differs.
2. **Selection scope.** Part 1 keeps the **top 75 per category**, then unions (1464 distinct). Part 2 keeps **the global top 2000** in one shot. Categories with very strong terms (e.g. `Baby`, `Pet_Supplies`) end up under-represented in Part 2 because a few "average" categories with many medium-strength terms can dominate the global ranking.
3. **IDF weighting.** Words that occur in *most* reviews in one category – the strongest doc-presence discriminators (`crib` in Baby, `dewalt` in Tools) – get a *lower* IDF precisely because they are not rare. Their TF-IDF magnitude is therefore moderate, which suppresses them in `ChiSqSelector`'s ranking. Conversely, medium-frequency long-tail words like `amazing` or `account` retain a higher IDF and slip into the global top-2000.

Together these effects favour broadly-distributed, IDF-boosted vocabulary over the sharp per-category labels that the Assignment 1 chi² formulation rewards.

## Take-aways for the report

- `output_ds.txt` is **not** directly comparable to Assignment 1's per-category file; only the joined-dictionary line of `output_rdd.txt` is the right reference.
- Roughly half of the Assignment 1 dictionary survives the Part 2 selection; the other half is replaced by IDF-favoured generic terms.
- The discrepancy is a property of **TF-IDF + multinomial chi²-on-continuous-features**, not a bug in either pipeline. Both implementations are doing exactly what their spec asks.
- If the goal were to *match* Assignment 1 with the DataFrame API, the equivalent setup would be `CountVectorizer(binary=True)` straight into `ChiSqSelector` with per-category top-N selection – but the spec asks for TF-IDF + global top-2000, so the divergence is intentional.
