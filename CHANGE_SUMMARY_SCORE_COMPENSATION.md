# Score Compensation Change Summary

This file records the code changes made during the current conversation for the `m-svs` workspace.

## Modified files

- `ndn-svs/version-vector.hpp`
- `ndn-svs/version-vector.cpp`
- `ndn-svs/core.hpp`
- `ndn-svs/core.cpp`
- `tests/unit-tests/version-vector.t.cpp`

## Summary

- Added explicit unselected-round compensation to `Score` selection.
- Added `scoreCompensationRounds` argument to `VersionVector::selectSubset` and normalized it in the score computation.
- Added `SVSyncCore::m_scoreUnselectedRounds` to track consecutive non-selected rounds per `NodeID`.
- Cleared or initialized `m_scoreUnselectedRounds` when returning a full vector.
- Updated `buildSyncVector()` to forward `m_scoreUnselectedRounds` into `selectSubset` and refresh the counts.
- Replaced the old `fairScore`/round-robin compensation path in `Score` strategy with explicit normalized compensation from `scoreCompensationRounds`.
- Added a unit test to verify starved entries receive higher selection priority via compensation.

## Saved diff

The exact diff is saved in:

- `/home/alice/m-svs/CHANGE_SUMMARY_SCORE_COMPENSATION.diff`

## Notes

The `Score` total score is computed as:

```cpp
score = seqWeight * seqScore
      + recentWeight * recentScore
      + fairWeight * compensationScore;
```

where:
- `seqScore = seqNo / maxSeq`
- `recentScore = 1.0 - rank / (recentNodes.size() - 1)`
- `compensationScore = a_i / (maxCompensationRounds + 1e-9)`
- `a_i` is the node's unselected-round count from `scoreCompensationRounds`
- weights are normalized to sum to 1

