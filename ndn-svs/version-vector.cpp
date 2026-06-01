/* -*- Mode: C++; c-file-style: "gnu"; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2012-2025 University of California, Los Angeles
 *
 * This file is part of ndn-svs, synchronization library for distributed realtime
 * applications for NDN.
 *
 * ndn-svs library is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, in version 2.1 of the License.
 *
 * ndn-svs library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 */

#include "version-vector.hpp"
#include "tlv.hpp"

#include <algorithm>
#include <map>
#include <random>
#include <string>
#include <vector>

namespace ndn::svs {

namespace {

std::string
extractClusterKey(const NodeID& nid)
{
  std::string text = nid.toUri();
  const auto slashPos = text.find_last_of('/');
  if (slashPos != std::string::npos && slashPos + 1 < text.size())
    text = text.substr(slashPos + 1);

  const auto underscorePos = text.find('_');
  if (underscorePos != std::string::npos && underscorePos > 0)
    return text.substr(0, underscorePos);

  const auto dashPos = text.find('-');
  if (dashPos != std::string::npos && dashPos > 0)
    return text.substr(0, dashPos);

  return text;
}

double
normalizeRankScore(size_t rank, size_t population, bool descending = true)
{
  if (population <= 1)
    return 1.0;

  const double normalized = 1.0 - static_cast<double>(rank) / static_cast<double>(population - 1);
  return descending ? normalized : 1.0 - normalized;
}

} // namespace

VersionVector::VersionVector(const ndn::Block& block)
{
  if (block.type() != tlv::StateVector)
    NDN_THROW(ndn::tlv::Error("StateVector", block.type()));

  block.parse();

  for (auto it = block.elements_begin(); it < block.elements_end(); it++) {
    if (it->type() != tlv::StateVectorEntry)
      NDN_THROW(ndn::tlv::Error("StateVectorEntry", it->type()));

    it->parse();
    NodeID nodeId(it->elements().at(0));
    SeqNo seqNo = ndn::encoding::readNonNegativeInteger(it->elements().at(1));

    m_map[nodeId] = seqNo;
  }
}

ndn::Block
VersionVector::encode() const
{
  ndn::encoding::EncodingBuffer enc;
  size_t totalLength = 0;

  for (auto it = m_map.rbegin(); it != m_map.rend(); it++) {
    // SeqNo
    size_t entryLength = ndn::encoding::prependNonNegativeIntegerBlock(enc, tlv::SeqNo, it->second);

    // NodeID (Name)
    entryLength += ndn::encoding::prependBlock(enc, it->first.wireEncode());

    totalLength += enc.prependVarNumber(entryLength);
    totalLength += enc.prependVarNumber(tlv::StateVectorEntry);
    totalLength += entryLength;
  }

  enc.prependVarNumber(totalLength);
  enc.prependVarNumber(tlv::StateVector);
  return enc.block();
}

VersionVector
VersionVector::selectSubset(size_t maxEntries,
                            size_t recentEntries,
                            size_t startIndex,
                            const NodeID& preferredNode,
                            SubsetStrategy strategy,
                            double hotRatio,
                            size_t minFairEntries,
                            double scoreSeqWeight,
                            double scoreRecentWeight,
                            double scoreFairWeight,
                            double scorePreferredBoost,
                            const std::vector<NodeID>& stickyEntries,
                            size_t stickyBudget,
                            const std::vector<NodeID>& noveltyBaseEntries,
                            size_t noveltyBudget,
                            const std::map<NodeID, size_t>& scoreCompensationRounds) const
{
  VersionVector selected;

  if (maxEntries == 0 || m_map.empty())
    return selected;

  if (strategy == SubsetStrategy::Full || m_map.size() <= maxEntries)
    return *this;

  auto addEntry = [this, &selected](const NodeID& nid) {
    auto it = m_map.find(nid);
    if (it == m_map.end() || selected.m_map.find(nid) != selected.m_map.end())
      return false;

    selected.m_map.emplace(it->first, it->second);

    auto lastUpdate = m_lastUpdate.find(nid);
    selected.m_lastUpdate[nid] =
      lastUpdate == m_lastUpdate.end() ? time::system_clock::time_point::min() : lastUpdate->second;
    return true;
  };

  if (!preferredNode.empty())
    addEntry(preferredNode);

  auto addStickyEntries = [&](size_t budget) {
    if (budget == 0 || stickyEntries.empty())
      return;

    for (const auto& nid : stickyEntries) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(nid))
        --budget;
    }
  };

  std::vector<NodeID> orderedNodes;
  orderedNodes.reserve(m_map.size());
  for (const auto& entry : m_map)
    orderedNodes.push_back(entry.first);

  auto addRecentEntries = [&](size_t budget) {
    if (budget == 0)
      return;

    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    for (const auto& nid : recentNodes) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(nid))
        --budget;
    }
  };

  auto addRoundRobinEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    size_t offset = startIndex % orderedNodes.size();
    for (size_t i = 0; i < orderedNodes.size() && selected.m_map.size() < maxEntries && budget > 0; ++i) {
      if (addEntry(orderedNodes[(offset + i) % orderedNodes.size()]))
        --budget;
    }
  };

  auto addRandomEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto shuffledNodes = orderedNodes;
    std::minstd_rand rng(static_cast<uint32_t>(startIndex));
    std::shuffle(shuffledNodes.begin(), shuffledNodes.end(), rng);

    for (const auto& nid : shuffledNodes) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(nid))
        --budget;
    }
  };

  auto addNoveltyEntries = [&](size_t budget) {
    if (budget == 0)
      return;

    std::set<NodeID> recentBase(noveltyBaseEntries.begin(), noveltyBaseEntries.end());
    std::vector<NodeID> noveltyNodes;
    noveltyNodes.reserve(orderedNodes.size());
    for (const auto& nid : orderedNodes) {
      if (recentBase.find(nid) == recentBase.end())
        noveltyNodes.push_back(nid);
    }

    std::sort(noveltyNodes.begin(), noveltyNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    for (const auto& nid : noveltyNodes) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(nid))
        --budget;
    }
  };

  auto addScoreEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    std::map<NodeID, size_t> recentRank;
    for (size_t i = 0; i < recentNodes.size(); ++i)
      recentRank[recentNodes[i]] = i;

    double seqWeight = std::max(0.0, scoreSeqWeight);
    double recentWeight = std::max(0.0, scoreRecentWeight);
    double fairWeight = std::max(0.0, scoreFairWeight);
    double weightSum = seqWeight + recentWeight + fairWeight;
    if (weightSum <= 0.0) {
      seqWeight = 0.35;
      recentWeight = 0.45;
      fairWeight = 0.20;
      weightSum = 1.0;
    }
    seqWeight /= weightSum;
    recentWeight /= weightSum;
    fairWeight /= weightSum;

    SeqNo maxSeq = 0;
    for (const auto& [nid, seqNo] : m_map)
      maxSeq = std::max(maxSeq, seqNo);

    size_t maxCompensationRounds = 0;
    for (const auto& nid : orderedNodes) {
      auto compensationIt = scoreCompensationRounds.find(nid);
      if (compensationIt != scoreCompensationRounds.end())
        maxCompensationRounds = std::max(maxCompensationRounds, compensationIt->second);
    }
    constexpr double compensationEpsilon = 1e-9;

    struct Candidate
    {
      NodeID nid;
      double score;
      time::system_clock::time_point ts;
    };

    std::vector<Candidate> candidates;
    candidates.reserve(orderedNodes.size());
    for (size_t i = 0; i < orderedNodes.size(); ++i) {
      const auto& nid = orderedNodes[i];
      if (selected.m_map.find(nid) != selected.m_map.end())
        continue;

      const auto seqNo = m_map.at(nid);
      const double seqScore = maxSeq > 0 ? static_cast<double>(seqNo) / static_cast<double>(maxSeq) : 0.0;

      auto rankIt = recentRank.find(nid);
      const size_t rank = rankIt == recentRank.end() ? recentNodes.size() : rankIt->second;
      const double recentScore = recentNodes.size() <= 1
                                   ? 1.0
                                   : 1.0 - static_cast<double>(rank) / static_cast<double>(recentNodes.size() - 1);

      auto compensationIt = scoreCompensationRounds.find(nid);
      const double compensationScore = compensationIt == scoreCompensationRounds.end()
                                         ? 0.0
                                         : static_cast<double>(compensationIt->second) /
                                             (static_cast<double>(maxCompensationRounds) + compensationEpsilon);

      double score = seqWeight * seqScore + recentWeight * recentScore + fairWeight * compensationScore;
      if (!preferredNode.empty() && nid == preferredNode)
        score += std::max(0.0, scorePreferredBoost);

      candidates.push_back({ nid, score, getLastUpdate(nid) });
    }

    std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
      if (lhs.score != rhs.score)
        return lhs.score > rhs.score;
      if (lhs.ts != rhs.ts)
        return lhs.ts > rhs.ts;
      return lhs.nid < rhs.nid;
    });

    for (const auto& candidate : candidates) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(candidate.nid))
        --budget;
    }
  };

  auto addAgeScoreEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto agedNodes = orderedNodes;
    std::sort(agedNodes.begin(), agedNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs < rhsTs;
    });

    std::map<NodeID, size_t> ageRank;
    for (size_t i = 0; i < agedNodes.size(); ++i)
      ageRank[agedNodes[i]] = i;

    SeqNo maxSeq = 0;
    for (const auto& [nid, seqNo] : m_map)
      maxSeq = std::max(maxSeq, seqNo);

    struct Candidate
    {
      NodeID nid;
      double score;
      time::system_clock::time_point ts;
    };

    std::vector<Candidate> candidates;
    candidates.reserve(orderedNodes.size());
    size_t offsetBase = startIndex % orderedNodes.size();
    for (size_t i = 0; i < orderedNodes.size(); ++i) {
      const auto& nid = orderedNodes[i];
      if (selected.m_map.find(nid) != selected.m_map.end())
        continue;

      const auto seqNo = m_map.at(nid);
      const double seqScore = maxSeq > 0 ? static_cast<double>(seqNo) / static_cast<double>(maxSeq) : 0.0;
      const double ageScore = normalizeRankScore(ageRank[nid], agedNodes.size(), true);
      const size_t rrOffset = (i + orderedNodes.size() - offsetBase) % orderedNodes.size();
      const double fairScore = normalizeRankScore(rrOffset, orderedNodes.size(), true);

      double score = 0.25 * seqScore + 0.60 * ageScore + 0.15 * fairScore;
      if (!preferredNode.empty() && nid == preferredNode)
        score += std::max(0.0, scorePreferredBoost);

      candidates.push_back({ nid, score, getLastUpdate(nid) });
    }

    std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
      if (lhs.score != rhs.score)
        return lhs.score > rhs.score;
      if (lhs.ts != rhs.ts)
        return lhs.ts < rhs.ts;
      return lhs.nid < rhs.nid;
    });

    for (const auto& candidate : candidates) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(candidate.nid))
        --budget;
    }
  };

  auto addDeficitScoreEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    std::map<NodeID, size_t> recentRank;
    for (size_t i = 0; i < recentNodes.size(); ++i)
      recentRank[recentNodes[i]] = i;

    SeqNo maxSeq = 0;
    for (const auto& [nid, seqNo] : m_map)
      maxSeq = std::max(maxSeq, seqNo);

    struct Candidate
    {
      NodeID nid;
      double score;
      time::system_clock::time_point ts;
    };

    std::vector<Candidate> candidates;
    candidates.reserve(orderedNodes.size());
    size_t offsetBase = startIndex % orderedNodes.size();
    for (size_t i = 0; i < orderedNodes.size(); ++i) {
      const auto& nid = orderedNodes[i];
      if (selected.m_map.find(nid) != selected.m_map.end())
        continue;

      const auto seqNo = m_map.at(nid);
      const double deficitScore = maxSeq > 0
        ? static_cast<double>(maxSeq - seqNo) / static_cast<double>(maxSeq)
        : 0.0;
      const double recentScore = normalizeRankScore(recentRank[nid], recentNodes.size(), true);
      const size_t rrOffset = (i + orderedNodes.size() - offsetBase) % orderedNodes.size();
      const double fairScore = normalizeRankScore(rrOffset, orderedNodes.size(), true);

      double score = 0.55 * deficitScore + 0.25 * recentScore + 0.20 * fairScore;
      if (!preferredNode.empty() && nid == preferredNode)
        score += std::max(0.0, scorePreferredBoost);

      candidates.push_back({ nid, score, getLastUpdate(nid) });
    }

    std::sort(candidates.begin(), candidates.end(), [](const Candidate& lhs, const Candidate& rhs) {
      if (lhs.score != rhs.score)
        return lhs.score > rhs.score;
      if (lhs.ts != rhs.ts)
        return lhs.ts > rhs.ts;
      return lhs.nid < rhs.nid;
    });

    for (const auto& candidate : candidates) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      if (addEntry(candidate.nid))
        --budget;
    }
  };

  auto addClusterRecentEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    std::map<std::string, bool> seenClusters;
    for (const auto& entry : selected.m_map)
      seenClusters[extractClusterKey(entry.first)] = true;

    for (const auto& nid : recentNodes) {
      if (selected.m_map.size() >= maxEntries || budget == 0)
        break;
      const auto cluster = extractClusterKey(nid);
      if (seenClusters.find(cluster) != seenClusters.end())
        continue;
      if (addEntry(nid)) {
        seenClusters[cluster] = true;
        --budget;
      }
    }

    if (budget > 0)
      addRecentEntries(budget);
  };

  auto addClusterScoreEntries = [&](size_t budget) {
    if (budget == 0 || orderedNodes.empty())
      return;

    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    std::map<NodeID, size_t> recentRank;
    for (size_t i = 0; i < recentNodes.size(); ++i)
      recentRank[recentNodes[i]] = i;

    SeqNo maxSeq = 0;
    for (const auto& [nid, seqNo] : m_map)
      maxSeq = std::max(maxSeq, seqNo);

    size_t offsetBase = startIndex % orderedNodes.size();
    std::map<std::string, size_t> selectedPerCluster;
    for (const auto& entry : selected.m_map)
      ++selectedPerCluster[extractClusterKey(entry.first)];

    while (budget > 0 && selected.m_map.size() < maxEntries) {
      bool added = false;
      NodeID bestNid;
      double bestScore = -1.0;
      time::system_clock::time_point bestTs = time::system_clock::time_point::max();

      for (size_t i = 0; i < orderedNodes.size(); ++i) {
        const auto& nid = orderedNodes[i];
        if (selected.m_map.find(nid) != selected.m_map.end())
          continue;

        const auto seqNo = m_map.at(nid);
        const double seqScore = maxSeq > 0 ? static_cast<double>(seqNo) / static_cast<double>(maxSeq) : 0.0;
        const double recentScore = normalizeRankScore(recentRank[nid], recentNodes.size(), true);
        const size_t rrOffset = (i + orderedNodes.size() - offsetBase) % orderedNodes.size();
        const double fairScore = normalizeRankScore(rrOffset, orderedNodes.size(), true);
        const auto cluster = extractClusterKey(nid);
        const size_t clusterCount = selectedPerCluster[cluster];
        const double clusterBonus = clusterCount == 0 ? 0.30 : std::max(0.0, 0.18 - 0.08 * clusterCount);

        double score = 0.28 * seqScore + 0.42 * recentScore + 0.18 * fairScore + clusterBonus;
        if (!preferredNode.empty() && nid == preferredNode)
          score += std::max(0.0, scorePreferredBoost);

        auto ts = getLastUpdate(nid);
        if (score > bestScore || (score == bestScore && ts > bestTs)) {
          bestScore = score;
          bestTs = ts;
          bestNid = nid;
          added = true;
        }
      }

      if (!added)
        break;
      if (addEntry(bestNid)) {
        ++selectedPerCluster[extractClusterKey(bestNid)];
        --budget;
      }
      else {
        break;
      }
    }
  };

  switch (strategy) {
    case SubsetStrategy::Recent:
      addStickyEntries(std::min(stickyBudget, maxEntries - selected.m_map.size()));
      addRecentEntries(maxEntries);
      break;

    case SubsetStrategy::StickyRecent:
      addStickyEntries(std::min(stickyBudget, maxEntries - selected.m_map.size()));
      addRecentEntries(maxEntries - selected.m_map.size());
      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;

    case SubsetStrategy::RecentNoveltyQuota: {
      const size_t quota = std::min(noveltyBudget, maxEntries - selected.m_map.size());
      const size_t recentBudget = maxEntries > selected.m_map.size() + quota
                                    ? maxEntries - selected.m_map.size() - quota
                                    : 0;
      if (recentBudget > 0)
        addRecentEntries(recentBudget);
      if (quota > 0)
        addNoveltyEntries(std::min(quota, maxEntries - selected.m_map.size()));
      if (selected.m_map.size() < maxEntries)
        addRecentEntries(maxEntries - selected.m_map.size());
      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::RecentRandomQuota: {
      const size_t quota = std::min(noveltyBudget, maxEntries - selected.m_map.size());
      const size_t recentBudget = maxEntries > selected.m_map.size() + quota
                                    ? maxEntries - selected.m_map.size() - quota
                                    : 0;
      if (recentBudget > 0)
        addRecentEntries(recentBudget);
      if (quota > 0)
        addRandomEntries(std::min(quota, maxEntries - selected.m_map.size()));
      if (selected.m_map.size() < maxEntries)
        addRecentEntries(maxEntries - selected.m_map.size());
      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::AdaptiveRecentScore:
      addRecentEntries(maxEntries);
      break;

    case SubsetStrategy::Random: {
      std::minstd_rand rng(static_cast<uint32_t>(startIndex));
      std::shuffle(orderedNodes.begin(), orderedNodes.end(), rng);
      for (const auto& nid : orderedNodes) {
        if (selected.m_map.size() >= maxEntries)
          break;
        addEntry(nid);
      }
      break;
    }

    case SubsetStrategy::RoundRobin:
      addRoundRobinEntries(maxEntries);
      break;

    case SubsetStrategy::Score: {
      addScoreEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::AgeScore: {
      const size_t fairnessReserve = maxEntries >= 4
                                       ? std::min(maxEntries - 1, std::max<size_t>(1, minFairEntries))
                                       : 0;
      const size_t agedBudget = maxEntries > selected.m_map.size() + fairnessReserve
                                  ? maxEntries - selected.m_map.size() - fairnessReserve
                                  : maxEntries - selected.m_map.size();

      if (agedBudget > 0)
        addAgeScoreEntries(agedBudget);

      if (fairnessReserve > 0 && selected.m_map.size() < maxEntries)
        addRoundRobinEntries(std::min(fairnessReserve, maxEntries - selected.m_map.size()));

      if (selected.m_map.size() < maxEntries)
        addAgeScoreEntries(maxEntries - selected.m_map.size());

      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::DeficitScore: {
      const size_t fairnessReserve = maxEntries >= 4
                                       ? std::min(maxEntries - 1, std::max<size_t>(1, minFairEntries))
                                       : 0;
      const size_t deficitBudget = maxEntries > selected.m_map.size() + fairnessReserve
                                     ? maxEntries - selected.m_map.size() - fairnessReserve
                                     : maxEntries - selected.m_map.size();

      if (deficitBudget > 0)
        addDeficitScoreEntries(deficitBudget);

      if (fairnessReserve > 0 && selected.m_map.size() < maxEntries)
        addRoundRobinEntries(std::min(fairnessReserve, maxEntries - selected.m_map.size()));

      if (selected.m_map.size() < maxEntries)
        addDeficitScoreEntries(maxEntries - selected.m_map.size());

      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::ClusterScore: {
      const size_t fairnessReserve = maxEntries >= 4
                                       ? std::min(maxEntries - 1, std::max<size_t>(1, minFairEntries))
                                       : 0;
      const size_t scoredBudget = maxEntries > selected.m_map.size() + fairnessReserve
                                    ? maxEntries - selected.m_map.size() - fairnessReserve
                                    : maxEntries - selected.m_map.size();

      if (scoredBudget > 0)
        addClusterScoreEntries(scoredBudget);

      if (fairnessReserve > 0 && selected.m_map.size() < maxEntries)
        addRoundRobinEntries(std::min(fairnessReserve, maxEntries - selected.m_map.size()));

      if (selected.m_map.size() < maxEntries)
        addClusterScoreEntries(maxEntries - selected.m_map.size());

      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::Hybrid: {
      const double boundedHotRatio = std::clamp(hotRatio, 0.50, 0.95);
      const size_t fairnessReserve = maxEntries >= 4
                                       ? std::min(maxEntries - 1, std::max<size_t>(1, minFairEntries))
                                       : 0;
      size_t hotTarget = static_cast<size_t>(std::ceil(static_cast<double>(maxEntries) * boundedHotRatio));
      if (hotTarget + fairnessReserve > maxEntries)
        hotTarget = maxEntries - fairnessReserve;
      hotTarget = std::max(hotTarget, selected.m_map.size());
      hotTarget = std::max(hotTarget,
                           std::min(maxEntries, selected.m_map.size() + std::max<size_t>(1, recentEntries)));

      if (hotTarget > selected.m_map.size())
        addRecentEntries(hotTarget - selected.m_map.size());

      if (fairnessReserve > 0 && selected.m_map.size() < maxEntries)
        addRoundRobinEntries(std::min(fairnessReserve, maxEntries - selected.m_map.size()));

      if (selected.m_map.size() < maxEntries)
        addRecentEntries(maxEntries - selected.m_map.size());

      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::ClusterHybrid: {
      const size_t fairnessReserve = maxEntries >= 4
                                       ? std::min(maxEntries - 1, std::max<size_t>(1, minFairEntries))
                                       : 0;
      size_t clusterBudget = maxEntries > selected.m_map.size() + fairnessReserve
                               ? maxEntries - selected.m_map.size() - fairnessReserve
                               : maxEntries - selected.m_map.size();
      clusterBudget = std::max(clusterBudget, std::min(maxEntries, recentEntries > 0 ? recentEntries : size_t{1}));

      if (clusterBudget > 0)
        addClusterRecentEntries(clusterBudget);

      if (fairnessReserve > 0 && selected.m_map.size() < maxEntries)
        addRoundRobinEntries(std::min(fairnessReserve, maxEntries - selected.m_map.size()));

      if (selected.m_map.size() < maxEntries)
        addClusterRecentEntries(maxEntries - selected.m_map.size());

      if (selected.m_map.size() < maxEntries)
        addRoundRobinEntries(maxEntries - selected.m_map.size());
      break;
    }

    case SubsetStrategy::Full:
    default:
      addRoundRobinEntries(maxEntries);
      break;
  }

  return selected;
}

std::string
VersionVector::toStr() const
{
  std::ostringstream stream;
  for (const auto& elem : m_map) {
    stream << elem.first << ":" << elem.second << " ";
  }
  return stream.str();
}

} // namespace ndn::svs
