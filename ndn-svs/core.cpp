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

#include "core.hpp"
#include "tlv.hpp"

#include <ndn-cxx/encoding/buffer-stream.hpp>
#include <ndn-cxx/lp/tags.hpp>
#include <ndn-cxx/security/signing-helpers.hpp>
#include <ndn-cxx/security/verification-helpers.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iostream>

#ifdef NDN_SVS_COMPRESSION
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/filter/lzma.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#endif

namespace ndn::svs {
namespace {

std::string
getEnvOrDefault(const char* name, const std::string& defaultValue)
{
  const char* value = std::getenv(name);
  return value == nullptr ? defaultValue : std::string(value);
}

size_t
getEnvSize(const char* name, size_t defaultValue)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
    return defaultValue;

  try {
    return static_cast<size_t>(std::stoul(value));
  }
  catch (const std::exception&) {
    return defaultValue;
  }
}

double
getEnvDouble(const char* name, double defaultValue)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
    return defaultValue;

  try {
    return std::stod(value);
  }
  catch (const std::exception&) {
    return defaultValue;
  }
}

bool
getEnvBool(const char* name, bool defaultValue)
{
  auto value = getEnvOrDefault(name, defaultValue ? "1" : "0");
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });
  return value == "1" || value == "true" || value == "yes" || value == "on";
}

VersionVector::SubsetStrategy
parseSubsetStrategy(const std::string& strategy)
{
  std::string value = strategy;
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });

  if (value == "full" || value == "baseline")
    return VersionVector::SubsetStrategy::Full;
  if (value == "roundrobin" || value == "round-robin" || value == "rotation")
    return VersionVector::SubsetStrategy::RoundRobin;
  if (value == "recent" || value == "latest")
    return VersionVector::SubsetStrategy::Recent;
  if (value == "random")
    return VersionVector::SubsetStrategy::Random;
  if (value == "cluster-hybrid" || value == "clusterhybrid" || value == "clustered-hybrid")
    return VersionVector::SubsetStrategy::ClusterHybrid;
  if (value == "sticky-recent" || value == "stickyrecent" || value == "recent-sticky")
    return VersionVector::SubsetStrategy::StickyRecent;
  if (value == "recent-novelty-quota" || value == "recentnoveltyquota" || value == "novelty-recent")
    return VersionVector::SubsetStrategy::RecentNoveltyQuota;
  if (value == "recent-random-quota" || value == "recentrandomquota" || value == "random-recent")
    return VersionVector::SubsetStrategy::RecentRandomQuota;
  if (value == "adaptive-recent-score" || value == "adaptiverecentscore" || value == "recent-score-adaptive")
    return VersionVector::SubsetStrategy::AdaptiveRecentScore;
  if (value == "cluster-score" || value == "clusterscore" || value == "clustered-score")
    return VersionVector::SubsetStrategy::ClusterScore;
  if (value == "age-score" || value == "agescore" || value == "stale-score")
    return VersionVector::SubsetStrategy::AgeScore;
  if (value == "deficit-score" || value == "deficitscore" || value == "lag-score")
    return VersionVector::SubsetStrategy::DeficitScore;
  if (value == "score" || value == "scored" || value == "score-based")
    return VersionVector::SubsetStrategy::Score;
  return VersionVector::SubsetStrategy::Hybrid;
}

TimerMode
parseTimerMode(const std::string& mode)
{
  std::string value = mode;
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });

  if (value == "fixed" || value == "static")
    return TimerMode::Fixed;
  if (value == "aware" || value == "network-aware" || value == "network" || value == "coord")
    return TimerMode::NetworkAware;
  return TimerMode::Adaptive;
}

const char*
toString(VersionVector::SubsetStrategy strategy)
{
  switch (strategy) {
    case VersionVector::SubsetStrategy::Full:
      return "full";
    case VersionVector::SubsetStrategy::ClusterHybrid:
      return "cluster-hybrid";
    case VersionVector::SubsetStrategy::StickyRecent:
      return "sticky-recent";
    case VersionVector::SubsetStrategy::RecentNoveltyQuota:
      return "recent-novelty-quota";
    case VersionVector::SubsetStrategy::RecentRandomQuota:
      return "recent-random-quota";
    case VersionVector::SubsetStrategy::AdaptiveRecentScore:
      return "adaptive-recent-score";
    case VersionVector::SubsetStrategy::RoundRobin:
      return "round-robin";
    case VersionVector::SubsetStrategy::Recent:
      return "recent";
    case VersionVector::SubsetStrategy::Random:
      return "random";
    case VersionVector::SubsetStrategy::ClusterScore:
      return "cluster-score";
    case VersionVector::SubsetStrategy::AgeScore:
      return "age-score";
    case VersionVector::SubsetStrategy::DeficitScore:
      return "deficit-score";
    case VersionVector::SubsetStrategy::Score:
      return "score";
    case VersionVector::SubsetStrategy::Hybrid:
    default:
      return "hybrid";
  }
}

const char*
toString(TimerMode mode)
{
  switch (mode) {
    case TimerMode::Fixed:
      return "fixed";
    case TimerMode::NetworkAware:
      return "network-aware";
    case TimerMode::Adaptive:
    default:
      return "adaptive";
  }
}

} // namespace

SVSyncCore::SVSyncCore(ndn::Face& face,
                       const Name& syncPrefix,
                       const UpdateCallback& onUpdate,
                       const SecurityOptions& securityOptions,
                       const NodeID& nid)
  : m_face(face)
  , m_syncPrefix(syncPrefix)
  , m_securityOptions(securityOptions)
  , m_id(nid)
  , m_onUpdate(onUpdate)
  , m_maxSuppressionTime(500_ms)
  , m_minPeriodicSyncTime(1_s)
  , m_maxPeriodicSyncTime(30_s)
  , m_periodicSyncTime(time::milliseconds(getEnvSize("NDN_SVS_FIXED_INTERVAL_MS", 30000)))
  , m_periodicSyncJitter(getEnvDouble("NDN_SVS_TIMER_JITTER", 0.1))
  , m_recentStateVectorEntries(getEnvSize("NDN_SVS_RECENT_ENTRIES", 8))
  , m_maxStateVectorEntries(getEnvSize("NDN_SVS_MAX_STATE_VECTOR_ENTRIES", 32))
  , m_stateVectorRatio(getEnvDouble("NDN_SVS_STATE_VECTOR_RATIO", 0.0))
  , m_stateVectorStrategy(parseSubsetStrategy(getEnvOrDefault("NDN_SVS_VECTOR_STRATEGY", "hybrid")))
  , m_hybridHotRatio(getEnvDouble("NDN_SVS_HYBRID_HOT_RATIO", 0.75))
  , m_hybridMinFairEntries(getEnvSize("NDN_SVS_HYBRID_MIN_FAIR_ENTRIES", 2))
  , m_scoreSeqWeight(getEnvDouble("NDN_SVS_SCORE_SEQ_WEIGHT", 0.35))
  , m_scoreRecentWeight(getEnvDouble("NDN_SVS_SCORE_RECENT_WEIGHT", 0.45))
  , m_scoreFairWeight(getEnvDouble("NDN_SVS_SCORE_FAIR_WEIGHT", 0.20))
  , m_scorePreferredBoost(getEnvDouble("NDN_SVS_SCORE_PREFERRED_BOOST", 0.60))
  , m_stickyHoldRatio(getEnvDouble("NDN_SVS_STICKY_HOLD_RATIO", 0.50))
  , m_stickyMinHold(getEnvSize("NDN_SVS_STICKY_MIN_HOLD", 4))
  , m_recentQuotaRatio(getEnvDouble("NDN_SVS_RECENT_QUOTA_RATIO", 0.20))
  , m_recentQuotaMinEntries(getEnvSize("NDN_SVS_RECENT_QUOTA_MIN_ENTRIES", 4))
  , m_adaptiveScoreThreshold(getEnvDouble("NDN_SVS_ADAPTIVE_SCORE_THRESHOLD", 0.50))
  , m_adaptiveScoreRounds(getEnvSize("NDN_SVS_ADAPTIVE_SCORE_ROUNDS", 2))
  , m_networkDiameterMs(getEnvDouble("NDN_SVS_NETWORK_DIAMETER_MS", 120.0))
  , m_networkDiameterHops(getEnvSize("NDN_SVS_NETWORK_DIAMETER_HOPS", 4))
  , m_linkLossRate(std::clamp(getEnvDouble("NDN_SVS_LINK_LOSS_PCT", 0.0) / 100.0, 0.0, 0.95))
  , m_expectedHotspotRatio(std::clamp(getEnvDouble("NDN_SVS_HOT_NODE_RATIO", 0.0), 0.0, 1.0))
  , m_enableCoordinatedSync(getEnvBool("NDN_SVS_COORDINATED_SYNC", true))
  , m_timerMode(parseTimerMode(getEnvOrDefault("NDN_SVS_TIMER_MODE", "network-aware")))
  , m_enableMetricsLog(getEnvBool("NDN_SVS_LOG_METRICS", false))
  , m_stateVectorCursor(0)
  , m_minSendInterval(time::milliseconds(getEnvSize("NDN_SVS_MIN_SEND_INTERVAL_MS", 0)))
  , m_localUpdateDelay(time::milliseconds(getEnvSize("NDN_SVS_LOCAL_UPDATE_DELAY_MS", 1)))
  , m_rng(ndn::random::getRandomNumberEngine())
  , m_intrReplyDist(0, m_maxSuppressionTime.count())
  , m_keyChainMem("pib-memory:", "tpm-memory:")
  , m_scheduler(m_face.getIoContext())
{
  if (m_timerMode == TimerMode::NetworkAware) {
    const double diameterMs = std::max(20.0, m_networkDiameterMs);
    auto suppressionMs = static_cast<long>(std::llround(diameterMs * (1.2 + 1.6 * m_linkLossRate)
                                                        + static_cast<double>(m_networkDiameterHops) * 8.0));
    suppressionMs = std::clamp<long>(suppressionMs, 40, 1200);
    m_maxSuppressionTime = time::milliseconds(suppressionMs);
    m_intrReplyDist = std::uniform_int_distribution<>(0, m_maxSuppressionTime.count());

    auto awareMin = static_cast<long>(std::llround(diameterMs * (3.5 + 2.5 * m_linkLossRate)
                                                   + static_cast<double>(m_networkDiameterHops) * 30.0));
    awareMin = std::clamp<long>(awareMin, 300, 5000);
    m_minPeriodicSyncTime = time::milliseconds(awareMin);
    auto initialPeriod = std::clamp<long>(static_cast<long>(std::llround(awareMin * (2.0 + 0.8 * m_linkLossRate))),
                                          awareMin,
                                          m_maxPeriodicSyncTime.count());
    m_periodicSyncTime = time::milliseconds(std::min<long>(m_periodicSyncTime.count(), initialPeriod));
  }

  if (m_periodicSyncTime < m_minPeriodicSyncTime)
    m_periodicSyncTime = m_minPeriodicSyncTime;
  if (m_periodicSyncTime > m_maxPeriodicSyncTime)
    m_periodicSyncTime = m_maxPeriodicSyncTime;

  if (m_enableMetricsLog) {
    std::cout << "SVS_CONFIG node=" << m_id
              << " strategy=" << toString(m_stateVectorStrategy)
              << " timer=" << toString(m_timerMode)
              << " ratio=" << m_stateVectorRatio
              << " max_entries=" << m_maxStateVectorEntries
              << " hybrid_hot_ratio=" << m_hybridHotRatio
              << " hybrid_min_fair=" << m_hybridMinFairEntries
              << " score_seq_weight=" << m_scoreSeqWeight
              << " score_recent_weight=" << m_scoreRecentWeight
              << " score_fair_weight=" << m_scoreFairWeight
              << " score_preferred_boost=" << m_scorePreferredBoost
              << " sticky_hold_ratio=" << m_stickyHoldRatio
              << " sticky_min_hold=" << m_stickyMinHold
              << " recent_quota_ratio=" << m_recentQuotaRatio
              << " recent_quota_min_entries=" << m_recentQuotaMinEntries
              << " adaptive_score_threshold=" << m_adaptiveScoreThreshold
              << " adaptive_score_rounds=" << m_adaptiveScoreRounds
              << " diameter_ms=" << m_networkDiameterMs
              << " diameter_hops=" << m_networkDiameterHops
              << " loss_rate=" << m_linkLossRate
              << " hot_ratio=" << m_expectedHotspotRatio
              << " coordinated=" << (m_enableCoordinatedSync ? 1 : 0)
              << " min_gap_ms=" << m_minSendInterval.count()
              << " local_batch_ms=" << m_localUpdateDelay.count()
              << std::endl;
  }

  // Register sync interest filter
  m_syncRegisteredPrefix =
    m_face.setInterestFilter(syncPrefix,
                             std::bind(&SVSyncCore::onSyncInterest, this, _2),
                             std::bind(&SVSyncCore::sendInitialInterest, this),
                             [](auto&&...) { NDN_THROW(Error("Failed to register sync prefix")); });
}

static inline int
suppressionCurve(int constFactor, int value)
{
  // This curve increases the probability that only one or a few
  // nodes pick lower values for timers compared to other nodes.
  // This leads to better suppression results.
  // Increasing the curve factor makes the curve steeper =>
  // better for more nodes, but worse for fewer nodes.

  float c = constFactor;
  float v = value;
  float f = 10.0; // curve factor

  return static_cast<int>(c * (1.0 - std::exp((v - c) / (c / f))));
}

void
SVSyncCore::sendInitialInterest()
{
  // Wait for 100ms before sending the first sync interest
  // This is necessary to give other things time to initialize
  m_scheduler.schedule(100_ms, [this] {
    m_initialized = true;
    retxSyncInterest(true, 0);
  });
}

size_t
SVSyncCore::getStateVectorLimit(size_t totalEntries) const
{
  if (totalEntries == 0)
    return 0;

  if (m_stateVectorStrategy == VersionVector::SubsetStrategy::Full)
    return totalEntries;

  size_t scaled = 0;
  if (m_stateVectorRatio > 0.0) {
    scaled = static_cast<size_t>(std::ceil(static_cast<double>(totalEntries) * m_stateVectorRatio));
  }
  else {
    scaled = static_cast<size_t>(std::ceil(2.0 * std::sqrt(static_cast<double>(totalEntries))));
    scaled = std::max(m_recentStateVectorEntries, scaled);
  }

  scaled = std::max<size_t>(1, scaled);

  if (m_enableCoordinatedSync) {
    const double topologyPressure = std::min(1.0, m_networkDiameterMs / 250.0);
    const double hotness = std::clamp(0.6 * m_hotspotScore + 0.4 * m_expectedHotspotRatio, 0.0, 1.0);
    const double pressure = std::clamp(0.55 * m_linkLossRate + 0.25 * topologyPressure + 0.35 * hotness,
                                       0.0,
                                       1.0);
    const size_t floorEntries = std::min(totalEntries,
                                         std::max<size_t>(m_recentStateVectorEntries + m_hybridMinFairEntries,
                                                          std::min<size_t>(m_maxStateVectorEntries, 8)));
    scaled = std::max(scaled, floorEntries);
    scaled = static_cast<size_t>(std::ceil(static_cast<double>(scaled) * (1.0 + 0.22 * pressure)));
  }

  scaled = std::min(m_maxStateVectorEntries, scaled);
  return std::min(totalEntries, scaled);
}

VersionVector
SVSyncCore::buildSyncVector()
{
  std::lock_guard<std::mutex> lock(m_vvMutex);

  const size_t totalEntries = m_vv.size();
  const size_t limit = getStateVectorLimit(totalEntries);
  if (limit >= totalEntries || m_stateVectorStrategy == VersionVector::SubsetStrategy::Full)
    return m_vv;

  size_t selector = m_stateVectorCursor;
  if (m_stateVectorStrategy == VersionVector::SubsetStrategy::Random)
    selector = static_cast<size_t>(m_rng());

  VersionVector::SubsetStrategy effectiveStrategy = m_stateVectorStrategy;

  double effectiveHotRatio = m_hybridHotRatio;
  size_t effectiveFairEntries = m_hybridMinFairEntries;
  double effectiveScoreSeqWeight = m_scoreSeqWeight;
  double effectiveScoreRecentWeight = m_scoreRecentWeight;
  double effectiveScoreFairWeight = m_scoreFairWeight;
  double effectiveScorePreferredBoost = m_scorePreferredBoost;

  const double coveragePressure = totalEntries > 0
                                    ? 1.0 - std::min(1.0, static_cast<double>(limit) / static_cast<double>(totalEntries))
                                    : 0.0;
  const double recencyStress = 1.0 - std::clamp(m_hotspotScore, 0.0, 1.0);
  const double repairPressure = std::clamp(0.55 * coveragePressure + 0.25 * m_linkLossRate + 0.20 * recencyStress,
                                           0.0,
                                           1.0);

  if (m_stateVectorStrategy == VersionVector::SubsetStrategy::AdaptiveRecentScore) {
    if (m_adaptiveScoreBurstRemaining > 0) {
      effectiveStrategy = VersionVector::SubsetStrategy::Score;
      --m_adaptiveScoreBurstRemaining;
    }
    else if (repairPressure >= std::clamp(m_adaptiveScoreThreshold, 0.0, 1.0)) {
      effectiveStrategy = VersionVector::SubsetStrategy::Score;
      m_adaptiveScoreBurstRemaining = m_adaptiveScoreRounds > 0 ? m_adaptiveScoreRounds - 1 : 0;
    }
    else {
      effectiveStrategy = VersionVector::SubsetStrategy::Recent;
    }
  }

  if (m_enableCoordinatedSync) {
    const double topologyPressure = std::min(1.0, m_networkDiameterMs / 250.0);
    const double hotness = std::clamp(0.6 * m_hotspotScore + 0.25 * m_activityScore + 0.15 * m_expectedHotspotRatio,
                                      0.0,
                                      1.0);

    if (effectiveStrategy == VersionVector::SubsetStrategy::Hybrid ||
      effectiveStrategy == VersionVector::SubsetStrategy::ClusterHybrid) {
      effectiveHotRatio = std::clamp(m_hybridHotRatio + 0.12 * hotness + 0.08 * m_linkLossRate - 0.03 * topologyPressure,
                                     0.55,
                                     0.92);
      if (hotness < 0.35) {
        effectiveFairEntries = std::min<size_t>(std::max<size_t>(1, m_hybridMinFairEntries + 1),
                                                std::max<size_t>(1, limit / 3));
      }
    }
    else if (effectiveStrategy == VersionVector::SubsetStrategy::Score ||
         effectiveStrategy == VersionVector::SubsetStrategy::ClusterScore ||
         effectiveStrategy == VersionVector::SubsetStrategy::AgeScore ||
         effectiveStrategy == VersionVector::SubsetStrategy::DeficitScore) {
      effectiveScoreSeqWeight = std::clamp(m_scoreSeqWeight + 0.10 * hotness, 0.10, 0.70);
      effectiveScoreRecentWeight = std::clamp(m_scoreRecentWeight + 0.08 * m_linkLossRate + 0.05 * topologyPressure,
                                              0.15,
                                              0.75);
      effectiveScoreFairWeight = std::clamp(m_scoreFairWeight + (hotness < 0.35 ? 0.10 : 0.03), 0.10, 0.50);
      effectiveScorePreferredBoost = std::clamp(m_scorePreferredBoost + 0.15 * hotness, 0.20, 1.20);
      if (hotness < 0.35) {
        effectiveFairEntries = std::min<size_t>(std::max<size_t>(1, m_hybridMinFairEntries + 1),
                                                std::max<size_t>(1, limit / 3));
      }
    }
  }

  std::vector<NodeID> stickyNodes;
  size_t stickyBudget = 0;
  if (m_stateVectorStrategy == VersionVector::SubsetStrategy::StickyRecent && !m_lastSelectedNodes.empty() && limit > 1) {
    stickyNodes = m_lastSelectedNodes;
    stickyBudget = std::min(limit - 1,
                            std::max(std::min(m_stickyMinHold, limit - 1),
                                     static_cast<size_t>(std::ceil(static_cast<double>(limit) * std::clamp(m_stickyHoldRatio,
                                                                                                              0.0,
                                                                                                              1.0)))));
  }

  std::vector<NodeID> noveltyBaseNodes;
  size_t noveltyBudget = 0;
  if ((m_stateVectorStrategy == VersionVector::SubsetStrategy::RecentNoveltyQuota ||
       m_stateVectorStrategy == VersionVector::SubsetStrategy::RecentRandomQuota) &&
      limit > 1) {
    noveltyBaseNodes = m_lastSelectedNodes;
    noveltyBudget = std::min(limit - 1,
                             std::max(std::min(m_recentQuotaMinEntries, limit - 1),
                                      static_cast<size_t>(std::ceil(static_cast<double>(limit) * std::clamp(m_recentQuotaRatio,
                                                                                                               0.0,
                                                                                                               1.0)))));
  }

  VersionVector partial = m_vv.selectSubset(limit,
                                            m_recentStateVectorEntries,
                                            selector,
                                            m_id,
                                            effectiveStrategy,
                                            effectiveHotRatio,
                                            effectiveFairEntries,
                                            effectiveScoreSeqWeight,
                                            effectiveScoreRecentWeight,
                                            effectiveScoreFairWeight,
                                            effectiveScorePreferredBoost,
                                            stickyNodes,
                                            stickyBudget,
                                            noveltyBaseNodes,
                                            noveltyBudget);

  if (totalEntries > 0 && effectiveStrategy != VersionVector::SubsetStrategy::Random)
    m_stateVectorCursor = (m_stateVectorCursor + limit) % totalEntries;

  m_lastSelectedNodes.clear();
  std::vector<std::pair<NodeID, time::system_clock::time_point>> selectedNodes;
  selectedNodes.reserve(partial.size());
  for (const auto& [nid, seqNo] : partial) {
    selectedNodes.push_back({ nid, partial.getLastUpdate(nid) });
  }
  std::sort(selectedNodes.begin(), selectedNodes.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.second != rhs.second)
      return lhs.second > rhs.second;
    return lhs.first < rhs.first;
  });
  for (const auto& [nid, ts] : selectedNodes)
    m_lastSelectedNodes.push_back(nid);

  if (m_enableMetricsLog && effectiveStrategy != m_stateVectorStrategy) {
    std::cout << "SVS_STRATEGY_SWITCH node=" << m_id
              << " configured=" << toString(m_stateVectorStrategy)
              << " effective=" << toString(effectiveStrategy)
              << " repair_pressure=" << repairPressure
              << " coverage_pressure=" << coveragePressure
              << std::endl;
  }

  return partial;
}

void
SVSyncCore::updateSyncInterval(SyncSignal signal)
{
  if (m_timerMode == TimerMode::Fixed)
    return;

  const long nowUs = getCurrentTime();
  const long current = static_cast<long>(m_periodicSyncTime.count());
  long floorMs = static_cast<long>(m_minPeriodicSyncTime.count());

  {
    std::lock_guard<std::mutex> lock(m_vvMutex);
    const auto totalEntries = static_cast<long>(m_vv.size());
    if (m_stateVectorStrategy == VersionVector::SubsetStrategy::Full) {
      floorMs = std::max<long>(floorMs, 1400 + totalEntries * 18);
    }
    else if (totalEntries > 0) {
      floorMs = std::max<long>(floorMs, 650 + std::min<long>(totalEntries, 64) * 9);
    }
  }

  if (m_lastTimerAdjustUs != 0 && (nowUs - m_lastTimerAdjustUs) < 220000) {
    if (signal == SyncSignal::RepairNeeded && current > floorMs) {
      long nudged = floorMs + std::max<long>(0, (current - floorMs) / 2);
      m_periodicSyncTime = time::milliseconds(nudged);
    }
    return;
  }
  m_lastTimerAdjustUs = nowUs;

  if (m_timerMode == TimerMode::Adaptive) {
    long next = current;
    switch (signal) {
      case SyncSignal::LocalUpdate:
        m_activityScore = std::min(1.0, m_activityScore * 0.85 + 0.18);
        next = std::max<long>(floorMs, current - std::max<long>(200, current / 8));
        break;

      case SyncSignal::RepairNeeded:
        m_activityScore = std::min(1.0, m_activityScore * 0.75 + 0.30);
        next = std::max<long>(floorMs, current - std::max<long>(350, current / 4));
        break;

      case SyncSignal::RemoteUpdate:
        m_activityScore = std::min(1.0, m_activityScore * 0.80 + 0.08);
        next = std::min<long>(m_maxPeriodicSyncTime.count(), current + std::max<long>(100, current / 16));
        break;

      case SyncSignal::Idle:
      default:
        m_activityScore *= 0.65;
        next = std::min<long>(m_maxPeriodicSyncTime.count(), current + std::max<long>(400, current / 5));
        break;
    }

    const long weightedFloor = floorMs + static_cast<long>(std::llround(m_activityScore * 250.0));
    next = std::max<long>(weightedFloor, next);
    next = std::min<long>(m_maxPeriodicSyncTime.count(), std::max<long>(m_minPeriodicSyncTime.count(), next));
    m_periodicSyncTime = time::milliseconds(next);
    return;
  }

  const double topologyPressure = std::min(1.0, m_networkDiameterMs / 250.0);
  const double coordinationGain = (m_enableCoordinatedSync && m_stateVectorStrategy != VersionVector::SubsetStrategy::Full)
                                    ? 0.88
                                    : 1.0;
  const long networkFloor = std::max<long>(
    floorMs,
    static_cast<long>(std::llround((m_networkDiameterMs * (3.0 + 2.2 * m_linkLossRate)
                                    + static_cast<double>(m_networkDiameterHops) * 24.0
                                    + 180.0)
                                   * coordinationGain)));

  long next = current;
  switch (signal) {
    case SyncSignal::LocalUpdate:
      m_activityScore = std::min(1.0, m_activityScore * 0.72 + 0.25);
      m_hotspotScore = std::min(1.0, m_hotspotScore * 0.65 + 0.35);
      {
        const double hotnessNow = std::clamp(0.55 * m_hotspotScore + 0.25 * m_activityScore + 0.20 * m_expectedHotspotRatio,
                                             0.0,
                                             1.0);
        const long divisor = (m_enableCoordinatedSync && m_stateVectorStrategy != VersionVector::SubsetStrategy::Full)
                               ? (hotnessNow > 0.72 ? 4 : 5)
                               : (hotnessNow > 0.72 ? 6 : 7);
        next = std::max<long>(networkFloor,
                              current - std::max<long>(120, current / divisor));
      }
      break;

    case SyncSignal::RepairNeeded:
      m_activityScore = std::min(1.0, m_activityScore * 0.75 + 0.22);
      m_hotspotScore = std::min(1.0, m_hotspotScore * 0.82 + 0.12);
      next = std::max<long>(networkFloor, current - std::max<long>(220, current / 4));
      break;

    case SyncSignal::RemoteUpdate:
      m_activityScore = std::min(1.0, m_activityScore * 0.80 + 0.06);
      m_hotspotScore = std::min(1.0, m_hotspotScore * 0.90 + 0.03);
      next = std::min<long>(m_maxPeriodicSyncTime.count(), current + std::max<long>(130, current / 10));
      break;

    case SyncSignal::Idle:
    default:
      m_activityScore *= 0.60;
      m_hotspotScore *= 0.55;
      next = std::min<long>(m_maxPeriodicSyncTime.count(), current + std::max<long>(420, current / 3));
      break;
  }

  const double hotness = std::clamp(0.55 * m_hotspotScore + 0.25 * m_activityScore + 0.20 * m_expectedHotspotRatio,
                                    0.0,
                                    1.0);
  const long hotFloor = static_cast<long>(std::llround(networkFloor * (1.0 - 0.22 * hotness * (1.05 - coordinationGain))));
  const long lossBuffer = static_cast<long>(std::llround(120.0 * m_linkLossRate + 80.0 * topologyPressure));

  next = std::max<long>(std::max<long>(m_minPeriodicSyncTime.count(), hotFloor), next);
  next = std::min<long>(m_maxPeriodicSyncTime.count(), next + lossBuffer / 4);

  m_periodicSyncTime = time::milliseconds(next);
}

unsigned int
SVSyncCore::sampleSyncDelay()
{
  auto base = std::max<long>(1, m_periodicSyncTime.count());
  double extraSpread = 0.0;
  if (m_timerMode == TimerMode::NetworkAware) {
    extraSpread = 0.10 * m_linkLossRate + 0.08 * m_hotspotScore + 0.05 * std::min(1.0, m_networkDiameterMs / 250.0);
  }

  auto low = std::max<long>(1, static_cast<long>(std::llround(base * (1.0 - m_periodicSyncJitter))));
  auto high = std::max<long>(low,
                             static_cast<long>(std::llround(base * (1.0 + m_periodicSyncJitter + extraSpread))));

  std::uniform_int_distribution<long> dist(low, high);
  return static_cast<unsigned int>(dist(m_rng));
}

void
SVSyncCore::onSyncInterest(const Interest& interest)
{
  switch (m_securityOptions.interestSigner->signingInfo.getSignerType()) {
    case security::SigningInfo::SIGNER_TYPE_NULL:
      onSyncInterestValidated(interest);
      return;

    case security::SigningInfo::SIGNER_TYPE_HMAC:
      if (security::verifySignature(interest,
                                    m_keyChainMem.getTpm(),
                                    m_securityOptions.interestSigner->signingInfo.getSignerName(),
                                    DigestAlgorithm::SHA256))
        onSyncInterestValidated(interest);
      return;

    default:
      if (m_securityOptions.validator)
        m_securityOptions.validator->validate(
          interest, std::bind(&SVSyncCore::onSyncInterestValidated, this, _1), nullptr);
      else
        onSyncInterestValidated(interest);
      return;
  }
}

void
SVSyncCore::onSyncInterestValidated(const Interest& interest)
{
  // Get incoming face (this is needed by NLSR)
  uint64_t incomingFace = 0;
  {
    auto tag = interest.getTag<ndn::lp::IncomingFaceIdTag>();
    if (tag) {
      incomingFace = tag->get();
    }
  }

  // Check for invalid Interest
  if (!interest.hasApplicationParameters()) {
    return;
  }

  // Decode state parameters
  ndn::Block params = interest.getApplicationParameters();
  params.parse();

#ifdef NDN_SVS_COMPRESSION
  // Decompress if necessary. The spec requires that if an LZMA block is
  // present, then no other blocks are present (everything is compressed
  // together)
  if (params.find(tlv::LzmaBlock) != params.elements_end()) {
    auto lzmaBlock = params.get(tlv::LzmaBlock);

    boost::iostreams::filtering_istreambuf in;
    in.push(boost::iostreams::lzma_decompressor());
    in.push(boost::iostreams::array_source(reinterpret_cast<const char*>(lzmaBlock.value()),
                                           lzmaBlock.value_size()));
    ndn::OBufferStream decompressed;
    boost::iostreams::copy(in, decompressed);

    auto parsed = ndn::Block::fromBuffer(decompressed.buf());
    if (!std::get<0>(parsed)) {
      // TODO: log error parsing inner block
      return;
    }

    params = std::get<1>(parsed);
    params.parse();
  }
#endif

  // Get state vector
  std::shared_ptr<VersionVector> vvOther;
  try {
    vvOther = std::make_shared<VersionVector>(params.get(tlv::StateVector));
  } catch (ndn::tlv::Error&) {
    // TODO: log error
    return;
  }

  // Read extra mapping blocks
  if (m_recvExtraBlock) {
    try {
      m_recvExtraBlock(params.get(tlv::MappingData), *vvOther);
    } catch (std::exception&) {
      // TODO: log error but continue
    }
  }

  // Merge state vector
  auto result = mergeStateVector(*vvOther);

  SyncSignal signal = SyncSignal::Idle;
  if (!result.missingInfo.empty())
    signal = SyncSignal::RepairNeeded;
  else if (result.myVectorNew || result.otherVectorNew)
    signal = SyncSignal::RemoteUpdate;
  updateSyncInterval(signal);

  // Callback if missing data found
  if (!result.missingInfo.empty()) {
    for (auto& e : result.missingInfo)
      e.incomingFace = incomingFace;
    m_onUpdate(result.missingInfo);
  }

  // Try to record; the call will check if in suppression state
  if (recordVector(*vvOther))
    return;

  // If incoming state identical/newer to local vector, reset timer
  // If incoming state is older, send sync interest immediately
  if (!result.myVectorNew) {
    retxSyncInterest(false, 0);
  } else {
    enterSuppressionState(*vvOther);
    // Check how much time is left on the timer,
    // reset to ~m_intrReplyDist if more than that.
    int delay = m_intrReplyDist(m_rng);

    // Curve the delay for better suppression in large groups
    // TODO: efficient curve depends on number of active nodes
    delay = suppressionCurve(m_maxSuppressionTime.count(), delay);

    if (getCurrentTime() + delay * 1000 < m_nextSyncInterest) {
      retxSyncInterest(false, delay);
    }
  }
}

void
SVSyncCore::retxSyncInterest(bool send, unsigned int delay)
{
  if (send) {
    long minGapUs = m_minSendInterval.count() * 1000;
    const double hotness = std::clamp(0.55 * m_hotspotScore + 0.25 * m_activityScore + 0.20 * m_expectedHotspotRatio,
                                      0.0,
                                      1.0);
    if (minGapUs > 0 && m_enableCoordinatedSync && hotness > 0.72)
      minGapUs = std::max<long>(8000, minGapUs / 2);

    const long lastTxUs = m_lastSyncTxUs.load();
    const long nowUs = getCurrentTime();
    if (minGapUs > 0 && lastTxUs > 0 && nowUs < lastTxUs + minGapUs) {
      delay = static_cast<unsigned int>(std::max<long>(1, (lastTxUs + minGapUs - nowUs + 999) / 1000));
      send = false;
    }
    else {
      std::lock_guard<std::mutex> lock(m_recordedVvMutex);

      // Only send interest if in steady state or local vector has newer state
      // than recorded interests
      if (!m_recordedVv || mergeStateVector(*m_recordedVv).myVectorNew)
        sendSyncInterest();
      m_recordedVv = nullptr;
    }
  }

  if (delay == 0)
    delay = sampleSyncDelay();

  {
    std::lock_guard<std::mutex> lock(m_schedulerMutex);

    // Store the scheduled time
    m_nextSyncInterest = getCurrentTime() + 1000 * delay;

    m_retxEvent = m_scheduler.schedule(time::milliseconds(delay), [this] { retxSyncInterest(true, 0); });
  }
}

void
SVSyncCore::sendSyncInterest()
{
  if (!m_initialized)
    return;

  // Build app parameters
  ndn::encoding::EncodingBuffer enc;
  VersionVector syncVv = buildSyncVector();
  {
    size_t length = 0;

    // Add extra mapping blocks
    if (m_getExtraBlock)
      length += ndn::encoding::prependBlock(enc, m_getExtraBlock(syncVv));

    // Add a compact state vector slice
    length += ndn::encoding::prependBlock(enc, syncVv.encode());

    // Add length and ApplicationParameters type
    enc.prependVarNumber(length);
    enc.prependVarNumber(ndn::tlv::ApplicationParameters);
  }

  ndn::Block wire = enc.block();
  wire.encode();

#ifdef NDN_SVS_COMPRESSION
  boost::iostreams::filtering_istreambuf in;
  in.push(boost::iostreams::lzma_compressor());
  in.push(boost::iostreams::array_source(reinterpret_cast<const char*>(wire.data()), wire.size()));
  ndn::OBufferStream compressed;
  boost::iostreams::copy(in, compressed);
  wire = ndn::Block(tlv::LzmaBlock, compressed.buf());
  wire.encode();
#endif

  if (m_enableMetricsLog) {
    std::cout << "SVS_TX_METRIC ts=" << getCurrentTime()
              << " node=" << m_id
              << " strategy=" << toString(m_stateVectorStrategy)
              << " timer=" << toString(m_timerMode)
              << " period_ms=" << m_periodicSyncTime.count()
              << " hot_score=" << (0.55 * m_hotspotScore + 0.25 * m_activityScore + 0.20 * m_expectedHotspotRatio)
              << " entries=" << syncVv.size()
              << " bytes=" << wire.size()
              << std::endl;
  }

  // Create Sync Interest
  Interest interest(Name(m_syncPrefix).appendVersion(2));
  interest.setApplicationParameters(wire);
  interest.setInterestLifetime(1_ms);

  switch (m_securityOptions.interestSigner->signingInfo.getSignerType()) {
    case security::SigningInfo::SIGNER_TYPE_NULL:
      break;

    case security::SigningInfo::SIGNER_TYPE_HMAC:
      m_keyChainMem.sign(interest, m_securityOptions.interestSigner->signingInfo);
      break;

    default:
      m_securityOptions.interestSigner->sign(interest);
      break;
  }

  m_lastSyncTxUs = getCurrentTime();
  m_face.expressInterest(interest, nullptr, nullptr, nullptr);
}

SVSyncCore::MergeResult
SVSyncCore::mergeStateVector(const VersionVector& vvOther)
{
  std::lock_guard<std::mutex> lock(m_vvMutex);
  SVSyncCore::MergeResult result;

  // Check if other vector has newer state
  for (const auto& entry : vvOther) {
    NodeID nidOther = entry.first;
    SeqNo seqOther = entry.second;
    SeqNo seqCurrent = m_vv.get(nidOther);

    if (seqCurrent < seqOther) {
      result.otherVectorNew = true;

      SeqNo startSeq = m_vv.get(nidOther) + 1;
      result.missingInfo.push_back({ nidOther, startSeq, seqOther, 0 });

      m_vv.set(nidOther, seqOther);
    }
  }

  // Check if I have newer state
  for (const auto& entry : m_vv) {
    NodeID nid = entry.first;
    SeqNo seq = entry.second;
    SeqNo seqOther = vvOther.get(nid);

    // Ignore this node if it was last updated within network RTT
    if (time::system_clock::now() - m_vv.getLastUpdate(nid) < m_maxSuppressionTime)
      continue;

    if (seqOther < seq) {
      result.myVectorNew = true;
      break;
    }
  }

  return result;
}

void
SVSyncCore::reset(bool isOnInterest)
{
}

SeqNo
SVSyncCore::getSeqNo(const NodeID& nid) const
{
  std::lock_guard<std::mutex> lock(m_vvMutex);
  NodeID t_nid = (nid == EMPTY_NODE_ID) ? m_id : nid;
  return m_vv.get(t_nid);
}

void
SVSyncCore::updateSeqNo(const SeqNo& seq, const NodeID& nid)
{
  NodeID t_nid = (nid == EMPTY_NODE_ID) ? m_id : nid;

  SeqNo prev;
  {
    std::lock_guard<std::mutex> lock(m_vvMutex);
    prev = m_vv.get(t_nid);
    m_vv.set(t_nid, seq);
  }

  if (seq > prev) {
    updateSyncInterval(SyncSignal::LocalUpdate);
    long localDelay = std::max<long>(1, m_localUpdateDelay.count());
    const double hotness = std::clamp(0.55 * m_hotspotScore + 0.25 * m_activityScore + 0.20 * m_expectedHotspotRatio,
                                      0.0,
                                      1.0);
    if (m_enableCoordinatedSync && hotness > 0.72)
      localDelay = std::max<long>(1, localDelay / 2);
    retxSyncInterest(false, static_cast<unsigned int>(localDelay));
  }
}

std::set<NodeID>
SVSyncCore::getNodeIds() const
{
  std::lock_guard<std::mutex> lock(m_vvMutex);
  std::set<NodeID> sessionNames;
  for (const auto& nid : m_vv) {
    sessionNames.insert(nid.first);
  }
  return sessionNames;
}

long
SVSyncCore::getCurrentTime() const
{
  return std::chrono::duration_cast<std::chrono::microseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

bool
SVSyncCore::recordVector(const VersionVector& vvOther)
{
  std::lock_guard<std::mutex> lock(m_recordedVvMutex);

  if (!m_recordedVv)
    return false;

  std::lock_guard<std::mutex> lock1(m_vvMutex);

  for (const auto& entry : vvOther) {
    NodeID nidOther = entry.first;
    SeqNo seqOther = entry.second;
    SeqNo seqCurrent = m_recordedVv->get(nidOther);

    if (seqCurrent < seqOther) {
      m_recordedVv->set(nidOther, seqOther);
    }
  }

  return true;
}

void
SVSyncCore::enterSuppressionState(const VersionVector& vvOther)
{
  std::lock_guard<std::mutex> lock(m_recordedVvMutex);

  if (!m_recordedVv)
    m_recordedVv = std::make_unique<VersionVector>(vvOther);
}

} // namespace ndn::svs
