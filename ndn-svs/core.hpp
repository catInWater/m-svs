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

#ifndef NDN_SVS_CORE_HPP
#define NDN_SVS_CORE_HPP

#include "common.hpp"
#include "security-options.hpp"
#include "version-vector.hpp"

#include <ndn-cxx/util/random.hpp>
#include <ndn-cxx/util/scheduler.hpp>

#include <atomic>
#include <mutex>

namespace ndn::svs {

class MissingDataInfo
{
public:
  /// @brief session name
  NodeID nodeId;
  /// @brief the lowest one of missing sequence numbers
  SeqNo low;
  /// @brief the highest one of missing sequence numbers
  SeqNo high;
  /// @brief ndn::lp::IncomingFaceIdTag
  uint64_t incomingFace;
};

/**
 * @brief The callback function to handle state updates
 *
 * The parameter is a set of MissingDataInfo, of which each corresponds to
 * a session that has changed its state.
 */
using UpdateCallback = std::function<void(const std::vector<MissingDataInfo>&)>;

enum class TimerMode
{
  Fixed,
  Adaptive,
  NetworkAware,
};

enum class SyncSignal
{
  Idle,
  RemoteUpdate,
  RepairNeeded,
  LocalUpdate,
};

/**
 * @brief Pure SVS
 */
class SVSyncCore : noncopyable
{
public:
  class Error : public std::runtime_error
  {
  public:
    using std::runtime_error::runtime_error;
  };

public:
  /**
   * @brief Constructor
   *
   * @param face The face used to communication
   * @param syncPrefix The prefix of the sync group
   * @param onUpdate The callback function to handle state updates
   * @param syncKey Base64 encoded key to sign sync interests
   * @param nid ID for the node
   */
  SVSyncCore(ndn::Face& face,
             const Name& syncPrefix,
             const UpdateCallback& onUpdate,
             const SecurityOptions& securityOptions = SecurityOptions::DEFAULT,
             const NodeID& nid = EMPTY_NODE_ID);

  /**
   * @brief Reset the sync tree (and restart synchronization again)
   *
   * @param isOnInterest a flag that tells whether the reset is called by reset
   * interest.
   */
  void reset(bool isOnInterest = false);

  /**
   * @brief Get the node ID of the local session.
   *
   * @param prefix prefix of the node
   */
  const NodeID& getNodeId()
  {
    return m_id;
  }

  /**
   * @brief Get current seqNo of the local session.
   *
   * This method gets the seqNo according to prefix, if prefix is not specified,
   * it returns the seqNo of default user.
   *
   * @param prefix prefix of the node
   */
  SeqNo getSeqNo(const NodeID& nid = EMPTY_NODE_ID) const;

  /**
   * @brief Update the seqNo of the local session
   *
   * The method updates the existing seqNo with the supplied seqNo and NodeID.
   *
   * @param seq The new seqNo.
   * @param nid The NodeID of node to update.
   */
  void updateSeqNo(const SeqNo& seq, const NodeID& nid = EMPTY_NODE_ID);

  /// @brief Get all the nodeIDs
  std::set<NodeID> getNodeIds() const;

  using GetExtraBlockCallback = std::function<ndn::Block(const VersionVector&)>;
  using RecvExtraBlockCallback = std::function<void(const ndn::Block&, const VersionVector&)>;

  /**
   * @brief Callback to get extra data block for sync interest.
   *
   * The version vector will be locked during the duration of this callback,
   * so it must return FAST!
   */
  void setGetExtraBlockCallback(const GetExtraBlockCallback& callback)
  {
    m_getExtraBlock = callback;
  }

  /**
   * @brief Callback on receiving extra data in a sync interest.
   * Will be called BEFORE the interest is processed.
   */
  void setRecvExtraBlockCallback(const RecvExtraBlockCallback& callback)
  {
    m_recvExtraBlock = callback;
  }

  /// @brief Get current version vector
  VersionVector& getState()
  {
    return m_vv;
  }

  /// @brief Get human-readable representation of version vector
  std::string getStateStr() const
  {
    return m_vv.toStr();
  }

  NDN_SVS_PUBLIC_WITH_TESTS_ELSE_PRIVATE : void onSyncInterest(const Interest& interest);

  void onSyncInterestValidated(const Interest& interest);

  /**
   * @brief Mark the instance as initialized and send the first interest
   */
  void sendInitialInterest();

  /**
   * @brief sendSyncInterest and schedule a new retxSyncInterest event.
   *
   * @param send Send a sync interest immediately
   * @param delay Delay in milliseconds to schedule next interest (0 for
   * default).
   */
  void retxSyncInterest(bool send, unsigned int delay);

  /**
   * @brief Add one sync interest to queue.
   *
   * Called by retxSyncInterest(), or after increasing a sequence
   * number with updateSeqNo()
   */
  void sendSyncInterest();

  struct MergeResult
  {
    /// @brief If the local state vector has newer entries
    bool myVectorNew = false;
    /// @brief If the incoming state vector has newer entries
    bool otherVectorNew = false;
    /// @brief Newly learned missing information from incoming state vector
    std::vector<MissingDataInfo> missingInfo;
  };

  /**
   * @brief Merge state vector into the current
   * @param vvOther state vector to merge in
   * @details Also adds missing data interests to data interest queue.
   */
  MergeResult mergeStateVector(const VersionVector& vvOther);

  /**
   * @brief Record vector by merging it into m_recordedVv
   * @param vvOther state vector to merge in
   * @returns if recorded successfully
   */
  bool recordVector(const VersionVector& vvOther);

  /**
   * @brief Enter suppression state by setting
   * m_recording to True and initializing m_recordedVv to vvOther.
   * Does nothing if already in suppression state
   *
   * @param vvOther first vector to record
   */
  void enterSuppressionState(const VersionVector& vvOther);

  /// @brief Reference to scheduler
  ndn::Scheduler& getScheduler()
  {
    return m_scheduler;
  }

  /// @brief Get the current time in microseconds with arbitrary reference
  long getCurrentTime() const;

public:
  static inline const NodeID EMPTY_NODE_ID;

private:
  size_t getStateVectorLimit(size_t totalEntries) const;
  VersionVector buildSyncVector();
  void updateSyncInterval(SyncSignal signal);
  unsigned int sampleSyncDelay();

private:
  // Communication
  ndn::Face& m_face;
  const Name m_syncPrefix;
  const SecurityOptions m_securityOptions;
  const NodeID m_id;
  ndn::ScopedRegisteredPrefixHandle m_syncRegisteredPrefix;

  const UpdateCallback m_onUpdate;

  // State
  VersionVector m_vv;
  mutable std::mutex m_vvMutex;
  // Aggregates incoming vectors while in suppression state
  std::unique_ptr<VersionVector> m_recordedVv = nullptr;
  mutable std::mutex m_recordedVvMutex;

  // Extra block
  GetExtraBlockCallback m_getExtraBlock;
  RecvExtraBlockCallback m_recvExtraBlock;

  // Max suppression time; this value is roughly
  // positively correlated to the network diameter
  time::milliseconds m_maxSuppressionTime;
  // Lower and upper bounds for the adaptive periodic sync timer.
  time::milliseconds m_minPeriodicSyncTime;
  time::milliseconds m_maxPeriodicSyncTime;
  // Current adaptive timer value.
  time::milliseconds m_periodicSyncTime;
  // Fraction of jitter in the periodic timer value.
  // Positively correlated to network diameter.
  double m_periodicSyncJitter;
  // Number of recently updated entries that are always prioritized.
  size_t m_recentStateVectorEntries;
  // Hard cap for the number of state vector entries per sync interest.
  size_t m_maxStateVectorEntries;
  // Ratio of the current vector to carry in a partial Sync Interest.
  double m_stateVectorRatio;
  // Selection strategy used to build partial state vectors.
  VersionVector::SubsetStrategy m_stateVectorStrategy;
  // Fraction of the hybrid budget reserved for recent/hot entries.
  double m_hybridHotRatio;
  // Minimum number of rotating fairness entries kept in hybrid mode.
  size_t m_hybridMinFairEntries;
  // Tunable weights for the score-based state-vector selector.
  double m_scoreSeqWeight;
  double m_scoreRecentWeight;
  double m_scoreFairWeight;
  double m_scorePreferredBoost;
  // Fraction of the previous recent subset to retain in sticky mode.
  double m_stickyHoldRatio;
  // Minimum number of previous entries to keep when sticky mode is enabled.
  size_t m_stickyMinHold;
  // Fraction of the state-vector budget reserved for the recent+quota family.
  double m_recentQuotaRatio;
  // Minimum quota slots reserved for novelty/random quota selectors.
  size_t m_recentQuotaMinEntries;
  // Trigger threshold for switching adaptive recent/score into score mode.
  double m_adaptiveScoreThreshold;
  // Number of consecutive score rounds to keep once adaptive mode triggers.
  size_t m_adaptiveScoreRounds;
  // Estimated one-way network diameter in milliseconds.
  double m_networkDiameterMs;
  // Estimated hop diameter of the topology.
  size_t m_networkDiameterHops;
  // Packet loss rate in the current scenario, normalized to [0,1].
  double m_linkLossRate;
  // Expected fraction of hot producers in the workload.
  double m_expectedHotspotRatio;
  // Whether timer and partial-vector decisions should be coordinated.
  bool m_enableCoordinatedSync;
  // Timer mode for comparison between adaptive and fixed scheduling.
  TimerMode m_timerMode;
  // Whether to print per-interest metrics for offline evaluation.
  bool m_enableMetricsLog;
  // Round-robin cursor through the state vector to guarantee coverage.
  size_t m_stateVectorCursor;
  // Smoothed activity estimate used by the adaptive timer.
  double m_activityScore = 0.0;
  // Smoothed hotspot pressure estimate.
  double m_hotspotScore = 0.0;
  // Remaining forced score rounds for the adaptive recent/score strategy.
  size_t m_adaptiveScoreBurstRemaining = 0;
  // Most recent partial selection, used by sticky recent mode.
  std::vector<NodeID> m_lastSelectedNodes;
  // Timestamp of the last timer adjustment to avoid overreacting.
  long m_lastTimerAdjustUs = 0;
  // Minimum transmit gap used to batch bursty sync notifications.
  time::milliseconds m_minSendInterval{ 0_ms };
  // Small batching delay for local publications.
  time::milliseconds m_localUpdateDelay{ 1_ms };
  // Timestamp of the last actual Sync Interest transmission.
  std::atomic_long m_lastSyncTxUs{ 0 };

  // Random Engine
  ndn::random::RandomNumberEngine& m_rng;
  // Milliseconds to send sync interest reply after
  std::uniform_int_distribution<> m_intrReplyDist;

  // Security
  ndn::KeyChain m_keyChainMem;

  ndn::Scheduler m_scheduler;
  mutable std::mutex m_schedulerMutex;
  scheduler::ScopedEventId m_retxEvent;
  scheduler::ScopedEventId m_packetEvent;

  // Time at which the next sync interest will be sent
  std::atomic_long m_nextSyncInterest;

  // Prevent sending interests before initialization
  bool m_initialized = false;
};

} // namespace ndn::svs

#endif // NDN_SVS_CORE_HPP
