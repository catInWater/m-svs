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
#include <random>
#include <vector>

namespace ndn::svs {

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
                            size_t minFairEntries) const
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

  switch (strategy) {
    case SubsetStrategy::Recent:
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
