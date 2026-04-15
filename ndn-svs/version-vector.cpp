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
                            const NodeID& preferredNode) const
{
  VersionVector selected;

  if (maxEntries == 0 || m_map.empty())
    return selected;

  if (m_map.size() <= maxEntries)
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

  if (recentEntries > 0) {
    auto recentNodes = orderedNodes;
    std::sort(recentNodes.begin(), recentNodes.end(), [this](const NodeID& lhs, const NodeID& rhs) {
      auto lhsTs = getLastUpdate(lhs);
      auto rhsTs = getLastUpdate(rhs);
      if (lhsTs == rhsTs)
        return lhs < rhs;
      return lhsTs > rhsTs;
    });

    for (const auto& nid : recentNodes) {
      if (selected.m_map.size() >= maxEntries || recentEntries == 0)
        break;
      if (addEntry(nid))
        --recentEntries;
    }
  }

  size_t offset = startIndex % orderedNodes.size();
  for (size_t i = 0; i < orderedNodes.size() && selected.m_map.size() < maxEntries; ++i)
    addEntry(orderedNodes[(offset + i) % orderedNodes.size()]);

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
