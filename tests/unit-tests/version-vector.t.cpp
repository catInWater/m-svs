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

#include "tests/boost-test.hpp"

namespace ndn::tests {

using namespace ndn::svs;

class VersionVectorFixture
{
protected:
  VersionVectorFixture()
  {
    v.set("one", 1);
    v.set("two", 2);
  }

protected:
  VersionVector v;
};

BOOST_FIXTURE_TEST_SUITE(TestVersionVector, VersionVectorFixture)

BOOST_AUTO_TEST_CASE(Get)
{
  BOOST_CHECK_EQUAL(v.get("one"), 1);
  BOOST_CHECK_EQUAL(v.get("two"), 2);
  BOOST_CHECK_EQUAL(v.get("five"), 0);
}

BOOST_AUTO_TEST_CASE(Set)
{
  BOOST_CHECK_EQUAL(v.set("four", 44), 44);
  BOOST_CHECK_EQUAL(v.get("four"), 44);
}

BOOST_AUTO_TEST_CASE(Iterate)
{
  std::unordered_map<NodeID, SeqNo> umap;
  for (auto elem : v) {
    umap[elem.first] = elem.second;
  }

  BOOST_CHECK_EQUAL(umap["one"], 1);
  BOOST_CHECK_EQUAL(umap["two"], 2);
  BOOST_CHECK_EQUAL(umap.size(), 2);
}

BOOST_AUTO_TEST_CASE(EncodeDecode)
{
  ndn::Block block = v.encode();
  BOOST_CHECK_EQUAL(block.value_size(), 24);

  VersionVector dv(block);
  BOOST_CHECK_EQUAL(dv.get("one"), 1);
  BOOST_CHECK_EQUAL(dv.get("two"), 2);
}

BOOST_AUTO_TEST_CASE(DecodeStatic)
{
  // Hex: CA0A070508036F6E65CC0101CA0A0705080374776FCC0102
  constexpr std::string_view encoded{ "\xCA\x0A\x07\x05\x08\x03\x6F\x6E\x65\xCC\x01\x01"
                                      "\xCA\x0A\x07\x05\x08\x03\x74\x77\x6F\xCC\x01\x02" };
  VersionVector dv(ndn::encoding::makeStringBlock(svs::tlv::StateVector, encoded));
  BOOST_CHECK_EQUAL(dv.get("one"), 1);
  BOOST_CHECK_EQUAL(dv.get("two"), 2);
}

BOOST_AUTO_TEST_CASE(Ordering)
{
  VersionVector v1;
  v1.set("one", 1);
  v1.set("two", 2);
  VersionVector v2;
  v2.set("two", 2);
  v2.set("one", 1);

  Block v1e = v1.encode();
  Block v2e = v2.encode();

  std::string v1str(reinterpret_cast<const char*>(v1e.value()), v1e.value_size());
  std::string v2str(reinterpret_cast<const char*>(v2e.value()), v2e.value_size());

  BOOST_CHECK_EQUAL(v1str, v2str);
}

BOOST_AUTO_TEST_CASE(SelectSubsetKeepsPreferredAndRecentEntries)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 3);
  vv.set("/node/four", 4);

  VersionVector subset = vv.selectSubset(2, 1, 0, "/node/two");

  BOOST_CHECK_EQUAL(subset.size(), 2);
  BOOST_CHECK(subset.has("/node/two"));
  BOOST_CHECK(subset.has("/node/four"));
  BOOST_CHECK_EQUAL(subset.get("/node/two"), 2);
  BOOST_CHECK_EQUAL(subset.get("/node/four"), 4);
}

BOOST_AUTO_TEST_CASE(SelectSubsetRoundRobinMaintainsBudget)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 3);
  vv.set("/node/four", 4);
  vv.set("/node/five", 5);

  VersionVector subset = vv.selectSubset(3,
                                         0,
                                         2,
                                         "/node/one",
                                         VersionVector::SubsetStrategy::RoundRobin);

  BOOST_CHECK_EQUAL(subset.size(), 3);
  BOOST_CHECK(subset.has("/node/one"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetRecentOnlyMaintainsBudget)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 3);
  vv.set("/node/four", 4);

  VersionVector subset = vv.selectSubset(2,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::Recent);

  BOOST_CHECK_EQUAL(subset.size(), 2);
  BOOST_CHECK(subset.has("/node/four"));
  BOOST_CHECK(subset.has("/node/three"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetRandomMaintainsBudget)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 3);
  vv.set("/node/four", 4);
  vv.set("/node/five", 5);

  VersionVector subset = vv.selectSubset(3,
                                         0,
                                         42,
                                         "/node/two",
                                         VersionVector::SubsetStrategy::Random);

  BOOST_CHECK_EQUAL(subset.size(), 3);
  BOOST_CHECK(subset.has("/node/two"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetHybridBalancesHotAndFairCoverage)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 3);
  vv.set("/node/four", 4);
  vv.set("/node/five", 5);
  vv.set("/node/six", 6);

  VersionVector subset = vv.selectSubset(4,
                                         1,
                                         2,
                                         "/node/one",
                                         VersionVector::SubsetStrategy::Hybrid);

  BOOST_CHECK_EQUAL(subset.size(), 4);
  BOOST_CHECK(subset.has("/node/one"));
  BOOST_CHECK(subset.has("/node/six"));
  BOOST_CHECK(subset.has("/node/five"));
  BOOST_CHECK(subset.has("/node/two") || subset.has("/node/three") || subset.has("/node/four"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetScoreMaintainsBudgetAndFavorsRecentHotEntries)
{
  VersionVector vv;
  vv.set("/node/one", 1);
  vv.set("/node/two", 2);
  vv.set("/node/three", 8);
  vv.set("/node/four", 10);
  vv.set("/node/five", 3);
  vv.set("/node/six", 4);

  VersionVector subset = vv.selectSubset(4,
                                         2,
                                         3,
                                         "/node/two",
                                         VersionVector::SubsetStrategy::Score,
                                         0.75,
                                         1,
                                         0.35,
                                         0.45,
                                         0.20,
                                         0.80);

  BOOST_CHECK_EQUAL(subset.size(), 4);
  BOOST_CHECK(subset.has("/node/two"));
  BOOST_CHECK(subset.has("/node/four"));
  BOOST_CHECK(subset.has("/node/three"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetClusterHybridSpreadsAcrossClusters)
{
  VersionVector vv;
  vv.set("/test/n0_0", 1);
  vv.set("/test/n0_1", 2);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 4);
  vv.set("/test/n2_0", 5);

  VersionVector subset = vv.selectSubset(3,
                                         1,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::ClusterHybrid);

  BOOST_CHECK_EQUAL(subset.size(), 3);
  BOOST_CHECK(subset.has("/test/n2_0"));
  BOOST_CHECK((subset.has("/test/n0_1") || subset.has("/test/n0_0")));
  BOOST_CHECK((subset.has("/test/n1_1") || subset.has("/test/n1_0")));
}

BOOST_AUTO_TEST_CASE(SelectSubsetClusterScorePrefersClusterDiversity)
{
  VersionVector vv;
  vv.set("/test/n0_0", 9);
  vv.set("/test/n0_1", 8);
  vv.set("/test/n1_0", 7);
  vv.set("/test/n2_0", 6);
  vv.set("/test/n2_1", 5);

  VersionVector subset = vv.selectSubset(3,
                                         1,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::ClusterScore);

  BOOST_CHECK_EQUAL(subset.size(), 3);
  BOOST_CHECK(subset.has("/test/n0_0") || subset.has("/test/n0_1"));
  BOOST_CHECK(subset.has("/test/n1_0"));
  BOOST_CHECK(subset.has("/test/n2_0") || subset.has("/test/n2_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetAgeScoreFavorsOlderEntries)
{
  VersionVector vv;
  vv.set("/test/n0_0", 3);
  vv.set("/test/n0_1", 3);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 3);

  VersionVector subset = vv.selectSubset(2,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::AgeScore);

  BOOST_CHECK_EQUAL(subset.size(), 2);
  BOOST_CHECK(subset.has("/test/n0_0"));
  BOOST_CHECK(subset.has("/test/n0_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetDeficitScoreFavorsLaggingEntries)
{
  VersionVector vv;
  vv.set("/test/n0_0", 10);
  vv.set("/test/n0_1", 9);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 2);

  VersionVector subset = vv.selectSubset(2,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::DeficitScore);

  BOOST_CHECK_EQUAL(subset.size(), 2);
  BOOST_CHECK(subset.has("/test/n1_0"));
  BOOST_CHECK(subset.has("/test/n1_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetStickyRecentKeepsPreviousWindow)
{
  VersionVector vv;
  vv.set("/test/n0_0", 1);
  vv.set("/test/n0_1", 2);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 4);
  vv.set("/test/n2_0", 5);

  std::vector<NodeID> sticky = { "/test/n0_0", "/test/n1_0" };
  VersionVector subset = vv.selectSubset(4,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::StickyRecent,
                                         0.75,
                                         1,
                                         0.35,
                                         0.45,
                                         0.20,
                                         0.60,
                                         sticky,
                                         2);

  BOOST_CHECK_EQUAL(subset.size(), 4);
  BOOST_CHECK(subset.has("/test/n0_0"));
  BOOST_CHECK(subset.has("/test/n1_0"));
  BOOST_CHECK(subset.has("/test/n2_0"));
  BOOST_CHECK(subset.has("/test/n1_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetRecentNoveltyQuotaPrefersPreviouslyUnseenEntries)
{
  VersionVector vv;
  vv.set("/test/n0_0", 1);
  vv.set("/test/n0_1", 2);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 4);
  vv.set("/test/n2_0", 5);

  std::vector<NodeID> previous = { "/test/n2_0", "/test/n1_1", "/test/n1_0" };
  VersionVector subset = vv.selectSubset(4,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::RecentNoveltyQuota,
                                         0.75,
                                         1,
                                         0.35,
                                         0.45,
                                         0.20,
                                         0.60,
                                         {},
                                         0,
                                         previous,
                                         2);

  BOOST_CHECK_EQUAL(subset.size(), 4);
  BOOST_CHECK(subset.has("/test/n2_0"));
  BOOST_CHECK(subset.has("/test/n1_1"));
  BOOST_CHECK(subset.has("/test/n0_0"));
  BOOST_CHECK(subset.has("/test/n0_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetRecentRandomQuotaMaintainsBudget)
{
  VersionVector vv;
  vv.set("/test/n0_0", 1);
  vv.set("/test/n0_1", 2);
  vv.set("/test/n1_0", 3);
  vv.set("/test/n1_1", 4);
  vv.set("/test/n2_0", 5);

  VersionVector subset = vv.selectSubset(4,
                                         0,
                                         7,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::RecentRandomQuota,
                                         0.75,
                                         1,
                                         0.35,
                                         0.45,
                                         0.20,
                                         0.60,
                                         {},
                                         0,
                                         {},
                                         2);

  BOOST_CHECK_EQUAL(subset.size(), 4);
  BOOST_CHECK(subset.has("/test/n2_0"));
  BOOST_CHECK(subset.has("/test/n1_1"));
}

BOOST_AUTO_TEST_CASE(SelectSubsetAdaptiveRecentScoreFallsBackToRecentOrdering)
{
  VersionVector vv;
  vv.set("/test/n0_0", 1);
  vv.set("/test/n0_1", 2);
  vv.set("/test/n1_0", 9);
  vv.set("/test/n1_1", 10);

  VersionVector subset = vv.selectSubset(2,
                                         0,
                                         0,
                                         NodeID(),
                                         VersionVector::SubsetStrategy::AdaptiveRecentScore);

  BOOST_CHECK_EQUAL(subset.size(), 2);
  BOOST_CHECK(subset.has("/test/n1_1"));
  BOOST_CHECK(subset.has("/test/n1_0"));
}

BOOST_AUTO_TEST_SUITE_END()

} // namespace ndn::tests
