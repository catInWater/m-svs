/* -*- Mode: C++; c-file-style: "gnu"; indent-tabs-mode:nil -*- */

#include <ndn-svs/svsync.hpp>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

long
nowMs()
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

long
getEnvLong(const char* name, long defaultValue)
{
  const char* value = std::getenv(name);
  if (value == nullptr)
    return defaultValue;

  try {
    return std::stol(value);
  }
  catch (const std::exception&) {
    return defaultValue;
  }
}

std::string
getEnvString(const char* name, const std::string& defaultValue)
{
  const char* value = std::getenv(name);
  return value == nullptr ? defaultValue : std::string(value);
}

class EvalProgram
{
public:
  EvalProgram(std::string nodeId, std::string syncPrefix)
    : m_nodeId(std::move(nodeId))
    , m_syncPrefix(std::move(syncPrefix))
    , m_scheduler(m_face.getIoContext())
    , m_publishIntervalMs(getEnvLong("SVS_PUB_INTERVAL_MS", 1000))
    , m_startDelayMs(getEnvLong("SVS_START_DELAY_MS", 1000))
    , m_cooldownMs(getEnvLong("SVS_COOLDOWN_MS", 3000))
    , m_durationMs(getEnvLong("SVS_RUN_DURATION_SEC", 10) * 1000)
    , m_role(getEnvString("SVS_NODE_ROLE", "slow"))
  {
    m_stopTimeMs = nowMs() + m_startDelayMs + m_durationMs;

    m_svs = std::make_shared<ndn::svs::SVSyncCore>(
      m_face,
      ndn::Name(m_syncPrefix),
      std::bind(&EvalProgram::onUpdate, this, _1),
      ndn::svs::SecurityOptions::DEFAULT,
      ndn::Name(m_nodeId));

    std::cout << "NODE_START node=" << m_nodeId
              << " prefix=" << m_syncPrefix
              << " role=" << m_role
              << " interval_ms=" << m_publishIntervalMs
              << " duration_ms=" << m_durationMs
              << " ts=" << nowMs() << std::endl;

    m_scheduler.schedule(ndn::time::milliseconds(m_startDelayMs), [this] { publishOnce(); });
    m_scheduler.schedule(ndn::time::milliseconds(m_startDelayMs + m_durationMs + m_cooldownMs),
                         [this] { stop(); });
  }

  void
  run()
  {
    m_face.processEvents();
  }

private:
  void
  publishOnce()
  {
    long ts = nowMs();
    if (ts > m_stopTimeMs)
      return;

    auto seq = m_svs->getSeqNo() + 1;
    m_svs->updateSeqNo(seq);

    std::cout << "PUB node=" << m_nodeId
              << " seq=" << seq
              << " ts=" << ts << std::endl;

    m_scheduler.schedule(ndn::time::milliseconds(m_publishIntervalMs), [this] { publishOnce(); });
  }

  void
  onUpdate(const std::vector<ndn::svs::MissingDataInfo>& updates)
  {
    long ts = nowMs();
    for (const auto& item : updates) {
      for (ndn::svs::SeqNo seq = item.low; seq <= item.high; ++seq) {
        std::cout << "LEARN listener=" << m_nodeId
                  << " producer=" << item.nodeId
                  << " seq=" << seq
                  << " ts=" << ts << std::endl;
      }
    }
  }

  void
  stop()
  {
    std::cout << "NODE_STOP node=" << m_nodeId << " ts=" << nowMs() << std::endl;
    m_face.shutdown();
  }

private:
  std::string m_nodeId;
  std::string m_syncPrefix;
  ndn::Face m_face;
  ndn::Scheduler m_scheduler;
  std::shared_ptr<ndn::svs::SVSyncCore> m_svs;
  long m_publishIntervalMs;
  long m_startDelayMs;
  long m_cooldownMs;
  long m_durationMs;
  long m_stopTimeMs = 0;
  std::string m_role;
};

} // namespace

int
main(int argc, char** argv)
{
  if (argc < 2 || argc > 3) {
    std::cerr << "Usage: " << argv[0] << " <node-id> [sync-prefix]" << std::endl;
    return 1;
  }

  std::string nodeId = argv[1];
  std::string syncPrefix = argc >= 3 ? argv[2] : getEnvString("SVS_SYNC_PREFIX", "/ndn/svs-eval");

  EvalProgram program(nodeId, syncPrefix);
  program.run();
  return 0;
}
