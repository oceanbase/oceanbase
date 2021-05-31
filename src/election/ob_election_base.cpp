/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <execinfo.h>
#include "lib/lock/Monitor.h"
#include "lib/net/ob_addr.h"
#include "lib/net/tbnetutil.h"
#include "ob_election_base.h"
#include "ob_election_async_log.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {

using namespace common;
using namespace tbutil;

namespace election {
// TODO need check versions
bool check_upgrade_env()
{
  return (GCONF.in_upgrade_mode());
}

// no need judge is_candidate_(msg.get_sender())
int get_self_addr(ObAddr& self, const char* dev, const int32_t port)
{
  int ret = OB_SUCCESS;
  char ipv6[MAX_IP_ADDR_LENGTH] = {'\0'};
  uint32_t ipv4 = 0;
  ObAddr server;

  if (NULL == dev || port <= 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", KP(dev), K(port));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (GCONF.use_ipv6) {
      if (OB_FAIL(obsys::CNetUtil::getLocalAddr6(dev, ipv6, sizeof(ipv6)))) {
        ELECT_ASYNC_LOG(WARN, "get local ipv6 error", K(dev));
        ret = OB_INVALID_ARGUMENT;
      }
    } else {
      if (0 == (ipv4 = obsys::CNetUtil::getLocalAddr(dev))) {
        ELECT_ASYNC_LOG(WARN, "get local ipv4 error", K(dev));
        ret = OB_INVALID_ARGUMENT;
      } else if (!server.set_ipv4_addr(ipv4, port)) {
        ELECT_ASYNC_LOG(ERROR, "server address invalid", K(dev), K(port));
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  if (OB_SUCC(ret)) {
    self = server;
  }
  return ret;
}

const char* ObElectionMemberName(ObElectionMemberType type)
{
  const char* ptr = "UNKNOWN";

  switch (type) {
    case MEMBER_TYPE_VOTER:
      ptr = "VOTER";
      break;
    case MEMBER_TYPE_CANDIDATE:
      ptr = "CANDIDATE";
      break;
    case MEMBER_TYPE_UNKNOWN:
    // fallthrough
    default:
      ptr = "UNKNOWN";
      break;
  }

  return ptr;
}

const char* ObElectionRoleName(ObElectionRole role)
{
  const char* ptr = "UNKNOWN";

  switch (role) {
    case ROLE_SLAVE:
      ptr = "SLAVE";
      break;
    case ROLE_LEADER:
      ptr = "LEADER";
      break;
    case ROLE_UNKNOWN:
    // fallthrough
    default:
      ptr = "UNKNOWN";
      break;
  }

  return ptr;
}

const char* ObElectionStatName(ObElectionStat stat)
{
  const char* ptr = "UNKNOWN";

  switch (stat) {
    case STATE_IDLE:
      ptr = "IDLE";
      break;
    case STATE_DECENTRALIZED_VOTING:
      ptr = "DEVOTING";
      break;
    case STATE_CENTRALIZED_VOTING:
      ptr = "VOTING";
      break;
    case STATE_UNKNOWN:
    // fallthrough
    default:
      ptr = "UNKNOWN";
      break;
  }

  return ptr;
}

const char* ObElectionStageName(ObElectionStage stage)
{
  const char* ptr = "UNKNOWN";

  switch (stage) {
    case STAGE_PREPARE:
      ptr = "PREPARE";
      break;
    case STAGE_VOTING:
      ptr = "VOTING";
      break;
    case STAGE_COUNTING:
      ptr = "COUNTING";
      break;
    default:
      ptr = "UNKNOWN";
      break;
  }

  return ptr;
}

void msleep(int64_t ms)
{
  tbutil::Monitor<tbutil::Mutex> monitor_;
  (void)monitor_.timedWait(Time(ms * 1000));
}

}  // namespace election
}  // namespace oceanbase
