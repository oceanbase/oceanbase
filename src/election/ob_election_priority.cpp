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

#include "ob_election_priority.h"
#include "clog/ob_log_define.h"
#include "ob_election_async_log.h"

namespace oceanbase {
using namespace oceanbase::common;
using namespace oceanbase::clog;
namespace election {
OB_SERIALIZE_MEMBER(
    ObElectionPriority, is_candidate_, membership_version_, log_id_, data_version_, locality_, system_score_);

int ObElectionPriority::init(
    const bool is_candidate, const int64_t membership_version, const uint64_t log_id, const uint64_t locality)
{
  int ret = OB_SUCCESS;

  if (membership_version < 0) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(membership_version), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_candidate_ = is_candidate;
    membership_version_ = membership_version;
    log_id_ = log_id;
    locality_ = locality;
    data_version_.reset();
    system_score_ = 0;
  }

  return ret;
}

void ObElectionPriority::reset()
{
  is_candidate_ = false;
  membership_version_ = -1;
  log_id_ = 0;
  data_version_.reset();
  locality_ = UINT64_MAX - 1;
  system_score_ = 0;
}

/* function:compare_without_logid
 * c:not compare logid when compare election priority
 */
int ObElectionPriority::compare_without_logid(const ObElectionPriority& priority) const
{
  const bool with_locality = false;
  const bool with_log_id = false;
  return compare_(priority, with_locality, with_log_id);
}

/* function:compare_with_buffered_logid
 * features:compare election priority with log_id(imprecise),used when count valid_candidates,allow caller's log behind
 * in a range(50000) limits  :this function can only called from follower_prioriy, arg setted as leader_priority
 */
int ObElectionPriority::compare_with_buffered_logid(const ObElectionPriority& priority) const
{
  const bool with_locality = false;
  const bool with_log_id = false;
  int ret = compare_(priority, with_locality, with_log_id);
  if (ret >= 0) {
    if (log_id_ + CLOG_SWITCH_LEADER_ALLOW_THRESHOLD >= priority.get_log_id()) {
    } else {
      ret = -1;
    }
  }

  return ret;
}

/* function:compare_with_accurate_logid
 * function:compare election priority with log_id(precise), used in decentralized voting, for elecing a highest priority
 * leader.
 */
int ObElectionPriority::compare_with_accurate_logid(const ObElectionPriority& priority) const
{
  const bool with_locality = true;
  const bool with_log_id = true;
  return compare_(priority, with_locality, with_log_id);
}

ObElectionPriority& ObElectionPriority::operator=(const ObElectionPriority& priority)
{
  // avoid assign to myself
  if (this != &priority) {
    is_candidate_ = priority.is_candidate();
    membership_version_ = priority.get_membership_version();
    log_id_ = priority.get_log_id();
    // data_version_ remained, just for compatibility reasons
    data_version_.reset();
    locality_ = priority.get_locality();
    system_score_ = priority.get_system_score();
  }

  return *this;
}

// not check data_version anymore
bool ObElectionPriority::is_valid() const
{
  return membership_version_ >= 0;
}

int64_t ObElectionPriority::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  databuff_printf(buf,
      buf_len,
      pos,
      "[is_candidate=%d, membership_version=%ld, log_id=%lu, data_version=%s, locality=%lu, system_score_=%ld]",
      is_candidate_,
      membership_version_,
      log_id_,
      to_cstring(data_version_),
      locality_,
      system_score_);

  return pos;
}

int ObElectionPriority::compare_(
    const ObElectionPriority& priority, const bool with_locality, const bool with_log_id) const
{
  int ret = 0;

  if (membership_version_ > priority.get_membership_version()) {
    ret = 1;
  } else if (membership_version_ == priority.get_membership_version()) {
  } else {
    ret = -1;
  }

  if (0 == ret) {
    if (is_candidate_ && !priority.is_candidate()) {
      ret = 1;
    } else if (!is_candidate_ && priority.is_candidate()) {
      ret = -1;
    }
  }

  if (0 == ret) {
    if (with_locality) {
      if (locality_ < priority.get_locality()) {
        ret = 1;
      } else if (locality_ == priority.get_locality()) {
      } else {
        ret = -1;
      }
    }
  }

  if (0 == ret) {
    if (get_system_score_without_election_blacklist() < priority.get_system_score_without_election_blacklist()) {
      ret = 1;
    } else if (get_system_score_without_election_blacklist() ==
               priority.get_system_score_without_election_blacklist()) {
    } else {
      ret = -1;
    }
  }

  if (0 == ret) {
    if (with_log_id) {
      if (log_id_ > priority.get_log_id()) {
        ret = 1;
      } else if (log_id_ == priority.get_log_id()) {
        // do nothing
      } else {
        ret = -1;
      }
    }
  }

  return ret;
}

int64_t ObElectionPriority::get_system_score_without_election_blacklist() const
{
  return ((system_score_ / 100) & (~SYSTEM_SCORE_IN_ELECTION_BLACKLIST)) * 100;
}

bool ObElectionPriority::is_in_election_blacklist() const
{
  return (system_score_ / 100) & SYSTEM_SCORE_IN_ELECTION_BLACKLIST;
}

void ObElectionPriority::set_system_clog_disk_error()
{
  system_score_ += SYSTEM_SCORE_CLOG_DISK_ERROR * 100;
}

void ObElectionPriority::set_system_tenant_out_of_memory()
{
  system_score_ += SYSTEM_SCORE_TENANT_OUT_OF_MEM * 100;
}

void ObElectionPriority::set_system_data_disk_error()
{
  system_score_ += SYSTEM_SCORE_DATA_DISK_ERROR * 100;
}

void ObElectionPriority::set_system_need_rebuild()
{
  system_score_ += SYSTEM_SCORE_NEED_REBUILD * 100;
}

void ObElectionPriority::set_system_in_election_blacklist()
{
  system_score_ += SYSTEM_SCORE_IN_ELECTION_BLACKLIST * 100;
}

void ObElectionPriority::set_system_service_not_started()
{
  system_score_ += SYSTEM_SCORE_SERVICE_NOT_STARTED * 100;
}

void ObElectionPriority::set_system_without_memstore()
{
  system_score_ += SYSTEM_SCORE_WITHOUT_MEMSTORE * 100;
}
}  // namespace election
}  // namespace oceanbase
