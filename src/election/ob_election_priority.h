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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_PRIORITY_
#define OCEANBASE_ELECTION_OB_ELECTION_PRIORITY_

#include <stdint.h>
#include <stdlib.h>
#include "share/ob_define.h"
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {

namespace election {

class ObElectionPriority {
  OB_UNIS_VERSION(1);

public:
  ObElectionPriority()
  {
    reset();
  }
  ~ObElectionPriority()
  {}
  int init(const bool is_candidate, const int64_t membership_version, const uint64_t log_id, const uint64_t locality);
  void reset();

public:
  void set_membership_version(const int64_t ts)
  {
    membership_version_ = ts;
  }
  int64_t get_membership_version() const
  {
    return membership_version_;
  }
  void set_log_id(const uint64_t log_id)
  {
    log_id_ = log_id;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  void set_candidate(const bool is_candidate)
  {
    is_candidate_ = is_candidate;
  }
  bool is_candidate() const
  {
    return is_candidate_;
  }
  uint64_t get_locality() const
  {
    return locality_;
  }
  void set_locality(const uint64_t locality)
  {
    locality_ = locality;
  }

  int64_t get_system_score() const
  {
    return system_score_;
  }
  int64_t get_system_score_without_election_blacklist() const;
  bool is_in_election_blacklist() const;
  void set_system_clog_disk_error();
  void set_system_tenant_out_of_memory();
  void set_system_data_disk_error();
  void set_system_need_rebuild();
  void set_system_in_election_blacklist();
  void set_system_service_not_started();
  void set_system_without_memstore();

  int compare_without_logid(const ObElectionPriority& priority) const;
  int compare_with_buffered_logid(const ObElectionPriority& priority) const;
  int compare_with_accurate_logid(const ObElectionPriority& p) const;
  ObElectionPriority& operator=(const ObElectionPriority& priority);
  bool is_valid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(is_candidate), Y_(membership_version), Y_(log_id), Y_(data_version), Y_(locality));

private:
  int compare_(const ObElectionPriority& priority, const bool with_locality, const bool with_log_id) const;

private:
  const static int64_t SYSTEM_SCORE_CLOG_DISK_ERROR = (1 << 6);
  const static int64_t SYSTEM_SCORE_TENANT_OUT_OF_MEM = (1 << 5);
  const static int64_t SYSTEM_SCORE_DATA_DISK_ERROR = (1 << 4);
  const static int64_t SYSTEM_SCORE_NEED_REBUILD = (1 << 3);
  const static int64_t SYSTEM_SCORE_IN_ELECTION_BLACKLIST = (1 << 2);
  const static int64_t SYSTEM_SCORE_SERVICE_NOT_STARTED = (1 << 1);
  const static int64_t SYSTEM_SCORE_WITHOUT_MEMSTORE = (1 << 0);

private:
  bool is_candidate_;
  int64_t membership_version_;
  uint64_t log_id_;
  common::ObVersion data_version_;
  uint64_t locality_;     // lower value means higher priority
  int64_t system_score_;  // lower value means higher priority
};

static const int64_t OB_ARRAY_COUNT = 16;
typedef common::ObSEArray<ObElectionPriority, OB_ARRAY_COUNT> PriorityArray;

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_PRIORITY_
