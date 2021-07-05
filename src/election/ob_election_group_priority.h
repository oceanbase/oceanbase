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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_PRIORITY_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_PRIORITY_

#include <stdint.h>
#include "lib/ob_name_id_def.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {

namespace election {

class ObElectionGroupPriority {
  OB_UNIS_VERSION(1);

public:
  ObElectionGroupPriority()
  {
    reset();
  }
  ObElectionGroupPriority(const bool is_candidate, const int64_t system_score);
  ~ObElectionGroupPriority()
  {}
  void reset();

public:
  void set_candidate(const bool is_candidate)
  {
    is_candidate_ = is_candidate;
  }
  bool is_candidate() const
  {
    return is_candidate_;
  }
  int64_t get_system_score() const
  {
    return system_score_;
  }
  void set_system_clog_disk_error();
  void set_system_data_disk_error();
  void set_system_service_not_started();

  int compare(const ObElectionGroupPriority& priority) const;
  ObElectionGroupPriority& operator=(const ObElectionGroupPriority& priority);
  bool is_valid() const;

  DECLARE_TO_STRING_AND_YSON;

private:
  const static int64_t SYSTEM_SCORE_CLOG_DISK_ERROR = (1 << 6);
  const static int64_t SYSTEM_SCORE_DATA_DISK_ERROR = (1 << 4);
  const static int64_t SYSTEM_SCORE_SERVICE_NOT_STARTED = (1 << 1);

private:
  bool is_candidate_;
  int64_t system_score_;  // lower score means higher priority
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_PRIORITY_
