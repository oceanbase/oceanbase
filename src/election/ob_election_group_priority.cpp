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

#include "ob_election_group_priority.h"
#include "ob_election_async_log.h"
#include "lib/json/ob_yson.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace election {
OB_SERIALIZE_MEMBER(ObElectionGroupPriority, is_candidate_, system_score_);

ObElectionGroupPriority::ObElectionGroupPriority(const bool is_candidate, const int64_t system_score)
{
  is_candidate_ = is_candidate;
  system_score_ = system_score;
}

void ObElectionGroupPriority::reset()
{
  is_candidate_ = false;
  system_score_ = 0;
}

ObElectionGroupPriority& ObElectionGroupPriority::operator=(const ObElectionGroupPriority& priority)
{
  if (this != &priority) {
    is_candidate_ = priority.is_candidate();
    system_score_ = priority.get_system_score();
  }

  return *this;
}

bool ObElectionGroupPriority::is_valid() const
{
  return true;
}

DEFINE_TO_STRING_AND_YSON(ObElectionGroupPriority, Y_(is_candidate), Y_(system_score));

int ObElectionGroupPriority::compare(const ObElectionGroupPriority& priority) const
{
  int ret = 0;

  if (system_score_ == priority.get_system_score()) {
    ret = 0;
  } else if (system_score_ < priority.get_system_score()) {
    ret = 1;
  } else {
    ret = -1;
  }

  return ret;
}

void ObElectionGroupPriority::set_system_clog_disk_error()
{
  system_score_ += SYSTEM_SCORE_CLOG_DISK_ERROR * 100;
}

void ObElectionGroupPriority::set_system_data_disk_error()
{
  system_score_ += SYSTEM_SCORE_DATA_DISK_ERROR * 100;
}

void ObElectionGroupPriority::set_system_service_not_started()
{
  system_score_ += SYSTEM_SCORE_SERVICE_NOT_STARTED * 100;
}
}  // namespace election
}  // namespace oceanbase
