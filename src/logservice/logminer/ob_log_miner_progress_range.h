/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LOG_MINER_PROGRESS_RANGE_H_
#define OCEANBASE_LOG_MINER_PROGRESS_RANGE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerProgressRange
{
  static const char *min_commit_ts_key;
  static const char *max_commit_ts_key;

  ObLogMinerProgressRange()
  { reset(); }

  void reset()
  {
    min_commit_ts_ = OB_INVALID_TIMESTAMP;
    max_commit_ts_ = OB_INVALID_TIMESTAMP;
  }

  bool is_valid() const
  {
    return min_commit_ts_ != OB_INVALID_TIMESTAMP && max_commit_ts_ != OB_INVALID_TIMESTAMP;
  }

  ObLogMinerProgressRange &operator=(const ObLogMinerProgressRange &that)
  {
    min_commit_ts_ = that.min_commit_ts_;
    max_commit_ts_ = that.max_commit_ts_;
    return *this;
  }

  bool operator==(const ObLogMinerProgressRange &that) const
  {
    return min_commit_ts_ == that.min_commit_ts_ && max_commit_ts_ == that.max_commit_ts_;
  }

  TO_STRING_KV(
    K(min_commit_ts_),
    K(max_commit_ts_)
  );

  NEED_SERIALIZE_AND_DESERIALIZE;

  int64_t min_commit_ts_;
  int64_t max_commit_ts_;
};

}
}
#endif