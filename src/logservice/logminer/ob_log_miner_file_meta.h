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

#ifndef OCEANBASE_LOG_MINER_FILE_META_H_
#define OCEANBASE_LOG_MINER_FILE_META_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_log_miner_progress_range.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerFileMeta
{
public:

  static const char *data_len_key;

  ObLogMinerFileMeta() {reset();}
  ~ObLogMinerFileMeta() {reset();}

  void reset() {
    range_.reset();
    data_length_ = 0;
  }

  bool operator==(const ObLogMinerFileMeta &that) const
  {
    return range_ == that.range_ && data_length_ == that.data_length_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(
    K(range_),
    K(data_length_)
  )

public:
  ObLogMinerProgressRange range_;
  int64_t data_length_;
};

}
}

#endif