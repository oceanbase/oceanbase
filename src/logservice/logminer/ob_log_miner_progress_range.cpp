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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_progress_range.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// ObLogMinerProgressRange //////////////////////////////

const char *ObLogMinerProgressRange::min_commit_ts_key = "MIN_COMMIT_TS";
const char *ObLogMinerProgressRange::max_commit_ts_key = "MAX_COMMIT_TS";

int ObLogMinerProgressRange::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n%s=%ld\n",
      min_commit_ts_key, min_commit_ts_, max_commit_ts_key, max_commit_ts_))) {
    LOG_ERROR("failed to serialize progress range into buffer", K(buf_len), K(pos), KPC(this));
  }
  return ret;
}

int ObLogMinerProgressRange::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_line(min_commit_ts_key, buf, data_len, pos, min_commit_ts_))) {
    LOG_ERROR("parse line for min_commit_ts failed", K(min_commit_ts_key), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(max_commit_ts_key, buf, data_len, pos, max_commit_ts_))) {
    LOG_ERROR("parse line for max_commit_ts failed", K(max_commit_ts_key), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObLogMinerProgressRange::get_serialize_size() const
{
  int64_t size = 0;
  const int64_t max_digit_size = 30;
  char digit_str[max_digit_size];
  size += snprintf(digit_str, max_digit_size, "%ld", min_commit_ts_);
  size += snprintf(digit_str, max_digit_size, "%ld", max_commit_ts_);
  // min_commit_ts_key + max_commit_ts_key + '=' + '\n' + '=' + '\n'
  size += strlen(min_commit_ts_key) + strlen(max_commit_ts_key) + 4;
  return size;
}

}
}