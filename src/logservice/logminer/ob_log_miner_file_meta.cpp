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

#include "ob_log_miner_file_meta.h"
#include "ob_log_miner_utils.h"
////////////////////////////// ObLogMinerFileMeta //////////////////////////////

namespace oceanbase
{
namespace oblogminer
{

const char *ObLogMinerFileMeta::data_len_key = "DATA_LEN";

int ObLogMinerFileMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_.serialize(buf, buf_len, pos))) {
    LOG_ERROR("failed to serialize range into buf", K(range_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n",
      data_len_key, data_length_))) {
    LOG_ERROR("failed to fill data_len into buf", K(buf_len), K(pos), K(data_length_));
  }
  return ret;
}

int ObLogMinerFileMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_.deserialize(buf, data_len, pos))) {
    LOG_ERROR("failed to deserialize range", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(data_len_key, buf, data_len, pos, data_length_))) {
    LOG_ERROR("failed to deserialize data_length", K(data_len_key), K(data_len), K(pos), K(data_length_));
  }

  return ret;
}

int64_t ObLogMinerFileMeta::get_serialize_size() const
{
  int64_t size = 0;
  const int64_t digit_max_len = 30;
  char digit_str[digit_max_len];
  size += range_.get_serialize_size();
  size += strlen(data_len_key) + 2;
  size += snprintf(digit_str, digit_max_len, "%ld", data_length_);
  return size;
}

}
}
