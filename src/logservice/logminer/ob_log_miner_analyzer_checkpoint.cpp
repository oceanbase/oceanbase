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

#include "ob_log_miner_analyzer_checkpoint.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// ObLogMinerCheckpoint //////////////////////////////
const char * ObLogMinerCheckpoint::progress_key_str = "PROGRESS";
const char * ObLogMinerCheckpoint::cur_file_id_key_str = "CUR_FILE_ID";
const char * ObLogMinerCheckpoint::max_file_id_key_str = "MAX_FILE_ID";

int ObLogMinerCheckpoint::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n%s=%ld\n%s=%ld\n",
      progress_key_str, progress_, cur_file_id_key_str, cur_file_id_, max_file_id_key_str, max_file_id_))) {
    LOG_ERROR("failed to fill formatted checkpoint into buffer", K(buf_len), K(pos), KPC(this));
  }
  return ret;
}

int ObLogMinerCheckpoint::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(parse_line(progress_key_str, buf, data_len, pos, progress_))) {
    LOG_ERROR("parse progress failed", K(progress_key_str), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(cur_file_id_key_str, buf, data_len, pos, cur_file_id_))) {
    LOG_ERROR("parse cur_file_id failed", K(cur_file_id_key_str), K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(max_file_id_key_str, buf, data_len, pos, max_file_id_))) {
    LOG_ERROR("parse max_file_id failed", K(max_file_id_key_str), K(data_len), K(pos));
  }

  return ret;
}

int64_t ObLogMinerCheckpoint::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t digit_max_len = 30;
  char digit[digit_max_len] = {0};
  len += snprintf(digit, digit_max_len, "%ld", progress_);
  len += snprintf(digit, digit_max_len, "%ld", cur_file_id_);
  len += snprintf(digit, digit_max_len, "%ld", max_file_id_);
  len += strlen(progress_key_str) + 2 + strlen(cur_file_id_key_str) + 2 + strlen(max_file_id_key_str) + 2;
  return len;
}



}
}