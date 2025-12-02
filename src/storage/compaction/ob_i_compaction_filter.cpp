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

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "ob_i_compaction_filter.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;

namespace compaction
{

const char *ObICompactionFilter::ObFilterRetStr[] =
{
  "NOT_CHANGE",
  "REMOVE",
};

bool ObICompactionFilter::is_valid_filter_ret(const ObFilterRet filter_ret)
{
  return filter_ret >= FILTER_RET_NOT_CHANGE && filter_ret < FILTER_RET_MAX;
}

void ObICompactionFilter::ObFilterStatistics::add(const ObFilterStatistics &other)
{
  for (int i = 0; i < FILTER_RET_MAX; ++i) {
    row_cnt_[i] += other.row_cnt_[i];
  }
}

void ObICompactionFilter::ObFilterStatistics::inc(ObFilterRet filter_ret)
{
  if (OB_LIKELY(is_valid_filter_ret(filter_ret))) {
    row_cnt_[filter_ret]++;
  }
}

void ObICompactionFilter::ObFilterStatistics::reset()
{
  MEMSET(row_cnt_, 0, sizeof(row_cnt_));
}

const char *ObICompactionFilter::get_filter_ret_str(const int64_t idx)
{
  STATIC_ASSERT(static_cast<int64_t>(FILTER_RET_MAX) == ARRAYSIZEOF(ObFilterRetStr), "filter ret string is mismatch");
  const char * ret_str = nullptr;
  if (idx < 0 || idx >= FILTER_RET_MAX) {
    ret_str = "invalid_ret";
  } else {
    ret_str = ObFilterRetStr[idx];
  }
  return ret_str;
}

const char *ObICompactionFilter::ObFilterTypeStr[] =
{
  "TX_DATA_MINOR",
  "MDS_MINOR_FILTER_DATA",
  "MDS_MINOR_CROSS_LS",
  "MDS_IN_MEDIUM_INFO",
  "MEMBER_TABLE_MINOR",
  "ROWSCN_FILTER",
  "MLOG_PURGE_FILTER",
  "FILTER_TYPE_MAX"
};

const char *ObICompactionFilter::get_filter_type_str(const int64_t idx)
{
  STATIC_ASSERT(static_cast<int64_t>(FILTER_TYPE_MAX + 1) == ARRAYSIZEOF(ObFilterTypeStr), "filter type string is mismatch");
  const char * ret_str = nullptr;
  if (idx < 0 || idx >= FILTER_TYPE_MAX) {
    ret_str = "invalid_type";
  } else {
    ret_str = ObFilterTypeStr[idx];
  }
  return ret_str;
}

int64_t ObICompactionFilter::ObFilterStatistics::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    gene_info(buf, buf_len, pos);
  }
  return pos;
}

void ObICompactionFilter::ObFilterStatistics::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || pos >= buf_len) {
  } else {
    J_NAME("stats:");
    J_OBJ_START();
    for (int i = 0; i < FILTER_RET_MAX; ++i) {
      if (i > 0) {
        J_COMMA();
      }
      J_OBJ_START();
      J_KV(get_filter_ret_str(i), row_cnt_[i]);
      J_OBJ_END();
    }
    J_OBJ_END();
  }
}

} // namespace compaction
} // namespace oceanbase
