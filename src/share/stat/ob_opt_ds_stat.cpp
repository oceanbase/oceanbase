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

#define USING_LOG_PREFIX SQL_OPT

#include "share/stat/ob_opt_ds_stat.h"

namespace oceanbase {
namespace common {
using namespace sql;

int ObOptDSStat::assign(const ObOptDSStat& other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  table_id_ = other.table_id_;
  partition_hash_ = other.partition_hash_;
  ds_level_ = other.ds_level_;
  dml_cnt_ = other.dml_cnt_;
  expression_hash_ = other.expression_hash_;
  rowcount_ = other.rowcount_;
  micro_block_num_ = other.micro_block_num_;
  macro_block_num_ = other.macro_block_num_;
  sample_block_ratio_ = other.sample_block_ratio_;
  ds_degree_ = other.ds_degree_;
  stat_expired_time_ = other.stat_expired_time_;
  if (OB_FAIL(col_stats_.assign(other.col_stats_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

void ObOptDSStat::init(const ObOptDSStat::Key &key)
{
  tenant_id_ = key.tenant_id_;
  table_id_ = key.table_id_;
  partition_hash_= key.partition_hash_;
  ds_level_ = key.ds_level_;
  expression_hash_ = key.expression_hash_;
}

int64_t ObOptDSStat::size() const
{
  int64_t base_size = sizeof(ObOptDSStat);
  for (int64_t i = 0; i < col_stats_.count(); ++i) {
    base_size += sizeof(ObOptDSColStat);
  }
  return base_size;
}

int ObOptDSStat::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
  } else {
    ObOptDSStat *stat = new (buf) ObOptDSStat();
    int64_t pos = sizeof(*this);
    if (OB_FAIL(stat->deep_copy(*this, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy ds stat failed.", K(ret));
    } else {
      value = stat;
    }
  }
  return ret;
}

int ObOptDSStat::deep_copy(const ObOptDSStat &src, char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  tenant_id_ = src.tenant_id_;
  table_id_ = src.table_id_;
  partition_hash_ = src.partition_hash_;
  ds_level_ = src.ds_level_;
  dml_cnt_ = src.dml_cnt_;
  expression_hash_ = src.expression_hash_;
  rowcount_ = src.rowcount_;
  macro_block_num_ = src.macro_block_num_;
  micro_block_num_ = src.micro_block_num_;
  sample_block_ratio_ = src.sample_block_ratio_;
  ds_degree_ = src.ds_degree_;
  stat_expired_time_ = src.stat_expired_time_;
  if (!src.col_stats_.empty()) {
    if (OB_UNLIKELY(src.col_stats_.count() * sizeof(ObOptDSColStat) + pos > buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer size is not enough", K(ret), K(src.col_stats_.count() * sizeof(ObOptDSColStat)),
                                            K(pos), K(buf_len));
    } else {
      ObOptDSColStat *col_stats= new (buf + pos) ObOptDSColStat[src.col_stats_.count()];
      col_stats_ = ObArrayWrap<ObOptDSColStat>(col_stats, src.col_stats_.count());
      pos += sizeof(ObOptDSColStat) * src.col_stats_.count();
      for (int64_t i = 0; i < src.col_stats_.count(); ++i) {
        col_stats_.at(i) = src.col_stats_.at(i);
      }
    }
  }
  return ret;
}

int ObOptDSStat::deep_copy(ObIAllocator &allocate, ObOptDSStat *&ds_stat) const
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = size();
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char*>(allocate.alloc(buf_len)))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
  } else {
    ObOptDSStat *stat = new (buf) ObOptDSStat();
    int64_t pos = sizeof(*this);
    if (OB_FAIL(stat->deep_copy(*this, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy ds stat failed.", K(ret));
    } else {
      ds_stat = stat;
    }
  }
  return ret;
}

int ObOptDSStat::prepare_allocate_col_stats(ObIAllocator &allocator, int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(col_stats_.allocate_array(allocator, row_cnt))) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  }
  return ret;
}

}
}