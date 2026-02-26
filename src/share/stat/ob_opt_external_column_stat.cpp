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

#include "share/stat/ob_opt_external_column_stat.h"

namespace oceanbase {
namespace share {

OB_DEF_SERIALIZE(ObOptExternalColumnStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, column_name_, num_null_,
              num_not_null_, num_distinct_, avg_length_, min_value_, max_value_,
              bitmap_size_, last_analyzed_, cs_type_);
  if (bitmap_size_ != 0 && buf_len - pos >= bitmap_size_) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(bitmap_), K(bitmap_size_),
               K(buf_len), K(pos));
    } else {
      MEMCPY(buf + pos, bitmap_, bitmap_size_);
      pos += bitmap_size_;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bitmap size is too large", K(ret), K(bitmap_size_), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptExternalColumnStat) {
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, len, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, column_name_, num_null_,
              num_not_null_, num_distinct_, avg_length_, min_value_, max_value_,
              bitmap_size_, last_analyzed_, cs_type_);
  if (bitmap_size_ != 0) {
    len += bitmap_size_;
  }
  return len;
}

OB_DEF_DESERIALIZE(ObOptExternalColumnStat) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tenant_id_, catalog_id_, database_name_,
              table_name_, partition_value_, column_name_, num_null_,
              num_not_null_, num_distinct_, avg_length_, min_value_, max_value_,
              bitmap_size_, last_analyzed_, cs_type_);
  if (bitmap_size_ != 0 && data_len - pos >= bitmap_size_) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(bitmap_), K(bitmap_size_),
               K(data_len), K(pos));
    } else {
      memcpy(bitmap_, buf + pos, bitmap_size_);
      pos += bitmap_size_;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bitmap size is too large", K(ret), K(bitmap_size_), K(data_len), K(pos));
  }
  return ret;
}

int64_t ObOptExternalColumnStat::size() const {
  return sizeof(*this) + database_name_.length() + table_name_.length() +
         partition_value_.length() + min_value_.get_deep_copy_size() +
         max_value_.get_deep_copy_size() + bitmap_size_;
}

int ObOptExternalColumnStat::deep_copy(char *buf, const int64_t buf_len,
                                       ObIKVCacheValue *&value) const {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len),
               K(size()));
  } else {
    int64_t pos = sizeof(ObOptExternalColumnStat);
    ObOptExternalColumnStat *tmp = new (buf) ObOptExternalColumnStat();
    if (OB_FAIL(tmp->assign(*this))) {
      COMMON_LOG(WARN, "failed to assign", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
            buf, buf_len, pos, database_name_, tmp->database_name_))) {
      COMMON_LOG(WARN, "deep copy database name failed.", K(ret),
                 K(database_name_));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, buf_len, pos, table_name_, tmp->table_name_))) {
      COMMON_LOG(WARN, "deep copy table name failed.", K(ret), K(table_name_));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, buf_len, pos, partition_value_,
                   tmp->partition_value_))) {
      COMMON_LOG(WARN, "deep copy partition name failed.", K(ret),
                 K(partition_value_));
    } else if (OB_FAIL(
                   tmp->min_value_.deep_copy(min_value_, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy min value failed.", K(ret), K(min_value_));
    } else if (OB_FAIL(
                   tmp->max_value_.deep_copy(max_value_, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy max value failed.", K(ret), K(max_value_));
    } else {
      if (bitmap_size_ == 0) {
        tmp->bitmap_ = NULL;
      } else if (pos + bitmap_size_ > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        COMMON_LOG(WARN, "bitmap size is too large.", K(ret), K(pos), K(buf_len), K(bitmap_size_));
      } else {
        tmp->bitmap_ = buf + pos;
        MEMCPY(tmp->bitmap_, bitmap_, bitmap_size_);
      }
      value = tmp;
    }
  }
  return ret;
}

int ObOptExternalColumnStat::assign(const ObOptExternalColumnStat &other) {
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  partition_value_ = other.partition_value_;
  column_name_ = other.column_name_;
  num_null_ = other.num_null_;
  num_not_null_ = other.num_not_null_;
  num_distinct_ = other.num_distinct_;
  avg_length_ = other.avg_length_;
  min_value_ = other.min_value_;
  max_value_ = other.max_value_;
  bitmap_ = other.bitmap_;
  bitmap_size_ = other.bitmap_size_;
  last_analyzed_ = other.last_analyzed_;
  cs_type_ = other.cs_type_;
  return ret;
}

} // namespace share
} // namespace oceanbase