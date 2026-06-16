/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/stat/ob_stat_item.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"

namespace oceanbase
{
namespace share
{

// ObOptCatalogColumnStat::Key methods

uint64_t ObOptCatalogColumnStat::Key::hash() const
{
  uint64_t hash_val = 0;
  // TODO: case sensitive
  hash_val
      = murmurhash(reinterpret_cast<const char *>(&tenant_id_), sizeof(tenant_id_), hash_val);
  hash_val
      = murmurhash(reinterpret_cast<const char *>(&catalog_id_), sizeof(catalog_id_), hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(database_name_.ptr()),
                        database_name_.length(),
                        hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(table_name_.ptr()),
                        table_name_.length(),
                        hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(partition_value_.ptr()),
                        partition_value_.length(),
                        hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(column_name_.ptr()),
                        column_name_.length(),
                        hash_val);
  return hash_val;
}

bool ObOptCatalogColumnStat::Key::operator==(const common::ObIKVCacheKey &other) const
{
  bool bret = false;
  const Key &other_key = reinterpret_cast<const Key &>(other);
  if (tenant_id_ == other_key.tenant_id_ && catalog_id_ == other_key.catalog_id_
      && database_name_ == other_key.database_name_ && table_name_ == other_key.table_name_
      && partition_value_ == other_key.partition_value_
      && column_name_ == other_key.column_name_) {
    bret = true;
  }
  return bret;
}

bool ObOptCatalogColumnStat::Key::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && catalog_id_ != OB_INTERNAL_CATALOG_ID
         && !database_name_.empty() && !table_name_.empty() && !column_name_.empty();
}

int64_t ObOptCatalogColumnStat::Key::size() const
{
  return sizeof(*this) + database_name_.length() + table_name_.length()
         + partition_value_.length() + column_name_.length();
}

int ObOptCatalogColumnStat::Key::deep_copy(char *buf,
                                           const int64_t buf_len,
                                           common::ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
  } else {
    int64_t pos = sizeof(Key);
    Key *tmp = new (buf) Key();
    if (OB_FAIL(tmp->assign(*this))) {
      COMMON_LOG(WARN, "failed to assign", K(ret));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                   database_name_,
                                                                   buf,
                                                                   pos,
                                                                   tmp->database_name_))) {
      COMMON_LOG(WARN, "deep copy database name failed.", K(ret), K(database_name_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                   table_name_,
                                                                   buf,
                                                                   pos,
                                                                   tmp->table_name_))) {
      COMMON_LOG(WARN, "deep copy table name failed.", K(ret), K(table_name_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                   partition_value_,
                                                                   buf,
                                                                   pos,
                                                                   tmp->partition_value_))) {
      COMMON_LOG(WARN, "deep copy partition name failed.", K(ret), K(partition_value_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                   column_name_,
                                                                   buf,
                                                                   pos,
                                                                   tmp->column_name_))) {
      COMMON_LOG(WARN, "deep copy column name failed.", K(ret), K(column_name_));
    } else {
      key = tmp;
    }
  }
  return ret;
}

int ObOptCatalogColumnStat::Key::assign(const Key &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  partition_value_ = other.partition_value_;
  column_name_ = other.column_name_;
  return ret;
}

// ObOptCatalogColumnStat methods

ObOptCatalogColumnStat::ObOptCatalogColumnStat(common::ObIAllocator &allocator)
    : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(), table_name_(),
      partition_value_(), column_name_(), num_null_(0), num_not_null_(0), num_distinct_(0),
      avg_length_(0), min_value_(), max_value_(), bitmap_size_(0), bitmap_(nullptr),
      last_analyzed_(0), cs_type_(ObCollationType::CS_TYPE_INVALID),
      total_col_len_(0), allocator_(&allocator)
{
  if (allocator_ != NULL) {
    if (NULL == (bitmap_ = static_cast<char*>(
        allocator_->alloc(ObOptCatalogColumnStat::NUM_BITMAP_BUCKET)))) {
      COMMON_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED,
                     "allocate memory for bitmap_ failed.");
    } else {
      bitmap_size_ = ObOptCatalogColumnStat::NUM_BITMAP_BUCKET;
      MEMSET(bitmap_, 0, bitmap_size_);
    }
  }
}

void ObOptCatalogColumnStat::reset()
{
  tenant_id_ = 0;
  catalog_id_ = OB_INTERNAL_CATALOG_ID;
  database_name_.reset();
  table_name_.reset();
  partition_value_.reset();
  column_name_.reset();
  num_null_ = 0;
  num_not_null_ = 0;
  num_distinct_ = 0;
  avg_length_ = 0;
  min_value_.set_null();
  max_value_.set_null();
  bitmap_ = NULL;
  bitmap_size_ = 0;
  last_analyzed_ = 0;
  cs_type_ = ObCollationType::CS_TYPE_INVALID;
  total_col_len_ = 0;
}

OB_DEF_SERIALIZE(ObOptCatalogColumnStat)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              column_name_,
              num_null_,
              num_not_null_,
              num_distinct_,
              avg_length_,
              min_value_,
              max_value_,
              bitmap_size_,
              last_analyzed_,
              cs_type_,
              total_col_len_);
  if (bitmap_size_ != 0 && buf_len - pos >= bitmap_size_) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(bitmap_), K(bitmap_size_), K(buf_len), K(pos));
    } else {
      MEMCPY(buf + pos, bitmap_, bitmap_size_);
      pos += bitmap_size_;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("llc_bitmap size is too large", K(ret), K(bitmap_size_), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptCatalogColumnStat)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              len,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              column_name_,
              num_null_,
              num_not_null_,
              num_distinct_,
              avg_length_,
              min_value_,
              max_value_,
              bitmap_size_,
              last_analyzed_,
              cs_type_,
              total_col_len_);
  if (bitmap_size_ != 0) {
    len += bitmap_size_;
  }
  return len;
}

OB_DEF_DESERIALIZE(ObOptCatalogColumnStat)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              column_name_,
              num_null_,
              num_not_null_,
              num_distinct_,
              avg_length_,
              min_value_,
              max_value_,
              bitmap_size_,
              last_analyzed_,
              cs_type_,
              total_col_len_);
  if (bitmap_size_ != 0 && data_len - pos >= bitmap_size_) {
    if (OB_ISNULL(bitmap_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(bitmap_), K(bitmap_size_), K(data_len), K(pos));
    } else {
      memcpy(bitmap_, buf + pos, bitmap_size_);
      pos += bitmap_size_;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("llc_bitmap size is too large", K(ret), K(bitmap_size_), K(data_len), K(pos));
  }
  return ret;
}

ObOptCatalogColumnStat *ObOptCatalogColumnStat::malloc_new_column_stat(
    common::ObIAllocator &allocator)
{
  // Aligned with internal table impl (ob_opt_column_stat.cpp:malloc_new_column_stat)
  ObOptCatalogColumnStat *new_col_stat = OB_NEWx(ObOptCatalogColumnStat, (&allocator), allocator);
  if (new_col_stat != NULL) {
    if (OB_ISNULL(new_col_stat->get_llc_bitmap())) {
      new_col_stat->~ObOptCatalogColumnStat();
      allocator.free(new_col_stat);
      new_col_stat = NULL;
    }
  }
  return new_col_stat;
}

int64_t ObOptCatalogColumnStat::size() const
{
  return sizeof(*this) + database_name_.length() + table_name_.length() + partition_value_.length()
         + column_name_.length() + min_value_.get_deep_copy_size() + max_value_.get_deep_copy_size()
         + bitmap_size_;
}

int ObOptCatalogColumnStat::deep_copy(char *buf,
                                      const int64_t buf_len,
                                      ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
  } else {
    int64_t pos = sizeof(ObOptCatalogColumnStat);
    ObOptCatalogColumnStat *tmp = new (buf) ObOptCatalogColumnStat();
    if (OB_FAIL(tmp->assign(*this))) {
      COMMON_LOG(WARN, "failed to assign", K(ret));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                        database_name_,
                                                                        buf,
                                                                        pos,
                                                                        tmp->database_name_))) {
      COMMON_LOG(WARN, "deep copy database name failed.", K(ret), K(database_name_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                        table_name_,
                                                                        buf,
                                                                        pos,
                                                                        tmp->table_name_))) {
      COMMON_LOG(WARN, "deep copy table name failed.", K(ret), K(table_name_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                        partition_value_,
                                                                        buf,
                                                                        pos,
                                                                        tmp->partition_value_))) {
      COMMON_LOG(WARN, "deep copy partition name failed.", K(ret), K(partition_value_));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(buf_len,
                                                                        column_name_,
                                                                        buf,
                                                                        pos,
                                                                        tmp->column_name_))) {
      COMMON_LOG(WARN, "deep copy column name failed.", K(ret), K(column_name_));
    } else if (OB_FAIL(tmp->min_value_.deep_copy(min_value_, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy min value failed.", K(ret), K(min_value_));
    } else if (OB_FAIL(tmp->max_value_.deep_copy(max_value_, buf, buf_len, pos))) {
      COMMON_LOG(WARN, "deep copy max value failed.", K(ret), K(max_value_));
    } else {
      tmp->bitmap_ = buf + pos;
      MEMCPY(tmp->bitmap_, bitmap_, bitmap_size_);
      pos += bitmap_size_;
      value = tmp;
    }
  }
  return ret;
}

int ObOptCatalogColumnStat::assign(const ObOptCatalogColumnStat &other)
{
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
  total_col_len_ = other.total_col_len_;
  return ret;
}

int ObOptCatalogColumnStat::deep_copy(const ObOptCatalogColumnStat &src)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "allocator is not initialized", K(ret));
  } else {
    tenant_id_ = src.tenant_id_;
    catalog_id_ = src.catalog_id_;
    database_name_ = src.database_name_;
    table_name_ = src.table_name_;
    partition_value_ = src.partition_value_;
    column_name_ = src.column_name_;
    num_null_ = src.num_null_;
    num_not_null_ = src.num_not_null_;
    num_distinct_ = src.num_distinct_;
    avg_length_ = src.avg_length_;
    last_analyzed_ = src.last_analyzed_;
    cs_type_ = src.cs_type_;
    bitmap_size_ = src.bitmap_size_;
    total_col_len_ = src.total_col_len_;

    // 深拷贝min/max值
    if (OB_FAIL(ob_write_obj(*allocator_, src.min_value_, min_value_))) {
      LOG_WARN("deep copy min_value_ failed.", K(ret));
    } else if (OB_FAIL(ob_write_obj(*allocator_, src.max_value_, max_value_))) {
      LOG_WARN("deep copy max_value_ failed.", K(ret));
    } else if (src.bitmap_size_ != 0 && src.bitmap_ != NULL) {
      // 深拷贝bitmap
      char *ptr = static_cast<char *>(allocator_->alloc(src.bitmap_size_));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for bitmap_");
      } else {
        MEMCPY(ptr, src.bitmap_, src.bitmap_size_);
        bitmap_ = ptr;
        bitmap_size_ = src.bitmap_size_;
      }
    }
  }
  return ret;
}

int ObOptCatalogColumnStat::deep_copy(const ObOptCatalogColumnStat &src,
                                      char *buf,
                                      const int64_t size,
                                      int64_t &pos)
{
  int ret = OB_SUCCESS;

  tenant_id_ = src.tenant_id_;
  catalog_id_ = src.catalog_id_;
  num_null_ = src.num_null_;
  num_not_null_ = src.num_not_null_;
  num_distinct_ = src.num_distinct_;
  avg_length_ = src.avg_length_;
  last_analyzed_ = src.last_analyzed_;
  cs_type_ = src.cs_type_;
  total_col_len_ = src.total_col_len_;
  bitmap_size_ = src.bitmap_size_;

  if (OB_UNLIKELY(OB_ISNULL(buf) || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(size), K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(size,
                                                                      src.database_name_,
                                                                      buf,
                                                                      pos,
                                                                      database_name_))) {
    LOG_WARN("deep copy database name failed.", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(size,
                                                                      src.table_name_,
                                                                      buf,
                                                                      pos,
                                                                      table_name_))) {
    LOG_WARN("deep copy table name failed.", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(size,
                                                                      src.partition_value_,
                                                                      buf,
                                                                      pos,
                                                                      partition_value_))) {
    LOG_WARN("deep copy partition value failed.", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::deep_copy_string_helper(size,
                                                                      src.column_name_,
                                                                      buf,
                                                                      pos,
                                                                      column_name_))) {
    LOG_WARN("deep copy column name failed.", K(ret));
  } else if (OB_FAIL(min_value_.deep_copy(src.min_value_, buf, size, pos))) {
    LOG_WARN("deep copy min_value_ failed.", K(ret));
  } else if (OB_FAIL(max_value_.deep_copy(src.max_value_, buf, size, pos))) {
    LOG_WARN("deep copy max_value_ failed.", K(ret));
  } else if (pos + src.bitmap_size_ > size) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("bitmap size overflow", K(ret), K(pos), K(src.bitmap_size_), K(size));
  } else {
    bitmap_ = buf + pos;
    MEMCPY(bitmap_, src.bitmap_, src.bitmap_size_);
    pos += bitmap_size_;
  }
  return ret;
}

int ObOptCatalogColumnStat::merge_column_stat(const ObOptCatalogColumnStat &other)
{
  int ret = OB_SUCCESS;

  // 验证key匹配
  if (tenant_id_ != other.tenant_id_ || catalog_id_ != other.catalog_id_
      || database_name_ != other.database_name_ || table_name_ != other.table_name_
      || column_name_ != other.column_name_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
               "the key not match",
               K(tenant_id_),
               K(catalog_id_),
               K(database_name_),
               K(table_name_),
               K(column_name_));
  } else {
    // 合并num_null和num_not_null
    num_null_ += other.num_null_;
    num_not_null_ += other.num_not_null_;
    total_col_len_ += other.total_col_len_;

    // 计算平均长度
    calc_avg_len();

    // 合并min/max值
    if (OB_FAIL(merge_min_max(min_value_, other.min_value_, true))) {
      COMMON_LOG(WARN, "failed to merge min value", K(ret));
    } else if (OB_FAIL(merge_min_max(max_value_, other.max_value_, false))) {
      COMMON_LOG(WARN, "failed to merge max value", K(ret));
    } else if (bitmap_size_ == other.bitmap_size_ && bitmap_size_ > 0) {
      // 合并bitmap（使用LLC合并）
      ObGlobalNdvEval::update_llc(bitmap_, other.bitmap_);
    }
  }

  return ret;
}

int ObOptCatalogColumnStat::merge_min_max(common::ObObj &cur,
                                          const common::ObObj &other,
                                          bool is_cmp_min)
{
  int ret = OB_SUCCESS;
  int cmp = 0;

  if (cur.is_null()) {
    if (OB_ISNULL(allocator_)) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "allocator is not initialized", K(ret));
    } else {
      ret = ob_write_obj(*allocator_, other, cur);
    }
  } else if (!other.is_null()) {
    if (OB_FAIL(other.compare(cur, cmp))) {
      COMMON_LOG(WARN, "failed to compare", K(ret));
    } else if (is_cmp_min) {
      if (cmp < 0) { // other < cur，更新为other
        if (OB_ISNULL(allocator_)) {
          ret = OB_NOT_INIT;
          COMMON_LOG(WARN, "allocator is not initialized", K(ret));
        } else {
          ret = ob_write_obj(*allocator_, other, cur);
        }
      }
    } else {
      if (cmp > 0) { // other > cur，更新为other
        if (OB_ISNULL(allocator_)) {
          ret = OB_NOT_INIT;
          COMMON_LOG(WARN, "allocator is not initialized", K(ret));
        } else {
          ret = ob_write_obj(*allocator_, other, cur);
        }
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase