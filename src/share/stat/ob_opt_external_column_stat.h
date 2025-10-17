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

#ifndef _OB_OPT_EXTERNAL_COLUMN_STAT_H_
#define _OB_OPT_EXTERNAL_COLUMN_STAT_H_

#include "share/cache/ob_kvcache_struct.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase {
namespace share {

class ObOptExternalColumnStatBuilder;
class ObOptExternalColumnStat : public common::ObIKVCacheValue {
  OB_UNIS_VERSION_V(1);

public:
  struct Key : public common::ObIKVCacheKey {
    Key()
        : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(),
          table_name_(), partition_value_(), column_name_() {}
    uint64_t get_tenant_id() const { return tenant_id_; }
    uint64_t hash() const {
      uint64_t hash_val = 0;
      //TODO: case sensitive
      hash_val = common::murmurhash(reinterpret_cast<const char *>(&tenant_id_),
                                    sizeof(tenant_id_), hash_val);
      hash_val =
          common::murmurhash(reinterpret_cast<const char *>(&catalog_id_),
                             sizeof(catalog_id_), hash_val);
      hash_val = common::murmurhash(
          reinterpret_cast<const char *>(database_name_.ptr()),
          database_name_.length(), hash_val);
      hash_val =
          common::murmurhash(reinterpret_cast<const char *>(table_name_.ptr()),
                             table_name_.length(), hash_val);
      hash_val = common::murmurhash(
          reinterpret_cast<const char *>(partition_value_.ptr()),
          partition_value_.length(), hash_val);
      hash_val =
          common::murmurhash(reinterpret_cast<const char *>(column_name_.ptr()),
                             column_name_.length(), hash_val);
      return hash_val;
    }
    virtual bool operator==(const ObIKVCacheKey &other) const override {
      bool ret = false;
      const Key &other_key = reinterpret_cast<const Key &>(other);
      if (tenant_id_ == other_key.tenant_id_ &&
          catalog_id_ == other_key.catalog_id_ &&
          database_name_ == other_key.database_name_ &&
          table_name_ == other_key.table_name_ &&
          partition_value_ == other_key.partition_value_ &&
          column_name_ == other_key.column_name_) {
        ret = true;
      }
      return ret;
    }
    bool is_valid() const {
      return tenant_id_ != OB_INVALID_ID &&
             catalog_id_ != OB_INTERNAL_CATALOG_ID && !database_name_.empty() &&
             !table_name_.empty() && !column_name_.empty();
    }
    int64_t size() const {
      return sizeof(*this) + database_name_.length() + table_name_.length() +
             partition_value_.length() + column_name_.length();
    }
    int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(buf) || buf_len < size()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len),
                   K(size()));
      } else {
        int64_t pos = sizeof(Key);
        Key *tmp = new (buf) Key();
        if (OB_FAIL(tmp->assign(*this))) {
          COMMON_LOG(WARN, "failed to assign", K(ret));
        } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                buf, buf_len, pos, database_name_, tmp->database_name_))) {
          COMMON_LOG(WARN, "deep copy database name failed.", K(ret),
                     K(database_name_));
        } else if (OB_FAIL(common::ObDbmsStatsUtils::deep_copy_string(
                       buf, buf_len, pos, table_name_, tmp->table_name_))) {
          COMMON_LOG(WARN, "deep copy table name failed.", K(ret),
                     K(table_name_));
        } else if (OB_FAIL(common::ObDbmsStatsUtils::deep_copy_string(
                       buf, buf_len, pos, partition_value_,
                       tmp->partition_value_))) {
          COMMON_LOG(WARN, "deep copy partition name failed.", K(ret),
                     K(partition_value_));
        } else if (OB_FAIL(common::ObDbmsStatsUtils::deep_copy_string(
                       buf, buf_len, pos, column_name_, tmp->column_name_))) {
          COMMON_LOG(WARN, "deep copy column name failed.", K(ret),
                     K(column_name_));
        } else {
          key = tmp;
        }
      }
      return ret;
    }

    int assign(const Key &other) {
      int ret = OB_SUCCESS;
      tenant_id_ = other.tenant_id_;
      catalog_id_ = other.catalog_id_;
      database_name_ = other.database_name_;
      table_name_ = other.table_name_;
      partition_value_ = other.partition_value_;
      column_name_ = other.column_name_;
      return ret;
    }

    TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_),
                 K(table_name_), K(partition_value_), K(column_name_));

    uint64_t tenant_id_;
    uint64_t catalog_id_;
    ObString database_name_;
    ObString table_name_;
    ObString partition_value_;
    ObString column_name_;
  };

public:
  ObOptExternalColumnStat()
      : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(),
        table_name_(), partition_value_(), column_name_(), num_null_(0),
        num_not_null_(0), num_distinct_(0), avg_length_(0), min_value_(),
        max_value_(), bitmap_size_(0), bitmap_(nullptr), last_analyzed_(0),
        cs_type_(common::ObCollationType::CS_TYPE_INVALID) {}

  ~ObOptExternalColumnStat() {}

  int64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_catalog_id() const { return catalog_id_; }
  const ObString &get_column_name() const { return column_name_; }
  const ObString &get_database_name() const { return database_name_; }
  const ObString &get_table_name() const { return table_name_; }
  const ObString &get_partition_value() const { return partition_value_; }

  ObString &get_database_name() { return database_name_; }
  ObString &get_table_name() { return table_name_; }
  ObString &get_partition_value() { return partition_value_; }
  ObString &get_column_name() { return column_name_; }

  int64_t get_num_null() const { return num_null_; }
  int64_t get_num_not_null() const { return num_not_null_; }
  int64_t get_num_distinct() const { return num_distinct_; }
  int64_t get_avg_length() const { return avg_length_; }
  const common::ObObj &get_min_value() const { return min_value_; }
  const common::ObObj &get_max_value() const { return max_value_; }
  int64_t get_bitmap_size() const { return bitmap_size_; }
  const char *get_bitmap() const { return bitmap_; }
  int64_t get_last_analyzed() const { return last_analyzed_; }
  common::ObCollationType get_collation_type() const { return cs_type_; }

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_catalog_id(uint64_t catalog_id) { catalog_id_ = catalog_id; }
  void set_column_name(const ObString &column_name) {
    column_name_ = column_name;
  }
  void set_database_name(const ObString &database_name) {
    database_name_ = database_name;
  }
  void set_table_name(const ObString &table_name) { table_name_ = table_name; }
  void set_partition_value(const ObString &partition_value) {
    partition_value_ = partition_value;
  }

  void set_num_null(int64_t num_null) { num_null_ = num_null; }
  void set_num_not_null(int64_t num_not_null) { num_not_null_ = num_not_null; }
  void set_num_distinct(int64_t num_distinct) { num_distinct_ = num_distinct; }
  void set_avg_length(int64_t avg_length) { avg_length_ = avg_length; }
  void set_min_value(const common::ObObj &min_value) { min_value_ = min_value; }
  void set_max_value(const common::ObObj &max_value) { max_value_ = max_value; }
  void set_bitmap(char *bitmap, int64_t bitmap_size) {
    bitmap_ = bitmap;
    bitmap_size_ = bitmap_size;
  }
  void set_last_analyzed(int64_t last_analyzed) {
    last_analyzed_ = last_analyzed;
  }
  void set_collation_type(common::ObCollationType cs_type) {
    cs_type_ = cs_type;
  }

  virtual int64_t size() const override;

  virtual int deep_copy(char *buf, const int64_t buf_len,
                        ObIKVCacheValue *&value) const override;

  int assign(const ObOptExternalColumnStat &other);

  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_), K(table_name_),
               K(partition_value_), K(column_name_), K(num_null_),
               K(num_not_null_), K(num_distinct_), K(avg_length_),
               K(min_value_), K(max_value_), K(bitmap_size_), K(bitmap_),
               K(last_analyzed_), K(cs_type_));

protected:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString database_name_;
  ObString table_name_;
  ObString partition_value_;
  ObString column_name_;
  int64_t num_null_;
  int64_t num_not_null_;
  int64_t num_distinct_;
  int64_t avg_length_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  int64_t bitmap_size_;
  char *bitmap_;
  int64_t last_analyzed_;
  common::ObCollationType cs_type_;
  friend class ObOptExternalColumnStatBuilder;
};

} // namespace share
} // namespace oceanbase
#endif