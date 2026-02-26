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

#ifndef _OB_OPT_EXTERNAL_TABLE_STAT_H_
#define _OB_OPT_EXTERNAL_TABLE_STAT_H_

#include "share/cache/ob_kvcache_struct.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase {
namespace share {
class ObOptExternalTableStatBuilder;
class ObOptExternalTableStat : public common::ObIKVCacheValue {
  OB_UNIS_VERSION_V(1);

public:
  struct Key : public common::ObIKVCacheKey {
    Key()
        : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(),
          table_name_(), partition_value_() {}
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
      return hash_val;
    }
    virtual bool operator==(const ObIKVCacheKey &other) const override {
      const Key &other_key = reinterpret_cast<const Key &>(other);
      return tenant_id_ == other_key.tenant_id_ &&
             catalog_id_ == other_key.catalog_id_ &&
             database_name_ == other_key.database_name_ &&
             table_name_ == other_key.table_name_ &&
             partition_value_ == other_key.partition_value_;
    }
    bool is_valid() const {
      return tenant_id_ != OB_INVALID_ID &&
             catalog_id_ != OB_INTERNAL_CATALOG_ID && !database_name_.empty() &&
             !table_name_.empty();
    }
    int64_t size() const {
      return sizeof(*this) + database_name_.length() + table_name_.length() +
             partition_value_.length();
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
        } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                       buf, buf_len, pos, table_name_, tmp->table_name_))) {
          COMMON_LOG(WARN, "deep copy table name failed.", K(ret),
                     K(table_name_));
        } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                       buf, buf_len, pos, partition_value_,
                       tmp->partition_value_))) {
          COMMON_LOG(WARN, "deep copy partition name failed.", K(ret),
                     K(partition_value_));
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
      return ret;
    }

    TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_),
                 K(table_name_), K(partition_value_));

    uint64_t tenant_id_;
    uint64_t catalog_id_;
    ObString database_name_;
    ObString table_name_;
    ObString partition_value_;
  };

  virtual int64_t size() const override {
    return sizeof(*this) + database_name_.length() + table_name_.length() +
           partition_value_.length();
  }

  virtual int deep_copy(char *buf, const int64_t buf_len,
                        ObIKVCacheValue *&value) const override {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len),
                 K(size()));
    } else {
      int64_t pos = sizeof(ObOptExternalTableStat);
      ObOptExternalTableStat *tmp = new (buf) ObOptExternalTableStat();
      if (OB_FAIL(tmp->assign(*this))) {
        COMMON_LOG(WARN, "failed to assign", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
              buf, buf_len, pos, database_name_, tmp->database_name_))) {
        COMMON_LOG(WARN, "deep copy database name failed.", K(ret),
                   K(database_name_));
      } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                     buf, buf_len, pos, table_name_, tmp->table_name_))) {
        COMMON_LOG(WARN, "deep copy table name failed.", K(ret),
                   K(table_name_));
      } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                     buf, buf_len, pos, partition_value_,
                     tmp->partition_value_))) {
        COMMON_LOG(WARN, "deep copy partition name failed.", K(ret),
                   K(partition_value_));
      } else {
        value = tmp;
      }
    }
    return ret;
  }

  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_), K(table_name_),
               K(partition_value_), K(row_count_), K(file_num_), K(data_size_),
               K(last_analyzed_));

public:
  ObOptExternalTableStat()
      : tenant_id_(0), catalog_id_(0), database_name_(), table_name_(),
        partition_value_(), row_count_(0), file_num_(0), data_size_(0),
        partition_num_(1), last_analyzed_(0) {}

  ~ObOptExternalTableStat() {}

  // Getters
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_catalog_id() const { return catalog_id_; }
  const common::ObString &get_database_name() const { return database_name_; }
  const common::ObString &get_table_name() const { return table_name_; }
  const common::ObString &get_partition_value() const {
    return partition_value_;
  }
  int64_t get_row_count() const { return row_count_; }
  int64_t get_file_num() const { return file_num_; }
  int64_t get_data_size() const { return data_size_; }
  int64_t get_last_analyzed() const { return last_analyzed_; }

  // Setters
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_catalog_id(uint64_t catalog_id) { catalog_id_ = catalog_id; }
  void set_row_count(int64_t row_count) { row_count_ = row_count; }
  void set_file_num(int64_t file_num) { file_num_ = file_num; }
  void set_data_size(int64_t data_size) { data_size_ = data_size; }
  void set_last_analyzed(int64_t last_analyzed) {
    last_analyzed_ = last_analyzed;
  }

  // Mutable getters for string fields (needed for deep copy operations)
  common::ObString &get_database_name() { return database_name_; }
  common::ObString &get_table_name() { return table_name_; }
  common::ObString &get_partition_value() { return partition_value_; }
  int64_t get_partition_num() const { return partition_num_; }

  // Validation
  bool is_valid() const { return last_analyzed_ > 0; }

  int assign(const ObOptExternalTableStat &other);

protected:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  ObString database_name_;
  ObString table_name_;
  ObString partition_value_;
  int64_t row_count_;
  int64_t file_num_;
  int64_t data_size_;
  int64_t partition_num_;
  int64_t last_analyzed_;
  friend class ObOptExternalTableStatBuilder;
};

} // namespace share
} // namespace oceanbase

#endif