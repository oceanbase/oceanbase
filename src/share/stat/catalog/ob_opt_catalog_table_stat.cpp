/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/stat/catalog/ob_opt_catalog_table_stat.h"

namespace oceanbase
{
namespace share
{

ObOptCatalogTableStat::Key::Key()
    : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(), table_name_(),
      partition_value_()
{
}

uint64_t ObOptCatalogTableStat::Key::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObOptCatalogTableStat::Key::hash() const
{
  uint64_t hash_val = 0;
  // TODO: case sensitive
  hash_val = murmurhash(reinterpret_cast<const char *>(&tenant_id_), sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(&catalog_id_), sizeof(catalog_id_), hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(database_name_.ptr()),
                        database_name_.length(),
                        hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(table_name_.ptr()),
                        table_name_.length(),
                        hash_val);
  hash_val = murmurhash(reinterpret_cast<const char *>(partition_value_.ptr()),
                        partition_value_.length(),
                        hash_val);
  return hash_val;
}

bool ObOptCatalogTableStat::Key::operator==(const common::ObIKVCacheKey &other) const
{
  const Key &other_key = reinterpret_cast<const Key &>(other);
  return tenant_id_ == other_key.tenant_id_ && catalog_id_ == other_key.catalog_id_
         && database_name_ == other_key.database_name_ && table_name_ == other_key.table_name_
         && partition_value_ == other_key.partition_value_;
}

bool ObOptCatalogTableStat::Key::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && catalog_id_ != OB_INTERNAL_CATALOG_ID
         && !database_name_.empty() && !table_name_.empty();
}

int64_t ObOptCatalogTableStat::Key::size() const
{
  return sizeof(*this) + database_name_.length() + table_name_.length()
         + partition_value_.length();
}

int ObOptCatalogTableStat::Key::deep_copy(char *buf,
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
    } else {
      key = tmp;
    }
  }
  return ret;
}

int ObOptCatalogTableStat::Key::assign(const Key &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  partition_value_ = other.partition_value_;
  return ret;
}

int64_t ObOptCatalogTableStat::size() const
{
  return sizeof(*this) + database_name_.length() + table_name_.length()
         + partition_value_.length();
}

int ObOptCatalogTableStat::deep_copy(char *buf,
                                     const int64_t buf_len,
                                     common::ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
  } else {
    int64_t pos = sizeof(ObOptCatalogTableStat);
    ObOptCatalogTableStat *tmp = new (buf) ObOptCatalogTableStat();
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
    } else {
      value = tmp;
    }
  }
  return ret;
}

ObOptCatalogTableStat::ObOptCatalogTableStat()
    : tenant_id_(0), catalog_id_(0), schema_version_(0), database_name_(), table_name_(),
      partition_value_(), row_count_(0), file_num_(0), data_size_(0), partition_num_(1),
      last_analyzed_(0), sample_size_(0), avg_row_len_(0)
{
}

ObOptCatalogTableStat::~ObOptCatalogTableStat()
{
}

uint64_t ObOptCatalogTableStat::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObOptCatalogTableStat::get_catalog_id() const
{
  return catalog_id_;
}

int64_t ObOptCatalogTableStat::get_schema_version() const
{
  return schema_version_;
}

const common::ObString &ObOptCatalogTableStat::get_database_name() const
{
  return database_name_;
}

const common::ObString &ObOptCatalogTableStat::get_table_name() const
{
  return table_name_;
}

const common::ObString &ObOptCatalogTableStat::get_partition_value() const
{
  return partition_value_;
}

int64_t ObOptCatalogTableStat::get_row_count() const
{
  return row_count_;
}

int64_t ObOptCatalogTableStat::get_file_num() const
{
  return file_num_;
}

int64_t ObOptCatalogTableStat::get_data_size() const
{
  return data_size_;
}

int64_t ObOptCatalogTableStat::get_last_analyzed() const
{
  return last_analyzed_;
}

int64_t ObOptCatalogTableStat::get_sample_size() const
{
  return sample_size_;
}

int64_t ObOptCatalogTableStat::get_avg_row_len() const
{
  return avg_row_len_;
}

bool ObOptCatalogTableStat::is_global_table_stat() const
{
  return partition_value_.empty();
}

bool ObOptCatalogTableStat::is_partition_table_stat() const
{
  return !partition_value_.empty();
}

void ObOptCatalogTableStat::set_tenant_id(uint64_t tenant_id)
{
  tenant_id_ = tenant_id;
}

void ObOptCatalogTableStat::set_catalog_id(uint64_t catalog_id)
{
  catalog_id_ = catalog_id;
}

void ObOptCatalogTableStat::set_schema_version(int64_t schema_version)
{
  schema_version_ = schema_version;
}

void ObOptCatalogTableStat::set_row_count(int64_t row_count)
{
  row_count_ = row_count;
}

void ObOptCatalogTableStat::set_file_num(int64_t file_num)
{
  file_num_ = file_num;
}

void ObOptCatalogTableStat::set_data_size(int64_t data_size)
{
  data_size_ = data_size;
}

void ObOptCatalogTableStat::set_last_analyzed(int64_t last_analyzed)
{
  last_analyzed_ = last_analyzed;
}

void ObOptCatalogTableStat::set_sample_size(int64_t sample_size)
{
  sample_size_ = sample_size;
}

void ObOptCatalogTableStat::set_avg_row_len(int64_t avg_row_len)
{
  avg_row_len_ = avg_row_len;
}

void ObOptCatalogTableStat::set_table_name(const common::ObString &table_name)
{
  table_name_ = table_name;
}

void ObOptCatalogTableStat::set_database_name(const common::ObString &database_name)
{
  database_name_ = database_name;
}

void ObOptCatalogTableStat::set_partition_value(const common::ObString &partition_value)
{
  partition_value_ = partition_value;
}

void ObOptCatalogTableStat::set_partition_num(int64_t partition_num)
{
  partition_num_ = partition_num;
}

common::ObString &ObOptCatalogTableStat::get_database_name()
{
  return database_name_;
}

common::ObString &ObOptCatalogTableStat::get_table_name()
{
  return table_name_;
}

common::ObString &ObOptCatalogTableStat::get_partition_value()
{
  return partition_value_;
}

int64_t ObOptCatalogTableStat::get_partition_num() const
{
  return partition_num_;
}

bool ObOptCatalogTableStat::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && catalog_id_ != OB_INTERNAL_CATALOG_ID
         && !database_name_.empty() && !table_name_.empty();
}

OB_DEF_SERIALIZE(ObOptCatalogTableStat)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              schema_version_,
              row_count_,
              file_num_,
              data_size_,
              last_analyzed_,
              partition_num_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObOptCatalogTableStat)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              len,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              schema_version_,
              row_count_,
              file_num_,
              data_size_,
              last_analyzed_,
              partition_num_);
  return len;
}

OB_DEF_DESERIALIZE(ObOptCatalogTableStat)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              tenant_id_,
              catalog_id_,
              database_name_,
              table_name_,
              partition_value_,
              schema_version_,
              row_count_,
              file_num_,
              data_size_,
              last_analyzed_,
              partition_num_);
  return ret;
}

int ObOptCatalogTableStat::assign(const ObOptCatalogTableStat &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  database_name_ = other.database_name_;
  table_name_ = other.table_name_;
  partition_value_ = other.partition_value_;
  schema_version_ = other.schema_version_;
  row_count_ = other.row_count_;
  file_num_ = other.file_num_;
  data_size_ = other.data_size_;
  partition_num_ = other.partition_num_;
  last_analyzed_ = other.last_analyzed_;
  sample_size_ = other.sample_size_;
  avg_row_len_ = other.avg_row_len_;
  return ret;
}

} // namespace share
} // namespace oceanbase
