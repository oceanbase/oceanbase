/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_TABLE_STAT_
#define OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_TABLE_STAT_

#include "share/cache/ob_kvcache_struct.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"

namespace oceanbase
{
namespace share
{

class ObOptCatalogTableStatBuilder;
class ObOptCatalogTableStat : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);

public:
  struct Key : public common::ObIKVCacheKey
  {
    Key();
    uint64_t get_tenant_id() const;
    uint64_t hash() const;
    virtual bool operator==(const common::ObIKVCacheKey &other) const override;
    bool is_valid() const;
    int64_t size() const;
    int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const;
    int assign(const Key &other);

    TO_STRING_KV(K(tenant_id_),
                 K(catalog_id_),
                 K(database_name_),
                 K(table_name_),
                 K(partition_value_));

    uint64_t tenant_id_;
    uint64_t catalog_id_;
    common::ObString database_name_;
    common::ObString table_name_;
    common::ObString partition_value_;
  };

  virtual int64_t size() const override;

  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        common::ObIKVCacheValue *&value) const override;

  TO_STRING_KV(K(tenant_id_),
               K(catalog_id_),
               K(schema_version_),
               K(database_name_),
               K(table_name_),
               K(partition_value_),
               K(row_count_),
               K(file_num_),
               K(data_size_),
               K(last_analyzed_),
               K(sample_size_),
               K(avg_row_len_));

public:
  ObOptCatalogTableStat();

  ~ObOptCatalogTableStat();

  // Getters
  uint64_t get_tenant_id() const;
  uint64_t get_catalog_id() const;
  int64_t get_schema_version() const;
  const common::ObString &get_database_name() const;
  const common::ObString &get_table_name() const;
  const common::ObString &get_partition_value() const;
  int64_t get_row_count() const;
  int64_t get_file_num() const;
  int64_t get_data_size() const;
  int64_t get_last_analyzed() const;
  int64_t get_sample_size() const;
  int64_t get_avg_row_len() const;
  bool is_global_table_stat() const;
  bool is_partition_table_stat() const;

  // Setters
  void set_tenant_id(uint64_t tenant_id);
  void set_catalog_id(uint64_t catalog_id);
  void set_schema_version(int64_t schema_version);
  void set_row_count(int64_t row_count);
  void set_file_num(int64_t file_num);
  void set_data_size(int64_t data_size);
  void set_last_analyzed(int64_t last_analyzed);
  void set_sample_size(int64_t sample_size);
  void set_avg_row_len(int64_t avg_row_len);
  void set_table_name(const common::ObString &table_name);
  void set_database_name(const common::ObString &database_name);
  void set_partition_value(const common::ObString &partition_value);
  void set_partition_num(int64_t partition_num);
  // Mutable getters for string fields (needed for deep copy operations)
  common::ObString &get_database_name();
  common::ObString &get_table_name();
  common::ObString &get_partition_value();
  int64_t get_partition_num() const;

  // Validation
  bool is_valid() const;

  int assign(const ObOptCatalogTableStat &other);

protected:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  int64_t schema_version_;
  ObString database_name_;
  ObString table_name_;
  ObString partition_value_;
  int64_t row_count_;
  int64_t file_num_;
  int64_t data_size_;
  int64_t partition_num_;
  int64_t last_analyzed_;
  int64_t sample_size_;
  int64_t avg_row_len_;
  friend class ObOptCatalogTableStatBuilder;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_TABLE_STAT_