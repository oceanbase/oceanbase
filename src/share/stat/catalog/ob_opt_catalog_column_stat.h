/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_COLUMN_STAT_
#define OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_COLUMN_STAT_

#include "share/cache/ob_kvcache_struct.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "lib/alloc/alloc_assist.h"

namespace oceanbase
{
namespace share
{

class ObOptCatalogColumnStatBuilder;
class ObOptCatalogColumnStat : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);

public:
  struct Key : public common::ObIKVCacheKey
  {
    Key()
        : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(), table_name_(),
          partition_value_(), column_name_()
    {
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }
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
                 K(partition_value_),
                 K(column_name_));

    uint64_t tenant_id_;
    uint64_t catalog_id_;
    common::ObString database_name_;
    common::ObString table_name_;
    common::ObString partition_value_;
    common::ObString column_name_;
  };

public:
  // Pre-allocate bitmap buffer size (aligned with internal table size)
  static const int64_t BUCKET_BITS = 10; // ln2(1024) = 10
  static const int64_t NUM_BITMAP_BUCKET = (1 << BUCKET_BITS); // 1024

  ObOptCatalogColumnStat()
      : tenant_id_(0), catalog_id_(OB_INTERNAL_CATALOG_ID), database_name_(), table_name_(),
        partition_value_(), column_name_(), num_null_(0), num_not_null_(0), num_distinct_(0),
        avg_length_(0), min_value_(), max_value_(), bitmap_size_(0), bitmap_(nullptr),
        last_analyzed_(0), cs_type_(ObCollationType::CS_TYPE_INVALID),
        total_col_len_(0), allocator_(NULL)
  {
  }

  explicit ObOptCatalogColumnStat(common::ObIAllocator &allocator);

  ~ObOptCatalogColumnStat()
  {
    reset();
  }

  void reset();

  static ObOptCatalogColumnStat *malloc_new_column_stat(common::ObIAllocator &allocator);

  int64_t get_num_rows() const { return num_null_ + num_not_null_; }
  void calc_avg_len() {
    avg_length_ = (get_num_rows() != 0) ? int64_t(round(total_col_len_ * 1.0 / get_num_rows())) : 0;
  }
  void add_num_null(int64_t num_null) { num_null_ += num_null; }
  void add_num_not_null(int64_t num_not_null) { num_not_null_ += num_not_null; }

  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  uint64_t get_catalog_id() const
  {
    return catalog_id_;
  }
  const common::ObString &get_column_name() const
  {
    return column_name_;
  }
  const common::ObString &get_database_name() const
  {
    return database_name_;
  }
  const common::ObString &get_table_name() const
  {
    return table_name_;
  }
  const common::ObString &get_partition_value() const
  {
    return partition_value_;
  }

  common::ObString &get_database_name()
  {
    return database_name_;
  }
  common::ObString &get_table_name()
  {
    return table_name_;
  }
  common::ObString &get_partition_value()
  {
    return partition_value_;
  }
  common::ObString &get_column_name()
  {
    return column_name_;
  }

  int64_t get_num_null() const
  {
    return num_null_;
  }
  int64_t get_num_not_null() const
  {
    return num_not_null_;
  }
  int64_t get_num_distinct() const
  {
    return num_distinct_;
  }
  int64_t get_avg_length() const
  {
    return avg_length_;
  }
  const common::ObObj &get_min_value() const
  {
    return min_value_;
  }
  const common::ObObj &get_max_value() const
  {
    return max_value_;
  }
  int64_t get_llc_bitmap_size() const
  {
    return bitmap_size_;
  }
  const char *get_llc_bitmap() const
  {
    return bitmap_;
  }
  int64_t get_last_analyzed() const
  {
    return last_analyzed_;
  }
  ObCollationType get_collation_type() const
  {
    return cs_type_;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_catalog_id(uint64_t catalog_id)
  {
    catalog_id_ = catalog_id;
  }
  void set_column_name(const common::ObString &column_name)
  {
    column_name_ = column_name;
  }
  void set_database_name(const common::ObString &database_name)
  {
    database_name_ = database_name;
  }
  void set_table_name(const common::ObString &table_name)
  {
    table_name_ = table_name;
  }
  void set_partition_value(const common::ObString &partition_value)
  {
    partition_value_ = partition_value;
  }

  void set_num_null(int64_t num_null)
  {
    num_null_ = num_null;
  }
  void set_num_not_null(int64_t num_not_null)
  {
    num_not_null_ = num_not_null;
  }
  void set_num_distinct(int64_t num_distinct)
  {
    num_distinct_ = num_distinct;
  }
  void set_avg_length(int64_t avg_length)
  {
    avg_length_ = avg_length;
  }
  void set_min_value(const common::ObObj &min_value)
  {
    min_value_ = min_value;
  }
  void set_max_value(const common::ObObj &max_value)
  {
    max_value_ = max_value;
  }
  void set_llc_bitmap(char *bitmap, int64_t bitmap_size)
  {
    bitmap_ = bitmap;
    bitmap_size_ = bitmap_size;
  }
  void set_last_analyzed(int64_t last_analyzed)
  {
    last_analyzed_ = last_analyzed;
  }
  void set_collation_type(ObCollationType cs_type)
  {
    cs_type_ = cs_type;
  }

  virtual int64_t size() const override;

  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        common::ObIKVCacheValue *&value) const override;

  int assign(const ObOptCatalogColumnStat &other);

  int merge_column_stat(const ObOptCatalogColumnStat &other);
  int deep_copy(const ObOptCatalogColumnStat &src);
  int deep_copy(const ObOptCatalogColumnStat &src, char *buf, const int64_t size, int64_t &pos);

  TO_STRING_KV(K(tenant_id_),
               K(catalog_id_),
               K(database_name_),
               K(table_name_),
               K(partition_value_),
               K(column_name_),
               K(num_null_),
               K(num_not_null_),
               K(num_distinct_),
               K(avg_length_),
               K(min_value_),
               K(max_value_),
               K(bitmap_size_),
               K(bitmap_),
               K(last_analyzed_),
               K(cs_type_));

private:
  int merge_min_max(common::ObObj &cur, const common::ObObj &other, bool is_cmp_min);

protected:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString partition_value_;
  common::ObString column_name_;
  int64_t num_null_;
  int64_t num_not_null_;
  int64_t num_distinct_;
  int64_t avg_length_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  int64_t bitmap_size_;
  char *bitmap_;
  int64_t last_analyzed_;
  ObCollationType cs_type_;
  int64_t total_col_len_;
  common::ObIAllocator *allocator_;
  friend class ObOptCatalogColumnStatBuilder;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_CATALOG_OB_OPT_CATALOG_COLUMN_STAT_