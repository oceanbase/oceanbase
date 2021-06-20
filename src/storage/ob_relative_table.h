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

#ifndef OB_RELATIVE_TABLE_H_
#define OB_RELATIVE_TABLE_H_

#include <stdint.h>
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_dml_param.h"
#include "ob_i_table.h"

namespace oceanbase {
namespace storage {
class ObRelativeTable {
  friend class ObRelativeTables;

public:
  ObTablesHandle tables_handle_;

  ObRelativeTable()
      : tables_handle_(), allow_not_ready_(false), schema_(NULL), schema_param_(NULL), use_schema_param_(false)
  {}
  ~ObRelativeTable() = default;
  bool set_schema_param(const share::schema::ObTableSchemaParam* param);
  uint64_t get_table_id() const;
  int64_t get_schema_version() const;
  int get_col_desc(const uint64_t column_id, share::schema::ObColDesc& col_desc) const;
  int get_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc& col_desc) const;
  int get_rowkey_col_id_by_idx(const int64_t idx, uint64_t& col_id) const;
  int get_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc>& column_ids) const;
  int get_column_data_length(const uint64_t column_id, int32_t& len) const;
  int is_rowkey_column_id(const uint64_t column_id, bool& is_rowkey) const;
  int is_column_nullable(const uint64_t column_id, bool& is_nullable) const;
  OB_INLINE bool allow_not_ready() const
  {
    return allow_not_ready_;
  }
  OB_INLINE void allow_not_ready(const bool allow)
  {
    allow_not_ready_ = allow;
  }
  int64_t get_rowkey_column_num() const;
  int64_t get_shadow_rowkey_column_num() const;
  int64_t get_column_count() const;
  int get_fulltext_column(uint64_t& column_id) const;
  int get_index_name(ObString& index_name) const;
  int get_primary_key_name(ObString& pk_name) const;
  bool is_index_table() const;
  bool is_storage_index_table() const;
  bool can_read_index() const;
  bool is_unique_index() const;
  bool is_domain_index() const;
  int check_rowkey_in_column_ids(const common::ObIArray<uint64_t>& column_ids, const bool has_other_column) const;
  int check_column_in_map(const share::schema::ColumnMap& col_map) const;
  int check_index_column_in_map(const share::schema::ColumnMap& col_map, const int64_t data_table_rowkey_cnt) const;
  int build_table_param(const common::ObIArray<uint64_t>& out_col_ids, share::schema::ObTableParam& table_param) const;
  int build_index_row(const common::ObNewRow& table_row, const share::schema::ColumnMap& col_map,
      const bool only_rowkey, common::ObNewRow& index_row, bool& null_idx_val,
      common::ObIArray<share::schema::ObColDesc>* idx_columns);
  OB_INLINE bool use_schema_param() const
  {
    return use_schema_param_;
  }
  OB_INLINE bool is_valid() const
  {
    return use_schema_param_ ? OB_NOT_NULL(schema_param_) && schema_param_->is_valid() : OB_NOT_NULL(schema_);
  }
  OB_INLINE const share::schema::ObTableSchemaParam* get_schema_param() const
  {
    return schema_param_;
  }
  TO_STRING_KV("index_id", NULL == schema_ ? 0 : schema_->get_table_id(), KP(schema_), K_(allow_not_ready),
      K_(use_schema_param), KPC(schema_param_));

private:
  int get_rowkey_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc& col_desc) const;
  // must follow index column order
  int set_index_value(const common::ObNewRow& table_row, const share::schema::ColumnMap& col_map,
      const share::schema::ObColDesc& col_desc, const int64_t rowkey_size, common::ObNewRow& index_row,
      common::ObIArray<share::schema::ObColDesc>* idx_columns);

private:
  bool allow_not_ready_;
  const share::schema::ObTableSchema* schema_;
  const share::schema::ObTableSchemaParam* schema_param_;
  bool use_schema_param_;
};

class ObRelativeTables {
public:
  explicit ObRelativeTables(common::ObIAllocator& allocator)
      : idx_cnt_(0),
        max_col_num_(0),
        data_table_(),
        index_tables_(),
        index_tables_buf_count_(0),
        allocator_(allocator),
        table_param_(NULL),
        use_table_param_(false)
  {}
  ~ObRelativeTables()
  {
    reset();
  }

  void reset()
  {
    if (NULL != index_tables_) {
      for (int64_t i = 0; i < index_tables_buf_count_; ++i) {
        index_tables_[i].~ObRelativeTable();
      }
      allocator_.free(index_tables_);
    }
    idx_cnt_ = 0;
    max_col_num_ = 0;
    index_tables_ = NULL;
    index_tables_buf_count_ = 0;
    table_param_ = NULL;
    use_table_param_ = false;
  }

  int prepare_tables(const uint64_t table_id, const int64_t version, const int64_t read_snapshot,
      const int64_t tenant_schema_version, const common::ObIArray<uint64_t>* upd_col_ids,
      share::schema::ObMultiVersionSchemaService* schema_service, ObPartitionStore& store);
  bool is_valid() const
  {
    return use_table_param_ ? OB_NOT_NULL(table_param_) && table_param_->is_valid()
                            : OB_NOT_NULL(data_table_.schema_) && data_table_.schema_->is_valid();
  }
  bool set_table_param(const share::schema::ObTableDMLParam* param);
  int get_tenant_schema_version(const uint64_t tenant_id, int64_t& schema_version);
  bool use_table_param() const
  {
    return use_table_param_;
  }
  int64_t get_index_tables_buf_count() const
  {
    return index_tables_buf_count_;
  }

private:
  int prepare_data_table(const int64_t read_snapshot, ObPartitionStore& store);
  int prepare_index_tables(
      const int64_t read_snapshot, const common::ObIArray<uint64_t>* upd_col_ids, ObPartitionStore& store);
  int prepare_data_table_from_param(const int64_t read_snapshot, ObPartitionStore& store);
  int prepare_index_tables_from_param(
      const int64_t read_snapshot, const common::ObIArray<uint64_t>* upd_col_ids, ObPartitionStore& store);
  int check_schema_version(share::schema::ObMultiVersionSchemaService& schema_service, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t tenant_schema_version, const int64_t table_version);
  int check_tenant_schema_version(share::schema::ObMultiVersionSchemaService& schema_service, const uint64_t tenant_id,
      const uint64_t table_id, const int64_t tenant_schema_version);

public:
  int64_t idx_cnt_;
  int64_t max_col_num_;
  ObRelativeTable data_table_;
  ObRelativeTable* index_tables_;

private:
  int64_t index_tables_buf_count_;
  common::ObIAllocator& allocator_;

  // schema related
  share::schema::ObSchemaGetterGuard schema_guard_;

  // param related
  const share::schema::ObTableDMLParam* table_param_;
  bool use_table_param_;
};
}  // namespace storage
}  // namespace oceanbase

#endif /* OB_RELATIVE_TABLE_H_ */
