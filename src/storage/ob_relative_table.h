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

#include "lib/container/ob_iarray.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
class ObString;
}
namespace share
{
namespace schema
{
class ObTableDMLParam;
class ObTableSchema;
class ObTableSchemaParam;
struct ObColDesc;
class ColumnMap;
}
}
namespace storage
{
class ObRelativeTable final
{
public:
  ObTabletTableIterator tablet_iter_;

  ObRelativeTable(): tablet_iter_(), allow_not_ready_(false), schema_param_(NULL),
      tablet_id_(), is_inited_(false)
  {}
  ~ObRelativeTable();

  bool is_valid() const;

  void destroy();
  int init(
      const share::schema::ObTableSchemaParam *param,
      const ObTabletID &tablet_id,
      const bool allow_not_ready = false);
  uint64_t get_table_id() const;
  const ObTabletID& get_tablet_id() const;
  int64_t get_schema_version() const;
  int get_col_desc(const uint64_t column_id, share::schema::ObColDesc &col_desc) const;
  int get_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc &col_desc) const;
  int get_rowkey_col_id_by_idx(const int64_t idx, uint64_t &col_id) const;
  int get_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const;
  int get_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  int get_column_data_length(const uint64_t column_id, int32_t &len) const;
  int is_rowkey_column_id(const uint64_t column_id, bool &is_rowkey) const;
  int is_column_nullable_for_write(const uint64_t column_id, bool &is_nullable_for_write) const;
  int is_column_nullable_for_read(const uint64_t column_id, bool &is_nullable_for_read) const;
  int is_nop_default_value(const uint64_t column_id, bool &is_nop) const;
  int has_udf_column(bool &has_udf) const;
  int is_hidden_column(const uint64_t column_id, bool &is_hidden) const;
  int is_gen_column(const uint64_t column_id, bool &is_gen_col) const;
  OB_INLINE bool allow_not_ready() const { return allow_not_ready_; }
  int64_t get_rowkey_column_num() const;
  int64_t get_shadow_rowkey_column_num() const;
  int64_t get_column_count() const;
  int get_fulltext_column(uint64_t &column_id) const;
  int get_index_name(common::ObString &index_name) const;
  int get_primary_key_name(common::ObString &pk_name) const;
  int get_spatial_geo_col_id(uint64_t &column_id) const;
  int get_spatial_cellid_col_id(uint64_t &column_id) const;
  int get_spatial_mbr_col_id(uint64_t &column_id) const;
  bool is_index_table() const;
  bool is_lob_meta_table() const;
  bool is_storage_index_table() const;
  bool can_read_index() const;
  bool is_unique_index() const;
  bool is_domain_index() const;
  bool is_spatial_index() const;
  int check_rowkey_in_column_ids(const common::ObIArray<uint64_t> &column_ids,
                                  const bool has_other_column) const;
  int build_index_row(const common::ObNewRow &table_row,
                      const share::schema::ColumnMap &col_map,
                      const bool only_rowkey,
                      common::ObNewRow &index_row,
                      bool &null_idx_val,
                      common::ObIArray<share::schema::ObColDesc> *idx_columns);
  const share::schema::ObTableSchemaParam *get_schema_param() const { return schema_param_;}

  DECLARE_TO_STRING;

private:
  int get_rowkey_col_desc_by_idx(const int64_t idx, share::schema::ObColDesc &col_desc) const;
  // must follow index column order
  int set_index_value(const common::ObNewRow &table_row,
                      const share::schema::ColumnMap &col_map,
                      const share::schema::ObColDesc &col_desc,
                      const int64_t rowkey_size,
                      common::ObNewRow &index_row,
                      common::ObIArray<share::schema::ObColDesc> *idx_columns);

private:
  bool allow_not_ready_;
  const share::schema::ObTableSchemaParam *schema_param_;
  ObTabletID tablet_id_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObRelativeTable);
};

}//namespace oceanbase
}//namespace storage

#endif /* OB_RELATIVE_TABLE_H_ */
