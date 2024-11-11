/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_block_sstable_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

struct ObColumnIndexItem
{
  uint64_t column_id_;
  common::ObObjMeta request_column_type_;
  common::ObObjMeta macro_column_type_; // column type stored in macro meta
  int16_t store_index_; //request_index-->store_index
  bool    is_column_type_matched_;
  OB_INLINE const common::ObObjMeta &get_obj_meta() const { return request_column_type_; }
  TO_STRING_KV(
      K_(column_id),
      K_(request_column_type),
      K_(macro_column_type),
      K_(store_index),
      K_(is_column_type_matched));
};

class ObColumnMap
{
public:
  ObColumnMap();
  ~ObColumnMap();
  int init(
      common::ObIAllocator &allocator,
      const int64_t schema_version,
      const int64_t schema_rowkey_cnt,
      const int64_t store_cnt,
      const common::ObIArray<share::schema::ObColDesc> &out_cols);
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE const share::schema::ColumnMap *get_cols_map() const { return cols_id_map_; }
  OB_INLINE int64_t get_request_count() const { return request_count_; }
  OB_INLINE int64_t get_store_count() const { return store_count_; }
  OB_INLINE int64_t get_rowkey_store_count() const { return rowkey_store_count_; }
  OB_INLINE int64_t get_seq_read_column_count() const { return seq_read_column_count_; }
  OB_INLINE const ObColumnIndexItem *get_column_indexs() const { return column_indexs_; }
  OB_INLINE bool is_all_column_matched() const { return is_all_column_matched_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE bool all_columns_exist() const { return !is_output_not_exist_; }
  TO_STRING_KV(
      K_(request_count),
      K_(store_count),
      K_(rowkey_store_count),
      K_(schema_version),
      K_(*cols_id_map),
      K_(seq_read_column_count),
      K_(cur_idx),
      K(common::ObArrayWrap<ObColumnIndexItem>(column_indexs_, seq_read_column_count_)),
      K_(is_all_column_matched),
      K_(create_col_id_map),
      K_(is_output_not_exist),
      K_(is_inited));
private:
  int init_column_index(
      const uint64_t col_id,
      const int64_t schema_rowkey_cnt,
      const int64_t store_index,
      const int64_t store_cnt,
      ObColumnIndexItem *column_index);
private:
  int64_t request_count_;
  int64_t store_count_;
  int64_t rowkey_store_count_;
  int64_t schema_version_;
  const share::schema::ColumnMap *cols_id_map_;
  int64_t seq_read_column_count_; // the count of common prefix between request columns and store columns
  int64_t cur_idx_;
  ObColumnIndexItem *column_indexs_;
  bool is_inited_;
  bool is_all_column_matched_;
  bool create_col_id_map_;
  bool is_output_not_exist_;
  common::ObIAllocator *allocator_;
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
