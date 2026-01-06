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
#include "observer/table_load/backup/ob_table_load_backup_icolumn_map.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObColumnIndexItemV2 : public ObIColumnIndexItem
{
public:
  ObColumnIndexItemV2()
    : store_index_(0), is_column_type_matched_(false)
  {}
  virtual ~ObColumnIndexItemV2() = default;
  virtual OB_INLINE common::ObObjMeta get_request_column_type() const override { return request_column_type_; }
  virtual OB_INLINE int16_t get_store_index() const override { return store_index_; }
  virtual OB_INLINE bool get_is_column_type_matched() const override { return is_column_type_matched_; }
  TO_STRING_KV(
      K_(column_id),
      K_(request_column_type),
      K_(macro_column_type),
      K_(store_index),
      K_(is_column_type_matched));
public:
  uint64_t column_id_;
  common::ObObjMeta request_column_type_;
  common::ObObjMeta macro_column_type_; // column type stored in macro meta
  int16_t store_index_; //request_index-->store_index
  bool    is_column_type_matched_;
};

// 用于2x和3x
class ObColumnMapV2 : public ObIColumnMap
{
public:
  ObColumnMapV2();
  virtual ~ObColumnMapV2() = default;
  int init(
      const int64_t schema_rowkey_cnt,
      const int64_t store_cnt,
      const common::ObIArray<share::schema::ObColDesc> &out_cols);
  void reset() override;
  OB_INLINE bool is_valid() const override { return is_inited_; }
  OB_INLINE int64_t get_request_count() const override { return request_count_; }
  OB_INLINE int64_t get_store_count() const override { return store_count_; }
  OB_INLINE int64_t get_rowkey_store_count() const override { return rowkey_store_count_; }
  OB_INLINE int64_t get_seq_read_column_count() const override { return seq_read_column_count_; }
  OB_INLINE const ObIColumnIndexItem *get_column_index(const int64_t &idx) const override { return &column_indexs_[idx]; }
  TO_STRING_KV(
      K_(request_count),
      K_(store_count),
      K_(rowkey_store_count),
      K_(seq_read_column_count),
      K_(is_inited));
private:
  int init_column_index(
      const uint64_t col_id,
      const int64_t schema_rowkey_cnt,
      const int64_t store_index,
      const int64_t store_cnt,
      ObColumnIndexItemV2 *column_index);
private:
  int64_t request_count_;
  int64_t store_count_;
  int64_t rowkey_store_count_;
  int64_t seq_read_column_count_; // the count of common prefix between request columns and store columns
  common::ObArray<ObColumnIndexItemV2> column_indexs_;
  bool is_inited_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
