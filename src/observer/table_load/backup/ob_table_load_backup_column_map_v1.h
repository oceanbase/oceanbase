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

class ObColumnIndexItemV1 : public ObIColumnIndexItem
{
public:
  ObColumnIndexItemV1()
    : store_index_(0), is_column_type_matched_(false)
  {}
  virtual ~ObColumnIndexItemV1() = default;
  virtual OB_INLINE common::ObObjMeta get_request_column_type() const override { return request_column_type_; }
  virtual OB_INLINE int16_t get_store_index() const override { return store_index_; }
  virtual OB_INLINE bool get_is_column_type_matched() const override { return is_column_type_matched_; }
  TO_STRING_KV(K_(request_column_type), K_(store_index), K_(is_column_type_matched));
public:
  common::ObObjMeta request_column_type_; //请求列的类型
  int16_t store_index_; //request_index-->store_index
  bool is_column_type_matched_;
};


// 仅用于14x
class ObColumnMapV1 : public ObIColumnMap
{
public:
  static const int64_t OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT = 640L;
  ObColumnMapV1();
  virtual ~ObColumnMapV1() = default;
  int init(const ObMacroBlockMeta *meta);
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
      K_(seq_read_column_count));
private:
  int64_t request_count_; //请求列数
  int64_t store_count_; //存储列数
  int64_t rowkey_store_count_; //rowkey列数
  int64_t seq_read_column_count_; //请求的列类型与顺序与存储列类型与顺序相同的列数
  common::ObArray<ObColumnIndexItemV1> column_indexs_;
  bool is_inited_;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
