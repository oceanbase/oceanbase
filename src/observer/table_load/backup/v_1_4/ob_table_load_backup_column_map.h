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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_block_sstable_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{

class ObTableLoadBackupColumnIndexItem
{
public:
  ObTableLoadBackupColumnIndexItem()
    : store_index_(0), is_column_type_matched_(false)
  {}
  ~ObTableLoadBackupColumnIndexItem() = default;
  OB_INLINE const common::ObObjMeta &get_obj_meta() const { return request_column_type_; }
  TO_STRING_KV(K_(request_column_type), K_(store_index), K_(is_column_type_matched));
public:
  common::ObObjMeta request_column_type_; //请求列的类型
  int16_t store_index_; //request_index-->store_index
  bool is_column_type_matched_;
};

class ObTableLoadBackupColumnMap
{
public:
  static const int64_t OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT = 640L;
  ObTableLoadBackupColumnMap();
  ~ObTableLoadBackupColumnMap() = default;
  int init(const ObTableLoadBackupMacroBlockMeta *meta);
  void reuse();
  bool is_inited() const { return is_inited_; }
  int64_t get_request_count() const { return request_count_; }
  int64_t get_store_count() const { return store_count_; }
  int64_t get_rowkey_store_count() const { return rowkey_store_count_; }
  int64_t get_seq_read_column_count() const { return seq_read_column_count_; }
  const ObTableLoadBackupColumnIndexItem *get_column_indexs() const { return column_indexs_; }
  bool is_read_full_rowkey() const { return read_full_rowkey_; }
  TO_STRING_KV(
      K_(request_count),
      K_(store_count),
      K_(rowkey_store_count),
      K_(seq_read_column_count),
      K(ObArrayWrap<ObTableLoadBackupColumnIndexItem>(column_indexs_, request_count_)),
      K_(read_full_rowkey));
private:
  int64_t request_count_; //请求列数
  int64_t store_count_; //存储列数
  int64_t rowkey_store_count_; //rowkey列数
  int64_t seq_read_column_count_; //请求的列类型与顺序与存储列类型与顺序相同的列数
  ObTableLoadBackupColumnIndexItem *column_indexs_;
  // 避免调用ObColumnIndexItem的构造函数
  char column_indexs_buf_[sizeof(ObTableLoadBackupColumnIndexItem) * OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT];
  bool read_full_rowkey_;
  bool is_inited_;
};

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
