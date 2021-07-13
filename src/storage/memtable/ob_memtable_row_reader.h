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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_ROW_READER_H_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_ROW_READER_H_
#include "common/object/ob_object.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/ob_nop_bitmap.h"

namespace oceanbase {
namespace memtable {

class ObMemtableRowReader {
public:
  ObMemtableRowReader();
  ~ObMemtableRowReader()
  {}
  int set_buf(const char* buf, int64_t buf_size);
  void reset();
  int get_memtable_row(bool& row_empty, const share::schema::ColumnMap& column_index,
      const storage::ObColDescIArray& columns, storage::ObStoreRow& row, memtable::ObNopBitMap& bitmap,
      int64_t& filled_column_count, bool& has_null);
  int get_memtable_sparse_row(const share::schema::ColumnMap& column_index,
      ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, storage::ObStoreRow& row, bool& loop_flag);
  TO_STRING_KV(K_(buf), K_(pos));

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableRowReader);
  int parse_no_meta(storage::ObStoreRow& row, bool& has_null, bool& row_empty, int64_t& filled_column_count);
  int parse_with_meta(storage::ObStoreRow& row, bool& row_empty, bool& loop_flag);
  inline int read_oracle_timestamp(
      const common::ObObjType obj_type, const uint8_t meta_attr, const common::ObOTimestampMetaAttrType otmat);
  int read_interval_ym();
  int read_interval_ds();
  int read_urowid();

  template <class T>
  const T* read();

private:
  const char* buf_;
  int64_t buf_size_;
  int64_t pos_;
  uint64_t column_id_;
  common::ObObj obj_;
};

class ObMemtableIterRowReader {
public:
  ObMemtableIterRowReader();
  ~ObMemtableIterRowReader();
  int init(common::ObArenaAllocator* allocator, const share::schema::ColumnMap* cols_map, ObNopBitMap* bitmap,
      const storage::ObColDescArray& columns);
  int get_memtable_row(storage::ObStoreRow& row);
  int set_buf(const char* buf, int64_t buf_size);
  void reset();
  void destory();
  bool is_iter_end();
  int set_nop_pos(storage::ObStoreRow& row);

private:
  bool is_inited_;
  bool loop_flag_;
  bool row_empty_;
  bool has_null_;
  int64_t column_cnt_;
  int64_t filled_column_count_;
  ObMemtableRowReader reader_;
  const share::schema::ColumnMap* cols_map_;
  ObNopBitMap* bitmap_;                           // for flat row
  const storage::ObColDescArray* columns_ptr_;    // for flat row
  ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>* bit_set_;  // for sparse row
};

}  // end namespace memtable
}  // end namespace oceanbase

#endif
