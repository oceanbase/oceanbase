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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_READER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_READER_H_
#include <stdint.h>
#include "ob_block_sstable_struct.h"
#include "lib/allocator/page_arena.h"
#include "storage/ob_i_store.h"
#include "common/object/ob_obj_type.h"
#include "common/rowkey/ob_rowkey.h"
#include "share/schema/ob_schema_struct.h"
namespace oceanbase {
namespace common {
class ObNewRow;
class ObObj;
class ObObjMeta;
}  // namespace common
namespace blocksstable {
class ObColumnMap;
class ObColumnIndexItem;
class ObIRowReader {
public:
  ObIRowReader();
  virtual ~ObIRowReader();
  // read rowkey with no meta(just column object array)
  // ( buf + pos ) point to header of column object array;
  virtual int read_compact_rowkey(const common::ObObjMeta* column_types, const int64_t column_count, const char* buf,
      const int64_t row_end_pos, int64_t& pos, common::ObNewRow& row) = 0;
  // read row from flat storage(RowHeader | cells array | column index array)
  // @param (row_buf + pos) point to RowHeader
  // @param row_len is buffer capacity
  // @param column_map use when schema version changed use column map to read row
  // @param [out]row parsed row object.
  // @param out_type indicates the type of ouput row
  virtual int read_row(const char* row_buf, const int64_t row_len, int64_t pos, const ObColumnMap& column_map,
      common::ObIAllocator& allocator, storage::ObStoreRow& row,
      const common::ObRowStoreType out_type = common::FLAT_ROW_STORE) = 0;
  // need to call setup_row first
  virtual int read_column(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, const int64_t col_index,
      common::ObObj& obj) = 0;
  // read all cells
  virtual int read_full_row(const char* row_buf, const int64_t row_len, int64_t pos,
      common::ObObjMeta* column_type_array, common::ObIAllocator& allocator, storage::ObStoreRow& row) = 0;
  virtual int compare_meta_rowkey(const common::ObStoreRowkey& rhs, const ObColumnMap* column_map,
      const int64_t compare_column_count, const char* buf, const int64_t row_end_pos, const int64_t pos,
      int32_t& cmp_result) = 0;
  inline void reuse_allocator()
  {
    allocator_.reuse();
  }
  /*
   * just parse rowkey column if needed.
   */
  // -1 == column_index_count, it means the row format is compact, without row header and column
  // index array
  // 0 == column_index_count, it means ignore column index array
  // row_store_column_count == column_index_count, it means the row is meta row, with row header and
  // column index arrray
  // other value of column_index_count is invalid
  virtual int setup_row(const char* buf, const int64_t row_end_pos, const int64_t pos,
      const int64_t column_index_count = INT_MAX, transaction::ObTransID* trans_id_ptr = nullptr) = 0;
  int get_row_header(const ObRowHeader*& row_header) const;
  int get_pos() const
  {
    return pos_;
  }
  static int cast_obj(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, common::ObObj& obj);
  void reset();
  bool judge_need_setup(const char* buf, const int64_t row_end_pos, const int64_t pos);

protected:
  int read_text_store(const storage::ObStoreMeta& store_meta, common::ObIAllocator& allocator, common::ObObj& obj);
  template <class T>
  static const T* read(const char* row_buf, int64_t& pos);

protected:
  const char* buf_;
  int64_t row_end_pos_;
  int64_t start_pos_;
  int64_t pos_;
  const ObRowHeader* row_header_;
  transaction::ObTransID* trans_id_ptr_;
  const void* store_column_indexs_;
  const uint16_t* column_ids_;
  common::ObArenaAllocator allocator_;
  bool is_setuped_;
};

template <class T>
inline const T* ObIRowReader::read(const char* row_buf, int64_t& pos)
{
  const T* ptr = reinterpret_cast<const T*>(row_buf + pos);
  pos += sizeof(T);
  return ptr;
}

class ObFlatRowReader : public ObIRowReader {
public:
  ObFlatRowReader();
  virtual ~ObFlatRowReader()
  {}

  virtual int setup_row(const char* buf, const int64_t row_end_pos, const int64_t pos,
      const int64_t column_index_count = INT_MAX, transaction::ObTransID* trans_id_ptr = nullptr) override;
  int read_row(const char* row_buf, const int64_t row_len, int64_t pos, const ObColumnMap& column_map,
      common::ObIAllocator& allocator, storage::ObStoreRow& row,
      const common::ObRowStoreType out_type = common::FLAT_ROW_STORE) override;
  int compare_meta_rowkey(const common::ObStoreRowkey& rhs, const ObColumnMap* column_map,
      const int64_t compare_column_count, const char* buf, const int64_t row_end_pos, const int64_t pos,
      int32_t& cmp_result) override;
  // need to call setup_row first
  int read_column(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, const int64_t col_index,
      common::ObObj& obj) override;
  int read_full_row(const char* row_buf, const int64_t row_len, int64_t pos, common::ObObjMeta* column_type_array,
      common::ObIAllocator& allocator, storage::ObStoreRow& row) override;
  // read rowkey with no meta(just column object array)
  // ( buf + pos ) point to header of column object array;
  int read_compact_rowkey(const common::ObObjMeta* column_types, const int64_t column_count, const char* buf,
      const int64_t row_end_pos, int64_t& pos, common::ObNewRow& row) override;
  int read_obj(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, common::ObObj& obj);
  int read_obj_no_meta(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, common::ObObj& obj);

protected:
  OB_INLINE int analyze_row_header(const int64_t column_cnt, transaction::ObTransID* trans_id_ptr);

private:
  int read_flat_row_from_flat_storage(
      const ObColumnMap& column_map, common::ObIAllocator& allocator, storage::ObStoreRow& row);
  int read_sparse_row_from_flat_storage(
      const ObColumnMap& column_map, common::ObIAllocator& allocator, storage::ObStoreRow& row);
  int sequence_read_flat_column(
      const ObColumnMap& column_map, common::ObIAllocator& allocator, storage::ObStoreRow& row);
};

class ObSparseRowReader : public ObIRowReader {
public:
  ObSparseRowReader();
  virtual ~ObSparseRowReader()
  {}
  virtual int setup_row(const char* buf, const int64_t row_end_pos, const int64_t pos,
      const int64_t column_index_count = INT_MAX, transaction::ObTransID* trans_id_ptr = nullptr) override;
  int read_row(const char* row_buf, const int64_t row_len, int64_t pos, const ObColumnMap& column_map,
      common::ObIAllocator& allocator, storage::ObStoreRow& row,
      const common::ObRowStoreType out_type = common::FLAT_ROW_STORE) override;
  int compare_meta_rowkey(const common::ObStoreRowkey& rhs, const ObColumnMap* column_map,
      const int64_t compare_column_count, const char* buf, const int64_t row_end_pos, const int64_t pos,
      int32_t& cmp_result) override;
  int read_column(const common::ObObjMeta& src_meta, common::ObIAllocator& allocator, const int64_t col_index,
      common::ObObj& obj) override;
  int read_full_row(const char* row_buf, const int64_t row_len, int64_t pos, common::ObObjMeta* column_type_array,
      common::ObIAllocator& allocator, storage::ObStoreRow& row) override;
  // read rowkey with no meta(just column object array)
  // ( buf + pos ) point to header of column object array;
  int read_compact_rowkey(const common::ObObjMeta* column_types, const int64_t column_count, const char* buf,
      const int64_t row_end_pos, int64_t& pos, common::ObNewRow& row) override;

protected:
  OB_INLINE int analyze_row_header(transaction::ObTransID* trans_id_ptr);

private:
  int read_flat_row_from_sparse_storage(
      const ObColumnMap& column_map, common::ObIAllocator& allocator, storage::ObStoreRow& row);
  int read_sparse_row_from_sparse_storage(
      const ObColumnMap& column_map, common::ObIAllocator& allocator, storage::ObStoreRow& row);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
