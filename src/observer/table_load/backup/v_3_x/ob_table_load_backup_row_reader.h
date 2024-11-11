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
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_column_map.h"
#include "storage/ob_i_store.h"
#include "common/object/ob_obj_type.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{
enum ObStoreAttr
{
  STORE_WITHOUT_COLLATION = 0,
  STORE_WITH_COLLATION = 1
};

struct ObStoreMeta
{
  ObStoreMeta(): type_(0), attr_(0) {}
  uint8_t type_: 5;
  uint8_t attr_: 3;
};

enum ObDataStoreType
{
  ObNullStoreType = 0,
  ObIntStoreType = 1,
  ObNumberStoreType = 2,
  ObCharStoreType = 3,
  ObHexStoreType = 4,
  ObFloatStoreType = 5,
  ObDoubleStoreType = 6,
  ObTimestampStoreType = 7,
  ObBlobStoreType = 8,
  ObTextStoreType = 9,
  ObEnumStoreType = 10,
  ObSetStoreType = 11,
  ObBitStoreType = 12,
  ObTimestampTZStoreType = 13,
  ObRawStoreType = 14,
  ObIntervalYMStoreType = 15,
  ObIntervalDSStoreType = 16,
  ObRowIDStoreType = 17,
  ObJsonStoreType = 18,
  ObGeometryStoreType = 19,
  ObExtendStoreType = 31
};

class ObIRowReader
{
public:
  const uint8_t STORE_TEXT_STORE_VERSION = 1;
  ObIRowReader();
  virtual ~ObIRowReader();
  // read row from flat storage(RowHeader | cells array | column index array)
  // @param (row_buf + pos) point to RowHeader
  // @param row_len is buffer capacity
  // @param column_map use when schema version changed use column map to read row
  // @param [out]row parsed row object.
  // @param out_type indicates the type of ouput row
  virtual int read_row(
      const char *row_buf,
      const int64_t row_len,
      int64_t pos,
      const ObColumnMap &column_map,
      ObIAllocator &allocator,
      common::ObNewRow &row) = 0;
  inline void reuse_allocator() { allocator_.reuse(); }
  /*
   * just parse rowkey column if needed.
   */
  // -1 == column_index_count, it means the row format is compact, without row header and column
  // index array
  // 0 == column_index_count, it means ignore column index array
  // row_store_column_count == column_index_count, it means the row is meta row, with row header and
  // column index arrray
  // other value of column_index_count is invalid
  virtual int setup_row(
      const char *buf,
      const int64_t row_end_pos,
      const int64_t pos,
      const int64_t column_index_count = INT_MAX,
      transaction::ObTransID *trans_id_ptr = nullptr) = 0;
  int get_pos() const { return pos_; }
  void reset();
  bool judge_need_setup(
      const char *buf,
      const int64_t row_end_pos,
      const int64_t pos);
protected:
  int read_text_store(
      const ObStoreMeta &store_meta,
      common::ObIAllocator &allocator,
      common::ObObj &obj);
  int read_json_store(
    const ObStoreMeta &store_meta,
    common::ObIAllocator &allocator,
    common::ObObj &obj);
  template<class T>
  static const T *read(const char *row_buf, int64_t &pos);
protected:
  const char *buf_;
  int64_t row_end_pos_;
  int64_t start_pos_;
  int64_t pos_;
  const ObRowHeader *row_header_;
  transaction::ObTransID *trans_id_ptr_;
  const void *store_column_indexs_;
  const uint16_t *column_ids_;
  common::ObArenaAllocator allocator_;
  bool is_setuped_;
};

template<class T>
inline const T *ObIRowReader::read(const char *row_buf, int64_t &pos)
{
  const T *ptr = reinterpret_cast<const T*>(row_buf + pos);
  pos += sizeof(T);
  return ptr;
}

class ObFlatRowReader : public ObIRowReader
{
public:
  ObFlatRowReader();
  virtual ~ObFlatRowReader() {}
  int read_row(
      const char *row_buf,
      const int64_t row_len,
      int64_t pos,
      const ObColumnMap &column_map,
      ObIAllocator &allocator,
      common::ObNewRow &row) override;
private:
  int setup_row(
      const char *buf,
      const int64_t row_end_pos,
      const int64_t pos,
      const int64_t column_index_count = INT_MAX,
      transaction::ObTransID *trans_id_ptr = nullptr) override;
  OB_INLINE int analyze_row_header(
      const int64_t column_cnt,
      transaction::ObTransID *trans_id_ptr);
  int read_flat_row_from_flat_storage(
      const ObColumnMap &column_map,
      ObIAllocator &allocator,
      common::ObNewRow &row);
  int sequence_read_flat_column(
      const ObColumnMap &column_map,
      ObIAllocator &allocator,
      common::ObNewRow &row);
  int read_obj(
      const ObObjMeta &src_meta,
      ObIAllocator &allocator,
      ObObj &obj);
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
