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
#include "observer/table_load/backup/v_1_4/ob_table_load_backup_macro_block_meta_v_1_4.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "lib/utility/ob_print_utils.h"


namespace oceanbase
{
namespace observer
{

class ObTableLoadBackupMacroBlockMeta_V_1_4;

struct ObTableLoadBackupColumnIndexItem_V_1_4
{
  OB_INLINE const common::ObObjMeta &get_obj_meta() const { return request_column_type_; }
  TO_STRING_KV(K(request_column_type_), K(store_index_), K(is_column_type_matched_));
  common::ObObjMeta request_column_type_; //请求列的类型
  int16_t store_index_; //request_index-->store_index
  bool    is_column_type_matched_;
};

class ObTableLoadBackupColumnMap_V_1_4
{
public:
  static const int64_t OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT = 640L;
  ObTableLoadBackupColumnMap_V_1_4()
    : request_count_(0),
      store_count_(0),
      rowkey_store_count_(0),
      seq_read_column_count_(0),
      read_full_rowkey_(false),
      is_inited_(false)
  {
    column_indexs_ = reinterpret_cast<ObTableLoadBackupColumnIndexItem_V_1_4 *>(column_indexs_buf_);
  }
  ~ObTableLoadBackupColumnMap_V_1_4() {}
  int init(const ObTableLoadBackupMacroBlockMeta_V_1_4 *meta);
  void reuse();
  bool is_inited() const { return is_inited_; }
  int64_t get_request_count() const { return request_count_; }
  int64_t get_store_count() const { return store_count_; }
  int64_t get_rowkey_store_count() const { return rowkey_store_count_; }
  int64_t get_seq_read_column_count() const { return seq_read_column_count_; }
  const ObTableLoadBackupColumnIndexItem_V_1_4 *get_column_indexs() const { return column_indexs_; }
  bool is_read_full_rowkey() const { return read_full_rowkey_; }
  TO_STRING_KV(K(request_count_), K(store_count_), K(rowkey_store_count_), K(seq_read_column_count_),
               K(ObArrayWrap<ObTableLoadBackupColumnIndexItem_V_1_4>(column_indexs_, request_count_)), K(read_full_rowkey_));
private:
  int64_t request_count_; //请求列数
  int64_t store_count_; //存储列数
  int64_t rowkey_store_count_; //rowkey列数
  int64_t seq_read_column_count_; //请求的列类型与顺序与存储列类型与顺序相同的列数
  ObTableLoadBackupColumnIndexItem_V_1_4 *column_indexs_;
  // 避免调用ObColumnIndexItem的构造函数
  char column_indexs_buf_[sizeof(ObTableLoadBackupColumnIndexItem_V_1_4) * OB_TABLE_LOAD_PRE_ROW_MAX_COLUMNS_COUNT];
  bool read_full_rowkey_;
  bool is_inited_;
};

struct ObTableLoadBackupRowHeaderDummy_V_1_4
{
  int8_t row_flag_;
  int8_t column_index_bytes_;
  int8_t row_dml_;
  int8_t reserved8_;
  int32_t reserved32_;
  TO_STRING_KV(K(row_flag_), K(column_index_bytes_), K(row_dml_), K(reserved8_), K(reserved32_));
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadBackupRowHeaderDummy_V_1_4);
};

class ObTableLoadBackupRowHeader_V_1_4
{
public:
  ObTableLoadBackupRowHeader_V_1_4() { memset(this, 0, sizeof(*this)); }
  bool is_valid() const { return row_flag_ >= 0 && column_index_bytes_ >= 0; }
  int8_t get_row_flag() const { return row_flag_; }
  int8_t get_row_dml() const { return row_dml_; }
  int8_t get_version() const { return version_; }
  int8_t get_column_index_bytes() const { return column_index_bytes_; }
  uint32_t get_modify_count() const
  {
    uint32_t modify_count = 0;
    if (0 == version_) {
      modify_count = 0;
    } else {
      modify_count = modify_count_;
    }
    return modify_count;
  }
  uint32_t get_acc_checksum() const
  {
    uint32_t acc_checksum = 0;
    if (0 == version_) {
      acc_checksum = 0;
    } else {
      acc_checksum = acc_checksum_;
    }
    return acc_checksum;
  }
  void set_row_flag(const int8_t row_flag) { row_flag_ = row_flag; }
  void set_column_index_bytes(const int8_t column_index_bytes) { column_index_bytes_ = column_index_bytes; }
  void set_row_dml(const int8_t row_dml) { row_dml_ = row_dml; }
  void set_version(const int8_t version) { version_ = version; }
  void set_reserved32(const int32_t reserved32) { reserved32_ = reserved32; }
  void set_modify_count(const uint32_t modify_count) { modify_count_ = modify_count; }
  void set_acc_checksum(const uint32_t acc_checksum) { acc_checksum_ = acc_checksum; }
  ObTableLoadBackupRowHeader_V_1_4 &operator=(const ObTableLoadBackupRowHeader_V_1_4 &src)
  {
    row_flag_ = src.row_flag_;
    column_index_bytes_ = src.column_index_bytes_;
    row_dml_ = src.row_dml_;
    version_ = src.version_;
    reserved32_ = src.reserved32_;
    modify_count_ = src.modify_count_;
    acc_checksum_ = src.acc_checksum_;
    return *this;
  }
  TO_STRING_KV(K(row_flag_), K(column_index_bytes_), K(row_dml_), K(version_), K(reserved32_), K(modify_count_), K(acc_checksum_));
private:
  int8_t row_flag_;
  int8_t column_index_bytes_;
  int8_t row_dml_;
  int8_t version_;
  int32_t reserved32_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
};

class ObTableLoadBackupRowReader_V_1_4
{
public:
  struct ObStoreMeta
  {
    ObStoreMeta(): type_(0), attr_(0)
    {
    }
    TO_STRING_KV(K(type_), K(attr_));
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
    ObExtendStoreType = 31
  };
  ObTableLoadBackupRowReader_V_1_4()
    : allocator_("TLD_BRR_V_1_4"),
      pos_(0),
      start_pos_(0),
      row_end_pos_(0),
      buf_(nullptr),
      store_column_indexs_(nullptr),
      row_header_(nullptr),
      is_first_row_(true)
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadBackupRowReader_V_1_4() {}
  void reset();
  int read_compact_rowkey(const common::ObObjMeta *column_types,
                          const int64_t column_count,
                          const char *buf,
                          const int64_t row_end_pos,
                          int64_t &pos,
                          common::ObNewRow &row);
  int read_meta_row(const ObIArray<int64_t> &column_ids,
                    const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                    const char *buf,
                    const int64_t row_end_pos,
                    int64_t pos,
                    common::ObNewRow &row);

private:
  void force_read_meta() { is_first_row_ = true; }
  template<class T>
  const T *read();
  int setup_row(const char *buf, const int64_t row_end_pos, const int64_t pos, const int64_t column_index_count);
  int read_column_no_meta(const common::ObObjMeta &src_meta, common::ObObj &obj);
  template<class T>
  int read_sequence_columns(const ObIArray<int64_t> &column_ids,
                            const T *items,
                            const int64_t column_count,
                            common::ObNewRow &row);
  int read_column(const ObObjMeta &src_meta, ObObj &obj);
  int read_columns(const ObIArray<int64_t> &column_ids,
                   const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                   common::ObNewRow &row);
  int read_columns(const ObIArray<int64_t> &column_ids,
                   const int64_t start_column_index,
                   const bool check_null_value,
                   const bool read_no_meta,
                   const ObTableLoadBackupColumnMap_V_1_4 *column_map,
                   common::ObNewRow &row,
                   bool &has_null_value);
private:
  ObArenaAllocator allocator_;
  int64_t pos_;
  int64_t start_pos_;
  int64_t row_end_pos_;
  const char *buf_;
  const void *store_column_indexs_;
  const ObTableLoadBackupRowHeader_V_1_4 *row_header_;
  bool is_first_row_;
};

} // namespace observer
} // namespace oceanbase
