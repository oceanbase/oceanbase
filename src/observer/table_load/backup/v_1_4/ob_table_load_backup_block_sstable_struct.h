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
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/ob_define.h"
#include "share/schema/ob_table_param.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_1_4
{
class ObTableLoadBackupRowReader;

class ObTableLoadBackupHiddenPK
{
public:
  static const int64_t HIDDEN_PK_COUNT = 3;
  const static uint64_t OB_HIDDEN_PK_INCREMENT_COLUMN_ID = 1;  //hidden pk contain 3 column (seq, cluster_id, partition_id)
  const static uint64_t OB_HIDDEN_PK_CLUSTER_COLUMN_ID = 4;
  const static uint64_t OB_HIDDEN_PK_PARTITION_COLUMN_ID = 5;
  static int64_t get_hidden_pk_count() { return HIDDEN_PK_COUNT; }
};

enum ObTableLoadBackupMacroBlockType
{
  Free = 0,
  SSTableData = 1,
  PartitionMeta = 2,
  // SchemaData = 3,
  // Compressor = 4,
  MacroMeta = 5,
  Reserved = 6,
  MacroBlockSecondIndex = 7,
  SortTempData = 8,
  LobData = 9,
  LobIndex = 10,
  MaxMacroType,
};

class ObSchemaInfo
{
public:
  ObSchemaInfo();
  ~ObSchemaInfo() = default;
  void reset();
  int assign(const ObSchemaInfo &other);
  TO_STRING_KV(K_(column_desc));
public:
  common::ObArray<share::schema::ObColDesc> column_desc_;
};

class ObTableLoadBackupMacroBlockMeta
{
public:
  ObTableLoadBackupMacroBlockMeta()
    : attr_(0),
      data_version_(0),
      column_number_(0),
      rowkey_column_number_(0),
      column_index_scale_(0),
      row_store_type_(0),
      row_count_(0),
      occupy_size_(0),
      data_checksum_(0),
      micro_block_count_(0),
      micro_block_data_offset_(0),
      micro_block_index_offset_(0),
      micro_block_endkey_offset_(0),
      compressor_(nullptr),
      column_id_array_(nullptr),
      column_type_array_(nullptr),
      column_checksum_(nullptr),
      endkey_(nullptr),
      table_id_(0),
      data_seq_(-1),
      schema_version_(0),
      write_seq_(0),
      create_timestamp_(0),
      retire_timestamp_(0),
      bf_build_timestamp_(0),
      empty_read_cnt_(nullptr),
      collation_free_endkey_(nullptr) {}
  ~ObTableLoadBackupMacroBlockMeta() {}
  int deserialize(const char *buf, int64_t data_len, int64_t &pos);
  TO_STRING_KV(K(attr_), K(data_version_), K(column_number_), K(rowkey_column_number_),
               K(column_index_scale_), K(row_store_type_), K(row_count_), K(occupy_size_),
               K(data_checksum_), K(micro_block_count_), K(micro_block_data_offset_),
               K(micro_block_index_offset_), K(micro_block_endkey_offset_), K(OB_P(compressor_)),
               K(ObArrayWrap<uint16_t>(column_id_array_, column_number_)),
               K(ObArrayWrap<ObObjMeta>(column_type_array_, column_number_)),
               K(ObArrayWrap<int64_t>(column_checksum_, column_number_)),
               K(common::ObRowkey(endkey_, rowkey_column_number_)), K(table_id_), K(data_seq_), K(schema_version_),
               K(write_seq_), K(create_timestamp_), K(retire_timestamp_), K(bf_build_timestamp_),
               K(ObArrayWrap<int64_t>(empty_read_cnt_, rowkey_column_number_)),
               K(common::ObRowkey(collation_free_endkey_, rowkey_column_number_)));
public:
  //For compatibility, the variables in this struct MUST NOT be deleted or moved.
  //You should ONLY add variables at the end.
  //Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  //The following variables need to be serialized
  int16_t attr_;       //低8位0,1,2,3,4,5分别表示空闲,sstable数据,tablet元数据,schema,compressor name,宏块元数据;其余位置0；
  union
  {
    uint64_t data_version_;  //sstable宏块：sstable的主版本号(高48位)及，小版本号(低16位)
    int64_t previous_block_index_; // nonsstable: previous_block_index_ link.
  };
  int16_t column_number_;            // column count of this table (size of column_checksum_)
  int16_t rowkey_column_number_;     // rowkey column count of this table
  int16_t column_index_scale_;       // store column index scale percent of column count;
  int16_t row_store_type_;                     // reserve
  int32_t row_count_;                // row count of macro block;
  int32_t occupy_size_;              // data size of macro block;
  int64_t data_checksum_;            // data checksum of macro block
  int32_t micro_block_count_;        // micro block info in ObSSTableMacroBlockHeader
  int32_t micro_block_data_offset_;  // data offset base on macro block header.
  int32_t micro_block_index_offset_; // data_size = index_offset - data_offset
  int32_t micro_block_endkey_offset_; // index_size = endkey_offset - index_offset, endkey_size = occupy_size - endkey_offset
  char    *compressor_;
  uint16_t *column_id_array_;
  common::ObObjMeta *column_type_array_;
  int64_t *column_checksum_;
  common::ObObj   *endkey_;
  uint64_t table_id_;
  int64_t data_seq_;  // sequence in partition meta.
  int64_t schema_version_;

  //The following variables do not need to be serialized
  int32_t write_seq_;       // increment 1 every reuse pass.
  int64_t create_timestamp_;
  int64_t retire_timestamp_;
  int64_t bf_build_timestamp_;
  int64_t *empty_read_cnt_;
  common::ObObj *collation_free_endkey_;
};

struct ObBackupLobScale
{
  static const uint8_t LOB_SCALE_MASK = 0xF;
  enum StorageType
  {
    STORE_IN_ROW = 0,
    STORE_OUT_ROW
  };
  union
  {
    int8_t scale_;
    struct
    {
      uint8_t reserve_: 4;
      uint8_t type_: 4;
    };
  };
  ObBackupLobScale() : scale_(0) {}
  ObBackupLobScale(const ObScale scale) : scale_(static_cast<const int8_t>(scale)) { reserve_ = 0; }
  OB_INLINE void reset() { scale_ = 0; }
  OB_INLINE bool is_valid() { return STORE_IN_ROW == type_ || STORE_OUT_ROW == type_; }
  OB_INLINE void set_in_row() { reserve_ = 0; type_ = STORE_IN_ROW; }
  OB_INLINE void set_out_row() { reserve_ = 0; type_ = STORE_OUT_ROW; }
  OB_INLINE bool is_in_row() const { return type_ == STORE_IN_ROW; }
  OB_INLINE bool is_out_row() const { return type_ == STORE_OUT_ROW; }
  OB_INLINE ObScale get_scale() const { return static_cast<ObScale>(scale_); }
  TO_STRING_KV(K_(scale));
};

struct ObBackupLogicMacroBlockId
{
public:
  static const int64_t LOGIC_BLOCK_ID_VERSION = 1;
  OB_UNIS_VERSION(LOGIC_BLOCK_ID_VERSION);
public:
  static const uint64_t SF_BIT_RESERVE = 16;
  static const uint64_t SF_BIT_MERGE_TYPE = 2;
  static const uint64_t SF_BIT_BLOCK_TYPE = 2;
  static const uint64_t SF_BIT_PARALLEL_IDX = 12;
  static const uint64_t SF_BIT_SEQ = 32;
  static const uint64_t SF_MASK_RESERVE = (0x1UL << SF_BIT_RESERVE) - 1;
  static const uint64_t SF_MASK_MERGE_TYPE = (0x1UL << SF_BIT_MERGE_TYPE) - 1;
  static const uint64_t SF_MASK_BLOCK_TYPE = (0x1UL << SF_BIT_BLOCK_TYPE) - 1;
  static const uint64_t SF_MASK_PARALLEL_IDX = (0x1UL << SF_BIT_PARALLEL_IDX) - 1;
  static const uint64_t SF_MASK_SEQ = (0x1UL << SF_BIT_SEQ) - 1;
  enum BlockType {
    DATA_BLOCK = 0,
    LOB_BLOCK,
  };
  ObBackupLogicMacroBlockId();
  ObBackupLogicMacroBlockId(const int64_t data_seq, const uint64_t data_version);
  int64_t hash() const;
  bool operator ==(const ObBackupLogicMacroBlockId &other) const;
  bool operator !=(const ObBackupLogicMacroBlockId &other) const;
  OB_INLINE bool is_valid() const
  {
    return data_seq_ >= 0 && data_version_ > 0;
  }
  OB_INLINE bool is_lob_block_id() const
  {
    return ((data_seq_ >> (SF_BIT_PARALLEL_IDX + SF_BIT_SEQ)) & SF_MASK_BLOCK_TYPE) == LOB_BLOCK;
  }
  TO_STRING_KV(K_(data_seq), K_(data_version));
public:
  int64_t data_seq_;
  uint64_t data_version_;
};

class ObBackupLobIndex
{
public:
  static const int64_t LOB_INDEX_VERSION = 1;
  OB_UNIS_VERSION(LOB_INDEX_VERSION);
public:
  ObBackupLobIndex();
  ~ObBackupLobIndex() = default;
  bool operator ==(const ObBackupLobIndex &other) const;
  bool operator !=(const ObBackupLobIndex &other) const;
  TO_STRING_KV(
      K_(version),
      K_(reserved),
      K_(logic_macro_id),
      K_(byte_size),
      K_(char_size));
public:
  uint32_t version_;
  uint32_t reserved_;
  ObBackupLogicMacroBlockId logic_macro_id_;
  uint64_t byte_size_;
  uint64_t char_size_;
};

class ObBackupLobData
{
public:
  static const int64_t DIRECT_CNT = 48;
  static const int64_t NDIRECT_CNT = 1;
  static const int64_t TOTAL_INDEX_CNT = DIRECT_CNT + NDIRECT_CNT;
  static const int64_t LOB_DATA_VERSION = 1;
  ObBackupLobData();
  ~ObBackupLobData() = default;
  void reset();
  bool operator ==(const ObBackupLobData &other) const;
  bool operator !=(const ObBackupLobData &other) const;
  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int64_t get_direct_cnt() const { return DIRECT_CNT; }
  OB_INLINE int32_t get_handle_size() const
  {
    return static_cast<int32_t>(offsetof(ObBackupLobData, lob_idx_) + sizeof(ObBackupLobIndex) * idx_cnt_);
  }
  TO_STRING_KV(
      K_(version),
      K_(byte_size),
      K_(char_size),
      K_(idx_cnt),
      "lob_idx_array", common::ObArrayWrap<ObBackupLobIndex>(lob_idx_, idx_cnt_));
public:
  uint32_t version_;
  uint32_t idx_cnt_;
  uint64_t byte_size_;
  uint64_t char_size_;
  ObBackupLobIndex lob_idx_[TOTAL_INDEX_CNT];
};

} // table_load_backup_v_1_4
} // namespace observer
} // namespace oceanbase
