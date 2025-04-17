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
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
class ObEncodingHashTable;
class ObMultiPrefixTree;
const int64_t SSTABLE_MACRO_BLOCK_HEADER_VERSION_v3 = 3;// add column order info to header
const int64_t MICRO_BLOCK_HEADER_MAGIC = 1005;
const int64_t SSTABLE_DATA_HEADER_MAGIC = 1007;
const int64_t LOB_MACRO_BLOCK_HEADER_MAGIC = 1009;
const int64_t LOB_MICRO_BLOCK_HEADER_MAGIC = 1010;
const int64_t LOB_MACRO_BLOCK_HEADER_VERSION_V1 = 1;
const int64_t MICRO_BLOCK_HEADER_VERSION = 1;
const int64_t MICRO_BLOCK_HEADERV2_VERSION = 1;

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

class ObSchemaColumnInfo
{
public:
  ObSchemaColumnInfo();
  ObSchemaColumnInfo(int64_t partkey_idx, bool is_partkey);
  ~ObSchemaColumnInfo() = default;
  void reset();
  int assign(const ObSchemaColumnInfo &other);
  TO_STRING_KV(K_(partkey_idx), K_(is_partkey));
public:
  int64_t partkey_idx_;
  bool is_partkey_;
};

class ObSchemaInfo
{
public:
  ObSchemaInfo();
  ~ObSchemaInfo() = default;
  void reset();
  int assign(const ObSchemaInfo &other);
  TO_STRING_KV(K_(column_desc), K_(column_info), K_(partkey_count), K_(is_heap_table));
public:
  common::ObArray<ObColDesc> column_desc_;
  common::ObArray<ObSchemaColumnInfo> column_info_;// 只记录堆表的列信息，有主键表该数组为空
  int64_t partkey_count_;
  bool is_heap_table_;
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


class ObMacroBlockCommonHeader
{
public:
  const static int32_t MACRO_BLOCK_COMMON_HEADER_VERSION = 1;
  const static int32_t MACRO_BLOCK_COMMON_HEADER_MAGIC = 1001;
  enum MacroBlockType
  {
    Free = 0,
    SSTableData = 1,
    PartitionMeta = 2,
    //SchemaData = 3,
    //Compressor = 4,
    MacroMeta = 5,
    Reserved = 6,
    MacroBlockSecondIndex = 7, // deprecated
    SortTempData = 8,
    LobData = 9,
    LobIndex = 10,
    TableMgrMeta = 11,
    TenantConfigMeta = 12,
    BloomFilterData = 13,
    MaxMacroType,
  };
  static_assert(static_cast<int64_t>(MacroBlockType::MaxMacroType) < common::OB_MAX_MACRO_BLOCK_TYPE,
      "MacroBlockType max value should less than 4 bits");

  ObMacroBlockCommonHeader();
  // DO NOT use virtual function. It is by design that sizeof(ObMacroBlockCommonHeader) equals to
  // the serialization length of all data members.
  ~ObMacroBlockCommonHeader() = default;
  void reset();
  bool is_valid() const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  int check_integrity();
  OB_INLINE bool is_data_block() const { return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block(); }
  OB_INLINE bool is_sstable_data_block() const { return SSTableData == attr_; }
  OB_INLINE bool is_lob_data_block() const { return LobData == attr_ || LobIndex == attr_; }
  OB_INLINE bool is_bloom_filter_data_block() const { return BloomFilterData == attr_; }
  OB_INLINE int32_t get_header_size() const { return header_size_; }
  OB_INLINE int32_t get_version() const { return version_; }
  OB_INLINE int32_t get_magic() const { return magic_; }
  OB_INLINE int32_t get_attr() const { return attr_; }
  OB_INLINE void set_attr(const MacroBlockType type) { attr_ = type; }
  OB_INLINE void set_data_version(const int64_t version) { data_version_ = version; }
  OB_INLINE int64_t get_previous_block_index() const { return previous_block_index_; }
  OB_INLINE void set_previous_block_index(const int64_t index) { previous_block_index_ = index; }
  OB_INLINE void set_reserved(const int64_t reserved) { reserved_ = reserved; }
  OB_INLINE void set_payload_size(const int32_t payload_size) { payload_size_ = payload_size; }
  OB_INLINE void set_payload_checksum(const int32_t payload_checksum) { payload_checksum_ = payload_checksum; }
  OB_INLINE int32_t get_payload_size() const { return payload_size_; }
  OB_INLINE int32_t get_payload_checksum() const { return payload_checksum_; }
  TO_STRING_KV(
      K_(header_size),
      K_(version),
      K_(magic),
      K_(attr),
      K_(data_version),
      K_(payload_size),
      K_(payload_checksum));
private:
  // NOTE: data members should be 64 bits aligned!!!
  int32_t header_size_; //struct size
  int32_t version_;     //header version
  int32_t magic_;       //magic number
  // each bits of the lower 8 bits represents:
  // is_free, sstable meta, tablet meta, compressor name, macro block meta, 0, 0, 0
  int32_t attr_;
  union
  {
    uint64_t data_version_;  //sstable macro block: major version(48 bits) + minor version(16 bits)
    /**
     * For meta block, it is organized as a link block. The next block has a index point to
     * its previous block. The entry block index is store in upper layer meta (super block)
     * 0 or -1 means no previous block
     */
    int64_t previous_block_index_;
  };
  union {
    int64_t reserved_;
    struct {
      int32_t payload_size_; // not include the size of common header
      int32_t payload_checksum_; // crc64 of payload
    };
  };
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockCommonHeader);
};

class ObSSTableMacroBlockHeader
{
public:
  ObSSTableMacroBlockHeader();
  ~ObSSTableMacroBlockHeader() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(
      K_(header_size),
      K_(version),
      K_(magic),
      K_(attr),
      K_(table_id),
      K_(data_version),
      K_(column_count),
      K_(rowkey_column_count),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(micro_block_count),
      K_(micro_block_size),
      K_(micro_block_data_offset),
      K_(micro_block_data_size),
      K_(micro_block_index_offset),
      K_(micro_block_index_size),
      K_(micro_block_endkey_offset),
      K_(micro_block_endkey_size),
      K_(data_checksum),
      K_(compressor_name),
      K_(encrypt_id),
      K_(master_key_id),
      KPHEX_(encrypt_key, sizeof(encrypt_key_)),
      K_(data_seq),
      K_(partition_id)
  );
public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;                    // magic number;
  int32_t attr_;
  uint64_t table_id_;
  int64_t data_version_;
  int32_t column_count_;
  int32_t rowkey_column_count_;
  int32_t column_index_scale_;       // index store scale
  int32_t row_store_type_;           // 0 flat, 1 encoding
  int32_t row_count_;                // sstable block row count
  int32_t occupy_size_;              // occupy size of the whole macro block, include common header
  int32_t micro_block_count_;        // block count
  int32_t micro_block_size_;
  int32_t micro_block_data_offset_;
  int32_t micro_block_data_size_;
  int32_t micro_block_index_offset_;
  int32_t micro_block_index_size_;
  int32_t micro_block_endkey_offset_;
  int32_t micro_block_endkey_size_;
  int64_t data_checksum_;            // ?? no need ??
  char compressor_name_[common::OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH];
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  //end encrypt
  int64_t data_seq_;
  int64_t partition_id_; // added since 2.-
};

class ObLobMacroBlockHeader : public ObSSTableMacroBlockHeader
{
public:
  ObLobMacroBlockHeader();
  ~ObLobMacroBlockHeader() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(header_size),
      K_(version),
      K_(magic),
      K_(attr),
      K_(table_id),
      K_(data_version),
      K_(column_count),
      K_(rowkey_column_count),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(micro_block_count),
      K_(micro_block_size),
      K_(micro_block_data_offset),
      K_(micro_block_data_size),
      K_(micro_block_index_offset),
      K_(micro_block_index_size),
      K_(micro_block_endkey_offset),
      K_(micro_block_endkey_size),
      K_(data_checksum),
      K_(compressor_name),
      K_(data_seq),
      K_(partition_id), //in 2.0 partition_id replace reserved_[1] in 14x
      K_(micro_block_size_array_offset),
      K_(micro_block_size_array_size)
  );
public:
  int32_t micro_block_size_array_offset_;
  int32_t micro_block_size_array_size_;
};

enum ObColumnChecksumMethod
{
  CCM_UNKOWN = 0,
  CCM_TYPE_AND_VALUE = 1,
  CCM_VALUE_ONLY = 2,
  CCM_IGNORE = 3,
  CCM_MAX
};

class ObMacroBlockMeta
{
public:
  ObMacroBlockMeta()
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
  ~ObMacroBlockMeta() {}
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

class ObMacroBlockMetaV2
{
public:
  ObMacroBlockMetaV2();
  ~ObMacroBlockMetaV2() = default;
  bool is_valid() const;
  int deserialize(const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  OB_INLINE bool is_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block();
  }
  OB_INLINE bool is_normal_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block();
  }
  OB_INLINE bool is_sstable_data_block() const
  {
    return ObMacroBlockCommonHeader::SSTableData == attr_;
  }
  OB_INLINE bool is_lob_data_block() const
  {
    return ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_;
  }
  OB_INLINE bool is_bloom_filter_data_block() const
  {
    return ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  TO_STRING_KV(
      K_(attr),
      K_(data_version),
      K_(column_number),
      K_(rowkey_column_number),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(data_checksum),
      K_(micro_block_count),
      K_(micro_block_data_offset),
      K_(micro_block_index_offset),
      K_(micro_block_endkey_offset),
      K(common::ObArrayWrap<int64_t>(column_checksum_, column_number_)),
      K(common::ObRowkey(endkey_, rowkey_column_number_)),
      K_(table_id),
      K_(data_seq),
      K_(schema_version),
      K_(snapshot_version),
      K_(schema_rowkey_col_cnt),
      K_(row_count_delta),
      K_(micro_block_mark_deletion_offset),
      K_(macro_block_deletion_flag),
      K_(micro_block_delta_offset),
      K_(partition_id),
      K_(column_checksum_method),
      K_(progressive_merge_round));
private:
  int64_t get_meta_content_serialize_size() const;
public:
  //For compatibility, the variables in this struct MUST NOT be deleted or moved.
  //You should ONLY add variables at the end.
  //Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  //The following variables need to be serialized
  int16_t attr_;       //低8位0,1,2,3,4,5分别表示空闲,sstable数据,tablet元数据,schema,compressor name,宏块元数据;其余位置0；
  union
  {
    uint64_t data_version_;  //sstable宏块：major sstable的data_version >0, minor sstable的data_version = 0
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
  int32_t micro_block_endkey_offset_; // index_size = endkey_offset - index_offset,
                                      // endkey_size = micro_block_mark_deletion_offset_ - endkey_offset
  char    *compressor_;
  uint16_t *column_id_array_;
  common::ObObjMeta *column_type_array_;
  int64_t *column_checksum_;
  common::ObObj   *endkey_;
  uint64_t table_id_;
  int64_t data_seq_;  // sequence in partition meta.
  int64_t schema_version_;
  int64_t snapshot_version_; //重用老的logic_block_index, 目前转储的宏块会设置为snapshot_version, data_version置0
  // major的宏块仍然依赖data_version，转储的宏块使用snapshot_version,目前仅lob宏块使用
  //转储多版本中的rowkey count与schema中的rowkey count不相同
  //schema_rowkey_col_cnt_（multi_version_real_column_number_的简写）是为了自解释引入
  //转储多版本中对应的rowkey_column_number_会有trans_version这一列作为rowkey
  //schema_rowkey_col_cnt_代表schema感知的rowkey_column_number
  //这个只用于多版本
  int16_t schema_rowkey_col_cnt_;
  common::ObOrderType *column_order_array_;
  // only valid for multi-version minor sstable, row count delta relative to the base data
  // default 0
  int32_t row_count_delta_;
  //以下用于标记删除
  int32_t micro_block_mark_deletion_offset_; //micro_block_mark_deletion_size = delte_offset - micro_block_mark_deletion_offset_
  bool macro_block_deletion_flag_; //true表示当前宏块的所有微块都被标记为删除
  int32_t micro_block_delta_offset_;  // delte_size = occupy_size - micro_block_delta_offset;
  int64_t partition_id_; // added since 2.0
  int16_t column_checksum_method_; // 0 for unkown, 1 for ObObj::checksum(), 2 for ObObj::checksum_v2()
  int64_t progressive_merge_round_;

  //The following variables do not need to be serialized
  int32_t write_seq_;       // increment 1 every reuse pass.
  uint32_t bf_flag_; // mark if rowkey_prefix bloomfilter in cache, bit=0: in, 0:not in
  int64_t create_timestamp_;
  int64_t retire_timestamp_;
  common::ObObj *collation_free_endkey_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
};

class ObMacroBlockMetaV3 final
{
public:
  const static int64_t MACRO_BLOCK_META_VERSION_V2 = 2; // upgrade in version 3.1, new meta format
  ObMacroBlockMetaV3();
  ~ObMacroBlockMetaV3() = default;
  bool is_valid() const;
  int deserialize(const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  OB_INLINE bool is_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block() || is_bloom_filter_data_block();
  }
  OB_INLINE bool is_normal_data_block() const
  {
    return is_sstable_data_block() || is_lob_data_block();
  }
  OB_INLINE bool is_sstable_data_block() const
  {
    return ObMacroBlockCommonHeader::SSTableData == attr_;
  }
  OB_INLINE bool is_lob_data_block() const
  {
    return ObMacroBlockCommonHeader::LobData == attr_ || ObMacroBlockCommonHeader::LobIndex == attr_;
  }
  OB_INLINE bool is_bloom_filter_data_block() const
  {
    return ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  TO_STRING_KV(
      K_(attr),
      K_(data_version),
      K_(column_number),
      K_(rowkey_column_number),
      K_(column_index_scale),
      K_(row_store_type),
      K_(row_count),
      K_(occupy_size),
      K_(data_checksum),
      K_(micro_block_count),
      K_(micro_block_data_offset),
      K_(micro_block_index_offset),
      K_(micro_block_endkey_offset),
      K(common::ObArrayWrap<int64_t>(column_checksum_, column_number_)),
      K(common::ObRowkey(endkey_, rowkey_column_number_)),
      K_(table_id),
      K_(data_seq),
      K_(schema_version),
      K_(snapshot_version),
      K_(schema_rowkey_col_cnt),
      K_(row_count_delta),
      K_(micro_block_mark_deletion_offset),
      K_(macro_block_deletion_flag),
      K_(micro_block_delta_offset),
      K_(partition_id),
      K_(column_checksum_method),
      K_(progressive_merge_round));
public:
  //For compatibility, the variables in this struct MUST NOT be deleted or moved.
  //You should ONLY add variables at the end.
  //Note that if you use complex structure as variables, the complex structure should also keep compatibility.

  //The following variables need to be serialized
  int16_t attr_;
  union
  {
    uint64_t data_version_;
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
  int32_t micro_block_endkey_offset_; // index_size = endkey_offset - index_offset,
                                      // endkey_size = micro_block_mark_deletion_offset_ - endkey_offset
  int64_t *column_checksum_;
  common::ObObj   *endkey_;
  uint64_t table_id_;
  int64_t data_seq_;  // sequence in partition meta.
  int64_t schema_version_;
  int64_t snapshot_version_;
  int16_t schema_rowkey_col_cnt_;
  // only valid for multi-version minor sstable, row count delta relative to the base data
  // default 0
  int32_t row_count_delta_;
  int32_t micro_block_mark_deletion_offset_; //micro_block_mark_deletion_size = delte_offset - micro_block_mark_deletion_offset_
  bool macro_block_deletion_flag_;
  int32_t micro_block_delta_offset_;  // delte_size = occupy_size - micro_block_delta_offset;
  int64_t partition_id_; // added since 2.0
  int16_t column_checksum_method_; // 0 for unkown, 1 for ObObj::checksum(), 2 for ObObj::checksum_v2()
  int64_t progressive_merge_round_;
  //The following variables do not need to be serialized
  int32_t write_seq_;       // increment 1 every reuse pass.
  uint32_t bf_flag_; // mark if rowkey_prefix bloomfilter in cache, bit=0: in, 0:not in
  int64_t create_timestamp_;
  int64_t retire_timestamp_;
  common::ObObj *collation_free_endkey_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  bool contain_uncommitted_row_;
  int64_t max_merged_trans_version_;
};

class ObMacroBlockSchemaInfo final
{
public:
  static const int64_t MACRO_BLOCK_SCHEMA_INFO_HEADER_VERSION = 1;
  ObMacroBlockSchemaInfo();
  ~ObMacroBlockSchemaInfo() = default;
  NEED_SERIALIZE_AND_DESERIALIZE;
  bool is_valid() const;
  TO_STRING_KV(
      K_(column_number),
      K_(rowkey_column_number),
      K_(schema_version),
      K_(schema_rowkey_col_cnt),
      K_(compressor),
      K(common::ObArrayWrap<uint16_t>(column_id_array_, column_number_)),
      K(common::ObArrayWrap<ObObjMeta>(column_type_array_, column_number_)),
      K(common::ObArrayWrap<ObOrderType>(column_order_array_, column_number_)));
public:
  int16_t column_number_;
  int16_t rowkey_column_number_;
  int64_t schema_version_;
  int16_t schema_rowkey_col_cnt_;
  char *compressor_;
  uint16_t *column_id_array_;
  common::ObObjMeta *column_type_array_;
  common::ObOrderType *column_order_array_;
};

class ObFullMacroBlockMetaEntry final
{
public:
  ObFullMacroBlockMetaEntry(ObMacroBlockMetaV3 &meta, ObMacroBlockSchemaInfo &schema);
  ~ObFullMacroBlockMetaEntry() = default;
  bool is_valid() const;
  int deserialize(const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(meta), K_(schema));
public:
  ObMacroBlockMetaV3 &meta_;
  ObMacroBlockSchemaInfo &schema_;
};

class ObBackupMacroData final
{
public:
  const static int64_t BACKUP_MARCO_DATA_VERSION = 1;
  OB_UNIS_VERSION(BACKUP_MARCO_DATA_VERSION);
public:
  ObBackupMacroData(blocksstable::ObBufferHolder &meta, blocksstable::ObBufferReader &data);
  TO_STRING_KV(K_(data), K_(meta));
public:
  blocksstable::ObBufferHolder &meta_;
  blocksstable::ObBufferReader &data_;
};

enum ObRowStoreType
{
  FLAT_ROW_STORE = 0,
  ENCODING_ROW_STORE = 1,
  SPARSE_ROW_STORE = 2,
  SELECTIVE_ENCODING_ROW_STORE = 3,
  MAX_ROW_STORE
};

class ObMicroBlockData
{
public:
  ObMicroBlockData()
    : buf_(NULL), size_(0), extra_buf_(0), extra_size_(0), store_type_(MAX_ROW_STORE)
  {}
  ObMicroBlockData(
      const char *buf,
      const int64_t size,
      const char *extra_buf = NULL,
      const int64_t extra_size = 0)
    : buf_(buf),
      size_(size),
      extra_buf_(extra_buf),
      extra_size_(extra_size)
  {}
  bool is_valid() const { return NULL != buf_ && size_ > 0; }
  const char *&get_buf() { return buf_; }
  const char *get_buf() const { return buf_; }
  int64_t &get_buf_size() { return size_; }
  int64_t get_buf_size() const { return size_; }
  const char *&get_extra_buf() { return extra_buf_; }
  const char *get_extra_buf() const { return extra_buf_; }
  int64_t get_extra_size() const { return extra_size_; }
  int64_t &get_extra_size() { return extra_size_; }
  int64_t total_size() const { return size_ + extra_size_; }
  void reset() { *this = ObMicroBlockData(); }
  TO_STRING_KV(KP_(buf), K_(size), KP_(extra_buf), K_(extra_size), K_(store_type));
public:
  const char *buf_;
  int64_t size_;
  const char *extra_buf_;
  int64_t extra_size_;
  ObRowStoreType store_type_;
};

class ObMicroBlockHeader
{
public:
  ObMicroBlockHeader();
  ~ObMicroBlockHeader() = default;
  bool is_valid() const;
  TO_STRING_KV(
      K_(header_size),
      K_(version),
      K_(magic),
      K_(attr),
      K_(column_count),
      K_(row_index_offset),
      K_(row_count));
public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t attr_;
  int32_t column_count_;
  int32_t row_index_offset_;
  int32_t row_count_;
};

class ObMicroBlockHeaderV2
{
public:
  ObMicroBlockHeaderV2();
  ~ObMicroBlockHeaderV2() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(
      K_(header_size),
      K_(version),
      K_(row_count),
      K_(var_column_count),
      K_(row_data_offset),
      K_(row_index_byte),
      K_(extend_value_bit),
      K_(store_row_header));
public:
  int16_t header_size_;
  int16_t version_;
  int16_t row_count_;
  int16_t var_column_count_;
  int32_t row_data_offset_;
  union {
    struct {
      uint16_t row_index_byte_:3;
      uint16_t extend_value_bit_:3;
      uint16_t store_row_header_:1;
    };
    uint16_t opt_;
  };
  int16_t reserved_;

} __attribute__((packed));

class ObLobMicroBlockHeader
{
public:
  ObLobMicroBlockHeader()
    : header_size_(0),
      version_(0),
      magic_(0)
  {
  }
  ~ObLobMicroBlockHeader() = default;
  TO_STRING_KV(K_(header_size), K_(version), K_(magic));
public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
};

class ObMicroBlockIndex
{
public:
  ObMicroBlockIndex()
    : data_offset_(0), endkey_offset_(0)
  {}
  inline bool is_valid() const { return data_offset_ >= 0 && endkey_offset_ >= 0; }
  TO_STRING_KV(K_(data_offset), K_(endkey_offset));
public:
  int32_t data_offset_;
  int32_t endkey_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndex);
};

class ObLobMicroBlockIndex
{
public:
  ObLobMicroBlockIndex()
    : data_offset_(0)
  {}
  inline bool is_valid() const { return data_offset_ >= 0; }
  TO_STRING_KV(K_(data_offset));
public:
  int32_t data_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobMicroBlockIndex);
};

enum ObRecordHeaderVersion
{
  RECORD_HEADER_VERSION_V2 = 2,
  RECORD_HEADER_VERSION_V3 = 3
};

class ObRecordHeaderV2
{
public:
  static const int8_t PRE_MICRO_BLOCK_RECORD_HEADER_VERSION = 0x2;
  static const int64_t PRE_MICRO_BLOCK_RECORD_HEADER_MAGIC = 1005;
  ObRecordHeaderV2()
  {
    memset(this, 0, sizeof(ObRecordHeaderV2));
    header_length_ = static_cast<int8_t>(sizeof(ObRecordHeaderV2));
    version_ = ObRecordHeaderV2::PRE_MICRO_BLOCK_RECORD_HEADER_VERSION;
  }
  ~ObRecordHeaderV2() {}
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size(void) const;
  bool is_compressed_data() const { return data_length_ != data_zlength_; }
  int check_header_checksum() const;
  int check_payload_checksum(const char *buf, const int64_t len) const;
  static int check_record(const char *ptr, const int64_t size);
  TO_STRING_KV(K(magic_), K(header_length_), K(version_), K(header_checksum_),
               K(reserved16_), K(data_length_), K(data_zlength_), K(data_checksum_));
public:
  int16_t magic_;
  int8_t header_length_;
  int8_t version_;
  int16_t header_checksum_;
  int16_t reserved16_;
  int64_t data_length_;
  int64_t data_zlength_;
  int64_t data_checksum_;
};

struct ObRecordHeaderV3
{
public:
  static int64_t get_serialize_size(const int64_t header_version, const int64_t column_cnt);
  struct ObRecordCommonHeader
  {
  public:
    ObRecordCommonHeader() = default;
    ~ObRecordCommonHeader() = default;
    inline bool is_compressed() const { return data_length_ != data_zlength_; }
    int16_t magic_;
    int8_t header_length_;
    int8_t version_;
    int16_t header_checksum_;
    int16_t reserved16_;
    int64_t data_length_;
    int64_t data_zlength_;
    int64_t data_checksum_;
  };
  ObRecordHeaderV3();
  ~ObRecordHeaderV3() = default;
  int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size(void) const;
  int check_and_get_record(
      const char *ptr,
      const int64_t size,
      const int16_t magic,
      const char *&payload_ptr,
      int64_t &payload_size) const;
  inline bool is_compressed_data() const { return data_length_ != data_zlength_; }
  TO_STRING_KV(
      K_(magic),
      K_(header_length),
      K_(version),
      K_(header_checksum),
      K_(reserved16),
      K_(data_length),
      K_(data_zlength),
      K_(data_checksum),
      K_(data_encoding_length),
      K_(row_count),
      K_(column_cnt),
      KP(column_checksums_));
private:
  int check_header_checksum() const;
  int check_payload_checksum(const char *buf, const int64_t len) const;
public:
  int16_t magic_;
  int8_t header_length_; ///  column_checksum is not contained is header_length_
  int8_t version_;
  int16_t header_checksum_;
  int16_t reserved16_;
  int64_t data_length_;
  int64_t data_zlength_;
  int64_t data_checksum_;
  // add since 2.2
  int32_t data_encoding_length_;
  int32_t row_count_;
  uint16_t column_cnt_;
  int64_t *column_checksums_;
};

struct ObRowHeaderV1Dummy
{
  int8_t row_flag_;
  int8_t column_index_bytes_;
  int8_t row_dml_;
  int8_t reserved8_;
  int32_t reserved32_;
  TO_STRING_KV(K(row_flag_), K(column_index_bytes_), K(row_dml_), K(reserved8_), K(reserved32_));
  DISALLOW_COPY_AND_ASSIGN(ObRowHeaderV1Dummy);
};

class ObRowHeaderV1
{
public:
  ObRowHeaderV1() { memset(this, 0, sizeof(*this)); }
  ~ObRowHeaderV1() = default;
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
  ObRowHeaderV1 &operator=(const ObRowHeaderV1 &src)
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

class ObRowHeaderV2
{
public:
  ObRowHeaderV2() { memset(this, 0, sizeof(*this)); }
  ~ObRowHeaderV2() = default;
  bool is_valid() const { return row_flag_ >= 0 && column_index_bytes_ >= 0; }
  int8_t get_row_flag() const { return row_flag_; }
  int8_t get_row_dml() const { return row_dml_; }
  int8_t get_version() const { return version_; }
  int8_t get_column_index_bytes() const { return column_index_bytes_; }
  int8_t get_row_type_flag() const { return row_type_flag_; }
  int16_t get_column_count() const { return column_count_; }
  void set_row_flag(const int8_t row_flag) { row_flag_ = row_flag; }
  void set_column_index_bytes(const int8_t column_index_bytes) { column_index_bytes_ = column_index_bytes; }
  void set_row_dml(const int8_t row_dml) { row_dml_ = row_dml; }
  void set_version(const int8_t version) { version_ = version; }
  void set_reserved8(const int8_t reserved8) { reserved8_ = reserved8; }
  void set_column_count(const int16_t column_count) { column_count_ = column_count; }
  void set_row_type_flag(const int8_t row_type_flag) { row_type_flag_ = row_type_flag; }
  static int get_serialized_size()
  {
    return sizeof(ObRowHeaderV2);
  }
  ObRowHeaderV2 &operator=(const ObRowHeaderV2 &src)
  {
    row_flag_ = src.row_flag_;
    column_index_bytes_ = src.column_index_bytes_;
    row_dml_ = src.row_dml_;
    version_ = src.version_;
    reserved8_ = src.reserved8_;
    column_count_ = src.column_count_;
    row_type_flag_ = src.row_type_flag_;
    return *this;
  }

  enum ObRowHeaderVersion
  {
    RHV_NO_TRANS_ID = 0,
    RHV_WITH_TRANS_ID = 1,
  };

  TO_STRING_KV(
      K_(row_flag),
      K_(column_index_bytes),
      K_(row_dml),
      K_(version),
      K_(row_type_flag),
      K_(reserved8),
      K_(column_count));

private:
  int8_t row_flag_;
  int8_t column_index_bytes_;
  int8_t row_dml_;
  int8_t version_;
  int8_t row_type_flag_;
  int8_t reserved8_;
  int16_t column_count_;
};

class ObColumnHeader
{
public:
  enum Type
  {
    RAW,
    DICT,
    RLE,
    CONST,
    INTEGER_BASE_DIFF,
    STRING_DIFF,
    HEX_PACKING,
    STRING_PREFIX,
    COLUMN_EQUAL,
    COLUMN_SUBSTR,
    MAX_TYPE
  };

  enum Attribute
  {
    FIX_LENGTH = 0x1,
    HAS_EXTEND_VALUE = 0x2,
    BIT_PACKING = 0x4,
    LAST_VAR_FIELD = 0x8,
    STORE_NUMBER_INTEGER = 0x10, // indicate the column original type is ObNumber
     MAX_ATTRIBUTE,
  };
  ObColumnHeader() { reuse(); }
  void reuse() { memset(this, 0, sizeof(*this)); }
  bool is_valid() const { return type_ >= 0 && type_ < MAX_TYPE; }

  inline bool is_fix_length() const { return attr_ & FIX_LENGTH; }
  inline bool has_extend_value() const { return attr_ & HAS_EXTEND_VALUE; }
  inline bool is_bit_packing() const { return attr_ & BIT_PACKING; }
  inline bool is_last_var_field() const { return attr_ & LAST_VAR_FIELD; }
  inline bool is_store_nbr_int() const { return attr_ & STORE_NUMBER_INTEGER; }
  inline bool is_span_column() const
  {
    return COLUMN_EQUAL == type_ || COLUMN_SUBSTR == type_;
  }
  inline static bool is_inter_column_encoder(const Type type)
  {
    return COLUMN_EQUAL == type || COLUMN_SUBSTR == type;
  }

  inline void set_fix_lenght_attr() { attr_ |= FIX_LENGTH; }
  inline void set_has_extend_value_attr() { attr_ |= HAS_EXTEND_VALUE; }
  inline void set_bit_packing_attr() { attr_ |= BIT_PACKING; }
  inline void set_last_var_field_attr() { attr_ |= LAST_VAR_FIELD; }
  inline void set_store_nbr_int_attr() { attr_ |= STORE_NUMBER_INTEGER; }

  TO_STRING_KV(K_(type), K_(attr), K_(extend_value_index), K_(offset), K_(length));
public:
  int8_t type_;
  int8_t attr_;
  int16_t extend_value_index_;
  uint16_t offset_;
  uint16_t length_;

} __attribute__((packed));

class ObMicroBlockEncoderOpt
{
public:
  static const bool ENCODINGS_DEFAULT[ObColumnHeader::MAX_TYPE];
  static const bool ENCODINGS_NONE[ObColumnHeader::MAX_TYPE];
  static const bool ENCODINGS_FOR_PERFORMANCE[ObColumnHeader::MAX_TYPE];
  ObMicroBlockEncoderOpt();
  ObMicroBlockEncoderOpt(ObRowStoreType store_type);
  void set_all(const bool enable);
  bool is_valid() const { return enable_raw(); }
  void reset() { set_all(false); }
  bool &enable(const int64_t type)
  {
    return const_cast<bool &>(static_cast<const ObMicroBlockEncoderOpt *>(this)->enable(type));
  }
  const bool &enable(const int64_t type) const
  {
    static bool dummy = false;
    return type < 0 || type >= ObColumnHeader::MAX_TYPE ? dummy : encodings_[type];
  }
  template <typename T>
  const bool &enable() const { return enable(T::type_); }
  template <typename T>
  const bool &enable() { return enable(T::type_); }
  bool &enable_raw() { return enable(ObColumnHeader::RAW); }
  bool &enable_dict() { return enable(ObColumnHeader::DICT); }
  bool &enable_int_diff() { return enable(ObColumnHeader::INTEGER_BASE_DIFF); }
  bool &enable_str_diff() { return enable(ObColumnHeader::STRING_DIFF); }
  bool &enable_hex_pack() { return enable(ObColumnHeader::HEX_PACKING); }
  bool &enable_rle() { return enable(ObColumnHeader::RLE); }
  bool &enable_const() { return enable(ObColumnHeader::CONST); }
  bool &enable_str_prefix() { return enable(ObColumnHeader::STRING_PREFIX); }

  const bool &enable_raw() const { return enable(ObColumnHeader::RAW); }
  const bool &enable_dict() const { return enable(ObColumnHeader::DICT); }
  const bool &enable_int_diff() const { return enable(ObColumnHeader::INTEGER_BASE_DIFF); }
  const bool &enable_str_diff() const { return enable(ObColumnHeader::STRING_DIFF); }
  const bool &enable_hex_pack() const { return enable(ObColumnHeader::HEX_PACKING); }
  const bool &enable_rle() const { return enable(ObColumnHeader::RLE); }
  const bool &enable_const() const { return enable(ObColumnHeader::CONST); }
  const bool &enable_str_prefix() const { return enable(ObColumnHeader::STRING_PREFIX); }
#define KF(f) #f, f()
  TO_STRING_KV(
      K_(enable_bit_packing),
      K_(store_sorted_var_len_numbers_dict),
      K_(store_row_header),
      KF(enable_raw),
      KF(enable_dict),
      KF(enable_int_diff),
      KF(enable_str_diff),
      KF(enable_hex_pack),
      KF(enable_rle),
      KF(enable_const));
#undef KF
public:
  bool store_row_header_;
  // disable bitpacking and store sorted var-length numbers dictionary in dict encoding under
  // SELECTIVE_ROW_STORE mode, vice versa
  bool enable_bit_packing_;
  bool store_sorted_var_len_numbers_dict_;
  const bool *encodings_;
};

class ObPreviousEncoding
{
public:
  ObPreviousEncoding() { MEMSET(this, 0, sizeof(*this)); }
  ObPreviousEncoding(const ObColumnHeader::Type type, const int64_t ref_col_idx)
    : type_(type), ref_col_idx_(ref_col_idx), last_prefix_length_(0) {}

  bool operator ==(const ObPreviousEncoding &other) const
  {
    return type_ == other.type_ && ref_col_idx_ == other.ref_col_idx_ && last_prefix_length_ == other.last_prefix_length_;
  }

  bool operator !=(const ObPreviousEncoding &other) const
  {
    return type_ != other.type_ || ref_col_idx_ != other.ref_col_idx_ || last_prefix_length_ != other.last_prefix_length_;
  }

  TO_STRING_KV(K_(type), K_(ref_col_idx), K_(last_prefix_length));
public:
  ObColumnHeader::Type type_;
  int64_t ref_col_idx_; // referenced column index for rules between columns.
  int64_t last_prefix_length_;
};

template<int64_t max_size>
class ObPreviousEncodingArray
{
public:
  ObPreviousEncodingArray() : last_pos_(0), size_(0) {}
  int put(const ObPreviousEncoding &prev);
  int64_t contain(const ObPreviousEncoding &prev);
  void reuse() { size_ = 0; }

  TO_STRING_KV(K_(prev_encodings), K_(last_pos), K_(size));
public:
  ObPreviousEncoding prev_encodings_[max_size];
  int64_t last_pos_;
  int64_t size_;
};

template<>
struct ObPreviousEncodingArray<2>
{
public:
  ObPreviousEncodingArray() : last_pos_(0), size_(0) {}
  OB_INLINE int put(const ObPreviousEncoding &prev)
  {
    int ret = common::OB_SUCCESS;
    if (0 == size_ || prev != prev_encodings_[last_pos_]) {
      if (2 == size_) {
        last_pos_ = (last_pos_ == 1) ? 0 : last_pos_ + 1;
        prev_encodings_[last_pos_] = prev;
      } else if (2 > size_) {
        last_pos_ = size_;
        prev_encodings_[last_pos_] = prev;
        ++size_;
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected size", K_(size));
      }
    }
    return ret;
  }
  void reuse() { size_ = 0; }
  TO_STRING_KV(
      K_(last_pos),
      K_(size),
      "prev_encoding0", prev_encodings_[0],
      "prev_encoding1", prev_encodings_[1]);

public:
  ObPreviousEncoding prev_encodings_[2];
  int64_t last_pos_;
  int64_t size_;
};

template<>
struct ObPreviousEncodingArray<1>
{
public:
  ObPreviousEncodingArray() : last_pos_(0), size_(0) {}
  OB_INLINE int put(const ObPreviousEncoding &prev)
  {
    int ret = common::OB_SUCCESS;
    prev_encodings_[0] = prev;
    return ret;
  }
  TO_STRING_KV(K_(prev_encodings));
public:
  ObPreviousEncoding prev_encodings_[1];
  int64_t last_pos_;
  int64_t size_;
};

class ObMicroBlockEncodingCtx
{
public:
  static const int64_t MAX_PREV_ENCODING_COUNT = 2;
  ObMicroBlockEncodingCtx();
  bool is_valid() const;
  TO_STRING_KV(
      K_(macro_block_size),
      K_(micro_block_size),
      K_(rowkey_column_cnt),
      K_(column_cnt),
      KP_(column_types),
      K_(estimate_block_size),
      K_(real_block_size),
      K_(micro_block_cnt),
      K_(encoder_opt),
      K_(previous_encodings),
      KP_(column_encodings),
      K_(store_nbr_int),
      K_(major_working_cluster_version));
public:
  int64_t macro_block_size_;
  int64_t micro_block_size_;
  int64_t rowkey_column_cnt_;
  int64_t column_cnt_;
  common::ObObjMeta *column_types_;
  ObMicroBlockEncoderOpt encoder_opt_;
  mutable int64_t estimate_block_size_;
  mutable int64_t real_block_size_;
  mutable int64_t micro_block_cnt_; // build micro block count
  mutable common::ObArray<ObPreviousEncodingArray<MAX_PREV_ENCODING_COUNT> > previous_encodings_;
  int64_t *column_encodings_;
  bool store_nbr_int_;
  int64_t major_working_cluster_version_;
};

template <typename T, int64_t MAX_COUNT, int64_t BLOCK_SIZE>
class ObPodFix2dArray;

class ObColumnEncodingCtx
{
public:
  ObColumnEncodingCtx() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }
  TO_STRING_KV(
      K_(null_cnt),
      K_(nope_cnt),
      K_(max_integer),
      K_(var_data_size),
      K_(dict_var_data_size),
      K_(fix_data_size),
      K_(need_sort),
      K_(extend_value_bit),
      KP_(col_values),
      KP_(ht),
      KP_(prefix_tree),
      K_(*encoding_ctx),
      K_(is_refed),
      K_(detected_encoders),
      K_(last_prefix_length),
      K_(max_string_size),
      K_(only_raw_encoding),
      K_(store_nbr_int));
public:
  int64_t null_cnt_;
  int64_t nope_cnt_;
  uint64_t max_integer_;
  int64_t var_data_size_;
  int64_t dict_var_data_size_;
  int64_t fix_data_size_;
  int64_t max_string_size_;
  bool need_sort_;
  int64_t extend_value_bit_;
  const ObPodFix2dArray<common::ObObj, 64 << 10, common::OB_MALLOC_MIDDLE_BLOCK_SIZE> *col_values_;
  ObEncodingHashTable *ht_;
  ObMultiPrefixTree *prefix_tree_;
  const ObMicroBlockEncodingCtx *encoding_ctx_;
  bool is_refed_;
  bool detected_encoders_[ObColumnHeader::MAX_TYPE];
  mutable int64_t last_prefix_length_;
  bool only_raw_encoding_;
  bool store_nbr_int_; // use integer type to store ObNumber
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
