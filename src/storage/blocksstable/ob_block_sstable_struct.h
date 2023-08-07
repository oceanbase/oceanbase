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

#ifndef __BLOCK_SSTABLE_DATA_STRUCTURE_H__
#define __BLOCK_SSTABLE_DATA_STRUCTURE_H__

#include "common/log/ob_log_cursor.h"
#include "common/ob_store_format.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/compress/ob_compress_util.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_encryption_util.h"
#include "share/schema/ob_table_schema.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_table.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace blocksstable
{

class ObEncodingHashTable;
class ObMultiPrefixTree;

extern const char *BLOCK_SSTBALE_DIR_NAME;
extern const char *BLOCK_SSTBALE_FILE_NAME;

//block sstable header magic number;
const int64_t MICRO_BLOCK_HEADER_MAGIC = 1005;
const int64_t BF_MACRO_BLOCK_HEADER_MAGIC = 1014;
const int64_t BF_MICRO_BLOCK_HEADER_MAGIC = 1015;
const int64_t SERVER_SUPER_BLOCK_MAGIC = 1018;
const int64_t LINKED_MACRO_BLOCK_HEADER_MAGIC = 1019;

const int64_t MICRO_BLOCK_HEADER_VERSION_1 = 1;
const int64_t MICRO_BLOCK_HEADER_VERSION_2 = 2;
const int64_t MICRO_BLOCK_HEADER_VERSION = MICRO_BLOCK_HEADER_VERSION_2;
const int64_t LINKED_MACRO_BLOCK_HEADER_VERSION = 1;
const int64_t BF_MACRO_BLOCK_HEADER_VERSION = 1;
const int64_t BF_MICRO_BLOCK_HEADER_VERSION = 1;

struct ObPosition
{
  int32_t offset_;
  int32_t length_;
  ObPosition() : offset_(0), length_(0) {}
  void reset() { offset_ = 0; length_ = 0; }
  bool is_valid() const
  {
    return offset_ >= 0 && length_ >= 0;
  }
  TO_STRING_KV(K_(offset), K_(length));
};

struct ObMacroDataSeq
{
  static const int64_t BIT_DATA_SEQ = 32;
  static const int64_t BIT_PARALLEL_IDX = 11;
  static const int64_t BIT_BLOCK_TYPE = 3;
  static const int64_t BIT_MERGE_TYPE = 2;
  static const int64_t BIT_SSTABLE_SEQ = 10;
  static const int64_t BIT_RESERVED = 5;
  static const int64_t BIT_SIGN = 1;
  static const int64_t MAX_PARALLEL_IDX = (0x1UL << BIT_PARALLEL_IDX) - 1;
  static const int64_t MAX_SSTABLE_SEQ = (0x1UL << BIT_SSTABLE_SEQ) - 1;
  enum BlockType {
    DATA_BLOCK = 0,
    INDEX_BLOCK = 1,
    META_BLOCK = 2,
  };
  enum MergeType {
    MAJOR_MERGE = 0,
    MINOR_MERGE = 1,
  };

  ObMacroDataSeq() : macro_data_seq_(0) {}
  ObMacroDataSeq(const int64_t data_seq) : macro_data_seq_(data_seq) {}
  virtual ~ObMacroDataSeq() = default;
  ObMacroDataSeq &operator=(const ObMacroDataSeq &other)
  {
    if (this != &other) {
      macro_data_seq_ = other.macro_data_seq_;
    }
    return *this;
  }
  bool operator ==(const ObMacroDataSeq &other) const { return macro_data_seq_ == other.macro_data_seq_; }
  bool operator !=(const ObMacroDataSeq &other) const { return macro_data_seq_ != other.macro_data_seq_; }
  OB_INLINE void reset() { macro_data_seq_ = 0; }
  OB_INLINE int64_t get_data_seq() const { return macro_data_seq_; }
  OB_INLINE bool is_valid() const { return macro_data_seq_ >= 0; }
  OB_INLINE bool is_data_block() const { return block_type_ == DATA_BLOCK; }
  OB_INLINE bool is_index_block() const { return block_type_ == INDEX_BLOCK; }
  OB_INLINE bool is_meta_block() const { return block_type_ == META_BLOCK; }
  OB_INLINE bool is_major_merge() const { return merge_type_ == MAJOR_MERGE; }
  OB_INLINE int set_sstable_seq(const int16_t sstable_logic_seq)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(sstable_logic_seq >= MAX_SSTABLE_SEQ || sstable_logic_seq < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid sstable seq", K(ret), K(sstable_logic_seq));
    } else {
      sstable_logic_seq_ = sstable_logic_seq;
    }
    return ret;
  }
  OB_INLINE int set_parallel_degree(const int64_t parallel_idx)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(parallel_idx >= MAX_PARALLEL_IDX || parallel_idx < 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid parallel idx", K(parallel_idx));
    } else {
      parallel_idx_ = parallel_idx;
    }
    return ret;
  }
  OB_INLINE void set_data_block() { block_type_ = DATA_BLOCK; }
  OB_INLINE void set_index_block() { block_type_ = INDEX_BLOCK; }
  OB_INLINE void set_macro_meta_block() { block_type_ = META_BLOCK; }
  OB_INLINE void set_index_merge_block() { block_type_ = INDEX_BLOCK; parallel_idx_ = MAX_PARALLEL_IDX; }
  TO_STRING_KV(K_(data_seq), K_(parallel_idx), K_(block_type), K_(merge_type), K_(reserved), K_(sign), K_(macro_data_seq));
  union
  {
    int64_t macro_data_seq_;
    struct
    {
      uint64_t data_seq_ : BIT_DATA_SEQ;
      uint64_t parallel_idx_ : BIT_PARALLEL_IDX;
      uint64_t block_type_ : BIT_BLOCK_TYPE;
      uint64_t merge_type_ : BIT_MERGE_TYPE;
      uint64_t sstable_logic_seq_ : BIT_SSTABLE_SEQ;
      uint64_t reserved_ : BIT_RESERVED;
      uint64_t sign_ : BIT_SIGN;
    };
  };
};

struct ObCommitLogSpec
{
  const char *log_dir_;
  int64_t max_log_file_size_;
  int64_t log_sync_type_;
  ObCommitLogSpec()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const
  {
    return NULL != log_dir_
        && max_log_file_size_ > 0
        && (log_sync_type_ == 0 || log_sync_type_ == 1);
  }
  TO_STRING_KV(K_(log_dir), K_(max_log_file_size), K_(log_sync_type));
};

struct ObStorageEnv
{
  enum REDUNDANCY_LEVEL
  {
    EXTERNAL_REDUNDANCY = 0,
    NORMAL_REDUNDANCY = 1,
    HIGH_REDUNDANCY = 2,
    MAX_REDUNDANCY
  };
  // for disk manager
  const char *data_dir_;
  const char *sstable_dir_;
  int64_t default_block_size_;
  int64_t data_disk_size_;
  int64_t data_disk_percentage_;
  int64_t log_disk_size_;
  int64_t log_disk_percentage_;
  REDUNDANCY_LEVEL redundancy_level_;

  // for sstable log writer
  ObCommitLogSpec log_spec_;

  // for clog/slog file handler
  ObLogFileSpec clog_file_spec_;
  ObLogFileSpec slog_file_spec_;

  // for clog writer
  const char *clog_dir_;

  // for cache
  int64_t tablet_ls_cache_priority_;
  int64_t index_block_cache_priority_;
  int64_t user_block_cache_priority_;
  int64_t user_row_cache_priority_;
  int64_t fuse_row_cache_priority_;
  int64_t bf_cache_priority_;
  int64_t storage_meta_cache_priority_;
  int64_t bf_cache_miss_count_threshold_;

  int64_t ethernet_speed_;

  ObStorageEnv()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const;
  TO_STRING_KV(K_(data_dir),
               K_(default_block_size),
               K_(data_disk_size),
               K_(data_disk_percentage),
               K_(log_disk_size),
               K_(log_disk_percentage),
               K_(redundancy_level),
               K_(log_spec),
               K_(clog_dir),
               K_(tablet_ls_cache_priority),
               K_(index_block_cache_priority),
               K_(user_block_cache_priority),
               K_(user_row_cache_priority),
               K_(fuse_row_cache_priority),
               K_(bf_cache_priority),
               K_(bf_cache_miss_count_threshold),
               K_(storage_meta_cache_priority),
               K_(ethernet_speed));
};

struct ObMicroBlockId
{
  ObMicroBlockId(const MacroBlockId &block_id, const int64_t offset, const int64_t size);
  ObMicroBlockId();
  OB_INLINE void reset()
  {
    macro_id_.reset();
    offset_ = 0;
    size_ = 0;
  }
  OB_INLINE bool is_valid() const { return macro_id_.is_valid() && offset_ > 0 && size_ > 0; }
  OB_INLINE bool operator == (const ObMicroBlockId &other) const
  {
    return macro_id_ == other.macro_id_ && offset_ == other.offset_ && size_ == other.size_;
  }
  TO_STRING_KV(K_(macro_id), K_(offset), K_(size));
  MacroBlockId macro_id_;
  int32_t offset_;
  int32_t size_;
};

struct ObBloomFilterMicroBlockHeader
{
  ObBloomFilterMicroBlockHeader() { reset();}
  void reset() { MEMSET(this, 0, sizeof(ObBloomFilterMicroBlockHeader));}
  OB_INLINE bool is_valid() const
  {
    return header_size_ == sizeof(ObBloomFilterMicroBlockHeader)
      && version_ >= BF_MICRO_BLOCK_HEADER_VERSION
      && BF_MICRO_BLOCK_HEADER_MAGIC == magic_
      && rowkey_column_count_ > 0
      && row_count_ > 0;
  }
  TO_STRING_KV(K_(header_size), K_(version), K_(magic), K_(rowkey_column_count), K_(row_count));

  int16_t header_size_;
  int16_t version_;
  int16_t magic_;
  int16_t rowkey_column_count_;
  int32_t row_count_;
  int32_t reserved_;
};

struct ObColumnHeader
{
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
    MAX_ATTRIBUTE,
  };
  static constexpr int8_t OB_COLUMN_HEADER_V1 = 0;

  int8_t version_;
  int8_t type_;
  int8_t attr_;
  uint8_t obj_type_;
  union {
    uint32_t extend_value_offset_; // for var column null-bitmap stored continuously
    uint32_t extend_value_index_;
  };
  uint32_t offset_;
  uint32_t length_;

  static_assert(UINT8_MAX >= ObObjType::ObMaxType, "ObObjType is stored in ObColumnHeader with 1 byte");
  ObColumnHeader() { reuse(); }
  void reuse() { memset(this, 0, sizeof(*this)); }
  bool is_valid() const
  {
    return version_ == OB_COLUMN_HEADER_V1
        && type_ >= 0
        && type_ < MAX_TYPE
        && obj_type_ < ObObjType::ObMaxType;
  }

  inline bool is_fix_length() const { return attr_ & FIX_LENGTH; }
  inline bool has_extend_value() const { return attr_ & HAS_EXTEND_VALUE; }
  inline bool is_bit_packing() const { return attr_ & BIT_PACKING; }
  inline bool is_last_var_field() const { return attr_ & LAST_VAR_FIELD; }
  inline bool is_span_column() const
  {
    return COLUMN_EQUAL == type_ || COLUMN_SUBSTR == type_;
  }
  inline static bool is_inter_column_encoder(const Type type)
  {
    return COLUMN_EQUAL == type || COLUMN_SUBSTR == type;
  }
  inline ObObjType get_store_obj_type() const { return static_cast<ObObjType>(obj_type_); }

  inline void set_fix_lenght_attr() { attr_ |= FIX_LENGTH; }
  inline void set_has_extend_value_attr() { attr_ |= HAS_EXTEND_VALUE; }
  inline void set_bit_packing_attr() { attr_ |= BIT_PACKING; }
  inline void set_last_var_field_attr() { attr_ |= LAST_VAR_FIELD; }

  TO_STRING_KV(K_(version), K_(type), K_(attr), K_(obj_type),
      K_(extend_value_offset), K_(offset), K_(length));
} __attribute__((packed));


struct ObMicroBlockEncoderOpt
{
  static const bool ENCODINGS_DEFAULT[ObColumnHeader::MAX_TYPE];
  static const bool ENCODINGS_NONE[ObColumnHeader::MAX_TYPE];
  static const bool ENCODINGS_FOR_PERFORMANCE[ObColumnHeader::MAX_TYPE];

  // disable bitpacking and store sorted var-length numbers dictionary in dict encoding under
  // SELECTIVE_ROW_STORE mode, vice versa
  bool enable_bit_packing_;
  bool store_sorted_var_len_numbers_dict_;
  const bool *encodings_;

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

  ObMicroBlockEncoderOpt() { set_store_type(ENCODING_ROW_STORE); }

  OB_INLINE bool is_valid() const { return enable_raw(); }
  OB_INLINE void reset() { set_store_type(FLAT_ROW_STORE); }
  OB_INLINE void set_store_type(common::ObRowStoreType store_type) {
    switch (store_type) {
      case SELECTIVE_ENCODING_ROW_STORE:
        enable_bit_packing_ = false;
        store_sorted_var_len_numbers_dict_ = true;
        encodings_ = ENCODINGS_FOR_PERFORMANCE;
        break;
      case ENCODING_ROW_STORE:
        enable_bit_packing_ = true;
        store_sorted_var_len_numbers_dict_ = false;
        encodings_ = ENCODINGS_DEFAULT;
        break;
      default:
        enable_bit_packing_ = false;
        store_sorted_var_len_numbers_dict_ = false;
        encodings_ = ENCODINGS_NONE;
        break;
    }
  }

#define KF(f) #f, f()
  TO_STRING_KV(K_(enable_bit_packing), K_(store_sorted_var_len_numbers_dict),
      KF(enable_raw), KF(enable_dict), KF(enable_int_diff), KF(enable_str_diff),
      KF(enable_hex_pack), KF(enable_rle),KF(enable_const));
#undef KF
};

struct ObPreviousEncoding
{
  ObColumnHeader::Type type_;
  int64_t ref_col_idx_; // referenced column index for rules between columns.
  int64_t last_prefix_length_;

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
};

template<int64_t max_size>
struct ObPreviousEncodingArray
{
  ObPreviousEncoding prev_encodings_[max_size];
  int64_t last_pos_;
  int64_t size_;

  ObPreviousEncodingArray() : last_pos_(0), size_(0) {}

  int put(const ObPreviousEncoding &prev);
  int64_t contain(const ObPreviousEncoding &prev);
  void reuse() { size_ = 0; }

  TO_STRING_KV(K_(prev_encodings), K_(last_pos), K_(size));
};

template<>
struct ObPreviousEncodingArray<2>
{
  ObPreviousEncoding prev_encodings_[2];
  int64_t last_pos_;
  int64_t size_;

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

  TO_STRING_KV(K_(last_pos), K_(size), "prev_encoding0", prev_encodings_[0],
      "prev_encoding1", prev_encodings_[1]);
};

template<>
struct ObPreviousEncodingArray<1>
{
  ObPreviousEncoding prev_encodings_[1];
  int64_t last_pos_;
  int64_t size_;

  ObPreviousEncodingArray() : last_pos_(0), size_(0) {}

  OB_INLINE int put(const ObPreviousEncoding &prev)
  {
    int ret = common::OB_SUCCESS;
    prev_encodings_[0] = prev;
    return ret;
  }

  TO_STRING_KV(K_(prev_encodings));
};

struct ObMicroBlockEncodingCtx
{
  static const int64_t MAX_PREV_ENCODING_COUNT = 2;
  int64_t macro_block_size_;
  int64_t micro_block_size_;
  int64_t rowkey_column_cnt_;
  int64_t column_cnt_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  ObMicroBlockEncoderOpt encoder_opt_;

  mutable int64_t estimate_block_size_;
  mutable int64_t real_block_size_;
  mutable int64_t micro_block_cnt_; // build micro block count
  mutable common::ObArray<ObPreviousEncodingArray<MAX_PREV_ENCODING_COUNT> > previous_encodings_;

  int64_t *column_encodings_;
  int64_t major_working_cluster_version_;
  common::ObRowStoreType row_store_type_;
  bool need_calc_column_chksum_;

  ObMicroBlockEncodingCtx() : macro_block_size_(0), micro_block_size_(0),
    rowkey_column_cnt_(0), column_cnt_(0), col_descs_(nullptr),
    encoder_opt_(), estimate_block_size_(0), real_block_size_(0), micro_block_cnt_(0),
    column_encodings_(nullptr), major_working_cluster_version_(0),
    row_store_type_(ENCODING_ROW_STORE), need_calc_column_chksum_(false)
  {
    previous_encodings_.set_attr(ObMemAttr(MTL_ID(), "MicroEncodeCtx"));
  }
  bool is_valid() const;
  TO_STRING_KV(K_(macro_block_size), K_(micro_block_size), K_(rowkey_column_cnt),
      K_(column_cnt), KP_(col_descs), K_(estimate_block_size), K_(real_block_size),
      K_(micro_block_cnt), K_(encoder_opt), K_(previous_encodings), KP_(column_encodings),
      K_(major_working_cluster_version), K_(row_store_type), K_(need_calc_column_chksum));
};

template <typename T, int64_t MAX_COUNT, int64_t BLOCK_SIZE>
class ObPodFix2dArray;

struct ObColumnEncodingCtx
{
  int64_t null_cnt_;
  int64_t nope_cnt_;
  uint64_t max_integer_;
  int64_t var_data_size_;
  int64_t dict_var_data_size_;
  int64_t fix_data_size_;
  int64_t max_string_size_;
  int64_t extend_value_bit_;
  const ObPodFix2dArray<ObDatum, 64 << 10, common::OB_MALLOC_MIDDLE_BLOCK_SIZE> *col_datums_;
  ObEncodingHashTable *ht_;
  ObMultiPrefixTree *prefix_tree_;
  const ObMicroBlockEncodingCtx *encoding_ctx_;
  bool detected_encoders_[ObColumnHeader::MAX_TYPE];
  mutable int64_t last_prefix_length_;
  bool only_raw_encoding_;
  bool is_refed_;
  bool need_sort_;

  ObColumnEncodingCtx() { reset(); }
  void reset() { memset(this, 0, sizeof(*this)); }
  void try_set_need_sort(const ObColumnHeader::Type type, const int64_t column_index)
  {
    ObObjTypeClass col_tc = encoding_ctx_->col_descs_->at(column_index).col_type_.get_type_class();
    ObObjTypeStoreClass col_sc = get_store_class_map()[col_tc];
    const bool encoding_type_need_sort = type == ObColumnHeader::DICT;
    need_sort_ = encoding_type_need_sort && !store_class_might_contain_lob_locator(col_sc);
  }

  TO_STRING_KV(K_(null_cnt), K_(nope_cnt), K_(max_integer), K_(var_data_size),
      K_(dict_var_data_size), K_(fix_data_size),
      K_(extend_value_bit), KP_(col_datums), KP_(ht), KP_(prefix_tree),
      K_(*encoding_ctx), K_(detected_encoders),
      K_(last_prefix_length), K_(max_string_size), K_(only_raw_encoding),
      K_(is_refed), K_(need_sort));
};

struct ObBloomFilterMacroBlockHeader
{
  ObBloomFilterMacroBlockHeader() { reset(); }
  void reset() { MEMSET(this, 0, sizeof(ObBloomFilterMacroBlockHeader)); }
  OB_INLINE bool is_valid() const{
    return header_size_ > 0
      && version_ >= BF_MACRO_BLOCK_HEADER_VERSION
      && BF_MACRO_BLOCK_HEADER_MAGIC == magic_
      && common::ObTabletID::INVALID_TABLET_ID != tablet_id_
      && snapshot_version_ >= 0
      && rowkey_column_count_ > 0
      && row_count_ > 0
      && occupy_size_ > 0
      && micro_block_count_ > 0
      && micro_block_data_offset_ > 0
      && micro_block_data_size_ > 0
      && data_checksum_ >= 0
      && ObMacroBlockCommonHeader::BloomFilterData == attr_;
  }
  TO_STRING_KV(K_(header_size),
      K_(version),
      K_(magic),
      K_(attr),
      K_(tablet_id),
      K_(snapshot_version),
      K_(rowkey_column_count),
      K_(row_count),
      K_(occupy_size),
      K_(micro_block_count),
      K_(micro_block_data_offset),
      K_(micro_block_data_size),
      K_(data_checksum),
      K_(compressor_type)
  );

  int32_t header_size_;
  int32_t version_;
  int32_t magic_;                    // magic number;
  int32_t attr_;
  uint64_t tablet_id_;
  int64_t snapshot_version_;
  int32_t rowkey_column_count_;
  int32_t row_count_;
  int32_t occupy_size_;              // occupy size of the whole macro block, include common header
  int32_t micro_block_count_;        // block count
  int32_t micro_block_data_offset_;
  int32_t micro_block_data_size_;
  int64_t data_checksum_;
  ObCompressorType compressor_type_;
};

class ObBufferReader;
class ObBufferWriter;

struct ObMacroBlockSchemaInfo final
{
public:
  static const int64_t MACRO_BLOCK_SCHEMA_INFO_HEADER_VERSION = 1;
  ObMacroBlockSchemaInfo();
  ~ObMacroBlockSchemaInfo() = default;
  NEED_SERIALIZE_AND_DESERIALIZE;
  int deep_copy(ObMacroBlockSchemaInfo *&new_schema_info, common::ObIAllocator &allocator) const;
  int64_t get_deep_copy_size() const;
  int64_t to_string(char *buffer, const int64_t length) const;
  bool is_valid() const { return (0 == column_number_ || (nullptr != compressor_ && nullptr != column_id_array_ && nullptr != column_type_array_ && nullptr != column_order_array_)); }
  bool operator==(const ObMacroBlockSchemaInfo &other) const;
  int16_t column_number_;
  int16_t rowkey_column_number_;
  int64_t schema_version_;
  int16_t schema_rowkey_col_cnt_;
  char *compressor_;
  uint16_t *column_id_array_;
  common::ObObjMeta *column_type_array_;
  common::ObOrderType *column_order_array_;
};

struct ObSSTableColumnMeta
{
  //For compatibility, the variables in this struct MUST NOT be deleted or moved.
  //You should ONLY add variables at the end.
  //Note that if you use complex structure as variables, the complex structure should also keep compatibility.
  static const int64_t SSTABLE_COLUMN_META_VERSION = 1;
  int64_t column_id_;
  int64_t column_default_checksum_;
  int64_t column_checksum_;
  ObSSTableColumnMeta();
  virtual ~ObSSTableColumnMeta();
  bool operator==(const ObSSTableColumnMeta &other) const;
  bool operator!=(const ObSSTableColumnMeta &other) const;
  void reset();
  bool is_valid() const
  {
    return column_id_ >= 0
        && column_default_checksum_ >= 0
        && column_checksum_ >= 0;
  }
  TO_STRING_KV(K_(column_id), K_(column_default_checksum), K_(column_checksum));
  OB_UNIS_VERSION_V(SSTABLE_COLUMN_META_VERSION);
};

class ObColClusterInfoMask
{
public:
  enum BYTES_LEN
  {
    BYTES_ZERO = 0,
    BYTES_UINT8 = 1,
    BYTES_UINT16 = 2,
    BYTES_UINT32 = 3,
    BYTES_MAX = 4,
  };
  static constexpr uint8_t BYTES_TYPE_TO_LEN[] =
  {
      0,
      1,
      2,
      4,
      UINT8_MAX,
  };
  OB_INLINE static uint8_t get_bytes_type_len(const BYTES_LEN type)
  {
    STATIC_ASSERT(static_cast<int64_t>(BYTES_MAX + 1) == ARRAYSIZEOF(BYTES_TYPE_TO_LEN), "type len array is mismatch");
    uint8_t ret_val = UINT8_MAX;
    if (OB_LIKELY(type >= BYTES_ZERO && type < BYTES_MAX)) {
      ret_val = BYTES_TYPE_TO_LEN[type];
    }
    return ret_val;
  }
public:
  ObColClusterInfoMask()
  : column_cnt_(0),
    info_mask_(0)
  {
  }
  static int get_serialized_size() { return sizeof(ObColClusterInfoMask); }
  static bool is_valid_col_idx_type(const BYTES_LEN col_idx_type)
  {
    return col_idx_type >= BYTES_ZERO && col_idx_type <= BYTES_UINT8;
  }
  static bool is_valid_offset_type(const BYTES_LEN column_offset_type)
  {
    return column_offset_type >= BYTES_ZERO && column_offset_type <= BYTES_UINT32;
  }
  OB_INLINE void reset() { column_cnt_ = 0; info_mask_ = 0; }
  OB_INLINE bool is_valid() const
  {
    return is_valid_offset_type(get_offset_type())
        && (!is_sparse_row_ || (is_valid_col_idx_type(get_column_idx_type()) && sparse_column_cnt_ >= 0))
        && column_cnt_ > 0;
  }
  OB_INLINE int64_t get_special_value_array_size(const int64_t serialize_column_cnt) const
  {
    return (sizeof(uint8_t) * serialize_column_cnt + 1) >> 1;
  }
  OB_INLINE int64_t get_total_array_size(const int64_t serialize_column_cnt) const
  {
    // offset_array + special_val_array + column_idx_array[SPARSE]
    return  (get_offset_type_len() + (is_sparse_row_ ? get_column_idx_type_len() : 0)) * serialize_column_cnt
                + (get_special_value_array_size(serialize_column_cnt));
  }
  OB_INLINE int set_offset_type(const BYTES_LEN column_offset_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_offset_type(column_offset_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column ofset bytes", K(column_offset_type));
    } else {
      offset_type_ = column_offset_type;
    }
    return ret;
  }
  OB_INLINE int set_column_idx_type(const BYTES_LEN column_idx_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_col_idx_type(column_idx_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column idx bytes", K(column_idx_type));
    } else {
      column_idx_type_ = column_idx_type;
    }
    return ret;
  }
  OB_INLINE BYTES_LEN get_offset_type() const { return (BYTES_LEN)offset_type_; }
  OB_INLINE uint8_t get_offset_type_len() const { return get_bytes_type_len(get_offset_type()); }
  OB_INLINE BYTES_LEN get_column_idx_type() const { return (BYTES_LEN)column_idx_type_; }
  OB_INLINE uint8_t get_column_idx_type_len() const { return get_bytes_type_len(get_column_idx_type()); }
  OB_INLINE void set_sparse_row_flag(const bool is_sparse_row) { is_sparse_row_ = is_sparse_row; }
  OB_INLINE bool is_sparse_row() const { return is_sparse_row_; }
  OB_INLINE void set_column_count(const uint8_t column_count) { column_cnt_ = column_count; }
  OB_INLINE uint8_t get_column_count() const { return column_cnt_; }
  OB_INLINE void set_sparse_column_count(const uint8_t sparse_column_count) { sparse_column_cnt_ = sparse_column_count; }
  OB_INLINE uint8_t get_sparse_column_count() const { return sparse_column_cnt_; }
  TO_STRING_KV(K_(column_cnt), K_(offset_type), K_(is_sparse_row), K_(column_idx_type), K_(sparse_column_cnt));

  static const int64_t SPARSE_COL_CNT_BYTES = 2;
  static const int64_t MAX_SPARSE_COL_CNT = (0x1 << SPARSE_COL_CNT_BYTES) - 1; // 3
private:

  uint8_t column_cnt_; // if row is single cluster, column_cnt= UINT8_MAX
  union
  {
    uint8_t info_mask_;
    struct
    {
      uint8_t offset_type_         :   2;
      uint8_t is_sparse_row_       :   1; // is sparse row
      uint8_t column_idx_type_     :   1; // means col_idx array bytes when is_sparse_row_ = true
      uint8_t sparse_column_cnt_   :   SPARSE_COL_CNT_BYTES; // 2 | means sparse column count when is_sparse_row_ = true
      uint8_t reserved_            :   2;
    };
  };
};

class ObRowHeader
{
public:
  ObRowHeader() { memset(this, 0, sizeof(*this)); }
  static int get_serialized_size() { return sizeof(ObRowHeader); }

  OB_INLINE bool is_valid() const
  {
    return column_cnt_ > 0 && rowkey_cnt_ > 0;
  }

  static const int64_t ROW_HEADER_VERSION_1 = 0;

  OB_INLINE uint8_t get_version() const { return version_; }
  OB_INLINE void set_version(const uint8_t version) { version_ = version; }
  OB_INLINE ObDmlRowFlag get_row_flag() const
  {
    return ObDmlRowFlag(row_flag_);
  }
  OB_INLINE void set_row_flag(const uint8_t row_flag) { row_flag_ = row_flag; }

  OB_INLINE uint8_t get_mvcc_row_flag() const { return multi_version_flag_; }
  OB_INLINE ObMultiVersionRowFlag get_row_multi_version_flag() const
  {
    return ObMultiVersionRowFlag(multi_version_flag_);
  }
  OB_INLINE void set_row_mvcc_flag(const uint8_t row_type_flag) { multi_version_flag_ = row_type_flag; }

  OB_INLINE uint16_t get_column_count() const { return column_cnt_; }
  OB_INLINE void set_column_count(const uint16_t column_count) { column_cnt_ = column_count; }

  OB_INLINE void clear_reserved_bits() { reserved8_ = 0; reserved_ = 0;}

  OB_INLINE void set_rowkey_count(const uint8_t rowkey_cnt) { rowkey_cnt_ = rowkey_cnt; }
  OB_INLINE uint8_t get_rowkey_count() const { return rowkey_cnt_; }

  OB_INLINE int64_t get_trans_id() const { return trans_id_; }
  OB_INLINE void set_trans_id(const int64_t trans_id) { trans_id_ = trans_id; }

  OB_INLINE bool is_single_cluster() const { return single_cluster_; }
  OB_INLINE void set_single_cluster(bool sigle_cluster) { single_cluster_ = sigle_cluster; }

  OB_INLINE int set_offset_type(const ObColClusterInfoMask::BYTES_LEN column_offset_type)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!ObColClusterInfoMask::is_valid_offset_type(column_offset_type))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid column offset bytes", K(column_offset_type));
    } else {
      offset_type_ = column_offset_type;
    }
    return ret;
  }
  OB_INLINE ObColClusterInfoMask::BYTES_LEN get_offset_type() const { return (ObColClusterInfoMask::BYTES_LEN)offset_type_; }
  OB_INLINE uint8_t get_offset_type_len() const { return ObColClusterInfoMask::get_bytes_type_len(get_offset_type()); }

  OB_INLINE bool has_rowkey_independent_cluster() const
  {
    return need_rowkey_independent_cluster(rowkey_cnt_);
  }
  OB_INLINE int64_t get_cluster_cnt() const
  {
    int64_t cluster_cnt = 0;
    if (single_cluster_) {
      cluster_cnt = 1;
    } else {
      cluster_cnt = calc_cluster_cnt(rowkey_cnt_, column_cnt_);
    }
    return cluster_cnt;
  }
  static int64_t calc_cluster_cnt(const int64_t rowkey_cnt, const int64_t column_cnt)
  {
    return need_rowkey_independent_cluster(rowkey_cnt) ?
              1 + calc_cluster_cnt_by_row_col_count(column_cnt - rowkey_cnt) :
              calc_cluster_cnt_by_row_col_count(column_cnt);
  }

  ObRowHeader &operator=(const ObRowHeader &src)
  {
    MEMCPY(this, &src, sizeof(ObRowHeader));
    return *this;
  }

  TO_STRING_KV(K_(version),
      K_(row_flag),
      K_(multi_version_flag),
      K_(column_cnt),
      K_(rowkey_cnt),
      K_(trans_id));

  static const int64_t CLUSTER_COLUMN_BYTES = 5;
  static const int64_t CLUSTER_COLUMN_CNT = 0x1 << CLUSTER_COLUMN_BYTES; // 32
  static const int64_t MAX_CLUSTER_COLUMN_CNT = 256;
  static const int64_t CLUSTER_COLUMN_CNT_MASK = CLUSTER_COLUMN_CNT - 1;
  static const int64_t USE_CLUSTER_COLUMN_COUNT = CLUSTER_COLUMN_CNT * 1.5; // 48
  static bool need_rowkey_independent_cluster(const int64_t rowkey_count)
  {
    return rowkey_count >= CLUSTER_COLUMN_CNT && rowkey_count <= USE_CLUSTER_COLUMN_COUNT;
  }
  static int64_t calc_cluster_cnt_by_row_col_count(const int64_t col_count)
  {
    return (col_count >> CLUSTER_COLUMN_BYTES) + ((col_count & CLUSTER_COLUMN_CNT_MASK) != 0);
  }
  static int64_t calc_cluster_idx(const int64_t column_idx)
  {
    return column_idx >> CLUSTER_COLUMN_BYTES;
  }
  static int64_t calc_column_cnt(const int64_t cluster_idx)
  {
    return cluster_idx << CLUSTER_COLUMN_BYTES;
  }
  static int64_t calc_column_idx_in_cluster(const int64_t column_count)
  {
    return column_count & CLUSTER_COLUMN_CNT_MASK;
  }
  enum SPECIAL_VAL {
    VAL_NORMAL = 0,
    VAL_OUTROW = 1,
    VAL_NOP = 2,
    VAL_NULL = 3,
    VAL_ENCODING_NORMAL = 4,
    VAL_MAX
  };

private:
  uint8_t version_;
  uint8_t row_flag_;
  uint8_t multi_version_flag_;
  uint8_t rowkey_cnt_;
  uint16_t column_cnt_;
  union
  {
    uint8_t header_info_mask_;
    struct
    {
      uint8_t offset_type_       :   2; // cluster offset
      uint8_t single_cluster_    :   1;
      uint8_t reserved_          :   5;
    };
  };
  uint8_t reserved8_;
  int64_t trans_id_;
};

struct ObSSTablePair
{
  int64_t data_version_;
  int64_t data_seq_;
  ObSSTablePair() : data_version_(0), data_seq_(0) {}
  ObSSTablePair(const int64_t dv, const int64_t seq)
      : data_version_(dv), data_seq_(seq)
  {
  }

  bool operator==(const ObSSTablePair &other) const
  {
    return data_version_ == other.data_version_ && data_seq_ == other.data_seq_;
  }

  bool operator!=(const ObSSTablePair &other) const
  {
    return !(*this == other);
  }
  uint64_t hash() const
  {
    return common::combine_two_ids(data_version_, data_seq_);
  }
  TO_STRING_KV(K_(data_version), K_(data_seq));
  OB_UNIS_VERSION(1);
};

//=========================oceanbase::blocksstable==================================
int write_compact_rowkey(
    ObBufferWriter &writer,
    const common::ObObj *endkey,
    const int64_t count,
    const common::ObRowStoreType row_store_type);
int read_compact_rowkey(
    ObBufferReader &reader,
    const common::ObObjMeta *column_type_array,
    common::ObObj *endkey,
    const int64_t count,
    const common::ObRowStoreType row_store_type);

class ObSimpleMacroBlockInfo final
{
public:
  ObSimpleMacroBlockInfo();
  ~ObSimpleMacroBlockInfo() = default;
  void reset();
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (!macro_id_.is_valid()) {
      J_NAME("nothing");
    } else {
      J_OBJ_START();
      J_KV(K_(macro_id), K_(last_access_time), K_(ref_cnt));
      J_OBJ_END();
    }
    return pos;
  }
public:
  MacroBlockId macro_id_;
  int64_t last_access_time_;
  int64_t ref_cnt_;
};

class ObMacroBlockMarkerStatus final
{
public:
  ObMacroBlockMarkerStatus();
  ~ObMacroBlockMarkerStatus() = default;
  void reuse();
  void reset();
  void fill_comment(char *buf, const int32_t buf_len) const;
  TO_STRING_KV(K_(total_block_count),
               K_(reserved_block_count),
               K_(linked_block_count),
               K_(tmp_file_count),
               K_(data_block_count),
               K_(shared_data_block_count),
               K_(index_block_count),
               K_(ids_block_count),
               K_(disk_block_count),
               K_(bloomfiter_count),
               K_(hold_count),
               K_(pending_free_count),
               K_(free_count),
               K_(shared_meta_block_count),
               K_(mark_cost_time),
               K_(sweep_cost_time),
               KTIME_(start_time),
               KTIME_(last_end_time),
               K_(mark_finished),
               K_(hold_info));
public:
  int64_t total_block_count_;
  int64_t reserved_block_count_;
  int64_t linked_block_count_;
  int64_t tmp_file_count_;
  int64_t data_block_count_;
  int64_t shared_data_block_count_;
  int64_t index_block_count_;
  int64_t ids_block_count_;
  int64_t disk_block_count_;
  int64_t bloomfiter_count_;
  int64_t hold_count_;
  int64_t pending_free_count_;
  int64_t free_count_;
  int64_t shared_meta_block_count_;
  int64_t mark_cost_time_;
  int64_t sweep_cost_time_;
  int64_t start_time_;
  int64_t last_end_time_;
  bool mark_finished_;
  ObSimpleMacroBlockInfo hold_info_;
};

/****************************** following codes are inline functions ****************************/
enum ObRecordHeaderVersion
{
  RECORD_HEADER_VERSION_V2 = 2,
  RECORD_HEADER_VERSION_V3 = 3
};

struct ObRecordHeaderV3
{
public:
  ObRecordHeaderV3();
  ~ObRecordHeaderV3() = default;
  static int deserialize_and_check_record(const char *ptr, const int64_t size,
      const int16_t magic, const char *&payload_ptr, int64_t &payload_size);
  static int deserialize_and_check_record(const char *ptr, const int64_t size, const int16_t magic);
  int check_and_get_record(const char *ptr, const int64_t size, const int16_t magic, const char *&payload_ptr, int64_t &payload_size) const;
  int check_record(const char *ptr, const int64_t size, const int16_t magic) const;
  static int64_t get_serialize_size(const int64_t header_version, const int64_t column_cnt) {
    return RECORD_HEADER_VERSION_V2 == header_version ? sizeof(ObRecordCommonHeader)
        : sizeof(ObRecordCommonHeader) + column_cnt * sizeof(column_checksums_[0]);
  }
  void set_header_checksum();
  int check_header_checksum() const;
  inline bool is_compressed_data() const { return data_length_ != data_zlength_; }
  NEED_SERIALIZE_AND_DESERIALIZE;

private:
  int check_payload_checksum(const char *buf, const int64_t len) const;

public:
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
    int32_t data_encoding_length_;
    uint16_t row_count_;
    uint16_t column_cnt_;
  };
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
  uint16_t row_count_;
  uint16_t column_cnt_;
  int64_t *column_checksums_;

  TO_STRING_KV(K_(magic), K_(header_length), K_(version), K_(header_checksum),
      K_(reserved16), K_(data_length), K_(data_zlength), K_(data_checksum),
      K_(data_encoding_length), K_(row_count), K_(column_cnt), KP(column_checksums_));
};

struct ObMicroBlockDesMeta
{
public:
  ObMicroBlockDesMeta()
    : compressor_type_(common::INVALID_COMPRESSOR), encrypt_id_(0),
      master_key_id_(0), encrypt_key_(nullptr) {}
  ObMicroBlockDesMeta(const common::ObCompressorType compressor_type, const int64_t encrypt_id,
      const int64_t master_key_id, const char *encrypt_key)
    : compressor_type_(compressor_type), encrypt_id_(encrypt_id),
      master_key_id_(master_key_id), encrypt_key_(encrypt_key) {}
  TO_STRING_KV(K_(compressor_type), K_(encrypt_id), K_(master_key_id),
      KPHEX_(encrypt_key, nullptr == encrypt_key_ ? 0 : share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH));
  OB_INLINE bool is_valid() const
  {
    return common::ObCompressorType::INVALID_COMPRESSOR < compressor_type_
        && compressor_type_ < common::ObCompressorType::MAX_COMPRESSOR;
  }
public:
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  const char *encrypt_key_;
};

enum ObDDLMacroBlockType
{
  DDL_MB_INVALID_TYPE = 0,
  DDL_MB_DATA_TYPE = 1,
  DDL_MB_INDEX_TYPE = 2,
};

struct ObDDLMacroBlockRedoInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLMacroBlockRedoInfo();
  ~ObDDLMacroBlockRedoInfo() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(table_key),  K_(data_buffer), K_(block_type), K_(logic_id), K_(start_scn));
public:
  storage::ObITable::TableKey table_key_;
  ObString data_buffer_;
  ObDDLMacroBlockType block_type_;
  ObLogicMacroBlockId logic_id_;
  share::SCN start_scn_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
