// owner: zs475329
// owner group: storage
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define protected public
#define private public
#include "common/ob_version_def.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share::schema;

namespace unittest
{
class MockMacroBlockId_432 final
{
public:
  MockMacroBlockId_432()
  : first_id_(0),
    second_id_(INT64_MAX),
    third_id_(0)
  {
  }

  MockMacroBlockId_432(const int64_t first_id, const int64_t second_id, const int64_t third_id)
  {
  first_id_ = first_id;
  second_id_ = second_id;
  third_id_ = third_id;
  }

  MockMacroBlockId_432(const MockMacroBlockId_432 &id)
  {
  first_id_ = id.first_id_;
  second_id_ = id.second_id_;
  third_id_ = id.third_id_;
  }

  OB_INLINE void reset()
  {
    first_id_ = 0;
    second_id_ = INT64_MAX;
    third_id_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return MACRO_BLOCK_ID_VERSION == version_
        && id_mode_ < (uint64_t)ObMacroBlockIdMode::ID_MODE_MAX
        && second_id_ >= AUTONOMIC_BLOCK_INDEX && second_id_ < INT64_MAX
        && 0 == third_id_;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    switch ((ObMacroBlockIdMode)id_mode_) {
    case ObMacroBlockIdMode::ID_MODE_LOCAL:
      databuff_printf(buf, buf_len, pos,
                      "[%ld](ver=%lu,mode=%lu,seq=%lu)",
                      second_id_,
                      (uint64_t) version_,
                      (uint64_t) id_mode_,
                      write_seq_);
      break;
    default:
      databuff_printf(buf, buf_len, pos,
                      "(ver=%lu,mode=%lu,1st=%ld,2nd=%ld,3rd=%ld)",
                      (uint64_t) version_,
                      (uint64_t) id_mode_,
                      first_id_, second_id_, third_id_);
      break;
    }
    return pos;
  }

  // General GETTER/SETTER
  int64_t first_id() const { return first_id_; }
  int64_t second_id() const { return second_id_; }
  int64_t third_id() const { return third_id_; }

  // Local mode
  int64_t block_index() const { return block_index_; }
  void set_block_index(const int64_t block_index) { block_index_ = block_index; }
  int64_t write_seq() const { return write_seq_; }
  void set_write_seq(const uint64_t write_seq) { write_seq_ = write_seq; }
  int64_t device_id() const { return device_id_; }
  void set_device_id(const uint64_t device_id) { device_id_ = device_id; }

  MockMacroBlockId_432& operator =(const MockMacroBlockId_432 &other)
  {
    first_id_ = other.first_id_;
    second_id_ = other.second_id_;
    third_id_ = other.third_id_;
    return *this;
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    const int64_t ser_len = get_serialize_size();
    int64_t new_pos = pos;
    if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id.", K(ret), K(*this));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, first_id_))) {
      LOG_WARN("serialize first id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, second_id_))) {
      LOG_WARN("serialize second id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, third_id_))) {
      LOG_WARN("serialize third id failed.", K(ret), K(new_pos), K(buf_len), K(ser_len), K(*this));
    } else {
      pos = new_pos;
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &first_id_))) {
      LOG_WARN("decode first_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &second_id_))) {
      LOG_WARN("decode second_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &third_id_))) {
      LOG_WARN("decode third_id_ failed.", K(ret), K(new_pos), K(data_len), K(*this));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id.", K(ret), K(*this));
    } else {
      pos = new_pos;
    }
    return ret;
  }

  int64_t get_serialize_size(void) const
  {
    int64_t len = 0;
    len += serialization::encoded_length_i64(first_id_);
    len += serialization::encoded_length_i64(second_id_);
    len += serialization::encoded_length_i64(third_id_);
    return len;
  }

public:
  static const int64_t MACRO_BLOCK_ID_VERSION = 0;

  static const int64_t EMPTY_ENTRY_BLOCK_INDEX = -1;
  static const int64_t AUTONOMIC_BLOCK_INDEX = -1;

  static const uint64_t SF_BIT_WRITE_SEQ = 52;
  static const uint64_t SF_BIT_ID_MODE = 8;
  static const uint64_t SF_BIT_VERSION = 4;

  static const uint64_t MAX_WRITE_SEQ = (0x1UL << MockMacroBlockId_432::SF_BIT_WRITE_SEQ) - 1;

private:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

private:
  union {
    int64_t first_id_;
    struct {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;
      uint64_t id_mode_   : SF_BIT_ID_MODE;
      uint64_t version_   : SF_BIT_VERSION;
    };
  };
  union {
    int64_t second_id_;
    int64_t block_index_;    // the block index in the block_file when Local mode
  };
  union {
    int64_t third_id_;
    struct {
      uint64_t device_id_ : 8;
      uint64_t reserved_ : 56;
    };
  };
};

struct MockMacroDataSeq_432
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
    REBUILD_MACRO_BLOCK_MERGE = 2,
  };

  MockMacroDataSeq_432() : macro_data_seq_(0) {}
  MockMacroDataSeq_432(const int64_t data_seq) : macro_data_seq_(data_seq) {}
  virtual ~MockMacroDataSeq_432() = default;
  MockMacroDataSeq_432 &operator=(const MockMacroDataSeq_432 &other)
  {
    if (this != &other) {
      macro_data_seq_ = other.macro_data_seq_;
    }
    return *this;
  }
  bool operator ==(const MockMacroDataSeq_432 &other) const { return macro_data_seq_ == other.macro_data_seq_; }
  bool operator !=(const MockMacroDataSeq_432 &other) const { return macro_data_seq_ != other.macro_data_seq_; }
  OB_INLINE void reset() { macro_data_seq_ = 0; }
  OB_INLINE int64_t get_data_seq() const { return macro_data_seq_; }
  OB_INLINE int64_t get_parallel_idx() const { return parallel_idx_; }
  OB_INLINE bool is_valid() const { return macro_data_seq_ >= 0; }
  OB_INLINE bool is_data_block() const { return block_type_ == DATA_BLOCK; }
  OB_INLINE bool is_index_block() const { return block_type_ == INDEX_BLOCK; }
  OB_INLINE bool is_meta_block() const { return block_type_ == META_BLOCK; }
  OB_INLINE bool is_major_merge() const { return merge_type_ == MAJOR_MERGE; }
  OB_INLINE void set_rebuild_merge_type()
  {
    merge_type_ = REBUILD_MACRO_BLOCK_MERGE;
  }
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
  int64_t get_serialize_size() const
  {
    int64_t len = 0;
    len += serialization::encoded_length_vi64(macro_data_seq_);
    return len;
  }

  int serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(buf_len));
    } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, macro_data_seq_))) {
      LOG_WARN("failed to serialize data seq", K(ret));
    }
    return ret;
  }

  int deserialize(
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len < pos)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &macro_data_seq_))) {
      LOG_WARN("failed to deserialize data seq", K(ret));
    }
    return ret;
  }
  TO_STRING_KV(K_(data_seq), K_(parallel_idx), K_(block_type), K_(merge_type),
      K_(sstable_logic_seq), K_(reserved), K_(sign), K_(macro_data_seq));
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

struct MockLogicMacroBlockId_432
{
private:
  static const int64_t LOGIC_BLOCK_ID_VERSION = 1;

public:
  MockLogicMacroBlockId_432()
    : data_seq_(), logic_version_(0), tablet_id_(0 /* ObTabletID::INVALID_TABLET_ID */), info_(0)
  {}
  MockLogicMacroBlockId_432(const int64_t data_seq, const uint64_t logic_version, const int64_t tablet_id)
    : data_seq_(data_seq), logic_version_(logic_version), tablet_id_(tablet_id), info_(0)
  {}

  void reset()
  {
    data_seq_.reset();
    logic_version_ = 0;
    tablet_id_ = 0;
    info_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return data_seq_.is_valid() && logic_version_ > 0 && tablet_id_ > 0;
  }

  TO_STRING_KV(K_(data_seq), K_(logic_version), K_(tablet_id), K_(column_group_idx), K_(is_mds));

public:
  MockMacroDataSeq_432 data_seq_;
  uint64_t logic_version_;
  int64_t tablet_id_;

  union {
    uint64_t info_;
    struct {
      uint64_t column_group_idx_ : 16;
      bool is_mds_               :  1;
      uint64_t reserved_         : 47;
    };
  };
  OB_UNIS_VERSION(LOGIC_BLOCK_ID_VERSION);
};

OB_SERIALIZE_MEMBER(MockLogicMacroBlockId_432,
                    data_seq_,
                    logic_version_,
                    tablet_id_,
                    info_);

struct MockLogicMacroBlockId_425
{
private:
  static const int64_t LOGIC_BLOCK_ID_VERSION = 1;

public:
  MockLogicMacroBlockId_425()
    : data_seq_(0), logic_version_(0), tablet_id_(0 /* ObTabletID::INVALID_TABLET_ID */)
  {}
  MockLogicMacroBlockId_425(const int64_t data_seq, const uint64_t logic_version, const int64_t tablet_id)
    : data_seq_(data_seq), logic_version_(logic_version), tablet_id_(tablet_id)
  {}
  void reset(){
    data_seq_ = 0;
    logic_version_ = 0;
    tablet_id_ = 0;
  }
  OB_INLINE bool is_valid() const
  {
    return data_seq_ >= 0 && logic_version_ > 0 && tablet_id_ > 0;
  }
  TO_STRING_KV(K_(data_seq), K_(logic_version), K_(tablet_id));

public:
  int64_t data_seq_;
  uint64_t logic_version_;
  int64_t tablet_id_;
  OB_UNIS_VERSION(LOGIC_BLOCK_ID_VERSION);
};

OB_SERIALIZE_MEMBER(MockLogicMacroBlockId_425,
                    data_seq_,
                    logic_version_,
                    tablet_id_);

class MockDataBlockMetaVal_4_2_5
{
  private:
  static const int32_t DATA_BLOCK_META_VAL_VERSION = 1;
public:
  MockDataBlockMetaVal_4_2_5()
  : version_(DATA_BLOCK_META_VAL_VERSION),
    length_(0),
    data_checksum_(0),
    rowkey_count_(0),
    column_count_(0),
    micro_block_count_(0),
    occupy_size_(0),
    data_size_(0),
    data_zsize_(0),
    original_size_(0),
    progressive_merge_round_(0),
    block_offset_(0),
    block_size_(0),
    row_count_(0),
    row_count_delta_(0),
    max_merged_trans_version_(0),
    is_encrypted_(false),
    is_deleted_(false),
    contain_uncommitted_row_(false),
    is_last_row_last_flag_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    master_key_id_(0),
    encrypt_id_(0),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    schema_version_(0),
    snapshot_version_(0),
    logic_id_(),
    macro_id_(),
    column_checksums_(sizeof(int64_t), ModulePageAllocator("MacroMetaChksum", MTL_ID())),
    has_string_out_row_(false),
    all_lob_in_row_(false)
  {
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  }
  ~MockDataBlockMetaVal_4_2_5()
  {
    reset();
  }
  void reset() {
    length_ = 0;
    data_checksum_ = 0;
    rowkey_count_ = 0;
    column_count_ = 0;
    micro_block_count_ = 0;
    occupy_size_ = 0;
    data_size_ = 0;
    data_zsize_ = 0;
    original_size_ = 0;
    progressive_merge_round_ = 0;
    block_offset_ = 0;
    block_size_ = 0;
    row_count_ = 0;
    row_count_delta_ = 0;
    max_merged_trans_version_ = 0;
    is_encrypted_ = false;
    is_deleted_ = false;
    contain_uncommitted_row_ = false;
    is_last_row_last_flag_ = false;
    compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
    master_key_id_ = 0;
    encrypt_id_ = 0;
    MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
    schema_version_ = 0;
    snapshot_version_ = 0;
    logic_id_.reset();
    macro_id_.reset();
    column_checksums_.reset();
    has_string_out_row_ = false;
    all_lob_in_row_ = false;
  }
  bool is_valid() const
  {
  return DATA_BLOCK_META_VAL_VERSION == version_
      && rowkey_count_ > 0
      && column_count_ > 0
      && micro_block_count_ >= 0
      && occupy_size_ >= 0
      && data_size_ >= 0
      && data_zsize_ >= 0
      && original_size_ >= 0
      && progressive_merge_round_ >= 0
      && block_offset_ >= 0
      && block_size_ >= 0
      && row_count_ >= 0
      && max_merged_trans_version_ >= 0
      && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
      && row_store_type_ < ObRowStoreType::MAX_ROW_STORE
      && logic_id_.is_valid()
      && macro_id_.is_valid();
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
    } else {
      int64_t start_pos = pos;
      const_cast<MockDataBlockMetaVal_4_2_5 *>(this)->length_ = get_serialize_size();
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
        LOG_WARN("fail to encode version", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
        LOG_WARN("fail to encode length", K(ret), K(buf_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect buf_len", K(ret), K(buf_len), K(pos));
      } else {
        MEMCPY(buf + pos, encrypt_key_, sizeof(encrypt_key_)); // do not serialize char[]
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_ENCODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_);
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KPC(this));
        }
      }
    }
    return ret;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
    } else {
      int64_t start_pos = pos;
      if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
        LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(version_ != DATA_BLOCK_META_VAL_VERSION)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("object version mismatch", K(ret), K(version_));
      } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
        LOG_WARN("fail to decode length", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect data_len", K(ret), K(data_len), K(pos));
      } else {
        MEMCPY(encrypt_key_, buf + pos, sizeof(encrypt_key_));
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_DECODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_);
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
        }
      }
    }
    return ret;
  }
  int64_t get_serialize_size() const{
    int64_t len = 0;
    len += serialization::encoded_length_i32(version_);
    len += serialization::encoded_length_i32(length_);
    len += sizeof(encrypt_key_);
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                data_checksum_,
                rowkey_count_,
                column_count_,
                micro_block_count_,
                occupy_size_,
                data_size_,
                data_zsize_,
                progressive_merge_round_,
                block_offset_,
                block_size_,
                row_count_,
                row_count_delta_,
                max_merged_trans_version_,
                is_encrypted_,
                is_deleted_,
                contain_uncommitted_row_,
                compressor_type_,
                master_key_id_,
                encrypt_id_,
                row_store_type_,
                schema_version_,
                snapshot_version_,
                logic_id_,
                macro_id_,
                column_checksums_,
                original_size_,
                has_string_out_row_,
                all_lob_in_row_,
                is_last_row_last_flag_);
    return len;
  }
  int64_t get_max_serialize_size()
  {
    int64_t len = sizeof(*this);
    len -= sizeof(column_checksums_);
    len += sizeof(int64_t); // serialize column count
    len += sizeof(int64_t) * column_count_; // serialize each checksum
    return len;
  }
  TO_STRING_KV(K_(version), K_(length), K_(data_checksum), K_(rowkey_count),
        K_(rowkey_count), K_(column_count), K_(micro_block_count), K_(occupy_size), K_(data_size),
        K_(data_zsize), K_(original_size), K_(progressive_merge_round), K_(block_offset), K_(block_size), K_(row_count),
        K_(row_count_delta), K_(max_merged_trans_version), K_(is_encrypted),
        K_(is_deleted), K_(contain_uncommitted_row), K_(compressor_type),
        K_(master_key_id), K_(encrypt_id), K_(encrypt_key), K_(row_store_type),
        K_(schema_version), K_(snapshot_version), K_(is_last_row_last_flag),
        K_(logic_id), K_(macro_id), K_(column_checksums), K_(has_string_out_row), K_(all_lob_in_row));
public:
  int32_t version_;
  int32_t length_;
  int64_t data_checksum_;
  int64_t rowkey_count_;
  int64_t column_count_;
  int64_t micro_block_count_;
  int64_t occupy_size_;   // size of whole macro block (including headers)
  int64_t data_size_; // sum of size of micro blocks (after encoding)
  int64_t data_zsize_;    // sum of size of compressed/encrypted micro blocks
  int64_t original_size_; // sum of size of original micro blocks
  int64_t progressive_merge_round_;
  int64_t block_offset_;  // offset of n-1 level index micro blocks
  int64_t block_size_;    // size of n-1 level index micro blocks
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  bool is_encrypted_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_last_row_last_flag_;
  ObCompressorType compressor_type_;
  int64_t master_key_id_;
  int64_t encrypt_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObRowStoreType row_store_type_;
  uint64_t schema_version_;
  int64_t snapshot_version_;
  MockLogicMacroBlockId_425 logic_id_;
  MockMacroBlockId_432 macro_id_;
  common::ObSEArray<int64_t, 4> column_checksums_;
  bool has_string_out_row_;
  bool all_lob_in_row_;

private:
  DISALLOW_COPY_AND_ASSIGN(MockDataBlockMetaVal_4_2_5);
};

class MockDataBlockMetaVal_4_3_0_0
{
  public:
  static const int32_t MockDataBlockMetaVal_4_3_0_0_VERSION = 1;
public:
  MockDataBlockMetaVal_4_3_0_0()
  : version_(0),
    length_(0),
    data_checksum_(0),
    rowkey_count_(0),
    column_count_(0),
    micro_block_count_(0),
    occupy_size_(0),
    data_size_(0),
    data_zsize_(0),
    original_size_(0),
    progressive_merge_round_(0),
    block_offset_(0),
    block_size_(0),
    row_count_(0),
    row_count_delta_(0),
    max_merged_trans_version_(0),
    is_encrypted_(false),
    is_deleted_(false),
    contain_uncommitted_row_(false),
    is_last_row_last_flag_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    master_key_id_(0),
    encrypt_id_(0),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    schema_version_(0),
    snapshot_version_(0),
    logic_id_(),
    macro_id_(),
    column_checksums_(),
    has_string_out_row_(false),
    all_lob_in_row_(false),
    agg_row_len_(0),
    agg_row_buf_(nullptr)
  {
  }
  ~MockDataBlockMetaVal_4_3_0_0() = default;
  void reset() {
    version_ = 0;
    length_ = 0;
    data_checksum_ = 0;
    rowkey_count_ = 0;
    column_count_ = 0;
    micro_block_count_ = 0;
    occupy_size_ = 0;
    data_size_ = 0;
    data_zsize_ = 0;
    original_size_ = 0;
    progressive_merge_round_ = 0;
    block_offset_ = 0;
    block_size_ = 0;
    row_count_ = 0;
    row_count_delta_ = 0;
    max_merged_trans_version_ = 0;
    is_encrypted_ = false;
    is_deleted_ = false;
    contain_uncommitted_row_ = false;
    is_last_row_last_flag_ = false;
    compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
    master_key_id_ = 0;
    encrypt_id_ = 0;
    row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
    schema_version_ = 0;
    snapshot_version_ = 0;
    logic_id_.reset();
    macro_id_.reset();
    column_checksums_.reset();
    has_string_out_row_ = false;
    all_lob_in_row_ = false;
    agg_row_len_ = 0;
    agg_row_buf_ = nullptr;
  }
  bool is_valid() const
  {
  return (MockDataBlockMetaVal_4_3_0_0_VERSION == version_)
      && rowkey_count_ >= 0
      && column_count_ > 0
      && micro_block_count_ >= 0
      && occupy_size_ >= 0
      && data_size_ >= 0
      && data_zsize_ >= 0
      && original_size_ >= 0
      && progressive_merge_round_ >= 0
      && block_offset_ >= 0
      && block_size_ >= 0
      && row_count_ >= 0
      && max_merged_trans_version_ >= 0
      && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
      && row_store_type_ < ObRowStoreType::MAX_ROW_STORE
      && logic_id_.is_valid()
      && macro_id_.is_valid()
      && (0 == agg_row_len_ || nullptr != agg_row_buf_);
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
    } else {
      int64_t start_pos = pos;
      const_cast<MockDataBlockMetaVal_4_3_0_0 *>(this)->length_ = get_serialize_size();
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
        LOG_WARN("fail to encode version", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
        LOG_WARN("fail to encode length", K(ret), K(buf_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect buf_len", K(ret), K(buf_len), K(pos));
      } else {
        MEMCPY(buf + pos, encrypt_key_, sizeof(encrypt_key_)); // do not serialize char[]
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_ENCODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          MEMCPY(buf + pos, agg_row_buf_, agg_row_len_);
          pos += agg_row_len_;
          if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
    } else {
      int64_t start_pos = pos;
      if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
        LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(version_ != MockDataBlockMetaVal_4_3_0_0_VERSION)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("object version mismatch", K(ret), K(version_));
      } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
        LOG_WARN("fail to decode length", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect data_len", K(ret), K(data_len), K(pos));
      } else {
        MEMCPY(encrypt_key_, buf + pos, sizeof(encrypt_key_));
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_DECODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          if (agg_row_len_ > 0) {
            agg_row_buf_ = buf + pos;
            pos += agg_row_len_;
          }
          if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int64_t get_max_serialize_size() const
  {
    int64_t len = sizeof(*this);
    len -= sizeof(column_checksums_);
    len -= sizeof(agg_row_buf_);
    len += sizeof(int64_t); // serialize column count
    len += sizeof(int64_t) * column_count_; // serialize each checksum
    len += agg_row_len_;
    return len;
  }
  int64_t get_serialize_size() const
  {
    int64_t len = 0;
    len += serialization::encoded_length_i32(version_);
    len += serialization::encoded_length_i32(length_);
    len += sizeof(encrypt_key_);
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                data_checksum_,
                rowkey_count_,
                column_count_,
                micro_block_count_,
                occupy_size_,
                data_size_,
                data_zsize_,
                progressive_merge_round_,
                block_offset_,
                block_size_,
                row_count_,
                row_count_delta_,
                max_merged_trans_version_,
                is_encrypted_,
                is_deleted_,
                contain_uncommitted_row_,
                compressor_type_,
                master_key_id_,
                encrypt_id_,
                row_store_type_,
                schema_version_,
                snapshot_version_,
                logic_id_,
                macro_id_,
                column_checksums_,
                original_size_,
                has_string_out_row_,
                all_lob_in_row_,
                is_last_row_last_flag_,
                agg_row_len_);
    len += agg_row_len_;
    return len;
  }

  TO_STRING_KV(K_(version), K_(length), K_(data_checksum), K_(rowkey_count),
        K_(column_count), K_(micro_block_count), K_(occupy_size), K_(data_size),
        K_(data_zsize), K_(original_size), K_(progressive_merge_round), K_(block_offset), K_(block_size), K_(row_count),
        K_(row_count_delta), K_(max_merged_trans_version), K_(is_encrypted),
        K_(is_deleted), K_(contain_uncommitted_row), K_(compressor_type),
        K_(master_key_id), K_(encrypt_id), K_(encrypt_key), K_(row_store_type),
        K_(schema_version), K_(snapshot_version), K_(is_last_row_last_flag),
        K_(logic_id), K_(macro_id), K_(column_checksums), K_(has_string_out_row), K_(all_lob_in_row),
          K_(agg_row_len), KP_(agg_row_buf));
public:
  int32_t version_;
  int32_t length_;
  int64_t data_checksum_;
  int64_t rowkey_count_;
  int64_t column_count_;
  int64_t micro_block_count_;
  int64_t occupy_size_;   // size of whole macro block (including headers)
  int64_t data_size_; // sum of size of micro blocks (after encoding)
  int64_t data_zsize_;    // sum of size of compressed/encrypted micro blocks
  int64_t original_size_; // sum of size of original micro blocks
  int64_t progressive_merge_round_;
  int64_t block_offset_;  // offset of n-1 level index micro blocks
  int64_t block_size_;    // size of n-1 level index micro blocks
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  bool is_encrypted_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_last_row_last_flag_;
  ObCompressorType compressor_type_;
  int64_t master_key_id_;
  int64_t encrypt_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObRowStoreType row_store_type_;
  uint64_t schema_version_;
  int64_t snapshot_version_;
  MockLogicMacroBlockId_432 logic_id_;
  MockMacroBlockId_432 macro_id_;
  common::ObSEArray<int64_t, 4> column_checksums_;
  bool has_string_out_row_;
  bool all_lob_in_row_;
  int64_t agg_row_len_; // size of agg_row_buf_
  const char *agg_row_buf_; // data buffer for pre aggregated row
private:
  DISALLOW_COPY_AND_ASSIGN(MockDataBlockMetaVal_4_3_0_0);
};

class MockDataBlockMetaVal_4_3_3
{
public:
  static const int32_t DATA_BLOCK_META_VAL_VERSION = 1;
  static const int32_t DATA_BLOCK_META_VAL_VERSION_V2 = 2;
public:
  MockDataBlockMetaVal_4_3_3()
  : version_(DATA_BLOCK_META_VAL_VERSION_V2),
    length_(0),
    data_checksum_(0),
    rowkey_count_(0),
    column_count_(0),
    micro_block_count_(0),
    occupy_size_(0),
    data_size_(0),
    data_zsize_(0),
    original_size_(0),
    progressive_merge_round_(0),
    block_offset_(0),
    block_size_(0),
    row_count_(0),
    row_count_delta_(0),
    max_merged_trans_version_(0),
    is_encrypted_(false),
    is_deleted_(false),
    contain_uncommitted_row_(false),
    is_last_row_last_flag_(false),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    master_key_id_(0),
    encrypt_id_(0),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    schema_version_(0),
    snapshot_version_(0),
    logic_id_(),
    macro_id_(),
    column_checksums_(sizeof(int64_t), ModulePageAllocator("MacroMetaChksum", MTL_ID())),
    has_string_out_row_(false),
    all_lob_in_row_(false),
    agg_row_len_(0),
    agg_row_buf_(nullptr),
    ddl_end_row_offset_(-1)
  {
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  }
  ~MockDataBlockMetaVal_4_3_3(){
    reset();
  }
  void reset()
  {
    length_ = 0;
    data_checksum_ = 0;
    rowkey_count_ = 0;
    column_count_ = 0;
    micro_block_count_ = 0;
    occupy_size_ = 0;
    data_size_ = 0;
    data_zsize_ = 0;
    original_size_ = 0;
    progressive_merge_round_ = 0;
    block_offset_ = 0;
    block_size_ = 0;
    row_count_ = 0;
    row_count_delta_ = 0;
    max_merged_trans_version_ = 0;
    is_encrypted_ = false;
    is_deleted_ = false;
    contain_uncommitted_row_ = false;
    is_last_row_last_flag_ = false;
    compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
    master_key_id_ = 0;
    encrypt_id_ = 0;
    MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
    schema_version_ = 0;
    snapshot_version_ = 0;
    logic_id_.reset();
    macro_id_.reset();
    column_checksums_.reset();
    has_string_out_row_ = false;
    all_lob_in_row_ = false;
    agg_row_len_ = 0;
    agg_row_buf_ = nullptr;
    ddl_end_row_offset_ = -1;
  }
  bool is_valid() const
  {
    return (DATA_BLOCK_META_VAL_VERSION == version_ || DATA_BLOCK_META_VAL_VERSION_V2 == version_)
        && rowkey_count_ >= 0
        && column_count_ > 0
        && micro_block_count_ >= 0
        && occupy_size_ >= 0
        && data_size_ >= 0
        && data_zsize_ >= 0
        && original_size_ >= 0
        && progressive_merge_round_ >= 0
        && block_offset_ >= 0
        && block_size_ >= 0
        && row_count_ >= 0
        && max_merged_trans_version_ >= 0
        && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
        && row_store_type_ < ObRowStoreType::MAX_ROW_STORE
        && logic_id_.is_valid()
        && macro_id_.is_valid()
        && agg_row_len_ >= 0
        && ((0 == agg_row_len_ && nullptr == agg_row_buf_) || (0 < agg_row_len_ && nullptr != agg_row_buf_))
        && (ddl_end_row_offset_ == -1 || (version_ >= DATA_BLOCK_META_VAL_VERSION_V2 && ddl_end_row_offset_ >= 0));
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
    } else {
      int64_t start_pos = pos;
      const_cast<MockDataBlockMetaVal_4_3_3 *>(this)->length_ = get_serialize_size();
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
        LOG_WARN("fail to encode version", K(ret), K(buf_len), K(pos));
      } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, length_))) {
        LOG_WARN("fail to encode length", K(ret), K(buf_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect buf_len", K(ret), K(buf_len), K(pos));
      } else {
        MEMCPY(buf + pos, encrypt_key_, sizeof(encrypt_key_)); // do not serialize char[]
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_ENCODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          MEMCPY(buf + pos, agg_row_buf_, agg_row_len_);
          pos += agg_row_len_;
          if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
            LST_DO_CODE(OB_UNIS_ENCODE, ddl_end_row_offset_);
          }
          if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
    } else {
      int64_t start_pos = pos;
      if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
        LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(version_ != DATA_BLOCK_META_VAL_VERSION && version_ != DATA_BLOCK_META_VAL_VERSION_V2)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("object version mismatch", K(ret), K(version_));
      } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
        LOG_WARN("fail to decode length", K(ret), K(data_len), K(pos));
      } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > data_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect data_len", K(ret), K(data_len), K(pos));
      } else {
        MEMCPY(encrypt_key_, buf + pos, sizeof(encrypt_key_));
        pos += sizeof(encrypt_key_);
        LST_DO_CODE(OB_UNIS_DECODE,
                    data_checksum_,
                    rowkey_count_,
                    column_count_,
                    micro_block_count_,
                    occupy_size_,
                    data_size_,
                    data_zsize_,
                    progressive_merge_round_,
                    block_offset_,
                    block_size_,
                    row_count_,
                    row_count_delta_,
                    max_merged_trans_version_,
                    is_encrypted_,
                    is_deleted_,
                    contain_uncommitted_row_,
                    compressor_type_,
                    master_key_id_,
                    encrypt_id_,
                    row_store_type_,
                    schema_version_,
                    snapshot_version_,
                    logic_id_,
                    macro_id_,
                    column_checksums_,
                    original_size_,
                    has_string_out_row_,
                    all_lob_in_row_,
                    is_last_row_last_flag_,
                    agg_row_len_);
        if (OB_SUCC(ret)) {
          if (agg_row_len_ == 0) {
            agg_row_buf_ = nullptr;
          } else if (agg_row_len_ > 0) {
            agg_row_buf_ = buf + pos;
            pos += agg_row_len_;
          }
          if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
            LST_DO_CODE(OB_UNIS_DECODE, ddl_end_row_offset_);
          } else {
            ddl_end_row_offset_ = -1;
          }
          if (OB_UNLIKELY(length_ != pos - start_pos)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
          }
        }
      }
    }
    return ret;
  }
  int64_t get_serialize_size() const
  {
    int64_t len = 0;
    len += serialization::encoded_length_i32(version_);
    len += serialization::encoded_length_i32(length_);
    len += sizeof(encrypt_key_);
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                data_checksum_,
                rowkey_count_,
                column_count_,
                micro_block_count_,
                occupy_size_,
                data_size_,
                data_zsize_,
                progressive_merge_round_,
                block_offset_,
                block_size_,
                row_count_,
                row_count_delta_,
                max_merged_trans_version_,
                is_encrypted_,
                is_deleted_,
                contain_uncommitted_row_,
                compressor_type_,
                master_key_id_,
                encrypt_id_,
                row_store_type_,
                schema_version_,
                snapshot_version_,
                logic_id_,
                macro_id_,
                column_checksums_,
                original_size_,
                has_string_out_row_,
                all_lob_in_row_,
                is_last_row_last_flag_,
                agg_row_len_);
    len += agg_row_len_;
    if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, ddl_end_row_offset_);
    }
    return len;
  }
  int64_t get_max_serialize_size() const
  {
    int64_t len = sizeof(*this);
    len -= (sizeof(column_checksums_) + sizeof(agg_row_buf_));
    len += sizeof(int64_t); // serialize column count
    len += sizeof(int64_t) * column_count_; // serialize each checksum
    len += agg_row_len_;
    if (version_ >= DATA_BLOCK_META_VAL_VERSION_V2) {
      len += sizeof(int64_t);
    }
    return len;
  }
  TO_STRING_KV(K_(version), K_(length), K_(data_checksum), K_(rowkey_count),
        K_(column_count), K_(micro_block_count), K_(occupy_size), K_(data_size),
        K_(data_zsize), K_(original_size), K_(progressive_merge_round), K_(block_offset), K_(block_size), K_(row_count),
        K_(row_count_delta), K_(max_merged_trans_version), K_(is_encrypted),
        K_(is_deleted), K_(contain_uncommitted_row), K_(compressor_type),
        K_(master_key_id), K_(encrypt_id), K_(encrypt_key), K_(row_store_type),
        K_(schema_version), K_(snapshot_version), K_(is_last_row_last_flag),
        K_(logic_id), K_(macro_id), K_(column_checksums), K_(has_string_out_row), K_(all_lob_in_row),
          K_(agg_row_len), KP_(agg_row_buf), K_(ddl_end_row_offset));
public:
  int32_t version_;
  int32_t length_;
  int64_t data_checksum_;
  int64_t rowkey_count_;
  int64_t column_count_;
  int64_t micro_block_count_;
  int64_t occupy_size_;   // size of whole macro block (including headers)
  int64_t data_size_; // sum of size of micro blocks (after encoding)
  int64_t data_zsize_;    // sum of size of compressed/encrypted micro blocks
  int64_t original_size_; // sum of size of original micro blocks
  int64_t progressive_merge_round_;
  int64_t block_offset_;  // offset of n-1 level index micro blocks
  int64_t block_size_;    // size of n-1 level index micro blocks
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  bool is_encrypted_;
  bool is_deleted_;
  bool contain_uncommitted_row_;
  bool is_last_row_last_flag_;
  ObCompressorType compressor_type_;
  int64_t master_key_id_;
  int64_t encrypt_id_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  ObRowStoreType row_store_type_;
  uint64_t schema_version_;
  int64_t snapshot_version_;
  ObLogicMacroBlockId logic_id_;
  MacroBlockId macro_id_;
  common::ObSEArray<int64_t, 4> column_checksums_;
  bool has_string_out_row_;
  bool all_lob_in_row_;
  int64_t agg_row_len_; // size of agg_row_buf_
  const char *agg_row_buf_; // data buffer for pre aggregated row
  // used for ddl sstable migration & backup rebuild sstable
  // eg: if only one macro block with 100 rows, ddl_end_row_offset_ is 99.
  int64_t ddl_end_row_offset_;
private:
  DISALLOW_COPY_AND_ASSIGN(MockDataBlockMetaVal_4_3_3);
};

class TestDataBlockMetaValCompact : public ::testing::Test
{
public:
  TestDataBlockMetaValCompact() = default;
  void SetUp() override {}
  void TearDown() override {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}



  // Build a minimal valid ObDataBlockMetaVal for compatibility tests.
  int build_minimal_meta_val(ObDataBlockMetaVal &val,
                             ObIAllocator &allocator,
                             const uint64_t data_version,
                             const bool use_macro_id_v2)
  {
    int ret = OB_SUCCESS;
    val.reset();
    val.version_ = ObDataBlockMetaVal::mapping_data_version_to_val_version(data_version);
    val.rowkey_count_ = 1;
    val.column_count_ = 2;
    val.micro_block_count_ = 1;
    val.occupy_size_ = val.data_size_ = val.data_zsize_ = val.original_size_ = 4096;
    val.progressive_merge_round_ = 0;
    val.block_offset_ = 0;
    val.block_size_ = 0;
    val.row_count_ = 10;
    val.row_count_delta_ = 0;
    val.max_merged_trans_version_ = 0;
    val.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    val.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
    val.logic_id_.data_seq_.macro_data_seq_ = 0;
    val.logic_id_.logic_version_ = 1;
    val.logic_id_.tablet_id_ = 1;
    val.agg_row_len_ = 0;
    val.agg_row_buf_ = nullptr;
    val.ddl_end_row_offset_ = 256;
    val.macro_block_bf_size_ = 0;
    val.macro_block_bf_buf_ = nullptr;

    MacroBlockId macro_id(100, 200, 0, use_macro_id_v2 ? 1 : 0);
    if (use_macro_id_v2) {
      macro_id.set_version_v2();
    } else {
      macro_id.set_version_v1();
    }
    val.macro_id_ = macro_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < val.column_count_; ++i) {
      if (OB_FAIL(val.column_checksums_.push_back(0))) {
        COMMON_LOG(WARN, "fail to push column checksum", K(ret), K(i));
      }
    }
    return ret;
  }

  int serialize_deserialize_round_trip(const ObDataBlockMetaVal &src,
                                         const int64_t data_version,
                                         ObDataBlockMetaVal &dst,
                                         ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    const int64_t buf_len = src.get_max_serialize_size(data_version);
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(src.serialize(buf, buf_len, pos, data_version))) {
      COMMON_LOG(WARN, "fail to serialize", K(ret), KDV(data_version), K(src));
    } else {
      pos = 0;
      if (OB_FAIL(dst.deserialize(buf, buf_len, pos))) {
        COMMON_LOG(WARN, "fail to deserialize", K(ret), KDV(data_version), K(dst));
      }
    }
    return ret;
  }
};

TEST_F(TestDataBlockMetaValCompact, round_trip_old_data_version)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDataBlockMetaVal val(allocator);
  const int64_t data_version = DATA_VERSION_4_3_0_0;  // < 4.3.3, macro_id v1, no fourth_id

  ret = build_minimal_meta_val(val, allocator, data_version, false);
  ASSERT_EQ(val.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V1);
  ASSERT_EQ(val.macro_id_.fourth_id_, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  ObDataBlockMetaVal decoded(allocator);
  ret = serialize_deserialize_round_trip(val, data_version, decoded, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(decoded.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(decoded.macro_id_.fourth_id_, 0);
}

TEST_F(TestDataBlockMetaValCompact, round_trip_new_data_version)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDataBlockMetaVal val(allocator);
  const int64_t data_version = DATA_VERSION_4_3_3_0;  // >= 4.3.3, macro_id v2, with fourth_id

  ret = build_minimal_meta_val(val, allocator, data_version, true);
  ASSERT_EQ(val.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(val.macro_id_.fourth_id_, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  ObDataBlockMetaVal decoded(allocator);
  ret = serialize_deserialize_round_trip(val, data_version, decoded, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(decoded.is_valid());

  ASSERT_EQ(val.macro_id_, decoded.macro_id_);
}

TEST_F(TestDataBlockMetaValCompact, serialize_version_correction)
{
  // When macro_id in memory has wrong version (e.g. v2) but data_version is old,
  // serialize should correct macro_id_first_id via revise_macro_id_version so that
  // the serialized form is compatible with old format.
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDataBlockMetaVal val(allocator);
  const int64_t old_data_version = DATA_VERSION_4_3_0_0;

  ret = build_minimal_meta_val(val, allocator, old_data_version, true);
  ASSERT_EQ(val.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(val.macro_id_.fourth_id_, 1);
  val.macro_id_.fourth_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  int64_t ser_len = val.get_serialize_size(old_data_version);
  char *buf = static_cast<char *>(allocator.alloc(ser_len + 1024));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = val.serialize(buf, ser_len + 1024, pos, old_data_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ser_len, pos);

  ObDataBlockMetaVal decoded(allocator);
  pos = 0;
  ret = decoded.deserialize(buf, ser_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(decoded.is_valid());
  ASSERT_EQ(decoded.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(decoded.macro_id_.fourth_id_, 0);
}

// LatestDataBlockMetaVal @ 4.2.5 serialize -> buf -> MockDataBlockMetaVal_4_3_0_0 deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_latest_to_mock_4_2_5)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = MOCK_DATA_VERSION_4_2_5_0;
  ObDataBlockMetaVal val(allocator);
  ret = build_minimal_meta_val(val, allocator, data_version, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  const int64_t buf_max_len = val.get_max_serialize_size(data_version);
  const int64_t buf_len = val.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = val.serialize(buf, buf_max_len, pos, data_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_ob = pos;
  MockDataBlockMetaVal_4_2_5 mock_from_ob;
  pos = 0;
  ret = mock_from_ob.deserialize(buf, ser_len_ob, pos);
  const int64_t mock_buf_max_len = mock_from_ob.get_max_serialize_size();
  const int64_t mock_buf_len = mock_from_ob.get_serialize_size();
  ASSERT_EQ(mock_buf_max_len, buf_max_len);
  ASSERT_EQ(mock_buf_len, buf_len);
  // 264 = 288-24: 4.2.5 format omits agg_row_len_ and logic_id_.info_ & logic_id_.vptr (added from 4.3.0);
  ASSERT_EQ(mock_buf_max_len, 264);
  // 90 = 92 - 2: 4.2.5 format omits agg_row_len_ and logic_id_.info_ (added from 4.3.0);
  ASSERT_EQ(mock_buf_len, 90);
  ASSERT_EQ(OB_SUCCESS, ret) << "MockDataBlockMetaVal_4_2_5 should deserialize ObDataBlockMetaVal 4.2.5 buf";
  ASSERT_TRUE(mock_from_ob.is_valid());
  ASSERT_EQ(mock_from_ob.version_, val.version_);
  ASSERT_EQ(mock_from_ob.macro_id_.first_id(), val.macro_id_.first_id());
  ASSERT_EQ(mock_from_ob.macro_id_.second_id(), val.macro_id_.second_id());
  ASSERT_EQ(mock_from_ob.macro_id_.third_id(), val.macro_id_.third_id());
  ASSERT_EQ(mock_from_ob.row_count_, val.row_count_);
  ASSERT_EQ(mock_from_ob.column_count_, val.column_count_);
}

// MockDataBlockMetaVal_4_2_5 serialize -> buf -> LatestDataBlockMetaVal deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_mock_4_2_5_to_latest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = MOCK_DATA_VERSION_4_2_5_0;
  MockDataBlockMetaVal_4_2_5 mock_val;
  mock_val.reset();
  mock_val.version_ = MockDataBlockMetaVal_4_2_5::DATA_BLOCK_META_VAL_VERSION;
  mock_val.rowkey_count_ = 1;
  mock_val.column_count_ = 2;
  mock_val.micro_block_count_ = 1;
  mock_val.occupy_size_ = mock_val.data_size_ = mock_val.data_zsize_ = mock_val.original_size_ = 4096;
  mock_val.progressive_merge_round_ = 0;
  mock_val.block_offset_ = 0;
  mock_val.block_size_ = 0;
  mock_val.row_count_ = 10;
  mock_val.row_count_delta_ = 0;
  mock_val.max_merged_trans_version_ = 0;
  mock_val.is_encrypted_ = false;
  mock_val.is_deleted_ = false;
  mock_val.contain_uncommitted_row_ = false;
  mock_val.is_last_row_last_flag_ = false;
  mock_val.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  mock_val.master_key_id_ = 0;
  mock_val.encrypt_id_ = 0;
  mock_val.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  mock_val.schema_version_ = 0;
  mock_val.snapshot_version_ = 0;
  mock_val.logic_id_.data_seq_ = 0;
  mock_val.logic_id_.logic_version_ = 1;
  mock_val.logic_id_.tablet_id_ = 1;
  mock_val.macro_id_ = MockMacroBlockId_432(100, 200, 0);
  mock_val.has_string_out_row_ = false;
  mock_val.all_lob_in_row_ = false;
  MEMSET(mock_val.encrypt_key_, 0, sizeof(mock_val.encrypt_key_));
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(mock_val.is_valid());

  const int64_t buf_max_len = mock_val.get_max_serialize_size();
  const int64_t buf_len = mock_val.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = mock_val.serialize(buf, buf_max_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_mock = pos;

  ObDataBlockMetaVal decoded_from_mock(allocator);
  pos = 0;
  ret = decoded_from_mock.deserialize(buf, buf_max_len, pos);
  const int64_t decoded_buf_len = decoded_from_mock.get_serialize_size(data_version);
  const int64_t decoded_buf_max_len = decoded_from_mock.get_max_serialize_size(data_version);
  ASSERT_EQ(decoded_buf_max_len, buf_max_len);
  ASSERT_EQ(decoded_buf_len, buf_len);
  // 264 = 288-24: 4.2.5 format omits agg_row_len_ and logic_id_.info_ & logic_id_.vptr (added from 4.3.0);
  ASSERT_EQ(decoded_buf_max_len, 264);
  // 90 = 92 - 2: 4.2.5 format omits agg_row_len_ and logic_id_.info_ (added from 4.3.0);
  ASSERT_EQ(decoded_buf_len, 90);
  ASSERT_EQ(OB_SUCCESS, ret) << "ObDataBlockMetaVal should deserialize MockDataBlockMetaVal_4_2_5 buf";
  ASSERT_TRUE(decoded_from_mock.is_valid());
  ASSERT_EQ(decoded_from_mock.version_, mock_val.version_);
  ASSERT_NE(decoded_from_mock.macro_id_.first_id(), mock_val.macro_id_.first_id());
  ASSERT_EQ(mock_val.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V1);
  ASSERT_EQ(decoded_from_mock.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(decoded_from_mock.macro_id_.second_id(), mock_val.macro_id_.second_id());
  ASSERT_EQ(decoded_from_mock.macro_id_.third_id(), mock_val.macro_id_.third_id());
  ASSERT_EQ(decoded_from_mock.row_count_, mock_val.row_count_);
  ASSERT_EQ(decoded_from_mock.column_count_, mock_val.column_count_);
}

// LatestDataBlockMetaVal @ 4.3.0 serialize -> buf -> MockDataBlockMetaVal_4_3_0_0 deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_latest_to_mock_4_3_0)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = DATA_VERSION_4_3_0_0;
  ObDataBlockMetaVal val(allocator);
  ret = build_minimal_meta_val(val, allocator, data_version, false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  const int64_t buf_max_len = val.get_max_serialize_size(data_version);
  const int64_t buf_len = val.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = val.serialize(buf, buf_max_len, pos, data_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_ob = pos;
  MockDataBlockMetaVal_4_3_0_0 mock_from_ob;
  pos = 0;
  ret = mock_from_ob.deserialize(buf, ser_len_ob, pos);
  const int64_t mock_buf_max_len = mock_from_ob.get_max_serialize_size();
  const int64_t mock_buf_len = mock_from_ob.get_serialize_size();
  ASSERT_EQ(mock_buf_max_len, buf_max_len);
  ASSERT_EQ(mock_buf_len, buf_len);
  // For Mock 4.3.0.0, without ddl_end_row_offset_, the max size calculation does not subtract twice the size of ddl_end_row_offset_,
  // resulting in an incorrect max size calculation. So the max size is 304 - 16 (sizeof(ddl_end_row_offset_) * 2) = 288 bytes.
  ASSERT_EQ(mock_buf_max_len, 288);
  // Mock 4.3.0.0: no ddl_end_row_offset_ (+0), macro_id has 3 ids only (24 bytes). data_version is
  // 4.3.0.0 so get_serialize_size does not add ddl_end_row_offset_ and uses macro_id V1 (24 bytes).
  // So total is 102 - 2 (no ddl) - 8 (macro_id 3 ids vs 4) = 92. The mock also serializes 92 bytes.
  ASSERT_EQ(mock_buf_len, 92);
  ASSERT_EQ(OB_SUCCESS, ret) << "MockDataBlockMetaVal_4_3_0_0 should deserialize ObDataBlockMetaVal 4.3.0 buf";
  ASSERT_TRUE(mock_from_ob.is_valid());
  ASSERT_EQ(mock_from_ob.version_, val.version_);
  ASSERT_EQ(mock_from_ob.macro_id_.first_id(), val.macro_id_.first_id());
  ASSERT_EQ(mock_from_ob.macro_id_.second_id(), val.macro_id_.second_id());
  ASSERT_EQ(mock_from_ob.macro_id_.third_id(), val.macro_id_.third_id());
  ASSERT_EQ(mock_from_ob.row_count_, val.row_count_);
  ASSERT_EQ(mock_from_ob.column_count_, val.column_count_);
}

// MockDataBlockMetaVal_4_3_0_0 serialize -> buf -> LatestDataBlockMetaVal deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_mock_4_3_0_to_latest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = DATA_VERSION_4_3_0_0;
  MockDataBlockMetaVal_4_3_0_0 mock_val;
  mock_val.reset();
  mock_val.version_ = MockDataBlockMetaVal_4_3_0_0::MockDataBlockMetaVal_4_3_0_0_VERSION;
  mock_val.rowkey_count_ = 1;
  mock_val.column_count_ = 2;
  mock_val.micro_block_count_ = 1;
  mock_val.occupy_size_ = mock_val.data_size_ = mock_val.data_zsize_ = mock_val.original_size_ = 4096;
  mock_val.progressive_merge_round_ = 0;
  mock_val.block_offset_ = 0;
  mock_val.block_size_ = 0;
  mock_val.row_count_ = 10;
  mock_val.row_count_delta_ = 0;
  mock_val.max_merged_trans_version_ = 0;
  mock_val.is_encrypted_ = false;
  mock_val.is_deleted_ = false;
  mock_val.contain_uncommitted_row_ = false;
  mock_val.is_last_row_last_flag_ = false;
  mock_val.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  mock_val.master_key_id_ = 0;
  mock_val.encrypt_id_ = 0;
  mock_val.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  mock_val.schema_version_ = 0;
  mock_val.snapshot_version_ = 0;
  mock_val.logic_id_.data_seq_.macro_data_seq_ = 0;
  mock_val.logic_id_.logic_version_ = 1;
  mock_val.logic_id_.tablet_id_ = 1;
  mock_val.macro_id_ = MockMacroBlockId_432(100, 200, 0);
  mock_val.has_string_out_row_ = false;
  mock_val.all_lob_in_row_ = false;
  mock_val.agg_row_len_ = 0;
  mock_val.agg_row_buf_ = nullptr;
  MEMSET(mock_val.encrypt_key_, 0, sizeof(mock_val.encrypt_key_));
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(mock_val.is_valid());

  const int64_t buf_max_len = mock_val.get_max_serialize_size();
  const int64_t buf_len = mock_val.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = mock_val.serialize(buf, buf_max_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_mock = pos;

  ObDataBlockMetaVal decoded_from_mock(allocator);
  pos = 0;
  ret = decoded_from_mock.deserialize(buf, buf_max_len, pos);
  const int64_t decoded_buf_len = decoded_from_mock.get_serialize_size(data_version);
  const int64_t decoded_buf_max_len = decoded_from_mock.get_max_serialize_size(data_version);
  ASSERT_EQ(decoded_buf_max_len, buf_max_len);
  ASSERT_EQ(decoded_buf_len, buf_len);
  // For Mock 4.3.0.0, without ddl_end_row_offset_, the max size calculation does not subtract twice the size of ddl_end_row_offset_,
  // resulting in an incorrect max size calculation. So the max size is 304 - 16 (sizeof(ddl_end_row_offset_) * 2) = 288 bytes.
  ASSERT_EQ(decoded_buf_max_len, 288);
  // Mock 4.3.0.0: no ddl_end_row_offset_ (+0), macro_id has 3 ids only (24 bytes). data_version is
  // 4.3.0.0 so get_serialize_size does not add ddl_end_row_offset_ and uses macro_id V1 (24 bytes).
  // So total is 102 - 2 (no ddl) - 8 (macro_id 3 ids vs 4) = 92. The mock also serializes 92 bytes.
  ASSERT_EQ(decoded_buf_len, 92);
  ASSERT_EQ(OB_SUCCESS, ret) << "ObDataBlockMetaVal should deserialize MockDataBlockMetaVal_4_3_0_0 buf";
  ASSERT_TRUE(decoded_from_mock.is_valid());
  ASSERT_NE(decoded_from_mock.macro_id_.first_id(), mock_val.macro_id_.first_id());
  ASSERT_EQ(decoded_from_mock.macro_id_.version(), MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(mock_val.macro_id_.version_, MacroBlockId::MACRO_BLOCK_ID_VERSION_V1);
  ASSERT_EQ(decoded_from_mock.macro_id_.second_id(), mock_val.macro_id_.second_id());
  ASSERT_EQ(decoded_from_mock.macro_id_.third_id(), mock_val.macro_id_.third_id());
  ASSERT_EQ(decoded_from_mock.row_count_, mock_val.row_count_);
  ASSERT_EQ(decoded_from_mock.column_count_, mock_val.column_count_);
}

// LatestDataBlockMetaVal @ 4.3.3 serialize -> buf -> MockDataBlockMetaVal_4_3_3 deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_latest_to_mock_4_3_3)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = DATA_VERSION_4_3_3_0;

  ObDataBlockMetaVal val(allocator);
  ret = build_minimal_meta_val(val, allocator, data_version, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(val.is_valid());

  const int64_t buf_max_len = val.get_max_serialize_size(data_version);
  const int64_t buf_len = val.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = val.serialize(buf, buf_max_len, pos, data_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_ob = pos;
  MockDataBlockMetaVal_4_3_3 mock_from_ob;
  pos = 0;
  ret = mock_from_ob.deserialize(buf, ser_len_ob, pos);
  const int64_t mock_buf_max_len = mock_from_ob.get_max_serialize_size();
  const int64_t mock_buf_len = mock_from_ob.get_serialize_size();
  ASSERT_EQ(mock_buf_max_len, buf_max_len);
  ASSERT_EQ(mock_buf_max_len, 304);
  ASSERT_EQ(mock_buf_len, buf_len);
  ASSERT_EQ(mock_buf_len, 102);
  ASSERT_EQ(OB_SUCCESS, ret) << "MockDataBlockMetaVal_4_3_0_0 should deserialize ObDataBlockMetaVal 4.3.0 buf";
  ASSERT_TRUE(mock_from_ob.is_valid());
  ASSERT_EQ(mock_from_ob.version_, val.version_);
  ASSERT_EQ(mock_from_ob.macro_id_.version(), MacroBlockId::MACRO_BLOCK_ID_VERSION_V2);
  ASSERT_EQ(mock_from_ob.macro_id_.first_id(), val.macro_id_.first_id());
  ASSERT_EQ(mock_from_ob.macro_id_.second_id(), val.macro_id_.second_id());
  ASSERT_EQ(mock_from_ob.macro_id_.third_id(), val.macro_id_.third_id());
  ASSERT_EQ(mock_from_ob.row_count_, val.row_count_);
  ASSERT_EQ(mock_from_ob.column_count_, val.column_count_);
}

// MockDataBlockMetaVal_4_3_3 serialize -> buf -> LatestDataBlockMetaVal deserialize
TEST_F(TestDataBlockMetaValCompact, version_compat_mock_4_3_3_to_latest)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t data_version = DATA_VERSION_4_3_3_0;

  MockDataBlockMetaVal_4_3_3 mock_val;
  mock_val.reset();
  mock_val.version_ = MockDataBlockMetaVal_4_3_3::DATA_BLOCK_META_VAL_VERSION_V2;
  mock_val.rowkey_count_ = 1;
  mock_val.column_count_ = 2;
  mock_val.micro_block_count_ = 1;
  mock_val.occupy_size_ = mock_val.data_size_ = mock_val.data_zsize_ = mock_val.original_size_ = 4096;
  mock_val.progressive_merge_round_ = 0;
  mock_val.block_offset_ = 0;
  mock_val.block_size_ = 0;
  mock_val.row_count_ = 10;
  mock_val.row_count_delta_ = 0;
  mock_val.max_merged_trans_version_ = 0;
  mock_val.is_encrypted_ = false;
  mock_val.is_deleted_ = false;
  mock_val.contain_uncommitted_row_ = false;
  mock_val.is_last_row_last_flag_ = false;
  mock_val.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  mock_val.master_key_id_ = 0;
  mock_val.encrypt_id_ = 0;
  mock_val.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  mock_val.schema_version_ = 0;
  mock_val.snapshot_version_ = 0;
  mock_val.logic_id_.data_seq_.macro_data_seq_ = 0;
  mock_val.logic_id_.logic_version_ = 1;
  mock_val.logic_id_.tablet_id_ = 1;
  mock_val.macro_id_ = MacroBlockId(100, 200, 0, 4);
  mock_val.macro_id_.set_version_v2();
  mock_val.has_string_out_row_ = false;
  mock_val.all_lob_in_row_ = false;
  mock_val.agg_row_len_ = 0;
  mock_val.agg_row_buf_ = nullptr;
  mock_val.ddl_end_row_offset_ = 256;
  MEMSET(mock_val.encrypt_key_, 0, sizeof(mock_val.encrypt_key_));
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mock_val.column_checksums_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(mock_val.is_valid());

  const int64_t buf_max_len = mock_val.get_max_serialize_size();
  const int64_t buf_len = mock_val.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_max_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ret = mock_val.serialize(buf, buf_max_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t ser_len_mock = pos;

  ObDataBlockMetaVal decoded_from_mock(allocator);
  pos = 0;
  ret = decoded_from_mock.deserialize(buf, buf_max_len, pos);
  const int64_t decoded_buf_len = decoded_from_mock.get_serialize_size(data_version);
  const int64_t decoded_buf_max_len = decoded_from_mock.get_max_serialize_size(data_version);
  ASSERT_EQ(decoded_buf_max_len, buf_max_len);
  ASSERT_EQ(decoded_buf_max_len, 304);
  ASSERT_EQ(decoded_buf_len, buf_len);
  ASSERT_EQ(decoded_buf_len, 102);
  ASSERT_EQ(OB_SUCCESS, ret) << "ObDataBlockMetaVal should deserialize MockDataBlockMetaVal_4_3_3 buf";
  ASSERT_TRUE(decoded_from_mock.is_valid());
  ASSERT_EQ(decoded_from_mock.version_, mock_val.version_);
  ASSERT_EQ(decoded_from_mock.macro_id_.first_id(), mock_val.macro_id_.first_id());
  ASSERT_EQ(decoded_from_mock.macro_id_.second_id(), mock_val.macro_id_.second_id());
  ASSERT_EQ(decoded_from_mock.macro_id_.third_id(), mock_val.macro_id_.third_id());
  ASSERT_EQ(decoded_from_mock.row_count_, mock_val.row_count_);
  ASSERT_EQ(decoded_from_mock.column_count_, mock_val.column_count_);
  ASSERT_EQ(decoded_from_mock.ddl_end_row_offset_, mock_val.ddl_end_row_offset_);
  ASSERT_EQ(decoded_from_mock.ddl_end_row_offset_, 256);
}



}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_data_block_meta_val_compact.log*");
  OB_LOGGER.set_file_name("test_data_block_meta_val_compact.log", true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_data_block_meta_val_compact");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}