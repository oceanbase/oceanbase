/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_bloom_filter_cache.h"
#include "storage/blocksstable/ob_macro_block_meta.h"

namespace oceanbase
{
namespace blocksstable
{
ObDataBlockMetaVal::ObDataBlockMetaVal()
  : version_(DEFAULT_DATA_BLOCK_META_VAL_VERSION),
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
    data_flag_pack_(0),
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
    ddl_end_row_offset_(-1),
    macro_block_bf_size_(0),
    macro_block_bf_buf_()
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

ObDataBlockMetaVal::ObDataBlockMetaVal(ObIAllocator &allocator)
    : version_(DEFAULT_DATA_BLOCK_META_VAL_VERSION),
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
    data_flag_pack_(0),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    master_key_id_(0),
    encrypt_id_(0),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    schema_version_(0),
    snapshot_version_(0),
    logic_id_(),
    macro_id_(),
    column_checksums_(sizeof(int64_t), ModulePageAllocator(allocator, "MacroMetaChksum")),
    has_string_out_row_(false),
    all_lob_in_row_(false),
    agg_row_len_(0),
    agg_row_buf_(nullptr),
    ddl_end_row_offset_(-1),
    macro_block_bf_size_(0),
    macro_block_bf_buf_()
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}
ObDataBlockMetaVal::~ObDataBlockMetaVal()
{
  reset();
}

void ObDataBlockMetaVal::reset()
{
  version_ = DEFAULT_DATA_BLOCK_META_VAL_VERSION;
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
  data_flag_pack_ = 0;
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
  macro_block_bf_size_ = 0;
  macro_block_bf_buf_ = nullptr;
}

bool ObDataBlockMetaVal::is_valid() const
{
return is_valid_version()
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
    && ddl_end_row_offset_ >= -1
    && macro_block_bf_size_ >= 0
    && (0 == macro_block_bf_size_ || nullptr != macro_block_bf_buf_);
}

int ObDataBlockMetaVal::assign(const ObDataBlockMetaVal &val)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!val.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(val));
  } else if (OB_FAIL(column_checksums_.assign(val.column_checksums_))) {
    LOG_WARN("fail to assign column checksums", K(ret), K(val.column_checksums_));
  } else {
    version_ = val.version_;
    length_ = val.length_;
    data_checksum_ = val.data_checksum_;
    rowkey_count_ = val.rowkey_count_;
    column_count_ = val.column_count_;
    micro_block_count_ = val.micro_block_count_;
    occupy_size_ = val.occupy_size_;
    data_size_ = val.data_size_;
    data_zsize_ = val.data_zsize_;
    original_size_ = val.original_size_;
    progressive_merge_round_ = val.progressive_merge_round_;
    block_offset_ = val.block_offset_;
    block_size_ = val.block_size_;
    row_count_ = val.row_count_;
    row_count_delta_ = val.row_count_delta_;
    max_merged_trans_version_ = val.max_merged_trans_version_;
    is_encrypted_ = val.is_encrypted_;
    is_deleted_ = val.is_deleted_;
    contain_uncommitted_row_ = val.contain_uncommitted_row_;
    data_flag_pack_ = val.data_flag_pack_;
    compressor_type_ = val.compressor_type_;
    master_key_id_ = val.master_key_id_;
    encrypt_id_ = val.encrypt_id_;
    MEMCPY(encrypt_key_, val.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    row_store_type_ = val.row_store_type_;
    schema_version_ = val.schema_version_;
    snapshot_version_ = val.snapshot_version_;
    logic_id_ = val.logic_id_;
    macro_id_ = val.macro_id_;
    has_string_out_row_ = val.has_string_out_row_;
    all_lob_in_row_ = val.all_lob_in_row_;
    agg_row_len_ = val.agg_row_len_;
    agg_row_buf_ = val.agg_row_buf_;
    ddl_end_row_offset_ = val.ddl_end_row_offset_;
    macro_block_bf_size_ = val.macro_block_bf_size_;
    macro_block_bf_buf_ = val.macro_block_bf_buf_;
  }
  return ret;
}

int ObDataBlockMetaVal::build_value(ObStorageDatum &datum,
                                    ObIAllocator &allocator,
                                    const int64_t data_version) const
{
  int ret = OB_SUCCESS;
  const int64_t size = get_serialize_size(data_version);
  const int64_t estimate_size = get_max_serialize_size(data_version);
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(size > estimate_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K(size), K(estimate_size), KPC(this));
  } else if (OB_UNLIKELY(data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to build value, invalid major working cluster version",
             K(ret), KDV(data_version), KPC(this));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size));
  } else if (OB_FAIL(serialize(buf, size, pos, data_version))) {
    LOG_WARN("fail to serialize for data version", K(ret), K(size), KDV(data_version));
  } else {
    ObString str(size, buf);
    datum.set_string(str);
  }
  return ret;
}

bool ObDataBlockMetaVal::is_valid_macro_id(const MacroBlockId &macro_id, const int64_t data_version) const
{
  // macro_id.fourth_id() is 0 in such situation:
  // 1. data version < 4.3.3.0
  // 2. data version >= 4.3.3.0 and in SN mode
  bool old_version_valid = data_version < DATA_VERSION_4_3_3_0
                        && macro_id.fourth_id() == 0
                        && MacroBlockId::MACRO_BLOCK_ID_VERSION_V1 == macro_id.version();
  bool new_version_valid = data_version >= DATA_VERSION_4_3_3_0
                        && MacroBlockId::MACRO_BLOCK_ID_VERSION_V2 == macro_id.version();
  return old_version_valid || new_version_valid;
}

void ObDataBlockMetaVal::check_and_revise_macro_id_version(const int64_t data_version, MacroBlockId &macro_id) const
{
  if (OB_UNLIKELY(!is_valid_macro_id(macro_id, data_version))) {
    // LOG_WARN_RET(OB_INVALID_ARGUMENT, "macro id is invalid", K(macro_id), KDV(data_version), KPC(this), K(lbt()));
    // Revise ensures when data_version >= 4.3.3 we encode V2, so fourth_id is written and
    // deserialize (which reads fourth_id only for V2) stays in sync.
    if (data_version < DATA_VERSION_4_3_3_0) {
      macro_id.set_version_v1();
    } else {
      macro_id.set_version_v2();
    }
    bool old_version_valid = data_version < DATA_VERSION_4_3_3_0
                          && macro_id.fourth_id() == 0
                          && MacroBlockId::MACRO_BLOCK_ID_VERSION_V1 == macro_id.version();
    bool new_version_valid = data_version >= DATA_VERSION_4_3_3_0
                          && MacroBlockId::MACRO_BLOCK_ID_VERSION_V2 == macro_id.version();
    OB_ASSERT(old_version_valid || new_version_valid);
  }
}

int ObDataBlockMetaVal::serialize(char *buf,
                                  const int64_t buf_len,
                                  int64_t &pos,
                                  const int64_t data_version) const
{
  int ret = OB_SUCCESS;
  // The version encoded in macro_id_first_id_ may be wrong; it is corrected during
  // serialization based on data_version (see check_and_revise_macro_id_version) for compatibility.
  MacroBlockId macro_id = macro_id_;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos), KDV(data_version));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(version_ > mapping_data_version_to_val_version(data_version))) {
    ret = OB_ERR_UNEXPECTED;
    const uint8_t max_suppor_val_version = mapping_data_version_to_val_version(data_version);
    LOG_ERROR("version mismatch", K(ret), K(version_), K(max_suppor_val_version), KDV(data_version));
  } else if (FALSE_IT(check_and_revise_macro_id_version(data_version, macro_id))) {
  } else {
    int64_t start_pos = pos;
    const_cast<ObDataBlockMetaVal *>(this)->length_ = get_serialize_size(data_version);
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
                  snapshot_version_);
      if (FAILEDx(logic_id_.serialize(buf, buf_len, pos, data_version))) {
        LOG_WARN("fail to serialize logic id", K(ret), K(buf_len), K(pos), KDV(data_version));
      }
      LST_DO_CODE(OB_UNIS_ENCODE,
                  macro_id,
                  column_checksums_,
                  original_size_,
                  has_string_out_row_,
                  all_lob_in_row_,
                  data_flag_pack_);
      if (OB_SUCC(ret)) {
        if (DATA_VERSION_4_3_0_0 <= data_version) {
          LST_DO_CODE(OB_UNIS_ENCODE, agg_row_len_);
          MEMCPY(buf + pos, agg_row_buf_, agg_row_len_);
          pos += agg_row_len_;
        }
        if (DATA_VERSION_4_3_1_0 <= data_version) {
          LST_DO_CODE(OB_UNIS_ENCODE, ddl_end_row_offset_);
        }
        // Determine whether to serialize the macro block bloom filter based on the current cluster's data version.
        if (DATA_VERSION_4_3_5_1 <= data_version) {
          LST_DO_CODE(OB_UNIS_ENCODE, macro_block_bf_size_);
          if (macro_block_bf_size_ > 0) {
            MEMCPY(buf + pos, macro_block_bf_buf_, macro_block_bf_size_);
            pos += macro_block_bf_size_;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KDV(data_version), KPC(this));
        }
      }
    }
  }
  return ret;
}

int ObDataBlockMetaVal::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  } else {
    int64_t start_pos = pos;
    if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
      LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
    } else if (OB_UNLIKELY(!is_valid_version())) {
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
                  data_flag_pack_);
      if (OB_SUCC(ret)) {
        if (pos < start_pos + length_) {
          LST_DO_CODE(OB_UNIS_DECODE, agg_row_len_);
        }
        if (agg_row_len_ == 0) {
          agg_row_buf_ = nullptr;
        } else if (agg_row_len_ > 0) {
          agg_row_buf_ = buf + pos;
          pos += agg_row_len_;
        }
        if (pos < start_pos + length_) {
          LST_DO_CODE(OB_UNIS_DECODE, ddl_end_row_offset_);
        }
        // Deserialize macro block bloom filter.
        if (pos < start_pos + length_) {
          LST_DO_CODE(OB_UNIS_DECODE, macro_block_bf_size_);
        }
        if (macro_block_bf_size_ > 0) {
          macro_block_bf_buf_ = buf + pos;
          pos += macro_block_bf_size_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
        }
      }
    }
  }
  return ret;
}

int64_t ObDataBlockMetaVal::get_max_serialize_size(const int64_t data_version) const
{
  int64_t len = sizeof(*this);
  uint64_t compact_fixed_version = DATA_VERSION_4_6_0_0;
  len -= (
          sizeof(column_checksums_)

          + sizeof(int64_t)               // logic_id_.info_ from 4.3.0
          // logic_id_.ObMacroDataSeq add vptr from 4.3.0, but has been removed in 4.3.3
          + sizeof(agg_row_len_)          // from 4.3.0
          + sizeof(agg_row_buf_)          // from 4.3.0

          + sizeof(ddl_end_row_offset_)   // from 4.3.1

          + sizeof(int64_t)               // MacroBlockId.fourth_id_ from 4.3.3

          + sizeof(macro_block_bf_size_) + sizeof(macro_block_bf_buf_)   // from 4.3.5
        );
  len += sizeof(int64_t); // serialize column count
  len += sizeof(int64_t) * column_count_; // serialize each checksum

  if (DATA_VERSION_4_3_0_0 <= data_version) {
    len += sizeof(int64_t);
    len += sizeof(char*);
    len += sizeof(agg_row_len_);
    len += agg_row_len_;
  }
  if (DATA_VERSION_4_3_1_0 <= data_version) {
    // For ddl_end_row_offset_, the default value -1 takes 10 bytes in variable-length encoding.
    // We reserve 16 bytes here to avoid insufficient buffer space
    // and to stay consistent with previous behavior, In versions [4.3.1, 4.6.0),
    // get_max_serialize_size did not subtract sizeof(int64_t) in the initial len-= section
    len += (sizeof(int64_t) * 2); // ddl_end_row_offset_
  }
  // if (DATA_VERSION_4_3_3_0 <= data_version) {
  //   len += sizeof(int64_t);                              // macro_id_fourth_id_
  //   len -= sizeof(char*);                                // vptr in logic_id_.ObMacroDataSeq
  // }
  // Get macro block bloom filter max serialize size.
  if (DATA_VERSION_4_3_5_1 <= data_version) {
    len += sizeof(macro_block_bf_size_);
    len += macro_block_bf_size_;
  }
  return len;
}

int64_t ObDataBlockMetaVal::get_serialize_size(const int64_t data_version) const
{
  int64_t len = 0;
  MacroBlockId macro_id = macro_id_;
  check_and_revise_macro_id_version(data_version, macro_id);
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
              snapshot_version_);
  len += logic_id_.get_serialize_size(data_version);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              macro_id,
              column_checksums_,
              original_size_,
              has_string_out_row_,
              all_lob_in_row_,
              data_flag_pack_);
  if (DATA_VERSION_4_3_0_0 <= data_version) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, agg_row_len_);
    len += agg_row_len_;
  }
  if (DATA_VERSION_4_3_1_0 <= data_version) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, ddl_end_row_offset_);
  }
  // Get macro block bloom filter serialize size.
  if (DATA_VERSION_4_3_5_1 <= data_version) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, macro_block_bf_size_);
    if (macro_block_bf_size_ > 0) {
      len += macro_block_bf_size_;
    }
  }
  return len;
}

int32_t ObDataBlockMetaVal::mapping_data_version_to_val_version(const uint64_t data_version)
{
  int32_t version = 0;
  if (0 == data_version) {
    version = DEFAULT_DATA_BLOCK_META_VAL_VERSION;
  } else if (data_version < DATA_VERSION_4_3_1_0) {
    version = DATA_BLOCK_META_VAL_VERSION_V1;
  } else {
    version = DATA_BLOCK_META_VAL_VERSION_V2;
  }
  return version;
}

//================================== ObDataMacroBlockMeta ==================================
ObDataMacroBlockMeta::ObDataMacroBlockMeta()
  : val_(),
    end_key_(),
    nested_offset_(0),
    nested_size_(0),
    version_(ObDataMacroBlockMeta::MACRO_BLOCK_META_VERSION)
{
}

ObDataMacroBlockMeta::ObDataMacroBlockMeta(ObIAllocator &allocator)
  : val_(allocator),
    end_key_(),
    nested_offset_(0),
    nested_size_(0),
    version_(ObDataMacroBlockMeta::MACRO_BLOCK_META_VERSION)
{
}

ObDataMacroBlockMeta::~ObDataMacroBlockMeta()
{
  reset();
}

int ObDataMacroBlockMeta::assign(const ObDataMacroBlockMeta &meta)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(meta));
  } else if (OB_FAIL(val_.assign(meta.val_))) {
    LOG_WARN("fail to assign meta val", K(ret), K(meta));
  } else if (OB_FAIL(end_key_.assign(meta.end_key_.datums_,
                                     meta.end_key_.datum_cnt_))) {
    LOG_WARN("fail to assign end key", K(ret), K(end_key_));
  }
  return ret;
}

int ObDataMacroBlockMeta::deep_copy(ObDataMacroBlockMeta *&dst, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t &rowkey_count = val_.rowkey_count_;
  char *agg_row_buf = nullptr;
  char *macro_block_bf_buf = nullptr;
  char *buf = nullptr;
  const int64_t buf_len = sizeof(ObDataMacroBlockMeta) + sizeof(ObStorageDatum) * rowkey_count;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src macro meta is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(buf_len));
  } else if (0 < val_.agg_row_len_
             && OB_ISNULL(agg_row_buf = static_cast<char *>(allocator.alloc(val_.agg_row_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for agg row", K(ret), K_(val));
  } else if (0 < val_.macro_block_bf_size_
             && OB_ISNULL(macro_block_bf_buf = static_cast<char *>(allocator.alloc(val_.macro_block_bf_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for macro block bloom filter", K(ret), K_(val));
  } else {
    ObDataMacroBlockMeta *meta = new (buf) ObDataMacroBlockMeta(allocator);
    ObStorageDatum *endkey = new (buf + sizeof(ObDataMacroBlockMeta)) ObStorageDatum[rowkey_count];
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_count; ++i) {
      if (OB_FAIL(endkey[i].deep_copy(end_key_.datums_[i], allocator))) {
        LOG_WARN("fail to deep copy datum", K(ret), K(i), K(end_key_.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(meta->val_.assign(val_))) {
        LOG_WARN("fail to assign data block meta value", K(ret), K(val_));
      } else if (OB_FAIL(meta->end_key_.assign(endkey, rowkey_count))) {
        LOG_WARN("fail to assign rowkey", K(ret), KP(endkey), K(rowkey_count));
      } else {
        if (val_.agg_row_len_ > 0) {
          MEMCPY(agg_row_buf, val_.agg_row_buf_, val_.agg_row_len_);
          meta->val_.agg_row_buf_ = agg_row_buf;
        }
        if (val_.macro_block_bf_size_ > 0) {
          MEMCPY(macro_block_bf_buf, val_.macro_block_bf_buf_, val_.macro_block_bf_size_);
          meta->val_.macro_block_bf_buf_ = macro_block_bf_buf;
        }
        dst = meta;
      }
    }
    if (OB_FAIL(ret)) {
      if (nullptr != meta) {
        meta->~ObDataMacroBlockMeta();
        allocator.free(buf);
        meta = nullptr;
      }
      if (nullptr != agg_row_buf) {
        allocator.free(agg_row_buf);
      }
    }
  }
  return ret;
}

int ObDataMacroBlockMeta::build_estimate_row(ObDatumRow &row,
                                             ObIAllocator &allocator,
                                             const uint64_t data_version) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(row), KPC(this));
  } else if (OB_UNLIKELY(val_.rowkey_count_ + 1 != row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Rowkey column count mismatch", K(ret), K(val_.rowkey_count_), K(row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < val_.rowkey_count_; ++i) {
      if (OB_FAIL(row.storage_datums_[i].deep_copy(end_key_.datums_[i], allocator))) {
        LOG_WARN("Fail to deep copy datum ", K(ret), K(i), K(end_key_.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t size = val_.get_max_serialize_size(data_version);
      char *buf = nullptr;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(size));
      } else {
        MEMSET(buf, 0, size); // fake char column
        ObString str(size, buf);
        row.storage_datums_[val_.rowkey_count_].set_string(str);
      }
    }
  }
  return ret;
}


int ObDataMacroBlockMeta::build_row(ObDatumRow &row, ObIAllocator &allocator, const uint64_t data_version) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !row.is_valid() || data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(row), KDV(data_version), KPC(this));
  } else if (OB_UNLIKELY(val_.rowkey_count_ + 1 != row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Rowkey column count mismatch", K(ret), K(val_.rowkey_count_), K(row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < val_.rowkey_count_; ++i) {
      if (OB_FAIL(row.storage_datums_[i].deep_copy(end_key_.datums_[i], allocator))) {
        LOG_WARN("Fail to deep copy datum ", K(ret), K(i), K(end_key_.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(val_.build_value(row.storage_datums_[val_.rowkey_count_], allocator, data_version))) {
        LOG_WARN("Fail to build value for macro meta", K(ret), KDV(data_version));
      } else {
        row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      }
    }
  }
  return ret;
}

int ObDataMacroBlockMeta::parse_row(ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row.get_column_count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument to parse row", K(ret), K(row));
  } else {
    const ObStorageDatum &datum = row.storage_datums_[row.get_column_count() - 1];
    ObString data_buf = datum.get_string();
    int64_t pos = 0;
    if (OB_FAIL(val_.deserialize(data_buf.ptr(), data_buf.length(), pos))) {
      LOG_WARN("fail to deserialize", K(ret), K(row), K(data_buf));
    } else if (OB_FAIL(end_key_.assign(row.storage_datums_, val_.rowkey_count_))) {
      STORAGE_LOG(WARN, "Failed to assign endkey", K(ret), K(row));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Parsed data macro block is not valid", K(ret), K_(val));
    }
  }
  return ret;
}

int ObDataMacroBlockMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos, const int64_t data_version) const
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  const int64_t initial_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || data_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos), KDV(data_version));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
  } else if (FALSE_IT(serialize_size = get_serialize_size(data_version))) {
  } else if (OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro block meta serialize size overflow", K(ret), K(buf_len), K(pos), K(serialize_size), KPC(this));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to serialize version_", K(ret), K(buf_len), K(pos), K(version_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, serialize_size))) {
    LOG_WARN("fail to serialize serialize_size", K(ret), K(buf_len), K(pos), K(serialize_size));
  } else if (OB_FAIL(val_.serialize(buf, buf_len, pos, data_version))) {
    LOG_WARN("fail to serialize meta val", K(ret), KP(buf), K(buf_len), K(pos), KDV(data_version), K(val_));
  } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize end key", K(ret), KP(buf), K(buf_len), K(pos), KDV(data_version), K(end_key_));
  } else if (OB_UNLIKELY(pos - initial_pos != serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to serialize macro block meta, unexpected error",
             K(ret), K(pos), K(initial_pos), K(serialize_size), K(buf_len), KPC(this));
  }
  return ret;
}

int ObDataMacroBlockMeta::deserialize(const char *buf, const int64_t data_len, ObIAllocator &allocator, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  const int64_t initial_pos = pos;
  ObStorageDatum *rowkey_datums = nullptr;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
    LOG_WARN("fail to deserialize version_", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &serialize_size))) {
    LOG_WARN("fail to deserialize additional serialize_size", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(val_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize meta val", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(val_.rowkey_count_ > OB_MAX_ROWKEY_COLUMN_NUMBER
                        || val_.rowkey_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey count is invalid", K(ret), K(val_.rowkey_count_));
  } else if (OB_ISNULL(rowkey_datums = static_cast<ObStorageDatum *>(
                      allocator.alloc(sizeof(ObStorageDatum) * val_.rowkey_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate rowkey datums", K(ret), K(val_.rowkey_count_));
  } else if (OB_UNLIKELY(end_key_.datums_ != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end key datums is not null", K(ret), KP(end_key_.datums_));
  } else if (FALSE_IT(end_key_.datums_ = rowkey_datums)) {
  } else if (OB_FAIL(end_key_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize end key", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(val_.rowkey_count_ != end_key_.datum_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey count mismatch", K(ret), K(val_.rowkey_count_), K(end_key_.datum_cnt_));
  } else if (OB_UNLIKELY(pos - initial_pos != serialize_size || !is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to deserialize macro block meta, unexpected error",
             K(ret), K(pos), K(initial_pos), K(serialize_size), K(data_len), KPC(this));
  }
  return ret;
}

int64_t ObDataMacroBlockMeta::get_serialize_size(const int64_t data_version) const
{
  // encoding version and serialize_size (32 + 64)
  return sizeof(version_) + sizeof(int64_t) + val_.get_serialize_size(data_version) + end_key_.get_serialize_size();
}

} // end namespace blocksstable
} // end namespace blocksstable
