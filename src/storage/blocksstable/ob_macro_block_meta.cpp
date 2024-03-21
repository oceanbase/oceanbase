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

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_row_reader.h"

namespace oceanbase
{
namespace blocksstable
{
ObDataBlockMetaVal::ObDataBlockMetaVal()
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

ObDataBlockMetaVal::ObDataBlockMetaVal(ObIAllocator &allocator)
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
    column_checksums_(sizeof(int64_t), ModulePageAllocator(allocator, "MacroMetaChksum")),
    has_string_out_row_(false),
    all_lob_in_row_(false),
    agg_row_len_(0),
    agg_row_buf_(nullptr)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}
ObDataBlockMetaVal::~ObDataBlockMetaVal()
{
  reset();
}

void ObDataBlockMetaVal::reset()
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

bool ObDataBlockMetaVal::is_valid() const
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
    && (0 == agg_row_len_ || nullptr != agg_row_buf_)
    && (ddl_end_row_offset_ == -1 || (version_ >= DATA_BLOCK_META_VAL_VERSION_V2 && ddl_end_row_offset_ >= 0));
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
    is_last_row_last_flag_ = val.is_last_row_last_flag_;
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
  }
  return ret;
}

int ObDataBlockMetaVal::build_value(ObStorageDatum &datum, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const int64_t size = get_serialize_size();
  const int64_t estimate_size = get_max_serialize_size();
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(size > estimate_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected size", K(ret), K(size), K(estimate_size), KPC(this));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(size));
  } else if (OB_FAIL(serialize(buf, size, pos))) {
    LOG_WARN("fail to serialize", K(ret), K(size));
  } else {
    ObString str(size, buf);
    datum.set_string(str);
  }
  return ret;
}

DEFINE_SERIALIZE(ObDataBlockMetaVal)
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
    const_cast<ObDataBlockMetaVal *>(this)->length_ = get_serialize_size();
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

DEFINE_DESERIALIZE(ObDataBlockMetaVal)
{
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
        if (agg_row_len_ > 0) {
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
int64_t ObDataBlockMetaVal::get_max_serialize_size() const
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
DEFINE_GET_SERIALIZE_SIZE(ObDataBlockMetaVal)
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

//================================== ObDataMacroBlockMeta ==================================
ObDataMacroBlockMeta::ObDataMacroBlockMeta()
  : val_(),
    end_key_(),
    nested_offset_(0),
    nested_size_(0)
{
}

ObDataMacroBlockMeta::ObDataMacroBlockMeta(ObIAllocator &allocator)
  : val_(allocator),
    end_key_(),
    nested_offset_(0),
    nested_size_(0)
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
  char *buf = nullptr;
  const int64_t buf_len = sizeof(ObDataMacroBlockMeta) + sizeof(ObStorageDatum) * rowkey_count;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src macro meta is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(buf_len));
  } else if (0 != val_.agg_row_len_
      && OB_ISNULL(agg_row_buf = static_cast<char *>(allocator.alloc(val_.agg_row_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for agg row", K(ret), K_(val));
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

int ObDataMacroBlockMeta::build_estimate_row(ObDatumRow &row, ObIAllocator &allocator) const
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
      const int64_t size = val_.get_max_serialize_size();
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


int ObDataMacroBlockMeta::build_row(ObDatumRow &row, ObIAllocator &allocator) const
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
      if (OB_FAIL(val_.build_value(row.storage_datums_[val_.rowkey_count_], allocator))) {
        LOG_WARN("Fail to build value for macro meta", K(ret));
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
} // end namespace blocksstable
} // end namespace blocksstable

