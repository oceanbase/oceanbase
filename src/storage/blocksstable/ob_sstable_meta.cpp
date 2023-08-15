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

#include "ob_sstable_meta.h"
#include "share/schema/ob_schema_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_macro_block_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace blocksstable
{
//================================== ObSSTableBasicMeta ==================================
ObSSTableBasicMeta::ObSSTableBasicMeta()
  : version_(SSTABLE_BASIC_META_VERSION),
    length_(0),
    row_count_(0),
    occupy_size_(0),
    original_size_(0),
    data_checksum_(0),
    index_type_(ObIndexType::INDEX_TYPE_IS_NOT),
    rowkey_column_count_(0),
    column_cnt_(0),
    data_macro_block_count_(0),
    data_micro_block_count_(0),
    use_old_macro_block_count_(0),
    index_macro_block_count_(0),
    sstable_format_version_(SSTABLE_FORMAT_VERSION_1),
    schema_version_(0),
    create_snapshot_version_(0),
    progressive_merge_round_(0),
    progressive_merge_step_(0),
    upper_trans_version_(0),
    max_merged_trans_version_(0),
    recycle_version_(0),
    ddl_scn_(SCN::min_scn()),
    filled_tx_scn_(SCN::min_scn()),
    data_index_tree_height_(0),
    table_mode_(),
    status_(0),
    contain_uncommitted_row_(false),
    root_row_store_type_(ObRowStoreType::MAX_ROW_STORE),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    encrypt_id_(0),
    master_key_id_(0),
    sstable_logic_seq_(0),
    latest_row_store_type_(ObRowStoreType::MAX_ROW_STORE)
{
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

bool ObSSTableBasicMeta::operator!=(const ObSSTableBasicMeta &other) const
{
  return !(*this == other);
}
bool ObSSTableBasicMeta::operator==(const ObSSTableBasicMeta &other) const
{
  // don't need to compare upper_trans_version, because meta's upper_trans_version
  // may be different from sstable shell's
  return version_ == other.version_
      && length_ == other.length_
      && row_count_ == other.row_count_
      && occupy_size_ == other.occupy_size_
      && original_size_ == other.original_size_
      && data_checksum_ == other.data_checksum_
      && index_type_ == other.index_type_
      && rowkey_column_count_ == other.rowkey_column_count_
      && column_cnt_ == other.column_cnt_
      && data_macro_block_count_ == other.data_macro_block_count_
      && data_micro_block_count_ == other.data_micro_block_count_
      && use_old_macro_block_count_ == other.use_old_macro_block_count_
      && index_macro_block_count_ == other.index_macro_block_count_
      && sstable_format_version_ == other.sstable_format_version_
      && schema_version_ == other.schema_version_
      && create_snapshot_version_ == other.create_snapshot_version_
      && progressive_merge_round_ == other.progressive_merge_round_
      && progressive_merge_step_ == other.progressive_merge_step_
      && max_merged_trans_version_ == other.max_merged_trans_version_
      && recycle_version_ == other.recycle_version_
      && ddl_scn_ == other.ddl_scn_
      && filled_tx_scn_ == other.filled_tx_scn_
      && data_index_tree_height_ == other.data_index_tree_height_
      && sstable_logic_seq_ == other.sstable_logic_seq_
      && table_mode_ == other.table_mode_
      && status_ == other.status_
      && contain_uncommitted_row_ == other.contain_uncommitted_row_
      && root_row_store_type_ == other.root_row_store_type_
      && compressor_type_ == other.compressor_type_
      && encrypt_id_ == other.encrypt_id_
      && master_key_id_ == other.master_key_id_
      && 0 == MEMCMP(encrypt_key_, other.encrypt_key_, sizeof(encrypt_key_))
      && latest_row_store_type_ == other.latest_row_store_type_;
}


bool ObSSTableBasicMeta::is_valid() const
{
  bool ret = (SSTABLE_BASIC_META_VERSION == version_
           && row_count_ >= 0
           && occupy_size_ >= 0
           && original_size_ >= 0
           && data_checksum_ >= 0
           && rowkey_column_count_ >= 0
           && index_type_ >= 0
           && data_macro_block_count_ >= 0
           && data_micro_block_count_ >= 0
           && use_old_macro_block_count_ >= 0
           && index_macro_block_count_ >= 0
           && column_cnt_ >= rowkey_column_count_
           && create_snapshot_version_ >= 0
           && table_mode_.is_valid()
           && upper_trans_version_ >= 0
           && max_merged_trans_version_ >= 0
           && recycle_version_ >= 0
           && ddl_scn_.is_valid()
           && filled_tx_scn_.is_valid()
           && data_index_tree_height_ >= 0
           && sstable_logic_seq_ >= 0
           && root_row_store_type_ < ObRowStoreType::MAX_ROW_STORE
           && is_latest_row_store_type_valid());
  return ret;
}

void ObSSTableBasicMeta::reset()
{
  version_ = SSTABLE_BASIC_META_VERSION;
  length_ = 0;
  row_count_ = 0;
  occupy_size_ = 0;
  original_size_ = 0;
  data_checksum_ = 0;
  index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  rowkey_column_count_ = 0;
  column_cnt_ = 0;
  data_macro_block_count_ = 0;
  data_micro_block_count_ = 0;
  use_old_macro_block_count_ = 0;
  index_macro_block_count_ = 0;
  sstable_format_version_ = SSTABLE_FORMAT_VERSION_1;
  schema_version_ = 0;
  create_snapshot_version_ = 0;
  progressive_merge_round_ = 0;
  progressive_merge_step_ = 0;
  upper_trans_version_ = 0;
  max_merged_trans_version_ = 0;
  recycle_version_ = 0;
  ddl_scn_.set_min();
  filled_tx_scn_.set_min();
  data_index_tree_height_ = 0;
  table_mode_.reset();
  status_ = SSTABLE_NOT_INIT;
  contain_uncommitted_row_ = false;
  root_row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  encrypt_id_ = 0;
  master_key_id_ = 0;
  sstable_logic_seq_ = 0;
  MEMSET(encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  latest_row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
}

DEFINE_SERIALIZE(ObSSTableBasicMeta)
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
    const_cast<ObSSTableBasicMeta *>(this)->length_ = get_serialize_size();
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
                  row_count_,
                  occupy_size_,
                  original_size_,
                  data_checksum_,
                  index_type_,
                  rowkey_column_count_,
                  column_cnt_,
                  data_macro_block_count_,
                  data_micro_block_count_,
                  use_old_macro_block_count_,
                  index_macro_block_count_,
                  sstable_format_version_,
                  schema_version_,
                  create_snapshot_version_,
                  progressive_merge_round_,
                  progressive_merge_step_,
                  upper_trans_version_,
                  max_merged_trans_version_,
                  recycle_version_,
                  ddl_scn_,
                  filled_tx_scn_,
                  data_index_tree_height_,
                  table_mode_,
                  status_,
                  contain_uncommitted_row_,
                  root_row_store_type_,
                  compressor_type_,
                  encrypt_id_,
                  master_key_id_,
                  sstable_logic_seq_,
                  latest_row_store_type_);
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, serialize may have bug", K(ret), K(pos), K(start_pos), KPC(this));
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSTableBasicMeta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    int64_t start_pos = pos;
    if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
      LOG_WARN("fail to decode version", K(ret), K(data_len), K(pos));
    } else if (OB_UNLIKELY(version_ != SSTABLE_BASIC_META_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("object version mismatch", K(ret), K(version_));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &length_))) {
      LOG_WARN("fail to decode length", K(ret), K(data_len), K(pos));
    } else if (OB_UNLIKELY(pos + sizeof(encrypt_key_) > data_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect buf_len", K(ret), K(data_len), K(pos));
    } else {
      //Since the data len is greater than the actual length_, it is not compatible when adding a field
      if (OB_FAIL(decode_for_compat(buf, start_pos + length_, pos))) {
        LOG_WARN("failed to decode", K(ret), K(pos), K(start_pos), KPC(this));
      } else if (OB_UNLIKELY(length_ != pos - start_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, deserialize may has bug", K(ret), K(pos), K(start_pos), KPC(this));
      }
    }
  }
  return ret;
}

int ObSSTableBasicMeta::decode_for_compat(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // set latest_row_store_type to invalid on deserialize for compatibility
  latest_row_store_type_ = ObRowStoreType::DUMMY_ROW_STORE;
  MEMCPY(encrypt_key_, buf + pos, sizeof(encrypt_key_));
  pos += sizeof(encrypt_key_);
  LST_DO_CODE(OB_UNIS_DECODE,
              row_count_,
              occupy_size_,
              original_size_,
              data_checksum_,
              index_type_,
              rowkey_column_count_,
              column_cnt_,
              data_macro_block_count_,
              data_micro_block_count_,
              use_old_macro_block_count_,
              index_macro_block_count_,
              sstable_format_version_,
              schema_version_,
              create_snapshot_version_,
              progressive_merge_round_,
              progressive_merge_step_,
              upper_trans_version_,
              max_merged_trans_version_,
              recycle_version_,
              ddl_scn_,
              filled_tx_scn_,
              data_index_tree_height_,
              table_mode_,
              status_,
              contain_uncommitted_row_,
              root_row_store_type_,
              compressor_type_,
              encrypt_id_,
              master_key_id_,
              sstable_logic_seq_,
              latest_row_store_type_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSTableBasicMeta)
{
  int64_t len = 0;
  len += serialization::encoded_length_i32(version_);
  len += serialization::encoded_length_i32(length_);
  len += sizeof(encrypt_key_);
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              row_count_,
              occupy_size_,
              original_size_,
              data_checksum_,
              index_type_,
              rowkey_column_count_,
              column_cnt_,
              data_macro_block_count_,
              data_micro_block_count_,
              use_old_macro_block_count_,
              index_macro_block_count_,
              sstable_format_version_,
              schema_version_,
              create_snapshot_version_,
              progressive_merge_round_,
              progressive_merge_step_,
              upper_trans_version_,
              max_merged_trans_version_,
              recycle_version_,
              ddl_scn_,
              filled_tx_scn_,
              data_index_tree_height_,
              table_mode_,
              status_,
              contain_uncommitted_row_,
              root_row_store_type_,
              compressor_type_,
              encrypt_id_,
              master_key_id_,
              sstable_logic_seq_,
              latest_row_store_type_);
  return len;
}

int ObSSTableBasicMeta::set_upper_trans_version(const int64_t upper_trans_version)
{
  int ret = OB_SUCCESS;
  const int64_t old_val = ATOMIC_LOAD(&upper_trans_version_);
  // only set once
  if (INT64_MAX == old_val && INT64_MAX != upper_trans_version) {
    const int64_t new_val = std::max(upper_trans_version, max_merged_trans_version_);
    ATOMIC_CAS(&upper_trans_version_, old_val, new_val);
  }
  return ret;
}

//================================== ObSSTableMeta ==================================
ObSSTableMeta::ObSSTableMeta()
  : basic_meta_(),
    data_root_info_(),
    macro_info_(),
    column_checksums_(nullptr),
    column_checksum_count_(0),
    is_inited_(false)
{
}

ObSSTableMeta::~ObSSTableMeta()
{
  reset();
}

int ObSSTableMeta::load_root_block_data(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObMicroBlockDesMeta des_meta(basic_meta_.compressor_type_, basic_meta_.encrypt_id_,
                               basic_meta_.master_key_id_, basic_meta_.encrypt_key_);
  if (OB_UNLIKELY(SSTABLE_WRITE_BUILDING != basic_meta_.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("state is not match.", K(ret), K_(basic_meta_.status));
  } else if (OB_FAIL(data_root_info_.load_root_block_data(allocator, des_meta))) {
    LOG_WARN("fail to load root block data for data root info", K(ret), K(data_root_info_));
  } else if (OB_FAIL(macro_info_.load_root_block_data(allocator, des_meta))) {
    LOG_WARN("fail to load root block data for macro info", K(ret), K(macro_info_));
  }
  return ret;
}

void ObSSTableMeta::reset()
{
  data_root_info_.reset();
  macro_info_.reset();
  basic_meta_.reset();
  column_checksums_ = nullptr;
  column_checksum_count_ = 0;
  is_inited_ = false;
}

int ObSSTableMeta::init_base_meta(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else {
    basic_meta_.status_ = SSTABLE_INIT;
    basic_meta_.row_count_ = param.row_count_;
    basic_meta_.occupy_size_ = param.occupy_size_;
    basic_meta_.original_size_ = param.original_size_;
    basic_meta_.data_checksum_ = param.data_checksum_;
    basic_meta_.index_type_ = param.index_type_;
    basic_meta_.rowkey_column_count_ = param.rowkey_column_cnt_;
    basic_meta_.column_cnt_ = param.column_cnt_;
    basic_meta_.data_macro_block_count_ = param.data_blocks_cnt_;
    basic_meta_.data_micro_block_count_ = param.micro_block_cnt_;
    basic_meta_.use_old_macro_block_count_ = param.use_old_macro_block_count_;
    basic_meta_.index_macro_block_count_ = param.index_blocks_cnt_;
    basic_meta_.sstable_format_version_ = ObSSTableBasicMeta::SSTABLE_FORMAT_VERSION_1;
    basic_meta_.schema_version_ = param.schema_version_;
    basic_meta_.create_snapshot_version_ = param.create_snapshot_version_;
    basic_meta_.progressive_merge_round_ = param.progressive_merge_round_;
    basic_meta_.progressive_merge_step_ = param.progressive_merge_step_;
    basic_meta_.table_mode_ = param.table_mode_;
    basic_meta_.contain_uncommitted_row_ = param.contain_uncommitted_row_;
    basic_meta_.max_merged_trans_version_ = param.max_merged_trans_version_;
    basic_meta_.recycle_version_ = param.recycle_version_;
    basic_meta_.upper_trans_version_ = contain_uncommitted_row() ?
        INT64_MAX : basic_meta_.max_merged_trans_version_;
    basic_meta_.ddl_scn_ = param.ddl_scn_;
    basic_meta_.filled_tx_scn_ = param.filled_tx_scn_;
    basic_meta_.data_index_tree_height_ = param.data_index_tree_height_;
    basic_meta_.sstable_logic_seq_ = param.sstable_logic_seq_;
    basic_meta_.root_row_store_type_ = param.root_row_store_type_;
    basic_meta_.latest_row_store_type_ = param.latest_row_store_type_;
    basic_meta_.compressor_type_ = param.compressor_type_;
    basic_meta_.encrypt_id_ = param.encrypt_id_;
    basic_meta_.master_key_id_ = param.master_key_id_;
    MEMCPY(basic_meta_.encrypt_key_, param.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    basic_meta_.length_ = basic_meta_.get_serialize_size();
    if (OB_FAIL(prepare_column_checksum(param.column_checksums_, allocator))) {
      LOG_WARN("fail to prepare column checksum", K(ret), K(param));
    }
  }
  return ret;
}

int ObSSTableMeta::init_data_index_tree_info(
    const storage::ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", K(ret), K(param));
  } else if (OB_FAIL(data_root_info_.init_root_block_info(allocator, param.root_block_addr_,
      param.root_block_data_))) {
    LOG_WARN("fail to init data root info", K(ret), K(param));
  } else {
    basic_meta_.status_ = SSTABLE_WRITE_BUILDING;
  }
  return ret;
}

int ObSSTableMeta::prepare_column_checksum(
    const common::ObIArray<int64_t> &column_checksums,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t count = column_checksums.count();
  if (OB_UNLIKELY(nullptr != column_checksums_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column checksum isn't empty, cannot initialize twice", K(ret), KP(column_checksums_));
  } else if (count > 0 && OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate column checksum memory", K(ret), K(count));
  } else {
    column_checksum_count_ = column_checksums.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_checksums.count(); ++i) {
      column_checksums_[i] = column_checksums.at(i);
    }
  }
  return ret;
}

bool ObSSTableMeta::check_meta() const
{
  return basic_meta_.is_valid()
      && data_root_info_.is_valid()
      && macro_info_.is_valid();
}

int ObSSTableMeta::init(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot initialize twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else if (OB_FAIL(init_base_meta(param, allocator))) {
    LOG_WARN("fail to init basic meta", K(ret), K(param));
  } else if (OB_FAIL(init_data_index_tree_info(param, allocator))) {
    LOG_WARN("fail to init data index tree info", K(ret), K(param));
  } else if (OB_UNLIKELY(SSTABLE_WRITE_BUILDING != basic_meta_.status_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sstable state is not match.", K(ret), K(basic_meta_.status_));
  } else if (OB_FAIL(macro_info_.init_macro_info(allocator, param))) {
    LOG_WARN("fail to init macro info", K(ret), K(param));
  } else if (OB_FAIL(load_root_block_data(allocator))) {
    LOG_WARN("fail to load root block data", K(ret), K(param));
  } else if (OB_UNLIKELY(!check_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check meta", K(ret), K(*this));
  } else {
    if (param.is_ready_for_read_) {
      basic_meta_.status_ = SSTABLE_READY_FOR_READ;
    }
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObSSTableMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t tmp_pos = 0;
    const int64_t len = get_serialize_size_();
    OB_UNIS_ENCODE(SSTABLE_META_VERSION);
    OB_UNIS_ENCODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialize_(buf + pos, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize_", K(ret), KP(buf), K(buf_len), K(pos), K(tmp_pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObSSTableMeta::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize basic meta", K(ret), KP(buf), K(buf_len), K(pos), K_(basic_meta));
  } else {
    OB_UNIS_ENCODE_ARRAY(column_checksums_, column_checksum_count_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_root_info_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize data root info", K(ret), K(buf_len), K(pos), K(data_root_info_));
    } else if (OB_FAIL(macro_info_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize macro info", K(ret), K(buf_len), K(pos), K(macro_info_));
    }
  }
  return ret;
}

int ObSSTableMeta::deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  int64_t len = 0;
  int64_t version = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited sstable meta", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(version != SSTABLE_META_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("object version mismatch", K(ret), K(version));
    } else if (OB_FAIL(deserialize_(allocator, buf + pos, data_len, tmp_pos))) {
      LOG_WARN("fail to deserialize_", K(ret), K(data_len), K(tmp_pos), K(pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObSSTableMeta::deserialize_(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize basic meta", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(column_checksum_count_);
    if (OB_FAIL(ret)) {
    } else if (column_checksum_count_ > 0 && OB_ISNULL(column_checksums_ = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * column_checksum_count_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate column checksum memory", K(ret), K(column_checksum_count_));
    } else {
      OB_UNIS_DECODE_ARRAY(column_checksums_, column_checksum_count_);
    }
  }
  if (OB_SUCC(ret)) {
    ObMicroBlockDesMeta des_meta(basic_meta_.compressor_type_, basic_meta_.encrypt_id_,
                                 basic_meta_.master_key_id_, basic_meta_.encrypt_key_);
    if (OB_FAIL(data_root_info_.deserialize(allocator, des_meta, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize data root info", K(ret), K(data_len), K(pos), K(des_meta));
    } else if (OB_FAIL(macro_info_.deserialize(allocator, des_meta, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize macro info", K(ret), K(data_len), K(pos), K(des_meta));
    }
  }
  return ret;
}

int64_t ObSSTableMeta::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t payload_size = get_serialize_size_();
  OB_UNIS_ADD_LEN(SSTABLE_META_VERSION);
  OB_UNIS_ADD_LEN(payload_size);
  len += get_serialize_size_();
  return len;
}

int64_t ObSSTableMeta::get_serialize_size_() const
{
  int64_t len = 0;
  MacroBlockId id_map_entry_id;
  len += basic_meta_.get_serialize_size();
  OB_UNIS_ADD_LEN_ARRAY(column_checksums_, column_checksum_count_);
  len += data_root_info_.get_serialize_size();
  len += macro_info_.get_serialize_size();
  return len;
}

int64_t ObSSTableMeta::get_variable_size() const
{
  return sizeof(int64_t) * column_checksum_count_ // column checksums
       + data_root_info_.get_variable_size()
       + macro_info_.get_variable_size();
}

int ObSSTableMeta::deep_copy(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObSSTableMeta *&dest) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  const int64_t deep_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_size + pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_size), K(pos));
  } else {
    char *meta_buf = buf + pos;
    dest = new (meta_buf) ObSSTableMeta();
    pos += sizeof(ObSSTableMeta);
    dest->basic_meta_ = basic_meta_;
    dest->column_checksum_count_ = column_checksum_count_;
    dest->column_checksums_ = reinterpret_cast<int64_t *>(buf + pos);
    MEMCPY(dest->column_checksums_, column_checksums_, sizeof(int64_t) * column_checksum_count_);
    pos += sizeof(int64_t) * column_checksum_count_;
    if (OB_FAIL(data_root_info_.deep_copy(buf, buf_len, pos, dest->data_root_info_))) {
      LOG_WARN("fail to deep copy data root info", K(ret), KP(buf), K(buf_len), K(pos), K(data_root_info_));
    } else if (OB_FAIL(macro_info_.deep_copy(buf, buf_len, pos, dest->macro_info_))) {
      LOG_WARN("fail to deep copy macro info", K(ret), KP(buf), K(buf_len), K(pos), K(macro_info_));
    } else {
      dest->is_inited_ = is_inited_;
    }
  }
  return ret;
}

//================================== ObMigrationSSTableParam ==================================
ObMigrationSSTableParam::ObMigrationSSTableParam()
  : allocator_("SSTableParam", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    basic_meta_(),
    column_checksums_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
    table_key_(),
    column_default_checksums_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
    is_small_sstable_(false)
{
}

ObMigrationSSTableParam::~ObMigrationSSTableParam()
{
  reset();
}

void ObMigrationSSTableParam::reset()
{
  table_key_.reset();
  column_checksums_.reset();
  column_default_checksums_.reset();
  basic_meta_.reset();
  is_small_sstable_ = false;
  allocator_.reset();
}

bool ObMigrationSSTableParam::is_valid() const
{
  return basic_meta_.is_valid() && table_key_.is_valid();
}

int ObMigrationSSTableParam::assign(const ObMigrationSSTableParam &param)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign migrate sstable param get invalid argument", K(ret), K(param));
  } else {
    basic_meta_ = param.basic_meta_;
    table_key_ = param.table_key_;
    is_small_sstable_ = param.is_small_sstable_;
    if (OB_FAIL(column_checksums_.assign(param.column_checksums_))) {
      LOG_WARN("fail to assign column checksums", K(ret), K(param));
    } else if (OB_FAIL(column_default_checksums_.assign(param.column_default_checksums_))) {
      LOG_WARN("fail to assign default column checksum", K(ret), K(param));
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObMigrationSSTableParam)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration sstable param is invalid", K(ret), K(*this));
  } else {
    int64_t tmp_pos = 0;
    const int64_t len = get_serialize_size_();
    OB_UNIS_ENCODE(UNIS_VERSION);
    OB_UNIS_ENCODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialize_(buf + pos, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize_", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObMigrationSSTableParam::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize basic meta", K(ret), KP(buf), K(buf_len), K(pos), K_(basic_meta));
  } else if (OB_FAIL(column_checksums_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize column checksums", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(table_key_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize table key", K(ret), KP(buf), K(buf_len), K(pos), K(table_key_));
  } else if (OB_FAIL(column_default_checksums_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize default column checksum", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_small_sstable_))) {
    LOG_WARN("fail to serialize is_small_sstable_", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObMigrationSSTableParam)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = 0;
  int64_t len = 0;
  int64_t version = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  } else {
    OB_UNIS_DECODE(version);
    OB_UNIS_DECODE(len);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(version != UNIS_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("object version mismatch", K(ret), K(version));
    } else if (OB_UNLIKELY(data_len - pos < len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("payload is out of the buf's boundary", K(ret), K(data_len), K(pos), K(len));
    } else if (OB_FAIL(deserialize_(buf + pos, len, tmp_pos))) {
       LOG_WARN("fail to deserialize_", K(ret), KP(buf), K(data_len), K(pos));
    } else if (OB_UNLIKELY(len != tmp_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, serialize may have bug", K(ret), K(len), K(tmp_pos), KPC(this));
    } else {
      pos += tmp_pos;
    }
  }
  return ret;
}

int ObMigrationSSTableParam::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (pos < data_len && OB_FAIL(basic_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize basic meta", K(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(column_checksums_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize column checksums", K(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(table_key_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize table key", K(ret), KP(buf), K(data_len), K(pos), K(table_key_));
  } else if (pos < data_len && OB_FAIL(column_default_checksums_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize default column checksums", K(ret), KP(buf), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_small_sstable_))) {
    LOG_WARN("fail to deserialize is_small_sstable_", K(ret), KP(buf), K(data_len), K(pos));
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObMigrationSSTableParam)
{
  int64_t len = 0;
  const int64_t payload_size = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  OB_UNIS_ADD_LEN(payload_size);
  len += get_serialize_size_();
  return len;
}

int64_t ObMigrationSSTableParam::get_serialize_size_() const
{
  int64_t len = 0;
  len += basic_meta_.get_serialize_size();
  len += column_checksums_.get_serialize_size();
  len += table_key_.get_serialize_size();
  len += column_default_checksums_.get_serialize_size();
  len += serialization::encoded_length_bool(is_small_sstable_);
  return len;
}

int ObSSTableMetaChecker::check_sstable_meta_strict_equality(
    const ObSSTableMeta &old_sstable_meta,
    const ObSSTableMeta &new_sstable_meta)
{
  int ret = OB_SUCCESS;

  if (!old_sstable_meta.is_valid() || !new_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old sstable meta or new sstable meta is invalid", K(ret));
  } else if (OB_UNLIKELY(old_sstable_meta.get_basic_meta() != new_sstable_meta.get_basic_meta())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("new sstable basic meta is not equal to old one", K(ret));
  } else if (OB_UNLIKELY(old_sstable_meta.get_col_checksum_cnt() != new_sstable_meta.get_col_checksum_cnt())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("new sstable column checksum count is not equal to old one", K(ret));
  } else if (OB_UNLIKELY(0 != MEMCMP(old_sstable_meta.get_col_checksum(), new_sstable_meta.get_col_checksum(), old_sstable_meta.get_col_checksum_cnt()))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("new sstable column checksum is not equal to one", K(ret));
  }

  return ret;
}

int ObSSTableMetaChecker::check_sstable_meta(
    const ObSSTableMeta &old_sstable_meta,
    const ObSSTableMeta &new_sstable_meta)
{
  int ret = OB_SUCCESS;

  if (!old_sstable_meta.is_valid() || !new_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old sstable meta or new sstable meta is invalid", K(ret), K(old_sstable_meta), K(new_sstable_meta));
  } else if (OB_FAIL(check_sstable_basic_meta_(old_sstable_meta.get_basic_meta(), new_sstable_meta.get_basic_meta()))) {
    LOG_WARN("failed to check sstable basic meta", K(ret), K(old_sstable_meta), K(new_sstable_meta));
  } else if (OB_FAIL(check_sstable_column_checksum_(old_sstable_meta.get_col_checksum(), old_sstable_meta.get_col_checksum_cnt(),
      new_sstable_meta.get_col_checksum(), new_sstable_meta.get_col_checksum_cnt()))) {
    LOG_WARN("failed to check sstable column checksum", K(ret), K(old_sstable_meta), K(new_sstable_meta));
  }
  return ret;
}

int ObSSTableMetaChecker::check_sstable_meta(
    const ObMigrationSSTableParam &migration_param,
    const ObSSTableMeta &new_sstable_meta)
{
  int ret = OB_SUCCESS;
  if (!migration_param.is_valid() || !new_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("migration param or new sstable meta is invalid", K(ret), K(migration_param), K(new_sstable_meta));
  } else if (OB_FAIL(check_sstable_basic_meta_(migration_param.basic_meta_, new_sstable_meta.get_basic_meta()))) {
    LOG_WARN("failed to check sstable basic meta", K(ret), K(migration_param), K(new_sstable_meta));
  }
  return ret;
}

int ObSSTableMetaChecker::check_sstable_basic_meta_(
    const ObSSTableBasicMeta &old_sstable_basic_meta,
    const ObSSTableBasicMeta &new_sstable_basic_meta)
{
  int ret = OB_SUCCESS;

  if (!old_sstable_basic_meta.is_valid() || !new_sstable_basic_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check sstable meta get invalid argument", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.row_count_ != old_sstable_basic_meta.row_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("row_count_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.occupy_size_ != old_sstable_basic_meta.occupy_size_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("occupy_size_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.data_checksum_ != old_sstable_basic_meta.data_checksum_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("data checksum not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.rowkey_column_count_ != old_sstable_basic_meta.rowkey_column_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("rowkey_column_count_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.index_type_ != old_sstable_basic_meta.index_type_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("index_type_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.data_macro_block_count_ != old_sstable_basic_meta.data_macro_block_count_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("macro_block_count_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  } else if (new_sstable_basic_meta.column_cnt_ != old_sstable_basic_meta.column_cnt_) {
    ret = OB_INVALID_DATA;
    LOG_WARN("column_cnt_ not match", K(ret), K(old_sstable_basic_meta), K(new_sstable_basic_meta));
  }
  return ret;
}

int ObSSTableMetaChecker::check_sstable_column_checksum_(
    const int64_t *old_column_checksum,
    const int64_t old_column_count,
    const int64_t *new_column_checksum,
    const int64_t new_column_count)
{
  int ret = OB_SUCCESS;
  if (old_column_count != new_column_count) {
    ret = OB_INVALID_DATA;
    LOG_WARN("col_checksum_ not match", K(ret), K(old_column_checksum), K(new_column_checksum));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_column_count; ++i) {
      const int64_t &old_col_checksum = old_column_checksum[i];
      const int64_t &new_col_checksum = new_column_checksum[i];
      if (old_col_checksum != new_col_checksum) {
        ret = OB_INVALID_DATA;
        LOG_WARN("column checksum not match", K(ret), K(i), K(old_col_checksum), K(new_col_checksum));
      }
    }
  }
  return ret;
}

int ObSSTableMetaChecker::check_sstable_column_checksum_(
    const common::ObIArray<int64_t> &old_column_checksum,
    const int64_t *new_column_checksum,
    const int64_t new_column_count)
{
  int ret = OB_SUCCESS;
  if (old_column_checksum.count() != new_column_count) {
    ret = OB_INVALID_DATA;
    LOG_WARN("col_checksum_ not match", K(ret), K(old_column_checksum), K(new_column_checksum));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < old_column_checksum.count(); ++i) {
      const int64_t &old_col_checksum = old_column_checksum.at(i);
      const int64_t &new_col_checksum = new_column_checksum[i];
      if (old_col_checksum != new_col_checksum) {
        ret = OB_INVALID_DATA;
        LOG_WARN("column checksum not match", K(ret), K(i), K(old_col_checksum), K(new_col_checksum));
      }
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
