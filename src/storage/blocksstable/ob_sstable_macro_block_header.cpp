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

#include "ob_sstable_macro_block_header.h"
#include "ob_macro_block.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace blocksstable
{

//=====================ObSSTableMacroBlockHeader========================
ObSSTableMacroBlockHeader::ObSSTableMacroBlockHeader()
  : fixed_header_(),
    column_types_(nullptr),
    column_orders_(nullptr),
    column_checksum_(nullptr),
    is_normal_cg_(false),
    is_inited_(false)
{
}

ObSSTableMacroBlockHeader::~ObSSTableMacroBlockHeader()
{
  reset();
}

void ObSSTableMacroBlockHeader::reset()
{
  fixed_header_.reset();
  column_types_ = nullptr;
  column_orders_ = nullptr;
  column_checksum_ = nullptr;
  is_normal_cg_ = false;
  is_inited_ = false;
}

int64_t ObSSTableMacroBlockHeader::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(fixed_header), KP_(column_types), KP_(column_orders), KP_(column_checksum), K_(is_normal_cg));
    J_COMMA();
    J_NAME("column_checksum");
    J_COLON();
    J_ARRAY_START();
    for (int64_t i = 0; i < fixed_header_.column_count_; ++i) {
      if (0 != i) {
        J_COMMA();
      }
      BUF_PRINTO(column_checksum_[i]);
    }
    J_ARRAY_END();
    J_OBJ_END();
  }
  return pos;
}

bool ObSSTableMacroBlockHeader::is_valid() const
{
  return fixed_header_.is_valid()
         && nullptr != column_types_
         && nullptr != column_orders_
         && nullptr != column_checksum_;
}

ObSSTableMacroBlockHeader::FixedHeader::FixedHeader()
  : header_size_(0),
    version_(SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2),
    magic_(SSTABLE_MACRO_BLOCK_HEADER_MAGIC),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    logical_version_(0),
    data_seq_(0),
    column_count_(0),
    rowkey_column_count_(0),
    row_store_type_(0),
    row_count_(0),
    occupy_size_(0),
    micro_block_count_(0),
    micro_block_data_offset_(0),
    micro_block_data_size_(0),
    idx_block_offset_(0),
    idx_block_size_(0),
    meta_block_offset_(0),
    meta_block_size_(0),
    data_checksum_(0),
    encrypt_id_(0),
    master_key_id_(-1),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR)
{
  MEMSET(encrypt_key_, 0x26, OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

bool ObSSTableMacroBlockHeader::FixedHeader::is_valid() const
{
  return header_size_ > 0
      && SSTABLE_MACRO_BLOCK_HEADER_VERSION_V1 <= version_
      && SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2 >= version_
      && SSTABLE_MACRO_BLOCK_HEADER_MAGIC == magic_
      && 0 != tablet_id_
      && logical_version_ >= 0
      //&& column_count_ >= rowkey_column_count_
      && rowkey_column_count_ >= 0
      && row_store_type_ >= 0
      && row_count_ > 0
      && occupy_size_ > 0
      && micro_block_count_ > 0
      && micro_block_data_offset_ > 0
      && micro_block_data_size_ > 0
      && data_checksum_ >= 0
      && encrypt_id_ >= 0
      && master_key_id_ >= -1
      && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR;
}

void ObSSTableMacroBlockHeader::FixedHeader::reset()
{
  header_size_ = 0;
  version_ = SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2;
  magic_ = SSTABLE_MACRO_BLOCK_HEADER_MAGIC;
  tablet_id_ = ObTabletID::INVALID_TABLET_ID;
  logical_version_ = 0;
  data_seq_ = 0;
  column_count_ = 0;
  rowkey_column_count_ = 0;
  row_store_type_ = 0;
  row_count_ = 0;
  occupy_size_ = 0;
  micro_block_count_ = 0;
  micro_block_data_offset_ = 0;
  micro_block_data_size_ = 0;
  idx_block_offset_ = 0;
  idx_block_size_ = 0;
  meta_block_offset_ = 0;
  meta_block_size_ = 0;
  data_checksum_ = 0;
  encrypt_id_ = 0;
  master_key_id_ = -1;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  MEMSET(encrypt_key_, 0x26, OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}

int ObSSTableMacroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("no initialize", K(ret), K(is_inited_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid() || get_serialize_size() != fixed_header_.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block header is invalid", K(ret), K(*this));
  } else {
    const int64_t col_type_array_cnt = fixed_header_.get_col_type_array_cnt();
    int64_t tmp_pos = pos;
    FixedHeader *fixed_header = reinterpret_cast<FixedHeader *>(buf + tmp_pos);
    *fixed_header = fixed_header_;
    tmp_pos += get_fixed_header_size();
    if (buf + tmp_pos != reinterpret_cast<char *>(column_types_)) {
      MEMCPY(buf + tmp_pos, column_types_, col_type_array_cnt * sizeof(ObObjMeta));
    }
    tmp_pos += col_type_array_cnt * sizeof(ObObjMeta);
    if (buf + tmp_pos != reinterpret_cast<char *>(column_orders_)) {
      MEMCPY(buf + tmp_pos, column_orders_, col_type_array_cnt * sizeof(ObOrderType));
    }
    tmp_pos += col_type_array_cnt * sizeof(ObOrderType);
    if (buf + tmp_pos != reinterpret_cast<char *>(column_checksum_)) {
      MEMCPY(buf + tmp_pos, column_checksum_, fixed_header_.column_count_ * sizeof(int64_t));
    }
    tmp_pos += fixed_header_.column_count_ * sizeof(int64_t);
    bool *is_normal_cg = reinterpret_cast<bool *>(buf + tmp_pos);
    *is_normal_cg = is_normal_cg_;
    tmp_pos += sizeof(is_normal_cg_);
    if (OB_UNLIKELY(get_serialize_size() != tmp_pos - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialize size doesn't match get_serialize_size func", K(ret), K(tmp_pos), K(pos),
          "get_serialize_size()", get_serialize_size());
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

int ObSSTableMacroBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const FixedHeader *fixed_header = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("maybe initialized, cann't de-serialize again", K(ret), K(is_inited_));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos + sizeof(FixedHeader) > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (FALSE_IT(fixed_header = reinterpret_cast<const FixedHeader *>(buf + pos))) {
  } else if (OB_UNLIKELY(pos + fixed_header->header_size_ > data_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("data buffer is not enough", K(ret), K(pos), K(data_len), KPC(fixed_header));
  } else {
    int64_t tmp_pos = pos;
    fixed_header_ = *fixed_header;
    const int64_t col_type_array_cnt = fixed_header_.get_col_type_array_cnt();
    tmp_pos += get_fixed_header_size();
    const int64_t max_pos = pos + fixed_header->header_size_;
    const int64_t obj_metas_size =  col_type_array_cnt * sizeof(ObObjMeta);
    const int64_t ord_types_size = col_type_array_cnt * sizeof(ObOrderType);
    const int64_t chksum_size = fixed_header->column_count_ * sizeof(int64_t);
    column_types_ = nullptr;
    column_orders_ = nullptr;
    column_checksum_ = nullptr;
    is_normal_cg_ = false;
    if (tmp_pos + obj_metas_size <= max_pos) {
      column_types_ = reinterpret_cast<ObObjMeta *>(const_cast<char *>(buf + tmp_pos));
    }
    tmp_pos += obj_metas_size;

    if (tmp_pos + ord_types_size <= max_pos) {
      column_orders_ = reinterpret_cast<ObOrderType *>(const_cast<char *>(buf + tmp_pos));
    }
    tmp_pos += ord_types_size;

    if (tmp_pos + chksum_size <= max_pos) {
      column_checksum_ = reinterpret_cast<int64_t *>(const_cast<char *>(buf + tmp_pos));
    }
    tmp_pos += chksum_size;

    if (tmp_pos + sizeof(is_normal_cg_) <= max_pos) {
      is_normal_cg_ = *(reinterpret_cast<bool *>(const_cast<char *>(buf + tmp_pos)));
      tmp_pos += sizeof(is_normal_cg_);
    }
    fixed_header_.header_size_ = get_serialize_size();
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block header is invalid", K(ret), KPC(this), K(tmp_pos), K(pos));
    } else {
      pos = tmp_pos;
      is_inited_ = true;
    }
  }
  return ret;
}

int64_t ObSSTableMacroBlockHeader::get_serialize_size() const
{
  return get_fixed_header_size() + get_variable_size_in_header(
    fixed_header_.column_count_, fixed_header_.rowkey_column_count_, fixed_header_.version_)
    + sizeof(is_normal_cg_);
}

int64_t ObSSTableMacroBlockHeader::get_fixed_header_size()
{
  return sizeof(FixedHeader);
}

int64_t ObSSTableMacroBlockHeader::get_variable_size_in_header(
    const int64_t column_cnt,
    const int64_t rowkey_col_cnt,
    const uint16_t version)
{
  const int64_t col_type_array_cnt = SSTABLE_MACRO_BLOCK_HEADER_VERSION_V2 == version ? rowkey_col_cnt : column_cnt;
  return col_type_array_cnt * sizeof(ObObjMeta) /* ObObjMeta */
       + col_type_array_cnt * sizeof(ObOrderType) /* column orders */
       + column_cnt * sizeof(int64_t) /* column checksum */;
}

int ObSSTableMacroBlockHeader::init(
    const ObDataStoreDesc &desc,
    common::ObObjMeta *col_types,
    common::ObOrderType *col_orders,
    int64_t *col_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot initialize twice", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())
             || OB_ISNULL(col_types)
             || OB_ISNULL(col_orders)
             || OB_ISNULL(col_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(desc), KP(col_types), KP(col_orders), KP(col_checksum));
  } else {
    fixed_header_.version_ = desc.get_fixed_header_version();
    fixed_header_.header_size_ = static_cast<int32_t>(get_fixed_header_size()
        + get_variable_size_in_header(desc.get_row_column_count(), desc.get_rowkey_column_count(), fixed_header_.version_))
        + sizeof(is_normal_cg_);
    fixed_header_.tablet_id_ = desc.get_tablet_id().id();
    fixed_header_.logical_version_ = desc.get_logical_version();
    fixed_header_.column_count_ =  static_cast<int32_t>(desc.get_row_column_count());
    fixed_header_.rowkey_column_count_ = static_cast<int32_t>(desc.get_rowkey_column_count());
    fixed_header_.row_store_type_ = static_cast<int32_t>(desc.get_row_store_type());
    fixed_header_.micro_block_data_offset_ = fixed_header_.header_size_
        + static_cast<int32_t>(ObMacroBlockCommonHeader::get_serialize_size());
    fixed_header_.encrypt_id_ = desc.get_encrypt_id();
    fixed_header_.master_key_id_ = desc.get_master_key_id();
    //the length of encrypt_key is always fixed
    MEMCPY(fixed_header_.encrypt_key_, desc.get_encrypt_key(), desc.get_encrypt_key_size());
    fixed_header_.compressor_type_ = desc.get_compressor_type();
    column_types_ = col_types;
    column_orders_ = col_orders;
    column_checksum_ = col_checksum;


    const ObIArray<share::schema::ObColDesc> &col_descs = desc.is_major_merge_type() ? desc.get_full_stored_col_descs() :desc.get_rowkey_col_descs();
    const int64_t col_descs_cnt = fixed_header_.get_col_type_array_cnt();
    if (OB_UNLIKELY(col_descs_cnt > col_descs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col desc array is unexpected invalid", K(ret), K(col_descs_cnt), K(desc));
    } else {
      for (int64_t i = 0; i < col_descs_cnt; ++i) {
        column_types_[i] = col_descs.at(i).col_type_;
        column_orders_[i] = col_descs.at(i).col_order_;
      }
    }
    //for compatibility, fill 0 to checksum and this will be serialized to disk
    for (int i = 0; i < fixed_header_.column_count_; i++) {
      column_checksum_[i] = 0;
    }
    is_normal_cg_ = desc.is_cg();
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}


} // end namespace blocksstable
} // end namespace oceanbase
