/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_BLKMGR

#include "storage/blocksstable/ob_simplified_sstable_macro_block_header.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace blocksstable
{

ObSimplifiedSSTableMacroBlockHeader::ObSimplifiedSSTableMacroBlockHeader()
    : idx_block_offset_(0),
      idx_block_size_(0),
      first_data_micro_block_offset_(0),
      rowkey_column_count_(0),
      micro_block_count_(0),
      row_store_type_(ObRowStoreType::FLAT_ROW_STORE),
      compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
      encrypt_id_(0),
      master_key_id_(-1),
      is_inited_(false)
{
  MEMSET(encrypt_key_, 0x26, OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
}



ObSimplifiedSSTableMacroBlockHeader::~ObSimplifiedSSTableMacroBlockHeader()
{
  reset();
}

void ObSimplifiedSSTableMacroBlockHeader::reset()
{
  idx_block_offset_ = 0;
  idx_block_size_ = 0;
  first_data_micro_block_offset_ = 0;
  rowkey_column_count_ = 0;
  micro_block_count_ = 0;
  encrypt_id_ = 0;
  master_key_id_ = -1;
  row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  MEMSET(encrypt_key_, 0x26, OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  is_inited_ = false;
}

int ObSimplifiedSSTableMacroBlockHeader::init(const ObSSTableMacroBlockHeader &macro_header,
                                              const int64_t macro_header_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot initialize twice", K(ret));
  } else if (OB_UNLIKELY(!macro_header.is_valid()
                  || macro_header_pos <= 0
                  || macro_header.fixed_header_.idx_block_offset_ <= macro_header_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro header or macro_header_offset", K(ret), K(macro_header), K(macro_header_pos));
  } else if (OB_UNLIKELY(get_serialize_size() > macro_header.get_serialize_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected serialize size", K(ret), K(get_serialize_size()),
                                          K(macro_header.get_serialize_size()));
  } else {
    const int64_t simplified_macro_header_pos =
            macro_header_pos + macro_header.get_serialize_size() - get_serialize_size();
    idx_block_offset_ = macro_header.fixed_header_.idx_block_offset_ - simplified_macro_header_pos;
    idx_block_size_ = macro_header.fixed_header_.idx_block_size_;
    first_data_micro_block_offset_ = macro_header_pos + macro_header.get_serialize_size() - simplified_macro_header_pos;
    rowkey_column_count_ = macro_header.fixed_header_.rowkey_column_count_;
    micro_block_count_ = macro_header.fixed_header_.micro_block_count_;
    encrypt_id_ = macro_header.fixed_header_.encrypt_id_;
    master_key_id_ = macro_header.fixed_header_.master_key_id_;
    row_store_type_ = static_cast<common::ObRowStoreType>(macro_header.fixed_header_.row_store_type_);
    compressor_type_ = macro_header.fixed_header_.compressor_type_;
    MEMCPY(encrypt_key_, macro_header.fixed_header_.encrypt_key_, sizeof(macro_header.fixed_header_.encrypt_key_));
    is_inited_ = true;
  }
  return ret;
}

int ObSimplifiedSSTableMacroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t& pos) const
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
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("simplified macro block header is invalid", K(ret), K(*this));
  } else {
    MEMCPY(buf + pos, this, get_serialize_size());
    pos += get_serialize_size();
  }
  return ret;
}

int ObSimplifiedSSTableMacroBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot initialize twice", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(buf) || data_len <= 0 || pos < 0 || pos + get_serialize_size() > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    MEMCPY(this, buf + pos, get_serialize_size());

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_ERROR("deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObSimplifiedSSTableMacroBlockHeader::is_valid() const
{
  return idx_block_offset_ > 0
      && idx_block_size_ > 0
      && first_data_micro_block_offset_ > 0
      && rowkey_column_count_ >= 0
      && micro_block_count_ > 0
      && encrypt_id_ >= 0
      && master_key_id_ >= -1
      && row_store_type_ >= ObRowStoreType::FLAT_ROW_STORE
      && (row_store_type_ < ObRowStoreType::MAX_ROW_STORE
          || ObRowStoreType::DUMMY_ROW_STORE == row_store_type_)
      && compressor_type_ > ObCompressorType::INVALID_COMPRESSOR
      && compressor_type_ < ObCompressorType::MAX_COMPRESSOR;
}

int ObSimplifiedSSTableMacroBlockHeader::simplify_macro_block(
      char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const char *&simplified_buffer,
      int64_t &simplified_buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t read_pos = 0;
  int64_t macro_header_pos = 0;
  ObMacroBlockCommonHeader common_header;
  ObSSTableMacroBlockHeader macro_header;
  ObSimplifiedSSTableMacroBlockHeader simplified_macro_header;
  if (OB_FAIL(common_header.deserialize(macro_block_buf, macro_block_buf_size, read_pos))) {
    LOG_WARN("Failed to deserialize macro header", K(ret), KP(macro_block_buf), K(macro_block_buf_size));
  } else if (OB_FAIL(common_header.check_integrity())) {
    LOG_ERROR("Invalid common header", K(ret), K(common_header));
  } else if (FALSE_IT(macro_header_pos = read_pos)) {
  } else if (OB_FAIL(macro_header.deserialize(macro_block_buf, macro_block_buf_size, read_pos))) {
    LOG_WARN("Fail to deserialize macro block header", K(ret), K(macro_header), K(macro_block_buf_size), K(read_pos));
  } else if (OB_FAIL(simplified_macro_header.init(macro_header, macro_header_pos))) {
    LOG_WARN("Fail to init simplified_macro_header", K(ret), K(macro_header), K(macro_header_pos));
  } else if (OB_UNLIKELY(!simplified_macro_header.is_valid())) {
    LOG_WARN("Invalid simplified macro header", K(ret), K(simplified_macro_header));
  } else if (macro_header.get_serialize_size() < simplified_macro_header.get_serialize_size()) {
    //Ensure that the allocated buffer can accommodate the serialized structure.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected serialize size", K(ret), K(macro_header.get_serialize_size()),
                                          K(simplified_macro_header.get_serialize_size()));
  } else {
    const int64_t simplified_macro_header_pos =
            macro_header_pos + macro_header.get_serialize_size() - simplified_macro_header.get_serialize_size();
    int64_t pos = simplified_macro_header_pos;
    if (OB_FAIL(simplified_macro_header.serialize(macro_block_buf, macro_block_buf_size, pos))) {
      LOG_WARN("Fail to serialize simplified macro header", K(ret), K(simplified_macro_header));
    } else {
      simplified_buffer = macro_block_buf + simplified_macro_header_pos;
      simplified_buffer_size = macro_header.fixed_header_.idx_block_offset_ + macro_header.fixed_header_.idx_block_size_ - simplified_macro_header_pos;
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
