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

#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_macro_block_bloom_filter.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"

namespace oceanbase
{
namespace blocksstable
{
ObMacroBlockBloomFilter::ObMacroBlockBloomFilter()
    : rowkey_column_count_(0),
      datum_utils_(nullptr),
      enable_macro_block_bloom_filter_(false),
      max_row_count_(0),
      version_(MACRO_BLOCK_BLOOM_FILTER_V1),
      row_count_(0),
      bf_(),
      macro_reader_()
{
}

ObMacroBlockBloomFilter::~ObMacroBlockBloomFilter()
{
  reset();
}

int ObMacroBlockBloomFilter::alloc_bf(const ObDataStoreDesc &data_store_desc, const int64_t bf_size)
{
  int ret = OB_SUCCESS;
  // TODO(baichangmin): 先写死 64KB，之后再改
  if (OB_UNLIKELY(bf_size <= 0 ||
                  !data_store_desc.is_valid() ||
                  !data_store_desc.enable_macro_block_bloom_filter() ||
                  !data_store_desc.get_datum_utils().is_valid() ||
                  data_store_desc.get_row_column_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to allocate new bloom filter, invalid data store desc", K(ret), K(data_store_desc), K(bf_size));
  } else if (OB_UNLIKELY(bf_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to allocate new bloom filter", K(ret), K(bf_));
  } else if (FALSE_IT(max_row_count_ = calc_max_row_count())) {
  } else if (OB_FAIL(bf_.init_by_row_count(max_row_count_, ObBloomFilter::BLOOM_FILTER_FALSE_POSITIVE_PROB))) {
    LOG_WARN("fail to init new bloom filter", K(ret), K(bf_size));
  } else {
    if (data_store_desc.is_cg()) { // Fetch datum utils for rowkey murmurhash.
      const ObITableReadInfo *index_read_info;
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
        LOG_WARN("fail to get index read info for cg sstable", K(ret), K(data_store_desc));
      } else if (OB_UNLIKELY(!index_read_info->get_datum_utils().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid datum utails for cg sstable", K(ret), KPC(index_read_info));
      } else {
        datum_utils_ = &index_read_info->get_datum_utils();
      }
    } else {
      datum_utils_ = &(data_store_desc.get_datum_utils());
    }
    rowkey_column_count_ = datum_utils_->get_rowkey_count();
    enable_macro_block_bloom_filter_ = data_store_desc.enable_macro_block_bloom_filter();
  }
  return ret;
}

bool ObMacroBlockBloomFilter::is_valid() const
{
  return version_ == MACRO_BLOCK_BLOOM_FILTER_V1
         && enable_macro_block_bloom_filter_ == true
         && rowkey_column_count_ > 0
         && datum_utils_ != nullptr
         && datum_utils_->is_valid()
         && max_row_count_ > 0
         && bf_.is_valid();
}

bool ObMacroBlockBloomFilter::should_persist() const
{
  return is_valid() && row_count_ > 0 && row_count_ <= max_row_count_;
}

int64_t ObMacroBlockBloomFilter::calc_max_row_count() const
{
  int64_t bf_nbit = 64 * 1024 * 8;  // 64KB
  double bf_nhash = -std::log(ObBloomFilter::BLOOM_FILTER_FALSE_POSITIVE_PROB) / std::log(2);
  return static_cast<int64_t>(bf_nbit * std::log(2) / bf_nhash);
}

int ObMacroBlockBloomFilter::insert_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  uint64_t key_hash = 0;

  // update row_count_.
  row_count_++;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to insert row", K(ret), KPC(this));
  } else if (row_count_ > max_row_count_) {
    // do nothing.
  } else if (OB_FAIL(rowkey.assign(row.storage_datums_, rowkey_column_count_))) {
    LOG_WARN("fail to fetch rowkey from datum row", K(ret), K(row), K(rowkey_column_count_));
  } else if (OB_FAIL(rowkey.murmurhash(0, *datum_utils_, key_hash))) {
    LOG_WARN("fail to murmurhash rowkey", K(ret), K(rowkey), K(row));
  } else if (OB_FAIL(bf_.insert(key_hash))) {
    LOG_WARN("fail to insert into bloom filter", K(ret), K(key_hash), K(bf_));
  }
  return ret;
}

int ObMacroBlockBloomFilter::insert_micro_block(const ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;

  ObMicroBlockData decompressed_data;
  ObMicroBlockData micro_block_data(micro_block.data_.get_buf(), micro_block.data_.get_buf_size());

  if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert micro block, invalid argument", K(ret), K(micro_block), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to insert micro block, unexpected internal status", K(ret), KPC(this));
  } else if (FALSE_IT(row_count_ += micro_block.header_.row_count_)) { // update row_count_.
  } else if (row_count_ > max_row_count_) {
    // do nothing.
  } else {
    compaction::ObLocalArena temp_allocator("MaBlkBFReuse");
    ObMicroBlockHeader header;
    header = micro_block.header_;
    ObMicroBlockData decompressed_data;
    ObMicroBlockData micro_data(micro_block.data_.get_buf(), micro_block.data_.get_buf_size());
    ObRowStoreType row_store_type = static_cast<ObRowStoreType>(header.row_store_type_);
    ObMicroBlockReaderHelper reader_helper;
    ObIMicroBlockReader *reader = nullptr;
    int64_t row_count = 0;
    if (OB_FAIL(decrypt_and_decompress_micro_data(header,
                                                  micro_data,
                                                  *micro_block.micro_index_info_,
                                                  decompressed_data))) {
      LOG_WARN("fail to decrypt and decompress micro data", K(ret), K(micro_block));
    } else if (FALSE_IT(reader->reset())) {
    } else if (OB_FAIL(reader_helper.init(temp_allocator))) {
      LOG_WARN("fail to init micro reader helper", K(ret));
    } else if (OB_FAIL(reader_helper.get_reader(row_store_type, reader))) {
      LOG_WARN("fail to get reader", K(ret), K(micro_block));
    } else if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null reader", K(ret), K(micro_block), KP(reader));
    } else if (OB_FAIL(reader->init(decompressed_data, nullptr))) {
      LOG_WARN("reader init failed", K(ret), K(micro_block));
    } else if (OB_FAIL(reader->get_row_count(row_count))) {
        LOG_WARN("fail to get row count from micro block reader", K(ret));
    } else {
      ObDatumRow row;
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        row.reuse();
        if (OB_FAIL(reader->get_row(row_idx, row))) {
          LOG_WARN("fail to get next row", K(ret), K(row.is_valid()));
        } else if (OB_FAIL(insert_row(row))) {
          LOG_WARN("fail to insert row into macro block bloom filter", K(ret), K(row), KPC(this));
        }
      }
    }
  }

  return ret;
}

int ObMacroBlockBloomFilter::insert_micro_block(const ObMicroBlockDesc &micro_block_desc,
                                                const ObMicroIndexInfo &micro_index_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!micro_block_desc.is_valid() || !micro_index_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert micro block, invalid argument",
             K(ret), K(micro_block_desc), K(micro_index_info), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to insert micro block, unexpected internal status", K(ret), KPC(this));
  } else if (FALSE_IT(row_count_ += micro_block_desc.row_count_)) { // update row_count_.
  } else if (row_count_ > max_row_count_) {
    // do nothing.
  } else {
    compaction::ObLocalArena temp_allocator("MaBlkBFReuse");
    const ObMicroBlockHeader *header = micro_block_desc.header_;
    ObMicroBlockData decompressed_data;
    ObMicroBlockData micro_data(micro_block_desc.get_block_buf(), micro_block_desc.get_block_size());
    ObRowStoreType row_store_type = static_cast<ObRowStoreType>(header->row_store_type_);
    ObMicroBlockReaderHelper reader_helper;
    ObIMicroBlockReader *reader = nullptr;
    int64_t row_count = 0;
    if (OB_FAIL(decrypt_and_decompress_micro_data(*header,
                                                  micro_data,
                                                  micro_index_info,
                                                  decompressed_data))) {
      LOG_WARN("fail to decrypt and decompress micro data", K(ret), K(micro_block_desc), K(micro_index_info));
    } else if (FALSE_IT(reader->reset())) {
    } else if (OB_FAIL(reader_helper.init(temp_allocator))) {
      LOG_WARN("fail to init micro reader helper", K(ret));
    } else if (OB_FAIL(reader_helper.get_reader(row_store_type, reader))) {
      LOG_WARN("fail to get reader", K(ret), K(micro_block_desc), K(micro_index_info));
    } else if (OB_ISNULL(reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null reader", K(ret), K(micro_block_desc), K(micro_index_info), KP(reader));
    } else if (OB_FAIL(reader->init(decompressed_data, nullptr))) {
      LOG_WARN("reader init failed", K(ret), K(micro_block_desc), K(micro_index_info));
    } else if (OB_FAIL(reader->get_row_count(row_count))) {
        LOG_WARN("fail to get row count from micro block reader", K(ret));
    } else {
      ObDatumRow row;
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        row.reuse();
        if (OB_FAIL(reader->get_row(row_idx, row))) {
          LOG_WARN("fail to get next row", K(ret), K(row.is_valid()));
        } else if (OB_FAIL(insert_row(row))) {
          LOG_WARN("fail to insert row into macro block bloom filter", K(ret), K(row), KPC(this));
        }
      }
    }
  }

  return ret;
}

int ObMacroBlockBloomFilter::decrypt_and_decompress_micro_data(const ObMicroBlockHeader &header,
                                                               const ObMicroBlockData &micro_data,
                                                               const ObMicroIndexInfo &micro_index_info,
                                                               ObMicroBlockData &decompressed_data)
{
  int ret = OB_SUCCESS;

  ObMicroBlockDesMeta micro_des_meta;
  bool is_compressed = false;

  if (OB_UNLIKELY(!header.is_valid() || !micro_data.is_valid() || !micro_index_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to decrypt and decompress micro data, invalid argument",
             K(ret), K(header), K(micro_data), K(micro_index_info));
  } else if (OB_FAIL(
                 micro_index_info.row_header_->fill_micro_des_meta(false /* need_deep_copy_key */, micro_des_meta))) {
    LOG_WARN("fail to fill micro block deserialize meta", K(ret), K(micro_index_info));
  } else if (OB_FAIL(macro_reader_.decrypt_and_decompress_data(micro_des_meta,
                                                               micro_data.get_buf(),
                                                               micro_data.get_buf_size(),
                                                               decompressed_data.get_buf(),
                                                               decompressed_data.get_buf_size(),
                                                               is_compressed))) {
    LOG_WARN("fail to decrypt and decompress data", K(ret), K(micro_des_meta), K(micro_data));
  }

  return ret;
}

void ObMacroBlockBloomFilter::reset()
{
  rowkey_column_count_ = 0;
  datum_utils_ = nullptr;
  enable_macro_block_bloom_filter_ = false;
  max_row_count_ = 0;
  version_ = MACRO_BLOCK_BLOOM_FILTER_V1;
  row_count_ = 0;
  bf_.destroy();
}

int ObMacroBlockBloomFilter::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  const int64_t initial_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!should_persist())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro block bloom filter", K(ret), KPC(this));
  } else if (FALSE_IT(serialize_size = get_serialize_size())) {
  } else if (OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro block bloom filter serialize size overflow",
             K(ret), K(serialize_size), K(buf_len), K(pos), KPC(this));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to serialize version_", K(ret), K(buf_len), K(pos), KPC(this));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, serialize_size))) {
    LOG_WARN("fail to serialize additional serialize_size", K(ret), K(buf_len), K(pos), K(serialize_size), KPC(this));
  } else if (OB_FAIL(bf_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize bloom filter", K(ret), K(buf_len), K(pos), KPC(this));
  } else if (OB_UNLIKELY(pos - initial_pos != serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to serialize macro block bloom filter, unexpected error",
             K(ret), K(pos), K(initial_pos), K(serialize_size), K(buf_len), KPC(this));
  }
  FLOG_INFO("cmdebug, serialize macro block bloom filter", K(ret), KPC(this));
  return ret;
}

int ObMacroBlockBloomFilter::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  const int64_t initial_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &version_))) {
    LOG_WARN("fail to deserialize version_", K(ret), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &serialize_size))) {
    LOG_WARN("fail to deserialize additional serialize_size", K(ret), K(data_len), K(pos), KPC(this));
  } else if (OB_UNLIKELY(serialize_size <= sizeof(version_) + sizeof(serialize_size))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to deserialize macro block bloom filter, unexpected serialize_size",
             K(ret), K(data_len), K(pos), K(serialize_size));
  } else if (pos - initial_pos < serialize_size && OB_FAIL(bf_.deserialize(buf, data_len, pos))) {
    LOG_WARN("fail to deserialize bloom filter", K(ret), K(data_len), K(pos), K(initial_pos), K(serialize_size));
  } else if (OB_UNLIKELY(pos - initial_pos != serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to deserialize macro block bloom filter, unexpected error",
             K(ret), K(pos), K(initial_pos), K(serialize_size), K(data_len), KPC(this));
  }
  return ret;
}

int64_t ObMacroBlockBloomFilter::get_serialize_size() const
{
  int64_t serialize_size = 0;
  if (should_persist()) {
    // We additionally encode an int64 in the persistent macro block bloom filter for deserialization verification.
    serialize_size = serialization::encoded_length_i32(version_) + sizeof(int64_t) + bf_.get_serialize_size();
  }
  return serialize_size;
}

} // namespace blocksstable
} // namespace oceanbase
