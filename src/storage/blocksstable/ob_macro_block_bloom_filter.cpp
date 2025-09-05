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

ObMicroBlockBloomFilter::ObMicroBlockBloomFilter()
    : rowkey_column_count_(0),
      empty_read_prefix_(0),
      datum_utils_(nullptr),
      hash_set_(),
      row_count_(0),
      macro_reader_(MTL_ID()),
      is_inited_(false)
{
}

ObMicroBlockBloomFilter::~ObMicroBlockBloomFilter()
{
  reset();
}

void ObMicroBlockBloomFilter::reuse()
{
  hash_set_.reuse();
  row_count_ = 0;
}

void ObMicroBlockBloomFilter::reset()
{
  rowkey_column_count_ = 0;
  empty_read_prefix_ = 0;
  datum_utils_ = nullptr;
  hash_set_.destroy();
  row_count_ = 0;
  is_inited_ = false;
}

int ObMicroBlockBloomFilter::init(const ObDataStoreDesc &data_store_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init micro block bloom filter, init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!data_store_desc.is_valid() ||
                         !data_store_desc.enable_macro_block_bloom_filter() ||
                         !data_store_desc.get_datum_utils().is_valid() ||
                         data_store_desc.get_row_column_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init micro block bloom filter, invalid data store desc", K(ret), K(data_store_desc));
  } else if (OB_FAIL(hash_set_.create(1024, "MicroBFHashset", "MicroBFHashset", MTL_ID()))) {
    LOG_WARN("fail to create hash set", K(ret));
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
    if (OB_SUCC(ret)) {
      rowkey_column_count_ = datum_utils_->get_rowkey_count();
      // As same as load bf.
      empty_read_prefix_
          = rowkey_column_count_ - storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() /* mvcc col */;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMicroBlockBloomFilter::insert_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  uint64_t key_hash = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to insert row", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert row, invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(row.get_column_count() < empty_read_prefix_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert row, invalid column count", K(ret), K(row), KPC(this));
  } else if (OB_FAIL(rowkey.assign(row.storage_datums_, empty_read_prefix_))) {
    LOG_WARN("fail to fetch rowkey from datum row",
             K(ret), K(row), K(rowkey_column_count_), K(empty_read_prefix_));
  } else if (OB_FAIL(rowkey.murmurhash(0, *datum_utils_, key_hash))) {
    LOG_WARN("fail to murmurhash rowkey", K(ret), K(rowkey), K(row));
  } else if (OB_FAIL(hash_set_.set_refactored(static_cast<uint32_t>(key_hash), 1 /* cover */))) {
    LOG_WARN("fail to insert into hash set", K(ret), K(row), K(key_hash), KPC(this));
  } else {
    row_count_++;
  }

  return ret;
}

int ObMicroBlockBloomFilter::insert_micro_block(const ObMicroBlock &micro_block)
{
  int ret = OB_SUCCESS;

  ObMicroBlockData decompressed_data;
  ObMicroBlockData micro_block_data(micro_block.data_.get_buf(), micro_block.data_.get_buf_size());

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to insert row", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!micro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert micro block, invalid argument", K(ret), K(micro_block), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to insert micro block, unexpected internal status", K(ret), KPC(this));
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
    ObDatumRow row;
    if (OB_FAIL(decrypt_and_decompress_micro_data(header,
                                                  micro_data,
                                                  *micro_block.micro_index_info_,
                                                  decompressed_data))) {
      LOG_WARN("fail to decrypt and decompress micro data", K(ret), K(micro_block));
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
    } else if (OB_FAIL(row.init(reader->get_column_count()))) {
      LOG_WARN("fail to init datum row", K(ret), K(reader->get_column_count()), K(row_count));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        row.reuse();
        if (OB_FAIL(reader->get_row(row_idx, row))) {
          LOG_WARN("fail to get next row",
                   K(ret), K(row.is_valid()), K(reader->get_column_count()), K(row_idx), K(row_count));
        } else if (OB_FAIL(insert_row(row))) {
          LOG_WARN("fail to insert row into micro block bloom filter",
                   K(ret), K(row), K(reader->get_column_count()), K(row_idx), K(row_count), KPC(this));
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockBloomFilter::insert_micro_block(const ObMicroBlockDesc &micro_block_desc,
                                                const ObMicroIndexInfo &micro_index_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to insert row", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!micro_block_desc.is_valid() || !micro_index_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to insert micro block, invalid argument",
             K(ret), K(micro_block_desc), K(micro_index_info), KPC(this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to insert micro block, unexpected internal status", K(ret), KPC(this));
  } else {
    compaction::ObLocalArena temp_allocator("MaBlkBFReuse");
    const ObMicroBlockHeader *header = micro_block_desc.header_;
    ObMicroBlockData decompressed_data;
    ObMicroBlockData micro_data(micro_block_desc.get_block_buf(), micro_block_desc.get_block_size());
    ObRowStoreType row_store_type = static_cast<ObRowStoreType>(header->row_store_type_);
    ObMicroBlockReaderHelper reader_helper;
    ObIMicroBlockReader *reader = nullptr;
    int64_t row_count = 0;
    ObDatumRow row;
    if (OB_FAIL(decrypt_and_decompress_micro_data(*header,
                                                  micro_data,
                                                  micro_index_info,
                                                  decompressed_data))) {
      LOG_WARN("fail to decrypt and decompress micro data", K(ret), K(micro_block_desc), K(micro_index_info));
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
     } else if (OB_FAIL(row.init(reader->get_column_count()))) {
      LOG_WARN("fail to init datum row", K(ret), K(reader->get_column_count()), K(row_count));
    } else {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; ++row_idx) {
        row.reuse();
        if (OB_FAIL(reader->get_row(row_idx, row))) {
          LOG_WARN("fail to get next row",
                   K(ret), K(row.is_valid()), K(reader->get_column_count()), K(row_idx), K(row_count));
        } else if (OB_FAIL(insert_row(row))) {
          LOG_WARN("fail to insert row into micro block bloom filter",
                   K(ret), K(row), K(reader->get_column_count()), K(row_idx), K(row_count), KPC(this));
        }
      }
    }
  }

  return ret;
}

int ObMicroBlockBloomFilter::decrypt_and_decompress_micro_data(const ObMicroBlockHeader &header,
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

template <typename F>
int ObMicroBlockBloomFilter::foreach(F &functor) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("fail to foreach hash set, not inited", K(ret), KPC(this));
  } else if (OB_FAIL(hash_set_.foreach_refactored(functor))) {
    LOG_WARN("fail to foreach hash set", K(ret), KPC(this));
  }
  return ret;
}

int ObMacroBlockBloomFilter::MergeMicroBlockFunctor::operator()(common::hash::HashSetTypes<uint32_t>::pair_type &pair)
{
  int ret = OB_SUCCESS;
  uint32_t hash_val = pair.first;
  if (OB_FAIL(bf_.insert(hash_val))) {
    LOG_WARN("fail to insert hash val", K(ret), K(hash_val), K(bf_));
  }
  return ret;
}

int64_t ObMacroBlockBloomFilter::predict_next(const int64_t curr_macro_block_row_count)
{
  int ret = OB_SUCCESS;
  int64_t row_count = curr_macro_block_row_count;
  if (OB_UNLIKELY(curr_macro_block_row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("fail to predict next, invalid argument", K(ret), K(curr_macro_block_row_count));
    row_count = 0;
  }
  return (row_count == 0) ? 0 : (row_count * 1.3 + 1);
}

ObMacroBlockBloomFilter::ObMacroBlockBloomFilter()
    : rowkey_column_count_(0),
      empty_read_prefix_(0),
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

int ObMacroBlockBloomFilter::alloc_bf(const ObDataStoreDesc &data_store_desc, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0 ||
                  !data_store_desc.is_valid() ||
                  !data_store_desc.enable_macro_block_bloom_filter() ||
                  !data_store_desc.get_datum_utils().is_valid() ||
                  data_store_desc.get_row_column_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to allocate new bloom filter, invalid data store desc", K(ret), K(data_store_desc), K(row_count));
  } else if (OB_UNLIKELY(bf_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to allocate new bloom filter", K(ret), K(bf_));
  } else {
    const int64_t MAX_ROW_COUNT = calc_max_row_count(MACRO_BLOCK_BLOOM_FILTER_MAX_SIZE);
    max_row_count_ = (row_count == 0 || row_count > MAX_ROW_COUNT) ? MAX_ROW_COUNT : row_count ;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bf_.init_by_row_count(max_row_count_, ObBloomFilter::BLOOM_FILTER_FALSE_POSITIVE_PROB))) {
    LOG_WARN("fail to init new bloom filter", K(ret), K(max_row_count_));
  } else {
    if (data_store_desc.is_cg()) { // Fetch datum utils for rowkey murmurhash.
      const ObITableReadInfo *index_read_info;
      if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
        LOG_WARN("fail to get index read info for cg sstable", K(ret), K(data_store_desc));
      } else if (OB_UNLIKELY(!index_read_info->get_datum_utils().is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid datum utails for cg sstable", K(ret), KPC(index_read_info));
      } else {
        rowkey_column_count_ = index_read_info->get_datum_utils().get_rowkey_count();
      }
    } else {
      rowkey_column_count_ = data_store_desc.get_datum_utils().get_rowkey_count();
    }
    // As same as load bf.
    empty_read_prefix_
        = rowkey_column_count_ - storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() /* mvcc col */;
  }
  return ret;
}

bool ObMacroBlockBloomFilter::is_valid() const
{
  return version_ == MACRO_BLOCK_BLOOM_FILTER_V1
         && rowkey_column_count_ > 0
         && empty_read_prefix_ > 0
         && max_row_count_ > 0
         && bf_.is_valid();
}

bool ObMacroBlockBloomFilter::should_persist() const
{
  return is_valid() && row_count_ > 0 && row_count_ <= max_row_count_;
}

int ObMacroBlockBloomFilter::merge(const ObMicroBlockBloomFilter &micro_bf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_column_count_ != micro_bf.get_rowkey_column_count()
                  || empty_read_prefix_ != micro_bf.get_empty_read_prefix())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to merge, invalid argument", K(ret), K(micro_bf), KPC(this));
  } else if (FALSE_IT(row_count_ += micro_bf.get_row_count())) {
  } else if (row_count_ > max_row_count_) {
    // do nothing.
  } else {
    MergeMicroBlockFunctor functor(bf_);
    if (OB_FAIL(micro_bf.foreach(functor))) {
      LOG_WARN("fail to merge micro bf", K(ret), K(micro_bf), KPC(this));
    }
  }
  return ret;
}

void ObMacroBlockBloomFilter::reuse()
{
  row_count_ = 0;
  bf_.clear();
}

void ObMacroBlockBloomFilter::reset()
{
  rowkey_column_count_ = 0;
  empty_read_prefix_ = 0;
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
