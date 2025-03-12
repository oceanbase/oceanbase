// Copyright (c) 2022 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "ob_macro_block.h"
#include "ob_micro_block_hash_index.h"
#include "ob_data_store_desc.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace blocksstable
{

 /**
 * -------------------------------------------------------------------ObMicroBlockHashIndexBuilder----------------------------------------------------------
 */
int ObMicroBlockHashIndexBuilder::check_need_build_hash_index(const ObDataStoreDesc &data_store_desc, bool &need_build)
{
  int ret = OB_SUCCESS;
  need_build = false;
  const common::ObIArray<share::schema::ObColDesc> &rowkey_col_descs = data_store_desc.get_rowkey_col_descs();
  const int64_t schema_rowkey_col_cnt = data_store_desc.get_schema_rowkey_col_cnt();
  int64_t int_column_count = 0;
  if (OB_UNLIKELY(schema_rowkey_col_cnt > rowkey_col_descs.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected schema rowkey col cnt", K(ret), K(schema_rowkey_col_cnt), K(rowkey_col_descs));
  } else {
    for (int64_t i = 0; i < schema_rowkey_col_cnt; ++i) {
      if (rowkey_col_descs.at(i).col_type_.is_integer_type()) {
        int_column_count++;
      }
    }
    if (int_column_count != schema_rowkey_col_cnt || int_column_count >= ObMicroBlockHashIndex::MIN_INT_COLUMNS_NEEDED) {
      need_build = true;
    }
  }
  return ret;
}


int ObMicroBlockHashIndexBuilder::init_if_needed(const ObDataStoreDesc *data_store_desc)
{
  int ret = OB_SUCCESS;
  bool need_build = false;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "Micro_hash_index_builder is inited twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == data_store_desc || !data_store_desc->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init micro hash index block builder", K(ret), KPC(data_store_desc));
  } else if (OB_FAIL(check_need_build_hash_index(*data_store_desc, need_build))) {
    STORAGE_LOG(WARN, "Failed to check if need build has index", K(ret));
  } else if (need_build) {
    row_index_ = 0;
    count_ = 0;
    last_key_with_L_flag_ = false;
    data_store_desc_ = data_store_desc;
    is_inited_ = true;
  }
  return ret;
}

int ObMicroBlockHashIndexBuilder::add(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = data_store_desc_->get_datum_utils();
  const int64_t schema_rowkey_col_cnt = data_store_desc_->get_schema_rowkey_col_cnt();
  if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() < schema_rowkey_col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid input argument", K(ret),
                    K(row), K(schema_rowkey_col_cnt));
  } else if (can_be_added_to_hash_index(row)) {
    // Caculate hash value by schema_rowkey_col_cnt.
    uint64_t hash_value = 0;
    ObDatumRowkey tmp_rowkey;
    if (OB_FAIL(tmp_rowkey.assign(row.storage_datums_, schema_rowkey_col_cnt))) {
      STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), K(row), K(schema_rowkey_col_cnt));
    } else if (OB_FAIL(tmp_rowkey.murmurhash(0, datum_utils, hash_value))) {
      STORAGE_LOG(WARN, "Failed to calc rowkey hash", K(ret), K(tmp_rowkey), K(datum_utils));
    } else if (OB_FAIL(internal_add(hash_value, row_index_))) {
      if (ret != OB_NOT_SUPPORTED) {
        STORAGE_LOG(WARN, "Failed to add row index to hash_index", K(ret), K(row_index_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ++row_index_;
    last_key_with_L_flag_ = row.mvcc_row_flag_.is_last_multi_version_row();
  }
  return ret;
}

int ObMicroBlockHashIndexBuilder::build_block(ObMicroBufferWriter &buffer)
{
  int ret = OB_SUCCESS;
  // ObMicroBlockHashIndexBuilder must be valid when call build_block.
  if (OB_UNLIKELY(count_ <= ObMicroBlockHashIndex::MIN_ROWS_BUILD_HASH_INDEX)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    uint16_t num_buckets = caculate_bucket_number(count_);
    if (OB_UNLIKELY(num_buckets > ObMicroBlockHashIndex::MAX_BUCKET_NUMBER)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Too much buckets ", K(ret), K(num_buckets));
    } else {
      const uint8_t no_entry = ObMicroBlockHashIndex::NO_ENTRY;
      MEMSET(buckets_, no_entry, num_buckets);
      uint32_t collision_count = 0;
      // Write the row_index array
      for (int i = 0; i < count_; ++i) {
        const uint64_t hash_value = hash_values_[i];
        const uint8_t row_index = row_indexes_[i];
        const uint16_t buck_idx = static_cast<uint16_t>(hash_value % num_buckets);
        if (buckets_[buck_idx] == ObMicroBlockHashIndex::NO_ENTRY) {
          buckets_[buck_idx] = row_index;
        } else if (buckets_[buck_idx] != ObMicroBlockHashIndex::COLLISION) {
          // Same bucket cannot store two different offset, mark collision.
          buckets_[buck_idx] = ObMicroBlockHashIndex::COLLISION;
          collision_count += 2;
        } else {
          ++collision_count;
        }
      }

      if ((collision_count * ObMicroBlockHashIndex::MAX_COLLISION_RATIO) <= count_) {
        const uint8_t reserved_byte = ObMicroBlockHashIndex::RESERVED_BYTE;
        if (OB_FAIL(buffer.write(reserved_byte))) {
          STORAGE_LOG(WARN, "Data buffer fail to write reserved byte", K(ret), K(num_buckets), K(count_), K(reserved_byte));
        } else if (OB_FAIL(buffer.write(num_buckets))) {
          STORAGE_LOG(WARN, "Data buffer fail to write hash index buckets number", K(ret), K(num_buckets), K(count_));
        } else if (OB_FAIL(buffer.write(reinterpret_cast<const void *>(buckets_), num_buckets))) {
          STORAGE_LOG(WARN, "Data buffer fail to write hash index buckets", K(ret), K(num_buckets), K(count_));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    }
  }
  STORAGE_LOG(DEBUG, "Build hash index block", K(count_), K(ret));
  return ret;
}

OB_INLINE bool ObMicroBlockHashIndexBuilder::can_be_added_to_hash_index(const ObDatumRow &row)
{
  return (data_store_desc_->is_major_merge_type()
              || last_key_with_L_flag_
              || is_empty())
             && !row.is_ghost_row();
}

int ObMicroBlockHashIndexBuilder::internal_add(const uint64_t hash_value, const uint32_t row_index)
{
  int ret = OB_SUCCESS;
  if (row_index >= ObMicroBlockHashIndex::MAX_OFFSET_SUPPORTED) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_UNLIKELY(!is_empty() && row_index <= row_indexes_[count_ - 1])) {
    ret = OB_ERR_UNEXPECTED;
    const uint32_t front_row_index = row_indexes_[count_ - 1];
    STORAGE_LOG(WARN, "Unexpected row_index ", K(ret), K(row_index), K(front_row_index), K(count_));
  } else {
    hash_values_[count_] = static_cast<uint32_t>(hash_value);
    row_indexes_[count_] = static_cast<uint8_t>(row_index);
    count_++;
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMicroBlockHashIndex-----------------------------------------------------------------
 */
int ObMicroBlockHashIndex::init(const ObMicroBlockData &micro_block_data)
{
  int ret = OB_SUCCESS;
  // ObMicroBlockHashIndex can be inited repeatedly.
  const ObMicroBlockHeader *micro_block_header = micro_block_data.get_micro_header();
  if (OB_UNLIKELY(nullptr == micro_block_header)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "Invalid micro block header", K(ret), K(micro_block_data));
  } else {
    const uint32_t hash_index_offset_from_end = micro_block_header->hash_index_offset_from_end_;
    const char* start_data = micro_block_data.get_buf() + micro_block_data.get_buf_size()
                                 - hash_index_offset_from_end;
    const uint8 reserved_byte = reinterpret_cast<const uint8_t *>(start_data)[0];
    bucket_table_ = reinterpret_cast<const uint8_t *>(start_data + get_fixed_header_size());
    num_buckets_ = reinterpret_cast<const uint16_t *>(start_data + 1)[0];
    STORAGE_LOG(DEBUG, "ObMicroBlockHashIndex init", K(num_buckets_), K(reserved_byte));
    bool is_valid = num_buckets_ != 0 && num_buckets_ <= MAX_BUCKET_NUMBER
                        && reserved_byte == RESERVED_BYTE
                        && get_serialize_size(num_buckets_) == hash_index_offset_from_end;
    if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected hash index in data micro block", K_(num_buckets),
                   K(reserved_byte), K(hash_index_offset_from_end), KPC(micro_block_header));
    } else {
      is_inited_ = true;
    }
  }
  return OB_SUCCESS;
}

}//end namespace blocksstable
}//end namespace oceanbase
