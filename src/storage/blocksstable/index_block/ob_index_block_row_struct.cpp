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
#include "ob_index_block_row_struct.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{

ObIndexBlockRowDesc::ObIndexBlockRowDesc()
  : data_store_desc_(nullptr), aggregated_row_(nullptr), row_key_(), macro_id_(),
    logic_micro_id_(), shared_data_macro_id_(), data_checksum_(0), block_offset_(0),
    row_count_(0), row_count_delta_(0), max_merged_trans_version_(0), block_size_(0),
    macro_block_count_(0), micro_block_count_(0), row_offset_(0),
    is_deleted_(false), contain_uncommitted_row_(false), is_data_block_(false),
    is_secondary_meta_(false), is_macro_node_(false), has_string_out_row_(false), has_lob_out_row_(false),
    is_last_row_last_flag_(false), is_serialized_agg_row_(false), is_clustered_index_(false), has_macro_block_bloom_filter_(false)
{
}

ObIndexBlockRowDesc::ObIndexBlockRowDesc(const ObDataStoreDesc &data_store_desc)
  : data_store_desc_(&data_store_desc), aggregated_row_(nullptr), row_key_(), macro_id_(),
    logic_micro_id_(), shared_data_macro_id_(), data_checksum_(0), block_offset_(0), row_count_(0), row_count_delta_(0), max_merged_trans_version_(0),
    block_size_(0), macro_block_count_(0), micro_block_count_(0), row_offset_(0),
    is_deleted_(false), contain_uncommitted_row_(false), is_data_block_(false),
    is_secondary_meta_(false), is_macro_node_(false), has_string_out_row_(false), has_lob_out_row_(false),
    is_last_row_last_flag_(false), is_serialized_agg_row_(false), is_clustered_index_(false), has_macro_block_bloom_filter_(false)
{
}

int ObIndexBlockRowDesc::init(const ObDataStoreDesc &data_store_desc,
                              ObIndexBlockRowParser &idx_row_parser,
                              ObDatumRow &index_row)
{
  int ret = OB_SUCCESS;
  const ObIndexBlockRowHeader *index_row_header = nullptr;
  const ObIndexBlockRowMinorMetaInfo *index_row_meta = nullptr;
  const int64_t rowkey_column_count = data_store_desc.get_rowkey_column_count();
  data_store_desc_ = &data_store_desc;
  if (OB_FAIL(row_key_.assign(index_row.storage_datums_, rowkey_column_count))) {
    STORAGE_LOG(WARN, "fail to assign src rowkey", K(ret), K(rowkey_column_count), K(index_row));
  } else if (OB_FAIL(idx_row_parser.get_header(index_row_header))){
    STORAGE_LOG(WARN, "fail to get index row header", K(ret), K(idx_row_parser));
  } else {
    ObDataStoreDesc *non_const_data_store_desc = const_cast<ObDataStoreDesc *>(data_store_desc_);
    ObStaticDataStoreDesc *static_desc = non_const_data_store_desc->static_desc_;
    non_const_data_store_desc->row_store_type_ = index_row_header->get_row_store_type();
    static_desc->compressor_type_ = index_row_header->get_compressor_type();
    static_desc->master_key_id_ = index_row_header->get_master_key_id();
    static_desc->encrypt_id_ = index_row_header->get_encrypt_id();
    MEMCPY(static_desc->encrypt_key_, index_row_header->get_encrypt_key(), sizeof(static_desc->encrypt_key_));
    static_desc->schema_version_ = index_row_header->get_schema_version();

    is_secondary_meta_ = false;
    is_macro_node_ = true;
    macro_id_ = index_row_header->get_macro_id();
    block_offset_ = index_row_header->block_offset_;
    block_size_ = index_row_header->block_size_;
    row_count_ = index_row_header->row_count_;
    is_deleted_ = index_row_header->is_deleted_;
    contain_uncommitted_row_ = index_row_header->contain_uncommitted_row_;
    micro_block_count_ = index_row_header->micro_block_count_;
    macro_block_count_ = 1;
    has_string_out_row_ = index_row_header->has_string_out_row_;
    has_lob_out_row_ = !index_row_header->all_lob_in_row_;
    row_offset_ = idx_row_parser.get_row_offset();
    is_serialized_agg_row_ = false;

    const char *agg_row_buf = nullptr;
    int64_t agg_buf_size = 0;
    if (!data_store_desc.is_major_or_meta_merge_type()) {
      if (OB_FAIL(idx_row_parser.get_minor_meta(index_row_meta))) {
        STORAGE_LOG(WARN, "fail to get minor meta", K(ret), K(idx_row_parser));
      } else {
        max_merged_trans_version_ = index_row_meta->max_merged_trans_version_;
        row_count_delta_ = index_row_meta->row_count_delta_;
      }
    } else if (!index_row_header->is_major_node() || !index_row_header->is_pre_aggregated()) {
      // Do not have aggregate data
    } else if (OB_FAIL(idx_row_parser.get_agg_row(agg_row_buf, agg_buf_size))) {
      STORAGE_LOG(WARN, "Fail to get aggregate", K(ret));
    } else {
      serialized_agg_row_buf_ = agg_row_buf;
      is_serialized_agg_row_ = true;
    }
  }
  return ret;
}

MacroBlockId ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID(0, DEFAULT_IDX_ROW_MACRO_IDX, 0);
MacroBlockId ObIndexBlockRowHeader::INVALID_MACRO_BLOCK_ID;
ObLogicMicroBlockId ObIndexBlockRowHeader::INVALID_LOGICAL_MICRO_BLOCK_ID;

ObIndexBlockRowHeader::ObIndexBlockRowHeader()
  : pack_(0), macro_id_first_id_(0), macro_id_second_id_(0), macro_id_third_id_(0),
    block_offset_(0), block_size_(0),
    master_key_id_(0), encrypt_id_(0), encrypt_key_(), row_count_(), schema_version_(0),
    macro_block_count_(0), micro_block_count_(0),
    macro_id_fourth_id_(0), logic_micro_id_(), data_checksum_(0)
{
  version_ = INDEX_BLOCK_HEADER_V2;
  set_has_logic_micro_id();
}

void ObIndexBlockRowHeader::reset()
{
  MEMSET(this, 0, sizeof(*this));
  version_ = INDEX_BLOCK_HEADER_V2;
  set_has_logic_micro_id();
}

int ObIndexBlockRowHeader::set_macro_id(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(version_ != INDEX_BLOCK_HEADER_V2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected index row header version", K(ret), KPC(this), K(macro_id));
  } else {
    macro_id_first_id_ = macro_id.first_id();
    macro_id_second_id_ = macro_id.second_id();
    macro_id_third_id_ = macro_id.third_id();
    macro_id_fourth_id_ = macro_id.fourth_id();
  }
  return ret;
}

int ObIndexBlockRowHeader::fill_micro_des_meta(
    const bool need_deep_copy_key,
    ObMicroBlockDesMeta &des_meta) const
{
  int ret = OB_SUCCESS;
  des_meta.compressor_type_ = get_compressor_type();
  des_meta.encrypt_id_ = get_encrypt_id();
  des_meta.master_key_id_ = get_master_key_id();
  if (need_deep_copy_key) {
    if (OB_ISNULL(des_meta.encrypt_key_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid des meta, null pointer to encrypt key", K(ret));
    } else {
      MEMCPY(const_cast<char *>(des_meta.encrypt_key_),
          encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    }
  } else {
    des_meta.encrypt_key_ = encrypt_key_;
  }
  return ret;
}

ObIndexBlockRowBuilder::ObIndexBlockRowBuilder()
  : allocator_(nullptr),
    index_data_allocator_(ObModIds::OB_BLOCK_INDEX_INTERMEDIATE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    data_desc_(nullptr),
    row_(),
    rowkey_column_count_(0),
    data_buf_(nullptr),
    write_pos_(0),
    header_(nullptr),
    is_inited_(false) {}

ObIndexBlockRowBuilder::~ObIndexBlockRowBuilder()
{
  reset();
}

void ObIndexBlockRowBuilder::reuse()
{
  index_data_allocator_.reuse();
  row_.reuse();
  data_buf_ = nullptr;
  write_pos_ = 0;
  header_ = nullptr;
}

void ObIndexBlockRowBuilder::reset()
{
  row_.reset();
  data_desc_ = nullptr;
  rowkey_column_count_ = 0;
  data_buf_ = nullptr;
  write_pos_ = 0;
  header_ = nullptr;
  index_data_allocator_.reset();
  is_inited_ = false;
}

int ObIndexBlockRowBuilder::init(ObIAllocator &allocator,
                                 const ObDataStoreDesc &data_desc,
                                 const ObDataStoreDesc &index_desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Double init", K(ret));
  } else if (OB_UNLIKELY(!data_desc.is_valid() || !index_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid data store description", K(ret), K(data_desc), K(index_desc));
  } else if (OB_FAIL(row_.init(allocator, index_desc.get_rowkey_column_count() + 1))) {
    STORAGE_LOG(WARN, "Failed to init row", K(ret), K(index_desc.get_rowkey_column_count()));
  } else {
    allocator_ = &allocator;
    data_desc_ = &data_desc;
    rowkey_column_count_ = index_desc.get_rowkey_column_count();
    is_inited_ = true;
    STORAGE_LOG(TRACE, "success to init ObIndexBlockRowBuilder", K(rowkey_column_count_), K(data_desc), K(index_desc));
  }
  return ret;
}

int ObIndexBlockRowBuilder::build_row(const ObIndexBlockRowDesc &desc, const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  reuse();
  ObAggRowWriter agg_writer;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Index block description is not valid", K(ret));
  } else if (OB_UNLIKELY(desc.row_offset_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected row offset", K(ret), K(desc));
  } else if (OB_FAIL(row_.reserve(rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "Failed to reserve index row", K(ret), K(rowkey_column_count_));
  } else if (OB_FAIL(set_rowkey(desc))) {
    LOG_WARN("Fail to set rowkey", K(ret));
  } else if (nullptr != desc.aggregated_row_ && !desc.is_serialized_agg_row_
      && OB_FAIL(agg_writer.init(data_desc_->get_agg_meta_array(), *desc.aggregated_row_, index_data_allocator_))) {
    LOG_WARN("Fail to init aggregate row writer", K(ret), K(desc), KPC(row));
  } else if (OB_FAIL(calc_data_size(desc, agg_writer, data_size))) {
    LOG_WARN("Fail to calculate row data size", K(ret));
  } else if (OB_ISNULL(data_buf_ = reinterpret_cast<char *>(index_data_allocator_.alloc(data_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for data buffer", K(ret), K(data_size));
  } else if (FALSE_IT(MEMSET(data_buf_, 0, data_size))) {
  } else if (OB_FAIL(append_header_and_meta(desc, data_size))) {
    LOG_WARN("Fail to append header and meta to buffer", K(ret), K(desc), K_(write_pos));
  } else if (OB_FAIL(append_aggregate_data(desc, data_size, agg_writer))) {
    LOG_WARN("Fail to append aggregated data to buffer", K(ret), K(desc), K_(write_pos));
  } else {
    ObString str(write_pos_, data_buf_);
    row_.storage_datums_[rowkey_column_count_].set_string(str);
    row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row = &row_;
    LOG_DEBUG("build index row", K_(desc.row_key), KPC_(header));
  }
  return ret;
}

int ObIndexBlockRowBuilder::set_rowkey(const ObIndexBlockRowDesc &desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!desc.row_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey, ", K(ret), K(desc.row_key_));
  } else if (OB_FAIL(set_rowkey(desc.row_key_))) {
    LOG_WARN("Fail to set rowkey", K(ret), K(desc.row_key_));
  }
  return ret;
}
int ObIndexBlockRowBuilder::set_rowkey(const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey dest_rowkey;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!rowkey.is_valid() || rowkey_column_count_ != rowkey.get_datum_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Rowkey column count mismatch", K(ret), K_(rowkey_column_count), K(rowkey));
  } else if (OB_FAIL(dest_rowkey.assign(row_.storage_datums_, rowkey_column_count_))) {
    STORAGE_LOG(WARN, "Failed to assign dest rowkey", K(ret), K(rowkey_column_count_));
  } else if (OB_FAIL(rowkey.semi_copy(dest_rowkey, index_data_allocator_))) {
    STORAGE_LOG(WARN, "Failed to semi copy dest rowkey", K(ret), K(rowkey));
  }

  return ret;
}

int ObIndexBlockRowBuilder::calc_data_size(
    const ObIndexBlockRowDesc &desc,
    ObAggRowWriter &agg_writer,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index block row description", K(ret), K(desc));
  } else if (desc.is_secondary_meta_) {
    size = sizeof(ObIndexBlockRowHeader);
    if (desc.get_data_store_desc()->is_major_or_meta_merge_type()) {
      size += sizeof(int64_t); // add row offset for major sstable
    }
  } else if (desc.get_data_store_desc()->is_major_or_meta_merge_type()) {
    size = sizeof(ObIndexBlockRowHeader);
    size += sizeof(int64_t); // add row offset for major sstable
    if (nullptr != desc.aggregated_row_) {
      if (desc.is_serialized_agg_row_) {
        const ObAggRowHeader *agg_header = reinterpret_cast<const ObAggRowHeader *>(desc.serialized_agg_row_buf_);
        if (OB_UNLIKELY(!agg_header->is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid aggregate row header", K(ret), K(desc), KPC(agg_header));
        } else {
          size += agg_header->length_;
        }
      } else {
        size += agg_writer.get_serialize_data_size();
      }
    }
  } else {
    size = sizeof(ObIndexBlockRowHeader) + sizeof(ObIndexBlockRowMinorMetaInfo);
  }
  return ret;
}

int ObIndexBlockRowBuilder::append_header_and_meta(const ObIndexBlockRowDesc &desc, const int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != write_pos_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write pos for buffer should be zero when write header", K(ret), K_(write_pos));
  } else {
    header_ = reinterpret_cast<ObIndexBlockRowHeader *>(data_buf_);
    header_->version_ = ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V2;
    header_->row_store_type_ = static_cast<uint8_t>(desc.get_data_store_desc()->get_row_store_type());
    header_->compressor_type_ = static_cast<uint8_t>(desc.get_data_store_desc()->get_compressor_type());
    // This micro block is a index tree micro block or a meta tree micro block
    header_->is_data_index_ = !desc.is_secondary_meta_;
    header_->is_data_block_ = desc.is_data_block_;
    header_->is_leaf_block_ = desc.is_macro_node_;
    header_->is_macro_node_ = desc.is_macro_node_;
    header_->has_macro_block_bloom_filter_ = desc.has_macro_block_bloom_filter_;
    header_->is_major_node_ = desc.get_data_store_desc()->is_major_or_meta_merge_type();
    header_->has_string_out_row_ = desc.has_string_out_row_;
    header_->all_lob_in_row_ = !desc.has_lob_out_row_;
    header_->is_pre_aggregated_ = nullptr != desc.aggregated_row_;
    header_->is_deleted_ = desc.is_deleted_;
    if (desc.shared_data_macro_id_.is_valid() && !desc.is_data_block_) {
      header_->set_shared_data_macro_id(desc.shared_data_macro_id_);
    } else {
      header_->set_logic_micro_id_and_checksum(desc.logic_micro_id_, desc.data_checksum_);
    }
    header_->block_offset_ = desc.block_offset_;
    header_->block_size_ = desc.block_size_;
    header_->macro_block_count_ = desc.macro_block_count_;
    header_->micro_block_count_ = desc.micro_block_count_;
    header_->master_key_id_ = desc.get_data_store_desc()->get_master_key_id();
    header_->encrypt_id_ = desc.get_data_store_desc()->get_encrypt_id();
    MEMCPY(header_->encrypt_key_, desc.get_data_store_desc()->get_encrypt_key(), sizeof(header_->encrypt_key_));
    header_->schema_version_ = desc.get_data_store_desc()->get_schema_version();
    header_->row_count_ = desc.row_count_;
    write_pos_ += sizeof(ObIndexBlockRowHeader);

    // Set macro id to special value (DEFAULT_IDX_ROW_MACRO_ID) for index micro
    // block at end of data macro block; Set macro id to actual value for others
    // (meta tree, internal level of index tree, clustered index of index tree).
    if (desc.is_data_block_ && !desc.is_secondary_meta_ && !desc.is_clustered_index_) {
      if (OB_FAIL(header_->set_macro_id(ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID))) {
        LOG_WARN("fail to set macro id to DEFAULT", K(ret), K(desc), K(buf_size));
      }
    } else {
      if (OB_FAIL(header_->set_macro_id(desc.macro_id_))) {
        LOG_WARN("fail to set macro id", K(ret), K(desc), K(buf_size));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (header_->is_data_index() && !header_->is_major_node()) {
      ObIndexBlockRowMinorMetaInfo *minor_meta
          = reinterpret_cast<ObIndexBlockRowMinorMetaInfo *>(data_buf_ + write_pos_);
      minor_meta->snapshot_version_ = desc.get_data_store_desc()->get_end_scn().get_val_for_tx();
      header_->contain_uncommitted_row_ = desc.contain_uncommitted_row_;
      minor_meta->max_merged_trans_version_ = desc.max_merged_trans_version_;
      minor_meta->row_count_delta_ = desc.row_count_delta_;
      write_pos_ += sizeof(ObIndexBlockRowMinorMetaInfo);
    } else if (header_->is_major_node()) {
      // we add row_offset for index rows of all major sstables(including secondary meta tree)
      if (OB_FAIL(serialization::encode_i64(data_buf_, buf_size, write_pos_, desc.row_offset_))) {
        LOG_WARN("fail to encode row offset", K(ret), K(buf_size), K_(write_pos));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Built an invalid index block row", K(ret), KPC(header_));
    }
  }
  return ret;
}

int ObIndexBlockRowBuilder::append_aggregate_data(
    const ObIndexBlockRowDesc &desc,
    const int64_t &buf_size,
    ObAggRowWriter &agg_writer)
{
  int ret = OB_SUCCESS;
  UNUSED(desc);
  if (OB_ISNULL(header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to append aggregation data to buffer", K(ret), KP_(header));
  } else if (!header_->is_pre_aggregated()) {
  } else if (desc.is_serialized_agg_row_) {
    const ObAggRowHeader *agg_header = reinterpret_cast<const ObAggRowHeader *>(desc.serialized_agg_row_buf_);
    MEMCPY(data_buf_ + write_pos_, desc.serialized_agg_row_buf_, agg_header->length_);
    write_pos_ += agg_header->length_;
  } else if (OB_FAIL(agg_writer.write_agg_data(data_buf_, buf_size, write_pos_))) {
    LOG_WARN("Fail to write aggregated data", K(ret));
  }
  return ret;
}


ObIndexBlockRowParser::ObIndexBlockRowParser()
  : header_(nullptr),
    minor_meta_info_(nullptr),
    row_offset_(0),
    pre_agg_row_buf_(nullptr),
    is_inited_(false) {}

void ObIndexBlockRowParser::reset()
{
  header_ = nullptr;
  minor_meta_info_ = nullptr;
  row_offset_ = 0;
  pre_agg_row_buf_ = nullptr;
  is_inited_ = false;
}

int ObIndexBlockRowParser::init(const int64_t rowkey_column_count, const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_column_count <= 0 || row.get_column_count() != rowkey_column_count + 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey column count", K(ret), K(rowkey_column_count), K(row));
  } else {
    ObString data_buf;
    const ObStorageDatum &datum = row.storage_datums_[rowkey_column_count];
    data_buf = datum.get_string();
    if (OB_FAIL(init(data_buf.ptr(), data_buf.length()))) {
      LOG_WARN("Fail to init index block row parser", K(ret), K(data_buf));
    }
  }
  return ret;
}

int ObIndexBlockRowParser::init(const char *data_buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Inited twice", K(ret));
  } else if (OB_ISNULL(data_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null data buffer for index block row data", K(ret));
  } else {
    header_ = reinterpret_cast<const ObIndexBlockRowHeader *>(data_buf);
    const int64_t header_size = header_->get_serialize_size();
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Invalid index block row header parsed from data", K(ret), KPC(header_));
      header_ = nullptr;
    } else if (OB_UNLIKELY(data_len < header_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_len less than header size", K(ret), KP(data_buf), K(data_len), K(header_size));
    } else if (!header_->is_data_index()) {
      // Init finished
    } else if (!header_->is_major_node()) {
      const int64_t minor_meta_offset = header_size;
      minor_meta_info_ = reinterpret_cast<const ObIndexBlockRowMinorMetaInfo *>(
          data_buf + minor_meta_offset);
    } else {
      // Major node
      int64_t pos = header_size;
      if (data_len > pos && OB_FAIL(serialization::decode_i64(data_buf, data_len, pos, &row_offset_))) {
        LOG_WARN("Fail to decode row offset column", K(ret), K(data_len), K(pos));
      } else if (header_->is_pre_aggregated()) {
        const ObAggRowHeader *agg_row_header = reinterpret_cast<const ObAggRowHeader *>(data_buf + pos);
        if (OB_UNLIKELY(!agg_row_header->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid pre aggregate row header", K(ret), KPC(agg_row_header), KPC(header_));
        } else {
          pre_agg_row_buf_ = data_buf + pos;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockRowParser::get_header(const ObIndexBlockRowHeader *&header) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    header = header_;
  }
  return ret;
}

int ObIndexBlockRowParser::get_minor_meta(const ObIndexBlockRowMinorMetaInfo *&meta) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!header_->is_data_index() || header_->is_major_node())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("This is not a row for minor sstable data", K(ret), KP_(header));
  } else {
    meta = minor_meta_info_;
  }
  return ret;
}

int ObIndexBlockRowParser::get_agg_row(const char *&row_buf, int64_t &buf_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!header_->is_major_node() || !header_->is_pre_aggregated())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Not a index row with preaggregated data", K(ret), KPC(header_));
  } else {
    row_buf = pre_agg_row_buf_;
    buf_size = reinterpret_cast<const ObAggRowHeader *>(row_buf)->length_;
  }
  return ret;
}

int ObIndexBlockRowParser::get_start_row_offset(int64_t &start_row_offset) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (header_->is_major_node() && header_->is_data_index_) {
    start_row_offset = row_offset_ - header_->row_count_ + 1;
  } else {
    start_row_offset = -INT64_MAX;
  }
  return ret;
}

int ObIndexBlockRowParser::is_macro_node(bool &is_macro_node) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else {
    is_macro_node = header_->is_macro_node();
  }
  return ret;
}

int64_t ObIndexBlockRowParser::get_snapshot_version() const
{
  OB_ASSERT(is_inited_);
  return header_->is_major_node() ? 0 : minor_meta_info_->snapshot_version_;
}

int64_t ObIndexBlockRowParser::get_max_merged_trans_version() const
{
  OB_ASSERT(is_inited_);
  return header_->is_major_node() ? 0 : minor_meta_info_->max_merged_trans_version_;
}

int64_t ObIndexBlockRowParser::get_row_count_delta() const
{
  OB_ASSERT(is_inited_);
  return header_->is_major_node() ? 0 : minor_meta_info_->row_count_delta_;
}

}//end namespace blocksstable
}//end namespace oceanbase
