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

#include "common/row/ob_row.h"
#include "ob_index_block_row_struct.h"
#include "ob_block_sstable_struct.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace blocksstable
{

ObIndexBlockRowDesc::ObIndexBlockRowDesc()
  : data_store_desc_(nullptr), row_key_(), macro_id_(), block_offset_(0),
    row_count_(0), row_count_delta_(0), max_merged_trans_version_(0), block_size_(0),
    macro_block_count_(0), micro_block_count_(0),
    is_deleted_(false), contain_uncommitted_row_(false), is_data_block_(false),
    is_secondary_meta_(false), is_macro_node_(false), has_string_out_row_(false), has_lob_out_row_(false),
    is_last_row_last_flag_(false) {}

ObIndexBlockRowDesc::ObIndexBlockRowDesc(ObDataStoreDesc &data_store_desc)
  : data_store_desc_(&data_store_desc), row_key_(), macro_id_(), block_offset_(0),
    row_count_(0), row_count_delta_(0), max_merged_trans_version_(0), block_size_(0),
    macro_block_count_(0), micro_block_count_(0),
    is_deleted_(false), contain_uncommitted_row_(false), is_data_block_(false),
    is_secondary_meta_(false), is_macro_node_(false), has_string_out_row_(false), has_lob_out_row_(false),
    is_last_row_last_flag_(false) {}

MacroBlockId ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID(0, DEFAULT_IDX_ROW_MACRO_IDX, 0);

ObIndexBlockRowHeader::ObIndexBlockRowHeader()
  : pack_(0), macro_id_(), block_offset_(0), block_size_(0),
    master_key_id_(0), encrypt_id_(0), encrypt_key_(), row_count_(), schema_version_(0),
    macro_block_count_(0), micro_block_count_(0)
{
  version_ = INDEX_BLOCK_HEADER_V1;
}

void ObIndexBlockRowHeader::reset()
{
  MEMSET(this, 0, sizeof(*this));
  version_ = INDEX_BLOCK_HEADER_V1;
  macro_id_.reset();
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
  : allocator_(ObModIds::OB_BLOCK_INDEX_INTERMEDIATE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    index_data_allocator_(ObModIds::OB_BLOCK_INDEX_INTERMEDIATE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    row_(),
    rowkey_column_count_(0),
    rowkey_column_types_(nullptr),
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
  rowkey_column_count_ = 0;
  rowkey_column_types_ = nullptr;
  data_buf_ = nullptr;
  write_pos_ = 0;
  header_ = nullptr;
  allocator_.reset();
  index_data_allocator_.reset();
  is_inited_ = false;
}

int ObIndexBlockRowBuilder::init(const ObDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Double init", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid data store description", K(ret), K(desc));
  } else if (OB_ISNULL(rowkey_column_types_ =
      reinterpret_cast<ObObjMeta *>(allocator_.alloc(desc.rowkey_column_count_ * sizeof(common::ObObjMeta))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc rowkey_column_types memory", K(ret), K(desc.rowkey_column_count_));
  } else if (OB_FAIL(row_.init(allocator_, desc.rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "Failed to init row", K(ret), K(desc.rowkey_column_count_));
  } else {
    rowkey_column_count_ = desc.rowkey_column_count_;
    const ObIArray<share::schema::ObColDesc> &descs = desc.get_rowkey_col_descs();
    for (int64_t i = 0; i < rowkey_column_count_; ++i) {
      rowkey_column_types_[i] = descs.at(i).col_type_;
    }
    is_inited_ = true;
  }
  return ret;
}

int ObIndexBlockRowBuilder::build_row(const ObIndexBlockRowDesc &desc, const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Index block description is not valid", K(ret));
  } else if (OB_FAIL(row_.reserve(rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "Failed to reserve index row", K(ret), K(rowkey_column_count_));
  } else if (OB_FAIL(set_rowkey(desc))) {
    LOG_WARN("Fail to set rowkey", K(ret));
  } else if (OB_FAIL(calc_data_size(desc, data_size))) {
    LOG_WARN("Fail to calculate row data size", K(ret));
  } else if (OB_ISNULL(data_buf_ = reinterpret_cast<char *>(index_data_allocator_.alloc(data_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to alloc memory for data buffer", K(ret), K(data_size));
  } else if (FALSE_IT(MEMSET(data_buf_, 0, data_size))) {
  } else if (OB_FAIL(append_header_and_meta(desc))) {
    LOG_WARN("Fail to append header and meta to buffer", K(ret), K_(write_pos));
  } else if (OB_FAIL(append_aggregate_data(desc))) {
    LOG_WARN("Fail to append aggregated data to buffer", K(ret), K_(write_pos));
  } else {
    ObString str(data_size, data_buf_);
    row_.storage_datums_[rowkey_column_count_].set_string(str);
    row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row = &row_;
    LOG_DEBUG("build index row", K_(desc.row_key), KPC_(header));
  }
  return ret;
}

int ObIndexBlockRowBuilder::build_row(
    const ObMicroIndexInfo &micro_idx_info,
    const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t data_size = 0;
  reuse();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(!micro_idx_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Micro index info is not valid", K(ret));
  } else if (OB_FAIL(row_.reserve(rowkey_column_count_ + 1))) {
    STORAGE_LOG(WARN, "Failed to reserve index row", K(ret), K(rowkey_column_count_));
  } else if (OB_FAIL(set_rowkey(*micro_idx_info.endkey_))) {
    LOG_WARN("Fail to set rowkey", K(ret));
  } else if (OB_FAIL(calc_data_size(*micro_idx_info.row_header_, data_size))) {
    LOG_WARN("Fail to calculate row data size", K(ret));
  } else {
    const char *ptr = reinterpret_cast<const char *>(micro_idx_info.row_header_);
    ObString str(data_size, ptr);
    row_.storage_datums_[rowkey_column_count_].set_string(str);
    row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row = &row_;
    LOG_DEBUG("build index row", KPC_(micro_idx_info.endkey), KPC_(header));
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

int ObIndexBlockRowBuilder::calc_data_size(const ObIndexBlockRowDesc &desc, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index block row description", K(ret), K(desc));
  } else if (desc.is_secondary_meta_) {
    size = sizeof(ObIndexBlockRowHeader);
  } else if (desc.data_store_desc_->is_major_merge()) {
    size = sizeof(ObIndexBlockRowHeader);
    // if (desc.is_pre_aggregated_) {
      // TODO: @saitong.zst Calculate aggregate size here
      // size += aggregate_size;
    // }
  } else {
    size = sizeof(ObIndexBlockRowHeader) + sizeof(ObIndexBlockRowMinorMetaInfo);
  }
  return ret;
}

int ObIndexBlockRowBuilder::calc_data_size(
    const ObIndexBlockRowHeader &idx_row_header,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_UNLIKELY(!idx_row_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid indeex block row header", K(ret), K(idx_row_header));
  } else if (!idx_row_header.is_data_index()) {
    size = sizeof(ObIndexBlockRowHeader);
  } else if (idx_row_header.is_major_node()) {
    size = sizeof(ObIndexBlockRowHeader);
  } else {
    size = sizeof(ObIndexBlockRowHeader) + sizeof(ObIndexBlockRowMinorMetaInfo);
  }
  return ret;
}

int ObIndexBlockRowBuilder::append_header_and_meta(const ObIndexBlockRowDesc &desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != write_pos_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write pos for buffer should be zero when write header", K(ret), K_(write_pos));
  } else {
    const bool is_data_mid_micro_block = !desc.is_secondary_meta_;
    header_ = reinterpret_cast<ObIndexBlockRowHeader *>(data_buf_);
    header_->version_ = ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V1;
    header_->row_store_type_ = static_cast<uint8_t>(desc.data_store_desc_->row_store_type_);
    header_->compressor_type_ = static_cast<uint8_t>(desc.data_store_desc_->compressor_type_);
    header_->is_data_index_ = is_data_mid_micro_block;
    header_->is_data_block_ = desc.is_data_block_;
    header_->is_leaf_block_ = desc.is_macro_node_;
    header_->is_macro_node_ = desc.is_macro_node_;
    header_->is_major_node_ = desc.data_store_desc_->is_major_merge();
    header_->has_string_out_row_ = desc.has_string_out_row_;
    header_->all_lob_in_row_ = !desc.has_lob_out_row_;
    // header_->is_pre_aggregated_ =
    header_->is_deleted_ = desc.is_deleted_;
    header_->macro_id_ =(desc.is_data_block_ && is_data_mid_micro_block)
        ? ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID : desc.macro_id_;
    header_->block_offset_ = desc.block_offset_;
    header_->block_size_ = desc.block_size_;
    header_->macro_block_count_ = desc.macro_block_count_;
    header_->micro_block_count_ = desc.micro_block_count_;
    header_->master_key_id_ = desc.data_store_desc_->master_key_id_;
    header_->encrypt_id_ = desc.data_store_desc_->encrypt_id_;
    MEMCPY(header_->encrypt_key_, desc.data_store_desc_->encrypt_key_, sizeof(header_->encrypt_key_));
    header_->schema_version_ = desc.data_store_desc_->schema_version_;
    header_->row_count_ = desc.row_count_;
    write_pos_ += sizeof(ObIndexBlockRowHeader);
    if (header_->is_data_index() && !header_->is_major_node()) {
      ObIndexBlockRowMinorMetaInfo *minor_meta
          = reinterpret_cast<ObIndexBlockRowMinorMetaInfo *>(data_buf_ + write_pos_);
      minor_meta->snapshot_version_ = desc.data_store_desc_->end_scn_.get_val_for_tx();
      header_->contain_uncommitted_row_ = desc.contain_uncommitted_row_;
      minor_meta->max_merged_trans_version_ = desc.max_merged_trans_version_;
      minor_meta->row_count_delta_ = desc.row_count_delta_;
      write_pos_ += sizeof(ObIndexBlockRowMinorMetaInfo);
    }
    if (OB_UNLIKELY(!header_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Built an invalid index block row", K(ret), KPC(header_));
    }
  }
  return ret;
}

int ObIndexBlockRowBuilder::append_aggregate_data(const ObIndexBlockRowDesc &desc)
{
  int ret = OB_SUCCESS;
  UNUSED(desc);
  if (OB_ISNULL(header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to append aggregation data to buffer", K(ret), KP_(header));
  } else if (!header_->is_pre_aggregated()) {
  } else {
    // TODO: @saitong.zst Write aggregate data here
  }
  return ret;
}


ObIndexBlockRowParser::ObIndexBlockRowParser()
  : header_(nullptr), minor_meta_info_(nullptr), is_inited_(false) {}

int ObIndexBlockRowParser::init(const int64_t rowkey_column_count, const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowkey_column_count <= 0 || row.get_column_count() != rowkey_column_count + 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid rowkey column count", K(ret), K(rowkey_column_count), K(row));
  } else {
    ObString data_buf;
    const ObStorageDatum &datum = row.storage_datums_[rowkey_column_count];
    if (OB_UNLIKELY(datum.len_ < sizeof(ObIndexBlockRowHeader))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data buffer length of row value less than header size", K(ret), K(datum));
    } else if (FALSE_IT(data_buf = datum.get_string())) {
      LOG_WARN("Fail to get varbinary data buffer from value object", K(ret), K(datum));
    } else if (OB_FAIL(init(data_buf.ptr()))) {
      LOG_WARN("Fail to init index block row parser", K(ret), K(data_buf));
    }
  }
  return ret;
}

int ObIndexBlockRowParser::init(const char *data_buf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Unexpected null data buffer for index block row data", K(ret));
  } else if (FALSE_IT(header_ = reinterpret_cast<const ObIndexBlockRowHeader *>(data_buf))) {
  } else if (OB_UNLIKELY(!header_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Invalid index block row header parsed from data", K(ret), KPC(header_));
    header_ = nullptr;
  } else if (!header_->is_data_index()) {
    // Init finished
  } else if (!header_->is_major_node()) {
    const int64_t minor_meta_offset = sizeof(ObIndexBlockRowHeader);
    minor_meta_info_ = reinterpret_cast<const ObIndexBlockRowMinorMetaInfo *>(
      data_buf + minor_meta_offset);
  }

  // TODO: @saitong.zst locate aggregated data logic here

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
