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

#include "common/ob_store_format.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "ob_block_manager.h"
#include "ob_macro_block.h"
#include "ob_micro_block_hash_index.h"
#include "observer/ob_server_struct.h"
#include "share/ob_encryption_util.h"
#include "share/ob_force_print_log.h"
#include "share/ob_task_define.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_sstable_struct.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/ob_sstable_struct.h"
#include "index_block/ob_index_block_row_struct.h"
#include "ob_macro_block_struct.h"
#include "ob_macro_block_handle.h"
#include "storage/blocksstable/ob_data_store_desc.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace blocksstable
{
/**
 * -------------------------------------------------------------------ObMicroBlockCompressor-------------------------------------------------------------------
 */
ObMicroBlockCompressor::ObMicroBlockCompressor()
  : is_none_(false),
    micro_block_size_(0),
    compressor_(NULL),
    comp_buf_("MicroBlkComp"),
    decomp_buf_("MicroBlkDecomp")
{
}

ObMicroBlockCompressor::~ObMicroBlockCompressor()
{
}

void ObMicroBlockCompressor::reset()
{
  is_none_ = false;
  micro_block_size_ = 0;
  if (compressor_ != nullptr) {
    compressor_ = nullptr;
  }
  comp_buf_.reuse();
  decomp_buf_.reuse();
}

int ObMicroBlockCompressor::init(const int64_t micro_block_size, const ObCompressorType comptype)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_UNLIKELY(micro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(micro_block_size), K(comptype));
  } else if (comptype == NONE_COMPRESSOR) {
    is_none_ = true;
    micro_block_size_ = micro_block_size;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(comptype, compressor_))) {
    STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), K(comptype));
  } else {
    is_none_ = false;
    micro_block_size_ = micro_block_size;
  }
  return ret;
}

int ObMicroBlockCompressor::compress(const char *in, const int64_t in_size, const char *&out,
                                     int64_t &out_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (is_none_) {
    out = in;
    out_size = in_size;
  } else if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "compressor is unexpected null", K(ret), K_(compressor));
  } else if (OB_FAIL(compressor_->get_max_overflow_size(in_size, max_overflow_size))) {
    STORAGE_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(in_size));
  } else {
    int64_t comp_size = 0;
    int64_t max_comp_size = max_overflow_size + in_size;
    int64_t need_size = std::max(max_comp_size, micro_block_size_ * 2);
    if (OB_FAIL(comp_buf_.ensure_space(need_size))) {
      STORAGE_LOG(WARN, "macro block writer fail to allocate memory for comp_buf_.", K(ret),
                  K(need_size));
    } else if (OB_FAIL(compressor_->compress(in, in_size, comp_buf_.data(), max_comp_size, comp_size))) {
      STORAGE_LOG(WARN, "compressor fail to compress.", K(in), K(in_size),
                  "comp_ptr", comp_buf_.data(), K(max_comp_size), K(comp_size));
    } else if (comp_size >= in_size) {
      STORAGE_LOG(TRACE, "compressed_size is larger than origin_size",
                  K(comp_size), K(in_size));
      out = in;
      out_size = in_size;
    } else {
      out = comp_buf_.data();
      out_size = comp_size;
      comp_buf_.reuse();
    }
  }
  return ret;
}

int ObMicroBlockCompressor::decompress(const char *in, const int64_t in_size,
                                       const int64_t uncomp_size,
                                       const char *&out, int64_t &out_size)
{
  int ret = OB_SUCCESS;
  int64_t decomp_size = 0;
  decomp_buf_.reuse();
  if (is_none_ || in_size == uncomp_size) {
    out = in;
    out_size = in_size;
  } else if (OB_FAIL(decomp_buf_.ensure_space(uncomp_size))) {
    STORAGE_LOG(WARN, "failed to ensure decomp space", K(ret), K(uncomp_size));
  } else if (OB_FAIL(compressor_->decompress(in, in_size, decomp_buf_.data(), uncomp_size,
                                             decomp_size))) {
    STORAGE_LOG(WARN, "failed to decompress data", K(ret), K(in_size), K(uncomp_size));
  } else {
    out = decomp_buf_.data();
    out_size = decomp_size;
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMacroBlock-------------------------------------------------------------------
 */
ObMacroBlock::ObMacroBlock()
  : spec_(NULL),
    data_("MacroBlock"),
    macro_header_(),
    data_base_offset_(0),
    last_rowkey_(),
    rowkey_allocator_("MacroBlkRowkey"),
    is_dirty_(false),
    common_header_(),
    max_merged_trans_version_(0),
    contain_uncommitted_row_(false),
    original_size_(0),
    data_size_(0),
    data_zsize_(0),
    cur_macro_seq_(-1),
    is_inited_(false)
{
}

ObMacroBlock::~ObMacroBlock()
{
}


int ObMacroBlock::init(const ObDataStoreDesc &spec, const int64_t &cur_macro_seq)
{
  int ret = OB_SUCCESS;
  reuse();
  spec_ = &spec;
  cur_macro_seq_ = cur_macro_seq;
  data_base_offset_ = calc_basic_micro_block_data_offset(
    spec.get_row_column_count(), spec.get_rowkey_column_count(), spec.get_fixed_header_version());
  is_inited_ = true;
  return ret;
}

int ObMacroBlock::inner_init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (data_.is_dirty()) {
    // has been inner_inited, do nothing
  } else if (OB_ISNULL(spec_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected null spec", K(ret));
  } else if (OB_FAIL(data_.ensure_space(spec_->get_macro_block_size()))) {
    STORAGE_LOG(WARN, "macro block fail to ensure space for data.",
                K(ret), "macro_block_size", spec_->get_macro_block_size());
  } else if (OB_FAIL(reserve_header(*spec_, cur_macro_seq_))) {
    STORAGE_LOG(WARN, "macro block fail to reserve header.", K(ret));
  } else if (spec_->is_cg()) {
    last_rowkey_.set_min_rowkey(); // used to protect cg sstable
  }
  return ret;
}

int64_t ObMacroBlock::get_data_size() const {
  int data_size = 0;
  if (data_.length() == 0) {
    data_size = data_base_offset_; // lazy_allocate
  } else {
    data_size = data_.length();
  }
  return data_size;
}

int64_t ObMacroBlock::get_remain_size() const {
  int remain_size = 0;
  if (data_.length() == 0) {
    remain_size = spec_->get_macro_block_size() - data_base_offset_; // lazy_allocate
  } else {
    remain_size = data_.remain();
  }
  return remain_size;
}

int64_t ObMacroBlock::calc_basic_micro_block_data_offset(
  const int64_t column_cnt,
  const int64_t rowkey_col_cnt,
  const uint16_t fixed_header_version)
{
  return sizeof(ObMacroBlockCommonHeader)
        + ObSSTableMacroBlockHeader::get_fixed_header_size()
        + sizeof(bool) /* is_normal_cg */
        + ObSSTableMacroBlockHeader::get_variable_size_in_header(column_cnt, rowkey_col_cnt, fixed_header_version);
}

int ObMacroBlock::check_micro_block(const ObMicroBlockDesc &micro_block_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(micro_block_desc), K(ret));
  } else {
    const int64_t header_size = micro_block_desc.header_->header_size_;
    if (micro_block_desc.buf_size_ + header_size > get_remain_size()) {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}

int ObMacroBlock::write_micro_block(const ObMicroBlockDesc &micro_block_desc, int64_t &data_offset)
{
  int ret = OB_SUCCESS;
  data_offset = data_.length();
  if (OB_FAIL(check_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to check micro block", K(ret));
  } else if (OB_FAIL(inner_init())) {
    STORAGE_LOG(WARN, "fail to inner init", K(ret));
  } else if (!spec_->is_cg()) {
    const ObDatumRowkey &last_rowkey = micro_block_desc.last_rowkey_;
    if (OB_UNLIKELY(last_rowkey.get_datum_cnt() != spec_->get_rowkey_column_count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected rowkey count", K(ret), K(last_rowkey), KPC_(spec));
    } else if (OB_FAIL(last_rowkey.deep_copy(last_rowkey_, rowkey_allocator_))) {
      STORAGE_LOG(WARN, "fail to deep copy last rowkey", K(ret), K(last_rowkey));
    }
  }
  if (OB_SUCC(ret)) {
    data_offset = data_.length();
    const char *data_buf = micro_block_desc.buf_;
    const int64_t data_size = micro_block_desc.buf_size_;
    const ObMicroBlockHeader *header = micro_block_desc.header_;
    is_dirty_ = true;
    int64_t pos = 0;
    if (OB_FAIL(header->serialize(data_.current(), header->header_size_, pos))) {
      STORAGE_LOG(WARN, "serialize header failed", K(ret), KPC(header));
    } else if (FALSE_IT(MEMCPY(data_.current() + pos, data_buf, data_size))) {
    } else if (OB_FAIL(data_.advance(header->header_size_ + data_size))) {
      STORAGE_LOG(WARN, "data advance failed", K(ret), KPC(header), K(data_size));
    } else {
      ++macro_header_.fixed_header_.micro_block_count_;
      macro_header_.fixed_header_.micro_block_data_size_ = static_cast<int32_t>(get_data_size() - data_base_offset_);
      macro_header_.fixed_header_.row_count_ += static_cast<int32_t>(micro_block_desc.row_count_);
      macro_header_.fixed_header_.occupy_size_ = static_cast<int32_t>(get_data_size());
      macro_header_.fixed_header_.data_checksum_ = ob_crc64_sse42(
          macro_header_.fixed_header_.data_checksum_, &header->data_checksum_,
          sizeof(header->data_checksum_));
      original_size_ += micro_block_desc.original_size_;
      data_size_ += micro_block_desc.data_size_;
      data_zsize_ += micro_block_desc.buf_size_;
      // update info from micro_block
      update_max_merged_trans_version(micro_block_desc.max_merged_trans_version_);
      if (micro_block_desc.contain_uncommitted_row_) {
        set_contain_uncommitted_row();
      }
      if (header->has_column_checksum_) {
        if (OB_FAIL(add_column_checksum(header->column_checksums_,
                                        header->column_count_,
                                        macro_header_.column_checksum_))) {
          STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
        }
      }
    }
  }
  return ret;
}


int ObMacroBlock::write_index_micro_block(
    const ObMicroBlockDesc &micro_block_desc,
    const bool is_leaf_index_block,
    int64_t &data_offset)
{
  int ret = OB_SUCCESS;
  data_offset = data_.length();
  if (OB_FAIL(check_micro_block(micro_block_desc))) {
    STORAGE_LOG(WARN, "fail to check index micro block", K(ret));
  } else if (OB_UNLIKELY(!is_dirty_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "can not write index micro block into empty macro block", K(micro_block_desc), K(ret));
  } else {
    const char *data_buf = micro_block_desc.buf_;
    const int64_t data_size = micro_block_desc.buf_size_;
    const ObMicroBlockHeader *header = micro_block_desc.header_;
    const int64_t block_size = header->header_size_ + data_size;
    int64_t pos = 0;
    if (OB_FAIL(header->serialize(data_.current(), header->header_size_, pos))) {
      STORAGE_LOG(WARN, "serialize header failed", K(ret), KPC(header));
    } else if (FALSE_IT(MEMCPY(data_.current() + pos, data_buf, data_size))) {
    } else if (OB_FAIL(data_.advance(block_size))) {
      STORAGE_LOG(WARN, "data advance failed", K(ret), KPC(header), K(block_size));
    } else if (is_leaf_index_block) {
      macro_header_.fixed_header_.idx_block_offset_ = data_offset;
      macro_header_.fixed_header_.idx_block_size_ = block_size;
    } else {
      macro_header_.fixed_header_.meta_block_offset_ = data_offset;
      macro_header_.fixed_header_.meta_block_size_ = block_size;
    }
  }
  return ret;
}

int ObMacroBlock::flush(ObMacroBlockHandle &macro_handle,
                        ObMacroBlocksWriteCtx &block_write_ctx)
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BAD_BLOCK_ERROR) OB_SUCCESS;
  if (OB_CHECKSUM_ERROR == ret) { // obtest will set this code
    STORAGE_LOG(INFO, "ERRSIM bad block: Insert a bad block.");
    macro_header_.fixed_header_.magic_ = 0;
    macro_header_.fixed_header_.data_checksum_ = 0;
  }
#endif

  if (OB_FAIL(write_macro_header())) {
    STORAGE_LOG(WARN, "fail to write macro header", K(ret), K_(macro_header));
  } else {
    const int64_t common_header_size = common_header_.get_serialize_size();
    const char *payload_buf = data_.data() + common_header_size;
    const int64_t payload_size = data_.length() - common_header_size;
    common_header_.set_payload_size(static_cast<int32_t>(payload_size));
    common_header_.set_payload_checksum(static_cast<int32_t>(ob_crc64(payload_buf, payload_size)));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(common_header_.build_serialized_header(data_.data(), data_.capacity()))) {
    STORAGE_LOG(WARN, "Fail to build common header, ", K(ret), K_(common_header));
  } else {
    ObMacroBlockWriteInfo write_info;
    write_info.buffer_ = data_.data();
    write_info.size_ = data_.upper_align_length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    write_info.io_desc_.set_sys_module_id(ObIOModule::SSTABLE_MACRO_BLOCK_WRITE_IO);
    write_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    if (OB_FAIL(macro_handle.async_write(write_info))) {
      STORAGE_LOG(WARN, "Fail to async write block", K(ret), K(macro_handle), K(write_info));
    } else if (OB_FAIL(block_write_ctx.add_macro_block_id(macro_handle.get_macro_id()))) {
      STORAGE_LOG(WARN, "fail to add macro id", K(ret), "macro id", macro_handle.get_macro_id());
    } else if (NULL != spec_ && NULL != spec_->merge_info_) {
      spec_->merge_info_->macro_block_count_++;
      spec_->merge_info_->new_flush_occupy_size_ += macro_header_.fixed_header_.occupy_size_;
      spec_->merge_info_->occupy_size_ += macro_header_.fixed_header_.occupy_size_;
      spec_->merge_info_->total_row_count_ += macro_header_.fixed_header_.row_count_;
    }
    const uint16_t table_cg_idx = spec_ == nullptr ? 0 : spec_->get_table_cg_idx();

    if (OB_SUCC(ret)) {
      // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
      share::ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "macro block writer succeed to flush macro block.",
                  "block_id", macro_handle.get_macro_id(), K(common_header_), K(macro_header_),
                  K_(contain_uncommitted_row), K_(max_merged_trans_version), KP(&macro_handle), K(table_cg_idx));
    }
  }
  return ret;
}

void ObMacroBlock::reset()
{
  data_.reset();
  macro_header_.reset();
  data_base_offset_ = 0;
  is_dirty_ = false;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
  original_size_ = 0;
  data_size_ = 0;
  data_zsize_ = 0;
  last_rowkey_.reset();
  rowkey_allocator_.reset();
  is_inited_ = false;
}

void ObMacroBlock::reuse()
{
  data_.reuse();
  macro_header_.reset();
  data_base_offset_ = 0;
  is_dirty_ = false;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
  original_size_ = 0;
  data_size_ = 0;
  data_zsize_ = 0;
  last_rowkey_.reset();
  rowkey_allocator_.reuse();
  is_inited_ = false;
}
int ObMacroBlock::reserve_header(const ObDataStoreDesc &spec, const int64_t &cur_macro_seq)
{
  int ret = OB_SUCCESS;
  common_header_.reset();
  common_header_.set_payload_size(0);
  common_header_.set_payload_checksum(0);
  common_header_.set_attr(cur_macro_seq);
  const int64_t common_header_size = common_header_.get_serialize_size();
  MEMSET(data_.data(), 0, data_.capacity());
  if (OB_FAIL(data_.advance(common_header_size))) {
    STORAGE_LOG(WARN, "data buffer is not enough for common header.", K(ret), K(common_header_size));
  } else {
    char *col_types_buf = data_.current()  + macro_header_.get_fixed_header_size();
    const int64_t col_type_array_cnt = spec.get_fixed_header_col_type_cnt();
    char *col_orders_buf = col_types_buf + sizeof(ObObjMeta) * col_type_array_cnt;
    char *col_checksum_buf = col_orders_buf + sizeof(ObOrderType) * col_type_array_cnt;
    if (OB_FAIL(macro_header_.init(spec,
                                   reinterpret_cast<ObObjMeta *>(col_types_buf),
                                   reinterpret_cast<ObOrderType *>(col_orders_buf),
                                   reinterpret_cast<int64_t *>(col_checksum_buf)))){
      STORAGE_LOG(WARN, "fail to init macro block header", K(ret), K(spec));
    } else {
      macro_header_.fixed_header_.data_seq_ = cur_macro_seq;
      const int64_t expect_base_offset = macro_header_.get_serialize_size() + common_header_size;
      // prevent static func calc_basic_micro_block_data_offset from returning wrong offset
      if (OB_UNLIKELY(data_base_offset_ != expect_base_offset)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "expect equal", K(ret), K_(data_base_offset), K(expect_base_offset),
            K_(macro_header), K(common_header_size), K(spec.get_row_column_count()), K(spec.get_rowkey_column_count()),
            K(spec.get_fixed_header_version()));
      } else if (OB_FAIL(data_.advance(macro_header_.get_serialize_size()))) {
        STORAGE_LOG(WARN, "macro_block_header_size out of data buffer.", K(ret));
      }
    }
  }
  return ret;
}

int ObMacroBlock::write_macro_header()
{
  int ret = OB_SUCCESS;
  const int64_t common_header_size = common_header_.get_serialize_size();
  const int64_t buf_len = macro_header_.get_serialize_size();
  const int64_t data_length = data_.length();
  int64_t pos = 0;
  if (OB_FAIL(macro_header_.serialize(data_.data() + common_header_size, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to serialize macro block", K(ret), K(macro_header_));
  }
  return ret;
}

int ObMacroBlock::get_macro_block_meta(ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  macro_meta.val_.logic_id_.logic_version_ = spec_->get_logical_version();
  macro_meta.val_.logic_id_.column_group_idx_= spec_->get_table_cg_idx();
  macro_meta.val_.logic_id_.data_seq_.macro_data_seq_ = macro_header_.fixed_header_.data_seq_;
  macro_meta.val_.logic_id_.tablet_id_ = spec_->get_tablet_id().id();
  macro_meta.val_.logic_id_.is_mds_ = is_mds_merge(spec_->get_merge_type());
  macro_meta.val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
  macro_meta.val_.rowkey_count_ = macro_header_.fixed_header_.rowkey_column_count_;
  macro_meta.val_.compressor_type_ = spec_->get_compressor_type();
  macro_meta.val_.micro_block_count_ = macro_header_.fixed_header_.micro_block_count_;
  macro_meta.val_.data_checksum_ = macro_header_.fixed_header_.data_checksum_;
  macro_meta.val_.progressive_merge_round_ = spec_->get_progressive_merge_round();
  macro_meta.val_.occupy_size_ = macro_header_.fixed_header_.occupy_size_;
  macro_meta.val_.data_size_ = data_size_;
  macro_meta.val_.data_zsize_ = data_zsize_;
  macro_meta.val_.original_size_ = original_size_;
  macro_meta.val_.column_count_ = macro_header_.fixed_header_.column_count_;
  macro_meta.val_.is_encrypted_ = spec_->get_encrypt_id() > 0;
  macro_meta.end_key_ = last_rowkey_;
  macro_meta.val_.master_key_id_ = spec_->get_master_key_id();
  macro_meta.val_.encrypt_id_ = spec_->get_encrypt_id();
  MEMCPY(macro_meta.val_.encrypt_key_, spec_->get_encrypt_key(),
      sizeof(macro_meta.val_.encrypt_key_));
  macro_meta.val_.row_store_type_ = spec_->get_row_store_type();
  macro_meta.val_.schema_version_ = spec_->get_schema_version();
  macro_meta.val_.snapshot_version_ = spec_->get_snapshot_version();
  if (OB_ISNULL(macro_header_.column_checksum_)) {
  } else if (OB_FAIL(macro_meta.val_.column_checksums_.reserve(macro_meta.val_.column_count_))) {
    STORAGE_LOG(WARN, "fail to reserve checksum array", K(ret), K(macro_meta));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_meta.val_.column_count_; ++i) {
      if (OB_FAIL(macro_meta.val_.column_checksums_.push_back(macro_header_.column_checksum_[i]))) {
        STORAGE_LOG(WARN, "fail to push column checksum", K(ret), K(macro_meta));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "build invalid macro meta", K(ret), K(macro_meta));
  } else if (spec_->is_cg()) {
    macro_meta.val_.rowkey_count_ = 1;
  }
  STORAGE_LOG(DEBUG, "build macro block meta", K(ret), K(macro_meta), K_(last_rowkey));
  return ret;
}

int ObMacroBlock::add_column_checksum(
    const int64_t *to_add_checksum,
    const int64_t column_cnt,
    int64_t *column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == to_add_checksum || column_cnt <= 0 || NULL == column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(to_add_checksum), K(column_cnt),
                KP(column_checksum));
  } else {
    for (int64_t i = 0; i < column_cnt; ++i) {
      column_checksum[i] += to_add_checksum[i];
    }
  }
  return ret;
}

}
}
