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

#include "ob_macro_block.h"
#include "ob_store_file.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_task_define.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "storage/ob_sstable.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_store_format.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_meta_redo_module.h"
#include "storage/ob_file_system_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase {
namespace blocksstable {
/**
 * -------------------------------------------------------------------ObDataStoreDesc-------------------------------------------------------------------
 */
const char* ObDataStoreDesc::DEFAULT_MINOR_COMPRESS_NAME = "lz4_1.0";
int ObDataStoreDesc::cal_row_store_type(const share::schema::ObTableSchema& table_schema, const ObMergeType merge_type)
{
  int ret = OB_SUCCESS;

  if (!is_major_merge(merge_type)) {  // not major
    if (GCONF._enable_sparse_row) {   // enable use sparse row
      // buf minor merge and trans table write flat row
      if (is_trans_table_id(table_schema.get_table_id())) {
        row_store_type_ = FLAT_ROW_STORE;
      } else {
        row_store_type_ = SPARSE_ROW_STORE;
      }
    } else {
      row_store_type_ = FLAT_ROW_STORE;
    }
  } else {
    // major merge support flat only
    row_store_type_ = FLAT_ROW_STORE;
  }
  STORAGE_LOG(DEBUG, "row store type", K(row_store_type_), K(merge_type));
  return ret;
}

int ObDataStoreDesc::get_major_working_cluster_version()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_major_ || data_version_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(
        WARN, "Unexpected data store to get major working cluster version", K(ret), K_(is_major), K_(data_version));
  } else {
    ObFreezeInfoSnapshotMgr::FreezeInfoLite freeze_info;
    if (OB_FAIL(ObFreezeInfoMgrWrapper::get_instance().get_freeze_info_by_major_version(data_version_, freeze_info))) {
      STORAGE_LOG(WARN, "Failed to get freeze info", K(ret), K_(data_version));
    } else if (freeze_info.cluster_version < 0) {
      STORAGE_LOG(ERROR, "Unexpected cluster version of freeze info", K(ret), K(freeze_info));
      major_working_cluster_version_ = 0;
    } else {
      major_working_cluster_version_ = freeze_info.cluster_version;
    }
    if (OB_SUCC(ret)) {
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO,
          "Succ to get major working cluster version",
          K_(major_working_cluster_version),
          K(freeze_info),
          K_(data_version));
    }
  }

  return ret;
}

int ObDataStoreDesc::init(const ObTableSchema& table_schema, const int64_t data_version,
    const ObMultiVersionRowInfo* multi_version_row_info, const int64_t partition_id, const ObMergeType merge_type,
    const bool need_calc_column_checksum, const bool store_micro_block_column_checksum, const common::ObPGKey& pg_key,
    const ObStorageFileHandle& file_handle, const int64_t snapshot_version, const bool need_check_order,
    const bool need_index_tree)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || data_version < 0 || !pg_key.is_valid() || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments is invalid", K(ret), K(table_schema), K(data_version), K(pg_key), K(snapshot_version));
  } else {
    reset();
    is_major_ = storage::is_major_merge(merge_type);
    table_id_ = table_schema.get_table_id();
    partition_id_ = partition_id;
    data_version_ = data_version;
    pg_key_ = pg_key;
    schema_rowkey_col_cnt_ = table_schema.get_rowkey_column_num();
    column_index_scale_ = 1;
    schema_version_ = table_schema.get_schema_version();
    pct_free_ = table_schema.get_pctfree();
    micro_block_size_ = table_schema.get_block_size();
    macro_block_size_ = OB_FILE_SYSTEM.get_macro_block_size();
    need_calc_column_checksum_ = need_calc_column_checksum && is_major_;
    need_index_tree_ = need_index_tree;
    store_micro_block_column_checksum_ = store_micro_block_column_checksum;
    snapshot_version_ = snapshot_version;
    need_calc_physical_checksum_ = true;
    rowkey_helper_ = nullptr;
    need_check_order_ = need_check_order;
    // need_calc_physical_checksum_ = table_schema.get_storage_format_version() >=
    // ObIPartitionStorage::STORAGE_FORMAT_VERSION_V3;
    if (pct_free_ >= 0 && pct_free_ <= 50) {
      macro_store_size_ = macro_block_size_ * (100 - pct_free_) / 100;
    } else {
      macro_store_size_ = macro_block_size_ * DEFAULT_RESERVE_PERCENT / 100;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_handle_.assign(file_handle))) {
        STORAGE_LOG(WARN, "failed to assign file handle", K(ret), K(file_handle));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(multi_version_row_info) && multi_version_row_info->is_valid()) {
        row_column_count_ = multi_version_row_info->column_cnt_;
        rowkey_column_count_ = multi_version_row_info->multi_version_rowkey_column_cnt_;
      } else if (OB_FAIL(table_schema.get_store_column_count(row_column_count_))) {
        STORAGE_LOG(WARN, "failed to get store column count", K(ret), K_(table_id), K_(partition_id));
      } else {
        rowkey_column_count_ = table_schema.get_rowkey_column_num();
      }
    }
    if (OB_SUCC(ret)) {
      const char* compress_func = nullptr;
      if (OB_ISNULL(compress_func = table_schema.get_compress_func_name())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null compress func name", K(ret), K(table_schema));
      } else if (strlen(compress_func) < static_cast<uint64_t>(OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH)) {
        if (!is_major_ && 0 != strcmp(compress_func, "none")) {
          compress_func = DEFAULT_MINOR_COMPRESS_NAME;
        }
        strncpy(compressor_name_, compress_func, OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH);
      } else {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "The name of compressor is too long", K(ret), K(table_schema));
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t header_version =
          store_micro_block_column_checksum ? RECORD_HEADER_VERSION_V3 : RECORD_HEADER_VERSION_V2;
      const int64_t entry_size = ObMicroBlockIndexWriter::get_entry_size(NULL != multi_version_row_info);
      const int64_t record_header_size = ObRecordHeaderV3::get_serialize_size(header_version, row_column_count_);
      const ObMacroBlockCommonHeader common_header;
      micro_block_size_limit_ = macro_block_size_ - common_header.get_serialize_size() -
                                sizeof(ObSSTableMacroBlockHeader) - entry_size - record_header_size -
                                ObMicroBlockIndexWriter::INDEX_ENTRY_SIZE  // last entry
                                - MIN_RESERVED_SIZE;
      progressive_merge_round_ = table_schema.get_progressive_merge_round();
      need_prebuild_bloomfilter_ = table_schema.is_use_bloomfilter();
      bloomfilter_size_ = 0;
      bloomfilter_rowkey_prefix_ = 0;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cal_row_store_type(table_schema, merge_type))) {
      STORAGE_LOG(WARN, "Failed to make the row store type", K(ret));
    } else if (OB_FAIL(table_schema.has_lob_column(has_lob_column_, true))) {
      STORAGE_LOG(WARN, "Failed to check lob column in table schema", K(ret));
    } else if (is_major_ && OB_FAIL(get_major_working_cluster_version())) {
      STORAGE_LOG(WARN, "Failed to get major working cluster version", K(ret));
    } else {
      ObSEArray<ObColDesc, OB_DEFAULT_SE_ARRAY_COUNT> column_list;
      if (OB_NOT_NULL(multi_version_row_info) && multi_version_row_info->is_valid()) {
        ObMultiVersionColDescGenerate multi_version_col_desc_gen;
        if (OB_FAIL(multi_version_col_desc_gen.init(&table_schema))) {
          STORAGE_LOG(WARN, "fail to init multi version col desc generate", K(ret));
        } else if (OB_FAIL(multi_version_col_desc_gen.generate_column_ids(column_list))) {
          STORAGE_LOG(WARN, "fail to get multi version col desc column list", K(ret));
        }
      } else {
        if (OB_FAIL(table_schema.get_store_column_ids(column_list))) {
          STORAGE_LOG(WARN, "Fail to get column ids, ", K(ret));
        }
      }

      if (OB_SUCC(ret) && 0 == column_list.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column ids should not be empty", K(ret), K(column_list.count()));
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_list.count(); ++i) {
          if (i >= common::OB_MAX_COLUMN_NUMBER) {
            ret = OB_SIZE_OVERFLOW;
            STORAGE_LOG(WARN, "column type&id list overflow.", K(ret));
          } else {
            column_ids_[i] = column_list.at(i).col_id_;
            column_types_[i] = column_list.at(i).col_type_;
            column_orders_[i] = column_list.at(i).col_order_;
          }
        }
      }
    }
  }
  return ret;
}

bool ObDataStoreDesc::is_valid() const
{
  return table_id_ > 0 && data_version_ >= 0 && micro_block_size_ > 0 && micro_block_size_limit_ > 0 &&
         row_column_count_ > 0 && rowkey_column_count_ > 0 && row_column_count_ >= rowkey_column_count_ &&
         column_index_scale_ > 0 && schema_version_ >= 0 && schema_rowkey_col_cnt_ >= 0 && partition_id_ >= -1 &&
         pct_free_ >= 0 && snapshot_version_ > 0 && pg_key_.is_valid() && file_handle_.is_valid();
  // TODO:compresor_name_
  // row_store_type_
}

void ObDataStoreDesc::reset()
{
  table_id_ = 0;
  partition_id_ = -1;
  data_version_ = 0;
  macro_block_size_ = 0;
  macro_store_size_ = 0;
  micro_block_size_ = 0;
  micro_block_size_limit_ = 0;
  row_column_count_ = 0;
  rowkey_column_count_ = 0;
  column_index_scale_ = 0;
  schema_rowkey_col_cnt_ = 0;
  row_store_type_ = FLAT_ROW_STORE;
  schema_version_ = 0;
  pct_free_ = 0;
  mark_deletion_maker_ = NULL;
  merge_info_ = NULL;
  has_lob_column_ = false;
  is_major_ = false;
  MEMSET(compressor_name_, 0, OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH);
  MEMSET(column_ids_, 0, sizeof(column_ids_));
  MEMSET(column_types_, 0, sizeof(column_types_));
  MEMSET(column_orders_, 0, sizeof(column_orders_));
  need_calc_column_checksum_ = false;
  need_index_tree_ = false;
  store_micro_block_column_checksum_ = false;
  snapshot_version_ = 0;
  need_calc_physical_checksum_ = false;
  need_prebuild_bloomfilter_ = false;
  bloomfilter_size_ = 0;
  bloomfilter_rowkey_prefix_ = 0;
  rowkey_helper_ = nullptr;
  pg_key_.reset();
  file_handle_.reset();
  need_check_order_ = true;
  progressive_merge_round_ = 0;
  major_working_cluster_version_ = 0;
}

int ObDataStoreDesc::assign(const ObDataStoreDesc& desc)
{
  int ret = OB_SUCCESS;
  table_id_ = desc.table_id_;
  partition_id_ = desc.partition_id_;
  data_version_ = desc.data_version_;
  macro_block_size_ = desc.macro_block_size_;
  macro_store_size_ = desc.macro_store_size_;
  micro_block_size_ = desc.micro_block_size_;
  micro_block_size_limit_ = desc.micro_block_size_limit_;
  row_column_count_ = desc.row_column_count_;
  rowkey_column_count_ = desc.rowkey_column_count_;
  column_index_scale_ = desc.column_index_scale_;
  row_store_type_ = desc.row_store_type_;
  schema_version_ = desc.schema_version_;
  schema_rowkey_col_cnt_ = desc.schema_rowkey_col_cnt_;
  pct_free_ = desc.pct_free_;
  mark_deletion_maker_ = desc.mark_deletion_maker_;
  merge_info_ = desc.merge_info_;
  has_lob_column_ = desc.has_lob_column_;
  is_major_ = desc.is_major_;
  MEMCPY(compressor_name_, desc.compressor_name_, OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH);
  MEMCPY(column_ids_, desc.column_ids_, sizeof(column_ids_));
  MEMCPY(column_types_, desc.column_types_, sizeof(column_types_));
  MEMCPY(column_orders_, desc.column_orders_, sizeof(column_orders_));
  need_calc_column_checksum_ = desc.need_calc_column_checksum_;
  store_micro_block_column_checksum_ = desc.store_micro_block_column_checksum_;
  snapshot_version_ = desc.snapshot_version_;
  need_calc_physical_checksum_ = desc.need_calc_physical_checksum_;
  need_index_tree_ = desc.need_index_tree_;
  need_prebuild_bloomfilter_ = desc.need_prebuild_bloomfilter_;
  bloomfilter_size_ = desc.need_prebuild_bloomfilter_;
  bloomfilter_rowkey_prefix_ = desc.bloomfilter_rowkey_prefix_;
  rowkey_helper_ = desc.rowkey_helper_;
  pg_key_ = desc.pg_key_;
  need_check_order_ = desc.need_check_order_;
  major_working_cluster_version_ = desc.major_working_cluster_version_;
  if (OB_FAIL(file_handle_.assign(desc.file_handle_))) {
    STORAGE_LOG(WARN, "failed to assign file handle", K(ret), K(desc.file_handle_));
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMicroBlockCompressor-------------------------------------------------------------------
 */
ObMicroBlockCompressor::ObMicroBlockCompressor()
    : is_none_(false),
      micro_block_size_(0),
      compressor_(NULL),
      comp_buf_(0, "MicrBlocComp"),
      decomp_buf_(0, "MicrBlocDecomp")
{}

ObMicroBlockCompressor::~ObMicroBlockCompressor()
{}

void ObMicroBlockCompressor::reset()
{
  is_none_ = false;
  micro_block_size_ = 0;
  compressor_ = NULL;
  comp_buf_.reuse();
  decomp_buf_.reuse();
}

int ObMicroBlockCompressor::init(const int64_t micro_block_size, const char* compname)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_UNLIKELY(micro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(micro_block_size), KP(compname));
  } else if (NULL == compname || compname[0] == '\0' || 0 == strcmp(compname, "none")) {
    is_none_ = true;
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(compname, compressor_))) {
    STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), "compressor_name", compname);
  } else {
    is_none_ = false;
    micro_block_size_ = micro_block_size;
  }
  return ret;
}

int ObMicroBlockCompressor::compress(const char* in, const int64_t in_size, const char*& out, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (is_none_) {
    out = in;
    out_size = in_size;
  } else if (OB_FAIL(compressor_->get_max_overflow_size(in_size, max_overflow_size))) {
    STORAGE_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(in_size));
  } else {
    int64_t comp_size = 0;
    int64_t max_comp_size = max_overflow_size + in_size;
    int64_t need_size = std::max(max_comp_size, micro_block_size_ * 2);
    if (OB_SUCCESS != (ret = comp_buf_.ensure_space(need_size))) {
      STORAGE_LOG(WARN, "macro block writer fail to allocate memory for comp_buf_.", K(ret), K(need_size));
    } else if (OB_SUCCESS != (ret = compressor_->compress(in, in_size, comp_buf_.data(), max_comp_size, comp_size))) {
      STORAGE_LOG(WARN,
          "compressor fail to compress.",
          K(in),
          K(in_size),
          "comp_ptr",
          comp_buf_.data(),
          K(max_comp_size),
          K(comp_size));
    } else if (comp_size >= in_size) {
      STORAGE_LOG(INFO, "compressed_size is larger than origin_size", K(comp_size), K(in_size));
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

int ObMicroBlockCompressor::decompress(
    const char* in, const int64_t in_size, const int64_t uncomp_size, const char*& out, int64_t& out_size)
{
  int ret = OB_SUCCESS;
  int64_t decomp_size = 0;
  decomp_buf_.reuse();
  if (is_none_ || in_size == uncomp_size) {
    out = in;
    out_size = in_size;
  } else if (OB_FAIL(decomp_buf_.ensure_space(uncomp_size))) {
    STORAGE_LOG(WARN, "failed to ensure decomp space", K(ret), K(uncomp_size));
  } else if (OB_FAIL(compressor_->decompress(in, in_size, decomp_buf_.data(), uncomp_size, decomp_size))) {
    STORAGE_LOG(WARN, "failed to decompress data", K(ret), K(in_size), K(uncomp_size));
  } else {
    out = decomp_buf_.data();
    out_size = decomp_size;
  }
  return ret;
}

/**
 * -------------------------------------------------------------------ObMicroBlockDesc-------------------------------------------------------------------
 */
bool ObMicroBlockDesc::is_valid() const
{
  return !last_rowkey_.empty() && NULL != buf_ && buf_size_ > 0 && data_size_ > 0 && row_count_ > 0 &&
         column_count_ > 0 && max_merged_trans_version_ >= 0;
}

void ObMicroBlockDesc::reset()
{
  last_rowkey_.reset();
  buf_ = NULL;
  buf_size_ = 0;
  data_size_ = 0;
  row_count_ = 0;
  column_count_ = 0;
  row_count_delta_ = 0;
  can_mark_deletion_ = false;
  column_checksums_ = NULL;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
}

/**
 * -------------------------------------------------------------------ObMacroBlock-------------------------------------------------------------------
 */
ObMacroBlock::ObMacroBlock()
    : spec_(NULL),
      row_reader_(NULL),
      data_(0, "MacrBlocData"),
      index_(),
      header_(NULL),
      column_ids_(NULL),
      column_types_(NULL),
      column_orders_(NULL),
      column_checksum_(NULL),
      data_base_offset_(0),
      row_(),
      is_dirty_(false),
      is_multi_version_(false),
      macro_block_deletion_flag_(true),
      delta_(0),
      need_calc_column_checksum_(false),
      common_header_(),
      max_merged_trans_version_(0),
      contain_uncommitted_row_(false),
      pg_guard_(),
      pg_file_(NULL)
{
  row_.cells_ = rowkey_objs_;
  row_.count_ = 0;
}

ObMacroBlock::~ObMacroBlock()
{}

int ObMacroBlock::init_row_reader(const ObRowStoreType row_store_type)
{
  int ret = OB_SUCCESS;
  if (FLAT_ROW_STORE == row_store_type) {
    row_reader_ = &flat_row_reader_;
  } else if (SPARSE_ROW_STORE == row_store_type) {
    row_reader_ = &sparse_row_reader_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row_store_type));
  }
  return ret;
}

int ObMacroBlock::init(ObDataStoreDesc& spec)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(data_.ensure_space(spec.macro_block_size_))) {
    STORAGE_LOG(WARN, "macro block fail to ensure space for data.", K(ret), "macro_block_size", spec.macro_block_size_);
  } else if (OB_FAIL(index_.init(spec.macro_block_size_, spec.is_multi_version_minor_sstable()))) {
    STORAGE_LOG(
        WARN, "macro block fail to ensure space for index.", K(ret), "macro_block_size", spec.macro_block_size_);
  } else if (OB_FAIL(reserve_header(spec))) {
    STORAGE_LOG(WARN, "macro block fail to reserve header.", K(ret));
  } else if (OB_ISNULL(pg_file_ = spec.file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to get pg file", K(ret), K(spec.file_handle_));
  } else if (OB_FAIL(init_row_reader(spec.row_store_type_))) {
    STORAGE_LOG(WARN, "macro block fail to init row reader.", K(ret));
  } else {
    is_multi_version_ = spec.is_multi_version_minor_sstable();
    need_calc_column_checksum_ = spec.need_calc_column_checksum_;
    spec_ = &spec;
  }
  return ret;
}

int ObMacroBlock::write_micro_record_header(const ObMicroBlockDesc& micro_block_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(micro_block_desc));
  } else {
    const char* buf = micro_block_desc.buf_;
    const int64_t size = micro_block_desc.buf_size_;
    const int64_t data_size = micro_block_desc.data_size_;
    const int64_t header_version =
        spec_->store_micro_block_column_checksum_ ? RECORD_HEADER_VERSION_V3 : RECORD_HEADER_VERSION_V2;
    ObRecordHeaderV3 record_header;
    int64_t pos = 0;
    record_header.magic_ = MICRO_BLOCK_HEADER_MAGIC;
    record_header.header_length_ =
        RECORD_HEADER_VERSION_V2 == header_version
            ? static_cast<int8_t>(ObRecordHeaderV3::get_serialize_size(header_version, 0 /*column cnt*/))
            : static_cast<int8_t>(sizeof(ObRecordHeaderV3));
    record_header.version_ = static_cast<int8_t>(header_version);
    record_header.header_checksum_ = 0;
    record_header.reserved16_ = 0;
    record_header.data_length_ = data_size;
    record_header.data_zlength_ = size;
    record_header.data_checksum_ = ob_crc64_sse42(0, buf, size);
    record_header.data_encoding_length_ = 0;
    record_header.row_count_ = static_cast<int32_t>(micro_block_desc.row_count_);
    record_header.column_cnt_ = static_cast<uint16_t>(micro_block_desc.column_count_);
    record_header.column_checksums_ = micro_block_desc.column_checksums_;
    record_header.set_header_checksum();
    header_->data_checksum_ =
        ob_crc64_sse42(header_->data_checksum_, &record_header.data_checksum_, sizeof(record_header.data_checksum_));
    if (OB_FAIL(record_header.serialize(data_.current(), get_remain_size(), pos))) {
      STORAGE_LOG(WARN, "fail to serialize record header", K(ret));
    }

    if (OB_SUCC(ret)) {
      const int64_t header_size = ObRecordHeaderV3::get_serialize_size(header_version, micro_block_desc.column_count_);
      if (OB_FAIL(data_.advance(header_size))) {
        STORAGE_LOG(WARN, "fail to advance record header", K(ret));
      }
    }
  }
  return ret;
}

int ObMacroBlock::write_micro_block(const ObMicroBlockDesc& micro_block_desc, int64_t& data_offset)
{
  int ret = OB_SUCCESS;
  data_offset = data_.length() - data_base_offset_;
  if (!micro_block_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(micro_block_desc), K(ret));
  } else {
    const int64_t entry_size = ObMicroBlockIndexWriter::get_entry_size(is_multi_version_);
    const int64_t header_version =
        spec_->store_micro_block_column_checksum_ ? RECORD_HEADER_VERSION_V3 : RECORD_HEADER_VERSION_V2;
    const int64_t record_header_size =
        ObRecordHeaderV3::get_serialize_size(header_version, micro_block_desc.column_count_);
    if (micro_block_desc.buf_size_ + entry_size + record_header_size + micro_block_desc.last_rowkey_.length() >
        get_remain_size()) {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  if (OB_SUCC(ret)) {
    const char* buf = micro_block_desc.buf_;
    const int64_t size = micro_block_desc.buf_size_;

    if (OB_FAIL(index_.add_entry(micro_block_desc.last_rowkey_,
            data_offset,
            micro_block_desc.can_mark_deletion_,
            micro_block_desc.row_count_delta_))) {
      STORAGE_LOG(WARN,
          "index add entry failed",
          K(ret),
          "last_rowkey",
          micro_block_desc.last_rowkey_,
          K(data_offset),
          K(micro_block_desc.can_mark_deletion_),
          K(micro_block_desc.row_count_delta_));
    } else if (OB_FAIL(write_micro_record_header(micro_block_desc))) {
      STORAGE_LOG(WARN, "fail to write micro record header", K(ret), K(micro_block_desc));
    } else {
      is_dirty_ = true;
      MEMCPY(data_.current(), buf, static_cast<size_t>(size));
      if (OB_FAIL(data_.advance(size))) {
        STORAGE_LOG(WARN, "data advance failed", K(ret), K(size));
      }
    }
    if (OB_SUCC(ret)) {
      delta_ += micro_block_desc.row_count_delta_;
      ++header_->micro_block_count_;
      header_->row_count_ += static_cast<int32_t>(micro_block_desc.row_count_);
      header_->occupy_size_ = static_cast<int32_t>(get_data_size());
      // update info from micro_block
      update_max_merged_trans_version(micro_block_desc.max_merged_trans_version_);
      if (micro_block_desc.contain_uncommitted_row_) {
        set_contain_uncommitted_row();
      }
      if (macro_block_deletion_flag_) {
        macro_block_deletion_flag_ = micro_block_desc.can_mark_deletion_;
      }
      if (need_calc_column_checksum_) {
        if (OB_FAIL(add_column_checksum(
                micro_block_desc.column_checksums_, micro_block_desc.column_count_, column_checksum_))) {
          STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMacroBlock::flush(
    const int64_t cur_macro_seq, ObMacroBlockHandle& macro_handle, ObMacroBlocksWriteCtx& block_write_ctx)
{
  int ret = OB_SUCCESS;
  ObFullMacroBlockMeta full_meta;
  ObMacroBlockMetaV2 macro_block_meta;
  ObMacroBlockSchemaInfo macro_schema;
  full_meta.meta_ = &macro_block_meta;
  full_meta.schema_ = &macro_schema;

#ifdef ERRSIM
  ret = E(EventTable::EN_BAD_BLOCK_ERROR) OB_SUCCESS;
  if (OB_CHECKSUM_ERROR == ret) {  // obtest will set this code
    STORAGE_LOG(INFO, "ERRSIM bad block: Insert a bad block.");
    header_->magic_ = 0;
    header_->data_checksum_ = 0;
  }
#endif

  if (OB_FAIL(build_header(cur_macro_seq))) {
    STORAGE_LOG(WARN, "Fail to build header, ", K(ret), K(cur_macro_seq));
  } else if (OB_FAIL(build_macro_meta(full_meta))) {
    STORAGE_LOG(WARN, "Fail to build macro block meta, ", K(ret));
  } else if (OB_FAIL(build_index())) {
    STORAGE_LOG(WARN, "Fail to build index, ", K(ret));
  } else if (spec_->need_calc_physical_checksum_) {
    const int64_t common_header_size = common_header_.get_serialize_size();
    const char* payload_buf = data_.data() + common_header_size;
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
    write_info.size_ = data_.capacity();
    write_info.meta_ = full_meta;
    write_info.io_desc_.category_ = SYS_IO;
    write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_WRITE;
    write_info.block_write_ctx_ = &block_write_ctx;
    macro_handle.set_file(pg_file_);
    if (OB_ISNULL(pg_file_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "pg_file is null", K(ret), K(pg_file_));
    } else if (OB_FAIL(pg_file_->async_write_block(write_info, macro_handle))) {
      STORAGE_LOG(WARN, "Fail to async write block, ", K(ret));
    } else {
      if (NULL != spec_ && NULL != spec_->merge_info_) {
        spec_->merge_info_->macro_block_count_++;
        spec_->merge_info_->occupy_size_ += macro_block_meta.occupy_size_;
        if (macro_block_meta.is_sstable_data_block()) {
          spec_->merge_info_->total_row_count_ += macro_block_meta.row_count_;
        }
      }

      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO,
          "macro block writer succeed to flush macro block.",
          "block_id",
          macro_handle.get_macro_id(),
          K(*header_),
          K(full_meta),
          KP(&macro_handle));
    }
  }
  return ret;
}

int ObMacroBlock::merge(const ObMacroBlock& macro_block)
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_observer_version = GET_MIN_CLUSTER_VERSION();
  int64_t prev_data_offset = data_.length() - data_base_offset_;

  if (get_remain_size() < macro_block.get_raw_data_size()) {
    STORAGE_LOG(WARN,
        "current macro block raw size out of remain buffer.",
        "remain_size",
        get_remain_size(),
        "raw_data_size",
        macro_block.get_raw_data_size());
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(index_.merge(prev_data_offset, macro_block.index_))) {
    STORAGE_LOG(WARN, "current macro block index data out of index buffer.", K(ret));
  } else if (OB_FAIL(data_.write(macro_block.get_micro_block_data_ptr(), macro_block.get_micro_block_data_size()))) {
    STORAGE_LOG(WARN,
        "macro block fail to writer micro block data.",
        K(ret),
        "buf",
        OB_P(macro_block.get_micro_block_data_ptr()),
        "size",
        macro_block.get_micro_block_data_size());
  } else {
    header_->row_count_ += macro_block.header_->row_count_;
    header_->occupy_size_ = static_cast<int32_t>(get_data_size());
    header_->micro_block_count_ += macro_block.header_->micro_block_count_;
    delta_ = is_multi_version_ ? (macro_block.delta_ + delta_) : 0;
    macro_block_deletion_flag_ =
        is_multi_version_ && macro_block_deletion_flag_ && macro_block.macro_block_deletion_flag_;
    max_merged_trans_version_ = std::max(max_merged_trans_version_, macro_block.max_merged_trans_version_);
    contain_uncommitted_row_ = contain_uncommitted_row_ || macro_block.contain_uncommitted_row_;

    if (cluster_observer_version < CLUSTER_VERSION_140) {
      header_->data_checksum_ = ob_crc64_sse42(
          header_->data_checksum_, &macro_block.header_->data_checksum_, sizeof(header_->data_checksum_));
    } else {
      if (OB_FAIL(merge_data_checksum(macro_block))) {
        STORAGE_LOG(WARN, "failed to merge data checksum", K(ret), K(cluster_observer_version));
      }
      if (OB_SUCC(ret) && need_calc_column_checksum_) {
        if (OB_FAIL(add_column_checksum(macro_block.column_checksum_, header_->column_count_, column_checksum_))) {
          STORAGE_LOG(WARN, "fail to add column checksum", K(ret));
        }
      }
    }
  }

  return ret;
}

bool ObMacroBlock::can_merge(const ObMacroBlock& macro_block)
{
  return is_dirty_ && macro_block.is_dirty_ && get_remain_size() > macro_block.get_raw_data_size();
}

void ObMacroBlock::reset()
{
  data_.reuse();
  index_.reset();
  header_ = NULL;
  column_ids_ = NULL;
  column_types_ = NULL;
  column_orders_ = NULL;
  column_checksum_ = NULL;
  data_base_offset_ = 0;
  is_dirty_ = false;
  delta_ = 0;
  row_reader_ = NULL;
  macro_block_deletion_flag_ = true;
  need_calc_column_checksum_ = false;
  max_merged_trans_version_ = 0;
  contain_uncommitted_row_ = false;
  pg_guard_.reset();
  pg_file_ = NULL;
}

int ObMacroBlock::reserve_header(const ObDataStoreDesc& spec)
{
  int ret = OB_SUCCESS;
  common_header_.reset();
  common_header_.set_attr(ObMacroBlockCommonHeader::SSTableData);
  common_header_.set_data_version(spec.data_version_);
  common_header_.set_reserved(0);
  const int64_t common_header_size = common_header_.get_serialize_size();

  MEMSET(data_.data(), 0, data_.capacity());
  if (OB_FAIL(data_.advance(common_header_size))) {
    STORAGE_LOG(WARN, "data buffer is not enough for common header.", K(ret), K(common_header_size));
  }

  if (OB_SUCC(ret)) {
    int64_t column_count = spec.row_column_count_;
    int64_t rowkey_column_count = spec.rowkey_column_count_;
    int64_t column_checksum_size = sizeof(int64_t) * column_count;
    int64_t column_id_size = sizeof(uint16_t) * column_count;
    int64_t column_type_size = sizeof(ObObjMeta) * column_count;
    int64_t column_order_size = sizeof(ObOrderType) * column_count;
    int64_t macro_block_header_size = sizeof(ObSSTableMacroBlockHeader);
    header_ = reinterpret_cast<ObSSTableMacroBlockHeader*>(data_.current());
    column_ids_ = reinterpret_cast<uint16_t*>(data_.current() + macro_block_header_size);
    column_types_ = reinterpret_cast<ObObjMeta*>(data_.current() + macro_block_header_size + column_id_size);
    column_orders_ =
        reinterpret_cast<ObOrderType*>(data_.current() + macro_block_header_size + column_id_size + column_type_size);
    column_checksum_ = reinterpret_cast<int64_t*>(
        data_.current() + macro_block_header_size + column_id_size + column_type_size + column_order_size);
    macro_block_header_size += column_checksum_size + column_id_size + column_type_size + column_order_size;
    // for compatibility, fill 0 to checksum and this will be serialized to disk
    for (int i = 0; i < column_count; i++) {
      column_checksum_[i] = 0;
    }

    if (OB_FAIL(data_.advance(macro_block_header_size))) {
      STORAGE_LOG(WARN, "macro_block_header_size out of data buffer.", K(ret));
    } else {
      memset(header_, 0, macro_block_header_size);
      header_->header_size_ = static_cast<int32_t>(macro_block_header_size);
      header_->version_ = SSTABLE_MACRO_BLOCK_HEADER_VERSION_v3;
      header_->magic_ = SSTABLE_DATA_HEADER_MAGIC;
      header_->attr_ = 0;
      header_->table_id_ = spec.table_id_;
      header_->data_version_ = spec.data_version_;
      header_->column_count_ = static_cast<int32_t>(column_count);
      header_->rowkey_column_count_ = static_cast<int32_t>(rowkey_column_count);
      header_->column_index_scale_ = static_cast<int32_t>(spec.column_index_scale_);
      header_->row_store_type_ = static_cast<int32_t>(spec.row_store_type_);
      header_->micro_block_size_ = static_cast<int32_t>(spec.micro_block_size_);
      header_->micro_block_data_offset_ = header_->header_size_ + static_cast<int32_t>(common_header_size);
      memset(header_->compressor_name_, 0, OB_MAX_HEADER_COMPRESSOR_NAME_LENGTH);
      MEMCPY(header_->compressor_name_, spec.compressor_name_, strlen(spec.compressor_name_));
      header_->data_seq_ = 0;
      header_->partition_id_ = spec.partition_id_;
      // copy column id & type array;
      for (int64_t i = 0; i < header_->column_count_; ++i) {
        column_ids_[i] = static_cast<int16_t>(spec.column_ids_[i]);
        column_types_[i] = spec.column_types_[i];
        column_orders_[i] = spec.column_orders_[i];
      }
    }
  }
  if (OB_SUCC(ret)) {
    data_base_offset_ = header_->header_size_ + common_header_size;
  }
  return ret;
}

int ObMacroBlock::build_header(const int64_t cur_macro_seq)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(header_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The header is NULL, ", K(ret), KP_(header));
  } else {
    int64_t data_length = data_.length();
    int64_t index_length = index_.get_index().length();
    int64_t endkey_length = index_.get_data().length();
    header_->micro_block_data_size_ = static_cast<int32_t>(data_length - data_base_offset_);
    header_->micro_block_index_offset_ = static_cast<int32_t>(data_length);
    header_->micro_block_index_size_ = static_cast<int32_t>(index_length + ObMicroBlockIndexWriter::INDEX_ENTRY_SIZE);
    header_->micro_block_endkey_offset_ = static_cast<int32_t>(data_length + header_->micro_block_index_size_);
    header_->micro_block_endkey_size_ = static_cast<int32_t>(endkey_length);
    header_->data_seq_ = cur_macro_seq;
  }
  return ret;
}

int ObMacroBlock::build_index()
{
  int ret = OB_SUCCESS;
  if (index_.get_block_size() > data_.remain()) {
    STORAGE_LOG(WARN,
        "micro block index size is larger than macro data remain size.",
        "index_size",
        index_.get_block_size(),
        "data_remain_size",
        data_.remain());
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(index_.add_last_entry(header_->micro_block_data_size_))) {
    STORAGE_LOG(WARN,
        "micro block index fail to add last entry",
        K(ret),
        "micro_block_data_size",
        header_->micro_block_data_size_);
  } else if (OB_FAIL(data_.write(index_.get_index().data(), index_.get_index().length()))) {
    STORAGE_LOG(WARN,
        "macro block fail to copy index.",
        K(ret),
        "index_ptr",
        OB_P(index_.get_index().data()),
        "index_length",
        index_.get_index().length());
  } else if (OB_FAIL(data_.write(index_.get_data().data(), index_.get_data().length()))) {
    STORAGE_LOG(WARN,
        "macro block fail to copy endkey.",
        K(ret),
        "endkey_ptr",
        OB_P(index_.get_data().data()),
        "endkey_length",
        index_.get_data().length());
  } else if (is_multi_version_) {
    if (OB_FAIL(data_.write(index_.get_mark_deletion().data(), index_.get_mark_deletion().length()))) {
      STORAGE_LOG(WARN,
          "macro block fail to copy mark deletion.",
          K(ret),
          "mark_deletion_ptr",
          OB_P(index_.get_mark_deletion().data()),
          "mark_deletion_length",
          index_.get_mark_deletion().length());
    } else if (OB_FAIL(data_.write(index_.get_delta().data(), index_.get_delta().length()))) {
      STORAGE_LOG(WARN,
          "macro block fail to copy delta",
          K(ret),
          "delta ptr",
          OB_P(index_.get_delta().data()),
          "delta size",
          index_.get_delta().length());
    }
  }
  return ret;
}

int ObMacroBlock::build_macro_meta(ObFullMacroBlockMeta& full_meta)
{
  int ret = OB_SUCCESS;
  ObString s_rowkey;
  int64_t pos = 0;
  if (OB_ISNULL(row_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "row reader is null", K(ret));
  } else if (OB_UNLIKELY(nullptr == full_meta.meta_ || nullptr == full_meta.schema_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(full_meta));
  } else if (OB_FAIL(index_.get_last_rowkey(s_rowkey))) {
    STORAGE_LOG(WARN, "micro block index writer fail to get last rowkey", K(ret));
  } else if (OB_FAIL(row_reader_->read_compact_rowkey(
                 spec_->column_types_, spec_->rowkey_column_count_, s_rowkey.ptr(), s_rowkey.length(), pos, row_))) {
    STORAGE_LOG(WARN, "row reader fail to read row.", K(ret));
  } else {
    ObMacroBlockMetaV2& mbi = const_cast<ObMacroBlockMetaV2&>(*full_meta.meta_);
    ObMacroBlockSchemaInfo& schema = const_cast<ObMacroBlockSchemaInfo&>(*full_meta.schema_);
    mbi.endkey_ = row_.cells_;
    mbi.snapshot_version_ = spec_->snapshot_version_;
    mbi.attr_ = ObMacroBlockCommonHeader::SSTableData;
    mbi.create_timestamp_ = 0;
    mbi.data_version_ = header_->data_version_;
    mbi.column_number_ = static_cast<int16_t>(header_->column_count_);
    mbi.rowkey_column_number_ = static_cast<int16_t>(header_->rowkey_column_count_);
    mbi.column_index_scale_ = static_cast<int16_t>(header_->column_index_scale_);
    mbi.row_store_type_ = static_cast<int16_t>(header_->row_store_type_);
    mbi.row_count_ = header_->row_count_;
    mbi.occupy_size_ = header_->occupy_size_;
    mbi.column_checksum_ = column_checksum_;

    mbi.data_checksum_ = header_->data_checksum_;
    mbi.micro_block_count_ = header_->micro_block_count_;
    mbi.micro_block_data_offset_ = header_->micro_block_data_offset_;
    mbi.micro_block_index_offset_ = header_->micro_block_index_offset_;
    mbi.micro_block_endkey_offset_ = header_->micro_block_endkey_offset_;
    mbi.table_id_ = header_->table_id_;
    mbi.data_seq_ = header_->data_seq_;
    mbi.schema_version_ = spec_->schema_version_;
    mbi.schema_rowkey_col_cnt_ = static_cast<int16_t>(spec_->schema_rowkey_col_cnt_);
    mbi.row_count_delta_ = is_multi_version_ ? delta_ : 0;
    mbi.micro_block_mark_deletion_offset_ =
        is_multi_version_ ? header_->micro_block_endkey_offset_ + header_->micro_block_endkey_size_ : 0;
    mbi.macro_block_deletion_flag_ = is_multi_version_ && macro_block_deletion_flag_;
    mbi.micro_block_delta_offset_ = is_multi_version_ ? mbi.micro_block_mark_deletion_offset_ +
                                                            static_cast<int32_t>(index_.get_mark_deletion().length())
                                                      : 0;
    mbi.partition_id_ = header_->partition_id_;
    mbi.column_checksum_method_ = spec_->need_calc_column_checksum_ ? CCM_VALUE_ONLY : CCM_UNKOWN;
    mbi.progressive_merge_round_ = spec_->progressive_merge_round_;
    mbi.max_merged_trans_version_ = max_merged_trans_version_;
    mbi.contain_uncommitted_row_ = contain_uncommitted_row_;

    schema.column_number_ = static_cast<int16_t>(header_->column_count_);
    schema.rowkey_column_number_ = static_cast<int16_t>(header_->rowkey_column_count_);
    schema.schema_rowkey_col_cnt_ = static_cast<int16_t>(spec_->schema_rowkey_col_cnt_);
    schema.schema_version_ = spec_->schema_version_;
    schema.compressor_ = header_->compressor_name_;
    schema.column_id_array_ = column_ids_;
    schema.column_type_array_ = column_types_;
    schema.column_order_array_ = column_orders_;

    if (!mbi.is_valid() || !schema.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "build incorrect meta", K(full_meta), K(*header_), K(mbi.is_valid()), K(schema.is_valid()));
    }
  }
  return ret;
}

int ObMacroBlock::merge_data_checksum(const ObMacroBlock& macro_block)
{
  int ret = OB_SUCCESS;
  int64_t micro_block_count = 0;
  const char* buf_ptr = macro_block.get_micro_block_data_ptr();
  const int64_t buf_size = macro_block.get_micro_block_data_size();
  int64_t offset = 0;

  if (NULL == macro_block.header_) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "macro block header must not null", K(ret));
  } else {
    micro_block_count = macro_block.header_->micro_block_count_;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < micro_block_count; ++i) {
    ObRecordHeaderV3 header;
    int64_t pos = 0;
    if (OB_FAIL(header.deserialize(buf_ptr + offset, buf_size, pos))) {
      STORAGE_LOG(WARN, "fail to deserialize record header", K(ret));
    } else {
      offset += header.get_serialize_size() + header.data_zlength_;
      header_->data_checksum_ =
          ob_crc64_sse42(header_->data_checksum_, &header.data_checksum_, sizeof(header.data_checksum_));
      if (OB_FAIL(header.check_header_checksum())) {
        STORAGE_LOG(WARN, "failed to check header checksum", K(ret), K(i), K(i), KP(buf_ptr), K(offset), K(buf_size));
      }
    }
  }

  return ret;
}

int ObMacroBlock::add_column_checksum(
    const int64_t* to_add_checksum, const int64_t column_cnt, int64_t* column_checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == to_add_checksum || column_cnt <= 0 || NULL == column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(to_add_checksum), K(column_cnt), KP(column_checksum));
  } else {
    for (int64_t i = 0; i < column_cnt; ++i) {
      column_checksum[i] += to_add_checksum[i];
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
