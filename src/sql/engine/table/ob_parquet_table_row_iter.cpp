/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_parquet_table_row_iter.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include <parquet/api/reader.h>

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

void ObArrowMemPool::init(uint64_t tenant_id)
{
  mem_attr_ = ObMemAttr(tenant_id, "ArrowMemPool");
}

arrow::Status ObArrowMemPool::Allocate(int64_t size, uint8_t** out)
{
  int ret = OB_SUCCESS;
  arrow::Status status_ret = arrow::Status::OK();
  if (0 == size) {
    *out = NULL;
  } else {
    void *buf = ob_malloc_align(64, size, mem_attr_);
    if (OB_ISNULL(buf)) {
      status_ret = arrow::Status::Invalid("allocate memory failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(size), K(lbt()));
    } else {
      *out = static_cast<uint8_t*>(buf);
      total_alloc_size_ += size;
    }
  }
  LOG_DEBUG("ObArrowMemPool::Allocate", K(size), "stack", lbt());
  return status_ret;
}

arrow::Status ObArrowMemPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr)
{
  int ret = OB_SUCCESS;
  uint8_t* old = *ptr;
  arrow::Status status_ret = Allocate(new_size, ptr);
  if (arrow::Status::OK() == status_ret) {
    MEMCPY(ptr, old, old_size);
    Free(old, old_size);
  }
  LOG_DEBUG("ObArrowMemPool::Reallocate", K(old_size), K(new_size), "stack", lbt());
  return status_ret;
}

void ObArrowMemPool::Free(uint8_t* buffer, int64_t size) {
  int ret = OB_SUCCESS;
  ob_free_align(buffer);
  total_alloc_size_ -= size;
  LOG_DEBUG("ObArrowMemPool::Free", K(size), "stack", lbt());
}

void ObArrowMemPool::ReleaseUnused() {
  LOG_DEBUG("ObArrowMemPool::ReleaseUnused", "stack", lbt());
}

int64_t ObArrowMemPool::bytes_allocated() const {
  LOG_DEBUG("ObArrowMemPool::bytes_allocated", "stack", lbt());
  return total_alloc_size_;
}


int ObArrowFile::open()
{
  return file_reader_.open(file_name_);
}

arrow::Status ObArrowFile::Seek(int64_t position) {
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> ObArrowFile::Read(int64_t nbytes, void *out)
{
  int ret = OB_SUCCESS;
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;
  if (OB_FAIL(file_reader_.pread(out, nbytes, position_, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(nbytes));
    ret_code = arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "read file failed"));
  } else {
    position_ += read_size;
    ret_code = read_size;
  }
  LOG_DEBUG("Read(int64_t nbytes, void *out)", K(nbytes));
  return ret_code;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ObArrowFile::Read(int64_t nbytes)
{
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
  }
  LOG_DEBUG("ObArrowFile::Read(int64_t nbytes)", K(nbytes));
  return std::move(buffer);
}


arrow::Result<int64_t> ObArrowFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
  int ret = OB_SUCCESS;
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;

  if (OB_FAIL(file_reader_.pread(out, nbytes, position, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(position), K(nbytes));
    ret_code = arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "read at file failed"));
  } else {
    position_ = position + read_size;
    ret_code = read_size;
  }
  LOG_DEBUG("ObArrowFile::Read(int64_t nbytes)", K(nbytes));
  return ret_code;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ObArrowFile::ReadAt(int64_t position, int64_t nbytes)
{
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                        ReadAt(position, nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
    buffer->ZeroPadding();
  }
  LOG_DEBUG("ObArrowFile::ReadAt(int64_t position, int64_t nbytes)", K(nbytes));
  return std::move(buffer);
}


arrow::Result<int64_t> ObArrowFile::Tell() const
{
  return position_;
}

arrow::Result<int64_t> ObArrowFile::GetSize()
{
  int ret = OB_SUCCESS;
  arrow::Result<int64_t> ret_code;
  int64_t file_size = 0;
  if (OB_FAIL(file_reader_.get_file_size(file_name_, file_size))) {
    LOG_WARN("fail to get file size", K(ret), K(file_name_));
    ret_code = arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "get file size"));
  } else {
    ret_code = file_size;
  }
  return ret_code;
}

arrow::Status ObArrowFile::Close()
{
  file_reader_.close();
  return arrow::Status::OK();
}

bool ObArrowFile::closed() const
{
  return !file_reader_.is_opened();
}

bool mem_zero_detect(void *buf, size_t n)
{
  size_t size = n;
  if (size == 0)
      return true;
  uint8_t * ptr = (uint8_t *)buf;
  if (*ptr == 0 && memcmp(ptr, ptr + 1, size - 1) == 0)
    return true;
  return false;
}

ObParquetTableRowIterator::~ObParquetTableRowIterator()
{
  for (int i = 0; i < column_readers_.count(); i++) {
    column_readers_.at(i) = NULL;
  }
}
int ObParquetTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
  mem_attr_ = ObMemAttr(MTL_ID(), "ParquetRowIter");
  allocator_.set_attr(mem_attr_);
  arrow_alloc_.init(MTL_ID());
  OZ (ObExternalTableRowIterator::init(scan_param));
  OZ (data_access_driver_.init(scan_param->external_file_location_,
                               scan_param->external_file_access_info_));

  if (OB_SUCC(ret)) {
    ObArray<ObExpr*> file_column_exprs;
    ObArray<ObExpr*> file_meta_column_exprs;
    for (int i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
      ObExpr* ext_file_column_expr = scan_param->ext_file_column_exprs_->at(i);
      if (OB_ISNULL(ext_file_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ptr", K(ret));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL
                 || ext_file_column_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        OZ (file_meta_column_exprs.push_back(ext_file_column_expr));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        OZ (file_column_exprs.push_back(ext_file_column_expr));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", KPC(ext_file_column_expr));
      }
    }
    OZ (file_column_exprs_.assign(file_column_exprs));
    OZ (file_meta_column_exprs_.assign(file_meta_column_exprs));

    if (file_column_exprs_.count() > 0) {
      OZ (column_indexs_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (column_readers_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (load_funcs_.allocate_array(allocator_, file_column_exprs_.count()));
    }
    LOG_DEBUG("check exprs", K(file_column_exprs), K(file_meta_column_exprs), KPC(scan_param->ext_file_column_exprs_));
  }

  if (OB_SUCC(ret) && OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }

  if (OB_SUCC(ret)) {
    OZ (def_levels_buf_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ (rep_levels_buf_.allocate_array(allocator_, eval_ctx.max_batch_size_));
  }

  if (OB_SUCC(ret)) {
    OZ (file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ (file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
  }

  return ret;
}

int ObParquetTableRowIterator::next_file()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObString location = scan_param_->external_file_location_;
  int64_t task_idx = 0;
  int64_t file_size = 0;

  do {
    if ((task_idx = state_.file_idx_++) >= scan_param_->key_ranges_.count()) {
      ret = OB_ITER_END;
    } else {
      state_.cur_file_url_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
      url_.reuse();
      const char *split_char = "/";
      OZ (url_.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                        (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                                        state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
      OZ (data_access_driver_.get_file_size(url_.string(), file_size));

      if (OB_SUCC(ret)) {
        ObString expr_file_url;
        if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
          ObSqlString full_name;
          if (ip_port_.empty()) {
            OZ(gen_ip_port(allocator_));
          }
          OZ (full_name.append_fmt("%.*s%%%.*s", ip_port_.length(), ip_port_.ptr(),
                                   state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
          OZ (ob_write_string(allocator_, full_name.string(), expr_file_url));
        } else {
          expr_file_url = state_.cur_file_url_;
        }
        for (int i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
          file_url_ptrs_.at(i) = expr_file_url.ptr();
          file_url_lens_.at(i) = expr_file_url.length();
        }
      }

      LOG_DEBUG("current external file", K(url_), K(file_size));
    }
  } while (OB_SUCC(ret) && OB_UNLIKELY(0 >= file_size)); //skip not exist or empty file

  if (OB_SUCC(ret)) {

    int64_t part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
    if (part_id != 0 && state_.part_id_ != part_id) {
      state_.part_id_ = part_id;
      OZ (calc_file_partition_list_value(part_id, allocator_, state_.part_list_val_));
    }

    state_.cur_file_id_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
    OZ (ObExternalTableUtils::resolve_line_number_range(scan_param_->key_ranges_.at(task_idx),
                                                        ObExternalTableUtils::ROW_GROUP_NUMBER,
                                                        state_.cur_row_group_idx_,
                                                        state_.end_row_group_idx_));

    try {
      file_meta_.reset();
      file_reader_.reset();
      std::shared_ptr<ObArrowFile> cur_file =
          std::make_shared<ObArrowFile>(data_access_driver_, url_.ptr(), &arrow_alloc_);
      OZ (cur_file.get()->open());
      if (OB_SUCC(ret)) {
        file_reader_ = parquet::ParquetFileReader::Open(cur_file, read_props_);
        file_meta_ = file_reader_->metadata();
        state_.end_row_group_idx_ = std::min((int64_t)(file_meta_->num_row_groups()), state_.end_row_group_idx_);
      }
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        ObDataAccessPathExtraInfo *data_access_info =
            static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
        int column_index =
            file_meta_->schema()->ColumnIndex(std::string(data_access_info->data_access_path_.ptr(),
                                                          data_access_info->data_access_path_.length()));
        const parquet::ColumnDescriptor *col_desc = NULL;
        if (column_index < 0) {
          ret = OB_INVALID_EXTERNAL_FILE_COLUMN_PATH;
          LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE_COLUMN_PATH,
                         data_access_info->data_access_path_.length(),
                         data_access_info->data_access_path_.ptr());
        } else {
          col_desc = file_meta_->schema()->Column(column_index);
          load_funcs_.at(i) = DataLoader::select_load_function(file_column_exprs_.at(i)->datum_meta_, col_desc);
          if (OB_ISNULL(load_funcs_.at(i))
              || col_desc->max_repetition_level() != 0) {
            ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
            std::string p_type = col_desc->logical_type()->ToString();
            int64_t pos = 0;
            ObArrayWrap<char> buf;
            ObDatumMeta &meta = file_column_exprs_.at(i)->datum_meta_;
            const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
            if (OB_SUCCESS == buf.allocate_array(allocator_, 100)) {
              ob_sql_type_str(buf.get_data(), buf.count(), pos, meta.type_,
                              OB_MAX_VARCHAR_LENGTH, meta.precision_, meta.scale_, meta.cs_type_);
              if (pos < buf.count()) {
                buf.at(pos++) = '\0';
                ob_type = buf.get_data();
              }
            }
            LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_),
                     K(ObString(p_type.length(), p_type.data())), "rep_level", col_desc->max_repetition_level());
            LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, p_type.c_str(), ob_type);
          } else {
            column_indexs_.at(i) = column_index;
            LOG_DEBUG("mapped ob type", K(column_index), "column type",
                      file_meta_->schema()->Column(column_index)->physical_type(), "path",
                      data_access_info->data_access_path_);
          }
        }
      }
    } catch(const std::exception& e) {
      if (OB_SUCC(ret)) {
        //invalid file
        ret = OB_INVALID_EXTERNAL_FILE;
        LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());
        LOG_WARN("unexpected error", K(ret), "Info", e.what());
      }
    } catch(...) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      }
    }
  }

  return ret;
}

int ObParquetTableRowIterator::next_row_group()
{
  int ret = OB_SUCCESS;
  //init all meta
  if (OB_SUCC(ret) && state_.cur_row_group_idx_ > state_.end_row_group_idx_) {
    if (OB_FAIL(next_file())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next row group", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t cur_row_group = (state_.cur_row_group_idx_++) - 1;
    try {
      std::shared_ptr<parquet::RowGroupReader> rg_reader = file_reader_->RowGroup(cur_row_group);
      state_.cur_row_group_read_row_count_ = 0;
      state_.cur_row_group_row_count_ = file_meta_->RowGroup(cur_row_group)->num_rows();
      for (int i = 0; OB_SUCC(ret) && i < column_indexs_.count(); i++) {
        column_readers_.at(i) = rg_reader->Column(column_indexs_.at(i));
      }
    } catch(const std::exception& e) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index", K(ret), "Info", e.what(), K(cur_row_group), K(column_indexs_));
    } catch(...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index", K(ret), K(cur_row_group), K(column_indexs_));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_data_for_col(LOAD_FUNC &func)
{
  return (this->*func)();
}

ObParquetTableRowIterator::DataLoader::LOAD_FUNC ObParquetTableRowIterator::DataLoader::select_load_function(
    const ObDatumMeta &datum_type, const parquet::ColumnDescriptor *col_desc)
{
  LOAD_FUNC func = NULL;
  const parquet::LogicalType* log_type = col_desc->logical_type().get();
  parquet::Type::type phy_type = col_desc->physical_type();
  bool no_log_type = log_type->is_none();
  if ((no_log_type || log_type->is_int()) && ob_is_integer_type(datum_type.type_)) {
    //convert parquet int storing as int32/int64 to
    // ObTinyIntType/ObSmallIntType/ObMediumIntType/ObInt32Type/ObIntType using int64_t memory layout
    // ObUTinyIntType/ObUSmallIntType/ObUMediumIntType/ObUInt32Type/ObUInt64Type using uint64_t memory layout
    if (parquet::Type::INT64 == phy_type) {
      func = &DataLoader::load_int64_to_int64_vec;
    } else if (parquet::Type::INT32 == phy_type) {
      func = &DataLoader::load_int32_to_int64_vec;
    }
    //sign and width
    ObObj temp_obj;
    temp_obj.set_int(datum_type.type_, 0);
    if ((no_log_type || static_cast<const parquet::IntLogicalType*>(log_type)->is_signed()) != temp_obj.is_signed_integer()) {
      func = NULL;
    }
    if (no_log_type ? (temp_obj.get_tight_data_len() != (parquet::Type::INT32 == phy_type ? 4 : 8))
                    : static_cast<const parquet::IntLogicalType*>(log_type)->bit_width() > temp_obj.get_tight_data_len() * 8) {
      func = NULL;
    }
  } else if ((no_log_type || log_type->is_string() || log_type->is_enum())
             && (ob_is_string_type(datum_type.type_) || ob_is_enum_or_set_type(datum_type.type_))) {
    //convert parquet enum/string to enum/string vector
    if (parquet::Type::BYTE_ARRAY == phy_type) {
      func = &DataLoader::load_string_col;
    } else if (parquet::Type::FIXED_LEN_BYTE_ARRAY == phy_type) {
      func = &DataLoader::load_fixed_string_col;
    }
  } else if ((no_log_type || log_type->is_decimal() || log_type->is_int())
             && ob_is_number_or_decimal_int_tc(datum_type.type_)) {
    //convert parquet int storing as int32/int64 to number/decimal vector
    if (log_type->is_decimal() && (col_desc->type_precision() != datum_type.precision_
                                   || col_desc->type_scale() != datum_type.scale_)) {
      func = NULL;
    } else {
      //there is 4 kinds of physical format in parquet(int32/int64/fixedbytearray/bytearray)
      // and 2 class of types for OB vector(decimalint/number)
      if (parquet::Type::INT32 == phy_type && ob_is_decimal_int_tc(datum_type.type_)
          && DECIMAL_INT_32 == get_decimalint_type(datum_type.precision_)) {
        func = &DataLoader::load_int32_to_int32_vec;
      } else if (parquet::Type::INT64 == phy_type && ob_is_decimal_int_tc(datum_type.type_)
                 && DECIMAL_INT_64 == get_decimalint_type(datum_type.precision_)) {
        func = &DataLoader::load_int64_to_int64_vec;
      } else if (parquet::Type::INT32 == phy_type
                 || parquet::Type::INT64 == phy_type
                 || parquet::Type::BYTE_ARRAY == phy_type
                 || parquet::Type::FIXED_LEN_BYTE_ARRAY == phy_type) {
        func = &DataLoader::load_decimal_any_col;
      }
    }
  } else if ((no_log_type || log_type->is_date())
             && (ob_is_datetime(datum_type.type_) || ob_is_date_tc(datum_type.type_))) {
    if (parquet::Type::INT32 == phy_type && ob_is_date_tc(datum_type.type_)) {
      func = &DataLoader::load_int32_to_int32_vec;
    } else if (parquet::Type::INT32 == phy_type && ob_is_datetime(datum_type.type_)) {
      func = &DataLoader::load_date_col_to_datetime;
    }
  } else if (log_type->is_time() && ob_is_time_tc(datum_type.type_)) {
    switch (static_cast<const parquet::TimeLogicalType*>(log_type)->time_unit()) {
      case parquet::LogicalType::TimeUnit::unit::MILLIS: {
        if (parquet::Type::INT32 == phy_type) {
          func = &DataLoader::load_time_millis_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::MICROS: {
        if (parquet::Type::INT64 == phy_type) {
          func = &DataLoader::load_int64_to_int64_vec;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::NANOS: {
        if (parquet::Type::INT64 == phy_type) {
          func = &DataLoader::load_time_nanos_col;
        }
        break;
      }
      default: {
        func = NULL;
      }
    }
  } else if (log_type->is_timestamp() && parquet::Type::INT64 == phy_type
             && (ob_is_otimestamp_type(datum_type.type_) || ObTimestampType == datum_type.type_)) {
    switch (static_cast<const parquet::TimestampLogicalType*>(log_type)->time_unit()) {
      case parquet::LogicalType::TimeUnit::unit::MILLIS: {
        if (ObTimestampType == datum_type.type_
            || ObTimestampLTZType == datum_type.type_
            || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_millis_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::MICROS: {
        if (ObTimestampType == datum_type.type_ && is_parquet_store_utc(log_type)) {
          //mysql timestamp storing utc timestamp as int64 values
          func = &DataLoader::load_int64_to_int64_vec;
        } else if (ObTimestampType == datum_type.type_
                   || ObTimestampLTZType == datum_type.type_
                   || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_micros_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::NANOS: {
        if (ObTimestampType == datum_type.type_
            || ObTimestampLTZType == datum_type.type_
            || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_nanos_col;
        }
        break;
      }
      default: {
        func = NULL;
      }
    }
  } else if ((no_log_type || log_type->is_timestamp()) && parquet::Type::INT96 == phy_type
             && (ob_is_otimestamp_type(datum_type.type_) || ObTimestampType == datum_type.type_)) {
    func = &DataLoader::load_timestamp_hive;
  } else if (no_log_type && parquet::Type::FLOAT == phy_type && ObFloatType == datum_type.type_) {
    func = &DataLoader::load_float;
  } else if (no_log_type && parquet::Type::DOUBLE == phy_type && ObDoubleType == datum_type.type_) {
    func = &DataLoader::load_double;
  } else if (log_type->is_interval()
             || log_type->is_map()
             || log_type->is_list()
             || log_type->is_JSON()) {
    func = NULL;
  }
  return func;
}


#define IS_PARQUET_COL_NOT_NULL (0 == max_def_level)
#define IS_PARQUET_COL_VALUE_IS_NULL(V) (V < max_def_level)

int ObParquetTableRowIterator::DataLoader::load_int32_to_int32_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<int32_t*>(dec_vec->get_data()), values.get_data(), sizeof(int32_t) * row_count_);
    } else {
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          file_col_expr_->get_vector(eval_ctx_)->set_null(i);
        } else {
          dec_vec->set_int32(i, values.at(j++));
        }
      }
    }
  }
  return ret;
}

// convert int value to decimal int or number
int ObParquetTableRowIterator::DataLoader::to_numeric(const int64_t idx, const int64_t int_value)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    ObDecimalInt *decint = NULL;
    int32_t int_bytes = 0;
    if (OB_FAIL(wide::from_integer(int_value, tmp_alloc_g.get_allocator(), decint,
                                   int_bytes, file_col_expr_->datum_meta_.precision_))) {
      LOG_WARN("fail to from integer", K(ret));
    } else {
      vec->set_decimal_int(idx, decint, int_bytes);
    }
  } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
    ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(int_value, tmp_alloc_g.get_allocator()))) {
      LOG_WARN("fail to from number", K(ret));
    } else {
      vec->set_number(idx, res_nmb);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
  }
  return ret;
}

// convert string value to decimal int or number
int ObParquetTableRowIterator::DataLoader::to_numeric(
    const int64_t idx,
    const char *str,
    const int32_t length)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObDecimalInt *decint = NULL;
  int32_t val_len = 0;
  int16_t in_precision = 0;
  int16_t in_scale = 0;
  int16_t out_precision = file_col_expr_->datum_meta_.precision_;
  int16_t out_scale = file_col_expr_->datum_meta_.scale_;
  if (OB_FAIL(wide::from_string(str, length, tmp_alloc_g.get_allocator(), in_scale, in_precision, val_len, decint))) {
    LOG_WARN("fail to from number", K(ret), KPHEX(str, length));
  } else {
    if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
      if (in_precision > out_precision) {
        ret = OB_DECIMAL_PRECISION_OVERFLOW;
      } else {
        ObDecimalIntBuilder res_val;
        if (ObDatumCast::need_scale_decimalint(in_scale, in_precision, out_scale, out_precision)) {
          if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale,
                                                           out_precision, 0, res_val))) {
            LOG_WARN("scale decimal int failed", K(ret));
          } else {
            vec->set_decimal_int(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
          }
        } else {
          vec->set_decimal_int(idx, decint, val_len);
        }
      }
    } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
      ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
      number::ObNumber res_nmb;
      if (OB_FAIL(wide::to_number(decint, val_len, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
        LOG_WARN("fail to from", K(ret));
      } else {
        vec->set_number(idx, res_nmb);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
    }
  }

  return ret;
}

int ObParquetTableRowIterator::DataLoader::to_numeric_hive(
    const int64_t idx,
    const char *str,
    const int32_t length,
    char *buf,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObDecimalInt *decint = NULL;
  int32_t val_len = 0;

  if (OB_UNLIKELY(length > data_len)) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
    LOG_WARN("overflow", K(length), K(data_len));
  } else {
    //to little endian
    MEMSET(buf, (*str >> 8), data_len);
    int64_t pos = 0;
    int64_t temp_len = length;
    while (temp_len >= 8) {
      uint64_t temp_v = *(pointer_cast<const uint64_t*>(str + temp_len - 8));
      *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
      pos+=8;
      temp_len-=8;
    }
    if (temp_len > 0) {
      MEMCPY(buf + pos + 8 - temp_len, str, temp_len);
      uint64_t temp_v = *(pointer_cast<uint64_t*>(buf + pos));
      *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
    }
    decint = pointer_cast<ObDecimalInt *>(buf);
    val_len = static_cast<int32_t>(data_len);
    if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
      vec->set_decimal_int(idx, decint, val_len);
    } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
      number::ObNumber res_nmb;
      if (OB_FAIL(wide::to_number(decint, val_len, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
        LOG_WARN("fail to from", K(ret));
      } else {
        vec->set_number(idx, res_nmb);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
    }
  }
  return ret;
}

//convert int32/int64/string value(from parquet file) to decimal int or number(ob types)
int ObParquetTableRowIterator::DataLoader::load_decimal_any_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  //int16_t def_precision = static_cast<int16_t>(reader_->descr()->type_precision());
  //int16_t def_scale = static_cast<int16_t>(reader_->descr()->type_precision());

  if (reader_->descr()->physical_type() == parquet::Type::type::INT32) {
    ObArrayWrap<int32_t> values;
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        OZ (to_numeric(i, values.at(j++)));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::type::INT64) {
    ObArrayWrap<int64_t> values;
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        OZ (to_numeric(i, values.at(j++)));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::Type::FIXED_LEN_BYTE_ARRAY) {
    ObArrayWrap<parquet::FixedLenByteArray> values;
    int32_t fixed_length = reader_->descr()->type_length();
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(file_col_expr_->datum_meta_.precision_);
    ObArrayWrap<char> buffer;
    OZ (buffer.allocate_array(tmp_alloc_g.get_allocator(), int_bytes));
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    row_count_ = static_cast<parquet::FixedLenByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        parquet::FixedLenByteArray &cur_v = values.at(j++);
        OZ (to_numeric_hive(i, pointer_cast<const char*>(cur_v.ptr), fixed_length, buffer.get_data(), buffer.count()));
        //OZ (to_numeric(i, pointer_cast<const char*>(cur_v.ptr), fixed_length));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::Type::BYTE_ARRAY) {
    ObArrayWrap<parquet::ByteArray> values;
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(file_col_expr_->datum_meta_.precision_);
    ObArrayWrap<char> buffer;
    OZ (buffer.allocate_array(tmp_alloc_g.get_allocator(), int_bytes));
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    row_count_ = static_cast<parquet::ByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        parquet::ByteArray &cur_v = values.at(j++);
        OZ (to_numeric_hive(i, pointer_cast<const char*>(cur_v.ptr), cur_v.len, buffer.get_data(), buffer.count()));
        //OZ (to_numeric(i, pointer_cast<const char*>(cur_v.ptr), cur_v.len));
      }
    }
  }

  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_fixed_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<parquet::FixedLenByteArray> values;

  CK (VEC_DISCRETE == text_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    int32_t fixed_length = reader_->descr()->type_length();
    row_count_ = static_cast<parquet::FixedLenByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      bool is_byte_length = is_oracle_byte_length(
            lib::is_oracle_mode(), file_col_expr_->datum_meta_.length_semantics_);
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          text_vec->set_null(i);
        } else {
          parquet::FixedLenByteArray &cur_v = values.at(j++);
          text_vec->set_string(i, pointer_cast<const char *>(cur_v.ptr), fixed_length);
          if (OB_UNLIKELY(fixed_length > file_col_expr_->max_length_
                          && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                       pointer_cast<const char *>(cur_v.ptr),
                                                                       fixed_length) > file_col_expr_->max_length_))) {
            ret = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("data too long", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<parquet::ByteArray> values;

  CK (VEC_DISCRETE == text_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::ByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      bool is_oracle_mode = lib::is_oracle_mode();
      bool is_byte_length = is_oracle_byte_length(
            is_oracle_mode, file_col_expr_->datum_meta_.length_semantics_);
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          text_vec->set_null(i);
        } else {
          parquet::ByteArray &cur_v = values.at(j++);
          if (is_oracle_mode && 0 == cur_v.len) {
            text_vec->set_null(i);
          } else {
            text_vec->set_string(i, pointer_cast<const char *>(cur_v.ptr), cur_v.len);
            if (OB_UNLIKELY(cur_v.len > file_col_expr_->max_length_
                            && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                        pointer_cast<const char *>(cur_v.ptr),
                                                                        cur_v.len) > file_col_expr_->max_length_))) {
              ret = OB_ERR_DATA_TOO_LONG;
              LOG_WARN("data too long", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_int32_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<int32_t> values;

  CK (VEC_FIXED == int32_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int32_vec->set_null(i);
        } else {
          int32_vec->set_int(i, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_int64_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<int64_t> values;

  CK (VEC_FIXED == int64_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<int64_t*>(int64_vec->get_data()), values.get_data(), sizeof(int64_t) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int64_vec->set_null(i);
        } else {
          int64_vec->set_int(i, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_date_col_to_datetime()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        dec_vec->set_datetime(i, values.at(j++) * USECS_PER_DAY);
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_time_millis_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        dec_vec->set_time(i, values.at(j++) * USECS_PER_MSEC);
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_time_nanos_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        dec_vec->set_time(i, values.at(j++) / NSECS_PER_USEC);
      }
    }
  }
  return ret;
}

bool ObParquetTableRowIterator::DataLoader::is_parquet_store_utc(const parquet::LogicalType *logtype)
{
  return logtype->is_timestamp() ? static_cast<const parquet::TimestampLogicalType*>(logtype)->is_adjusted_to_utc() : true;
}

bool ObParquetTableRowIterator::DataLoader::is_ob_type_store_utc(const ObDatumMeta &meta)
{
  return (lib::is_mysql_mode() && ObTimestampType == meta.type_)
         || (lib::is_oracle_mode() && ObTimestampLTZType == meta.type_);
}

int64_t ObParquetTableRowIterator::DataLoader::calc_tz_adjust_us()
{
  int64_t res = 0;
  bool is_utc_src = is_parquet_store_utc(reader_->descr()->logical_type().get());
  bool is_utc_dst = is_ob_type_store_utc(file_col_expr_->datum_meta_);
  if (is_utc_src != is_utc_dst) {
    int32_t tmp_offset = 0;
    if (OB_NOT_NULL(eval_ctx_.exec_ctx_.get_my_session())
        && OB_NOT_NULL(eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info())
        && OB_SUCCESS == eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset)) {
      res = SEC_TO_USEC(tmp_offset) * (is_utc_src ? 1 : -1);
    }
  }
  return res;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_millis_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us();

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        int64_t adjusted_value = values.at(j++) * USECS_PER_MSEC + adjust_us;
        if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
          dec_vec->set_timestamp(i, adjusted_value);
        } else {
          ObOTimestampData data;
          data.time_us_ = adjusted_value;
          dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_micros_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us();
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        int64_t adjusted_value = (values.at(j++) + adjust_us);
        if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
          dec_vec->set_timestamp(i, adjusted_value);
        } else {
          ObOTimestampData data;
          data.time_us_ = adjusted_value;
          dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_nanos_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us();
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
          dec_vec->set_timestamp(i, values.at(j++) / NSECS_PER_USEC + adjust_us);
        } else {
          ObOTimestampData data;
          int64_t cur_value = values.at(j++);
          data.time_us_ = cur_value / NSECS_PER_USEC + adjust_us;
          data.time_ctx_.set_tail_nsec(cur_value % NSECS_PER_USEC);
          dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_hive()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<parquet::Int96> values;
  int64_t adjust_us = calc_tz_adjust_us();
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int96Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i);
      } else {
        parquet::Int96 &value = values.at(j++);
        uint64_t nsec_time_value = ((uint64_t)value.value[1] << 32) + (uint64_t)value.value[0];
        uint32_t julian_date_value = value.value[2];
        int64_t utc_timestamp =((int64_t)julian_date_value - 2440588LL) * 86400000000LL + (int64_t)(nsec_time_value / NSECS_PER_USEC);
        if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
          dec_vec->set_timestamp(i, utc_timestamp + adjust_us);
        } else {
          ObOTimestampData data;
          data.time_us_ = utc_timestamp + adjust_us;
          data.time_ctx_.set_tail_nsec((int32_t)(nsec_time_value % NSECS_PER_USEC));
          dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_float()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *float_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<float> values;

  CK (VEC_FIXED == float_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::FloatReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<float*>(float_vec->get_data()), values.get_data(), sizeof(float) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          float_vec->set_null(i);
        } else {
          float_vec->set_float(i, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_double()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<double> values;

  CK (VEC_FIXED == double_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::DoubleReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<double*>(double_vec->get_data()), values.get_data(), sizeof(double) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          double_vec->set_null(i);
        } else {
          double_vec->set_double(i, values.at(j++));
        }
      }
    }
  }
  return ret;
}

#undef IS_PARQUET_COL_NOT_NULL
#undef IS_PARQUET_COL_VALUE_IS_NULL

int ObParquetTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_convert_exprs_);
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);

  if (OB_SUCC(ret) && state_.cur_row_group_read_row_count_ >= state_.cur_row_group_row_count_) {
    if (OB_FAIL(next_row_group())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next row group", K(ret));
      }
    }
  }

  if (!file_column_exprs_.count()) {
    read_count = std::min(capacity, state_.cur_row_group_row_count_ - state_.cur_row_group_read_row_count_);
  } else {
    //load vec data from parquet file to file column expr
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      if (OB_UNLIKELY(!column_readers_.at(i).get()->HasNext())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("page end unexpected", K(ret));
      }
      if (OB_SUCC(ret)) {
        DataLoader loader(eval_ctx, file_column_exprs_.at(i), column_readers_.at(i).get(),
                          def_levels_buf_, rep_levels_buf_, capacity, read_count);
        OZ (file_column_exprs_.at(i)->init_vector_for_write(
              eval_ctx, file_column_exprs_.at(i)->get_default_res_format(), eval_ctx.max_batch_size_));
        OZ (loader.load_data_for_col(load_funcs_.at(i)));
        file_column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
  }
  if (OB_SUCC(ret) && read_count > 0) {
    //fill expr results from metadata
    for (int i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      ObExpr *meta_expr = file_meta_column_exprs_.at(i);
      if (meta_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
        StrDiscVec *text_vec = static_cast<StrDiscVec *>(meta_expr->get_vector(eval_ctx));
        OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, read_count));
        if (OB_SUCC(ret)) {
          text_vec->set_ptrs(file_url_ptrs_.get_data());
          text_vec->set_lens(file_url_lens_.get_data());
        }
      } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_UNIFORM, read_count));
        OZ (fill_file_partition_expr(meta_expr, state_.part_list_val_, read_count));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", KPC(meta_expr));
      }
      meta_expr->set_evaluated_projected(eval_ctx);
    }

    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      //column_conv_exprs is 1-1 mapped to column_exprs
      //calc gen column exprs
      if (!column_conv_exprs.at(i)->get_eval_info(eval_ctx).evaluated_) {
        OZ (column_conv_exprs.at(i)->init_vector_default(eval_ctx, read_count));
        OZ (column_conv_exprs.at(i)->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
        column_conv_exprs.at(i)->set_evaluated_projected(eval_ctx);
      }
      //assign gen column exprs value to column exprs(output exprs)
      if (OB_SUCC(ret)) {
        ObExpr *to = column_exprs_.at(i);
        ObExpr *from = column_conv_exprs.at(i);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < read_count && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx);
          if (src != dst) {
            MEMCPY(dst, src, read_count * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
        } else {
          to_vec_header = from_vec_header;
        }
        column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
    OZ (calc_exprs_for_rowid(read_count));
  }
  if (OB_SUCC(ret)) {
    state_.cur_row_group_read_row_count_ += read_count;
    count = read_count;
  }
  return ret;
}

int ObParquetTableRowIterator::get_next_row()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

void ObParquetTableRowIterator::reset() {
  // reset state_ to initial values for rescan
  state_.reuse();
}

int ObParquetTableRowIterator::calc_exprs_for_rowid(const int64_t read_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_id_expr_->get_vector(eval_ctx));
      vec->set_int(i, state_.cur_file_id_);
    }
    file_id_expr_->set_evaluated_flag(eval_ctx);
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    OZ (line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(line_number_expr_->get_vector(eval_ctx));
      vec->set_int(i, state_.cur_line_number_ + i);
    }
    line_number_expr_->set_evaluated_flag(eval_ctx);
  }
  state_.cur_line_number_ += read_count;
  return ret;
}


}
}
