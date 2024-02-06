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

#define USING_LOG_PREFIX SQL
#include "ob_external_table_access_service.h"

#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_device_manager.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql
{

ObExternalDataAccessDriver::~ObExternalDataAccessDriver() {
  close();
  if (OB_NOT_NULL(device_handle_)) {
    ObDeviceManager::get_instance().release_device(device_handle_);
  }
}

void ObExternalDataAccessDriver::close()
{
  if (OB_NOT_NULL(device_handle_) && fd_.is_valid()) {
    device_handle_->close(fd_);
    fd_.reset();
  }
}

bool ObExternalDataAccessDriver::is_opened()
{
  return fd_.is_valid();
}

int ObExternalDataAccessDriver::get_file_sizes(const ObString &location,
                                               const ObIArray<ObString> &urls,
                                               ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  file_sizes.reuse();
  ObString tmp_location(location.length(), location.ptr());
  for (int64_t i = 0; OB_SUCC(ret) && i < urls.count(); i++) {
    int64_t len = 0;
    ObSqlString path;
    if (OB_FAIL(path.append(tmp_location.trim())) ||
        (*(path.ptr() + path.length() - 1) != '/' && OB_FAIL(path.append("/"))) ||
        OB_FAIL(path.append(urls.at(i)))) {
      LOG_WARN("append string failed", K(ret));
    } else if (OB_FAIL(get_file_size(path.string(), len))) {
      LOG_WARN("get file size failed", K(ret));
    } else if (OB_FAIL(file_sizes.push_back(len))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}
int ObExternalDataAccessDriver::get_file_size(const ObString &url, int64_t &file_size)
{
  int ret = OB_SUCCESS;

  ObBackupIoAdapter util;

  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ObIODFileStat statbuf;
    int temp_ret = device_handle_->stat(to_cstring(url), statbuf);
    if (OB_SUCCESS != temp_ret) {
      file_size = -1;
      if (OB_BACKUP_FILE_NOT_EXIST == temp_ret) {
        file_size = -1;
      } else {
        ret = temp_ret;
      }
      LOG_WARN("fail to get file length", K(temp_ret), K(url));
    } else {
      file_size = statbuf.size_;
    }
  }
  return ret;
}

int ObExternalDataAccessDriver::open(const ObString &url)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = device_handle_->open(url.ptr(), -1, 0, fd_, &iod_opts_);
  }
  return ret;
}

int ObExternalDataAccessDriver::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
  } else {
    ret = device_handle_->pread(fd_, offset, count, buf, read_size);
  }
  return ret;
}

int ObExternalDataAccessDriver::get_file_list(const ObString &path,
                                              ObIArray<ObString> &file_urls,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_VISIT_COUNT = 100000;
  ObArray<ObString> file_dirs;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObExternalDataAccessDriver not init", K(ret));
  } else if (get_storage_type() == OB_STORAGE_OSS
             || get_storage_type() == OB_STORAGE_COS) {
    ObSEArray<ObString, 16> temp_file_urls;
    ObFileListArrayOp file_op(temp_file_urls, allocator);
    if (OB_FAIL(device_handle_->scan_dir(to_cstring(path), file_op))) {
      LOG_WARN("scan dir failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_file_urls.count(); i++) {
      if (temp_file_urls.at(i).length() <= 0) {
        //do nothing
      } else if ( '/' == *(temp_file_urls.at(i).ptr() + temp_file_urls.at(i).length() - 1)) {
        //is direcotry
      } else {
        OZ (file_urls.push_back(temp_file_urls.at(i)));
      }
    }
    LOG_DEBUG("show oss files", K(file_urls), K(file_dirs));
  } else if (get_storage_type() == OB_STORAGE_FILE) {
    OZ (file_dirs.push_back(path));
    for (int64_t i = 0; OB_SUCC(ret) && i < file_dirs.count(); i++) {
      ObString file_dir = file_dirs.at(i);
      ObFullPathArrayOp dir_op(file_dirs, file_dir, allocator);
      ObFullPathArrayOp file_op(file_urls, file_dir, allocator);
      dir_op.set_dir_flag();
      if (file_dir.case_compare(".") == 0
          || file_dir.case_compare("..") == 0) {
        //do nothing
      } else if (OB_FAIL(device_handle_->scan_dir(to_cstring(file_dir), file_op))) {
        LOG_WARN("scan dir failed", K(ret));
      } else if (OB_FAIL(device_handle_->scan_dir(to_cstring(file_dir), dir_op))) {
        LOG_WARN("scan dir failed", K(ret));
      } else if (file_dirs.count() + file_urls.count() > MAX_VISIT_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("too many files and dirs to visit", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
      if (file_urls.at(i).length() <= path.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid file url", K(ret), K(path), K(file_urls.at(i)));
      } else {
        file_urls.at(i) += path.length();
        if (OB_LIKELY(file_urls.at(i).length() > 0)) {
          if (OB_NOT_NULL(file_urls.at(i).ptr()) && *file_urls.at(i).ptr() == '/') {
            file_urls.at(i) += 1;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file name length is invalid", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExternalDataAccessDriver::resolve_storage_type(const ObString &location, ObStorageType &device_type)
{
  int ret = OB_SUCCESS;

  if (location.prefix_match_ci(OB_OSS_PREFIX)) {
    device_type = OB_STORAGE_OSS;
  } else if (location.prefix_match_ci(OB_COS_PREFIX)) {
    device_type = OB_STORAGE_COS;
  } else if (location.prefix_match_ci(OB_FILE_PREFIX)) {
    device_type = OB_STORAGE_FILE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Not supported location type", K(ret), K(location));
  }
  return ret;
}

int ObExternalDataAccessDriver::init(const ObString &location, const ObString &access_info)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;
  ObArenaAllocator temp_allocator;
  ObString location_cstr;
  ObString access_info_cstr;
  ObBackupIoAdapter util;

  iod_opts_.opts_ = opts_;
  iod_opts_.opt_cnt_ = 0;

  if (OB_FAIL(resolve_storage_type(location, device_type))) {
    LOG_WARN("fail to resove storage type", K(ret));
  } else {
    storage_type_ = device_type;
    if (device_type == OB_STORAGE_FILE) {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      access_info_cstr.assign_ptr(&dummy_empty_char, strlen(&dummy_empty_char));
    } else {
      OZ (ob_write_string(temp_allocator, location, location_cstr, true));
      OZ (ob_write_string(temp_allocator, access_info, access_info_cstr, true));
    }
  }

  OZ (access_info_.set(device_type, access_info_cstr.ptr()));
  OZ (util.get_and_init_device(device_handle_, &access_info_, location_cstr));
  OZ (util.set_access_type(&iod_opts_, false, 1));

  return ret;
}


int ObExternalTableAccessService::table_scan(
    ObVTableScanParam &param,
    ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObExternalTableRowIterator* row_iter = NULL;

  auto &scan_param = static_cast<storage::ObTableScanParam&>(param);

  switch (param.external_file_format_.format_type_) {
    case ObExternalFileFormat::CSV_FORMAT:
      if (OB_ISNULL(row_iter = OB_NEWx(ObCSVTableRowIterator, (scan_param.allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->init(&scan_param))) {
      LOG_WARN("fail to open iter", K(ret));
    } else {
      result = row_iter;
    }
  }

  LOG_DEBUG("external table access service iter init", K(ret), "type", param.external_file_format_.format_type_);

  return ret;
}

int ObExternalTableAccessService::table_rescan(ObVTableScanParam &param, ObNewRowIterator *result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    switch (param.external_file_format_.format_type_) {
      case ObExternalFileFormat::CSV_FORMAT:
        result->reset();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), "format", param.external_file_format_.format_type_);
    }
  }
  LOG_DEBUG("external table rescan", K(param.key_ranges_), K(param.range_array_pos_));
  return ret;
}

int ObExternalTableAccessService::reuse_scan_iter(const bool switch_param, ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  UNUSED(switch_param);
  iter->reset();
  return OB_SUCCESS;
}

int ObExternalTableAccessService::revert_scan_iter(ObNewRowIterator *iter)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected iter", K(ret));
  } else {
    iter->~ObNewRowIterator();
  }
  return ret;
}

ObCSVTableRowIterator::~ObCSVTableRowIterator()
{
  release_buf();
  if (nullptr != bit_vector_cache_) {
    allocator_.free(bit_vector_cache_);
  }
}

void ObCSVTableRowIterator::release_buf()
{
  if (nullptr != state_.buf_) {
    allocator_.free(state_.buf_);
  }

  if (nullptr != state_.escape_buf_) {
    allocator_.free(state_.escape_buf_);
  }
}

int ObCSVTableRowIterator::expand_buf()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUFFER_SIZE = (1 << 30); //MEMORY LIMIT 1G
  int64_t new_buf_len = 0;
  char *old_buf = state_.buf_;
  char *new_buf = nullptr;
  char *new_escape_buf = nullptr;

  if (nullptr != old_buf) {
    new_buf_len = state_.buf_len_ * 2;
  } else {
    if (data_access_driver_.get_storage_type() != OB_STORAGE_FILE) {
      //for better performance
      new_buf_len = OB_MALLOC_BIG_BLOCK_SIZE;
    } else {
      new_buf_len = OB_MALLOC_NORMAL_BLOCK_SIZE;
    }
  }

  if (OB_UNLIKELY(new_buf_len > MAX_BUFFER_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size overflow", K(ret), K(new_buf_len));
  } else if (OB_ISNULL(new_buf = static_cast<char *>(allocator_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(new_escape_buf = static_cast<char *>(allocator_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    int64_t remain_len =  (nullptr != old_buf) ? (state_.data_end_ - state_.pos_) : 0;
    if (remain_len > 0) {
      MEMCPY(new_buf, state_.pos_, remain_len);
    }

    release_buf();

    state_.buf_ = new_buf;
    state_.escape_buf_ = new_escape_buf;
    state_.escape_buf_end_ = new_escape_buf + new_buf_len;
    state_.buf_len_ = new_buf_len;
    state_.pos_ = new_buf;
    state_.data_end_ = new_buf + remain_len;


    LOG_DEBUG("extend buf", K(new_buf_len));
  }

  return ret;
}

int ObCSVTableRowIterator::init_exprs(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    if (scan_param->column_ids_.count() != scan_param->output_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column ids not equal to access expr", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < scan_param->column_ids_.count(); i++) {
      ObExpr *cur_expr = scan_param->output_exprs_->at(i);
      switch (scan_param->column_ids_.at(i)) {
        case OB_HIDDEN_LINE_NUMBER_COLUMN_ID:
          line_number_expr_ = cur_expr;
          break;
        case OB_HIDDEN_FILE_ID_COLUMN_ID:
          file_id_expr_ = cur_expr;
          break;
        default:
          OZ (column_exprs_.push_back(cur_expr));
          break;
      }
    }
    if (OB_SUCC(ret) && column_exprs_.count() != scan_param->ext_column_convert_exprs_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr not equal to convert convert expr", K(ret),
               K(column_exprs_), KPC(scan_param->ext_column_convert_exprs_));
    }
  }
  return ret;
}

int ObCSVTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    allocator_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "CSVRowIter"));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (parser_.init(scan_param->external_file_format_.csv_format_));
    OZ (init_exprs(scan_param));
    OZ (data_access_driver_.init(scan_param_->external_file_location_, scan_param->external_file_access_info_));
    OZ (expand_buf());
  }
  return ret;
}

int ObCSVTableRowIterator::get_next_file_and_line_number(const int64_t task_idx,
                                                         ObString &file_url,
                                                         int64_t &file_id,
                                                         int64_t &start_line,
                                                         int64_t &end_line)
{
  int ret = OB_SUCCESS;
  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(ObExternalTableUtils::resolve_line_number_range(
                                                              scan_param_->key_ranges_.at(task_idx),
                                                              ObExternalTableUtils::LINE_NUMBER,
                                                              start_line,
                                                              end_line))) {
    LOG_WARN("failed to resolve range in external table", K(ret));
  } else {
    file_url = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
    file_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
  }
  return ret;
}

int ObCSVTableRowIterator::open_next_file()
{
  int ret = OB_SUCCESS;
  ObString location = scan_param_->external_file_location_;

  if (data_access_driver_.is_opened()) {
    data_access_driver_.close();
  }

  do {
    ObString file_url;
    int64_t file_id = 0;
    int64_t start_line = 0;
    int64_t end_line = 0;
    int64_t task_idx = state_.file_idx_++;
    url_.reuse();
    ret = get_next_file_and_line_number(task_idx, file_url, file_id, start_line, end_line);
    if (OB_SUCC(ret)) {
      if (start_line == MIN_EXTERNAL_TABLE_LINE_NUMBER && end_line == INT64_MAX) {
        state_.cur_file_name_ = file_url;
        state_.cur_file_id_ = file_id;
        state_.cur_line_number_ = MIN_EXTERNAL_TABLE_LINE_NUMBER;
        state_.skip_lines_ = parser_.get_format().skip_header_lines_;
        state_.line_count_limit_ = INT64_MAX;
      } else {
        // [3, 7] --> skip = 2, cnt = 5
        state_.cur_file_name_ = file_url;
        state_.cur_file_id_ = file_id;
        state_.cur_line_number_ = start_line;
        state_.skip_lines_ = parser_.get_format().skip_header_lines_ + start_line - 1;
        state_.line_count_limit_ = end_line - start_line + 1;
      }
      const char *split_char = "/";
      OZ (url_.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                        (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                                        file_url.length(), file_url.ptr()));
      OZ (data_access_driver_.get_file_size(url_.string(), state_.file_size_));
    }
    LOG_DEBUG("try next file", K(ret), K(url_), K(file_url), K(state_));
  } while (OB_SUCC(ret) && 0 >= state_.file_size_); //skip empty file
  OZ (data_access_driver_.open(url_.string()), url_);

  LOG_DEBUG("open external file", K(ret), K(url_), K(state_.file_size_), K(location));

  return ret;
}

int ObCSVTableRowIterator::load_next_buf()
{
  int ret = OB_SUCCESS;
  do {
    char *next_load_pos = NULL;
    int64_t next_buf_len = 0;
    if (state_.is_end_file_) {
      if (OB_FAIL(open_next_file())) {
        //do not print log
      } else {
        state_.is_end_file_ = false;
        state_.file_offset_ = 0;
        next_load_pos = state_.buf_;
        next_buf_len = state_.buf_len_;
      }
    } else {
      //move unfinish tail in old buf to the front
      int64_t remain_bytes = state_.data_end_ - state_.pos_;
      if (remain_bytes > 0) {
        if (state_.pos_ > state_.buf_) {
          MEMMOVE(state_.buf_, state_.pos_, remain_bytes);
        } else {
          // buffer is not big enough to store data line
          OZ (expand_buf());
        }
      }
      next_load_pos = state_.buf_ + remain_bytes;
      next_buf_len = state_.buf_len_ - remain_bytes;
    }

    if (OB_SUCC(ret)) {
      int64_t read_size = 0;
      OZ (data_access_driver_.pread(next_load_pos, next_buf_len, state_.file_offset_, read_size));
      if (OB_SUCC(ret)) {
        state_.file_offset_ += read_size;
        state_.pos_ = state_.buf_;
        state_.data_end_ = next_load_pos + read_size;
        state_.is_end_file_ = (state_.file_offset_ >= state_.file_size_);
      }
    }

  } while (false);
  return ret;
}

int ObCSVTableRowIterator::skip_lines()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  int64_t nrows = 0;
  auto temp_handle = [](ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
    UNUSED(arr);
    return OB_SUCCESS;
  };
  do {
    nrows = state_.skip_lines_;
    OZ (parser_.scan(state_.pos_, state_.data_end_, nrows, nullptr, nullptr,
                     temp_handle, error_msgs, state_.is_end_file_));
    error_msgs.reuse();
    state_.skip_lines_ -= nrows;
  } while (OB_SUCC(ret) && state_.skip_lines_ > 0 && OB_SUCC(load_next_buf()));
  return ret;
}

void ObCSVTableRowIterator::dump_error_log(ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs)
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < error_msgs.count(); ++i) {
    LOG_WARN("parse row warning",
             "file_name", state_.cur_file_name_,
             "file_id", state_.cur_file_id_,
             "rownum", error_msgs.at(i).line_no + parser_.get_format().skip_header_lines_,
             "ret", common::ob_error_name(error_msgs.at(i).err_code));
  }
}

int ObCSVTableRowIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  bool is_oracle_mode = lib::is_oracle_mode();
  int64_t returned_row_cnt = 0; // rows count for scan output, exclude blank lines or skip header
  auto handle_one_line = [&file_column_exprs, &eval_ctx, &is_oracle_mode, &returned_row_cnt]
      (ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); ++i) {
      ObDatum &datum = file_column_exprs.at(i)->locate_datum_for_write(eval_ctx);
      int64_t loc_idx = file_column_exprs.at(i)->extra_ - 1;
      if (OB_UNLIKELY(loc_idx < 0 || loc_idx > arr.count())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (arr.at(loc_idx).is_null_|| (0 == arr.at(loc_idx).len_ && is_oracle_mode)) {
          datum.set_null();
        } else {
          datum.set_string(arr.at(loc_idx).ptr_, arr.at(loc_idx).len_);
        }
      }
    }

    returned_row_cnt++;
    return ret;
  };

  int64_t nrows = 0;
  do {
    if (state_.skip_lines_ > 0) {
      OZ (skip_lines());
    }
    if (OB_SUCC(ret)) {
      nrows = MIN(1, state_.line_count_limit_);
      if (OB_UNLIKELY(0 == nrows)) {
        // if line_count_limit = 0, get next file.
        state_.is_end_file_ = true;
      } else {
        ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                                  state_.escape_buf_, state_.escape_buf_end_,
                                                  handle_one_line, error_msgs, state_.is_end_file_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to scan csv", K(ret));
        } else if (OB_UNLIKELY(error_msgs.count() > 0)) {
          dump_error_log(error_msgs);
        }
      }
    }
  } while (OB_SUCC(ret) && returned_row_cnt < 1 && OB_SUCC(load_next_buf()));
  if (OB_SUCC(ret) && returned_row_cnt > 0) {
    if (OB_NOT_NULL(file_id_expr_)) {
      ObDatum &datum = file_id_expr_->locate_datum_for_write(eval_ctx);
      datum.set_int(state_.cur_file_id_);
    }
    if (OB_NOT_NULL(line_number_expr_)) {
      ObDatum &datum = line_number_expr_->locate_datum_for_write(eval_ctx);
      datum.set_int(state_.cur_line_number_);
    }
    state_.line_count_limit_ -= returned_row_cnt;
    state_.cur_line_number_ += returned_row_cnt;
  }

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
    file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
  }

  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
    ObExpr *column_expr = column_exprs_.at(i);
    ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
    ObDatum *convert_datum = NULL;
    OZ (column_convert_expr->eval(eval_ctx, convert_datum));
    if (OB_SUCC(ret)) {
      column_expr->locate_datum_for_write(eval_ctx) = *convert_datum;
      column_expr->set_evaluated_flag(eval_ctx);
    }
  }

  return ret;
}

int ObCSVTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  int64_t batch_size = capacity;
  int64_t returned_row_cnt = 0; // rows count for scan output, exclude blank lines or skip header
  bool is_oracle_mode = lib::is_oracle_mode();

  if (OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  auto handle_one_line = [&file_column_exprs, &eval_ctx, &is_oracle_mode, &returned_row_cnt]
      (ObIArray<ObCSVGeneralParser::FieldValue> &arr) -> int {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); ++i) {
      ObDatum *datums = file_column_exprs.at(i)->locate_batch_datums(eval_ctx);
      int64_t loc_idx = file_column_exprs.at(i)->extra_ - 1;
      if (OB_UNLIKELY(loc_idx < 0 || loc_idx > arr.count())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (arr.at(loc_idx).is_null_ || (0 == arr.at(loc_idx).len_ && is_oracle_mode)) {
          datums[returned_row_cnt].set_null();
        } else {
          datums[returned_row_cnt].set_string(arr.at(loc_idx).ptr_, arr.at(loc_idx).len_);
        }
      }
    }
    returned_row_cnt++;
    return ret;
  };

  int64_t nrows = 0;
  do {
    if (state_.skip_lines_ > 0) {
      OZ (skip_lines());
    }
    if (OB_SUCC(ret)) {
      nrows = MIN(batch_size, state_.line_count_limit_);
      if (OB_UNLIKELY(0 == nrows)) {
        // if line_count_limit = 0, get next file.
        state_.is_end_file_ = true;
      } else {
        ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                        state_.escape_buf_, state_.escape_buf_end_, handle_one_line,
                                        error_msgs, state_.is_end_file_);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to scan csv", K(ret));
        } else if (OB_UNLIKELY(error_msgs.count() > 0)) {
          dump_error_log(error_msgs);
        }
      }
    }
  } while (OB_SUCC(ret) && returned_row_cnt < 1 && OB_SUCC(load_next_buf()));

  if (OB_ITER_END == ret && returned_row_cnt > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && returned_row_cnt > 0) {
    if (OB_NOT_NULL(file_id_expr_)) {
      ObDatum *datums = file_id_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < returned_row_cnt; i++) {
        datums[i].set_int(state_.cur_file_id_);
      }
      file_id_expr_->set_evaluated_flag(eval_ctx);
    }
    if (OB_NOT_NULL(line_number_expr_)) {
      ObDatum *datums = line_number_expr_->locate_batch_datums(eval_ctx);
      for (int64_t i = 0; i < returned_row_cnt; i++) {
        datums[i].set_int(state_.cur_line_number_ + i);
      }
      line_number_expr_->set_evaluated_flag(eval_ctx);
    }
    state_.line_count_limit_ -= returned_row_cnt;
    state_.cur_line_number_ += returned_row_cnt;
  }

  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
    file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
  }

  for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
    ObExpr *column_expr = column_exprs_.at(i);
    ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
    OZ (column_convert_expr->eval_batch(eval_ctx, *bit_vector_cache_, returned_row_cnt));
    if (OB_SUCC(ret)) {
      MEMCPY(column_expr->locate_batch_datums(eval_ctx),
             column_convert_expr->locate_batch_datums(eval_ctx), sizeof(ObDatum) * returned_row_cnt);
      column_expr->set_evaluated_flag(eval_ctx);
    }
  }

  count = returned_row_cnt;

  return ret;
}

void ObCSVTableRowIterator::reset()
{
  // reset state_ to initial values for rescan
  state_.reuse();
}





}
}
