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
#include "ob_csv_table_row_iter.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

ObCSVTableRowIterator::~ObCSVTableRowIterator()
{
  release_buf();
  if (nullptr != bit_vector_cache_) {
    malloc_alloc_.free(bit_vector_cache_);
  }
}

void ObCSVTableRowIterator::release_buf()
{
  if (nullptr != state_.buf_) {
    malloc_alloc_.free(state_.buf_);
  }

  if (nullptr != state_.escape_buf_) {
    malloc_alloc_.free(state_.escape_buf_);
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
    if (file_reader_.get_storage_type() != OB_STORAGE_FILE) {
      //for better performance
      new_buf_len = OB_MALLOC_BIG_BLOCK_SIZE;
    } else {
      new_buf_len = OB_MALLOC_NORMAL_BLOCK_SIZE;
    }
  }

  if (OB_UNLIKELY(new_buf_len > MAX_BUFFER_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size overflow", K(ret), K(new_buf_len));
  } else if (OB_ISNULL(new_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))
             || OB_ISNULL(new_escape_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (OB_NOT_NULL(new_buf)) {
      malloc_alloc_.free(new_buf);
    }
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

int ObCSVTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else {
    // state_.cur_file_id_ = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID;
    // state_.cur_line_number_ = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER;
    malloc_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "CSVRowIter"));
    arena_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "CSVRowIter"));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (parser_.init(scan_param->external_file_format_.csv_format_));
    OZ (file_reader_.init(scan_param_->external_file_location_, scan_param->external_file_access_info_,
                          scan_param_->external_file_format_.csv_format_.compression_algorithm_, malloc_alloc_));
    OZ (expand_buf());

    if (OB_SUCC(ret)) {
      if (file_reader_.get_storage_type() == OB_STORAGE_FILE) {
        if (OB_ISNULL(state_.ip_port_buf_ = static_cast<char *>(arena_alloc_.alloc(max_ipv6_port_length)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        }
      }
    }
  }
  for (int i = 0; i < scan_param_->key_ranges_.count(); ++i) {
    int64_t start = 0;
    int64_t step = 0;
    int64_t part_id = scan_param_->key_ranges_.at(i).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
    const ObString &file_url = scan_param_->key_ranges_.at(i).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
    int64_t file_id = scan_param_->key_ranges_.at(i).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
  }
  return ret;
}

int ObCSVTableRowIterator::get_next_file_and_line_number(const int64_t task_idx,
                                                         ObString &file_url,
                                                         int64_t &file_id,
                                                         int64_t &part_id,
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
    part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
    file_url = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
    file_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
  }
  return ret;
}

int ObCSVTableRowIterator::open_next_file()
{
  int ret = OB_SUCCESS;
  ObString location = scan_param_->external_file_location_;
  int64_t file_size = 0;

  file_reader_.close();
  do {
    ObString file_url;
    int64_t file_id = 0;
    int64_t part_id = 0;
    int64_t start_line = 0;
    int64_t end_line = 0;
    int64_t task_idx = state_.file_idx_++;
    if (state_.file_idx_ == 1) {
      // Skip to handle first file
    } else {
      LOG_TRACE("print lastest csv file state infos", K(ret), K(state_));
      state_.duration_ = 0;
    }

    file_size = 0;
    url_.reuse();
    ret = get_next_file_and_line_number(task_idx, file_url, file_id, part_id, start_line, end_line);
    if (OB_FAIL(ret)) {
    } else if (part_id == 0) {
      //empty file do not belong to any partitions
    } else {
      if (part_id != state_.part_id_) {
        state_.part_id_ = part_id;
        OZ (calc_file_partition_list_value(part_id, arena_alloc_, state_.part_list_val_));
      }
    }
    if (OB_SUCC(ret)) {
      if (start_line == ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER && end_line == INT64_MAX) {
        state_.cur_file_name_ = file_url;
        state_.cur_file_id_ = file_id;
        state_.cur_line_number_ = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER;
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
      // skip empty file and non-exist file
      OZ (file_reader_.get_data_access_driver().get_file_size(url_.string(), file_size));
      state_.cur_file_size_ = file_size;
      if (OB_SUCC(ret) && file_reader_.get_storage_type() == OB_STORAGE_FILE) {
        ObSqlString full_name;
        if (state_.ip_port_len_ == 0) {
          OZ (GCONF.self_addr_.addr_to_buffer(state_.ip_port_buf_, max_ipv6_port_length, state_.ip_port_len_));
        }
        OZ (full_name.append(state_.ip_port_buf_, state_.ip_port_len_));
        OZ (full_name.append("%"));
        OZ (full_name.append(this->state_.cur_file_name_));
        OZ (ob_write_string(arena_alloc_, full_name.string(), state_.cur_file_url_));
      }
    }
    LOG_DEBUG("try next file", K(ret), K(url_), K(file_url), K(state_));
  } while (OB_SUCC(ret) && file_size <= 0);

  OZ(file_reader_.open(url_.ptr()));

  LOG_DEBUG("open external file", K(ret), K(url_), K(location));

  return ret;
}

int ObCSVTableRowIterator::load_next_buf()
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  do {
    char *next_load_pos = NULL;
    int64_t next_buf_len = 0;
    if (file_reader_.eof()) {
      if (OB_FAIL(open_next_file())) {
        //do not print log
      } else {
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
      // `read` may return read_size 0.
      // If we read a compressed empty file, we need to read it twice
      // to know that we have reached the end of the file. The first
      // time we read the original file data and decompress it, we get
      // 0 bytes, and the second time we read it to know that we have
      // reached the end of the file.
      int64_t start_read_time = ObTimeUtility::current_time();
      OZ (file_reader_.read(next_load_pos, next_buf_len, read_size));
      state_.duration_ += ObTimeUtility::current_time() - start_read_time;
      if (OB_SUCC(ret)) {
        state_.pos_ = state_.buf_;
        state_.data_end_ = next_load_pos + read_size;
      }
    }

  } while (OB_SUCC(ret) && read_size <= 0);
  return ret;
}

int ObCSVTableRowIterator::skip_lines()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  int64_t nrows = 0;
  struct Functor {
    int operator()(ObCSVGeneralParser::HandleOneLineParam param) {
      UNUSED(param);
      return OB_SUCCESS;
    }
  };
  struct Functor temp_handle;
  do {
    nrows = state_.skip_lines_;
    OZ (parser_.scan(state_.pos_, state_.data_end_, nrows, nullptr, nullptr,
                     temp_handle, error_msgs, file_reader_.eof()));
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
  struct Functor {
    Functor(ObCSVTableRowIterator *csv_iter,
            const ExprFixedArray &file_column_exprs,
            ObEvalCtx &eval_ctx,
            bool is_oracle_mode,
            int64_t &returned_row_cnt) :
            csv_iter_(csv_iter), file_column_exprs_(file_column_exprs), eval_ctx_(eval_ctx),
            is_oracle_mode_(is_oracle_mode), returned_row_cnt_(returned_row_cnt)
    {}
    ObCSVTableRowIterator *csv_iter_;
    const ExprFixedArray &file_column_exprs_;
    ObEvalCtx &eval_ctx_;
    bool is_oracle_mode_;
    int64_t &returned_row_cnt_;

    int operator()(ObCSVGeneralParser::HandleOneLineParam param) {
      int ret = OB_SUCCESS;
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
        ObDatum &datum = file_column_exprs_.at(i)->locate_datum_for_write(eval_ctx_);
        if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
          if (csv_iter_->file_reader_.get_storage_type() == OB_STORAGE_FILE) {
            datum.set_string(csv_iter_->state_.cur_file_url_.ptr(), csv_iter_->state_.cur_file_url_.length());
          } else {
            datum.set_string(csv_iter_->state_.cur_file_name_.ptr(), csv_iter_->state_.cur_file_name_.length());
          }
        } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_PARTITION_LIST_COL) {
          int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
          if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= csv_iter_->state_.part_list_val_.get_count())) {
            ret = OB_ERR_UNEXPECTED;
          } else if (csv_iter_->state_.part_list_val_.get_cell(loc_idx).is_null()) {
            datum.set_null();
          } else {
            CK (OB_NOT_NULL(datum.ptr_));
            OZ (datum.from_obj(csv_iter_->state_.part_list_val_.get_cell(loc_idx)));
          }
        } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
          int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
          if (OB_UNLIKELY(loc_idx < 0 || loc_idx > param.fields_.count())) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            if (param.fields_.at(loc_idx).is_null_ || (0 == param.fields_.at(loc_idx).len_ && is_oracle_mode_)) {
              datum.set_null();
            } else {
              datum.set_string(param.fields_.at(loc_idx).ptr_, param.fields_.at(loc_idx).len_);
            }
          }
        }
      }

      returned_row_cnt_++;
      return ret;
    }
  };

  struct Functor handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);

  int64_t nrows = 0;
  do {
    if (state_.skip_lines_ > 0) {
      OZ (skip_lines());
    }
    if (OB_SUCC(ret)) {
      nrows = MIN(1, state_.line_count_limit_);
      if (OB_UNLIKELY(0 == nrows)) {
        // if line_count_limit = 0, get next file.
      } else {
        ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                                  state_.escape_buf_, state_.escape_buf_end_,
                                                  handle_one_line, error_msgs, file_reader_.eof());
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
    if (OB_ISNULL(mem = malloc_alloc_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  struct Functor {
    Functor(ObCSVTableRowIterator *csv_iter,
            const ExprFixedArray &file_column_exprs,
            ObEvalCtx &eval_ctx,
            bool is_oracle_mode,
            int64_t &returned_row_cnt) :
            csv_iter_(csv_iter), file_column_exprs_(file_column_exprs), eval_ctx_(eval_ctx),
            is_oracle_mode_(is_oracle_mode), returned_row_cnt_(returned_row_cnt)
    {}
    ObCSVTableRowIterator *csv_iter_;
    const ExprFixedArray &file_column_exprs_;
    ObEvalCtx &eval_ctx_;
    bool is_oracle_mode_;
    int64_t &returned_row_cnt_;

    int operator()(ObCSVGeneralParser::HandleOneLineParam param) {
      int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      ObDatum *datums = file_column_exprs_.at(i)->locate_batch_datums(eval_ctx_);
      if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
        if (csv_iter_->file_reader_.get_storage_type() == OB_STORAGE_FILE) {
          datums[returned_row_cnt_].set_string(csv_iter_->state_.cur_file_url_.ptr(), csv_iter_->state_.cur_file_url_.length());
        } else {
          datums[returned_row_cnt_].set_string(csv_iter_->state_.cur_file_name_.ptr(), csv_iter_->state_.cur_file_name_.length());
        }
      } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= csv_iter_->state_.part_list_val_.get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loc idx is out of range", K(loc_idx), K(csv_iter_->state_.part_list_val_), K(csv_iter_->state_.part_id_), K(ret));
        } else {
          if (csv_iter_->state_.part_list_val_.get_cell(loc_idx).is_null()) {
            datums[returned_row_cnt_].set_null();
          } else {
            CK (OB_NOT_NULL(datums[returned_row_cnt_].ptr_));
            OZ (datums[returned_row_cnt_].from_obj(csv_iter_->state_.part_list_val_.get_cell(loc_idx)));
          }
        }
      } else if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx > param.fields_.count())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          if (param.fields_.at(loc_idx).is_null_ || (0 == param.fields_.at(loc_idx).len_ && is_oracle_mode_)) {
            datums[returned_row_cnt_].set_null();
          } else {
            datums[returned_row_cnt_].set_string(param.fields_.at(loc_idx).ptr_, param.fields_.at(loc_idx).len_);
          }
        }
      }
    }
    returned_row_cnt_++;
    return ret;
    }
  };
  if (state_.need_expand_buf_) {
    OZ (expand_buf());
    state_.need_expand_buf_ = false;
  }
  struct Functor handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);
  int64_t nrows = 0;
  if (OB_SUCC(ret)) {
    do {
      if (state_.skip_lines_ > 0) {
        OZ (skip_lines());
      }
      if (OB_SUCC(ret)) {
        nrows = MIN(batch_size, state_.line_count_limit_);
        if (OB_UNLIKELY(0 == nrows)) {
          // if line_count_limit = 0, get next file.
        } else {
          ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
                                          state_.escape_buf_, state_.escape_buf_end_, handle_one_line,
                                          error_msgs, file_reader_.eof());
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to scan csv", K(ret));
          } else if (OB_UNLIKELY(error_msgs.count() > 0)) {
            dump_error_log(error_msgs);
          }
        }
      }
      if (OB_SUCC(ret)
          && !file_reader_.eof()
          && returned_row_cnt >= 1
          && returned_row_cnt < capacity
          && state_.buf_len_ * 2 <= OB_MALLOC_BIG_BLOCK_SIZE) {
        state_.need_expand_buf_ = true;
      }
    } while (OB_SUCC(ret) && returned_row_cnt < 1 && OB_SUCC(load_next_buf()));
  }

  if (OB_ITER_END == ret && returned_row_cnt > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && returned_row_cnt > 0) {
    OZ(calc_exprs_for_rowid(returned_row_cnt, state_));
    state_.line_count_limit_ -= returned_row_cnt;
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


DEF_TO_STRING(ObCSVIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("ob_external_iterator_state");
  J_COLON();
  pos += ObExternalIteratorState::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(KP_(buf),
       K_(buf_len),
       KP_(pos),
       KP_(data_end),
       K_(skip_lines),
       K_(line_count_limit),
       K_(cur_file_name),
       K_(line_count_limit),
       K_(ip_port_len),
       K_(duration),
       K_(cur_file_size));
  J_OBJ_END();
  return pos;
}

}
}