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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_select_into_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/charset/ob_charset_string_helper.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER(ObSelectIntoOpInput, task_id_, sqc_id_);
OB_SERIALIZE_MEMBER((ObSelectIntoSpec, ObOpSpec), into_type_, user_vars_, outfile_name_,
    field_str_, // FARM COMPAT WHITELIST FOR filed_str_: renamed
    line_str_, closed_cht_, is_optional_, select_exprs_, is_single_, max_file_size_,
    escaped_cht_, cs_type_, parallel_, file_partition_expr_, buffer_size_);


int ObSelectIntoOp::inner_open()
{
  int ret = OB_SUCCESS;
  file_name_ = MY_SPEC.outfile_name_;
  field_str_ = MY_SPEC.field_str_;
  line_str_ = MY_SPEC.line_str_;
  has_enclose_ = MY_SPEC.closed_cht_.get_val_len() > 0;
  char_enclose_ = has_enclose_ ? MY_SPEC.closed_cht_.get_char().ptr()[0] : 0;
  has_escape_ = MY_SPEC.escaped_cht_.get_val_len() > 0;
  char_escape_ = has_escape_ ? MY_SPEC.escaped_cht_.get_char().ptr()[0] : 0;
  ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(input_);
  int64_t row_count = 0;
  bool need_check = false;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  const ObItemType into_type = MY_SPEC.into_type_;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  } else if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.outfile_name_,
                                                 phy_plan_ctx->get_param_store(),
                                                 file_name_,
                                                 need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.field_str_,
                                                 phy_plan_ctx->get_param_store(),
                                                 field_str_,
                                                 need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.line_str_,
                                                 phy_plan_ctx->get_param_store(),
                                                 line_str_,
                                                 need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else if (OB_FAIL(prepare_escape_printer())) {
    LOG_WARN("failed to calc escape info", K(ret));
  } else if (OB_FAIL(check_has_lob_or_json())) {
    LOG_WARN("failed to check has lob", K(ret));
  } else {
    print_params_.tz_info_ = session->get_timezone_info();
    print_params_.use_memcpy_ = true;
    print_params_.binary_string_print_hex_ = lib::is_oracle_mode();
    print_params_.cs_type_ = MY_SPEC.cs_type_;
    // since we call get_next_row in inner_open, we have to set opened_ first in avoid to a infinite loop.
    opened_ = true;
    if (!lib::is_oracle_mode()) {
      if (OB_FAIL(session->get_sql_select_limit(top_limit_cnt_))) {
        LOG_WARN("fail tp get sql select limit", K(ret));
      }
    }
  }
  //create buffer
  if (OB_SUCC(ret)) {
    const int64_t buf_len = has_lob_ ? (5 * OB_MALLOC_BIG_BLOCK_SIZE) : OB_MALLOC_BIG_BLOCK_SIZE;
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate buffer", K(ret), K(buf_len));
    } else {
      data_writer_.init(buf, buf_len);
    }
    if (has_json_ && has_escape_) {
      const int64_t json_buf_len = OB_MALLOC_MIDDLE_BLOCK_SIZE;
      char *json_buf = NULL;
      if (OB_ISNULL(json_buf = static_cast<char*>(ctx_.get_allocator().alloc(json_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate buffer", K(ret), K(json_buf_len));
      } else {
        data_writer_.init_json_buf(json_buf, json_buf_len);
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObString path = file_name_.get_varchar().trim();
    ObSqlString file_name_with_suffix;
    ObString input_file_name;
    file_location_ = path.prefix_match_ci(OB_OSS_PREFIX)
                     ? IntoFileLocation::REMOTE_OSS
                     : IntoFileLocation::SERVER_DISK;
    if (T_INTO_OUTFILE == into_type && !MY_SPEC.is_single_ && OB_FAIL(calc_first_file_path(path))) {
      LOG_WARN("failed to calc first file path", K(ret));
    } else if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      ObString temp_url = path.split_on('?');
      temp_url.trim();
      ObString storage_info;
      if (OB_FAIL(ob_write_string(ctx_.get_allocator(), temp_url, url_, true))) {
        LOG_WARN("fail to append string", K(ret));
      } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, storage_info, true))) {
        LOG_WARN("fail to append string", K(ret));
      } else if (OB_FAIL(access_info_.set(url_.ptr(), storage_info.ptr()))) {
        LOG_WARN("fail to set access info", K(ret), K(path));
      }
      //init device handle
      if (OB_SUCC(ret)) {
        ObBackupIoAdapter util;
        if (url_.empty() || !access_info_.is_valid()) {
          ret = OB_FILE_NOT_EXIST;
          LOG_WARN("file path not exist", K(ret), K(url_), K(access_info_));
        } else if (OB_FAIL(util.get_and_init_device(device_handle_, &access_info_, url_))) {
          LOG_WARN("fail to init device", K(ret), K(url_), K(access_info_));
        }
      }
    } else { // IntoFileLocation::SERVER_DISK
      if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, url_, true))) {
        LOG_WARN("fail to write string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)
      && (T_INTO_OUTFILE == into_type || T_INTO_DUMPFILE == into_type)
      && IntoFileLocation::SERVER_DISK == file_location_) {
    ObString file_name = url_;
    ObString file_path = file_name.split_on(file_name.reverse_find('/'));
    char full_path_buf[PATH_MAX+1];
    char *actual_path = nullptr;
    ObSqlString sql_str;
    if (OB_FAIL(sql_str.append(file_path.empty() ? "." : file_path))) {
      LOG_WARN("fail to append string", K(ret));
    } else if (OB_ISNULL(actual_path = realpath(sql_str.ptr(), full_path_buf))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file not exist", K(ret), K(sql_str));
    }
    if (OB_SUCC(ret)) {
      ObString secure_file_priv;
      int64_t tenant_id = MTL_ID();
      if (OB_FAIL(ObSchemaUtils::get_tenant_varchar_variable(
              tenant_id,
              SYS_VAR_SECURE_FILE_PRIV,
              ctx_.get_allocator(),
              secure_file_priv))) {
        LOG_WARN("fail get tenant variable", K(tenant_id), K(secure_file_priv), K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, actual_path))) {
        LOG_WARN("failed to check secure path", K(ret), K(secure_file_priv));
        if (OB_ERR_NO_PRIVILEGE == ret) {
          ret = OB_ERR_NO_PRIV_DIRECT_PATH_ACCESS;
          LOG_ERROR("fail to check secure path", K(ret), K(secure_file_priv), K(session->get_is_deserialized()));
        }
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::inner_get_next_row()
{
  int ret = 0 == top_limit_cnt_ ? OB_ITER_END : OB_SUCCESS;
  int64_t row_count = 0;
  const ObItemType into_type = MY_SPEC.into_type_;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  } else if (T_INTO_OUTFILE == into_type && MY_SPEC.is_single_ && OB_FAIL(open_file())) {
    LOG_WARN("failed to open file", K(ret));
  }
  while (OB_SUCC(ret) && row_count < top_limit_cnt_) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    } else {
      ++row_count;
      if (T_INTO_VARIABLES == into_type) {
        if (OB_FAIL(into_varlist())) {
          LOG_WARN("into varlist failed", K(ret));
        }
      } else if (T_INTO_OUTFILE == into_type) {
        if (OB_FAIL(into_outfile())) {
          LOG_WARN("into outfile failed", K(ret));
        }
      } else {
        if (OB_FAIL(into_dumpfile())) {
          LOG_WARN("into dumpfile failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) { // if into user variables or into dumpfile, must be one row
      if ((T_INTO_VARIABLES == into_type || T_INTO_DUMPFILE == into_type) && row_count > 1) {
        ret = OB_ERR_TOO_MANY_ROWS;
        LOG_WARN("more than one row for into variables or into dumpfile", K(ret), K(row_count));
      }
    }
  } //end while
  if (OB_ITER_END == ret || OB_SUCC(ret)) { // set affected rows
    phy_plan_ctx->set_affected_rows(row_count);
  }
  return ret;
}

int ObSelectIntoOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = NULL;
  int64_t batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  int64_t row_count = 0;
  const ObItemType into_type = MY_SPEC.into_type_;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  } else if (T_INTO_OUTFILE == into_type && MY_SPEC.is_single_ && OB_FAIL(open_file())) {
    LOG_WARN("failed to open file", K(ret));
  }
  bool stop_loop = false;
  bool is_iter_end = false;
  if (0 == top_limit_cnt_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    stop_loop = true;
  }
  while (OB_SUCC(ret) && !stop_loop) {
    clear_evaluated_flag();
    int64_t rowkey_batch_size = min(batch_size, top_limit_cnt_ - row_count);
    if (OB_FAIL(child_->get_next_batch(rowkey_batch_size, child_brs))) {
      LOG_WARN("get next batch failed", K(ret));
    } else {
      brs_.size_ = child_brs->size_;
      brs_.end_ = child_brs->end_;
      is_iter_end = brs_.end_ && 0 == brs_.size_;
      if (brs_.size_ > 0) {
        brs_.skip_->deep_copy(*(child_brs->skip_), brs_.size_);
        row_count += brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_);
        if (T_INTO_OUTFILE == into_type) {
          if (OB_FAIL(into_outfile_batch(brs_))) {
            LOG_WARN("into outfile batch failed", K(ret));
          }
        } else {
          ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
          guard.set_batch_size(brs_.size_);
          for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
            if (brs_.skip_->contain(i)) {
              continue;
            }
            guard.set_batch_idx(i);
            if (T_INTO_VARIABLES == into_type) {
              if (OB_FAIL(into_varlist())) {
                LOG_WARN("into varlist failed", K(ret));
              }
            } else {
              if (OB_FAIL(into_dumpfile())) {
                LOG_WARN("into dumpfile failed", K(ret));
              }
            }
          }
        }
      }
    }
    if (is_iter_end || row_count >= top_limit_cnt_) {
      stop_loop = true;
    }
    if (OB_SUCC(ret) || is_iter_end) { // if into user variables or into dumpfile, must be one row
      if ((T_INTO_VARIABLES == into_type || T_INTO_DUMPFILE == into_type) && row_count > 1) {
        ret = OB_ERR_TOO_MANY_ROWS;
        LOG_WARN("more than one row for into variables or into dumpfile", K(ret), K(row_count));
      }
    }
  } //end while
  if (OB_SUCC(ret)) { // set affected rows
    phy_plan_ctx->set_affected_rows(row_count);
  }
  return ret;
}

int ObSelectIntoOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObSelectIntoOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (!has_lob_ && OB_FAIL(data_writer_.flush(get_flush_function()))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else if (has_lob_ && OB_FAIL(data_writer_.flush_all_for_lob(get_flush_function()))) {
    LOG_WARN("failed to flush buffer for lob", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::get_row_str(const int64_t buf_len,
                                bool is_first_row,
                                char *buf,
                                int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObObj &field_str = field_str_;
  char closed_cht = char_enclose_;
  bool is_optional = MY_SPEC.is_optional_;
  //before 4_1 use output
  //after 4_1 use select exprs
  const ObIArray<ObExpr*> &select_exprs = (MY_SPEC.select_exprs_.empty()) ?
                                           MY_SPEC.output_ : MY_SPEC.select_exprs_;
  if (!is_first_row && line_str_.is_varying_len_char_type()) { // lines terminated by "a"
    ret = databuff_printf(buf, buf_len, pos, "%.*s", line_str_.get_varchar().length(),
                         line_str_.get_varchar().ptr());
  }

  for (int i = 0 ; OB_SUCC(ret) && i < select_exprs.count() ; i++) {
    const ObExpr *expr = select_exprs.at(i);
    if (0 != closed_cht && (!is_optional || ob_is_string_type(expr->datum_meta_.type_))) {
      // closed by "a" (for all cell) or optionally by "a" (for string cell)
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c", closed_cht))) {
        LOG_WARN("print closed character failed", K(ret), K(closed_cht));
      }
    }
    if (OB_SUCC(ret)) {
      ObObj cell;
      ObDatum *datum = NULL;
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expr eval failed", K(ret));
      } else if (OB_FAIL(datum->to_obj(cell, expr->obj_meta_))) {
        LOG_WARN("to obj failed", K(ret));
      } else if (OB_FAIL(cell.print_plain_str_literal(buf, buf_len, pos))) { // cell value
        LOG_WARN("print sql failed", K(ret), K(cell));
      } else if (0 != closed_cht && (!is_optional || ob_is_string_type(expr->datum_meta_.type_))) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%c", closed_cht))) {
          LOG_WARN("print closed character failed", K(ret), K(closed_cht));
        }
      }
      // field terminated by "a"
      if (OB_SUCC(ret) && i != select_exprs.count() - 1 && field_str.is_varying_len_char_type()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s", field_str.get_varchar().length(), field_str.get_varchar().ptr()))) {
          LOG_WARN("print field str failed", K(ret), K(field_str));
        }
      }
    }
  }

  return ret;
}

int ObSelectIntoOp::open_file()
{
  int ret = OB_SUCCESS;
  if (IntoFileLocation::REMOTE_OSS == file_location_) {
    ObIODOpt opt;
    ObIODOpts iod_opts;
    opt.set("AccessType", "appender");
    iod_opts.opts_ = &opt;
    iod_opts.opt_cnt_ = 1;
    bool is_exist = false;
    if (OB_FAIL(device_handle_->exist(url_.ptr(), is_exist))) {
      LOG_WARN("failed to check file exist", K(ret), K(url_));
    } else if (is_exist) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("file already exist", K(ret), K(url_));
    } else if (OB_FAIL(device_handle_->open(url_.ptr(), -1, 0, fd_, &iod_opts))) {
      LOG_WARN("failed to open file", K(ret));
    } else {
      is_file_opened_ = true;
    }
  } else if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(file_appender_.create(url_, true))) {
      LOG_WARN("failed to create file", K(ret), K(url_));
    } else {
      is_file_opened_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error. invalid file location", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::calc_first_file_path(ObString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString file_name_with_suffix;
  ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(input_);
  ObString input_file_name = file_location_ == IntoFileLocation::REMOTE_OSS
                             ? path.split_on('?').trim()
                             : path;
  if (input_file_name.length() == 0 || path.length() == 0 || OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected path or input is null", K(ret));
  } else {
    if (input_file_name.ptr()[input_file_name.length() - 1] == '/'){
      file_name_with_suffix.append_fmt("%sdata", to_cstring(input_file_name));
    } else {
      file_name_with_suffix.append_fmt("%s", to_cstring(input_file_name));
    }
    if (MY_SPEC.parallel_ > 1) {
      file_name_with_suffix.append_fmt("_%ld_%ld_%ld", input->sqc_id_, input->task_id_, split_file_id_);
    } else {
      file_name_with_suffix.append_fmt("_%ld", split_file_id_);
    }
    if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      file_name_with_suffix.append_fmt("?%s", to_cstring(path));
    }
    if (OB_FAIL(ob_write_string(ctx_.get_allocator(), file_name_with_suffix.string(), path))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::calc_next_file_path()
{
  int ret = OB_SUCCESS;
  ObSqlString url_with_suffix;
  ObString file_path;
  if (split_file_id_ > 0) {
    if (MY_SPEC.is_single_ && IntoFileLocation::REMOTE_OSS == file_location_) {
      file_path = (split_file_id_ > 1) ? url_.split_on(url_.reverse_find('.')) : url_;
      if (OB_FAIL(url_with_suffix.assign(file_path))) {
        LOG_WARN("fail to assign string", K(ret));
      } else if (OB_FAIL(url_with_suffix.append_fmt(".extend%ld", split_file_id_))) {
        LOG_WARN("fail to append string", K(ret));
      }
    } else {
      file_path = url_.split_on(url_.reverse_find('_'));
      if (OB_FAIL(url_with_suffix.assign(file_path))) {
        LOG_WARN("fail to assign string", K(ret));
      } else if (OB_FAIL(url_with_suffix.append_fmt("_%ld", split_file_id_))) {
        LOG_WARN("fail to append string", K(ret));
      }
    }
    if (OB_SUCC(ret)
        && OB_FAIL(ob_write_string(ctx_.get_allocator(), url_with_suffix.string(), url_, true))) {
      LOG_WARN("fail to write string", K(ret));
    }
  }
  return ret;
}

void ObSelectIntoOp::close_file()
{
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    file_appender_.close();
  } else {
    if (fd_.is_valid()) {
      device_handle_->close(fd_);
      fd_.reset();
    }
  }
  is_file_opened_ = false;
}

std::function<int(const char *, int64_t)> ObSelectIntoOp::get_flush_function()
{
  return [this](const char *data, int64_t data_len) -> int
  {
    int ret = OB_SUCCESS;
    if (!is_file_opened_ && OB_FAIL(open_file())) {
      LOG_WARN("failed to open file", K(ret), K(url_));
    } else if (file_location_ == IntoFileLocation::SERVER_DISK) {
      if (OB_FAIL(file_appender_.append(data, data_len, false))) {
        LOG_WARN("failed to append file", K(ret), K(data_len));
      }
    } else if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      int64_t write_size = 0;
      int64_t begin_ts = ObTimeUtility::current_time();
      if (OB_FAIL(device_handle_->write(fd_, data, data_len, write_size))) {
        LOG_WARN("failed to write device", K(ret));
      } else if (OB_UNLIKELY(write_size != data_len)) {
        ret = OB_IO_ERROR;
        LOG_WARN("write size not equal super block size", K(ret), K(write_size), K(data_len));
      } else {
        write_offset_ += write_size;
        int64_t end_ts = ObTimeUtility::current_time();
        int64_t cost_time = end_ts - begin_ts;
        long double speed = (cost_time <= 0) ? 0 :
                        (long double) write_size * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
        long double total_write = (long double) write_offset_ / 1024.0 / 1024.0;
        _OB_LOG(TRACE, "write oss stat, time:%ld write_size:%ld speed:%.2Lf MB/s total_write:%.2Lf MB",
                cost_time, write_size, speed, total_write);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error. invalid file location", K(ret));
    }
    return ret;
  };
}

int ObSelectIntoOp::split_file()
{
  int ret = OB_SUCCESS;
  int64_t dummy_pos = 0;
  if (OB_FAIL(flush_buf(dummy_pos))) {
    LOG_WARN("fail to flush buffer", K(ret));
  } else {
    close_file();
  }

  //rename the first file name
  /* rename not support for current version
  if (OB_SUCC(ret) && 0 == split_file_id_) {
    ObSqlString url_old;
    ObSqlString url_new;

    if (OB_FAIL(url_old.assign(url_))) {
      LOG_WARN("fail to assign string", K(ret));
    } else if (OB_FAIL(url_new.assign(url_))) {
      LOG_WARN("fail to assign string", K(ret));
    } else if (OB_FAIL(url_new.append_fmt(".part%ld", split_file_id_))) {
      LOG_WARN("fail to append format", K(ret));
    } else if (OB_FAIL(device_handle_->rename(url_old.ptr(), url_new.ptr()))) {
      LOG_WARN("fail to rename", K(ret));
    }
  }
  */

  //create new file
  if (OB_SUCC(ret)) {
    split_file_id_++;
    if (OB_FAIL(calc_next_file_path())) {
      LOG_WARN("failed to calculate new file path", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::try_split_file()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_OSS_FILE_SIZE = 5LL * 1024 * 1024 * 1024; //5G
  int64_t curr_line_len = 0;
  int64_t curr_bytes = 0;
  bool has_split = false;
  if (!has_lob_ || data_writer_.get_curr_line_len() == 0) {
    curr_line_len = data_writer_.get_curr_pos() - data_writer_.get_last_line_pos();
  } else {
    curr_line_len = data_writer_.get_curr_pos() + data_writer_.get_curr_line_len();
  }
  curr_bytes = write_bytes_ + curr_line_len;
  if (!has_lob_ && data_writer_.get_last_line_pos() == 0) {
  } else if ((file_location_ == IntoFileLocation::SERVER_DISK
              && !MY_SPEC.is_single_ && curr_bytes > MY_SPEC.max_file_size_)
             || (file_location_ == IntoFileLocation::REMOTE_OSS
                 && ((!MY_SPEC.is_single_ && curr_bytes > min(MY_SPEC.max_file_size_, MAX_OSS_FILE_SIZE))
                     || (MY_SPEC.is_single_ && curr_bytes > MAX_OSS_FILE_SIZE)))) {
    if (OB_FAIL(split_file())) {
      LOG_WARN("failed to split file", K(ret));
    } else {
      has_split = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (!has_lob_) {
      write_bytes_ = has_split ? curr_line_len : curr_bytes;
    } else {
      write_bytes_ = has_split ? 0 : curr_bytes;
      data_writer_.reset_curr_line_len();
    }
    data_writer_.update_last_line_pos();
  }
  return ret;
}

void ObSelectIntoOp::get_buf(char* &buf, int64_t &buf_len, int64_t &pos, bool is_json)
{
  buf = is_json ? data_writer_.get_json_buf() : data_writer_.get_buf();
  buf_len = is_json ? data_writer_.get_json_buf_len() : data_writer_.get_buf_len();
  pos = is_json ? 0 : data_writer_.get_curr_pos();
}

int ObSelectIntoOp::flush_buf(int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!has_lob_ && OB_FAIL(data_writer_.flush(get_flush_function()))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else if (has_lob_ && OB_FAIL(data_writer_.flush_all_for_lob(get_flush_function()))) {
    LOG_WARN("failed to flush buffer for lob", K(ret));
  } else {
    pos = data_writer_.get_curr_pos();
  }
  return ret;
}

int ObSelectIntoOp::resize_buf(char* &buf, int64_t &buf_len, int64_t &pos, bool is_json)
{
  int ret = OB_SUCCESS;
  int64_t new_buf_len = buf_len * 2;
  char* new_buf = NULL;
  int curr_pos = data_writer_.get_curr_pos();
  if (OB_ISNULL(new_buf = static_cast<char*>(ctx_.get_allocator().alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(new_buf_len));
  } else if (!is_json) {
    data_writer_.init(new_buf, new_buf_len);
    if (curr_pos > 0) {
      MEMCPY(new_buf, buf, curr_pos);
    }
  } else {
    data_writer_.init_json_buf(new_buf, new_buf_len);
  }
  if (OB_SUCC(ret)) {
    get_buf(buf, buf_len, pos, is_json);
  }
  return ret;
}

int ObSelectIntoOp::write_obj_to_file(const ObObj &obj, bool need_escape)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool print_succ = false;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
  ObCharsetType dst_type = ObCharset::charset_type_by_coll(MY_SPEC.cs_type_);
  escape_printer_.do_encode_ = !(src_type == CHARSET_BINARY || src_type == dst_type
                                 || src_type == CHARSET_INVALID);
  escape_printer_.need_enclose_ = has_enclose_ && !obj.is_null()
                                  && (!MY_SPEC.is_optional_ || obj.is_string_type());
  escape_printer_.do_escape_ = need_escape;
  escape_printer_.print_hex_ = obj.get_collation_type() == CS_TYPE_BINARY
                               && print_params_.binary_string_print_hex_;
  ObString str_to_escape;

  if ((obj.is_string_type() || obj.is_json()) && need_escape) {
    if (obj.is_json()) {
      ObObj inrow_obj = obj;
      if (obj.is_lob_storage()) {
        ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
        common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
        if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(obj, inrow_obj, NULL, &temp_allocator))) {
          LOG_WARN("failed to convert outrow lobs", K(ret), K(obj));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(print_normal_obj_without_escape(inrow_obj, buf, buf_len, pos, true))) {
        LOG_WARN("failed to print normal obj without escape", K(ret));
      } else {
        str_to_escape.assign_ptr(buf, pos);
        escape_printer_.do_encode_ = false;
      }
    } else {
      str_to_escape = obj.get_varchar();
    }
    if (OB_SUCC(ret)) {
      get_buf(escape_printer_.buf_, escape_printer_.buf_len_, escape_printer_.pos_);
    }
    for (int i = 0; OB_SUCC(ret) && !print_succ; ++i) {
      if (OB_FAIL(ObFastStringScanner::foreach_char(str_to_escape,
                                                    src_type,
                                                    escape_printer_,
                                                    escape_printer_.do_encode_,
                                                    escape_printer_.ignore_convert_failed_))) {
        if (OB_SIZE_OVERFLOW == ret) {
          if (i == 0 && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = flush_buf(escape_printer_.pos_)))) {
            LOG_WARN("failed to flush buffer", K(tmp_ret), K(ret));
          } else if (i > 0 && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = resize_buf(
                                                                        escape_printer_.buf_,
                                                                        escape_printer_.buf_len_,
                                                                        escape_printer_.pos_)))) {
            LOG_WARN("failed to resize buffer", K(tmp_ret), K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_WARN("failed to print plain str", K(ret), K(src_type), K(escape_printer_.do_encode_));
        }
      } else {
        print_succ = true;
      }
    }
    if (OB_SUCC(ret)) {
      data_writer_.set_curr_pos(escape_printer_.pos_);
    }
  } else {
    if (OB_FAIL(print_normal_obj_without_escape(obj, buf, buf_len, pos))) {
      LOG_WARN("failed to print normal obj without escape", K(ret));
    } else {
      data_writer_.set_curr_pos(pos);
    }
  }
  return ret;
}

int ObSelectIntoOp::print_normal_obj_without_escape(const ObObj &obj,
                                                    char* &buf,
                                                    int64_t &buf_len,
                                                    int64_t &pos,
                                                    bool is_json)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool print_succ = false;
  get_buf(buf, buf_len, pos, is_json);
  for (int i = 0; OB_SUCC(ret) && !print_succ; ++i) {
    if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos, print_params_))) {
      if (OB_SIZE_OVERFLOW == ret) {
        if (i == 0 && !is_json && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = flush_buf(pos)))) {
          LOG_WARN("failed to flush buffer", K(tmp_ret), K(ret));
        } else if ((i > 0 || is_json)
                   && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = resize_buf(buf, buf_len, pos, is_json)))) {
          LOG_WARN("failed to resize buffer", K(tmp_ret), K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("failed to print plain str", K(ret));
      }
    } else {
      print_succ = true;
    }
  }
  return ret;
}

int ObSelectIntoOp::write_lob_to_file(const ObObj &obj, const ObExpr &expr, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
  ObCharsetType dst_type = ObCharset::charset_type_by_coll(MY_SPEC.cs_type_);
  escape_printer_.need_enclose_ = has_enclose_;
  escape_printer_.do_encode_ = !(src_type == CHARSET_BINARY || src_type == dst_type
                                 || src_type == CHARSET_INVALID);
  escape_printer_.do_escape_ = has_escape_;
  escape_printer_.print_hex_ = obj.get_collation_type() == CS_TYPE_BINARY
                               && print_params_.binary_string_print_hex_;
  get_buf(escape_printer_.buf_, escape_printer_.buf_len_, escape_printer_.pos_);

  ObDatumMeta input_meta = expr.datum_meta_;
  ObTextStringIterState state;
  ObString src_block_data;
  ObTextStringIter lob_iter(input_meta.type_, input_meta.cs_type_, datum.get_string(),
                            expr.obj_meta_.has_lob_header());
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  int64_t truncated_len = 0;
  bool stop_when_truncated = false;

  if (OB_FAIL(lob_iter.init(0, NULL, &temp_allocator))) {
    LOG_WARN("init lob_iter failed ", K(ret), K(lob_iter));
  }
  // 当truncated_len == src_block_data.length()时
  // 表明当前foreach_char处理的仅为lob末尾的无效的数据, 即上一轮的truncated data, 要避免死循环
  while (OB_SUCC(ret)
         && (state = lob_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
    // outrow lob最后一次才有可能为false, inrow lob只迭代一次, 为false
    stop_when_truncated = (truncated_len != src_block_data.length()) && lob_iter.is_outrow_lob();
    if ((escape_printer_.buf_len_ - escape_printer_.pos_) < (src_block_data.length() * 5)
        && OB_FAIL(flush_buf(escape_printer_.pos_))) {
      LOG_WARN("failed to flush buf", K(ret));
    } else if (OB_FAIL(ObFastStringScanner::foreach_char(src_block_data,
                                                         src_type,
                                                         escape_printer_,
                                                         escape_printer_.do_encode_,
                                                         escape_printer_.ignore_convert_failed_,
                                                         stop_when_truncated,
                                                         &truncated_len))) {
      if (OB_ERR_DATA_TRUNCATED == ret && stop_when_truncated) {
        lob_iter.set_reserved_byte_len(truncated_len);
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to print lob", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      data_writer_.set_curr_pos(escape_printer_.pos_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
    ret = (lob_iter.get_inner_ret() != OB_SUCCESS) ?
          lob_iter.get_inner_ret() : OB_INVALID_DATA;
    LOG_WARN("iter state invalid", K(ret), K(state), K(lob_iter));
  }
  return ret;
}

int ObSelectIntoOp::write_single_char_to_file(const char *wchar)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  get_buf(buf, buf_len, pos);
  if (pos == buf_len && OB_FAIL(flush_buf(pos))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else if (pos < buf_len) {
    MEMCPY(buf + pos, wchar, 1);
    data_writer_.set_curr_pos(pos + 1);
  } else if (OB_FAIL(resize_buf(buf, buf_len, pos))) {
    LOG_WARN("failed to resize buffer", K(ret));
  } else if (pos < buf_len) {
    MEMCPY(buf + pos, wchar, 1);
    data_writer_.set_curr_pos(pos + 1);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::print_lob_field(const ObObj &obj, const ObExpr &expr, const ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (has_enclose_) {
    OZ(write_single_char_to_file(&char_enclose_));
  }
  OZ(write_lob_to_file(obj, expr, datum));
  if (has_enclose_) {
    OZ(write_single_char_to_file(&char_enclose_));
  }
  return ret;
}

int ObSelectIntoOp::print_field(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  char char_n = 'N';
  const bool need_enclose = has_enclose_ && !obj.is_null()
                            && (!MY_SPEC.is_optional_ || obj.is_string_type());
  if (need_enclose) {
    OZ(write_single_char_to_file(&char_enclose_));
  }
  if (!has_escape_) {
    OZ(write_obj_to_file(obj, false));
  } else if (obj.is_null()) {
    OZ(write_single_char_to_file(&char_escape_));
    OZ(write_single_char_to_file(&char_n));
  } else {
    OZ(write_obj_to_file(obj, true));
  }
  if (need_enclose) {
    OZ(write_single_char_to_file(&char_enclose_));
  }
  return ret;
}

int ObSelectIntoOp::into_outfile()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObDatum *datum = NULL;
  ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval(eval_ctx_, datum))) {
      LOG_WARN("eval expr failed", K(ret));
    } else if (OB_ISNULL(datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("datum is unexpected null", K(ret));
    } else if (OB_FAIL(datum->to_obj(obj,
                                     select_exprs.at(i)->obj_meta_,
                                     select_exprs.at(i)->obj_datum_map_))) {
      LOG_WARN("failed to get obj from datum", K(ret));
    } else if (!ob_is_text_tc(select_exprs.at(i)->obj_meta_.get_type())) {
      OZ(print_field(obj));
    } else { // text tc
      OZ(print_lob_field(obj, *select_exprs.at(i), *datum));
    }
    // print field terminator
    if (OB_SUCC(ret) && i != select_exprs.count() - 1) {
      OZ(write_obj_to_file(MY_SPEC.field_str_));
    }
  }
  // print line terminator
  OZ(write_obj_to_file(MY_SPEC.line_str_));
  // check if need split file
  OZ(try_split_file());
  return ret;
}

int ObSelectIntoOp::into_outfile_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<ObDatumVector> datum_vectors;
  ObDatum *datum = NULL;
  ObObj obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_FAIL(select_exprs.at(i)->eval_batch(eval_ctx_, *brs.skip_, brs.size_))) {
      LOG_WARN("failed to eval batch", K(ret));
    } else if (OB_FAIL(datum_vectors.push_back(select_exprs.at(i)->locate_expr_datumvector(eval_ctx_)))) {
      LOG_WARN("failed to push back datum vector", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
    if (brs.skip_->contain(i)) {
      // do nothing
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < select_exprs.count(); ++j) {
        if (OB_ISNULL(datum = datum_vectors.at(j).at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is unexpected null", K(ret));
        } else if (OB_FAIL(datum->to_obj(obj,
                                         select_exprs.at(j)->obj_meta_,
                                         select_exprs.at(j)->obj_datum_map_))) {
          LOG_WARN("failed to get obj from datum", K(ret));
        } else if (!ob_is_text_tc(select_exprs.at(j)->obj_meta_.get_type())) {
          OZ(print_field(obj));
        } else { // text tc
          OZ(print_lob_field(obj, *select_exprs.at(j), *datum));
        }
        // print field terminator
        if (OB_SUCC(ret) && j != select_exprs.count() - 1) {
          OZ(write_obj_to_file(MY_SPEC.field_str_));
        }
      }
      // print line terminator
      OZ(write_obj_to_file(MY_SPEC.line_str_));
      // check if need split file
      OZ(try_split_file());
    }
  }
  return ret;
}

int ObSelectIntoOp::into_dumpfile()
{
  int ret = OB_SUCCESS;
  char buf[MAX_VALUE_LENGTH];
  int64_t buf_len = MAX_VALUE_LENGTH;
  int64_t pos = 0;
  if (OB_FAIL(get_row_str(buf_len, is_first_, buf, pos))) {
    LOG_WARN("get str failed", K(ret));
  } else if (is_first_) { // create file
    if (OB_FAIL(file_appender_.create(file_name_.get_varchar(), true))) {
      LOG_WARN("create dumpfile failed", K(ret), K(file_name_));
    } else {
      is_first_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(file_appender_.append(buf, pos, false))) {
      LOG_WARN("failed to append file");
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObSelectIntoOp::into_varlist()
{
  int ret = OB_SUCCESS;
  //before 4_1 use output
  //after 4_1 use select exprs
  const ObIArray<ObExpr*> &select_exprs = (MY_SPEC.select_exprs_.empty()) ?
                                           MY_SPEC.output_ : MY_SPEC.select_exprs_;
  const ObIArray<ObString> &user_vars = MY_SPEC.user_vars_;
  ObArenaAllocator lob_tmp_allocator("LobTmp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (select_exprs.count() != user_vars.count()) {
    ret = OB_ERR_COLUMN_SIZE;
    LOG_WARN("user vars count should be equal to select exprs count" , K(ret),
            K(select_exprs.count()), K(user_vars.count()));
  } else {
    for (int i = 0 ; i < user_vars.count(); ++i) {
      const ObString &var_name = user_vars.at(i);
      ObObj obj;
      ObDatum *datum = NULL;
      if (OB_FAIL(select_exprs.at(i)->eval(eval_ctx_, datum))) {
        LOG_WARN("eval expr failed", K(ret));
      } else if (OB_FAIL(datum->to_obj(obj, select_exprs.at(i)->obj_meta_))) {
        LOG_WARN("convert datum to obj failed", K(ret), KPC(select_exprs.at(i)));
      } else if (obj.is_lob_storage()
          // outrow lob can not be assigned to user var, so convert outrow to inrow lob
          // user var has independent memory, so using temporary memory here is fine
          && OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(obj, obj, nullptr, &lob_tmp_allocator, true/*allow_persist_inrow*/))) {
        LOG_WARN("convert outrow to inrow lob failed", K(ret), K(obj));
      } else if (OB_FAIL(ObVariableSetExecutor::set_user_variable(obj, var_name,
                  ctx_.get_my_session()))) {
        LOG_WARN("set user variable failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::extract_fisrt_wchar_from_varhcar(const ObObj &obj, int32_t &wchar)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  if (obj.is_varying_len_char_type()) {
    ObString str = obj.get_varchar();
    if (str.length() > 0) {
      ret = ObCharset::mb_wc(obj.get_collation_type(), str.ptr(), str.length(), length, wchar);
    }
  }
  return ret;
}

int ObSelectIntoOp::print_wchar_to_buf(char *buf,
                                       const int64_t buf_len,
                                       int64_t &pos,
                                       int32_t wchar,
                                       ObString &str,
                                       ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  int result_len = 0;
  if (OB_FAIL(ObCharset::wc_mb(coll_type, wchar, buf + pos, buf_len - pos, result_len))) {
    LOG_WARN("failed to convert wc to mb");
  } else {
    str = ObString(result_len, buf + pos);
    pos += result_len;
  }
  return ret;
}

int ObSelectIntoOp::prepare_escape_printer()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char *buf = NULL;
  int64_t buf_len = 6 * ObCharset::MAX_MB_LEN;
  // mb->wc
  int32_t wchar_enclose = char_enclose_;
  int32_t wchar_escape = char_escape_;
  int32_t wchar_field = 0;
  int32_t wchar_line = 0;
  int32_t wchar_zero = '\0';
  int32_t wchar_replace = 0;
  OZ(extract_fisrt_wchar_from_varhcar(MY_SPEC.field_str_, wchar_field));
  OZ(extract_fisrt_wchar_from_varhcar(MY_SPEC.line_str_, wchar_line));
  OZ(ObCharset::get_replace_character(MY_SPEC.cs_type_, wchar_replace));
  // wc->mb
  if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate buffer", K(ret), K(buf_len));
  }
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_enclose, escape_printer_.enclose_, MY_SPEC.cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_escape, escape_printer_.escape_, MY_SPEC.cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_zero, escape_printer_.zero_, MY_SPEC.cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_field, escape_printer_.field_terminator_, MY_SPEC.cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_line, escape_printer_.line_terminator_, MY_SPEC.cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_replace, escape_printer_.convert_replacer_, MY_SPEC.cs_type_));
  escape_printer_.coll_type_ = MY_SPEC.cs_type_;
  escape_printer_.ignore_convert_failed_ = true; // TODO: provide user-defined interface
  return ret;
}

int ObSelectIntoOp::check_has_lob_or_json()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  for (int64_t i = 0; OB_SUCC(ret) && (!has_lob_ || !has_json_) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is unexpected null", K(ret));
    } else if (ob_is_text_tc(select_exprs.at(i)->obj_meta_.get_type())) {
      has_lob_ = true;
    } else if (ob_is_json_tc(select_exprs.at(i)->obj_meta_.get_type())) {
      has_json_ = true;
    }
  }
  return ret;
}

void ObSelectIntoOp::destroy()
{
  file_appender_.~ObFileAppender();
  close_file();
  if (NULL != device_handle_) {
    common::ObDeviceManager::get_instance().release_device(device_handle_);
    device_handle_ = NULL;
  }
  ObOperator::destroy();
}



}
}
