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
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObSelectIntoSpec, ObOpSpec), into_type_, user_vars_,
    outfile_name_, filed_str_, line_str_, closed_cht_, is_optional_, select_exprs_);


int ObSelectIntoOp::inner_open()
{
  int ret = OB_SUCCESS;
  file_name_ = MY_SPEC.outfile_name_;
  filed_str_ = MY_SPEC.filed_str_;
  line_str_ = MY_SPEC.line_str_;
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
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.filed_str_,
                                                  phy_plan_ctx->get_param_store(),
                                                  filed_str_,
                                                  need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.line_str_,
                                                  phy_plan_ctx->get_param_store(),
                                                  line_str_,
                                                  need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else {
    // since we call get_next_row in inner_open, we have to set opened_ first in avoid to a infinite loop.
    opened_ = true;
    if (!lib::is_oracle_mode()) {
      if (OB_FAIL(session->get_sql_select_limit(top_limit_cnt_))) {
        LOG_WARN("fail tp get sql select limit", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString path = file_name_.get_varchar().trim();
    if (path.prefix_match_ci(OB_OSS_PREFIX)) {
      file_location_ = IntoFileLocation::REMOTE_OSS;
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

      //create buffer
      if (OB_SUCC(ret)) {
        const int64_t buf_len = OB_MALLOC_BIG_BLOCK_SIZE;
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate buffer", K(ret), K(buf_len));
        } else {
          data_writer_.init(buf, buf_len);
        }
      }

      //init device handle
      if (OB_SUCC(ret)) {
        ObBackupIoAdapter util;

        if (url_.empty() || access_info_.is_valid()) {
          ret = OB_FILE_NOT_EXIST;
          LOG_WARN("file path not exist", K(ret), K(url_), K(access_info_));
        } else if (OB_FAIL(util.get_and_init_device(device_handle_, &access_info_, url_))) {
          LOG_WARN("fail to init device", K(ret), K(url_), K(access_info_));
        }
      }
    } else {
      file_location_ = IntoFileLocation::SERVER_DISK;
      url_ = path;
    }
  }

  if (OB_SUCC(ret)
      && T_INTO_OUTFILE == into_type
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
      if (OB_FAIL(session->get_secure_file_priv(secure_file_priv))) {
        LOG_WARN("failed to get secure file priv", K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, actual_path))) {
        LOG_WARN("failed to check secure path", K(ret), K(secure_file_priv));
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
  if (file_location_ == IntoFileLocation::REMOTE_OSS) {
    if (OB_FAIL(data_writer_.flush(get_flush_function()))) {
      LOG_WARN("fail to flush data", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::get_row_str(const int64_t buf_len,
                                bool is_first_row,
                                char *buf,
                                int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObObj &filed_str = filed_str_;
  char closed_cht = MY_SPEC.closed_cht_;
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
      // filed terminated by "a"
      if (OB_SUCC(ret) && i != select_exprs.count() - 1 && filed_str.is_varying_len_char_type()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s", filed_str.get_varchar().length(), filed_str.get_varchar().ptr()))) {
          LOG_WARN("print filed str failed", K(ret), K(filed_str));
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


    ObSqlString url_with_suffix;
    bool is_exist = false;

    if (OB_FAIL(url_with_suffix.assign(url_))) {
      LOG_WARN("fail to assign string", K(ret));
    } else if (0 != split_file_id_  //using origin file name, assuming file do not need split
               && OB_FAIL(url_with_suffix.append_fmt(".extend%ld", split_file_id_))) {
      LOG_WARN("fail to append string", K(ret));
    } else if (OB_FAIL(device_handle_->exist(url_with_suffix.ptr(), is_exist))) {
      LOG_WARN("fail to check file exist", K(ret));
    } else if (is_exist) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("fail already exist", K(ret), K(url_));
    } else if (OB_FAIL(device_handle_->open(url_with_suffix.ptr(), -1, 0, fd_, &iod_opts))) {
      LOG_WARN("fail to open file", K(ret));
    }
  } else {
    if (OB_FAIL(file_appender_.create(url_, true))) {
      LOG_WARN("create dumpfile failed", K(ret), K(url_));
    }
    file_location_ = IntoFileLocation::SERVER_DISK;
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
}

std::function<int(const char *, int64_t)> ObSelectIntoOp::get_flush_function()
{
  return [this](const char *data, int64_t data_len) -> int
  {
    int ret = OB_SUCCESS;
    int64_t write_size = 0;
    int64_t begin_ts = ObTimeUtility::current_time();
    if (OB_FAIL(device_handle_->write(fd_, data, data_len, write_size))) {
      LOG_WARN("fail to write device", K(ret));
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
    return ret;
  };
}

int ObSelectIntoOp::split_file()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_writer_.flush(get_flush_function()))) {
    LOG_WARN("fail to flush data", K(ret));
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
    if (OB_FAIL(open_file())) {
      LOG_WARN("fail to open file", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::write_file(const char *data, int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (file_location_ == IntoFileLocation::SERVER_DISK) {
    if (OB_FAIL(file_appender_.append(data, data_len, false))) {
      LOG_WARN("failed to append file", K(ret), K(data_len));
    }
  } else {
    const int64_t MAX_OSS_FILE_SIZE = 5LL * 1024 * 1024 * 1024; //5G
    if (write_bytes_ + data_len > MAX_OSS_FILE_SIZE) {
      if (OB_FAIL(split_file())) {
        LOG_WARN("fail to split file", K(ret));
      } else {
        write_bytes_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_writer_.write(data, data_len, get_flush_function()))) {
        LOG_WARN("fail to write data into cloud", K(ret));
      } else {
        write_bytes_ += data_len;
      }
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

int ObSelectIntoOp::into_outfile()
{
  int ret = OB_SUCCESS;
  //before 4_1 use output
  //after 4_1 use select exprs
  const ObIArray<ObExpr*> &select_exprs = (MY_SPEC.select_exprs_.empty()) ?
                                           MY_SPEC.output_ : MY_SPEC.select_exprs_ ;
  if (select_exprs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count of exprs in select into outfile", K(select_exprs.count()), K(ret));
  } else if (is_first_) { // create file
    if (OB_FAIL(open_file())) {
      LOG_WARN("open file failed", K(ret), K(file_name_));
    } else {
      is_first_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    ObDatum *datum = NULL;
    if (OB_FAIL(select_exprs.at(0)->eval(eval_ctx_, datum))) {
      LOG_WARN("eval expr failed", K(ret));
    } else if (OB_FAIL(write_file(datum->ptr_, datum->len_))) {
      LOG_WARN("failed to append file");
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObSelectIntoOp::into_outfile_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  //before 4_1 use output
  //after 4_1 use select exprs
  const ObIArray<ObExpr*> &select_exprs = (MY_SPEC.select_exprs_.empty()) ?
                                           MY_SPEC.output_ : MY_SPEC.select_exprs_;
  if (select_exprs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count of exprs in select into outfile", K(select_exprs.count()), K(ret));
  } else if (is_first_) { // create file
    if (OB_FAIL(open_file())) {
      LOG_WARN("open file failed", K(ret), K(file_name_));
    } else {
      is_first_ = false;
    }
  }
  OZ(select_exprs.at(0)->eval_batch(eval_ctx_, *brs.skip_, brs.size_));
  if (OB_SUCC(ret)) {
    ObDatumVector datum_vector = select_exprs.at(0)->locate_expr_datumvector(eval_ctx_);
    for (int64_t i = 0; i < brs.size_ && OB_SUCC(ret); i++) {
      if (brs.skip_->contain(i)) {
        // do nothing
      } else if (OB_FAIL(write_file(datum_vector.at(i)->ptr_, datum_vector.at(i)->len_))) {
        LOG_WARN("failed to append file");
      } else {
        //do nothing
      }
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
      } else if (OB_FAIL(ObVariableSetExecutor::set_user_variable(obj, var_name,
                  ctx_.get_my_session()))) {
        LOG_WARN("set user variable failed", K(ret));
      }
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
