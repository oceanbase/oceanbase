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
#include "share/ob_device_manager.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static constexpr uint64_t OB_STORAGE_ID_EXPORT = 2002;

ObStorageAppender::ObStorageAppender()
    : is_opened_(false),
      offset_(0),
      fd_(),
      device_handle_(nullptr),
      access_type_(OB_STORAGE_ACCESS_MAX_TYPE)
{}

ObStorageAppender::~ObStorageAppender()
{
  reset();
}

void ObStorageAppender::reset()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  if (is_opened_) {
    if (OB_FAIL(adapter.close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), K_(fd), KP_(device_handle));
    }
  }
  offset_ = 0;
  fd_.reset();
  device_handle_ = nullptr;
  access_type_ = OB_STORAGE_ACCESS_MAX_TYPE;
  is_opened_ = false;
}

int ObStorageAppender::open(const share::ObBackupStorageInfo *storage_info,
    const ObString &uri, const ObStorageAccessType &access_type)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageAppender has been opened",
        KR(ret), K_(is_opened), KPC(storage_info), K(uri), K(access_type));
  } else if (OB_UNLIKELY(access_type != OB_STORAGE_ACCESS_APPENDER
      && access_type != OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access type is not supported",
        KR(ret), K_(is_opened), KPC(storage_info), K(uri), K(access_type));
  // The validity of the input parameters is verified by the open_with_access_type function.
  } else if (OB_FAIL(adapter.open_with_access_type(device_handle_, fd_,
      storage_info, uri, access_type,
      ObStorageIdMod(OB_STORAGE_ID_EXPORT, ObStorageUsedMod::STORAGE_USED_EXPORT)))) {
    LOG_WARN("fail to open appender", KR(ret), KPC(storage_info), K(uri), K(access_type));
  } else {
    offset_ = 0;
    access_type_ = access_type;
    is_opened_ = true;
  }
  return ret;
}

int ObStorageAppender::append(const char *buf, const int64_t size, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  ObBackupIoAdapter adapter;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_EXPORT);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageAppender not opened", KR(ret), K_(is_opened));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size));
  } else if (OB_STORAGE_ACCESS_APPENDER == access_type_) {
    if (OB_FAIL(adapter.pwrite(*device_handle_, fd_,
        buf, offset_, size, write_size, false/*is_can_seal*/))) {
      LOG_WARN("fail to append data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else if (OB_UNLIKELY(size != write_size)) {
      ret = OB_IO_ERROR;
      LOG_WARN("write size not equal to expected size",
          KR(ret), K_(fd), K_(offset), K(size), K(write_size));
    }
  } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type_) {
    ObIOHandle io_handle;
    // The BUFFERED_MULTIPART_WRITER writes to the buffer first during upload,
    // and only uploads when the buffer is full.
    // Therefore, the return value of io_handle.get_data_size() may be 0
    // or the total size of the buffer during upload.
    if (OB_FAIL(adapter.async_upload_data(*device_handle_, fd_, buf, offset_, size, io_handle))) {
      LOG_WARN("fail to upload data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait uploading data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else {
      write_size = io_handle.get_data_size();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access type is invalid, please check open status",
        KR(ret), KP_(device_handle), K_(fd), K_(access_type));
  }

  if (OB_SUCC(ret)) {
    offset_ += size;
  }
  return ret;
}

int ObStorageAppender::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_EXPORT);
  // ignore error if not opened
  if (OB_LIKELY(is_opened_)) {
    if (OB_STORAGE_ACCESS_APPENDER == access_type_) {
      if (OB_FAIL(device_handle_->seal_file(fd_))) {
        LOG_WARN("fail to seal file",
            KR(ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
      }
    } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type_) {
      if (OB_FAIL(adapter.complete(*device_handle_, fd_))) {
        LOG_WARN("fail to complete",
            KR(ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
      }

      // if complete failed, need to abort
      if (OB_FAIL(ret)) {
        if (OB_TMP_FAIL(adapter.abort(*device_handle_, fd_))) {
          LOG_WARN("fail to abort",
              KR(ret), KR(tmp_ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access type is invalid, please check open status",
          KR(ret), KP_(device_handle), K_(fd), K_(access_type));
    }

    if (OB_TMP_FAIL(adapter.close_device_and_fd(device_handle_, fd_))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to close device and fd", KR(ret), KR(tmp_ret), K_(fd), KP_(device_handle));
    }
  }
  offset_ = 0;
  fd_.reset();
  device_handle_ = nullptr;
  is_opened_ = false;
  return ret;
}

OB_SERIALIZE_MEMBER(ObSelectIntoOpInput, task_id_, sqc_id_);
OB_SERIALIZE_MEMBER((ObSelectIntoSpec, ObOpSpec), into_type_, user_vars_, outfile_name_,
    field_str_, // FARM COMPAT WHITELIST FOR filed_str_: renamed
    line_str_, closed_cht_, is_optional_, select_exprs_, is_single_, max_file_size_,
    escaped_cht_, cs_type_, parallel_, file_partition_expr_, buffer_size_, is_overwrite_,
    external_properties_, external_partition_);


int ObSelectIntoOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool need_check = false;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else {
    // since we call get_next_row in inner_open, we have to set opened_ first in avoid to a infinite loop.
    opened_ = true;
    if (!lib::is_oracle_mode()) {
      if (OB_FAIL(session->get_sql_select_limit(top_limit_cnt_))) {
        LOG_WARN("fail tp get sql select limit", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !MY_SPEC.external_properties_.str_.empty()) {
    if (OB_FAIL(external_properties_.load_from_string(MY_SPEC.external_properties_.str_,
                                                      ctx_.get_allocator()))) {
      LOG_WARN("failed to load external properties", K(ret));
    } else {
      format_type_ = external_properties_.format_type_;
    }
  }
  if (OB_SUCC(ret)) {
    switch (format_type_)
    {
      case ObExternalFileFormat::FormatType::CSV_FORMAT:
      {
        if (OB_FAIL(init_csv_env())) {
          LOG_WARN("failed to init csv env", K(ret));
        }
        break;
      }
      case ObExternalFileFormat::FormatType::ODPS_FORMAT:
      {
#ifdef OB_BUILD_CPP_ODPS
        if (OB_FAIL(init_odps_tunnel())) {
          LOG_WARN("failed to init odps tunnel", K(ret));
        }
#else
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
        LOG_WARN("not support to write odps in opensource", K(ret));
#endif
        break;
      }
      default:
      {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support select into type", K(format_type_));
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::init_csv_env()
{
  int ret = OB_SUCCESS;
  file_name_ = MY_SPEC.outfile_name_;
  field_str_ = MY_SPEC.field_str_;
  line_str_ = MY_SPEC.line_str_;
  has_enclose_ = MY_SPEC.closed_cht_.get_val_len() > 0;
  char_enclose_ = has_enclose_ ? MY_SPEC.closed_cht_.get_char().ptr()[0] : 0;
  has_escape_ = MY_SPEC.escaped_cht_.get_val_len() > 0;
  char_escape_ = has_escape_ ? MY_SPEC.escaped_cht_.get_char().ptr()[0] : 0;
  do_partition_ = MY_SPEC.file_partition_expr_ == NULL ? false : true;
  bool need_check = false;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  const ObItemType into_type = MY_SPEC.into_type_;
  if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_FAIL(check_has_lob_or_json())) {
    LOG_WARN("failed to check has lob", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
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
  } else {
    print_params_.tz_info_ = session->get_timezone_info();
    print_params_.use_memcpy_ = true;
    print_params_.binary_string_print_hex_ = lib::is_oracle_mode();
    print_params_.cs_type_ = MY_SPEC.cs_type_;
  }
  //create buffer
  if (OB_SUCC(ret) && T_INTO_OUTFILE == into_type && OB_FAIL(create_shared_buffer_for_data_writer())) {
    LOG_WARN("failed to create buffer for data writer", K(ret));
  }
  //calc first data_writer.url_ and basic_url_
  if (OB_SUCC(ret)) {
    ObString path = file_name_.get_varchar().trim();
    file_location_ = path.prefix_match_ci(OB_OSS_PREFIX)
                     ? IntoFileLocation::REMOTE_OSS
                     : IntoFileLocation::SERVER_DISK;
    if (file_location_ == IntoFileLocation::SERVER_DISK && do_partition_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support partition option on server disk", K(ret));
	    LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition option on server disk");
    } else if (T_INTO_OUTFILE == into_type && !MY_SPEC.is_single_ && OB_FAIL(calc_first_file_path(path))) {
      LOG_WARN("failed to calc first file path", K(ret));
    } else if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      ObString temp_url = path.split_on('?');
      temp_url.trim();
      ObString storage_info;
      if (OB_FAIL(ob_write_string(ctx_.get_allocator(), temp_url, basic_url_, true))) {
        LOG_WARN("failed to append string", K(ret));
      } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, storage_info, true))) {
        LOG_WARN("failed to append string", K(ret));
      } else if (OB_FAIL(access_info_.set(basic_url_.ptr(), storage_info.ptr()))) {
        LOG_WARN("failed to set access info", K(ret), K(path));
      }
      //init device handle
      if (OB_SUCC(ret)) {
        if (basic_url_.empty() || !access_info_.is_valid()) {
          ret = OB_FILE_NOT_EXIST;
          LOG_WARN("file path not exist", K(ret), K(basic_url_), K(access_info_));
        }
      }
    } else { // IntoFileLocation::SERVER_DISK
      if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, basic_url_, true))) {
        LOG_WARN("failed to write string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && (T_INTO_OUTFILE == into_type || T_INTO_DUMPFILE == into_type)
      && IntoFileLocation::SERVER_DISK == file_location_ && OB_FAIL(check_secure_file_path(basic_url_))) {
    LOG_WARN("failed to check secure file path", K(ret));
  }
  if (OB_SUCC(ret) && do_partition_) {
    partition_map_.create(128, ObLabel("SelectInto"), ObLabel("SelectInto"), MTL_ID());
  }
  return ret;
}

#ifdef OB_BUILD_CPP_ODPS
int ObSelectIntoOp::init_odps_tunnel()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(input_);
  if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is unexpected null", K(ret));
  } else if (is_in_px) {
    ObOdpsPartitionDownloaderMgr &odps_mgr = ctx_.get_sqc_handler()->get_sqc_ctx().gi_pump_.get_odps_mgr();
    if (OB_FAIL(odps_mgr.get_odps_uploader(input->task_id_, upload_, record_writer_))) {
      LOG_WARN("failed to get odps uploader", K(ret));
    }
  } else if (OB_FAIL(external_properties_.odps_format_.decrypt())) {
    LOG_WARN("failed to decrypt odps format", K(ret));
  } else {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
    try {
      if (OB_FAIL(ObOdpsPartitionDownloaderMgr::create_upload_session(external_properties_.odps_format_,
                                                                      MY_SPEC.external_partition_.str_,
                                                                      MY_SPEC.is_overwrite_,
                                                                      upload_))) {
        LOG_WARN("failed to create upload session", K(ret));
      } else if (OB_UNLIKELY(!(record_writer_ = upload_->OpenWriter(block_id_, true)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    need_commit_ = false;
  }
  return ret;
}
#endif

int ObSelectIntoOp::inner_get_next_row()
{
  int ret = 0 == top_limit_cnt_ ? OB_ITER_END : OB_SUCCESS;
  int64_t row_count = 0;
  const ObItemType into_type = MY_SPEC.into_type_;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObIOBufferWriter *data_writer = NULL;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  }
  //when do_partition is false, create the only data_writer here
  if (OB_SUCC(ret) && ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_
      && T_INTO_VARIABLES != into_type && !do_partition_
      && OB_FAIL(create_the_only_data_writer(data_writer))) {
    LOG_WARN("failed to create the only data writer", K(ret));
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
      if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
#ifdef OB_BUILD_CPP_ODPS
        if (OB_FAIL(into_odps())) {
          LOG_WARN("into odps failed", K(ret));
        }
#else
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
        LOG_WARN("not support to write odps in opensource", K(ret));
#endif
      } else if (T_INTO_VARIABLES == into_type) {
        if (OB_FAIL(into_varlist())) {
          LOG_WARN("into varlist failed", K(ret));
        }
      } else if (T_INTO_OUTFILE == into_type) {
        if (OB_FAIL(into_outfile(data_writer))) {
          LOG_WARN("into outfile failed", K(ret));
        }
      } else {
        if (OB_FAIL(into_dumpfile(data_writer))) {
          LOG_WARN("into dumpfile failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) { // if into user variables or into dumpfile, must be one row
      if (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_
          && (T_INTO_VARIABLES == into_type || T_INTO_DUMPFILE == into_type) && row_count > 1) {
        ret = OB_ERR_TOO_MANY_ROWS;
        LOG_WARN("more than one row for into variables or into dumpfile", K(ret), K(row_count));
      }
    }
  } //end while
  if (OB_ITER_END == ret || OB_SUCC(ret)) { // set affected rows
    phy_plan_ctx->set_affected_rows(row_count);
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    need_commit_ = false;
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
  ObIOBufferWriter *data_writer = NULL;
  bool stop_loop = false;
  bool is_iter_end = false;
  LOG_TRACE("debug select into get next batch begin");
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  }
  //when do_partition is false, create the only data_writer here
  if (OB_SUCC(ret) && ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_
      && T_INTO_VARIABLES != into_type && !do_partition_
      && OB_FAIL(create_the_only_data_writer(data_writer))) {
    LOG_WARN("failed to create the only data writer", K(ret));
  }
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
        if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
#ifdef OB_BUILD_CPP_ODPS
          if (OB_FAIL(into_odps_batch(brs_))) {
            LOG_WARN("into odps batch failed", K(ret));
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
          LOG_WARN("not support to write odps in opensource", K(ret));
#endif
        } else if (T_INTO_OUTFILE == into_type) {
          if (OB_FAIL(into_outfile_batch(brs_, data_writer))) {
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
              if (OB_FAIL(into_dumpfile(data_writer))) {
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
  if (OB_FAIL(ret)) {
    need_commit_ = false;
  }
  LOG_TRACE("debug select into get next batch end");
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
  ObIOBufferWriter *data_writer = NULL;
  if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
#ifdef OB_BUILD_CPP_ODPS
    if (OB_FAIL(odps_commit_upload())) {
      LOG_WARN("failed to commit upload", K(ret));
    }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
    LOG_WARN("not support to write odps in opensource", K(ret));
#endif
  } else if (do_partition_) {
    for (ObPartitionWriterMap::iterator iter = partition_map_.begin();
         OB_SUCC(ret) && iter != partition_map_.end(); iter++) {
      if (OB_ISNULL(data_writer = iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data writer is unexpected null", K(ret));
      } else if (OB_FAIL(flush_buf(*data_writer))) {
        LOG_WARN("failed to flush buffer", K(ret));
      }
    }
  } else if (OB_NOT_NULL(data_writer_) && OB_FAIL(flush_buf(*data_writer_))) {
    LOG_WARN("failed to flush buffer", K(ret));
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

int ObSelectIntoOp::open_file(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (IntoFileLocation::REMOTE_OSS == file_location_) {
    bool is_exist = false;
    ObBackupIoAdapter adapter;
    // TODO @linyi.cl: for S3, should use OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER
    const ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
    if (OB_FAIL(adapter.is_exist(data_writer.url_, &access_info_, is_exist))) {
      LOG_WARN("fail to check file exist", KR(ret), K(data_writer.url_), K_(access_info));
    } else if (is_exist) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("file already exist", KR(ret), K(data_writer.url_), K_(access_info));
    } else if (OB_FAIL(data_writer.storage_appender_.open(&access_info_, data_writer.url_, access_type))) {
      LOG_WARN("fail to open file", KR(ret), K(data_writer.url_), K_(access_info));
    } else {
      data_writer.is_file_opened_ = true;
    }
  } else if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(data_writer.file_appender_.create(data_writer.url_, true))) {
      LOG_WARN("failed to create file", K(ret), K(data_writer.url_));
    } else {
      data_writer.is_file_opened_ = true;
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
      file_name_with_suffix.append_fmt("%.*sdata", input_file_name.length(), input_file_name.ptr());
    } else {
      file_name_with_suffix.append_fmt("%.*s", input_file_name.length(), input_file_name.ptr());
    }
    if (MY_SPEC.parallel_ > 1) {
      file_name_with_suffix.append_fmt("_%ld_%ld_%d", input->sqc_id_, input->task_id_, 0);
    } else {
      file_name_with_suffix.append_fmt("_%d", 0);
    }
    if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      file_name_with_suffix.append_fmt("?%.*s", path.length(), path.ptr());
    }
    if (OB_FAIL(ob_write_string(ctx_.get_allocator(), file_name_with_suffix.string(), path))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::calc_next_file_path(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  ObSqlString url_with_suffix;
  ObString file_path;
  data_writer.split_file_id_++;
  if (data_writer.split_file_id_ > 0) {
    if (MY_SPEC.is_single_ && IntoFileLocation::REMOTE_OSS == file_location_) {
      file_path = (data_writer.split_file_id_ > 1)
                  ? data_writer.url_.split_on(data_writer.url_.reverse_find('.'))
                  : data_writer.url_;
      if (OB_FAIL(url_with_suffix.assign(file_path))) {
        LOG_WARN("failed to assign string", K(ret));
      } else if (OB_FAIL(url_with_suffix.append_fmt(".extend%ld", data_writer.split_file_id_))) {
        LOG_WARN("failed to append string", K(ret));
      }
    } else if (!MY_SPEC.is_single_) {
      file_path = data_writer.url_.split_on(data_writer.url_.reverse_find('_'));
      if (OB_FAIL(url_with_suffix.assign(file_path))) {
        LOG_WARN("failed to assign string", K(ret));
      } else if (OB_FAIL(url_with_suffix.append_fmt("_%ld", data_writer.split_file_id_))) {
        LOG_WARN("failed to append string", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected single value", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(ctx_.get_allocator(),
                                                url_with_suffix.string(),
                                                data_writer.url_, true))) {
      LOG_WARN("failed to write string", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected split file id", K(ret));
  }
  return ret;
}

// 根据传入的partition和basic_url_设置当前data_writer的url_, 每个分区只需要计算一次, 后续只要改split id
int ObSelectIntoOp::calc_file_path_with_partition(ObString partition, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  ObSqlString url_with_partition;
  ObString dir_path;
  if (OB_FAIL(ob_write_string(ctx_.get_allocator(), basic_url_, data_writer.url_))) {
    LOG_WARN("failed to write string", K(ret));
  } else {
    dir_path = data_writer.url_.split_on(data_writer.url_.reverse_find('/'));
    if (OB_FAIL(url_with_partition.assign(dir_path))) {
      LOG_WARN("failed to assign string", K(ret));
    } else if (url_with_partition.length() != 0 && OB_FAIL(url_with_partition.append("/"))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (partition.length() != 0 && OB_FAIL(url_with_partition.append_fmt("%.*s/",
                                                                                partition.length(),
                                                                                partition.ptr()))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (partition.length() == 0 && OB_FAIL(url_with_partition.append("__NULL__/"))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FAIL(url_with_partition.append_fmt("%.*s",
                                                     data_writer.url_.length(),
                                                     data_writer.url_.ptr()))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(),
                                       url_with_partition.string(),
                                       data_writer.url_,
                                       true))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

// TODO @linyi.cl: should handle failure of `close_file`
void ObSelectIntoOp::close_file(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    data_writer.file_appender_.close();
  } else if (OB_FAIL(data_writer.storage_appender_.close())) {
    LOG_WARN("fail to close storage appender", KR(ret), K(data_writer.url_), K(access_info_));
  }

  if (OB_SUCC(ret)) {
    data_writer.is_file_opened_ = false;
  }
}

std::function<int(const char *, int64_t, ObSelectIntoOp::ObIOBufferWriter *)> ObSelectIntoOp::get_flush_function()
{
  return [this](const char *data, int64_t data_len, ObSelectIntoOp::ObIOBufferWriter *data_writer) -> int
  {
    int ret = OB_SUCCESS;
    if (data == NULL || data_len == 0) {
    } else if (OB_ISNULL(data_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    } else if (!data_writer->is_file_opened_ && OB_FAIL(open_file(*data_writer))) {
      LOG_WARN("failed to open file", K(ret), K(data_writer->url_));
    } else if (file_location_ == IntoFileLocation::SERVER_DISK) {
      if (OB_FAIL(data_writer->file_appender_.append(data, data_len, false))) {
        LOG_WARN("failed to append file", K(ret), K(data_len));
      }
    } else if (file_location_ == IntoFileLocation::REMOTE_OSS) {
      int64_t write_size = 0;
      int64_t begin_ts = ObTimeUtility::current_time();
      if (OB_FAIL(data_writer->storage_appender_.append(data, data_len, write_size))) {
        LOG_WARN("fail to append data", KR(ret), KP(data), K(data_len), K(data_writer->url_), K_(access_info));
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

int ObSelectIntoOp::split_file(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (!use_shared_buf_ && OB_FAIL(flush_buf(data_writer))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else if (has_lob_ && use_shared_buf_ && OB_FAIL(flush_shared_buf(data_writer, get_flush_function()))) {
    // 要保证文件中每一行的完整性, 有lob的时候shared buffer里不一定是完整的一行
    // 因此剩下的shared buffer里的内容也要刷到当前文件里, 这种情况下无法严格满足max_file_size的限制
    LOG_WARN("failed to flush shared buffer", K(ret));
  } else {
    close_file(data_writer);
  }
  if (OB_SUCC(ret) && OB_FAIL(calc_next_file_path(data_writer))) {
    LOG_WARN("failed to calculate new file path", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::try_split_file(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  int64_t curr_line_len = 0;
  int64_t curr_bytes = 0;
  bool has_split = false;
  bool has_use_shared_buf = use_shared_buf_;
  if (!has_lob_ || data_writer.get_curr_line_len() == 0) {
    curr_line_len = data_writer.get_curr_pos() - data_writer.get_last_line_pos();
  } else {
    curr_line_len = data_writer.get_curr_pos() + data_writer.get_curr_line_len();
  }
  curr_bytes = data_writer.get_write_bytes() + curr_line_len;
  if (!(has_lob_ && has_use_shared_buf) && data_writer.get_write_bytes() == 0) {
  } else if ((file_location_ == IntoFileLocation::SERVER_DISK
              && !MY_SPEC.is_single_ && curr_bytes > MY_SPEC.max_file_size_)
             || (file_location_ == IntoFileLocation::REMOTE_OSS
                 && ((!MY_SPEC.is_single_ && curr_bytes > min(MY_SPEC.max_file_size_, MAX_OSS_FILE_SIZE))
                     || (MY_SPEC.is_single_ && curr_bytes > MAX_OSS_FILE_SIZE)))) {
    if (OB_FAIL(split_file(data_writer))) {
      LOG_WARN("failed to split file", K(ret));
    } else {
      has_split = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (has_lob_ && has_use_shared_buf) {
      data_writer.set_write_bytes(has_split ? 0 : curr_bytes);
      data_writer.reset_curr_line_len();
    } else {
      data_writer.set_write_bytes(has_split ? curr_line_len : curr_bytes);
    }
    data_writer.update_last_line_pos();
  }
  return ret;
}

int ObSelectIntoOp::get_buf(char* &buf, int64_t &buf_len, int64_t &pos, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  buf = use_shared_buf_ ? get_shared_buf() : data_writer.get_buf();
  buf_len = use_shared_buf_ ? get_shared_buf_len() : data_writer.get_buf_len();
  pos = data_writer.get_curr_pos();
  if (OB_ISNULL(buf) && !use_shared_buf_ && OB_FAIL(use_shared_buf(data_writer, buf, buf_len, pos))) {
    LOG_WARN("failed to use shared buffer", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf should not be null", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::flush_buf(ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (use_shared_buf_) {
    // do nothing
  } else if (OB_FAIL(data_writer.flush(get_flush_function()))) {
    LOG_WARN("failed to flush buffer", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::use_shared_buf(ObIOBufferWriter &data_writer,
                                   char* &buf,
                                   int64_t &buf_len,
                                   int64_t &pos) {
  int ret = OB_SUCCESS;
  int64_t curr_pos = data_writer.get_curr_pos();
  if (!use_shared_buf_ && data_writer.get_last_line_pos() == 0) {
    if (OB_NOT_NULL(data_writer.get_buf()) && curr_pos > 0) {
      MEMCPY(shared_buf_, data_writer.get_buf(), curr_pos);
    }
    use_shared_buf_ = true;
    buf = shared_buf_;
    buf_len = shared_buf_len_;
    pos = curr_pos;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last line should be flushed before this line copied", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::resize_buf(char* &buf,
                               int64_t &buf_len,
                               int64_t &pos,
                               int64_t curr_pos,
                               bool is_json)
{
  int ret = OB_SUCCESS;
  int64_t new_buf_len = buf_len * 2;
  char* new_buf = NULL;
  if (OB_ISNULL(new_buf = static_cast<char*>(ctx_.get_allocator().alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(new_buf_len));
  } else if (!is_json) {
    if (curr_pos > 0) {
      MEMCPY(new_buf, shared_buf_, curr_pos);
    }
    shared_buf_ = new_buf;
    shared_buf_len_ = new_buf_len;
  } else {
    json_buf_ = new_buf;
    json_buf_len_ = new_buf_len;
  }
  if (OB_SUCC(ret)) {
    buf = new_buf;
    buf_len = new_buf_len;
    pos = is_json ? 0 : curr_pos;
  }
  return ret;
}

int ObSelectIntoOp::resize_or_flush_shared_buf(ObIOBufferWriter &data_writer,
                                               char* &buf,
                                               int64_t &buf_len,
                                               int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!use_shared_buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(use_shared_buf_), K(ret));
  } else if (has_lob_ && data_writer.get_curr_pos() > 0) {
    if (OB_FAIL(flush_shared_buf(data_writer, get_flush_function(), true))) {
      LOG_WARN("failed to flush shared buffer", K(ret));
    } else {
      pos = 0;
    }
  } else if (OB_FAIL(resize_buf(buf, buf_len, pos, data_writer.get_curr_pos()))) {
    LOG_WARN("failed to resize shared buffer", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::check_buf_sufficient(ObIOBufferWriter &data_writer,
                                         char* &buf,
                                         int64_t &buf_len,
                                         int64_t &pos,
                                         int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (buf_len < str_len * 1.1) {
    if (OB_FAIL(flush_buf(data_writer))) {
      LOG_WARN("failed to flush buffer", K(ret));
    } else if (OB_FAIL(use_shared_buf(data_writer, buf, buf_len, pos))) {
      LOG_WARN("failed to use shared buffer", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::write_obj_to_file(const ObObj &obj, ObIOBufferWriter &data_writer, bool need_escape)
{
  int ret = OB_SUCCESS;
  if ((obj.is_string_type() || obj.is_json()) && need_escape) {
    if (OB_FAIL(print_str_or_json_with_escape(obj, data_writer))) {
      LOG_WARN("failed to print str or json with escape", K(ret));
    }
  } else if (OB_FAIL(print_normal_obj_without_escape(obj, data_writer))) {
    LOG_WARN("failed to print normal obj without escape", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::print_str_or_json_with_escape(const ObObj &obj, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
  ObCharsetType dst_type = ObCharset::charset_type_by_coll(MY_SPEC.cs_type_);
  escape_printer_.do_encode_ = !(src_type == CHARSET_BINARY || src_type == dst_type
                                 || src_type == CHARSET_INVALID);
  escape_printer_.need_enclose_ = has_enclose_ && !obj.is_null()
                                  && (!MY_SPEC.is_optional_ || obj.is_string_type());
  escape_printer_.do_escape_ = true;
  escape_printer_.print_hex_ = obj.get_collation_type() == CS_TYPE_BINARY
                               && print_params_.binary_string_print_hex_;
  ObString str_to_escape;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(get_buf(escape_printer_.buf_, escape_printer_.buf_len_, escape_printer_.pos_, data_writer))) {
    LOG_WARN("failed to get buffer", K(ret));
  } else if (obj.is_json()) {
    ObObj inrow_obj = obj;
    if (obj.is_lob_storage()
        && OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(obj, inrow_obj, NULL, &temp_allocator))) {
      LOG_WARN("failed to convert outrow lobs", K(ret), K(obj));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(print_json_to_json_buf(inrow_obj, buf, buf_len, pos, data_writer))) {
      LOG_WARN("failed to print normal obj without escape", K(ret));
    } else {
      str_to_escape.assign_ptr(buf, pos);
      escape_printer_.do_encode_ = false;
    }
  } else {
    str_to_escape = obj.get_varchar();
  }
  if (OB_SUCC(ret) && !use_shared_buf_ && OB_FAIL(check_buf_sufficient(data_writer,
                                                                       escape_printer_.buf_,
                                                                       escape_printer_.buf_len_,
                                                                       escape_printer_.pos_,
                                                                       str_to_escape.length()))) {
    LOG_WARN("failed to check if buf is sufficient", K(ret));
  }
  if (OB_SUCC(ret) && !use_shared_buf_) {
    if (OB_FAIL(ObFastStringScanner::foreach_char(str_to_escape,
                                                  src_type,
                                                  escape_printer_,
                                                  escape_printer_.do_encode_,
                                                  escape_printer_.ignore_convert_failed_))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to print plain str", K(ret), K(src_type), K(escape_printer_.do_encode_));
      } else if (OB_FAIL(flush_buf(data_writer))) {
        LOG_WARN("failed to flush buffer", K(ret));
      } else if (OB_FALSE_IT(escape_printer_.pos_ = data_writer.get_curr_pos())) {
      } else if (OB_FAIL(ObFastStringScanner::foreach_char(str_to_escape,
                                                           src_type,
                                                           escape_printer_,
                                                           escape_printer_.do_encode_,
                                                           escape_printer_.ignore_convert_failed_))) {
        if (OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to print plain str", K(ret), K(src_type), K(escape_printer_.do_encode_));
        } else if (OB_FAIL(use_shared_buf(data_writer,
                                          escape_printer_.buf_,
                                          escape_printer_.buf_len_,
                                          escape_printer_.pos_))) {
          LOG_WARN("failed to use shared buffer", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && use_shared_buf_) {
    do {
      if (OB_FAIL(ObFastStringScanner::foreach_char(str_to_escape,
                                                    src_type,
                                                    escape_printer_,
                                                    escape_printer_.do_encode_,
                                                    escape_printer_.ignore_convert_failed_))) {
        LOG_WARN("failed to print plain str", K(ret), K(src_type), K(escape_printer_.do_encode_));
      }
    } while (OB_SIZE_OVERFLOW == ret && OB_SUCC(resize_or_flush_shared_buf(data_writer,
                                                                           escape_printer_.buf_,
                                                                           escape_printer_.buf_len_,
                                                                           escape_printer_.pos_)));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to print plain str", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    data_writer.set_curr_pos(escape_printer_.pos_);
  }

  return ret;
}

int ObSelectIntoOp::print_normal_obj_without_escape(const ObObj &obj, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  OZ(get_buf(buf, buf_len, pos, data_writer));
  if (OB_SUCC(ret) && !use_shared_buf_) {
    if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos, print_params_))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to print obj", K(ret));
      } else if (OB_FAIL(flush_buf(data_writer))) {
        LOG_WARN("failed to flush buffer", K(ret));
      } else if (OB_FALSE_IT(pos = data_writer.get_curr_pos())) {
      } else if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos, print_params_))) {
        if (OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to print obj", K(ret));
        } else if (OB_FAIL(use_shared_buf(data_writer, buf, buf_len, pos))) {
          LOG_WARN("failed to use shared buffer", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && use_shared_buf_) {
    do {
      if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos, print_params_))) {
        LOG_WARN("failed to print obj", K(ret));
      }
    } while (OB_SIZE_OVERFLOW == ret
             && OB_SUCC(resize_or_flush_shared_buf(data_writer, buf, buf_len, pos)));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to print obj", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    data_writer.set_curr_pos(pos);
  }
  return ret;
}

int ObSelectIntoOp::print_json_to_json_buf(const ObObj &obj,
                                           char* &buf,
                                           int64_t &buf_len,
                                           int64_t &pos,
                                           ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  buf = get_json_buf();
  buf_len = get_json_buf_len();
  pos = 0;
  do {
    if (OB_FAIL(obj.print_plain_str_literal(buf, buf_len, pos, print_params_))) {
      LOG_WARN("failed to print obj", K(ret));
    }
  } while (OB_SIZE_OVERFLOW == ret
           && OB_SUCC(resize_buf(buf, buf_len, pos, data_writer.get_curr_pos(), true)));
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to print json to json buffer", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::write_lob_to_file(const ObObj &obj,
                                      const ObExpr &expr,
                                      const ObDatum &datum,
                                      ObIOBufferWriter &data_writer)
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
  ObDatumMeta input_meta = expr.datum_meta_;
  ObTextStringIterState state;
  ObString src_block_data;
  ObTextStringIter lob_iter(input_meta.type_, input_meta.cs_type_, datum.get_string(),
                            expr.obj_meta_.has_lob_header());
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  int64_t truncated_len = 0;
  bool stop_when_truncated = false;
  OZ(lob_iter.init(0, NULL, &temp_allocator));
  OZ(get_buf(escape_printer_.buf_, escape_printer_.buf_len_, escape_printer_.pos_, data_writer));

  // 当truncated_len == src_block_data.length()时
  // 表明当前foreach_char处理的仅为lob末尾的无效的数据, 即上一轮的truncated data, 要避免死循环
  while (OB_SUCC(ret)
         && (state = lob_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
    // outrow lob最后一次才有可能为false, inrow lob只迭代一次, 为false
    stop_when_truncated = (truncated_len != src_block_data.length()) && lob_iter.is_outrow_lob();
    if (!use_shared_buf_ && OB_FAIL(check_buf_sufficient(data_writer,
                                                         escape_printer_.buf_,
                                                         escape_printer_.buf_len_,
                                                         escape_printer_.pos_,
                                                         src_block_data.length()))) {
      LOG_WARN("failed to check if buf is sufficient", K(ret));
    }
    if (OB_SUCC(ret) && !use_shared_buf_) {
      if (OB_FAIL(ObFastStringScanner::foreach_char(src_block_data,
                                                    src_type,
                                                    escape_printer_,
                                                    escape_printer_.do_encode_,
                                                    escape_printer_.ignore_convert_failed_,
                                                    stop_when_truncated,
                                                    &truncated_len))) {
        if (OB_ERR_DATA_TRUNCATED == ret && stop_when_truncated) {
          lob_iter.set_reserved_byte_len(truncated_len);
          ret = OB_SUCCESS;
        } else if (OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to print lob", K(ret));
        } else if (OB_FAIL(flush_buf(data_writer))) {
          LOG_WARN("failed to flush buffer", K(ret));
        } else if (OB_FALSE_IT(escape_printer_.pos_ = data_writer.get_curr_pos())) {
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
          } else if (OB_SIZE_OVERFLOW != ret) {
            LOG_WARN("failed to print lob", K(ret));
          } else if (OB_FAIL(use_shared_buf(data_writer,
                                            escape_printer_.buf_,
                                            escape_printer_.buf_len_,
                                            escape_printer_.pos_))) {
            LOG_WARN("failed to use shared buffer", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && use_shared_buf_) {
      if (OB_FAIL(ObFastStringScanner::foreach_char(src_block_data,
                                                    src_type,
                                                    escape_printer_,
                                                    escape_printer_.do_encode_,
                                                    escape_printer_.ignore_convert_failed_,
                                                    stop_when_truncated,
                                                    &truncated_len))) {
        if (OB_ERR_DATA_TRUNCATED == ret && stop_when_truncated) {
          lob_iter.set_reserved_byte_len(truncated_len);
          ret = OB_SUCCESS;
        } else if (OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to print lob", K(ret));
        } else if (OB_FAIL(flush_shared_buf(data_writer, get_flush_function(), true))) {
          LOG_WARN("failed to flush shared buffer", K(ret));
        } else if (OB_FALSE_IT(escape_printer_.pos_ = 0)) {
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
            LOG_WARN("failed to print lob", K(ret), K(src_block_data.length()), K(shared_buf_len_),
            K(data_writer.get_curr_pos()), K(escape_printer_.buf_len_), K(escape_printer_.pos_));
          }
        }
      }
    }
    data_writer.set_curr_pos(escape_printer_.pos_);
  }
  if (OB_FAIL(ret)) {
  } else if (state != TEXTSTRING_ITER_NEXT && state != TEXTSTRING_ITER_END) {
    ret = (lob_iter.get_inner_ret() != OB_SUCCESS) ?
          lob_iter.get_inner_ret() : OB_INVALID_DATA;
    LOG_WARN("iter state invalid", K(ret), K(state), K(lob_iter));
  }
  return ret;
}

int ObSelectIntoOp::write_single_char_to_file(const char *wchar, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  OZ(get_buf(buf, buf_len, pos, data_writer));
  if (OB_SUCC(ret) && !use_shared_buf_) {
    if (pos < buf_len) {
      MEMCPY(buf + pos, wchar, 1);
      data_writer.set_curr_pos(pos + 1);
    } else if (OB_FAIL(flush_buf(data_writer))) {
      LOG_WARN("failed to flush buffer", K(ret));
    } else if (OB_FALSE_IT(pos = data_writer.get_curr_pos())) {
    } else if (pos < buf_len) {
      MEMCPY(buf + pos, wchar, 1);
      data_writer.set_curr_pos(pos + 1);
    } else if (OB_FAIL(use_shared_buf(data_writer, buf, buf_len, pos))) {
      LOG_WARN("failed to use shared buffer", K(ret));
    }
  }
  if (OB_SUCC(ret) && use_shared_buf_) {
    if (pos < buf_len) {
      MEMCPY(buf + pos, wchar, 1);
      data_writer.set_curr_pos(pos + 1);
    } else if (OB_FAIL(resize_or_flush_shared_buf(data_writer, buf, buf_len, pos))) {
      LOG_WARN("failed to resize or flush shared buffer", K(ret));
    } else if (pos < buf_len) {
      MEMCPY(buf + pos, wchar, 1);
      data_writer.set_curr_pos(pos + 1);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::print_lob_field(const ObObj &obj,
                                    const ObExpr &expr,
                                    const ObDatum &datum,
                                    ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (has_enclose_) {
    OZ(write_single_char_to_file(&char_enclose_, data_writer));
  }
  OZ(write_lob_to_file(obj, expr, datum, data_writer));
  if (has_enclose_) {
    OZ(write_single_char_to_file(&char_enclose_, data_writer));
  }
  return ret;
}

int ObSelectIntoOp::print_field(const ObObj &obj, ObIOBufferWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char char_n = 'N';
  const bool need_enclose = has_enclose_ && !obj.is_null()
                            && (!MY_SPEC.is_optional_ || obj.is_string_type());
  if (need_enclose) {
    OZ(write_single_char_to_file(&char_enclose_, data_writer));
  }
  if (!has_escape_) {
    OZ(write_obj_to_file(obj, data_writer, false));
  } else if (obj.is_null()) {
    OZ(write_single_char_to_file(&char_escape_, data_writer));
    OZ(write_single_char_to_file(&char_n, data_writer));
  } else {
    OZ(write_obj_to_file(obj, data_writer, true));
  }
  if (need_enclose) {
    OZ(write_single_char_to_file(&char_enclose_, data_writer));
  }
  return ret;
}

int ObSelectIntoOp::into_outfile(ObIOBufferWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObDatum *datum = NULL;
  ObObj obj;
  ObDatum *partition_datum = NULL;
  if (do_partition_) {
    if (OB_FAIL(MY_SPEC.file_partition_expr_->eval(eval_ctx_, partition_datum))) {
      LOG_WARN("eval expr failed", K(ret));
    } else if (OB_FAIL(get_data_writer_for_partition(partition_datum, data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(data_writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null data writer", K(ret));
  }
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
      OZ(print_field(obj, *data_writer));
    } else { // text tc
      OZ(print_lob_field(obj, *select_exprs.at(i), *datum, *data_writer));
    }
    // print field terminator
    if (OB_SUCC(ret) && i != select_exprs.count() - 1) {
      OZ(write_obj_to_file(MY_SPEC.field_str_, *data_writer));
    }
  }
  // print line terminator
  OZ(write_obj_to_file(MY_SPEC.line_str_, *data_writer));
  // check if need split file
  OZ(try_split_file(*data_writer));
  // clear shared buffer
  OZ(flush_shared_buf(*data_writer, get_flush_function()));
  return ret;
}

#ifdef OB_BUILD_CPP_ODPS
int ObSelectIntoOp::into_odps()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  apsara::odps::sdk::ODPSTableRecordPtr table_record;
  ObDatum *datum = NULL;
  try {
    if (OB_UNLIKELY(!upload_ || !record_writer_ || !(table_record = upload_->CreateBufferRecord())
                    || !(table_record->GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (table_record->GetSchema()->GetColumnCount() != select_exprs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_WARN(OB_NOT_SUPPORTED, "insert into partial column in external table");
      LOG_WARN("column count of odps record is not equal to count of select exprs",
               K(table_record->GetSchema()->GetColumnCount()), K(select_exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
        if (OB_ISNULL(select_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select expr is unexpected null", K(ret));
        } else if (OB_FAIL(select_exprs.at(i)->eval(eval_ctx_, datum))) {
          LOG_WARN("eval expr failed", K(ret));
        } else if (OB_ISNULL(datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is unexpected null", K(ret));
        } else if (lib::is_mysql_mode()
                   && OB_FAIL(set_odps_column_value_mysql(*table_record, *datum,
                                                          select_exprs.at(i)->datum_meta_,
                                                          select_exprs.at(i)->obj_meta_,
                                                          i))) {
          LOG_WARN("failed to set odps column value", K(ret));
        } else if (lib::is_oracle_mode()
                   && OB_FAIL(set_odps_column_value_oracle(*table_record, *datum,
                                                           select_exprs.at(i)->datum_meta_,
                                                           select_exprs.at(i)->obj_meta_,
                                                           i))) {
          LOG_WARN("failed to set odps column value", K(ret));
        }
      }
      record_writer_->Write(*table_record);
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::into_odps_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<ObDatumVector> datum_vectors;
  ObDatum *datum = NULL;
  apsara::odps::sdk::ODPSTableRecordPtr table_record;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval_batch(eval_ctx_, *brs.skip_, brs.size_))) {
      LOG_WARN("failed to eval batch", K(ret), KPC(select_exprs.at(i)));
    } else if (OB_FAIL(datum_vectors.push_back(select_exprs.at(i)->locate_expr_datumvector(eval_ctx_)))) {
      LOG_WARN("failed to push back datum vector", K(ret));
    }
  }
  try {
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!upload_ || !record_writer_
                           || !(table_record = upload_->CreateBufferRecord())
                           || !(table_record->GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (table_record->GetSchema()->GetColumnCount() != select_exprs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert into partial column in external table");
      LOG_WARN("column count of odps record is not equal to count of select exprs",
               K(table_record->GetSchema()->GetColumnCount()), K(select_exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
        if (brs.skip_->contain(i)) {
          // do nothing
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < select_exprs.count(); ++j) {
            if (OB_ISNULL(datum = datum_vectors.at(j).at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("datum is unexpected null", K(ret));
            } else if (lib::is_mysql_mode()
                       && OB_FAIL(set_odps_column_value_mysql(*table_record, *datum,
                                                              select_exprs.at(j)->datum_meta_,
                                                              select_exprs.at(j)->obj_meta_,
                                                              j))) {
              LOG_WARN("failed to set odps column value", K(ret));
            } else if (lib::is_oracle_mode()
                       && OB_FAIL(set_odps_column_value_oracle(*table_record, *datum,
                                                               select_exprs.at(j)->datum_meta_,
                                                               select_exprs.at(j)->obj_meta_,
                                                               j))) {
              LOG_WARN("failed to set odps column value", K(ret));
            }
          }
          record_writer_->Write(*table_record);
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::set_odps_column_value_mysql(apsara::odps::sdk::ODPSTableRecord &table_record,
                                                const ObDatum &datum,
                                                const ObDatumMeta &datum_meta,
                                                const ObObjMeta &obj_meta,
                                                uint32_t col_idx)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  apsara::odps::sdk::ODPSColumnType odps_type;
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
  try {
    if (OB_UNLIKELY(!(table_record.GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (datum.is_null()) {
      table_record.SetNullValue(col_idx);
    } else {
      odps_type = table_record.GetSchema()->GetTableColumn(col_idx).GetType();
      switch (odps_type)
      {
        case apsara::odps::sdk::ODPS_BOOLEAN:
        {
          if (ObTinyIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_tinyint() != 0);
          } else if (ObSmallIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_smallint() != 0);
          } else if (ObMediumIntType == ob_type || ObInt32Type == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_int32() != 0);
          } else if (ObIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_int() != 0);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TINYINT:
        {
          table_record.SetTinyIntValue(col_idx, datum.get_tinyint());
          break;
        }
        case apsara::odps::sdk::ODPS_SMALLINT:
        {
          table_record.SetSmallIntValue(col_idx, datum.get_smallint());
          break;
        }
        case apsara::odps::sdk::ODPS_INTEGER:
        {
          table_record.SetIntegerValue(col_idx, datum.get_int32());
          break;
        }
        case apsara::odps::sdk::ODPS_BIGINT:
        {
          table_record.SetBigIntValue(col_idx, datum.get_int());
          break;
        }
        case apsara::odps::sdk::ODPS_FLOAT:
        {
          table_record.SetFloatValue(col_idx, datum.get_float());
          break;
        }
        case apsara::odps::sdk::ODPS_DOUBLE:
        {
          table_record.SetDoubleValue(col_idx, datum.get_double());
          break;
        }
        case apsara::odps::sdk::ODPS_DECIMAL:
        {
          std::string dec;
          if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
            LOG_WARN("failed to get string", K(ret));
          } else {
            table_record.SetDecimalValue(col_idx, dec);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_CHAR:
        case apsara::odps::sdk::ODPS_VARCHAR:
        {
          buf_size = datum.get_string().length() * ObCharset::MAX_MB_LEN;
          if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
            res_len = static_cast<uint32_t>(datum.get_string().length());
            buf = const_cast<char *>(datum.get_string().ptr());
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        datum.get_string().ptr(),
                                                        datum.get_string().length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if ((apsara::odps::sdk::ODPS_CHAR == odps_type && res_len > 255)
                     || (apsara::odps::sdk::ODPS_VARCHAR == odps_type && res_len > 65535)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (buf == NULL && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_STRING:
        case apsara::odps::sdk::ODPS_BINARY:
        {
          ObString lob_str;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                obj_meta.has_lob_header(),
                                                                lob_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (apsara::odps::sdk::ODPS_BINARY == odps_type
                     || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)
                     || CS_TYPE_BINARY == datum_meta.cs_type_) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        lob_str.ptr(),
                                                        lob_str.length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (res_len > 8 * 1024 * 1024) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (buf == NULL && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            LOG_DEBUG("debug select into lob", K(datum_meta.cs_type_), K(ObString(res_len, buf)));
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_JSON:
        {
          ObString json_str;
          ObIJsonBase *j_base = NULL;
          ObJsonBuffer jbuf(&allocator);
          ObJsonInType in_type = ObJsonInType::JSON_BIN;
          uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                obj_meta.has_lob_header(),
                                                                json_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, in_type,
                                                              in_type, j_base, parse_flag))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            LOG_DEBUG("debug select into json", K(datum_meta.cs_type_), K(ObString(jbuf.length(), jbuf.ptr())));
            table_record.SetJsonValue(col_idx, jbuf.ptr(), static_cast<uint32_t>(jbuf.length()));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TIMESTAMP:
        case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
        {
          int64_t us = apsara::odps::sdk::ODPS_TIMESTAMP == odps_type
                       ? datum.get_timestamp()
                       : datum.get_datetime();
          int64_t sec = us / 1000000;
          int32_t ns = (us % 1000000) * 1000;
          if (us < ORACLE_DATETIME_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps timestamp min value is 0001-01-01 00:00:00", K(ret), K(us));
          } else {
            table_record.SetTimeValue(col_idx, sec, ns, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_DATE:
        {
          if (datum.get_date() < ODPS_DATE_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps date min value is 0001-01-01", K(ret));
          } else {
            table_record.SetDateValue(col_idx, datum.get_date());
          }
          break;
        }
        case apsara::odps::sdk::ODPS_DATETIME:
        {
          int32_t tmp_offset = 0;
          if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else if (datum.get_datetime() < ORACLE_DATETIME_MIN_VAL + SEC_TO_USEC(tmp_offset)) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps datetime min value is 0001-01-01 00:00:00", K(ret));
          } else {
            table_record.SetDatetimeValue(col_idx, (datum.get_datetime() - SEC_TO_USEC(tmp_offset)) / 1000);
          }
          break;
        }
        default:
        {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ob_type), K(odps_type), K(ret));
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::set_odps_column_value_oracle(apsara::odps::sdk::ODPSTableRecord &table_record,
                                                 const ObDatum &datum,
                                                 const ObDatumMeta &datum_meta,
                                                 const ObObjMeta &obj_meta,
                                                 uint32_t col_idx)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  apsara::odps::sdk::ODPSColumnType odps_type;
  int64_t int_value = 0;
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
  try {
    if (OB_UNLIKELY(!(table_record.GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (datum.is_null()) {
      table_record.SetNullValue(col_idx);
    } else {
      odps_type = table_record.GetSchema()->GetTableColumn(col_idx).GetType();
      switch (odps_type)
      {
        case apsara::odps::sdk::ODPS_BOOLEAN:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            table_record.SetBoolValue(col_idx, int_value != 0);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TINYINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT8_MIN || int_value > INT8_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetTinyIntValue(col_idx, static_cast<int8_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_SMALLINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT16_MIN || int_value > INT16_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetSmallIntValue(col_idx, static_cast<int16_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_INTEGER:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT32_MIN || int_value > INT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetIntegerValue(col_idx, static_cast<int32_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_BIGINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            table_record.SetBigIntValue(col_idx, int_value);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_FLOAT:
        {
          table_record.SetFloatValue(col_idx, datum.get_float());
          break;
        }
        case apsara::odps::sdk::ODPS_DOUBLE:
        {
          table_record.SetDoubleValue(col_idx, datum.get_double());
          break;
        }
        case apsara::odps::sdk::ODPS_DECIMAL:
        {
          std::string dec;
          if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
            LOG_WARN("failed to get string", K(ret));
          } else {
            table_record.SetDecimalValue(col_idx, dec);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_CHAR:
        case apsara::odps::sdk::ODPS_VARCHAR:
        {
          buf_size = datum.get_string().length() * ObCharset::MAX_MB_LEN;
          if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
            res_len = static_cast<uint32_t>(datum.get_string().length());
            buf = const_cast<char *>(datum.get_string().ptr());
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        datum.get_string().ptr(),
                                                        datum.get_string().length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if ((apsara::odps::sdk::ODPS_CHAR == odps_type && res_len > 255)
                     || (apsara::odps::sdk::ODPS_VARCHAR == odps_type && res_len > 65535)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (buf == NULL && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_STRING:
        case apsara::odps::sdk::ODPS_BINARY:
        {
          ObString lob_str;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                obj_meta.has_lob_header(),
                                                                lob_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (apsara::odps::sdk::ODPS_BINARY == odps_type
                     || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)
                     || CS_TYPE_BINARY == datum_meta.cs_type_) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        lob_str.ptr(),
                                                        lob_str.length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (res_len > 8 * 1024 * 1024) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (buf == NULL && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_JSON:
        {
          ObString json_str;
          ObIJsonBase *j_base = NULL;
          ObJsonBuffer jbuf(&allocator);
          ObJsonInType in_type = ObJsonInType::JSON_BIN;
          uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                obj_meta.has_lob_header(),
                                                                json_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, in_type,
                                                              in_type, j_base, parse_flag))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            table_record.SetJsonValue(col_idx, jbuf.ptr(), static_cast<uint32_t>(jbuf.length()));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TIMESTAMP:
        case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          table_record.SetTimeValue(col_idx, timestamp.time_us_ / 1000000, timestamp.time_ctx_.tail_nsec_, odps_type);
          break;
        }
        case apsara::odps::sdk::ODPS_DATE:
        {
          table_record.SetDateValue(col_idx, datum.get_datetime() / 1000000 / 3600 / 24);
          break;
        }
        case apsara::odps::sdk::ODPS_DATETIME:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          int32_t tmp_offset = 0;
          if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else {
            table_record.SetDatetimeValue(col_idx, (timestamp.time_us_ - SEC_TO_USEC(tmp_offset)) / 1000);
          }
          break;
        }
        default:
        {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ob_type), K(odps_type), K(ret));
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret));
    }
  }
  return ret;
}
#endif

int ObSelectIntoOp::decimal_to_string(const ObDatum &datum,
                                      const ObDatumMeta &datum_meta,
                                      std::string &res,
                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(OB_CAST_TO_VARCHAR_MAX_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(wide::to_string(datum.get_decimal_int(), datum.get_int_bytes(), datum_meta.scale_,
                                     buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, pos))) {
    LOG_WARN("failed to get string", K(ret));
  } else {
    res.assign(buf, pos);
  }
  return ret;
}

int ObSelectIntoOp::decimal_or_number_to_int64(const ObDatum &datum,
                                               const ObDatumMeta &datum_meta,
                                               int64_t &res)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  if (ObNumberType == ob_type) {
    const number::ObNumber nmb(datum.get_number());
    if (OB_FAIL(nmb.extract_valid_int64_with_trunc(res))) {
      LOG_WARN("failed to cast number to int64", K(ret));
    }
  } else if (ObDecimalIntType == ob_type) {
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
    bool is_valid;
    if (OB_FAIL(wide::check_range_valid_int64(datum.get_decimal_int(), int_bytes, is_valid, res))) {
      LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decimal int is not valid int64", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ob_type), K(ret));
  }
  return ret;
}

int ObSelectIntoOp::into_outfile_batch(const ObBatchRows &brs, ObIOBufferWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<ObDatumVector> datum_vectors;
  ObDatum *datum = NULL;
  ObObj obj;
  ObDatumVector partition_datum_vector;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_FAIL(select_exprs.at(i)->eval_batch(eval_ctx_, *brs.skip_, brs.size_))) {
      LOG_WARN("failed to eval batch", K(ret));
    } else if (OB_FAIL(datum_vectors.push_back(select_exprs.at(i)->locate_expr_datumvector(eval_ctx_)))) {
      LOG_WARN("failed to push back datum vector", K(ret));
    }
  }
  if (OB_SUCC(ret) && do_partition_) {
    if (OB_FAIL(MY_SPEC.file_partition_expr_->eval_batch(eval_ctx_, *brs.skip_, brs.size_))) {
      LOG_WARN("failed to eval batch", K(ret));
    } else {
      partition_datum_vector = MY_SPEC.file_partition_expr_->locate_expr_datumvector(eval_ctx_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
    if (brs.skip_->contain(i)) {
      // do nothing
    } else if (do_partition_ && OB_FAIL(get_data_writer_for_partition(partition_datum_vector.at(i),
                                                                      data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    } else if (OB_ISNULL(data_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
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
          OZ(print_field(obj, *data_writer));
        } else { // text tc
          OZ(print_lob_field(obj, *select_exprs.at(j), *datum, *data_writer));
        }
        // print field terminator
        if (OB_SUCC(ret) && j != select_exprs.count() - 1) {
          OZ(write_obj_to_file(MY_SPEC.field_str_, *data_writer));
        }
      }
      // print line terminator
      OZ(write_obj_to_file(MY_SPEC.line_str_, *data_writer));
      // check if need split file
      OZ(try_split_file(*data_writer));
      // clear shared buffer
      OZ(flush_shared_buf(*data_writer, get_flush_function()));
    }
  }
  return ret;
}

int ObSelectIntoOp::into_dumpfile(ObIOBufferWriter *data_writer)
{
  int ret = OB_SUCCESS;
  char buf[MAX_VALUE_LENGTH];
  int64_t buf_len = MAX_VALUE_LENGTH;
  int64_t pos = 0;
  if (OB_ISNULL(data_writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_row_str(buf_len, is_first_, buf, pos))) {
    LOG_WARN("get str failed", K(ret));
  } else if (is_first_) { // create file
    if (OB_FAIL(data_writer->file_appender_.create(file_name_.get_varchar(), true))) {
      LOG_WARN("create dumpfile failed", K(ret), K(file_name_));
    } else {
      is_first_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(data_writer->file_appender_.append(buf, pos, false))) {
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
    LOG_WARN("failed to allocate buffer", K(ret), K(buf_len));
  }
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_enclose, escape_printer_.enclose_, MY_SPEC.cs_type_)); //todo@linyi if has_enclose_
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_escape, escape_printer_.escape_, MY_SPEC.cs_type_)); //todo@linyi
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

int ObSelectIntoOp::create_shared_buffer_for_data_writer()
{
  int ret = OB_SUCCESS;
  shared_buf_len_ = has_lob_ ? (5 * SHARED_BUFFER_SIZE) : SHARED_BUFFER_SIZE;
  if (OB_ISNULL(shared_buf_ = static_cast<char*>(ctx_.get_allocator().alloc(shared_buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(shared_buf_len_));
  }
  if (OB_SUCC(ret) && has_json_ && has_escape_) {
    json_buf_len_ = OB_MALLOC_MIDDLE_BLOCK_SIZE;
    if (OB_ISNULL(json_buf_ = static_cast<char*>(ctx_.get_allocator().alloc(json_buf_len_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer", K(ret), K(json_buf_len_));
    }
  }
  return ret;
}

int ObSelectIntoOp::check_secure_file_path(ObString file_name)
{
  int ret = OB_SUCCESS;
  ObString file_path = file_name.split_on(file_name.reverse_find('/'));
  char full_path_buf[PATH_MAX+1];
  char *actual_path = nullptr;
  ObSqlString sql_str;
  ObString secure_file_priv;
  int64_t tenant_id = MTL_ID();
  if (OB_FAIL(sql_str.append(file_path.empty() ? "." : file_path))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_ISNULL(actual_path = realpath(sql_str.ptr(), full_path_buf))) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("file not exist", K(ret), K(sql_str));
  } else if (OB_FAIL(ObSchemaUtils::get_tenant_varchar_variable(tenant_id,
                                                                SYS_VAR_SECURE_FILE_PRIV,
                                                                ctx_.get_allocator(),
                                                                secure_file_priv))) {
    LOG_WARN("fail get tenant variable", K(tenant_id), K(secure_file_priv), K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, actual_path))) {
    LOG_WARN("failed to check secure path", K(ret), K(secure_file_priv));
    if (OB_ERR_NO_PRIVILEGE == ret) {
      ret = OB_ERR_NO_PRIV_DIRECT_PATH_ACCESS;
      LOG_ERROR("failed to check secure path", K(ret), K(secure_file_priv));
    }
  }
  return ret;
}

int ObSelectIntoOp::get_data_writer_for_partition(ObDatum *partition_datum,
                                                  ObIOBufferWriter *&data_writer)
{
  int ret = OB_SUCCESS;
  ObString partition;
  void *ptr = NULL;
  ObIOBufferWriter *value = NULL;
  const int64_t buf_len = MY_SPEC.buffer_size_;
  char *buf = NULL;
  if (OB_ISNULL(partition_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_SUCC(partition_map_.get_refactored(partition_datum->get_string(), value))) {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      data_writer = value;
    }
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)){
    LOG_WARN("get unexpected error", K(ret));
  } else if (curr_partition_num_ >= OB_MAX_PARTITION_NUM_ORACLE) {
    ret = OB_TOO_MANY_PARTITIONS_ERROR;
    LOG_WARN("too many partitions", K(ret));
  } else {
    ret = OB_SUCCESS;
    //new data_writer
    if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObIOBufferWriter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObIOBufferWriter)));
    } else {
      data_writer = new(ptr) ObIOBufferWriter();
    }
    //init buffer
    if (OB_FAIL(ret) || buf_len <= 0) {
    } else if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer", K(ret), K(buf_len));
    } else {
      data_writer->init(buf, buf_len);
    }
    //add to hashmap
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(),
                                       partition_datum->get_string(),
                                       partition))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(partition_map_.set_refactored(partition, data_writer))) {
      LOG_WARN("failed to add data writer to map", K(ret));
    } else {
      curr_partition_num_++;
    }
    if (OB_FAIL(ret) && NULL != data_writer) {
      data_writer->~ObIOBufferWriter();
    }
    //calc file path
    if (OB_SUCC(ret) && OB_FAIL(calc_file_path_with_partition(partition, *data_writer))) {
      LOG_WARN("failed to calc file path with partition", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::create_the_only_data_writer(ObIOBufferWriter *&data_writer)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  const int64_t buf_len = MY_SPEC.buffer_size_;
  char *buf = NULL;
  if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObIOBufferWriter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObIOBufferWriter)));
  } else {
    data_writer = new(ptr) ObIOBufferWriter();
    data_writer->url_ = basic_url_;
    data_writer_ = data_writer;
  }
  if (OB_FAIL(ret)) {
  } else if (T_INTO_OUTFILE == MY_SPEC.into_type_ && MY_SPEC.is_single_
             && OB_FAIL(open_file(*data_writer))) {
    LOG_WARN("failed to open file", K(ret));
  } else if (buf_len <= 0) {
  } else if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(buf_len));
  } else {
    data_writer->init(buf, buf_len);
  }
  if (OB_FAIL(ret) && NULL != data_writer) {
    data_writer->~ObIOBufferWriter();
  }
  return ret;
}

#ifdef OB_BUILD_CPP_ODPS
int ObSelectIntoOp::odps_commit_upload()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  if (is_in_px) {
    ObOdpsPartitionDownloaderMgr &odps_mgr = ctx_.get_sqc_handler()->get_sqc_ctx().gi_pump_.get_odps_mgr();
    if (!need_commit_) {
      odps_mgr.set_fail();
    }
    __sync_synchronize();
    int64_t ref = odps_mgr.dec_ref();
    if (0 > ref) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected ref", K(ref), K(ret));
    } else if (0 == ref && OB_FAIL(odps_mgr.commit_upload())) {
      LOG_WARN("failed to commit upload", K(ret));
    }
  } else {
    std::vector<uint32_t> blocks;
    try {
      if (OB_UNLIKELY(!record_writer_ || !upload_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else {
        record_writer_->Close();
        blocks.push_back(block_id_);
        if (need_commit_) {
          upload_->Commit(blocks);
        }
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret));
      }
    }
  }
  return ret;
}
#endif

void ObSelectIntoOp::destroy()
{
  ObIOBufferWriter *data_writer = NULL;
  if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
#ifdef OB_BUILD_CPP_ODPS
    upload_.reset();
    record_writer_.reset();
#endif
  } else if (do_partition_) {
    for (ObPartitionWriterMap::iterator iter = partition_map_.begin();
         iter != partition_map_.end(); iter++) {
      if (OB_ISNULL(data_writer = iter->second)) {
      } else {
        close_file(*data_writer);
        data_writer->~ObIOBufferWriter();
      }
    }
  } else if (OB_NOT_NULL(data_writer_)) {
    close_file(*data_writer_);
    data_writer_->~ObIOBufferWriter();
  }
  external_properties_.~ObExternalFileFormat();
  partition_map_.destroy();
  ObOperator::destroy();
}



}
}
