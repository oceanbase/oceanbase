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

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/writer.h>
#include <parquet/exception.h>
#include <cmath>
#include <orc/Writer.hh>
#include <orc/OrcFile.hh>
#include <orc/Type.hh>
#include <memory>

#include "ob_select_into_op.h"
#include "sql/engine/cmd/ob_variable_set_executor.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/udt/ob_collection_type.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/connector/ob_java_env.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"

#include <arrow/c/bridge.h>
#include <arrow/array.h>


namespace oceanbase
{
using namespace common;
namespace sql
{


OB_SERIALIZE_MEMBER(ObSelectIntoOpInput, task_id_, sqc_id_);
OB_SERIALIZE_MEMBER((ObSelectIntoSpec, ObOpSpec), into_type_, user_vars_, outfile_name_,
    field_str_, // FARM COMPAT WHITELIST FOR filed_str_: renamed
    line_str_, closed_cht_, is_optional_, select_exprs_, is_single_, max_file_size_,
    escaped_cht_, cs_type_, parallel_, file_partition_expr_, buffer_size_, is_overwrite_,
    external_properties_, external_partition_, alias_names_);


int ObSelectIntoOp::inner_open()
{
  int ret = OB_SUCCESS;
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
        if (!GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS)
          is_odps_cpp_table_ = true;
          if (OB_FAIL(init_odps_tunnel())) {
            LOG_WARN("failed to init odps tunnel", K(ret));
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support odps format", K(ret));
#endif
        } else {
#if defined(OB_BUILD_JNI_ODPS)
          is_odps_java_table_ = true;
          if (OB_FAIL(init_odps_jni_tunnel())) {
            LOG_WARN("failed to init odps jni", K(ret));
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support odps format", K(ret));
#endif
        }
        break;
      }
      case ObExternalFileFormat::FormatType::PARQUET_FORMAT:
      {
        if (OB_FAIL(init_parquet_env())) {
          LOG_WARN("failed to init csv env", K(ret));
        }
        break;
      }
      case ObExternalFileFormat::FormatType::ORC_FORMAT:
      {
        if (OB_FAIL(init_orc_env())) {
          LOG_WARN("failed to init csv env", K(ret));
        }
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
  ObSQLSessionInfo *session = NULL;
  set_csv_format_options();
  if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed", K(ret));
  } else if (OB_FAIL(init_env_common())) {
    LOG_WARN("failed to init env common", K(ret));
  } else if (OB_FAIL(prepare_escape_printer())) {
    LOG_WARN("failed to calc escape info", K(ret));
  } else {
    if (external_properties_.csv_format_.compression_algorithm_ != CsvCompressType::NONE) {
      has_compress_ = true;
    }
    // setup binary output format for bit/binary
    switch (external_properties_.csv_format_.binary_format_) {
      case ObCSVGeneralFormat::ObCSVBinaryFormat::DEFAULT:
        print_params_.binary_string_print_hex_ = lib::is_oracle_mode();
        break;
      case ObCSVGeneralFormat::ObCSVBinaryFormat::HEX:
        print_params_.binary_string_print_hex_ = true;
        break;
      case ObCSVGeneralFormat::ObCSVBinaryFormat::BASE64:
        print_params_.binary_string_print_base64_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set csv binary output format", K(ret));
    }
    print_params_.tz_info_ = session->get_timezone_info();
    print_params_.use_memcpy_ = true;
    print_params_.cs_type_ = cs_type_;
  }
  //create buffer
  if (OB_SUCC(ret) && T_INTO_OUTFILE == MY_SPEC.into_type_ && OB_FAIL(create_shared_buffer_for_data_writer())) {
    LOG_WARN("failed to create buffer for data writer", K(ret));
  }
  return ret;
}

void ObSelectIntoOp::set_csv_format_options()
{
  if (MY_SPEC.external_properties_.str_.empty()) {
    field_str_ = MY_SPEC.field_str_;
    line_str_ = MY_SPEC.line_str_;
    has_enclose_ = MY_SPEC.closed_cht_.get_val_len() > 0;
    char_enclose_ = has_enclose_ ? MY_SPEC.closed_cht_.get_char().ptr()[0] : 0;
    is_optional_ = MY_SPEC.is_optional_;
    has_escape_ = MY_SPEC.escaped_cht_.get_val_len() > 0;
    char_escape_ = has_escape_ ? MY_SPEC.escaped_cht_.get_char().ptr()[0] : 0;
    cs_type_ = MY_SPEC.cs_type_;
  } else {
    is_optional_ = external_properties_.csv_format_.is_optional_;
    cs_type_ = ObCharset::get_default_collation(external_properties_.csv_format_.cs_type_);
    field_str_.set_varchar(external_properties_.csv_format_.field_term_str_);
    field_str_.set_collation_type(cs_type_);
    line_str_.set_varchar(external_properties_.csv_format_.line_term_str_);
    line_str_.set_collation_type(cs_type_);
    if (external_properties_.csv_format_.field_enclosed_char_ == INT64_MAX) { // null
      has_enclose_ = false;
      char_enclose_ = 0;
    } else {
      has_enclose_ = true;
      char_enclose_ = external_properties_.csv_format_.field_enclosed_char_;
    }
    if (external_properties_.csv_format_.field_escaped_char_ == INT64_MAX) { // null
      has_escape_ = false;
      char_escape_ = 0;
    } else {
      has_escape_ = true;
      char_escape_ = external_properties_.csv_format_.field_escaped_char_;
    }
  }
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
#ifdef OB_BUILD_JNI_ODPS

int ObSelectIntoOp::init_odps_jni_tunnel()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(input_);
  if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is unexpected null", K(ret));
  }
  arrow_alloc_.init(MTL_ID());

  ObJavaEnv &java_env = ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!java_env.is_env_inited()) {
    if (OB_FAIL(java_env.setup_java_env())) {
      LOG_WARN("failed to setup java env", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_in_px) {
    int task_id = input->task_id_;
    ObOdpsJniUploaderMgr &odps_mgr = ctx_.get_sqc_handler()->get_sqc_ctx().gi_pump_.get_odps_jni_uploader_mgr();
    if (OB_FAIL(odps_mgr.get_odps_uploader_in_px(task_id, MY_SPEC.select_exprs_, uploader_))) {
      LOG_WARN("failed to get odps writer from global config", K(ret));
    } else if (OB_FAIL(create_odps_schema())) {
      LOG_WARN("failed to create the odps schema", K(ret));
    }
  } else {
    common::hash::ObHashMap<ObString, ObString> writer_params;

    if (OB_FAIL(external_properties_.odps_format_.decrypt())) {
      LOG_WARN("failed to decrypt odps format", K(ret));
    } else if (OB_FAIL(ObOdpsJniUploaderMgr::create_writer_params_map(ctx_.get_allocator(),
                                                                      external_properties_.odps_format_,
                                                                      MY_SPEC.external_partition_.str_,
                                                                      MY_SPEC.is_overwrite_,
                                                                      writer_params))) {
      LOG_WARN("failed to create params from odps format", K(ret));
    } else if (FALSE_IT(uploader_.writer_ptr = create_odps_jni_writer())) {
      /* do nothing */
    } else if (OB_FAIL(uploader_.writer_ptr->init_params(writer_params))) {
      LOG_WARN("failed to init writer params", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->do_open())) {
      LOG_WARN("failed to do open writer", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->do_open_record(block_id_))) {
      LOG_WARN("failed to open record writer", K(ret));
    } else if(OB_FAIL(create_odps_schema())) {
      LOG_WARN("failed to create the odps schema", K(ret));
    }

    if (writer_params.created()) {
      writer_params.destroy();
    }
  }
  if (OB_FAIL(ret)) {
    need_commit_ = false;
  }
  return ret;
}
#endif

int ObSelectIntoOp::init_parquet_env()
{
  int ret = OB_SUCCESS;
  arrow_alloc_.init(MTL_ID());
  if (OB_FAIL(setup_parquet_schema())) {
    LOG_WARN("failed to set up parquet schema", K(ret));
  } else if (OB_FAIL(init_env_common())) {
    LOG_WARN("failed to init env common", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::init_orc_env()
{
  int ret = OB_SUCCESS;
  orc_alloc_.init(MTL_ID());
  if (OB_FAIL(setup_orc_schema())) {
    LOG_WARN("failed to set up orc schema", K(ret));
  } else if (OB_FAIL(init_env_common())) {
    LOG_WARN("failed to init env common", K(ret));
  } else if (external_properties_.orc_format_.compression_block_size_ < 100) { // parameter guard
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("compress block is too low or too high", K(external_properties_.orc_format_.compression_block_size_));
  } else {
    options_.setStripeSize(external_properties_.orc_format_.stripe_size_)
            .setRowIndexStride(external_properties_.orc_format_.row_index_stride_)
            .setCompressionBlockSize(external_properties_.orc_format_.compression_block_size_)
            .setCompression(static_cast<orc::CompressionKind>(external_properties_.orc_format_.compress_type_index_))
            .setMemoryPool(&orc_alloc_);
  }
  return ret;
}

int ObSelectIntoOp::init_env_common()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  bool need_check = false;
  file_name_ = MY_SPEC.outfile_name_;
  do_partition_ = MY_SPEC.file_partition_expr_ == NULL ? false : true;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  } else if (OB_FAIL(ObSQLUtils::get_param_value(MY_SPEC.outfile_name_,
                                                 phy_plan_ctx->get_param_store(),
                                                 file_name_,
                                                 need_check))) {
    LOG_WARN("get param value failed", K(ret));
  } else if (OB_FAIL(calc_url_and_set_access_info())) {
    LOG_WARN("failed to calc basic url and set device handle", K(ret));
  } else if (OB_FAIL(check_has_lob_or_json())) {
    LOG_WARN("failed to check has lob", K(ret));
  } else if (do_partition_
             && OB_FAIL(partition_map_.create(128, ObLabel("SelectInto"), ObLabel("SelectInto"), MTL_ID()))) {
    LOG_WARN("failed to create hashmap", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_0
             && MY_SPEC.select_exprs_.count() != MY_SPEC.alias_names_.strs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(MY_SPEC.select_exprs_.count()),
              K(MY_SPEC.alias_names_.strs_.count()), K(ret));
  }
  return ret;
}

//calc first data_writer.url_ and basic_url_
int ObSelectIntoOp::calc_url_and_set_access_info()
{
  int ret = OB_SUCCESS;
  const ObItemType into_type = MY_SPEC.into_type_;
  ObString path = file_name_.get_varchar().trim();
  if (path.prefix_match_ci(OB_OSS_PREFIX)) {
    file_location_ = IntoFileLocation::REMOTE_OSS;
  } else if (path.prefix_match_ci(OB_COS_PREFIX)) {
    file_location_ = IntoFileLocation::REMOTE_COS;
  } else if (path.prefix_match_ci(OB_S3_PREFIX)) {
    file_location_ = IntoFileLocation::REMOTE_S3;
  } else {
    file_location_ = IntoFileLocation::SERVER_DISK;
  }
  if (file_location_ == IntoFileLocation::SERVER_DISK && do_partition_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support partition option on server disk", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition option on server disk");
  } else if (T_INTO_OUTFILE == into_type && !MY_SPEC.is_single_ && OB_FAIL(calc_first_file_path(path))) {
    LOG_WARN("failed to calc first file path", K(ret));
  } else if (file_location_ != IntoFileLocation::SERVER_DISK) {
    ObString temp_url = path.split_on('?');
    temp_url.trim();
    ObString storage_info;
    if (OB_FAIL(ob_write_string(ctx_.get_allocator(), temp_url, basic_url_, true))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, storage_info, true))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FAIL(access_info_.set(basic_url_.ptr(), storage_info.ptr()))) {
      LOG_WARN("failed to set access info", K(ret), K(path));
    } else if (basic_url_.empty() || !access_info_.is_valid()) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file path not exist", K(ret), K(basic_url_), K(access_info_));
    }
  } else { // IntoFileLocation::SERVER_DISK
    if (OB_FAIL(ob_write_string(ctx_.get_allocator(), path, basic_url_, true))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  if (OB_SUCC(ret) && (T_INTO_OUTFILE == into_type || T_INTO_DUMPFILE == into_type)
      && IntoFileLocation::SERVER_DISK == file_location_ && OB_FAIL(check_secure_file_path(basic_url_))) {
    LOG_WARN("failed to check secure file path", K(ret));
  }
  return ret;
}

// csv, odps支持batch和非batch接口; parquet, orc只支持batch接口; 非batch接口之后会取消
int ObSelectIntoOp::inner_get_next_row()
{
  int ret = 0 == top_limit_cnt_ ? OB_ITER_END : OB_SUCCESS;
  int64_t row_count = 0;
  const ObItemType into_type = MY_SPEC.into_type_;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObExternalFileWriter *data_writer = NULL;
  if (ObExternalFileFormat::FormatType::CSV_FORMAT != format_type_
      && ObExternalFileFormat::FormatType::ODPS_FORMAT != format_type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("this type not supported in not batch interface", K(ret), K(format_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this upload type");
  } else if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
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
        if (is_odps_cpp_table_ == is_odps_java_table_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid table mode for odps table", K(ret),
                   K(is_odps_cpp_table_), K(is_odps_java_table_));
        } else if (is_odps_cpp_table_) {
#if defined (OB_BUILD_CPP_ODPS)
          if (OB_FAIL(into_odps())) {
            LOG_WARN("into odps failed", K(ret));
          }
#else
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps cpp table");
          LOG_WARN("use supported version", K(ret));
#endif
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
          LOG_WARN("not support jni odps single write", K(ret));
        }
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
  ObExternalFileWriter *data_writer = NULL;
  bool stop_loop = false;
  bool is_iter_end = false;
  if (OB_ISNULL(phy_plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get phy_plan_ctx failed", K(ret));
  }
  //when do_partition is false, create the only data_writer here
  if (OB_SUCC(ret) && T_INTO_VARIABLES != into_type && !do_partition_
      && (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_
          || ObExternalFileFormat::FormatType::PARQUET_FORMAT == format_type_
          || ObExternalFileFormat::FormatType::ORC_FORMAT == format_type_)) {
    if (OB_FAIL(create_the_only_data_writer(data_writer))) {
      LOG_WARN("failed to create the only data writer", K(ret));
    } else if (OB_ISNULL(data_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
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
          if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
            if (OB_FAIL(into_odps_batch(brs_))) {
              LOG_WARN("into odps batch failed", K(ret));
            }
#else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("odps cpp connector is not supported", K(ret));
#endif
          } else {
#if defined (OB_BUILD_JNI_ODPS)
            if (OB_FAIL(into_odps_jni_batch(brs_))) {
              LOG_WARN("into odps batch failed", K(ret));
            }
#else
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("odps jni connector is not supported", K(ret));
#endif
          }
        } else if (T_INTO_OUTFILE == into_type) {
          if (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_) {
            if (OB_FAIL(into_outfile_batch_csv(brs_, data_writer))) {
              LOG_WARN("csv into outfile batch failed", K(ret));
            }
          } else if (ObExternalFileFormat::FormatType::PARQUET_FORMAT == format_type_) {
            if (OB_FAIL(into_outfile_batch_parquet(brs_, data_writer))) {
              LOG_WARN("parquet into outfile batch failed", K(ret));
            }
          } else if (ObExternalFileFormat::FormatType::ORC_FORMAT == format_type_) {
            if (OB_FAIL(into_outfile_batch_orc(brs_, data_writer))) {
              LOG_WARN("orc into outfile batch failed", K(ret));
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support to write into outfile format.", K(ret), K(format_type_));
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
  ObExternalFileWriter *data_writer = NULL;
  int64_t estimated_bytes = 0;
  if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
    if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      if (OB_FAIL(odps_commit_upload())) {
        LOG_WARN("failed to commit upload", K(ret));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("odps jni connector is not supported", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (OB_FAIL(odps_jni_commit_upload())) {
        LOG_WARN("failed to commit upload", K(ret));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("odps jni connector is not supported", K(ret));
#endif
    }
  } else if (do_partition_) {
    for (ObPartitionWriterMap::iterator iter = partition_map_.begin();
         OB_SUCC(ret) && iter != partition_map_.end(); iter++) {
      if (OB_ISNULL(data_writer = iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data writer is unexpected null", K(ret));
      } else if (OB_FAIL(data_writer->close_data_writer())) {
        LOG_WARN("failed to close data writer", K(ret));
      }
    }
  } else if (OB_NOT_NULL(data_writer_) && OB_FAIL(data_writer_->close_data_writer())) {
    LOG_WARN("failed to close data writer", K(ret));
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
    if (0 != closed_cht && (!is_optional_ || ob_is_string_type(expr->datum_meta_.type_))) {
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
      } else if (0 != closed_cht && (!is_optional_ || ob_is_string_type(expr->datum_meta_.type_))) {
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

int ObSelectIntoOp::calc_first_file_path(ObString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString file_name_with_suffix;
  ObString file_extension;
  ObSelectIntoOpInput *input = static_cast<ObSelectIntoOpInput*>(input_);
  ObString input_file_name = file_location_ != IntoFileLocation::SERVER_DISK
                             ? path.split_on('?').trim()
                             : path;
  if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op input is null", K(ret));
  } else if (input_file_name.length() == 0 || path.length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid outfile path");
    LOG_WARN("invalid outfile path", K(ret));
  } else {
    if (input_file_name.ptr()[input_file_name.length() - 1] == '/'){
      OZ(file_name_with_suffix.append_fmt("%.*sdata", input_file_name.length(), input_file_name.ptr()));
    } else {
      OZ(file_name_with_suffix.append_fmt("%.*s", input_file_name.length(), input_file_name.ptr()));
    }
    if (MY_SPEC.parallel_ > 1) {
      OZ(file_name_with_suffix.append_fmt("_%ld_%ld_%d", input->sqc_id_, input->task_id_, 0));
    } else {
      OZ(file_name_with_suffix.append_fmt("_%d", 0));
    }
    OZ(external_properties_.get_format_file_extension(format_type_, file_extension));
    if (!file_extension.empty() && file_extension.ptr()[0] != '.') {
      OZ(file_name_with_suffix.append("."));
    }
    OZ(file_name_with_suffix.append(file_extension));
    if (format_type_ == ObExternalFileFormat::FormatType::CSV_FORMAT) {
      OZ(file_name_with_suffix.append(compression_algorithm_to_suffix(external_properties_.csv_format_.compression_algorithm_)));
    }
    if (file_location_ != IntoFileLocation::SERVER_DISK) {
      OZ(file_name_with_suffix.append_fmt("?%.*s", path.length(), path.ptr()));
    }
    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(ctx_.get_allocator(), file_name_with_suffix.string(), path))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::calc_next_file_path(ObExternalFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  ObSqlString url_with_suffix;
  ObString file_path;
  data_writer.split_file_id_++;
  if (data_writer.split_file_id_ > 0) {
    if (MY_SPEC.is_single_ && IntoFileLocation::SERVER_DISK != file_location_) {
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
    if (!MY_SPEC.is_single_) {
      ObString file_extension;
      OZ(external_properties_.get_format_file_extension(format_type_, file_extension));
      if (!file_extension.empty() && file_extension.ptr()[0] != '.') {
        OZ(url_with_suffix.append("."));
      }
      OZ(url_with_suffix.append(file_extension));
    }
    if (!MY_SPEC.is_single_
        && format_type_ == ObExternalFileFormat::FormatType::CSV_FORMAT) {
      OZ(url_with_suffix.append(compression_algorithm_to_suffix(external_properties_.csv_format_.compression_algorithm_)));
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
int ObSelectIntoOp::calc_file_path_with_partition(ObString partition, ObExternalFileWriter &data_writer)
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

int ObSelectIntoOp::split_file(ObExternalFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  if (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_) {
    ObCsvFileWriter *csv_data_writer = static_cast<ObCsvFileWriter*>(&data_writer);
    if (OB_ISNULL(csv_data_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    } else if (!use_shared_buf_ && OB_FAIL(csv_data_writer->flush_buf())) {
      LOG_WARN("failed to flush buffer", K(ret));
    } else if (has_lob_ && use_shared_buf_ && OB_FAIL(csv_data_writer->flush_shared_buf(shared_buf_))) {
      // 要保证文件中每一行的完整性, 有lob的时候shared buffer里不一定是完整的一行
      // 因此剩下的shared buffer里的内容也要刷到当前文件里, 这种情况下无法严格满足max_file_size的限制
      LOG_WARN("failed to flush shared buffer", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(data_writer.close_file())) {
    LOG_WARN("failed to close file", K(ret));
  } else if (OB_FAIL(calc_next_file_path(data_writer))) {
    LOG_WARN("failed to calculate new file path", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::check_csv_file_size(ObCsvFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  int64_t curr_bytes = data_writer.get_file_size();
  int64_t curr_bytes_exclude_curr_line = data_writer.get_curr_bytes_exclude_curr_line();
  int64_t curr_line_len = curr_bytes - curr_bytes_exclude_curr_line;
  bool has_split = false;
  bool has_use_shared_buf = use_shared_buf_;
  if (has_compress_ && OB_ISNULL(data_writer.get_compress_stream_writer())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null compress stream writer", K(ret));
  } else if (!(has_lob_ && has_use_shared_buf) && curr_bytes_exclude_curr_line == 0) {
  } else if (file_need_split(curr_bytes)) {
    if (OB_FAIL(split_file(data_writer))) {
      LOG_WARN("failed to split file", K(ret));
    } else {
      has_split = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (has_lob_ && has_use_shared_buf) {
      if (!has_compress_) {
        data_writer.set_write_bytes(has_split ? 0 : curr_bytes);
      }
      data_writer.reset_curr_line_len();
    } else {
      if (!has_compress_) {
        data_writer.set_write_bytes(has_split ? curr_line_len : curr_bytes);
      }
    }
    if (has_compress_ && has_split) {
      data_writer.get_compress_stream_writer()->reuse();
    }
    data_writer.update_last_line_pos();
  }
  return ret;
}

int ObSelectIntoOp::get_buf(char* &buf, int64_t &buf_len, int64_t &pos, ObCsvFileWriter &data_writer)
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

int ObSelectIntoOp::use_shared_buf(ObCsvFileWriter &data_writer,
                                   char* &buf,
                                   int64_t &buf_len,
                                   int64_t &pos)
{
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

int ObSelectIntoOp::resize_or_flush_shared_buf(ObCsvFileWriter &data_writer,
                                               char* &buf,
                                               int64_t &buf_len,
                                               int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!use_shared_buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(use_shared_buf_), K(ret));
  } else if (has_lob_ && data_writer.get_curr_pos() > 0) {
    if (OB_FAIL(data_writer.flush_shared_buf(shared_buf_, true))) {
      LOG_WARN("failed to flush shared buffer", K(ret));
    } else {
      pos = 0;
    }
  } else if (OB_FAIL(resize_buf(buf, buf_len, pos, data_writer.get_curr_pos()))) {
    LOG_WARN("failed to resize shared buffer", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::check_buf_sufficient(ObCsvFileWriter &data_writer,
                                         char* &buf,
                                         int64_t &buf_len,
                                         int64_t &pos,
                                         int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (buf_len < str_len * 1.1) {
    if (OB_FAIL(data_writer.flush_buf())) {
      LOG_WARN("failed to flush buffer", K(ret));
    } else if (OB_FAIL(use_shared_buf(data_writer, buf, buf_len, pos))) {
      LOG_WARN("failed to use shared buffer", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::write_obj_to_file(const ObObj &obj, ObCsvFileWriter &data_writer, bool need_escape)
{
  int ret = OB_SUCCESS;
  // binary collation do not require to escape when encode with base64/hex
  if (obj.get_collation_type() == CS_TYPE_BINARY &&
      (print_params_.binary_string_print_hex_ || print_params_.binary_string_print_base64_)) {
    need_escape = false;
  }

  if ((obj.is_string_type() || obj.is_json() || obj.is_collection_sql_type()) && need_escape) {
    if (OB_FAIL(print_str_or_json_with_escape(obj, data_writer))) {
      LOG_WARN("failed to print str or json with escape", K(ret));
    }
  } else if (OB_FAIL(print_normal_obj_without_escape(obj, data_writer))) {
    LOG_WARN("failed to print normal obj without escape", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::print_str_or_json_with_escape(const ObObj &obj, ObCsvFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
  ObCharsetType dst_type = ObCharset::charset_type_by_coll(cs_type_);
  escape_printer_.do_encode_ = !(src_type == CHARSET_BINARY || src_type == dst_type
                                 || src_type == CHARSET_INVALID);
  escape_printer_.need_enclose_ = has_enclose_ && !obj.is_null();
  escape_printer_.do_escape_ = true;
  escape_printer_.print_hex_ = obj.get_collation_type() == CS_TYPE_BINARY
                               && print_params_.binary_string_print_hex_;
  ObString str_to_escape;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (OB_FAIL(get_buf(escape_printer_.buf_, escape_printer_.buf_len_, escape_printer_.pos_, data_writer))) {
    LOG_WARN("failed to get buffer", K(ret));
  } else if (obj.is_json() || obj.is_collection_sql_type()) {
    ObObj inrow_obj = obj;
    if (obj.is_lob_storage()
        && OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(obj, inrow_obj, NULL, &temp_allocator))) {
      LOG_WARN("failed to convert outrow lobs", K(ret), K(obj));
    } else if (obj.is_collection_sql_type()) {
      ObSubSchemaValue sub_meta;
      if (OB_FAIL((get_exec_ctx().get_sqludt_meta_by_subschema_id(obj.get_meta().get_subschema_id(), sub_meta)))) {
        LOG_WARN("failed to get collection subschema", K(ret), K(obj.get_meta().get_subschema_id()));
      } else {
        print_params_.coll_meta_ = reinterpret_cast<ObSqlCollectionInfo *>(sub_meta.value_);
      }
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
      } else if (OB_FAIL(data_writer.flush_buf())) {
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

int ObSelectIntoOp::print_normal_obj_without_escape(const ObObj &obj, ObCsvFileWriter &data_writer)
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
      } else if (OB_FAIL(data_writer.flush_buf())) {
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
                                           ObCsvFileWriter &data_writer)
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
                                      ObCsvFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  ObCharsetType src_type = ObCharset::charset_type_by_coll(obj.get_collation_type());
  ObCharsetType dst_type = ObCharset::charset_type_by_coll(cs_type_);
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
        } else if (OB_FAIL(data_writer.flush_buf())) {
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
        } else if (OB_FAIL(data_writer.flush_shared_buf(shared_buf_, true))) {
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

int ObSelectIntoOp::write_single_char_to_file(const char *wchar, ObCsvFileWriter &data_writer)
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
    } else if (OB_FAIL(data_writer.flush_buf())) {
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
                                    ObCsvFileWriter &data_writer)
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

int ObSelectIntoOp::print_field(const ObObj &obj, ObCsvFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  char char_n = 'N';
  const bool need_enclose = has_enclose_ && !obj.is_null()
                            && (!is_optional_ || obj.is_string_type() || obj.is_collection_sql_type()
                                || obj.is_json() || obj.is_geometry() || obj.is_date()
                                || obj.is_time() || obj.is_timestamp() || obj.is_datetime()
                                || obj.is_mysql_date() || obj.is_mysql_datetime());
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

int ObSelectIntoOp::into_outfile(ObExternalFileWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObDatum *datum = NULL;
  ObObj obj;
  ObDatum *partition_datum = NULL;
  ObCsvFileWriter *csv_data_writer = NULL;
  if (do_partition_) {
    if (OB_FAIL(MY_SPEC.file_partition_expr_->eval(eval_ctx_, partition_datum))) {
      LOG_WARN("eval expr failed", K(ret));
    } else if (OB_ISNULL(partition_datum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_data_writer_for_partition(partition_datum->get_string(), data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(csv_data_writer = static_cast<ObCsvFileWriter *>(data_writer))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    }
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
    } else if (!ob_is_text_tc(select_exprs.at(i)->obj_meta_.get_type()) || obj.is_null()) {
      OZ(print_field(obj, *csv_data_writer));
    } else { // text tc
      OZ(print_lob_field(obj, *select_exprs.at(i), *datum, *csv_data_writer));
    }
    // print field terminator
    if (OB_SUCC(ret) && i != select_exprs.count() - 1) {
      OZ(write_obj_to_file(field_str_, *csv_data_writer));
    }
  }
  // print line terminator
  OZ(write_obj_to_file(line_str_, *csv_data_writer));
  // check if need split file
  OZ(check_csv_file_size(*csv_data_writer));
  // clear shared buffer
  OZ(csv_data_writer->flush_shared_buf(shared_buf_));
  if (has_compress_) {
    OZ(csv_data_writer->flush_buf());
  }
  return ret;
}

#ifdef OB_BUILD_CPP_ODPS
int ObSelectIntoOp::into_odps()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  apsara::odps::sdk::ODPSTableRecordPtr table_record;
  ObDatum *datum = NULL;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode());
  try {
    if (OB_UNLIKELY(!upload_ || !record_writer_ || !(table_record = upload_->CreateBufferRecord())
                    || !(table_record->GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (table_record->GetSchema()->GetColumnCount() != select_exprs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert into partial column in external table");
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
                                                          i, date_sql_mode))) {
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
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode());
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
          for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
            if (OB_ISNULL(datum = datum_vectors.at(col_idx).at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("datum is unexpected null", K(ret));
            } else if (lib::is_mysql_mode()
                       && OB_FAIL(set_odps_column_value_mysql(*table_record, *datum,
                                                              select_exprs.at(col_idx)->datum_meta_,
                                                              select_exprs.at(col_idx)->obj_meta_,
                                                              col_idx, date_sql_mode))) {
              LOG_WARN("failed to set odps column value", K(ret));
            } else if (lib::is_oracle_mode()
                       && OB_FAIL(set_odps_column_value_oracle(*table_record, *datum,
                                                               select_exprs.at(col_idx)->datum_meta_,
                                                               select_exprs.at(col_idx)->obj_meta_,
                                                               col_idx))) {
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
                                                uint32_t col_idx,
                                                const ObDateSqlMode date_sql_mode)
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
          if (buf_size == 0 || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
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
          } else if (OB_ISNULL(buf) && res_len == 0) {
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
          } else if (lob_str.length() == 0 || apsara::odps::sdk::ODPS_BINARY == odps_type
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
          } else if (OB_ISNULL(buf) && res_len == 0) {
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
                                                              in_type, j_base, parse_flag,
                                                              ObJsonExprHelper::get_json_max_depth_config()))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            LOG_DEBUG("set json value", K(datum_meta.cs_type_), K(ObString(jbuf.length(), jbuf.ptr())));
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
          int32_t date = datum.get_date();
          ObMySQLDate mdate(date);
          if (ob_is_mysql_date_tc(ob_type)
              && OB_FAIL(ObTimeConverter::mdate_to_date(mdate, date, date_sql_mode))) {
            LOG_WARN("mdate_to_date fail", K(ret));
          } else if (date < ODPS_DATE_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps date min value is 0001-01-01", K(ret));
          } else {
            table_record.SetDateValue(col_idx, date);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_DATETIME:
        {
          int32_t tmp_offset = 0;
          int64_t datetime = datum.get_datetime();
          ObMySQLDateTime mdatetime(datetime);
          if (ob_is_mysql_datetime_tc(ob_type)
              && OB_FAIL(ObTimeConverter::mdatetime_to_datetime(mdatetime, datetime, date_sql_mode))) {
            LOG_WARN("mdate_to_date fail", K(ret));
          } else if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else if (datetime < ORACLE_DATETIME_MIN_VAL + SEC_TO_USEC(tmp_offset)) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps datetime min value is 0001-01-01 00:00:00", K(ret));
          } else {
            table_record.SetDatetimeValue(col_idx, (datetime - SEC_TO_USEC(tmp_offset)) / 1000);
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
          if (buf_size == 0 || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
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
          } else if (OB_ISNULL(buf) && res_len == 0) {
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
          } else if (lob_str.length() == 0 || apsara::odps::sdk::ODPS_BINARY == odps_type
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
          } else if (OB_ISNULL(buf) && res_len == 0) {
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
                                                              in_type, j_base, parse_flag,
                                                              ObJsonExprHelper::get_json_max_depth_config()))) {
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
          table_record.SetTimeValue(col_idx,
                                    timestamp.time_us_ / 1000000,
                                    timestamp.time_ctx_.tail_nsec_ + (timestamp.time_us_ % 1000000) * 1000,
                                    odps_type);
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
#ifdef OB_BUILD_JNI_ODPS
int ObSelectIntoOp::create_odps_schema()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &select_exprs = MY_SPEC.select_exprs_;
  const ObIArray<JniWriter::OdpsType> &col_type_from_odps = uploader_.writer_ptr->get_schema_from_odps();
  intptr_t export_schema_ptr = uploader_.writer_ptr->get_export_schema_ptr();

  if (export_schema_ptr == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("export schema ptr is null", K(ret));
  } else {
    arrow::Result<std::shared_ptr<arrow::DataType>> res = arrow::ImportType(reinterpret_cast<ArrowSchema*>(export_schema_ptr));
    if (!res.ok()) {
      ret = OB_ERR_INVALID_DATA_TYPE;
      LOG_WARN("failed to get remote arrow schema from odps jni", K(ret));
    } else if (select_exprs.count() != col_type_from_odps.count()) {
      ret = OB_ERR_TYPE_MISMATCH;
      LOG_WARN("select exprs count not equal to odps schema", K(ret));
    } else {
      std::shared_ptr<arrow::DataType> tableStructType = res.ValueOrDie();
      arrow_schema_ = arrow::schema(tableStructType->fields());
    }
  }
  return ret;
}

template<typename T>
inline T arrow_get(ObIVector& expr_vector, int64_t idx)
{
  static_assert(sizeof(T) <= sizeof(int64_t), "invalid type");
  return *reinterpret_cast<const T *>(expr_vector.get_payload(idx));
}

#define PUT_DATUM_INTO_ARROW_BUILDER(BUILDER_TYPE, GET_FUNC)                 \
  do {                                                                       \
      if(OB_SUCC(ret)) {                                                     \
        BUILDER_TYPE* builder_ref = dynamic_cast<BUILDER_TYPE*>(builder);    \
        if (OB_ISNULL(builder_ref)) {                                        \
          ret = OB_ERR_TYPE_MISMATCH;                                        \
          LOG_WARN("UNEXPECTED null bool builder ptr", K(ret));              \
        } else {                                                             \
          arrow::Status st = builder_ref->Append((GET_FUNC));                \
          if (!st.ok()) {                                                    \
            ret = OB_ODPS_ERROR;                                             \
            LOG_WARN("failed to append int value", K(ret));                  \
          }                                                                  \
        }                                                                    \
      }                                                                      \
  } while(false);

int ObSelectIntoOp::set_odps_column_value_mysql_jni(arrow::ArrayBuilder *builder,
                                                    JniWriter::OdpsType odps_type,
                                                    const ObDatum &datum,
                                                    const ObDatumMeta &datum_meta,
                                                    const ObObjMeta &obj_meta,
                                                    arrow::Field &arrow_field,
                                                    uint32_t col_idx)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (OB_ISNULL(builder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("UNEXPECTED null builder ptr", K(ret));
  } else if (datum.is_null()) {
    arrow::Status st = builder->AppendNull();
    if (!st.ok()) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("failed to append null value", K(ret));
    }
  } else {
    switch (odps_type)
    {
      case JniWriter::OdpsType::BOOLEAN:
      {
        if (ObTinyIntType == ob_type) {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, datum.get_tinyint() != 0);
        } else if (ObSmallIntType == ob_type) {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, datum.get_smallint() != 0)
        } else if (ObMediumIntType == ob_type || ObInt32Type == ob_type) {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, datum.get_int32() != 0);
        } else if (ObIntType == ob_type) {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, datum.get_int() != 0);
        }
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, false);
        break;
      }
      case JniWriter::OdpsType::TINYINT:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int8Builder, datum.get_tinyint());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int8Builder, 0);
        break;
      }
      case JniWriter::OdpsType::SMALLINT:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int16Builder, datum.get_smallint());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int16Builder, 0);
        break;
      }
      case JniWriter::OdpsType::INT:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int32Builder, datum.get_int32());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int32Builder, 0);
        break;
      }
      case JniWriter::OdpsType::BIGINT:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int64Builder, datum.get_int());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int64Builder, 0);
        break;
      }
      case JniWriter::OdpsType::FLOAT:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::FloatBuilder, datum.get_float());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::FloatBuilder, 0);
        break;
      }
      case JniWriter::OdpsType::DOUBLE:
      {
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::DoubleBuilder, datum.get_double());
        PUT_DATUM_INTO_ARROW_BUILDER(arrow::DoubleBuilder, 0);
        break;
      }
      case JniWriter::OdpsType::DECIMAL:
      {
        std::string dec;
        if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
          LOG_WARN("failed to get string", K(ret));
        } else {
          if (arrow_field.type()->id() == arrow::Type::DECIMAL || arrow_field.type()->id() == arrow::Type::DECIMAL128) {
            arrow::Result<arrow::Decimal128> res = arrow::Decimal128::FromString(dec);
            if (!res.ok()) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to convert decimal", K(ret), K(dec.c_str()));
            } else {
              arrow::Decimal128Builder *builder_ref = dynamic_cast<arrow::Decimal128Builder*>(builder);
              arrow::Status st = builder_ref->Append(res.ValueOrDie());
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append decimal value", K(ret));
              }
            }
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Decimal128Builder, arrow::Decimal128::FromString("0").ValueOrDie());
          } else if (arrow_field.type()->id() == arrow::Type::DECIMAL256) {
            arrow::Result<arrow::Decimal256> res = arrow::Decimal256::FromString(dec);
            if (!res.ok()) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to convert decimal", K(ret), K(dec.c_str()));
            } else {
              arrow::Decimal256Builder *builder_ref = dynamic_cast<arrow::Decimal256Builder*>(builder);
              arrow::Status st = builder_ref->Append(res.ValueOrDie());
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append decimal value", K(ret));
              }
            }
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Decimal256Builder, arrow::Decimal256::FromString("0").ValueOrDie());
          }
        }
        break;
      }
      case JniWriter::OdpsType::CHAR:
      case JniWriter::OdpsType::VARCHAR:
      case JniWriter::OdpsType::STRING:
      case JniWriter::OdpsType::BINARY:
      {
        ObString lob_str;
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                              datum,
                                                              datum_meta,
                                                              obj_meta.has_lob_header(),
                                                              lob_str,
                                                              &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
        } else if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)
            || JniWriter::OdpsType::BINARY == odps_type
            || CS_TYPE_BINARY == datum_meta.cs_type_
        ) {
          res_len = static_cast<uint32_t>(lob_str.length());
          buf = const_cast<char *>(lob_str.ptr());
        } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(lob_str.length() * ObCharset::MAX_MB_LEN)))) {
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
        } else if ((JniWriter::OdpsType::CHAR == odps_type && res_len > 255) ||
                   (JniWriter::OdpsType::VARCHAR == odps_type && res_len > 65535) ||
                   (JniWriter::OdpsType::STRING == odps_type && res_len > 8 * 1024 * 1024) ||
                   (JniWriter::OdpsType::BINARY == odps_type && res_len > 8 * 1024 * 1024)) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("string length out of range", K(res_len));
        } else if (OB_ISNULL(buf) && res_len == 0) {
          if (JniWriter::OdpsType::BINARY != odps_type) {
            arrow::StringBuilder* builder_ref = dynamic_cast<arrow::StringBuilder *>(builder);
            if (OB_ISNULL(builder_ref)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to append null value", K(ret));
            } else {
              arrow::Status st = builder_ref->Append(buf, res_len);
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              arrow::Status st = builder_ref->Append("_magic", 6);
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              }
            }
          } else {
            arrow::BinaryBuilder* builder_ref = dynamic_cast<arrow::BinaryBuilder *>(builder);
            if (OB_ISNULL(builder_ref)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to append null value", K(ret));
            } else {
              arrow::Status st = builder_ref->Append(buf, res_len);
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              arrow::Status st = builder_ref->Append("_magic", 6);
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              }
            }
          }
        }
        break;
      }
      case JniWriter::OdpsType::JSON:
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
                                                            in_type, j_base, parse_flag,
                                                            ObJsonExprHelper::get_json_max_depth_config()))) {
          COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
        } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
          COMMON_LOG(WARN, "fail to convert json to string", K(ret));
        } else if (jbuf.length() > UINT32_MAX) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
        } else {
          LOG_DEBUG("debug select into json", K(datum_meta.cs_type_), K(ObString(jbuf.length(), jbuf.ptr())));
          arrow::StringBuilder* builder_ref = dynamic_cast<arrow::StringBuilder *>(builder);
          if (OB_ISNULL(builder_ref)) {
            ret = OB_ODPS_ERROR;
            LOG_WARN("failed to append null value", K(ret));
          } else {
            arrow::Status st = builder_ref->Append(jbuf.ptr(), jbuf.length());
            if (!st.ok()) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to append null value", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            arrow::Status st = builder_ref->Append("{}", 2);
            if (!st.ok()) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to append null value", K(ret));
            }
          }
        }
        break;
      }
      case JniWriter::OdpsType::TIMESTAMP:
      case JniWriter::OdpsType::TIMESTAMP_NTZ:
      {
        int64_t us = JniWriter::OdpsType::TIMESTAMP == odps_type
                     ? datum.get_timestamp()
                     : datum.get_datetime();
        int64_t sec = us / 1000000;
        int32_t ns = (us % 1000000) * 1000;

        if (us < ORACLE_DATETIME_MIN_VAL) {
          ret = OB_DATETIME_FUNCTION_OVERFLOW;
          LOG_WARN("odps timestamp min value is 0001-01-01 00:00:00", K(ret), K(us));
        } else {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::TimestampBuilder, us * 1000);
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::TimestampBuilder, 0);
        }
        break;
      }
      case JniWriter::OdpsType::DATE:
      {
        if (datum.get_date() < ODPS_DATE_MIN_VAL) {
          ret = OB_DATETIME_FUNCTION_OVERFLOW;
          LOG_WARN("odps date min value is 0001-01-01", K(ret));
        } else {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date32Builder, datum.get_date());
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date32Builder, 0);
        }
        break;
      }
      case JniWriter::OdpsType::DATETIME:
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
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date64Builder, ((datum.get_datetime() - SEC_TO_USEC(tmp_offset)) / 1000));
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date64Builder, 0);
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
  return ret;
}


int ObSelectIntoOp::set_odps_column_value_oracle_jni(arrow::ArrayBuilder *builder,
                                                 JniWriter::OdpsType odps_type,
                                                 const ObDatum &datum,
                                                 const ObDatumMeta &datum_meta,
                                                 const ObObjMeta &obj_meta,
                                                 arrow::Field &arrow_field,
                                                 uint32_t col_idx)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  int64_t int_value = 0;
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_ISNULL(builder)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("UNEXPECTED null builder ptr", K(ret));
  } else if (datum.is_null()) {
    arrow::Status st = builder->AppendNull();
    if (!st.ok()) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("failed to append null value", K(ret));
    }
  } else {
      switch (odps_type)
      {
        case JniWriter::OdpsType::BOOLEAN:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, int_value != 0);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::BooleanBuilder, false);
          }
          break;
        }
        case JniWriter::OdpsType::TINYINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT8_MIN || int_value > INT8_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int8Builder, int_value);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int8Builder, 0);
          }
          break;
        }
        case JniWriter::OdpsType::SMALLINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT16_MIN || int_value > INT16_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int16Builder, int_value);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int16Builder, 0);
          }
          break;
        }
        case JniWriter::OdpsType::INT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT32_MIN || int_value > INT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int32Builder, int_value);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int32Builder, 0);
          }
          break;
        }
        case JniWriter::OdpsType::BIGINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int64Builder, int_value);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Int64Builder, 0);
          }
          break;
        }
        case JniWriter::OdpsType::FLOAT:
        {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::FloatBuilder, datum.get_float());
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::FloatBuilder, 0);
          break;
        }
        case JniWriter::OdpsType::DOUBLE:
        {
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::DoubleBuilder, datum.get_double());
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::DoubleBuilder, 0);
          break;
        }
        case JniWriter::OdpsType::DECIMAL:
        {
          std::string dec;
          if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
            LOG_WARN("failed to get string", K(ret));
          } else {
            if (arrow_field.type()->id() == arrow::Type::DECIMAL || arrow_field.type()->id() == arrow::Type::DECIMAL128) {
              arrow::Result<arrow::Decimal128> res = arrow::Decimal128::FromString(dec);
              if (!res.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to convert decimal", K(ret), K(dec.c_str()));
              } else {
                arrow::Decimal128Builder *builder_ref = dynamic_cast<arrow::Decimal128Builder*>(builder);
                arrow::Status st = builder_ref->Append(res.ValueOrDie());
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append decimal value", K(ret));
                }
              }
              PUT_DATUM_INTO_ARROW_BUILDER(arrow::Decimal128Builder, arrow::Decimal128::FromString("0").ValueOrDie());
            } else if (arrow_field.type()->id() == arrow::Type::DECIMAL256) {
              arrow::Result<arrow::Decimal256> res = arrow::Decimal256::FromString(dec);
              if (!res.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to convert decimal", K(ret), K(dec.c_str()));
              } else {
                arrow::Decimal256Builder *builder_ref = dynamic_cast<arrow::Decimal256Builder*>(builder);
                arrow::Status st = builder_ref->Append(res.ValueOrDie());
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append decimal value", K(ret));
                }
              }
              PUT_DATUM_INTO_ARROW_BUILDER(arrow::Decimal256Builder, arrow::Decimal256::FromString("0").ValueOrDie());
            }
          }
          break;
        }
        case JniWriter::OdpsType::CHAR:
        case JniWriter::OdpsType::VARCHAR:
        case JniWriter::OdpsType::STRING:
        case JniWriter::OdpsType::BINARY: {
          ObString lob_str;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  allocator, datum, datum_meta, obj_meta.has_lob_header(), lob_str, &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_) ||
                     JniWriter::OdpsType::BINARY == odps_type || CS_TYPE_BINARY == datum_meta.cs_type_) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(lob_str.length() * ObCharset::MAX_MB_LEN)))) {
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
          } else if ((JniWriter::OdpsType::CHAR == odps_type && res_len > 255) ||
                     (JniWriter::OdpsType::VARCHAR == odps_type && res_len > 65535) ||
                     (JniWriter::OdpsType::STRING == odps_type && res_len > 8 * 1024 * 1024) ||
                     (JniWriter::OdpsType::BINARY == odps_type && res_len > 8 * 1024 * 1024)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            if (JniWriter::OdpsType::BINARY != odps_type) {
              arrow::StringBuilder *builder_ref = dynamic_cast<arrow::StringBuilder *>(builder);
              if (OB_ISNULL(builder_ref)) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              } else {
                arrow::Status st = builder_ref->Append("", 0);
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append null value", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                arrow::Status status = builder_ref->Append("_magic", 6);
                if (!status.ok()) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
                }
              }
            } else {
              arrow::BinaryBuilder *builder_ref = dynamic_cast<arrow::BinaryBuilder *>(builder);
              if (OB_ISNULL(builder_ref)) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              } else {
                arrow::Status st = builder_ref->Append("", 0);
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append null value", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                arrow::Status status = builder_ref->Append("_magic", 6);
                if (!status.ok()) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
                }
              }
            }
          } else {
            if (JniWriter::OdpsType::BINARY != odps_type) {
              arrow::StringBuilder *builder_ref = dynamic_cast<arrow::StringBuilder *>(builder);
              if (OB_ISNULL(builder_ref)) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              } else {
                arrow::Status st = builder_ref->Append(buf, res_len);
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append null value", K(ret));
                }
              }

              if (OB_SUCC(ret)) {
                arrow::Status status = builder_ref->Append("_magic", 6);
                if (!status.ok()) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
                }
              }
            } else {
              arrow::BinaryBuilder *builder_ref = dynamic_cast<arrow::BinaryBuilder *>(builder);
              if (OB_ISNULL(builder_ref)) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              } else {
                arrow::Status st = builder_ref->Append(buf, res_len);
                if (!st.ok()) {
                  ret = OB_ODPS_ERROR;
                  LOG_WARN("failed to append null value", K(ret));
                }
              }

              if (OB_SUCC(ret)) {
                arrow::Status status = builder_ref->Append("_magic", 6);
                if (!status.ok()) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
                }
              }
            }
          }
          break;
        }
        case JniWriter::OdpsType::JSON: {
          ObString json_str;
          ObIJsonBase *j_base = NULL;
          ObJsonBuffer jbuf(&allocator);
          ObJsonInType in_type = ObJsonInType::JSON_BIN;
          uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(
                  allocator, datum, datum_meta, obj_meta.has_lob_header(), json_str, &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator,
                         json_str,
                         in_type,
                         in_type,
                         j_base,
                         parse_flag,
                         ObJsonExprHelper::get_json_max_depth_config()))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) {  // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            LOG_DEBUG("debug select into json", K(datum_meta.cs_type_), K(ObString(jbuf.length(), jbuf.ptr())));
            arrow::StringBuilder *builder_ref = dynamic_cast<arrow::StringBuilder *>(builder);
            if (OB_ISNULL(builder_ref)) {
              ret = OB_ODPS_ERROR;
              LOG_WARN("failed to append null value", K(ret));
            } else {
              arrow::Status st = builder_ref->Append(jbuf.ptr(), jbuf.length());
              if (!st.ok()) {
                ret = OB_ODPS_ERROR;
                LOG_WARN("failed to append null value", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              arrow::Status status = builder_ref->Append("{}", 6);
              if (!status.ok()) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
              }
            }
          }
          break;
        }
        case JniWriter::OdpsType::TIMESTAMP:
        case JniWriter::OdpsType::TIMESTAMP_NTZ:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::TimestampBuilder, timestamp.time_us_ * 1000 + timestamp.time_ctx_.tail_nsec_);
          PUT_DATUM_INTO_ARROW_BUILDER(arrow::TimestampBuilder, 0);
          break;
        }
        case JniWriter::OdpsType::DATE:
        {
          int64_t day = datum.get_datetime() / 1000000 / 3600 / 24;
          if (datum.get_date() < ODPS_DATE_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps date min value is 0001-01-01", K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date32Builder, day);
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date32Builder, 0);
          }
          break;
        }
        case JniWriter::OdpsType::DATETIME:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          int32_t tmp_offset = 0;
          if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else {
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date64Builder, ((datum.get_datetime() - SEC_TO_USEC(tmp_offset)) / 1000));
            PUT_DATUM_INTO_ARROW_BUILDER(arrow::Date64Builder, 0);
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

  return ret;
}

int ObSelectIntoOp::into_odps_jni()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  const ObIArray<JniWriter::OdpsType> &col_type_from_odps = uploader_.writer_ptr->get_schema_from_odps();
  const arrow::FieldVector &type_vec = arrow_schema_->fields();
  ObDatum *datum = NULL;
  arrow::Result<std::shared_ptr<arrow::RecordBatchBuilder> > rbatchRes =
        arrow::RecordBatchBuilder::Make(arrow_schema_, &arrow_alloc_);
  if (!rbatchRes.ok()) {
    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
    LOG_WARN("fail to make empty record batch", K(ret));
  } else if (OB_FAIL(uploader_.writer_ptr->get_current_block_addr())) {
    LOG_WARN("failed to get bloock addr");
  } else {
    std::shared_ptr<arrow::RecordBatchBuilder> rbatch = rbatchRes.ValueOrDie();
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
      ObDatumMeta &meta = select_exprs.at(col_idx)->datum_meta_;
      ObObjMeta &obj_meta = select_exprs.at(col_idx)->obj_meta_;
      arrow::ArrayBuilder *array_builder = rbatch->GetField(col_idx);
      JniWriter::OdpsType odps_type = col_type_from_odps.at(col_idx);

      if (OB_ISNULL(select_exprs.at(col_idx)) || OB_ISNULL(array_builder) || OB_ISNULL(type_vec.at(col_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is unexpected null", K(ret));
      } else if (OB_FAIL(select_exprs.at(col_idx)->eval(eval_ctx_, datum))) {
        LOG_WARN("eval expr failed", K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("datum is unexpected null", K(ret));
      } else if (lib::is_mysql_mode() && OB_FAIL(set_odps_column_value_mysql_jni(array_builder,
                                             odps_type,
                                             *datum,
                                             select_exprs.at(col_idx)->datum_meta_,
                                             select_exprs.at(col_idx)->obj_meta_,
                                             *type_vec.at(col_idx),
                                             col_idx))) {
        LOG_WARN("failed to set odps column value", K(ret));
      } else if (lib::is_oracle_mode()
                   && OB_FAIL(set_odps_column_value_oracle_jni(array_builder, odps_type, *datum,
                                                           select_exprs.at(col_idx)->datum_meta_,
                                                           select_exprs.at(col_idx)->obj_meta_,
                                                           *type_vec.at(col_idx),
                                                           col_idx))) {
        LOG_WARN("failed to set odps column value", K(ret));
      }
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> res;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to build the batch", K(ret));
    } else if (FALSE_IT(res = rbatch->Flush())) {
      // do nothing
    } else if (!res.ok()) {
      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
      std::ostringstream stream;
      stream << res.status();
      LOG_WARN("fail to export array", K(ret), K(stream.str().c_str()));
    } else {
      struct ArrowSchema *c_schema = reinterpret_cast<struct ArrowSchema *>(uploader_.writer_ptr->get_schema_ptr());
      struct ArrowArray *c_array = reinterpret_cast<struct ArrowArray *>(uploader_.writer_ptr->get_array_ptr());
      std::shared_ptr<arrow::RecordBatch> record_batch_ptr = res.ValueOrDie();
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(c_array) || OB_ISNULL(c_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("array ptr is unexpected null", K(ret));
        } else {
          arrow::Status c_array_status = arrow::ExportRecordBatch(*record_batch_ptr, c_array, c_schema);
          if (!c_array_status.ok()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to export scheam", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(uploader_.writer_ptr->do_write_next_brs(0, 1))) {
        LOG_WARN("failed to write data");
      } else {
        need_commit_ = true;
      }
    }
  }
  return ret;
}

using DaysChecker = std::function<bool(int32_t)>;
template <typename ArrowType, typename ObType>
int vectorize_fill_int_mysql(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector,
    ObDatumMeta &datum_meta, int& act_cnt, std::shared_ptr<std::function<bool(ObType)> > filter = nullptr)
{
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          if (OB_LIKELY(filter == nullptr)) {
            arrow::Status status = builder_ref->Append(arrow_get<ObType>(expr_vector, i));
            if (OB_UNLIKELY(!status.ok())) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          } else {
            ObType value = arrow_get<ObType>(expr_vector, i);
            if ((*filter)(value)) {
              arrow::Status status = builder_ref->Append(value);
              if (OB_UNLIKELY(!status.ok())) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            } else {
              ret = OB_DATA_OUT_OF_RANGE;
              LOG_WARN("data out of range", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(0);
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

template<typename ArrowType, typename ObType>
int vectorize_fill_int_oracle(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, ObDatumMeta &datum_meta, int &act_cnt) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (is_oracle_mode()){
    // oracle mode
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
        // do nothing
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          int64_t int_value = 0;

          if (ObNumberType == datum_meta.get_type()) {
            const number::ObNumber nmb(expr_vector.get_number(i));
            if (OB_FAIL(nmb.extract_valid_int64_with_trunc(int_value))) {
              LOG_WARN("failed to cast number to int64", K(ret));
            }
          } else if (ObDecimalIntType == datum_meta.get_type()) {
            int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
            bool is_valid;
            if (OB_FAIL(wide::check_range_valid_int64(expr_vector.get_decimal_int(i), int_bytes, is_valid, int_value))) {
              LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
            } else if (!is_valid) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("decimal int is not valid int64", K(ret));
            }
          } else {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("type mismatch in odps table and oracle mode table", K(ret), K(datum_meta));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_UNLIKELY(int_value > std::numeric_limits<ObType>::max() || int_value < std::numeric_limits<ObType>::min())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("int value is out of range", K(ret), K(int_value));
          } else {
            arrow::Status status = builder_ref->Append(int_value);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(0);
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

template<typename ObType>
int vectorize_fill_bool_mysql(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::BooleanBuilder *builder_ref = dynamic_cast<arrow::BooleanBuilder*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          arrow::Status status = builder_ref->Append(arrow_get<ObType>(expr_vector, i) != 0);
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(false);
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

int vectorize_fill_bool_oracle(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::BooleanBuilder *builder_ref = dynamic_cast<arrow::BooleanBuilder*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (is_oracle_mode()){
    // oracle mode
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
        // do nothing
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          int64_t int_value = 0;

          if (ObNumberType == datum_meta.get_type()) {
            const number::ObNumber nmb(expr_vector.get_number(i));
            if (OB_FAIL(nmb.extract_valid_int64_with_trunc(int_value))) {
              LOG_WARN("failed to cast number to int64", K(ret));
            }
          } else if (ObDecimalIntType == datum_meta.get_type()) {
            int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
            bool is_valid;
            if (OB_FAIL(wide::check_range_valid_int64(expr_vector.get_decimal_int(i), int_bytes, is_valid, int_value))) {
              LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
            } else if (!is_valid) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("decimal int is not valid int64", K(ret));
            }
          } else {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("type mismatch in odps table and oracle mode table", K(ret), K(datum_meta));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else {
            arrow::Status status = builder_ref->Append(int_value != 0);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(false);
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

template<typename ArrowType, typename ObType>
int vectorize_fill_double(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          arrow::Status status = builder_ref->Append(arrow_get<ObType>(expr_vector, i));
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(0.0);
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

template<typename ArrowBuilderType>
int vectorize_fill_decimal(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  ArrowBuilderType *builder_ref = dynamic_cast<ArrowBuilderType*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    int ob_precision = static_cast<int>(datum_meta.precision_);
    int ob_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);

    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          const ObDecimalInt *value = expr_vector.get_decimal_int(i);
          int64_t int_bytes = ArrowBuilderType::ValueType::kByteWidth;
          uint8_t buf[ArrowBuilderType::ValueType::kByteWidth];
          uint8_t* buffer = (uint8_t *)value;
          if (ob_int_bytes > int_bytes) {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("failed to convert value to decimal", K(ob_int_bytes), K(int_bytes));
          } else {
            for (int i = 0; i < ob_int_bytes; ++i) {
              buf[ArrowBuilderType::ValueType::kByteWidth - i - 1] = buffer[i];
            }
            arrow::Result<typename ArrowBuilderType::ValueType> res = ArrowBuilderType::ValueType::FromBigEndian((const uint8_t *)buf, int_bytes);
            if (!res.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("failed to convert value to decimal");
            } else {
              arrow::Status status = builder_ref->Append(res.ValueOrDie());
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append(ArrowBuilderType::ValueType::FromString("0").ValueOrDie());
      if (!status.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append null", K(ret));
      }
    }
  }
  return ret;
}

template <typename ArrowBuilderType>
int vectorize_fill_string(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, ObDatumMeta &meta,
    const ObObjMeta &obj_meta, ObIAllocator &alloc, JniWriter::OdpsType odps_type, int& act_cnt)
{
  int ret = OB_SUCCESS;
  act_cnt = 0;
  ArrowBuilderType *builder_ref = dynamic_cast<ArrowBuilderType *>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          ObString lob_str;
          uint32_t res_len = 0;
          char *buf = NULL;
          int64_t buf_size = 0;
          bool has_lob_header = obj_meta.has_lob_header();

          if (OB_FAIL(
                  ObTextStringHelper::read_real_string_data(alloc, &expr_vector, meta, has_lob_header, lob_str, i))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(meta.cs_type_) ||
                     meta.cs_type_ == CS_TYPE_BINARY) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(meta.cs_type_,
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
            // do nothing
          } else if ((JniWriter::OdpsType::CHAR == odps_type && res_len > 255) ||
                     (JniWriter::OdpsType::VARCHAR == odps_type && res_len > 65535) ||
                     (JniWriter::OdpsType::STRING == odps_type && res_len > 8 * 1024 * 1024) ||
                     (JniWriter::OdpsType::BINARY == odps_type && res_len > 8 * 1024 * 1024)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            arrow::Status status = builder_ref->Append("", 0);
            if (!status.ok()) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
            }
          } else {
            arrow::Status status = builder_ref->Append(buf, res_len);
            if (!status.ok()) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      arrow::Status status = builder_ref->Append("_magic", 6);
      if (!status.ok()) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
      }
    }
  }
  return ret;
}

bool ObSelectIntoOp::day_number_checker(int32_t days) { return days > ODPS_DATE_MIN_VAL; }

int ObSelectIntoOp::into_odps_jni_batch_one_col(int64_t col_idx, JniWriter::OdpsType odps_type,
    arrow::Field &arrow_field, ObDatumMeta &meta, ObObjMeta &obj_meta, ObIVector &expr_vector,
    arrow::ArrayBuilder *builder, const ObBatchRows &brs, int &act_cnt, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  switch (odps_type) {
    case JniWriter::OdpsType::BIGINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int64Type, int64_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int64Type, int64_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case JniWriter::OdpsType::TINYINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int8Type, int8_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int8Type, int8_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }

      break;
    case JniWriter::OdpsType::SMALLINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int16Type, int16_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int16Type, int16_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }

      break;
    case JniWriter::OdpsType::INT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int32Type, int32_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int32Type, int32_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case JniWriter::OdpsType::DOUBLE:
      if (OB_FAIL((vectorize_fill_double<arrow::DoubleType, double>(builder, brs, expr_vector, act_cnt)))) {
        LOG_WARN("fail to fill double", K(ret));
      }
      break;
    case JniWriter::OdpsType::FLOAT:
      if (OB_FAIL((vectorize_fill_double<arrow::FloatType, float>(builder, brs, expr_vector, act_cnt)))) {
        LOG_WARN("fail to fill float", K(ret));
      }
      break;
    case JniWriter::OdpsType::BOOLEAN:
      if (!is_oracle_mode()) {
        if (obj_meta.get_type() == ObTinyIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int8_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int8", K(ret));
          }
        } else if (obj_meta.get_type() == ObSmallIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int16_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int16", K(ret));
          }
        } else if (obj_meta.get_type() == ObMediumIntType || obj_meta.get_type() == ObInt32Type) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int32_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int32", K(ret));
          }
        } else if (obj_meta.get_type() == ObIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int64_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int64", K(ret));
          }
        }
      } else {
        if (OB_FAIL((vectorize_fill_bool_oracle(builder, brs, expr_vector, act_cnt, meta)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case JniWriter::OdpsType::VARCHAR:
    case JniWriter::OdpsType::CHAR:
    case JniWriter::OdpsType::STRING:
      vectorize_fill_string<arrow::StringBuilder>(builder, brs, expr_vector, meta, obj_meta, alloc, odps_type, act_cnt);
      break;
    case JniWriter::OdpsType::BINARY:
      vectorize_fill_string<arrow::BinaryBuilder>(builder, brs, expr_vector, meta, obj_meta, alloc, odps_type, act_cnt);
      break;
    case JniWriter::OdpsType::DECIMAL:
      if (arrow_field.type()->id() == arrow::Type::DECIMAL || arrow_field.type()->id() == arrow::Type::DECIMAL128) {
        if (OB_FAIL(vectorize_fill_decimal<arrow::Decimal128Builder>(builder, brs, expr_vector, act_cnt, meta))) {
          LOG_WARN("fail to vectorize decimal", K(ret));
        }
      } else if (arrow_field.type()->id() == arrow::Type::DECIMAL256) {
        if (OB_FAIL(vectorize_fill_decimal<arrow::Decimal256Builder>(builder, brs, expr_vector, act_cnt, meta))) {
          LOG_WARN("fail to vectorize decimal", K(ret));
        }
      }
      break;
    case JniWriter::OdpsType::DATE:  // arrowType = new ArrowType.Date(DateUnit.DAY);
    {
      std::shared_ptr<DaysChecker> filter = std::make_shared<DaysChecker>(day_number_checker);
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Date32Type, int32_t>(builder, brs, expr_vector, meta, act_cnt, filter)))) {
          LOG_WARN("fail to vectorize date", K(ret));
        }
      } else {
        act_cnt = 0;
        arrow::NumericBuilder<arrow::Date32Type> *builder_ref =
            dynamic_cast<arrow::NumericBuilder<arrow::Date32Type> *>(builder);
        if (OB_ISNULL(builder_ref)) {
          ret = OB_ERR_TYPE_MISMATCH;
          LOG_WARN("builder is unexpected null", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
            if (brs.skip_->contain(i)) {
            } else {
              ++act_cnt;
              if (expr_vector.is_null(i)) {
                arrow::Status status = builder_ref->AppendNull();
                if (!status.ok()) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              } else {
                int32_t days = expr_vector.get_datetime(i) / 1000000 / 3600 / 24;
                if ((*filter)(arrow_get<int32_t>(expr_vector, days))) {
                  arrow::Status status = builder_ref->Append(days);
                  if (OB_UNLIKELY(!status.ok())) {
                    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                    LOG_WARN("fail to append null", K(ret));
                  }
                } else {
                  ret = OB_DATA_OUT_OF_RANGE;
                  LOG_WARN("data out of range", K(ret));
                }
              }
            }
          }

          if (OB_SUCC(ret)) {
            arrow::Status status = builder_ref->Append(0);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to finish", K(ret));
            }
          }
        }
      }
      break;
    }
    case JniWriter::OdpsType::DATETIME:  // arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
    {
      arrow::NumericBuilder<arrow::Date64Type> *builder_ref =
          dynamic_cast<arrow::NumericBuilder<arrow::Date64Type> *>(builder);
      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else {
            if (expr_vector.is_null(i)) {
              arrow::Status status = builder_ref->AppendNull();
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            } else {
              int32_t tmp_offset = 0;
              if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
                LOG_WARN("failed to get timezone offset", K(ret));
              } else {
                if (!is_oracle_mode()) {
                  int32_t tmp_offset = 0;
                  if (expr_vector.get_datetime(i) < ORACLE_DATETIME_MIN_VAL + SEC_TO_USEC(tmp_offset)) {
                    ret = OB_DATETIME_FUNCTION_OVERFLOW;
                    LOG_WARN("odps datetime min value is 0001-01-01 00:00:00", K(ret));
                  } else {
                    arrow::Status status = builder_ref->Append((expr_vector.get_datetime(i) - SEC_TO_USEC(tmp_offset)) / 1000);
                    if (OB_UNLIKELY(!status.ok())) {
                      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                      LOG_WARN("fail to append null", K(ret));
                    }
                  }
                } else {
                  ObOTimestampTinyData timestamp = expr_vector.get_otimestamp_tiny(i);
                  arrow::Status status = builder_ref->Append((timestamp.time_us_ - SEC_TO_USEC(tmp_offset)) / 1000);
                  if (OB_UNLIKELY(!status.ok())) {
                    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                    LOG_WARN("fail to append null", K(ret));
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          arrow::Status status = builder_ref->Append(0);
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to finish", K(ret));
          }
        }
      }
      break;
    }
    case JniWriter::OdpsType::TIMESTAMP:
    case JniWriter::OdpsType::TIMESTAMP_NTZ:
      // arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
      // arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    {
      arrow::NumericBuilder<arrow::TimestampType> *builder_ref =
          dynamic_cast<arrow::NumericBuilder<arrow::TimestampType> *>(builder);

      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else {
            if (expr_vector.is_null(i)) {
              arrow::Status status = builder_ref->AppendNull();
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            } else {
              if (!is_oracle_mode()) {
                int64_t us = JniWriter::OdpsType::TIMESTAMP == odps_type
                       ? expr_vector.get_timestamp(i)
                       : expr_vector.get_datetime(i);
                int64_t sec = us / 1000000;
                int32_t ns = (us % 1000000) * 1000;
                if (us < ORACLE_DATETIME_MIN_VAL) {
                  ret = OB_DATETIME_FUNCTION_OVERFLOW;
                  LOG_WARN("odps timestamp min value is 0001-01-01 00:00:00", K(ret), K(us));
                } else {
                  arrow::Status status = builder_ref->Append(us * 1000);
                  if (!status.ok()) {
                    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                    LOG_WARN("fail to append null", K(ret));
                  }
                }
              } else {
                ObOTimestampData&& timestamp = const_cast<ObOTimestampTinyData*>(&expr_vector.get_otimestamp_tiny(i))->to_timestamp_data();
                arrow::Status status = builder_ref->Append(timestamp.time_us_ * 1000 + timestamp.time_ctx_.tail_nsec_);
                if (!status.ok()) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          arrow::Status status = builder_ref->Append(0);
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        }
      }
      break;
    }
    case JniWriter::OdpsType::JSON:
    {
      arrow::StringBuilder *builder_ref =
          dynamic_cast<arrow::StringBuilder *>(builder);

      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else {
            if (expr_vector.is_null(i)) {
              arrow::Status status = builder_ref->AppendNull();
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            } else {
              ObString json_str;
              ObIJsonBase *j_base = NULL;
              ObJsonBuffer jbuf(&alloc);
              ObJsonInType in_type = ObJsonInType::JSON_BIN;
              uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
              if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc,
                                                                    &expr_vector,
                                                                    meta,
                                                                    obj_meta.has_lob_header(),
                                                                    json_str,
                                                                    i))) {
                LOG_WARN("failed to read string", K(ret));
              } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&alloc, json_str, in_type,
                                                                  in_type, j_base, parse_flag,
                                                                  ObJsonExprHelper::get_json_max_depth_config()))) {
                COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
              } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
                COMMON_LOG(WARN, "fail to convert json to string", K(ret));
              } else if (jbuf.length() > UINT32_MAX) {
                ret = OB_DATA_OUT_OF_RANGE;
                LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
              } else {
                arrow::Status status = builder_ref->Append(jbuf.ptr(), jbuf.length());
                if (!status.ok()) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            arrow::Status status = builder_ref->Append("{}", 2);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
      break;
    }
    case JniWriter::OdpsType::ARRAY:
    case JniWriter::OdpsType::STRUCT:
    case JniWriter::OdpsType::INTERVAL_DAY_TIME:
    case JniWriter::OdpsType::INTERVAL_YEAR_MONTH:
    case JniWriter::OdpsType::MAP:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(odps_type), K(ret));
      break;
  }
  return ret;
}

int ObSelectIntoOp::into_odps_jni_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  need_commit_ = false;
  const ObIArray<ObExpr *> &select_exprs = MY_SPEC.select_exprs_;

  ObArray<common::ObIVector *> expr_vectors;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval vector", K(ret));
    } else if (OB_FAIL(expr_vectors.push_back(select_exprs.at(i)->get_vector(eval_ctx_)))) {
      LOG_WARN("failed to push back vector", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to push back datum vector", K(ret));
  } else if (OB_ISNULL(uploader_.writer_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(uploader_.writer_ptr->get_current_block_addr())) {
    LOG_WARN("failed to get bloock addr");
  } else {
    // TODO alloc
    // ObIAllocator& alloc = ctx_.get_allocator();
    ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));//???

    int act_cnt = 0;
    ObDatum *datum = NULL;

    const ObIArray<JniWriter::OdpsType> &col_type_from_odps = uploader_.writer_ptr->get_schema_from_odps();
    const arrow::FieldVector &type_vec = arrow_schema_->fields();

    arrow::Result<std::shared_ptr<arrow::RecordBatchBuilder> > rbatchRes =
        arrow::RecordBatchBuilder::Make(arrow_schema_, &arrow_alloc_);
    if (!rbatchRes.ok()) {
      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
      LOG_WARN("fail to make empty record batch", K(ret));
    } else {
      std::shared_ptr<arrow::RecordBatchBuilder> rbatch = rbatchRes.ValueOrDie();
      int act_cnt = 0;
      // 向量化
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
        ObDatumMeta &meta = select_exprs.at(col_idx)->datum_meta_;
        ObObjMeta &obj_meta = select_exprs.at(col_idx)->obj_meta_;
        ObIVector *vector = expr_vectors.at(col_idx);
        arrow::ArrayBuilder *array_builder = rbatch->GetField(col_idx);
        if (OB_ISNULL(vector) || OB_ISNULL(array_builder) || OB_ISNULL(type_vec.at(col_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vector or array builder is unexpected null", K(ret));
        } else if (OB_FAIL(into_odps_jni_batch_one_col(col_idx,
                       col_type_from_odps.at(col_idx),
                       *type_vec.at(col_idx),
                       meta,
                       obj_meta,
                       *vector,
                       array_builder,
                       brs_,
                       act_cnt,
                       allocator))) {
          LOG_WARN("failed to into odps jni batch one col", K(ret));
        }
      }

      arrow::Result<std::shared_ptr<arrow::RecordBatch>> res;
      std::shared_ptr<arrow::StructArray> array;
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to build the batch", K(ret));
      } else if (FALSE_IT(res = rbatch->Flush())) {
        // do nothing
      } else if (!res.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        std::ostringstream stream;
        stream << res.status();
        LOG_WARN("fail to export array", K(ret), K(stream.str().c_str()));
      } else {
        struct ArrowSchema *c_schema = reinterpret_cast<struct ArrowSchema *>(uploader_.writer_ptr->get_schema_ptr());
        struct ArrowArray *c_array = reinterpret_cast<struct ArrowArray *>(uploader_.writer_ptr->get_array_ptr());
        std::shared_ptr<arrow::RecordBatch> record_batch_ptr = res.ValueOrDie();
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(c_array) || OB_ISNULL(c_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("array ptr is unexpected null", K(ret));
          } else {
            arrow::Status c_array_status = arrow::ExportRecordBatch(*record_batch_ptr, c_array, c_schema);
            if (!c_array_status.ok()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to export scheam", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(uploader_.writer_ptr->do_write_next_brs(0, act_cnt))) {
          LOG_WARN("failed to write data");
        } else {
          need_commit_ = true;
        }
      }
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

int ObSelectIntoOp::into_outfile_batch_csv(const ObBatchRows &brs, ObExternalFileWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<ObDatumVector> datum_vectors;
  ObDatum *datum = NULL;
  ObObj obj;
  ObDatumVector partition_datum_vector;
  ObCsvFileWriter *csv_data_writer = NULL;
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
    } else if (do_partition_ && OB_ISNULL(partition_datum_vector.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (do_partition_ && OB_FAIL(get_data_writer_for_partition(partition_datum_vector.at(i)->get_string(),
                                                                      data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    } else if (OB_ISNULL(csv_data_writer = static_cast<ObCsvFileWriter *>(data_writer))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    } else if (has_compress_ && OB_ISNULL(csv_data_writer->get_compress_stream_writer())
               && OB_FAIL(csv_data_writer->init_compress_writer(ctx_.get_allocator(),
                                                                external_properties_.csv_format_.compression_algorithm_,
                                                                MY_SPEC.buffer_size_))) {
      LOG_WARN("failed to init compress stream writer", K(ret));
    } else {
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
        if (OB_ISNULL(datum = datum_vectors.at(col_idx).at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is unexpected null", K(ret));
        } else if (OB_FAIL(datum->to_obj(obj,
                                         select_exprs.at(col_idx)->obj_meta_,
                                         select_exprs.at(col_idx)->obj_datum_map_))) {
          LOG_WARN("failed to get obj from datum", K(ret));
        } else if (!ob_is_text_tc(select_exprs.at(col_idx)->obj_meta_.get_type()) || obj.is_null()) {
          OZ(print_field(obj, *csv_data_writer));
        } else { // text tc
          OZ(print_lob_field(obj, *select_exprs.at(col_idx), *datum, *csv_data_writer));
        }
        // print field terminator
        if (OB_SUCC(ret) && col_idx != select_exprs.count() - 1) {
          OZ(write_obj_to_file(field_str_, *csv_data_writer));
        }
      }
      // print line terminator
      OZ(write_obj_to_file(line_str_, *csv_data_writer));
      // check if need split file
      OZ(check_csv_file_size(*csv_data_writer));
      // clear shared buffer
      OZ(csv_data_writer->flush_shared_buf(shared_buf_));
      if (has_compress_) {
        OZ(csv_data_writer->flush_buf());
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::get_parquet_logical_type(std::shared_ptr<const parquet::LogicalType> &logical_type,
                                             const ObObjType &obj_type,
                                             const int32_t precision,
                                             const int32_t scale)
{
  int ret = OB_SUCCESS;
  //todo@linyi oracle type
  if (ObTinyIntType == obj_type) {
    logical_type = parquet::LogicalType::Int(8, true);
  } else if (ObSmallIntType == obj_type) {
    logical_type = parquet::LogicalType::Int(16, true);
  } else if (ObMediumIntType == obj_type || ObInt32Type == obj_type) {
    logical_type = parquet::LogicalType::Int(32, true);
  } else if (ObIntType == obj_type) {
    logical_type = parquet::LogicalType::Int(64, true);
  } else if (ObUTinyIntType == obj_type) {
    logical_type = parquet::LogicalType::Int(8, false);
  } else if (ObUSmallIntType == obj_type) {
    logical_type = parquet::LogicalType::Int(16, false);
  } else if (ObUMediumIntType == obj_type || ObUInt32Type == obj_type) {
    logical_type = parquet::LogicalType::Int(32, false);
  } else if (ObUInt64Type == obj_type) {
    logical_type = parquet::LogicalType::Int(64, false);
  } else if (ob_is_float_tc(obj_type) || ob_is_double_tc(obj_type)) { // float, ufloat, double, udouble
    logical_type = parquet::LogicalType::None();
  } else if (ob_is_number_or_decimal_int_tc(obj_type)) {
    logical_type = parquet::LogicalType::Decimal(precision, scale);
  } else if (ob_is_datetime_or_mysql_datetime(obj_type)) {
    logical_type = parquet::LogicalType::Timestamp(false, parquet::LogicalType::TimeUnit::MICROS);
  } else if (ObTimestampType == obj_type) {
    logical_type = parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS);
  } else if (ObTimestampNanoType == obj_type || ObTimestampLTZType == obj_type) {
    logical_type = parquet::LogicalType::None();
  } else if (ob_is_date_or_mysql_date(obj_type)) {
    logical_type = parquet::LogicalType::Date();
  } else if (ob_is_time_tc(obj_type)) {
    logical_type = parquet::LogicalType::Time(false, parquet::LogicalType::TimeUnit::MICROS);
  } else if (ob_is_year_tc(obj_type)) {
    logical_type = parquet::LogicalType::Int(8, false);
  } else if (ob_is_string_type(obj_type) || ObNullType == obj_type || ObRawType == obj_type) {
    logical_type = parquet::LogicalType::String();
  } else if (ob_is_bit_tc(obj_type) /*uint64_t*/) {
    logical_type = parquet::LogicalType::Int(64, false);
  } else if (ob_is_enum_or_set_type(obj_type) /*uint64_t*/) {
    logical_type = parquet::LogicalType::Enum();
  } else {
    // TODO(bitao): support json/bson/uuid/map/list
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "convert this ob type to parquet type");
    LOG_WARN("unsupported obj type", K(ret), K(obj_type));
  }
  return ret;
}

int ObSelectIntoOp::get_parquet_physical_type(parquet::Type::type &physical_type,
                                              const ObObjType &obj_type)
{
  int ret = OB_SUCCESS;
  if (ObTinyIntType == obj_type || ObSmallIntType == obj_type
      || ObMediumIntType == obj_type || ObInt32Type == obj_type
      || ObUTinyIntType == obj_type || ObUSmallIntType == obj_type
      || ObUMediumIntType == obj_type || ObUInt32Type == obj_type
      || ob_is_date_or_mysql_date(obj_type) || ob_is_year_tc(obj_type)) {
    physical_type = parquet::Type::INT32;
  } else if (ObIntType == obj_type || ObUInt64Type == obj_type
             || ob_is_datetime_or_mysql_datetime_tc(obj_type)
             || ob_is_time_tc(obj_type) || ob_is_bit_tc(obj_type)) {
    physical_type = parquet::Type::INT64;
  } else if (ObTimestampNanoType == obj_type || ObTimestampLTZType == obj_type) {
    physical_type = parquet::Type::INT96;
  } else if (ob_is_float_tc(obj_type)) { // float, ufloat
    physical_type = parquet::Type::FLOAT;
  } else if (ob_is_double_tc(obj_type)) { // double, udouble
    physical_type = parquet::Type::DOUBLE;
  } else if (ob_is_number_or_decimal_int_tc(obj_type)) {
    physical_type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
  } else if (ob_is_string_tc(obj_type) /*varchar,char,varbinary,binary*/
             || ob_is_text_tc(obj_type) /*TinyText,MediumText,Text,LongText,TinyBLOB,MediumBLOB,BLOB,LongBLOB*/
             || ob_is_enum_or_set_type(obj_type)
             || ObNullType == obj_type || ObRawType == obj_type) {
    physical_type = parquet::Type::BYTE_ARRAY;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "convert this ob type to parquet type");
    LOG_WARN("unsupported obj type", K(ret), K(obj_type));
  }
  return ret;
}

int ObSelectIntoOp::calc_parquet_decimal_length(int precision)
{
  // 放utils里?
  return std::ceil((1 + precision / std::log10(2)) / 8);
}


int ObSelectIntoOp::orc_type_mapping_of_ob_type(ObDatumMeta& meta, int max_length, std::unique_ptr<orc::Type>& orc_type)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = meta.get_type();
  int precision = 0;
  int scale = 0;
  int int_bytes = 0;
  if (ObTinyIntType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::BYTE);
  } else if (ObSmallIntType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::SHORT);
  } else if (ObMediumIntType == obj_type || ObInt32Type == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::INT);
  } else if (ObIntType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::LONG);
  } else if (ObFloatType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::FLOAT);
  } else if (ObDoubleType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::DOUBLE);
  } else if (ob_is_number_or_decimal_int_tc(obj_type)) {
    if (OB_FAIL(check_oracle_number(obj_type, meta.precision_, meta.scale_))) {
      LOG_WARN("not support number type", K(ret));
    } else {
      int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(meta.precision_);
      if (int_bytes <= sizeof(int128_t)) {
        orc_type = orc::createDecimalType(meta.precision_, meta.scale_);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this type is not supported in orc");
        LOG_WARN("unsupport type in orc", K(obj_type), K(int_bytes));
      }
    }
  } else if (ObTimestampType == obj_type || ObTimestampLTZType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::TIMESTAMP_INSTANT);
  } else if (ob_is_datetime_or_mysql_datetime(obj_type) || ObTimestampNanoType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::TIMESTAMP);
  } else if (ob_is_date_or_mysql_date(obj_type)) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::DATE);
  } else if (ObVarcharType == obj_type && meta.cs_type_ != CS_TYPE_BINARY) {
    orc_type = orc::createCharType(orc::TypeKind::VARCHAR, max_length);
  } else if (ObCharType == obj_type && meta.cs_type_ != CS_TYPE_BINARY) {
    if (!(is_oracle_mode() && meta.length_semantics_ == LS_BYTE)) {
      orc_type = orc::createCharType(orc::TypeKind::CHAR, max_length);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupport type for oracle mode byte LENGTH BYTES in orc", K(obj_type), K(meta.length_semantics_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "oracle mode char length by bytes not support in orc");
    }
  } else if (ObYearType == obj_type) {
    orc_type = orc::createPrimitiveType(orc::TypeKind::INT);
  } else if (ObNullType == obj_type || ObRawType == obj_type
             || (CS_TYPE_BINARY == meta.cs_type_ && ob_is_string_type(obj_type))) {
    orc_type = orc::createCharType(orc::TypeKind::BINARY, max_length);
  } else if (CS_TYPE_BINARY != meta.cs_type_ && ob_is_string_type(obj_type)) { // not binary
    orc_type = orc::createCharType(orc::TypeKind::STRING, max_length);
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupport type in orc", K(obj_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "unsupported column type in orc file");
  }
  return ret;
}

int ObSelectIntoOp::create_orc_schema(std::unique_ptr<orc::Type> &schema)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &select_exprs = MY_SPEC.select_exprs_;
  if (schema == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is not null", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    ObString alias_name = MY_SPEC.alias_names_.strs_.at(i);
    std::string column_name(alias_name.ptr(), alias_name.length());
    std::unique_ptr<orc::Type> column_type;
    if (OB_FAIL(orc_type_mapping_of_ob_type(select_exprs.at(i)->datum_meta_,
                                            select_exprs.at(i)->max_length_,
                                            column_type))) {
      LOG_WARN("unsupported type ob the column", K(ret));
    } else {
      try {
        schema->addStructField(column_name, std::move(column_type));
      } catch (...) {
        ret = OB_ERR_FIELD_NOT_FOUND_PART;
        LOG_WARN("failed to add struct field", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::setup_orc_schema()
{
  int ret = OB_SUCCESS;
  try {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOrc"));
    orc_schema_ = orc::createStructType();
    if (OB_FAIL(create_orc_schema(orc_schema_))) {
      LOG_WARN("create orc schema failed", K(ret));
    }
  } catch (const std::exception& e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("catch the exception in setup_orc_schema", K(ret), "execption", e.what());
  } catch (...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error in setup_orc_schema", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::setup_parquet_schema()
{
  int ret = OB_SUCCESS;
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoParquet"));
  parquet::schema::NodeVector fields;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  std::shared_ptr<const parquet::LogicalType> logical_type;
  parquet::Type::type physical_type;
  parquet::schema::NodePtr node;
  try {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      ObDatumMeta meta = select_exprs.at(i)->datum_meta_;
      ObObjType obj_type = meta.get_type();
      ObString alias_name = MY_SPEC.alias_names_.strs_.at(i);
      std::string column_name(alias_name.ptr(), alias_name.length());
      int primitive_length = -1;
      if (OB_FAIL(check_oracle_number(obj_type,
                                      select_exprs.at(i)->datum_meta_.precision_,
                                      select_exprs.at(i)->datum_meta_.scale_))) {
        LOG_WARN("not support number type", K(ret));
      } else if (OB_FAIL(get_parquet_logical_type(logical_type,
                                                  obj_type,
                                                  select_exprs.at(i)->datum_meta_.precision_,
                                                  select_exprs.at(i)->datum_meta_.scale_))) {
        LOG_WARN("failed to get related logical type", K(ret));
      } else if (OB_FAIL(get_parquet_physical_type(physical_type, obj_type))) {
        LOG_WARN("failed to get related physical type", K(ret));
      } else if (ob_is_number_or_decimal_int_tc(obj_type)
                && OB_FALSE_IT(primitive_length = calc_parquet_decimal_length(
                                                      select_exprs.at(i)->datum_meta_.precision_))) {
      } else {
        //todo@linyi repetition level
        node = parquet::schema::PrimitiveNode::Make(column_name, parquet::Repetition::OPTIONAL,
                                                    logical_type, physical_type, primitive_length);
        fields.push_back(node);
      }
    }
    if (OB_SUCC(ret)) {
      parquet_writer_schema_ = std::static_pointer_cast<parquet::schema::GroupNode>(
          parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception when setup parquet schema", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("caught exception when setup parquet schema", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::into_outfile_batch_parquet(const ObBatchRows &brs, ObExternalFileWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<common::ObIVector*> expr_vectors;
  common::ObIVector* partition_vector;
  int64_t estimated_bytes = 0;
  int64_t row_group_size = 0;
  int64_t file_size = 0;
  ObParquetFileWriter *parquet_data_writer = NULL;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode());
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval vector", K(ret));
    } else if (OB_FAIL(expr_vectors.push_back(select_exprs.at(i)->get_vector(eval_ctx_)))) {
      LOG_WARN("failed to push back vector", K(ret));
    }
  }
  if (OB_SUCC(ret) && do_partition_) {
    if (OB_FAIL(MY_SPEC.file_partition_expr_->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval batch", K(ret));
    } else if (OB_ISNULL(partition_vector = MY_SPEC.file_partition_expr_->get_vector(eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null vector", K(ret));
    }
  }
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < brs.size_; ++row_idx) {
    if (brs.skip_->contain(row_idx)) {
      // do nothing
    } else if (do_partition_ && OB_FAIL(get_data_writer_for_partition(partition_vector->get_string(row_idx),
                                                                      data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    } else if (OB_ISNULL(parquet_data_writer = static_cast<ObParquetFileWriter*>(data_writer))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    } else if (parquet_data_writer->is_file_writer_null()
               && OB_FAIL(parquet_data_writer->open_parquet_file_writer(arrow_alloc_,
                                                                        external_properties_.parquet_format_.row_group_size_,
                                                                        external_properties_.parquet_format_.compress_type_index_,
                                                                        brs.size_,
                                                                        ctx_.get_allocator()))) {
      LOG_WARN("failed to init parquet file writer", K(ret));
    } else if (!parquet_data_writer->is_valid_to_write()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      try {
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); col_idx++) {
          if (OB_FAIL(build_parquet_cell(parquet_data_writer->get_row_group_writer(),
                                         select_exprs.at(col_idx)->datum_meta_,
                                         select_exprs.at(col_idx)->obj_meta_,
                                         expr_vectors.at(col_idx),
                                         col_idx,
                                         row_idx,
                                         parquet_data_writer->get_row_batch_offset(),
                                         parquet_data_writer->get_parquet_value_offsets().at(col_idx),
                                         parquet_data_writer->get_parquet_row_def_levels().at(col_idx),
                                         parquet_data_writer->get_batch_allocator(),
                                         parquet_data_writer->get_parquet_row_batch().at(col_idx),
                                         date_sql_mode))) {
            LOG_WARN("failed to build parquet cell", K(ret));
          }
        }
        parquet_data_writer->set_batch_written(false);
        parquet_data_writer->increase_row_batch_offset();
        if (OB_FAIL(ret)) {
        } else if (parquet_data_writer->reach_batch_end()) {
          if (OB_FAIL(parquet_data_writer->write_file())) {
            LOG_WARN("failed to write parquet row batch", K(ret));
          } else if (OB_FAIL(check_parquet_file_size(*parquet_data_writer))) {
            LOG_WARN("failed to check parquet file size", K(ret));
          }
          parquet_data_writer->set_batch_written(true);
          parquet_data_writer->reset_row_batch_offset();
          parquet_data_writer->reset_value_offsets();
        }
      } catch (const std::exception& ex) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("caught exception when write parquet file", K(ret), "Info", ex.what());
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
        }
      } catch (...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("caught exception when write parquet file", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::get_data_from_expr_vector(const common::ObIVector* expr_vector,
                                              int row_idx,
                                              ObObjType type,
                                              int64_t &value,
                                              const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  int32_t date;
  switch(type) {
    case ObTinyIntType:
      value = expr_vector->get_tinyint(row_idx);
      break;
    case ObSmallIntType:
      value = expr_vector->get_smallint(row_idx);
      break;
    case ObMediumIntType:
      value = expr_vector->get_mediumint(row_idx);
      break;
    case ObInt32Type:
      value = expr_vector->get_int32(row_idx);
      break;
    case ObIntType:
      value = expr_vector->get_int(row_idx);
      break;
    case ObYearType:
      value = expr_vector->get_year(row_idx);
      break;
    case ObDateType:
      value = expr_vector->get_date(row_idx);
      break;
    case ObMySQLDateType:
      ret = ObTimeConverter::mdate_to_date(expr_vector->get_mysql_date(row_idx), date, date_sql_mode);
      value = date;
      break;
    case ObMySQLDateTimeType:
      ret = ObTimeConverter::mdatetime_to_datetime(expr_vector->get_mysql_datetime(row_idx), value,
                                                   date_sql_mode);
      break;
    default:
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObSelectIntoOp::into_outfile_batch_orc(const ObBatchRows &brs, ObExternalFileWriter *data_writer)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = MY_SPEC.select_exprs_;
  ObArray<common::ObIVector*> expr_vectors;
  common::ObIVector* partition_vector;
  int64_t file_size = 0;
  orc::StructVectorBatch *root = NULL;
  ObOrcFileWriter *orc_data_writer = NULL;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode());
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval vector", K(ret));
    } else if (OB_FAIL(expr_vectors.push_back(select_exprs.at(i)->get_vector(eval_ctx_)))) {
      LOG_WARN("failed to push back vector", K(ret));
    }
  }
  if (OB_SUCC(ret) && do_partition_) {
    if (OB_FAIL(MY_SPEC.file_partition_expr_->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval batch", K(ret));
    } else if (OB_ISNULL(partition_vector = MY_SPEC.file_partition_expr_->get_vector(eval_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null vector", K(ret));
    }
  }
  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < brs.size_; ++row_idx) {
    if (brs.skip_->contain(row_idx)) {
      // do nothing
    } else if (do_partition_ && OB_FAIL(get_data_writer_for_partition(partition_vector->get_string(row_idx),
                                                                      data_writer))) {
      LOG_WARN("failed to set data writer for partition", K(ret));
    } else if (OB_ISNULL(orc_data_writer = static_cast<ObOrcFileWriter*>(data_writer))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null data writer", K(ret));
    } else if (orc_data_writer->is_file_writer_null()
               && OB_FAIL(orc_data_writer->open_orc_file_writer(*orc_schema_, options_, brs.size_))) {
      LOG_WARN("failed to init orc file writer", K(ret));
    } else if (!orc_data_writer->is_valid_to_write(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file writer unexpected null error", K(ret));
    } else {
      try {
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); col_idx++) {
          if (OB_FAIL(build_orc_cell(select_exprs.at(col_idx)->datum_meta_,
                                     select_exprs.at(col_idx)->obj_meta_,
                                     expr_vectors.at(col_idx),
                                     col_idx,
                                     row_idx,
                                     orc_data_writer->get_row_batch_offset(),
                                     root->fields[col_idx],
                                     orc_data_writer->get_batch_allocator(),
                                     date_sql_mode))) {
            LOG_WARN("failed to build orc cell", K(ret));
          }
        }
        orc_data_writer->set_batch_written(false);
        orc_data_writer->increase_row_batch_offset();
        if (OB_FAIL(ret)) {
        } else if (orc_data_writer->reach_batch_end()) {
          if (OB_FAIL(orc_data_writer->write_file())) {
            LOG_WARN("failed to write parquet row batch", K(ret));
          } else if (OB_FAIL(check_orc_file_size(*orc_data_writer))) {
            LOG_WARN("failed to check parquet file size", K(ret));
          }
          orc_data_writer->set_batch_written(true);
          orc_data_writer->reset_row_batch_offset();
        }
      } catch (const std::exception& ex) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("caught exception when write orc file", K(ret), "Info", ex.what());
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, ex.what());
        }
      } catch (...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("caught exception when write orc file", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::build_orc_cell(const ObDatumMeta &datum_meta,
                                   const ObObjMeta &obj_meta,
                                   const common::ObIVector* expr_vector,
                                   int64_t col_idx,
                                   int64_t row_idx,
                                   int64_t row_offset,
                                   orc::ColumnVectorBatch* col_vector_batch,
                                   ObIAllocator &allocator,
                                   const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_vector_batch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(col_idx), K(row_idx));
  } else if (ob_is_integer_type(datum_meta.type_)
             || ObYearType == datum_meta.type_
             || ob_is_date_or_mysql_date(datum_meta.type_)) {
    orc::LongVectorBatch *long_batch = static_cast<orc::LongVectorBatch *>(col_vector_batch);
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      if (OB_FAIL(get_data_from_expr_vector(expr_vector, row_idx, datum_meta.type_, long_batch->data[row_offset], date_sql_mode))) {
        LOG_WARN("faild to get data from expr vector", K(ret), K(col_idx), K(row_idx), K(datum_meta.type_));
      }
    }
  } else if (ob_is_number_or_decimal_int_tc(datum_meta.type_)) {
    int int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
    const ObDecimalInt* value;
    ObDecimalIntBuilder tmp_dec_alloc;
    ObDecimalInt* tmp_decimal;
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      if (ob_is_decimal_int_tc(datum_meta.get_type())) {
        value = expr_vector->get_decimal_int(row_idx);
      } else if (ob_is_number_tc(datum_meta.get_type())) {
        number::ObNumber number(expr_vector->get_number(row_idx));
        if (OB_FAIL(wide::from_number_to_decimal_fixed_length(number, tmp_dec_alloc, datum_meta.scale_,
                                                              int_bytes, tmp_decimal))){
          LOG_WARN("failed to case number to decimal int", K(ret));
        } else {
          value = tmp_decimal;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (int_bytes <= sizeof(int64_t)) {
        orc::Decimal64VectorBatch *decimal64vectorbatch = static_cast<orc::Decimal64VectorBatch *>(col_vector_batch);
        decimal64vectorbatch->precision = datum_meta.precision_;
        decimal64vectorbatch->scale = datum_meta.scale_;
        if (int_bytes == sizeof(int32_t)) {
          decimal64vectorbatch->values[row_offset] = value->int32_v_[0];
        } else {
          decimal64vectorbatch->values[row_offset] = value->int64_v_[0];
        }
      } else if (int_bytes <= sizeof(int128_t)) {
        orc::Decimal128VectorBatch *decimal128vectorbatch = static_cast<orc::Decimal128VectorBatch *>(col_vector_batch);
        decimal128vectorbatch->precision = datum_meta.precision_;
        decimal128vectorbatch->scale = datum_meta.scale_;
        decimal128vectorbatch->values[row_offset] = orc::Int128(value->int128_v_[0] >> 64, value->int128_v_[0]);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this decimal type for orc");
        LOG_WARN("unsupport type for orc", K(datum_meta.type_), K(datum_meta.precision_), K(int_bytes));
      }
    }
  } else if (ObDoubleType == datum_meta.type_ || ObFloatType == datum_meta.type_) {
    orc::DoubleVectorBatch *double_vector_batch = static_cast<orc::DoubleVectorBatch *>(col_vector_batch);
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      double value = (datum_meta.type_ == ObDoubleType)
                     ? expr_vector->get_double(row_idx)
                     : expr_vector->get_float(row_idx);
      double_vector_batch->data[row_offset] = value;
    }
  } else if (ob_is_text_tc(datum_meta.type_) || ob_is_string_tc(datum_meta.type_) || ObRawType == datum_meta.type_
             || ObNullType == datum_meta.type_) {
    orc::StringVectorBatch * string_vector_batch = static_cast<orc::StringVectorBatch *>(col_vector_batch);
    bool has_lob_header = obj_meta.has_lob_header();
    char *buf = nullptr;
    uint32_t res_len = 0;
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      if (OB_FAIL(calc_byte_array(expr_vector, row_idx, datum_meta, obj_meta, allocator, buf, res_len))) {
        LOG_WARN("failed to calc parquet byte array", K(ret), K(col_idx), K(row_idx));
      } else {
        string_vector_batch->data[row_offset] = buf;
        string_vector_batch->length[row_offset] = res_len;
      }
    }
  } else if (ob_is_datetime_or_mysql_datetime_tc(datum_meta.type_)) { // ObDatetimeType | ObTimestampType
    orc::TimestampVectorBatch *timestamp_vector_batch = static_cast<orc::TimestampVectorBatch *>(col_vector_batch);
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      int64_t out_usec = expr_vector->get_int(row_idx);
      if (ob_is_mysql_datetime(datum_meta.type_)
          && OB_FAIL(ObTimeConverter::mdatetime_to_datetime(
            expr_vector->get_mysql_datetime(row_idx), out_usec, date_sql_mode))) {
        LOG_WARN("mdatetime_to_datetime fail", K(ret));
      } else {
        timestamp_vector_batch->data[row_offset] = out_usec / USECS_PER_SEC;
        timestamp_vector_batch->nanoseconds[row_offset] = (out_usec % USECS_PER_SEC) * NSECS_PER_USEC; //  usec to nanosecond
      }
    }
  } else if (ObTimestampNanoType == datum_meta.type_ || ObTimestampLTZType == datum_meta.type_) {
    orc::TimestampVectorBatch *timestamp_vector_batch = static_cast<orc::TimestampVectorBatch *>(col_vector_batch);
    if (expr_vector->is_null(row_idx)) {
      col_vector_batch->hasNulls = true;
      col_vector_batch->notNull[row_offset] = false;
    } else {
      col_vector_batch->notNull[row_offset] = true;
      const ObOTimestampTinyData& rtime = expr_vector->get_otimestamp_tiny(row_idx);
      timestamp_vector_batch->data[row_offset] = rtime.time_us_ / USECS_PER_SEC; // usec to sec
      timestamp_vector_batch->nanoseconds[row_offset] = (rtime.time_us_ % USECS_PER_SEC) * NSECS_PER_USEC + rtime.to_timestamp_data().time_ctx_.tail_nsec_; //  usec to nanosecond
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(col_idx), K(row_idx), K(datum_meta.type_));
  }
  return ret;
}

int ObSelectIntoOp::check_orc_file_size(ObOrcFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  int64_t file_size = data_writer.get_file_size();
  if (file_need_split(file_size)) {
    if (OB_FAIL(split_file(data_writer))) {
      LOG_WARN("failed to split file", K(ret));
    }
  }
  return ret;
}

bool ObSelectIntoOp::file_need_split(int64_t file_size)
{
  return (file_location_ == IntoFileLocation::SERVER_DISK
          && !MY_SPEC.is_single_ && file_size > MY_SPEC.max_file_size_)
        || (file_location_ != IntoFileLocation::SERVER_DISK
            && ((!MY_SPEC.is_single_ && file_size > min(MY_SPEC.max_file_size_, MAX_OSS_FILE_SIZE))
                || (MY_SPEC.is_single_ && file_size > MAX_OSS_FILE_SIZE)));
}

int ObSelectIntoOp::check_oracle_number(ObObjType obj_type, int16_t &precision, int8_t scale)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode() && ob_is_number_tc(obj_type)) {
    if (scale == 0 && precision == -1) {
      precision = 38; // oracle int
    } else if (precision < 1 || scale < -84) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "number without specified precision and scale");
      LOG_WARN("not support number without specified precision and scale", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::calc_parquet_decimal_array(const common::ObIVector* expr_vector,
                                               int row_idx,
                                               const ObDatumMeta &datum_meta,
                                               int parquet_decimal_length,
                                               uint8_t* parquet_flba_ptr)
{
  int ret = OB_SUCCESS;
  const ObDecimalInt* ob_decimal;
  const uint8_t* decimal_bytes;
  ObDecimalIntBuilder tmp_dec_alloc;
  ObDecimalInt* tmp_decimal;
  int ob_decimal_length = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
  if (ob_is_decimal_int_tc(datum_meta.get_type())) {
    ob_decimal = expr_vector->get_decimal_int(row_idx);
  } else if (ob_is_number_tc(datum_meta.get_type())) {
    number::ObNumber number(expr_vector->get_number(row_idx));
    if (OB_FAIL(wide::from_number_to_decimal_fixed_length(number, tmp_dec_alloc, datum_meta.scale_,
                                                          ob_decimal_length, tmp_decimal))){
      LOG_WARN("failed to case number to decimal int", K(ret));
    } else {
      ob_decimal = tmp_decimal;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(datum_meta.get_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (ob_decimal_length < parquet_decimal_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected decimal length", K(ob_decimal_length), K(parquet_decimal_length), K(ret));
  } else {
    switch (ob_decimal_length) {
      case sizeof(int32_t):
      {
        decimal_bytes = reinterpret_cast<const uint8_t*>(ob_decimal->int32_v_);
        break;
      }
      case sizeof(int64_t):
      {
        decimal_bytes = reinterpret_cast<const uint8_t*>(ob_decimal->int64_v_);
        break;
      }
      case sizeof(int128_t):
      {
        decimal_bytes = reinterpret_cast<const uint8_t*>(ob_decimal->int128_v_);
        break;
      }
      case sizeof(int256_t):
      {
        decimal_bytes = reinterpret_cast<const uint8_t*>(ob_decimal->int256_v_);
        break;
      }
      case sizeof(int512_t):
      {
        decimal_bytes = reinterpret_cast<const uint8_t*>(ob_decimal->int512_v_);
        break;
      }
      default:
      {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ob_decimal_length), K(ret));
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < parquet_decimal_length; i++) {
    parquet_flba_ptr[i] = decimal_bytes[parquet_decimal_length - i - 1];
  }
  return ret;
}

int ObSelectIntoOp::calc_byte_array(const common::ObIVector* expr_vector,
                                    int row_idx,
                                    const ObDatumMeta &datum_meta,
                                    const ObObjMeta &obj_meta,
                                    ObIAllocator &allocator,
                                    char* &buf,
                                    uint32_t &res_len)
{
  int ret = OB_SUCCESS;
  ObString ob_str;
  ObString res_str;
  bool has_lob_header = obj_meta.has_lob_header();
  res_len = 0;
  buf = nullptr;
  int64_t buf_size = 0;
  if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, expr_vector, datum_meta,
                                                        has_lob_header, ob_str, row_idx))) {
    LOG_WARN("failed to get string", K(ret));
  } else if (ob_str.length() == 0 || CS_TYPE_BINARY == datum_meta.cs_type_
             || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
    if (OB_FAIL(ob_write_string(allocator, ob_str, res_str))) {
      LOG_WARN("failed to write string", K(ret));
    } else {
      res_len = static_cast<uint32_t>(res_str.length());
      buf = const_cast<char *>(res_str.ptr());
    }
  } else if (OB_FALSE_IT(buf_size = ob_str.length() * ObCharset::MAX_MB_LEN)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(buf_size));
  } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_, ob_str.ptr(),
                                                ob_str.length(), CS_TYPE_UTF8MB4_BIN,
                                                buf, buf_size, res_len, false, false))) {
    LOG_WARN("failed to convert charset", K(ret));
  }
  return ret;
}

int ObSelectIntoOp::oracle_timestamp_to_int96(const common::ObIVector* expr_vector,
                                              int64_t row_idx,
                                              const ObDatumMeta &datum_meta,
                                              parquet::Int96 &res)
{
  int ret = OB_SUCCESS;
  int64_t out_usec = 0;
  int32_t tmp_offset = 0;
  ObOTimestampData oracle_timestamp;
  if (ObTimestampTZType == datum_meta.type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support this type in parquet", K(ret));
  } else if (ObTimestampLTZType == datum_meta.type_ || ObTimestampNanoType == datum_meta.type_) {
    oracle_timestamp = expr_vector->get_otimestamp_tiny(row_idx).to_timestamp_data();
    out_usec = expr_vector->get_otimestamp_tiny(row_idx).time_us_;
  }
  // oracle timestamp logical type is none, only stored as utc
  // convert nano to utc
  if (OB_SUCC(ret) && ObTimestampNanoType == datum_meta.type_) {
    if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
      LOG_WARN("failed to get timezone offset", K(ret));
    } else {
      out_usec -= SEC_TO_USEC(tmp_offset);
    }
  }
  uint32_t julian_date_value = (out_usec / 86400000000LL) + 2440588;
  uint64_t nsec_time_value = oracle_timestamp.time_ctx_.tail_nsec_ + std::abs(out_usec % 86400000000LL) * 1000;
  res.value[2] = julian_date_value;
  res.value[1] = nsec_time_value >> 32;
  res.value[0] = nsec_time_value & UINT32_MAX;
  return ret;
}

int ObSelectIntoOp::check_parquet_file_size(ObParquetFileWriter &data_writer)
{
  int ret = OB_SUCCESS;
  int64_t row_group_size = data_writer.get_row_group_size();
  int64_t file_size = data_writer.get_file_size();
  if (file_need_split(file_size)) {
    if (OB_FAIL(split_file(data_writer))) {
      LOG_WARN("failed to split file", K(ret));
    } else {
      data_writer.set_write_bytes(0);
    }
  } else if (row_group_size > external_properties_.parquet_format_.row_group_size_) {
    data_writer.get_row_group_writer()->Close();
    data_writer.set_write_bytes(file_size);
    data_writer.open_next_row_group_writer();
  }
  return ret;
}
int ObSelectIntoOp::build_parquet_column_vector(parquet::RowGroupWriter* rg_writer,
                                                int col_idx,
                                                const ObBatchRows &brs,
                                                const ObDatumMeta &datum_meta,
                                                const ObObjMeta &obj_meta,
                                                const common::ObIVector* expr_vector,
                                                int64_t &estimated_bytes)
{
  int ret = OB_SUCCESS;
  int16_t null_definition_level = 0;
  int16_t normal_definition_level = 1;
  int16_t def_levels[brs.size_];
  int value_idx = 0;
  int def_idx = 0;
  std::shared_ptr<parquet::schema::PrimitiveNode> p_node;
  ObArenaAllocator allocator("IntoParquet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  parquet::ColumnWriter *col_writer = nullptr;
  if (OB_ISNULL(expr_vector) || !parquet_writer_schema_ || OB_ISNULL(rg_writer)
      || OB_ISNULL(col_writer = rg_writer->column(col_idx))
      || !(p_node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(parquet_writer_schema_->field(col_idx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret));
  } else if (p_node->is_group()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "group type in parquet");
    LOG_WARN("not support group type in parquet", K(ret));
  } else {
    switch (p_node->physical_type()) {
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArray values[brs.size_];
        parquet::ByteArrayWriter *writer = static_cast<parquet::ByteArrayWriter *>(col_writer);
        char *buf = nullptr;
        uint32_t res_len = 0;
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else if (OB_FAIL(calc_byte_array(expr_vector,
                                             row_idx,
                                             datum_meta,
                                             obj_meta,
                                             allocator,
                                             buf,
                                             res_len))) {
            LOG_WARN("failed to calc parquet byte array", K(ret));
          } else {
            values[value_idx].ptr = reinterpret_cast<const uint8_t *>(buf);
            values[value_idx].len = res_len;
            value_idx++;
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        if (OB_SUCC(ret)) {
          writer->WriteBatch(def_idx, def_levels, nullptr, values);
          estimated_bytes += writer->EstimatedBufferedValueBytes();
        }
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      {
        parquet::FixedLenByteArray values[brs.size_];
        parquet::FixedLenByteArrayWriter *writer = static_cast<parquet::FixedLenByteArrayWriter *>(col_writer);
        int parquet_decimal_length = writer->descr()->type_length();
        uint8 parquet_flba_ptr[brs.size_][parquet_decimal_length];
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else if (OB_FAIL(calc_parquet_decimal_array(expr_vector,
                                                        row_idx,
                                                        datum_meta,
                                                        parquet_decimal_length,
                                                        parquet_flba_ptr[row_idx]))) {
            LOG_WARN("failed to calc parquet decimal", K(ret));
          } else {
            values[value_idx++].ptr = parquet_flba_ptr[row_idx];
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        if (OB_SUCC(ret)) {
          writer->WriteBatch(def_idx, def_levels, nullptr, values);
          estimated_bytes += writer->EstimatedBufferedValueBytes();
        }
        break;
      }
      case parquet::Type::DOUBLE:
      {
        double values[brs.size_];
        parquet::DoubleWriter *writer = static_cast<parquet::DoubleWriter *>(col_writer);
        for (int64_t row_idx = 0; row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else {
            values[value_idx++] = expr_vector->get_double(row_idx);
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        writer->WriteBatch(def_idx, def_levels, nullptr, values);
        estimated_bytes += writer->EstimatedBufferedValueBytes();
        break;
      }
      case parquet::Type::FLOAT:
      {
        float values[brs.size_];
        parquet::FloatWriter *writer = static_cast<parquet::FloatWriter *>(col_writer);
        for (int64_t row_idx = 0; row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else {
            values[value_idx++] = expr_vector->get_float(row_idx);
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        writer->WriteBatch(def_idx, def_levels, nullptr, values);
        estimated_bytes += writer->EstimatedBufferedValueBytes();
        break;
      }
      case parquet::Type::INT32:
      {
        int32_t values[brs.size_];
        parquet::Int32Writer *writer = static_cast<parquet::Int32Writer *>(col_writer);
        for (int64_t row_idx = 0; row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else {
            values[value_idx++] = expr_vector->get_int32(row_idx);
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        writer->WriteBatch(def_idx, def_levels, nullptr, values);
        estimated_bytes += writer->EstimatedBufferedValueBytes();
        break;
      }
      case parquet::Type::INT64:
      {
        int64_t values[brs.size_];
        parquet::Int64Writer *writer = static_cast<parquet::Int64Writer *>(col_writer);
        for (int64_t row_idx = 0; row_idx < brs.size_; row_idx++) {
          if (brs.skip_->contain(row_idx)) {
            // do nothing
          } else if (expr_vector->is_null(row_idx)) {
            def_levels[def_idx++] = null_definition_level;
          } else {
            values[value_idx++] = expr_vector->get_int(row_idx);
            def_levels[def_idx++] = normal_definition_level;
          }
        }
        writer->WriteBatch(def_idx, def_levels, nullptr, values);
        estimated_bytes += writer->EstimatedBufferedValueBytes();
        break;
      }
      default:
      {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(p_node->physical_type()), K(ret));
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::build_parquet_cell(parquet::RowGroupWriter* rg_writer,
                                       const ObDatumMeta &datum_meta,
                                       const ObObjMeta &obj_meta,
                                       const common::ObIVector* expr_vector,
                                       int64_t col_idx,
                                       int64_t row_idx,
                                       int64_t row_offset,
                                       int64_t &value_offset,
                                       int16_t* definition_levels,
                                       ObIAllocator &allocator,
                                       void* value_batch,
                                       const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  int16_t null_definition_level = 0;
  int16_t normal_definition_level = 1;
  std::shared_ptr<parquet::schema::PrimitiveNode> p_node;
  parquet::ColumnWriter *col_writer = nullptr;
  if (OB_ISNULL(expr_vector) || !parquet_writer_schema_ || OB_ISNULL(rg_writer)
      || OB_ISNULL(col_writer = rg_writer->column(col_idx))
      || OB_ISNULL(definition_levels) || OB_ISNULL(value_batch)
      || !(p_node = std::static_pointer_cast<parquet::schema::PrimitiveNode>(parquet_writer_schema_->field(col_idx)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret));
  } else if (p_node->is_group()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "group type in parquet");
    LOG_WARN("not support group type in parquet", K(ret));
  } else {
    switch (p_node->physical_type()) {
      case parquet::Type::BYTE_ARRAY:
      {
        parquet::ByteArray* value = reinterpret_cast<parquet::ByteArray*>(value_batch);
        value += value_offset;
        char *buf = nullptr;
        uint32_t res_len = 0;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else if (OB_FAIL(calc_byte_array(expr_vector,
                                           row_idx,
                                           datum_meta,
                                           obj_meta,
                                           allocator,
                                           buf,
                                           res_len))) {
          LOG_WARN("failed to calc parquet byte array", K(ret));
        } else {
          value->ptr = reinterpret_cast<const uint8_t *>(buf);
          value->len = res_len;
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      {
        parquet::FixedLenByteArray* value = reinterpret_cast<parquet::FixedLenByteArray*>(value_batch);
        value += value_offset;
        parquet::FixedLenByteArrayWriter *writer = static_cast<parquet::FixedLenByteArrayWriter *>(col_writer);
        int parquet_decimal_length = writer->descr()->type_length();
        ObArrayWrap<uint8> parquet_flba;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else if (OB_FAIL(parquet_flba.allocate_array(allocator, parquet_decimal_length))) {
          LOG_WARN("failed to allocate array", K(ret));
        } else if (OB_FAIL(calc_parquet_decimal_array(expr_vector,
                                                      row_idx,
                                                      datum_meta,
                                                      parquet_decimal_length,
                                                      parquet_flba.get_data()))) {
          LOG_WARN("failed to calc parquet decimal", K(ret));
        } else {
          value->ptr = parquet_flba.get_data();
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::DOUBLE:
      {
        double* value = reinterpret_cast<double*>(value_batch);
        value += value_offset;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else {
          *value = expr_vector->get_double(row_idx);
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::FLOAT:
      {
        float* value = reinterpret_cast<float*>(value_batch);
        value += value_offset;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else {
          *value = expr_vector->get_float(row_idx);
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::INT32:
      {
        int32_t* value = reinterpret_cast<int32_t*>(value_batch);
        value += value_offset;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else if (ob_is_mysql_date_tc(datum_meta.type_)) {
          ObMySQLDate mdate(expr_vector->get_int32(row_idx));
          if (OB_FAIL(ObTimeConverter::mdate_to_date(mdate, *value, date_sql_mode))) {
            LOG_WARN("mdatetime_to_datetime fail", K(ret));
          } else {
            value_offset++;
            definition_levels[row_offset] = normal_definition_level;
          }
        } else {
          *value = expr_vector->get_int32(row_idx);
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::INT64:
      {
        int64_t* value = reinterpret_cast<int64_t*>(value_batch);
        value += value_offset;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else if (ob_is_mysql_datetime(datum_meta.type_)) {
          ObMySQLDateTime mdatetime(expr_vector->get_int(row_idx));
          if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime(mdatetime, *value, date_sql_mode))) {
            LOG_WARN("mdatetime_to_datetime fail", K(ret));
          } else {
            value_offset++;
            definition_levels[row_offset] = normal_definition_level;
          }
        } else {
          *value = expr_vector->get_int(row_idx);
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      case parquet::Type::INT96:
      {
        parquet::Int96* value = reinterpret_cast<parquet::Int96*>(value_batch);
        value += value_offset;
        if (expr_vector->is_null(row_idx)) {
          definition_levels[row_offset] = null_definition_level;
        } else if (OB_FAIL(oracle_timestamp_to_int96(expr_vector, row_idx, datum_meta, *value))) {
          LOG_WARN("failed to convert timestamp to int96", K(ret));
        } else {
          value_offset++;
          definition_levels[row_offset] = normal_definition_level;
        }
        break;
      }
      default:
      {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(p_node->physical_type()), K(ret));
      }
    }
  }
  return ret;
}

int ObSelectIntoOp::into_dumpfile(ObExternalFileWriter *data_writer)
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
  OZ(extract_fisrt_wchar_from_varhcar(field_str_, wchar_field));
  OZ(extract_fisrt_wchar_from_varhcar(line_str_, wchar_line));
  OZ(ObCharset::get_replace_character(cs_type_, wchar_replace));
  // wc->mb
  if (OB_ISNULL(buf = static_cast<char*>(ctx_.get_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(buf_len));
  }
  if (has_enclose_) {
    OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_enclose, escape_printer_.enclose_, cs_type_));
  }
  if (has_escape_) {
    OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_escape, escape_printer_.escape_, cs_type_));
  }
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_zero, escape_printer_.zero_, cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_field, escape_printer_.field_terminator_, cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_line, escape_printer_.line_terminator_, cs_type_));
  OZ(print_wchar_to_buf(buf, buf_len, pos, wchar_replace, escape_printer_.convert_replacer_, cs_type_));
  escape_printer_.coll_type_ = cs_type_;
  escape_printer_.ignore_convert_failed_ = true; // todo@linyi provide user-defined interface
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
    } else if (ob_is_json_tc(select_exprs.at(i)->obj_meta_.get_type()) ||
               ob_is_collection_sql_type(select_exprs.at(i)->obj_meta_.get_type())) {
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

int ObSelectIntoOp::get_data_writer_for_partition(const ObString &partition_str,
                                                  ObExternalFileWriter *&data_writer)
{
  int ret = OB_SUCCESS;
  ObString partition;
  ObExternalFileWriter *value = NULL;
  ObCsvFileWriter *csv_data_writer = NULL;
  if (OB_SUCC(partition_map_.get_refactored(partition_str, value))) {
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      data_writer = value;
    }
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
    LOG_WARN("get unexpected error", K(ret));
  } else if (curr_partition_num_ >= OB_MAX_PARTITION_NUM_ORACLE) {
    ret = OB_TOO_MANY_PARTITIONS_ERROR;
    LOG_WARN("too many partitions", K(ret));
  } else {
    ret = OB_SUCCESS;
    bool writer_added = false;
    if (OB_FAIL(new_data_writer(data_writer))) {
      LOG_WARN("failed to new data writer", K(ret));
    } else if (OB_ISNULL(data_writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_ && MY_SPEC.buffer_size_ > 0) {
      csv_data_writer = static_cast<ObCsvFileWriter*>(data_writer);
      if (OB_FAIL(csv_data_writer->alloc_buf(ctx_.get_allocator(), MY_SPEC.buffer_size_))) {
        LOG_WARN("failed to alloc buffer", K(ret));
      }
    }
    //add to hashmap
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_write_string(ctx_.get_allocator(),
                                       partition_str,
                                       partition))) {
      LOG_WARN("failed to write string", K(ret));
    } else if (OB_FAIL(partition_map_.set_refactored(partition, data_writer))) {
      LOG_WARN("failed to add data writer to map", K(ret));
    } else {
      curr_partition_num_++;
      writer_added = true;
    }
    if (OB_FAIL(ret) && NULL != data_writer && !writer_added) {
      data_writer->~ObExternalFileWriter();
    }
    //calc file path
    if (OB_SUCC(ret) && OB_FAIL(calc_file_path_with_partition(partition, *data_writer))) {
      LOG_WARN("failed to calc file path with partition", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::create_the_only_data_writer(ObExternalFileWriter *&data_writer)
{
  int ret = OB_SUCCESS;
  ObCsvFileWriter *csv_data_writer = NULL;
  if (OB_FAIL(new_data_writer(data_writer))) {
    LOG_WARN("failed to new data writer", K(ret));
  } else if (OB_ISNULL(data_writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    data_writer->url_ = basic_url_;
    data_writer_ = data_writer;
  }
  if (OB_FAIL(ret)) {
  } else if (T_INTO_OUTFILE == MY_SPEC.into_type_ && MY_SPEC.is_single_
             && OB_FAIL(data_writer->open_file())) {
    LOG_WARN("failed to open file", K(ret));
  } else if (ObExternalFileFormat::FormatType::CSV_FORMAT == format_type_ && MY_SPEC.buffer_size_ > 0) {
    csv_data_writer = static_cast<ObCsvFileWriter*>(data_writer);
    if (OB_FAIL(csv_data_writer->alloc_buf(ctx_.get_allocator(), MY_SPEC.buffer_size_))) {
      LOG_WARN("failed to alloc buffer", K(ret));
    }
  }
  return ret;
}

int ObSelectIntoOp::new_data_writer(ObExternalFileWriter *&data_writer)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  switch (format_type_)
  {
    case ObExternalFileFormat::FormatType::CSV_FORMAT:
    {
      if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObCsvFileWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObCsvFileWriter)));
      } else {
        data_writer = new(ptr) ObCsvFileWriter(access_info_, file_location_, use_shared_buf_,
                                               has_compress_, has_lob_, write_offset_);
      }
      break;
    }
    case ObExternalFileFormat::FormatType::PARQUET_FORMAT:
    {
      if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObParquetFileWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObParquetFileWriter)));
      } else {
        data_writer = new(ptr) ObParquetFileWriter(access_info_, file_location_, parquet_writer_schema_);
      }
      break;
    }
    case ObExternalFileFormat::FormatType::ORC_FORMAT:
    {
      if (OB_ISNULL(ptr = ctx_.get_allocator().alloc(sizeof(ObOrcFileWriter)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObOrcFileWriter)));
      } else {
        data_writer = new(ptr) ObOrcFileWriter(access_info_, file_location_);
      }
      break;
    }
    default:
    {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support select into type", K(format_type_));
    }
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

#ifdef OB_BUILD_JNI_ODPS
int ObSelectIntoOp::odps_jni_commit_upload()
{
  int ret = OB_SUCCESS;

  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  if (is_in_px) {
    ObOdpsJniUploaderMgr &odps_upload_mgr = ctx_.get_sqc_handler()->get_sqc_ctx().gi_pump_.get_odps_jni_uploader_mgr();
    if (!need_commit_) {
      odps_upload_mgr.set_fail();
    }
    {
      __sync_synchronize();
      if (OB_ISNULL(uploader_.writer_ptr.get())) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("failed to get the writer by sid", K(ret));
      } else if (OB_FAIL(uploader_.writer_ptr->finish_write())) {
        LOG_WARN("failed to finish write");
      } else {
        int64_t ref = odps_upload_mgr.dec_ref();
        if (0 > ref) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected ref", K(ref), K(ret));
        } else if (0 == ref && odps_upload_mgr.could_commit()) {
          uploader_.writer_ptr->commit_session();
        }
      }
      odps_upload_mgr.release_hold_session();
    }
  } else {
    if (OB_ISNULL(uploader_.writer_ptr.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->finish_write())) {
      LOG_WARN("failed to finish write", K(ret));
    } else if (need_commit_) {
      if (OB_FAIL(uploader_.writer_ptr->commit_session())) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("commmit failed in odps write");
      }
    }
  }
  return ret;
}
#endif

void ObSelectIntoOp::destroy()
{
  ObExternalFileWriter *data_writer = NULL;
  if (ObExternalFileFormat::FormatType::ODPS_FORMAT == format_type_) {
    if (!GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS)
      ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
      upload_.reset();
      record_writer_.reset();
#endif
    } else {
#if defined(OB_BUILD_JNI_ODPS)
      if (OB_NOT_NULL(uploader_.writer_ptr.get())) {
        uploader_.writer_ptr->do_close();
      }
      if (OB_NOT_NULL(arrow_schema_.get())) {
        arrow_schema_.reset();
      }
#endif
    }
  } else if (do_partition_) {
    for (ObPartitionWriterMap::iterator iter = partition_map_.begin();
         iter != partition_map_.end(); iter++) {
      if (OB_ISNULL(data_writer = iter->second)) {
      } else {
        data_writer->~ObExternalFileWriter();
      }
    }
  } else if (OB_NOT_NULL(data_writer_)) {
    data_writer_->~ObExternalFileWriter();
  }
  {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoParquet"));
    parquet_writer_schema_.reset();
  }
  {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOrc"));
    orc_schema_.reset();
  }
  external_properties_.~ObExternalFileFormat();
  partition_map_.destroy();
  ObOperator::destroy();
}



}
}
