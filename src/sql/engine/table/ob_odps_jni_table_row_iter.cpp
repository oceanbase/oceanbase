#ifdef OB_BUILD_JNI_ODPS
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
#include "ob_odps_jni_table_row_iter.h"
#include <arrow/api.h>
#include <arrow/type.h>
#include <memory>

#include "share/external_table/ob_external_table_utils.h"
#include "lib/charset/ob_charset.h"
#include "lib/charset/ob_charset_string_helper.h"
#include "sql/engine/connector/ob_jni_scanner.h"
#include "sql/engine/connector/ob_java_env.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

namespace oceanbase {
namespace sql {

const char *ObODPSJNITableRowIterator::NON_PARTITION_FLAG = "__NaN__";
const char *ObODPSJNITableRowIterator::DATETIME_PREFIX = "DATETIME ";
const char *ObODPSJNITableRowIterator::TIMESTAMP_PREFIX = "TIMESTAMP ";
const char *ObODPSJNITableRowIterator::TIMESTAMP_NTZ_PREFIX = "TIMESTAMP_NTZ ";

int ObODPSJNITableRowIterator::init_jni_schema_scanner(const ObODPSGeneralFormat &odps_format, const ObSQLSessionInfo* session_ptr_in)
{
  int ret = OB_SUCCESS;
  ObJavaEnv &java_env = ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (!GCONF.ob_enable_java_env) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("java env is not enabled", K(ret));
  } else if (!java_env.is_env_inited()) {
    if (OB_FAIL(java_env.setup_java_env())) {
      LOG_WARN("failed to setup java env", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!java_env.is_env_inited()) {
    // Recheck the java env whether is ready.
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("java env is not ready for scanner", K(ret));
  } else if (OB_FAIL(odps_format_.deep_copy(odps_format))) {
    LOG_WARN("failed to deep copy odps format", K(ret));
  } else if (OB_FAIL(odps_format_.decrypt())) {
    LOG_WARN("failed to decrypt odps format", K(ret));
  } else {
    if (!odps_params_map_.created() && OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE, "JNITableParams"))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(init_required_mini_params(session_ptr_in))) {
      LOG_WARN("failed to init required params", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(
                   ObString::make_string("service"), ObString::make_string("schema")))) {
      LOG_WARN("failed to init service param", K(ret));
    } else { /* do nothing */
      if (odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (odps_format_.api_mode_ == ObODPSGeneralFormat::ApiMode::BYTE) {
          if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("split_option"), ObString::make_string("byte"), 1))) {
            LOG_WARN("failed to init service param", K(ret));
          }
        } else {
          if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("split_option"), ObString::make_string("rows")))) {
            LOG_WARN("failed to init service param", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    odps_jni_schema_scanner_ = create_odps_jni_scanner(true);
    LOG_TRACE("timer init schema scanner and open connection");
    if (OB_FAIL(odps_jni_schema_scanner_->do_init(odps_params_map_))) {
      LOG_WARN("failed to init odps jni scanner", K(ret));
    } else if (OB_FAIL(odps_jni_schema_scanner_->do_open())) {
      // NOTE!!! only open schema scanner directly before using
      LOG_WARN("failed to open odps jni schema scanner", K(ret));
    } else { /* do nothing */
    }
    LOG_TRACE("timer init schema scanner and end open connection");
  }

  // 成功的分支revert_scan_iter兜底
  // 失败分支失败或者临时变量的时候处理析构函数兜底
  return ret;
}

int ObODPSJNITableRowIterator::init_jni_meta_scanner(const ObODPSGeneralFormat &odps_format, const ObSQLSessionInfo* session_ptr_in)
{
  int ret = OB_SUCCESS;
  ObJavaEnv &java_env = ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (!GCONF.ob_enable_java_env) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("java env is not enabled", K(ret));
  } else if (!java_env.is_env_inited()) {
    if (OB_FAIL(java_env.setup_java_env())) {
      LOG_WARN("failed to setup java env", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (!java_env.is_env_inited()) {
    // Recheck the java env whether is ready.
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("java env is not ready for scanner", K(ret));
  } else if (OB_FAIL(odps_format_.deep_copy(odps_format))) {
    LOG_WARN("failed to deep copy odps format", K(ret));
  } else if (OB_FAIL(odps_format_.decrypt())) {
    LOG_WARN("failed to decrypt odps format", K(ret));
  } else {
    if (!odps_params_map_.created() && OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE, "JNITableParams"))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(init_required_mini_params(session_ptr_in))) {
      LOG_WARN("failed to init required params", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(
                   ObString::make_string("service"), ObString::make_string("meta"), 1))) {
      LOG_WARN("failed to init service param", K(ret));
    } else {
      if (odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (predicate_buf_ != nullptr && OB_FAIL(odps_params_map_.set_refactored(
                    ObString::make_string("pushdown_predicate"), ObString(predicate_buf_len_, predicate_buf_)))) {
          LOG_WARN("failed to set pushdown predicate");
        } else if (odps_format_.api_mode_ == ObODPSGeneralFormat::ApiMode::BYTE) {
          if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("split_option"), ObString::make_string("byte"), 1))) {
            LOG_WARN("failed to init service param", K(ret));
          }
        } else {
          if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("split_option"), ObString::make_string("rows"), 1))) {
            LOG_WARN("failed to init service param", K(ret));
          }
        }
        if (predicate_buf_ != nullptr) {
          LOG_INFO("show storage api pushdown filter", "filter", ObString(predicate_buf_len_, predicate_buf_));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("timer init meta scanner and open connection");
    odps_jni_schema_scanner_ = create_odps_jni_scanner(true);
    if (OB_FAIL(odps_jni_schema_scanner_->do_init(odps_params_map_))) {
      LOG_WARN("failed to init odps jni scanner", K(ret));
    } else if (OB_FAIL(odps_jni_schema_scanner_->do_open())) {
      // NOTE!!! only open schema scanner directly before using
      LOG_WARN("failed to open odps jni schema scanner", K(ret));
    } else { /* do nothing */
    }
    LOG_TRACE("timer init schema scanner and end connection");
  }

  // 成功的分支revert_scan_iter兜底
  // 失败分支失败或者临时变量的时候处理析构函数兜底
  return ret;
}

/*
 * service Init
 */
int ObODPSJNITableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
    LOG_WARN("failed to call ObExternalTableRowIterator::init", K(ret));
  } else if (OB_FAIL(prepare_bit_vector())) {
    LOG_WARN("failed to init bit vector", K(ret));
  } else if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->ext_file_column_exprs_) || OB_ISNULL(scan_param_->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrong scan param for scan");
  } else {
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    if (scan_param->external_file_format_.odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
      if (OB_FAIL(init_jni_schema_scanner(scan_param->external_file_format_.odps_format_, eval_ctx.exec_ctx_.get_my_session()))) {
        // Using schema scanner to get columns
        LOG_WARN("failed to init odps jni scanner", K(ret));
      }
    } else {
      if (OB_FAIL(init_jni_meta_scanner(scan_param->external_file_format_.odps_format_, eval_ctx.exec_ctx_.get_my_session()))) {
        // Using schema scanner to get columns
        LOG_WARN("failed to init odps jni scanner", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      session_ptr_ = eval_ctx.exec_ctx_.get_my_session();
      if (OB_ISNULL(session_ptr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      } else if (OB_FAIL(session_ptr_->get_sys_variable(SYS_VAR_TIME_ZONE, timezone_str_))) {
        LOG_WARN("failed to get store idx", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      int ret_more = OB_SUCCESS;
      int32_t offset_sec = 0;
      if (OB_FAIL(ObTimeConverter::str_to_offset(timezone_str_, offset_sec, ret_more,
                                is_oracle_mode(), true))) {
        if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
          LOG_WARN("fail to convert str_to_offset", K(timezone_str_), K(ret));
        } else {
          timezone_ret_ = OB_ERR_UNKNOWN_TIME_ZONE;
        }
      } else {
        timezone_offset_ = offset_sec;
      }
    }
    if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
      ret = OB_SUCCESS;
      MEMCPY(ob_time_.tz_name_, timezone_str_.ptr(), timezone_str_.length());
      ob_time_.tz_name_[timezone_str_.length()] = '\0';
      ob_time_.is_tz_name_valid_ = true;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("service"), ObString::make_string("reader"), 1))) {
      LOG_WARN("failed to add service options");
    } else if (OB_FAIL(pull_and_prepare_column_exprs(*scan_param_->ext_file_column_exprs_))) {
      LOG_WARN("failed to pull and prepare column exprs");
    } else if (OB_FAIL(init_all_columns_name_as_odps_params())) {
      LOG_WARN("failed to init expected columns and related types", K(ret));
    }
  }

  // 成功的分支revert_scan_iter兜底
  // 失败分支失败处理析构函数兜底
  return ret;
}


int ObODPSJNITableRowIterator::init_required_mini_params(const ObSQLSessionInfo* session_ptr_in)
{
  int ret = OB_SUCCESS;
  ObString access_id;
  ObString access_key;
  ObString project;
  ObString table;
  ObString region;
  ObString schema;
  ObString quota;
  ObString endpoint;
  ObString tunnel_endpoint;
  ObString compression_code;
  ObString trace_id;
  char buf_trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
  ObString tmp_trace_id;
  if (OB_ISNULL(session_ptr_in)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params map", K(ret));
  } else {
    int64_t len = session_ptr_in->get_current_trace_id().to_string(buf_trace_id, sizeof(buf_trace_id));
    tmp_trace_id = ObString(len, buf_trace_id);
  }

  if (OB_FAIL(ret)) {
  } else if (!odps_params_map_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params map", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_alloc_, odps_format_.access_id_, access_id, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.access_key_, access_key, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.project_, project, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.table_, table, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.region_, region, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.schema_, schema, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.quota_, quota, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.endpoint_, endpoint, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.tunnel_endpoint_, tunnel_endpoint, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, odps_format_.compression_code_, compression_code, true)) ||
          OB_FAIL(ob_write_string(arena_alloc_, tmp_trace_id, trace_id, true))) {
    LOG_WARN("failed to write string", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("access_id"), access_id, 1))) {
    LOG_WARN("failed to add access id to params", K(ret));
  } else if (OB_FAIL(
                 odps_params_map_.set_refactored(ObString::make_string("access_key"), access_key, 1))) {
    LOG_WARN("failed to add access key to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("project"), project, 1))) {
    LOG_WARN("failed to add project to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("table_name"), table, 1))) {
    LOG_WARN("failed to add table name to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("start_offset"), ObString::make_string("0"), 1))) {
    LOG_WARN("failed to add start offset to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("split_size"), ObString::make_string("256"), 1))) {
    LOG_WARN("failed to add split size to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("region"), region, 1))) {
    LOG_WARN("failed to add region to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("public_access"), ObString::make_string("true"), 1))) {
    LOG_WARN("failed to add public access to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("use_epoch_offset"), ObString::make_string("true"), 1))) {
    // Note: set this param to make java side return the date/datetime format which use
    // epoch interval day/microseconds.
    LOG_WARN("failed to add use_epoch_day to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("schema"), schema, 1))) {
    LOG_WARN("failed to add schema to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("quota"), quota, 1))) {
    LOG_WARN("failed to add quota to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("odps_url"), endpoint, 1))) {
    LOG_WARN("failed to add endpoint to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("tunnel_url"), tunnel_endpoint, 1))) {
    LOG_WARN("failed to add tunnel endpoint to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                 ObString::make_string("compress_option"), compression_code, 1))) {
    LOG_WARN("failed to add compress code to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                ObString::make_string("trace_id"), trace_id, 1))) {
    LOG_WARN("failed to add trace id", K(ret));
  } else { /* do nothing */
    if (odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
      if (OB_FAIL(odps_params_map_.set_refactored(
              ObString::make_string("api_mode"), ObString::make_string("storage_api"), 1))) {
        // Note: current only support tunnel api.
        LOG_WARN("failed to add api_mode to params", K(ret));
      }
    } else {
      if (OB_FAIL(
              odps_params_map_.set_refactored(ObString::make_string("api_mode"), ObString::make_string("tunnel_api"), 1))) {
        // Note: current only support tunnel api.
        LOG_WARN("failed to add api_mode to params", K(ret));
      }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_storage_api_meta_param(
    const ExprFixedArray &ext_file_column_expr, const ObString &part_list_str, int64_t parallel)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt fs_block_size(parallel);
  ObString str = ObString::make_string(fs_block_size.str());
  ObString size_str;
  if (!odps_params_map_.created() && OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE, "JNITableParams"))) {
    LOG_WARN("failed to create odps params map", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_alloc_, str, size_str, true))) {
    LOG_WARN("failed to write c_str style for split size", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("parallel"), size_str))) {
    LOG_WARN("failed to init split block size", K(ret));
  } else if (OB_FAIL(pull_and_prepare_column_exprs(ext_file_column_expr))) {
    LOG_WARN("failed to pull and prepare column expr", K(ret));
  } else if (OB_FAIL(init_all_columns_name_as_odps_params())) {
    LOG_WARN("failed to init expected columns and related types", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("partition_spec"), part_list_str))) {
  } else { /* do nothing */
  }
  return ret;
}

int ObODPSJNITableRowIterator::pull_and_prepare_column_exprs(const ExprFixedArray &ext_file_column_expr)
{
  int ret = OB_SUCCESS;
  if (scan_param_ != nullptr && OB_FAIL(array_helpers_.prepare_allocate(ext_file_column_expr.count()))) {
    LOG_WARN("failed to prepare allocate array helper");
  } else if (OB_FAIL(pull_data_columns())) {
    LOG_WARN("failed to pull column info", K(ret));
  } else if (OB_FAIL(prepare_data_expr(ext_file_column_expr))) {
    LOG_WARN("failed to prepare expr", K(ret));
  } else if (OB_FAIL(pull_partition_columns())) {
    LOG_WARN("failed to pull partition columns", K(ret));
  } else if (OB_FAIL(prepare_partition_expr(ext_file_column_expr))) {
    LOG_WARN("failed to prepare expr", K(ret));
  }
  return ret;
}


int ObODPSJNITableRowIterator::prepare_data_expr(const ExprFixedArray &ext_file_column_expr)
{
  int ret = OB_SUCCESS;
  obexpr_odps_nonpart_col_idsmap_.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && i < ext_file_column_expr.count(); ++i) {
    const ObExpr *cur_expr = ext_file_column_expr.at(i);  // do no check is NULL or not
    int64_t target_idx = cur_expr->extra_ - 1;
    ObODPSArrayHelper* array_helper = nullptr;
    if (cur_expr->type_ != T_PSEUDO_EXTERNAL_FILE_COL) {
      // do nothing
    } else if (OB_UNLIKELY(target_idx < 0 || target_idx >= mirror_nonpart_column_list_.count())) {
      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
      LOG_WARN("unexcepted target_idx", K(ret), K(target_idx), K(mirror_nonpart_column_list_.count()));
      LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
          "wrong column index point to odps, please check the index of external$tablecol[index] and "
          "metadata$partition_list_col[index]");
    } else if (OB_FAIL(obexpr_odps_nonpart_col_idsmap_.push_back({i, target_idx}))) {
      LOG_WARN("failed to keep target_idx of external col", K(ret), K(target_idx));
    } else if (OB_FAIL(sorted_column_ids_.push_back(ExternalPair{i, target_idx}))) {
      LOG_WARN("failed to keep sorted_column_ids", K(ret), K(target_idx));
    } else if (scan_param_ != nullptr) {
      if (ObCollectionSQLType == cur_expr->obj_meta_.get_type() &&
          OB_FAIL(ObODPSTableUtils::create_array_helper(scan_param_->op_->get_eval_ctx().exec_ctx_,
                                                        arena_alloc_, *cur_expr, array_helper))) {
        LOG_WARN("failed to init array helper");
      } else if (OB_FAIL(check_type_static(mirror_nonpart_column_list_.at(target_idx), cur_expr, array_helper))) {
        LOG_WARN("odps type map ob type not support", K(ret), K(target_idx));
      } else {
        array_helpers_.at(i) = array_helper;
      }
    }
  }
  lib::ob_sort(sorted_column_ids_.begin(), sorted_column_ids_.end(), ExternalPair::Compare());
  return ret;
}

int ObODPSJNITableRowIterator::prepare_partition_expr(const ExprFixedArray &ext_file_column_expr)
{
  int ret = OB_SUCCESS;
  obexpr_odps_part_col_idsmap_.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && i < ext_file_column_expr.count(); ++i) {
    const ObExpr *cur_expr = ext_file_column_expr.at(i);  // do no check is NULL or not
    int64_t target_idx = cur_expr->extra_ - 1;
    if (cur_expr->type_ != T_PSEUDO_PARTITION_LIST_COL) {
      // do nothing
    } else if (OB_UNLIKELY(target_idx < 0 || target_idx >= mirror_partition_column_list_.count())) {
      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
      LOG_WARN("unexcepted target_idx", K(ret), K(target_idx), K(mirror_partition_column_list_.count()));
      LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
          "wrong column index point to odps, please check the index of external$tablecol[index] and "
          "metadata$partition_list_col[index]");
    } else if (OB_FAIL(obexpr_odps_part_col_idsmap_.push_back({i, target_idx}))) {
      LOG_WARN("failed to keep target_idx of external col", K(ret), K(target_idx));
    } else if (OB_FAIL(sorted_column_ids_.push_back({i, target_idx + mirror_nonpart_column_list_.count()}))) {
      LOG_WARN("failed to keep sorted_column_ids", K(ret), K(target_idx));
    } else if (scan_param_ != nullptr) {
      if (OB_FAIL(check_type_static(mirror_partition_column_list_.at(target_idx), cur_expr, nullptr))) {
        LOG_WARN("odps type map ob type not support", K(ret), K(target_idx));
      } else {
        array_helpers_.at(i) = nullptr;
      }
    }
  }
  lib::ob_sort(sorted_column_ids_.begin(), sorted_column_ids_.end(), ExternalPair::Compare());
  LOG_DEBUG("sorted column ids", K(sorted_column_ids_));
  return ret;
}

int ObODPSJNITableRowIterator::close_schema_scanner()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
  } else if (OB_FAIL(odps_jni_schema_scanner_->do_close())) {
    LOG_WARN("failed to close odps jni schema scanner", K(ret));
  }
  odps_jni_schema_scanner_.reset();
  return ret;
}


int ObODPSJNITableRowIterator::pull_data_columns()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    // ObString columns;
    ObSEArray<ObString, 8> mirror_columns;

    if (OB_FAIL(odps_jni_schema_scanner_->get_odps_mirror_data_columns(
            arena_alloc_, mirror_columns, ObString::make_string("getMirrorDataColumns")))) {
      LOG_WARN("failed to get mirror columns", K(ret));
    } else if (OB_FAIL(extract_mirror_odps_columns(mirror_columns, mirror_nonpart_column_list_))) {
      LOG_WARN("failed to extract mirror columns", K(ret), K(mirror_columns));
    } else { /* do nothing */
    }
  }
  return ret;
}
int ObODPSJNITableRowIterator::pull_partition_columns()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    // ObString columns;
    ObSEArray<ObString, 8> mirror_columns;

    if (OB_FAIL(odps_jni_schema_scanner_->get_odps_mirror_data_columns(
            arena_alloc_, mirror_columns, ObString::make_string("getMirrorPartitionColumns")))) {
      LOG_WARN("failed to get mirror columns", K(ret));
    } else if (OB_FAIL(extract_mirror_odps_columns(mirror_columns, mirror_partition_column_list_))) {
      LOG_WARN("failed to extract mirror columns", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}
int ObODPSJNITableRowIterator::extract_mirror_odps_columns(ObSEArray<ObString, 8> &mirror_columns,
                                                           ObSEArray<MirrorOdpsJniColumn, 8> &mirror_target_columns)
{
  int ret = OB_SUCCESS;
  int count = mirror_columns.count();
  for (int idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
    ObString mirror_column_string = mirror_columns.at(idx);
    if (OB_FAIL(extract_mirror_odps_column(mirror_column_string, mirror_target_columns))) {
      LOG_WARN("failed to extract mirror odps column");
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::extract_mirror_odps_column(ObString &mirror_column_string,
                                                          ObIArray<MirrorOdpsJniColumn> &mirror_columns) {
  int ret = OB_SUCCESS;
  ObString name = mirror_column_string.split_on('#');
  ObString type = mirror_column_string.split_on('#');
  OdpsType odps_type = ObJniConnector::get_odps_type_by_string(type);
  int type_size = 0;
  ObString type_expr;
  if (odps_type == OdpsType::CHAR || odps_type == OdpsType::VARCHAR) {
    // if type is `CHAR` or `VARCHAR`, then it will be with `length`
    int length = 0;
    if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, length))) {
      LOG_WARN("failed to get length", K(mirror_column_string));
    } else if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, type_size))) {
      LOG_WARN("failed to get type size", K(mirror_column_string));
    } else if (OB_FAIL(get_type_expr_from_mirror_column_string(mirror_column_string, type_expr))) {
      LOG_WARN("failed to get type expr", K(mirror_column_string));
    } else if (OB_FAIL(mirror_columns.push_back(MirrorOdpsJniColumn(name, odps_type, length,
                                                                    type_size, type_expr)))) {
      LOG_WARN("failed to push back mirror column");
    }
  } else if (odps_type == OdpsType::DECIMAL) {
    int precision = 0;
    int scale = 0;
    if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, precision))) {
      LOG_WARN("failed to get precision", K(mirror_column_string));
    } else if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, scale))) {
      LOG_WARN("failed to get scale", K(mirror_column_string));
    } else if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, type_size))) {
      LOG_WARN("failed to get type size", K(mirror_column_string));
    } else if (OB_FAIL(get_type_expr_from_mirror_column_string(mirror_column_string, type_expr))) {
      LOG_WARN("failed to get type expr", K(mirror_column_string));
    } else if (OB_FAIL(mirror_columns.push_back(MirrorOdpsJniColumn(name, odps_type, precision,
                                                                    scale, type_size, type_expr)))) {
      LOG_WARN("failed to push back mirror column");
    }
  } else if (odps_type == OdpsType::ARRAY) {
    // column_name#ARRAY#element#INT#4#INT#-1#ARRAY<INT>
    MirrorOdpsJniColumn array_column(name, odps_type);
    if (OB_FAIL(SMART_CALL(extract_mirror_odps_column(mirror_column_string, array_column.child_columns_)))) {
      LOG_WARN("failed to extract mirror odps column");
    } else if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, array_column.type_size_))) {
      LOG_WARN("failed to get type size", K(mirror_column_string));
    } else if (OB_FAIL(get_type_expr_from_mirror_column_string(mirror_column_string, array_column.type_expr_))) {
      LOG_WARN("failed to get type expr", K(mirror_column_string));
    } else if (OB_FAIL(mirror_columns.push_back(array_column))) {
      LOG_WARN("failed to push back mirror column");
    }
  } else if (odps_type == OdpsType::STRUCT) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported struct type now", K(ret));
  } else if (odps_type == OdpsType::MAP) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported map type now", K(ret));
  } else { // simple premitive types
    if (OB_FAIL(get_int_from_mirror_column_string(mirror_column_string, type_size))) {
      LOG_WARN("failed to get type size", K(mirror_column_string));
    } else if (OB_FAIL(get_type_expr_from_mirror_column_string(mirror_column_string, type_expr))) {
      LOG_WARN("failed to get type expr", K(mirror_column_string));
    } else if (OB_FAIL(mirror_columns.push_back(MirrorOdpsJniColumn(name, odps_type, type_size, type_expr)))) {
      LOG_WARN("failed to push back mirror column");
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_int_from_mirror_column_string(ObString &mirror_column_string,
                                                                int &int_val)
{
  int ret = OB_SUCCESS;
  ObString int_str = mirror_column_string.split_on('#');
  int err = 0;
  if (OB_UNLIKELY(!int_str.is_numeric())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid int value string", K(int_str));
  } else {
    int_val = ObCharset::strntoll(int_str.ptr(), int_str.length(), 10, &err);
    if (OB_UNLIKELY(0 != err)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid int value", K(err), K(int_str));
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_type_expr_from_mirror_column_string(ObString &mirror_column_string,
                                                                      ObString &type_expr)
{
  int ret = OB_SUCCESS;
  type_expr = mirror_column_string.split_on('#');
  if (type_expr.empty()) {
    type_expr = mirror_column_string;
  }
  if (OB_UNLIKELY(type_expr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type expr", K(type_expr));
  }
  return ret;
}

int ObODPSJNITableRowIterator::prepare_bit_vector()
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  void *vec_mem = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 > eval_ctx.max_batch_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max_batch_size_", K(ret), K(eval_ctx.max_batch_size_));
  } else {
    if (0 == eval_ctx.max_batch_size_) {
      // do nothing
    } else if (OB_ISNULL(vec_mem = malloc_alloc_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(vec_mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::check_type_static(MirrorOdpsJniColumn &odps_column,
                                                 const ObExpr *ob_type_expr,
                                                 ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_type_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    const ObObjType ob_type = ob_type_expr->obj_meta_.get_type();
    const int32_t ob_type_length = ob_type_expr->max_length_;
    const int32_t ob_type_precision = ob_type_expr->datum_meta_.precision_;
    const int32_t ob_type_scale = ob_type_expr->datum_meta_.scale_;
    if (OB_FAIL(check_type_static(odps_column, ob_type, ob_type_length,
                                  ob_type_precision, ob_type_scale, array_helper))) {
      LOG_WARN("failed to check type static");
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::check_type_static(MirrorOdpsJniColumn &odps_column,
                                                 const ObObjType ob_type,
                                                 const int32_t ob_type_length,
                                                 const int32_t ob_type_precision,
                                                 const int32_t ob_type_scale,
                                                 ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  const OdpsType odps_type = odps_column.type_;
  const int32_t odps_type_length = odps_column.length_;
  const int32_t odps_type_precision = odps_column.precision_;
  const int32_t odps_type_scale = odps_column.scale_;
  if (odps_type == OdpsType::TINYINT || odps_type == OdpsType::BOOLEAN) {
    if (ObTinyIntType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::SMALLINT) {
    if (ObSmallIntType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::INT) {
    if (ObInt32Type == ob_type) {
      is_match = true;
    } else if (is_oracle_mode() && ObNumberType == ob_type) {
      is_match = true;
    } else if (is_oracle_mode() && ObDecimalIntType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::BIGINT) {
    if (ObIntType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::FLOAT) {
    if (ObFloatType == ob_type && ob_type_scale == DEFAULT_FLOAT_SCALE) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::DOUBLE) {
    if (ObDoubleType == ob_type && ob_type_scale == DEFAULT_DOUBLE_SCALE) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::DECIMAL) {
    if (ObDecimalIntType == ob_type || ObNumberType == ob_type) {
      if (ob_type_precision == odps_type_precision &&
          ob_type_scale == odps_type_scale) {
        is_match = true;
      }
    }
  } else if (odps_type == OdpsType::CHAR) {
    if (ObCharType == ob_type && ob_type_length == odps_type_length) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::VARCHAR) {
    if (ObVarcharType == ob_type && ob_type_length == odps_type_length) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::STRING || odps_type == OdpsType::BINARY) {
    if (ObVarcharType == ob_type || ObTinyTextType == ob_type || ObTextType == ob_type ||
        ObLongTextType == ob_type || ObMediumTextType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::JSON) {
    if (ObJsonType == ob_type) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::TIMESTAMP_NTZ) {
    if (ob_is_datetime_or_mysql_datetime(ob_type) && ob_type_scale >= 6) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::TIMESTAMP) {
    if (ObTimestampType == ob_type && ob_type_scale >= 6) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::DATETIME) {
    if (ob_is_datetime_or_mysql_datetime(ob_type) && ob_type_scale >= 3) {
      is_match = true;
    }
  } else if (odps_type == OdpsType::DATE) {
    if (ob_is_date_or_mysql_date(ob_type)) {
      is_match = true;
    }
  } else if (odps_column.type_ == OdpsType::ARRAY) {
    if (ObCollectionSQLType == ob_type) {
      if (OB_ISNULL(array_helper) || OB_UNLIKELY(odps_column.child_columns_.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected argument", KP(array_helper), K(odps_column.child_columns_));
      } else if (OB_FAIL(check_type_static(odps_column.child_columns_.at(0),
                                                array_helper->element_type_,
                                                array_helper->element_length_,
                                                array_helper->element_precision_,
                                                array_helper->element_scale_,
                                                array_helper->child_helper_))) {
        LOG_WARN("failed to chekc type static");
      } else {
        is_match = true;
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported odps type", K(ret), K(odps_type), K(ob_type));
  }
  if (OB_SUCC(ret) && !is_match) {
    ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
    LOG_WARN("invalid odps type map to ob type", K(odps_type), K(odps_type_length), K(odps_type_precision),
                K(odps_type_scale), K(ob_type), K(ob_type_length), K(ob_type_precision), K(ob_type_scale));
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_empty_require_column() {
  int ret = OB_SUCCESS;
  ObString fields = ObString::make_string("");
  ObString types = ObString::make_string("");
  ObString temp_required_fields = ObString::make_string("required_fields");
  ObString temp_columns_types = ObString::make_string("columns_types");
  if (!odps_params_map_.created() && OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE, "JNITableParams"))) {
    LOG_WARN("failed to create odps params map", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(temp_required_fields, fields, 1))) {
    LOG_WARN("failed to add required fields to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(temp_columns_types, types, 1))) {
    LOG_WARN("failed to add column types to params", K(ret));
  } else { /* do nothing */
  }
  return ret;
}


int ObODPSJNITableRowIterator::init_part_spec(const ObString& part_spec) {
  int ret = OB_SUCCESS;
  ObString temp_partition_spec = ObString::make_string("partition_spec");
  ObString partition_spec;

  if (!odps_params_map_.created() && OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE, "JNITableParams"))) {
    LOG_WARN("failed to create odps params map", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_alloc_, part_spec, partition_spec, true))) {
      LOG_WARN("failed to write c_str style for partition spec", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(temp_partition_spec, partition_spec, 1))) {
    LOG_WARN("failed to add required fields to params", K(ret));
  } else { /* do nothing */
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_all_columns_name_as_odps_params()
{
  int ret = OB_SUCCESS;
  if (!odps_params_map_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params", K(ret));
  } else if (inited_columns_and_types_) {
    // do nothing
  } else {
    if (0 == obexpr_odps_nonpart_col_idsmap_.count()) {
      is_empty_external_file_exprs_ = true;
      // If external_file_exprs is empty, but total_column_ids_ is not,
      // means that the query is only partition column(s).
    }

    /**
     * tunnel 不需要分区列，但是性能差，因为每个分区都要开session
     * storage 需要把分区列，透给java
     */
    ObSqlString required_fields;
    ObSqlString field_types;


    int64_t column_count = obexpr_odps_nonpart_col_idsmap_.count();
    int64_t pc_count = obexpr_odps_part_col_idsmap_.count();
    for (int i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      int32_t target_column_id = obexpr_odps_nonpart_col_idsmap_.at(i).odps_col_idx_;
      MirrorOdpsJniColumn column = mirror_nonpart_column_list_.at(target_column_id);
      ObString temp_column_name(column.name_.length(), column.name_.ptr());
      ObString temp_type_expr(column.type_expr_.length(), column.type_expr_.ptr());

      ObString column_name;
      ObString type_expr;
      // Note: should transfer obstring to be c_str style
      if (OB_FAIL(ob_write_string(arena_alloc_, temp_column_name, column_name, true))) {
        LOG_WARN("failed to write column name", K(ret), K(column.name_));
      } else if (OB_FAIL(ob_write_string(arena_alloc_, temp_type_expr, type_expr, true))) {
        LOG_WARN("failed to write type expr", K(ret), K(column.type_expr_));
      } else if (OB_FAIL(required_fields.append(column_name))) {
        LOG_WARN("failed to append required columns", K(ret), K(column_name), K(target_column_id));
      } else if (column_count > 1 && i != column_count - 1 && OB_FAIL(required_fields.append(","))) {
        LOG_WARN("failed to append comma for column name splittor", K(ret));
      } else if (OB_FAIL(field_types.append(type_expr))) {
        LOG_WARN("failed to append field type", K(ret), K(target_column_id), K(type_expr));
      } else if (column_count > 1 && i != column_count - 1 && OB_FAIL(field_types.append("#"))) {
        LOG_WARN("failed to append '#' for type expr splittor", K(ret));
      } else {
        /* do nothing */
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(required_fields.append(","))) {
      LOG_WARN("failed to append comma for column name splittor", K(ret));
    } else if (OB_FAIL(field_types.append("#"))) {
      LOG_WARN("failed to append '#' for type expr splittor", K(ret));
    }

    for (int i = 0; OB_SUCC(ret) && i < pc_count; ++i) {
      int32_t target_column_id = obexpr_odps_part_col_idsmap_.at(i).odps_col_idx_;
      MirrorOdpsJniColumn column = mirror_partition_column_list_.at(target_column_id);
      ObString temp_column_name(column.name_.length(), column.name_.ptr());
      ObString temp_type_expr(column.type_expr_.length(), column.type_expr_.ptr());

      ObString column_name;
      ObString type_expr;
      // Note: should transfer obstring to be c_str style
      if (OB_FAIL(ob_write_string(arena_alloc_, temp_column_name, column_name, true))) {
        LOG_WARN("failed to write column name", K(ret), K(column.name_));
      } else if (OB_FAIL(ob_write_string(arena_alloc_, temp_type_expr, type_expr, true))) {
        LOG_WARN("failed to write type expr", K(ret), K(column.type_expr_));
      } else if (OB_FAIL(required_fields.append(column_name))) {
        LOG_WARN("failed to append required columns", K(ret), K(column_name), K(target_column_id));
      } else if (pc_count > 1 && i != pc_count - 1 && OB_FAIL(required_fields.append(","))) {
        LOG_WARN("failed to append comma for column name splittor", K(ret));
      } else if (OB_FAIL(field_types.append(type_expr))) {
        LOG_WARN("failed to append field type", K(ret), K(target_column_id), K(type_expr));
      } else if (pc_count > 1 && i != pc_count - 1 && OB_FAIL(field_types.append("#"))) {
        LOG_WARN("failed to append '#' for type expr splittor", K(ret));
      } else {
        /* do nothing */
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (odps_format_.api_mode_ == ObODPSGeneralFormat::ApiMode::TUNNEL_API && (is_empty_external_file_exprs_)) {
      // Empty external file exprs does not need to init any fields and columns.
      // do nothing
    } else {
      ObString fields;
      ObString types;
      ObString temp_required_fields = ObString::make_string("required_fields");
      ObString temp_columns_types = ObString::make_string("columns_types");
      if (OB_FAIL(ob_write_string(arena_alloc_, required_fields.string(), fields, true))) {
        LOG_WARN("failed to write required fields", K(ret));
      } else if (OB_FAIL(ob_write_string(arena_alloc_, field_types.string(), types, true))) {
        LOG_WARN("failed to write column types", K(ret));
      } else if (OB_FAIL(odps_params_map_.set_refactored(temp_required_fields, fields, 1))) {
        LOG_WARN("failed to add required fields to params", K(ret));
      } else if (OB_FAIL(odps_params_map_.set_refactored(temp_columns_types, types, 1))) {
        LOG_WARN("failed to add column types to params", K(ret));
      } else { /* do nothing */
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_columns_and_types_ = true;
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
    if (OB_FAIL(get_next_rows_storage_api(count, capacity))) {
      LOG_WARN("failed to get storage next rows");
    }
  } else {
    if (OB_FAIL(get_next_rows_tunnel(count, capacity))) {
      LOG_WARN("failed to get next rows tunnel", K(count));
    }
  }
  read_rows_ += count;
  return ret;
}


int ObODPSJNITableRowIterator::next_task_storage_row_without_data_getter(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  task_alloc_.reset();
  int64_t task_idx = state_.task_idx_;
  ++task_idx;
  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("odps table iter end", K(ret), K(state_), K(task_idx), K_(read_rounds));
  } else {
    // do nothing
    const ObString &part_spec = scan_param_->key_ranges_.at(task_idx)
                                    .get_start_key()
                                    .get_obj_ptr()[ObExternalTableUtils::FILE_URL]
                                    .get_string();
    int64_t start = 0;
    int64_t step = 0;
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                    ObExternalTableUtils::LINE_NUMBER,
                                                    start,
                                                    step))) {
      LOG_WARN("failed to resolve odps start step", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
      ret = OB_ITER_END;
      LOG_INFO("iterator of odps jni scanner is end with dummy file", K(ret));
    } else {
      state_.count_ = 0;
      state_.start_ = start;
      state_.step_ = step;
      state_.task_idx_ = task_idx;
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_next_rows_storage_api(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard guard(mem_attr_);
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);

  column_exprs_alloc_.clear();
  int64_t should_read_rows = 0;
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  if (is_empty_external_file_exprs_
     && 0 == mirror_partition_column_list_.count()
     && odps_format_.api_mode_  == ObODPSGeneralFormat::ApiMode::ROW) {
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task_storage_row_without_data_getter(capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      read_rounds_ += 1;
      // Means that may input the maybe count(1)/sum(1)/max(1)/min(1) and the `1`
      // is not a real column.
      should_read_rows = std::min(capacity, state_.step_ - state_.count_);
      state_.count_ += should_read_rows;
      if (0 == should_read_rows) {
        if (INT64_MAX == state_.step_ || state_.count_ == state_.step_) {
          state_.step_ = state_.count_;  // go to next task
          count = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(should_read_rows), K(state_), K(ret));
        }
      }
    }
  } else {
    bool eof = false;
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task_storage(capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      int64_t returned_row_cnt = 0;
      int64_t read_rows = 0;
      bool eof = false;
      if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
        if (OB_FAIL(state_.odps_jni_scanner_->do_get_next_split_by_ob(&read_rows, &eof, capacity))) {
          // It should close remote scanner whatever get next is iter end or
          // other failures.
          LOG_WARN("failed to get next rows by jni scanner", K(ret), K(eof), K(read_rows));
        }
      } else {
        if (OB_FAIL(state_.odps_jni_scanner_->do_get_next_split_by_odps(&read_rows, &eof, capacity))) {
          // It should close remote scanner whatever get next is iter end or
          // other failures.
          LOG_WARN("failed to get next rows by jni scanner", K(ret), K(eof), K(read_rows));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (0 == read_rows) {
        if (eof) {
          state_.step_ = state_.count_;
          count = 0;
        } else {
          count = 0;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(returned_row_cnt), K(state_), K(ret));
        }
      } else if (!eof) {
        state_.step_ = 1;
        state_.count_ = 0;
      } else {
        state_.step_ = 0;
        state_.count_ = 0;
      }

      if (OB_FAIL(ret)) {
      } else if (eof) {
        // eof 分支中可能也有空元结构体生成
        if (OB_FAIL(ret)) {
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_table(should_read_rows))) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_ODPS_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_slice())) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        }
      } else {
        should_read_rows = std::min(read_rows, capacity);
        read_rounds_ += 1;
        // fill ob expr with odps data
        // if transfer mode is off heap table, then we need to fill column exprs
        // whatever, we should release table
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fill_column_exprs_storage(file_column_exprs, ctx, should_read_rows))) {
          // do not cover the failure code
          (void)state_.odps_jni_scanner_->release_table(should_read_rows);
          LOG_WARN("failed to fill columns", K(ret), K(should_read_rows));
        }

        if (OB_FAIL(ret)) {
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_table(should_read_rows))) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_ODPS_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_slice())) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == should_read_rows) {
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
      file_column_exprs.at(i)->set_evaluated_flag(ctx);
    }
    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
      if (OB_FAIL(column_convert_expr->eval_batch(ctx, *bit_vector_cache_, should_read_rows))) {
        LOG_WARN("failed to eval batch",
            K(ret),
            K(i),
            K(should_read_rows),
            K(odps_format_.table_),
            K(obexpr_odps_nonpart_col_idsmap_),
            K(obexpr_odps_part_col_idsmap_));
      }
      if (OB_SUCC(ret)) {
        MEMCPY(column_expr->locate_batch_datums(ctx),
            column_convert_expr->locate_batch_datums(ctx),
            sizeof(ObDatum) * should_read_rows);
        column_expr->set_evaluated_flag(ctx);
      }
      OZ(column_expr->init_vector(ctx, VEC_UNIFORM, should_read_rows));
    }
    if (OB_SUCC(ret)) {
      count = should_read_rows;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_exprs_for_rowid(count))) {
    LOG_WARN("failed to calc exprs for rowid", K(ret), K(count));
  }
  if (OB_UNLIKELY(OB_FAIL(ret) && OB_ITER_END != ret)) {
    // END ABNORMALLY CLEAN TASK IN THIS ROUND
    if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
      (void)state_.odps_jni_scanner_->do_close();
      state_.odps_jni_scanner_.reset();
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::next_task_storage(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  task_alloc_.reset();
  int64_t task_idx = state_.task_idx_;
  ++task_idx;
  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
      int tmp_ret = state_.odps_jni_scanner_->do_close();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to close the jni scanner for this state", K(ret));
      } else {
        state_.odps_jni_scanner_ = nullptr;
      }
    }  // 覆盖ret正确，后续查询不在做了
  } else {
    int64_t part_id = scan_param_->key_ranges_.at(task_idx)
                          .get_start_key()
                          .get_obj_ptr()[ObExternalTableUtils::PARTITION_ID]
                          .get_int();
    const ObString &part_spec = scan_param_->key_ranges_.at(task_idx)
                                    .get_start_key()
                                    .get_obj_ptr()[ObExternalTableUtils::FILE_URL]
                                    .get_string();
    int64_t start_split =
        scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::SPLIT_IDX].get_int();
    int64_t end_split =
        scan_param_->key_ranges_.at(task_idx).get_end_key().get_obj_ptr()[ObExternalTableUtils::SPLIT_IDX].get_int();
    const ObString &session_id = scan_param_->key_ranges_.at(task_idx)
                                  .get_start_key()
                                  .get_obj_ptr()[ObExternalTableUtils::SESSION_ID]
                                  .get_string();
    int64_t start = 0;
    int64_t step = 0;
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                    ObExternalTableUtils::LINE_NUMBER,
                                                    start,
                                                    step))) {
      LOG_WARN("failed to resolve odps start step", K(ret));
    }

    LOG_TRACE("task storage",
        K(part_id),
        K(part_spec),
        K(start_split),
        K(end_split),
        K(start),
        K(step),
        K(session_id),
        K(task_idx));
    if (OB_FAIL(ret)) {
    } else if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
      ret = OB_ITER_END;
      LOG_TRACE("iterator of odps jni scanner is end with dummy file", K(ret));
      int tmp_ret = OB_SUCCESS;
      if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
        int tmp_ret = state_.odps_jni_scanner_->do_close();
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("failed to close the jni scanner for this state", K(ret));
        } else {
          state_.odps_jni_scanner_ = nullptr;
        }
      }  // 覆盖ret正确，后续查询不在做了
    } else if (OB_FAIL(build_storage_task_state(
                   task_idx, part_id, start_split, end_split, start, step, session_id, capacity))) {
      LOG_WARN("failed to build storage task state ");
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::build_storage_task_state(int64_t task_idx, int64_t part_id, int64_t start_split_idx,
    int64_t end_split_idx, int64_t start, int64_t step, const ObString &session_id, int64_t capacity)
{
  int ret = OB_SUCCESS;
  state_.task_idx_ = task_idx;
  state_.part_id_ = part_id;
  state_.count_ = 0;
  if (OB_NOT_NULL(state_.odps_jni_scanner_) && OB_FAIL(state_.odps_jni_scanner_->do_close())) {
    LOG_WARN("failed to close previous scanner", K(ret));
  } else {
    state_.odps_jni_scanner_ = create_odps_jni_scanner();
    LOG_TRACE("timer build storage reader init and open connection");
    if (OB_ISNULL(state_.odps_jni_scanner_.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret), K(state_.is_from_gi_pump_));
    } else if (OB_FAIL(init_data_storage_reader_params(
                   start_split_idx, end_split_idx, start, step, session_id, capacity))) {
      LOG_WARN("failed to init reader params");
    } else if (state_.odps_jni_scanner_->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("jni scanner should not be inited", K(ret));
    } else if (OB_FAIL(state_.odps_jni_scanner_->do_init(odps_params_map_))) {
      // 注意在init_data_storage_reader_params构造odps_params_map_的函数
      // 这个函数在每次切换task的时候都要插入新的值，所以set factor的时候一定要覆盖
      // 否则obstring是里面是过期的ptr导致use after free的问题
      // 这里注意判断错误码
      // If the jni scanner is total new fresh, then init before open.
      LOG_WARN("failed to init next odps jni scanner", K(ret));
    } else if (OB_FAIL(state_.odps_jni_scanner_->do_open())) {
      LOG_WARN("failed to open next odps jni scanner",
          K(ret),
          K(task_idx),
          K(part_id),
          K(start_split_idx),
          K(end_split_idx),
          K(session_id));
    } else { /* do nothing */
    }
    LOG_TRACE("timer build storage reader end open connection");
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_data_storage_reader_params(int64_t start_split, int64_t end_split,
    int64_t start_rows, int64_t row_count, const ObString &session_str, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (!odps_params_map_.created() || odps_params_map_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params", K(ret));
  } else {
    // update `batch_size`, `start` and `step` to jni scanner
    ObFastFormatInt fssp(start_split);
    ObFastFormatInt fsep(end_split);
    ObFastFormatInt fsc(capacity);
    ObFastFormatInt fsso(start_rows);
    ObFastFormatInt fsrc(row_count);

    ObString start_split_str = ObString::make_string(fssp.str());
    ObString end_split_str = ObString::make_string(fsep.str());
    ObString capacity_str = ObString::make_string(fsc.str());
    ObString start_offset_str = ObString::make_string(fsso.str());
    ObString split_size_str = ObString::make_string(fsrc.str());

    ObString temp_start_offset = ObString::make_string("start_offset");
    ObString temp_split_size = ObString::make_string("split_size");
    ObString temp_start_split = ObString::make_string("start_split_offset");
    ObString temp_end_split = ObString::make_string("end_split_offset");
    ObString temp_session_str = ObString::make_string("serialized_session");
    ObString temp_capacity = ObString::make_string("capacity");
    ObString temp_transfer_mode = ObString::make_string("transfer_mode");
    ObString transfer_option = state_.odps_jni_scanner_->get_transfer_mode_str();

    ObString start_split;
    ObString end_split;
    ObString start_offset;
    ObString split_size;  // 这个命名比较困惑但是实际上代表row count
    ObString capacity_obstr;
    if (OB_FAIL(ob_write_string(task_alloc_, start_split_str, start_split, true))) {
      LOG_WARN("failed to write c_str style for start", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, end_split_str, end_split, true))) {
      LOG_WARN("failed to write c_str style for end", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, start_offset_str, start_offset, true))) {
      LOG_WARN("failed to write c_str style for start offset", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, split_size_str, split_size, true))) {
      LOG_WARN("failed to write c_str style for split size", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, capacity_str, capacity_obstr, true))) {
      LOG_WARN("failed to write c_str style for capacity", K(ret));
    } else if (!start_split.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("configure start offset is not number", K(ret), K(start_split));
    } else if (!end_split.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("configure split size is not number", K(ret), K(end_split));
    } else if (!capacity_obstr.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to fill capcacity", K(capacity_obstr));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_start_split, start_split, 1))) {
      LOG_WARN("failed to add start split ", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_end_split, end_split, 1))) {
      LOG_WARN("failed to add end split ", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_session_str, session_str, 1))) {
      LOG_WARN("failed to add session_str", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_start_offset, start_offset, 1))) {
      LOG_WARN("failed to add start offset", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_split_size, split_size, 1))) {
      LOG_WARN("failed to add split size", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_transfer_mode, transfer_option, 1))) {
      LOG_WARN("failed to add transfer mode for tunnel");
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_capacity, capacity_obstr, 1))) {
      LOG_WARN("failted to set capacity", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_next_rows_tunnel(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard guard(mem_attr_);
  // column_exprs_alloc_ must not clear
  column_exprs_alloc_.clear();
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);

  int64_t should_read_rows = 0;
  if (is_empty_external_file_exprs_) {
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task_tunnel_without_data_getter(capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      read_rounds_ += 1;
      // Means that may input the maybe count(1)/sum(1)/max(1)/min(1) and the `1`
      // is not a real column.
      should_read_rows = std::min(capacity, state_.step_ - state_.count_);
      state_.count_ += should_read_rows;

      if (0 == should_read_rows) {
        if (INT64_MAX == state_.step_ || state_.count_ == state_.step_) {
          state_.step_ = state_.count_;  // go to next task
          count = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(should_read_rows), K(state_), K(ret));
        }
      } else {
        if (obexpr_odps_part_col_idsmap_.count() != 0) {
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(fill_column_exprs_tunnel(file_column_exprs, ctx, should_read_rows))) {
            LOG_WARN("failed to fill column exprs", K(ret), K(count));
          }
        }
      }
    }
  } else {
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task_tunnel(capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      read_rounds_ += 1;
      bool eof = false;
      if (OB_ISNULL(state_.odps_jni_scanner_.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret), K(scan_param_));
      } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
        if (OB_FAIL(state_.odps_jni_scanner_->do_get_next_split_by_ob(&should_read_rows, &eof, capacity))) {
          // It should close remote scanner whatever get next is iter end or
          // other failures.
          LOG_WARN("failed to get next rows by jni scanner", K(ret), K(eof), K(should_read_rows));
        }
      } else {
        if (OB_FAIL(state_.odps_jni_scanner_->do_get_next_split_by_odps(&should_read_rows, &eof, capacity))) {
          // It should close remote scanner whatever get next is iter end or
          // other failures.
          LOG_WARN("failed to get next rows by jni scanner", K(ret), K(eof), K(should_read_rows));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (0 == should_read_rows) {
        if (INT64_MAX == state_.step_ || state_.count_ == state_.step_ || eof) {
          state_.step_ = state_.count_;  // goto get next task, next task do close
          count = 0;
        } else {
          // do nothing
          count = 0;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(should_read_rows), K(state_), K(ret));
        }
      } else {
        state_.count_ += should_read_rows;
        LOG_DEBUG("current read rows", K(ret), K(eof), K(state_));
      }
      if (OB_FAIL(ret)) {
      } else if (eof) {
        if (OB_FAIL(ret)) {
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_table(should_read_rows))) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_ODPS_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_slice())) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        }
        LOG_DEBUG("current read rows", K(ret), K(eof), K(state_), K(should_read_rows));
      } else {
        // fill ob expr with odps data
        // if transfer mode is off heap table, then we need to fill column exprs
        // whatever, we should release table
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fill_column_exprs_tunnel(file_column_exprs, ctx, should_read_rows))) {
          // do not cover the failure code
          (void)state_.odps_jni_scanner_->release_table(should_read_rows);
        }

        if (OB_FAIL(ret)) {
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_OB_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_table(should_read_rows))) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        } else if (state_.odps_jni_scanner_->get_split_mode() == JniScanner::SplitMode::RETURN_ODPS_BATCH) {
          if (OB_FAIL(state_.odps_jni_scanner_->release_slice())) {
            LOG_WARN("failed to release table", K(ret), K(should_read_rows));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {

  } else if (0 == should_read_rows) {
    // if should_read_rows is 0 means that it is unnecessary to do data copy.
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
      file_column_exprs.at(i)->set_evaluated_flag(ctx);
    }
    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      // OFFHEAPTABLEMARK
      // OFF in all situation
      // /*
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_convert_exprs_->at(i);
      if (OB_FAIL(column_convert_expr->eval_batch(ctx, *bit_vector_cache_, should_read_rows))) {
        LOG_WARN("failed to eval batch",
            K(ret),
            K(i),
            K(should_read_rows),
            K(odps_format_.table_),
            K(obexpr_odps_nonpart_col_idsmap_),
            K(obexpr_odps_part_col_idsmap_));
      }
      if (OB_SUCC(ret)) {
        MEMCPY(column_expr->locate_batch_datums(ctx),
            column_convert_expr->locate_batch_datums(ctx),
            sizeof(ObDatum) * should_read_rows);
        column_expr->set_evaluated_flag(ctx);
      }
      OZ(column_expr->init_vector(ctx, VEC_UNIFORM, should_read_rows));
      // */
    }
    if (OB_SUCC(ret)) {
      count = should_read_rows;
    }
  }
  if (OB_FAIL(ret)) {
  } else if(OB_FAIL(calc_exprs_for_rowid(count))) {
    LOG_WARN("failed to calc exprs for rowid", K(ret), K(count));
  }
  if (OB_UNLIKELY(OB_FAIL(ret) && OB_ITER_END != ret)) {
    // END ABNORMALLY CLEAN TASK IN THIS ROUND
    if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
      (void)state_.odps_jni_scanner_->do_close();
      state_.odps_jni_scanner_.reset();
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::next_task_tunnel_without_data_getter(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  task_alloc_.reset();
  int64_t task_idx = state_.task_idx_;
  ++task_idx;
  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("odps table iter end", K(ret), K(state_), K(task_idx), K_(read_rounds));
  } else {
    int64_t part_id = scan_param_->key_ranges_.at(task_idx)
                          .get_start_key()
                          .get_obj_ptr()[ObExternalTableUtils::PARTITION_ID]
                          .get_int();
    const ObString &part_spec = scan_param_->key_ranges_.at(task_idx)
                                    .get_start_key()
                                    .get_obj_ptr()[ObExternalTableUtils::FILE_URL]
                                    .get_string();

    int64_t start = 0;
    int64_t step = 0;
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                    ObExternalTableUtils::LINE_NUMBER,
                                                    start,
                                                    step))) {
      LOG_WARN("failed to resolve odps start step", K(ret));
    } else if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
      ret = OB_ITER_END;
      LOG_INFO(" iterator of odps jni scanner is end with dummy file", K(ret));
    } else {
      int64_t real_time_partition_row_count = 0;
      if (OB_ISNULL(odps_jni_schema_scanner_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("could not init schema scanner which is null", K(ret));
      } else if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_row_count(
                     task_alloc_, part_spec, real_time_partition_row_count))) {
        LOG_WARN("failed to get partition row count", K(ret));
      } else if (OB_UNLIKELY(-1 == real_time_partition_row_count)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("odps get invalid partition", K(ret));
      } else {
        if (start >= real_time_partition_row_count) {
          start = real_time_partition_row_count;
          step = 0;
        } else if (INT64_MAX == step || start + step > real_time_partition_row_count) {
          step = real_time_partition_row_count - start;
        }
      }
      LOG_TRACE("next_task_tunnel_without_data_getter", K(ret), K(part_id), K(part_spec), K(start), K(step));

      if (OB_SUCC(ret)) {
        state_.task_idx_ = task_idx;
        state_.part_id_ = part_id;
        state_.start_ = start;
        state_.step_ = step;
        state_.count_ = 0;
        state_.part_spec_ = part_spec;
        if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
          state_.odps_jni_scanner_.reset();
        }
        state_.odps_jni_scanner_ = create_odps_jni_scanner();
        if (obexpr_odps_part_col_idsmap_.count() != 0) {
          bool is_part_table = scan_param_->table_param_->is_partition_table();
          bool is_external_object = is_external_object_id(scan_param_->table_param_->get_table_id());
          if (obexpr_odps_part_col_idsmap_.count() != 0 && is_external_object) {
            // for partitioned external url table,
            // calculate partition values by calc_file_part_list_value_by_array
            if (is_part_table) {
              if (OB_FAIL(calc_file_part_list_value_by_array(
                      part_id, task_alloc_, scan_param_->partition_infos_, state_.part_list_val_))) {
                LOG_WARN("failed to calc parttion list value for mocked table", K(part_id), K(ret));
              }
            }
          } else {
            // for regular external table, calculate partition values by calc_file_partition_list_value
            if (OB_FAIL(calc_file_partition_list_value(part_id, task_alloc_, state_.part_list_val_))) {
              LOG_WARN("failed to calc parttion list value", K(part_id), K(ret));
            }
          }
        }
        LOG_TRACE("get a new task of odps jni iterator", K(ret), K(state_));
      }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::next_task_tunnel(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  task_alloc_.reset();
  int64_t task_idx = state_.task_idx_;
  ++task_idx;

  if (task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
      int tmp_ret = state_.odps_jni_scanner_->do_close();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to close the jni scanner for this state", K(tmp_ret));
        ret = tmp_ret;  // OB_ITER_END被覆盖，这里发生错误，停止
      }
    }
  } else {
    int64_t part_id = scan_param_->key_ranges_.at(task_idx)
                          .get_start_key()
                          .get_obj_ptr()[ObExternalTableUtils::PARTITION_ID]
                          .get_int();
    const ObString &part_spec = scan_param_->key_ranges_.at(task_idx)
                                    .get_start_key()
                                    .get_obj_ptr()[ObExternalTableUtils::FILE_URL]
                                    .get_string();

    int64_t start = 0;
    int64_t step = 0;
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                    ObExternalTableUtils::LINE_NUMBER,
                                                    start,
                                                    step))) {
      LOG_WARN("failed to resolve odps start step", K(ret));
    }
    LOG_TRACE("next_task_tunnel", K(ret), K(part_id), K(part_spec), K(start), K(step));

    if (OB_FAIL(ret)) {
    } else if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
      ret = OB_ITER_END;
      // END NORMALLY
      if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
        int tmp_ret = state_.odps_jni_scanner_->do_close();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to close the jni scanner for this state", K(tmp_ret));
          ret = tmp_ret;  // OB_ITER_END被覆盖，这里发生错误，停止
        }
      }
      LOG_TRACE("iterator of odps jni scanner is end with dummy file", K(ret));
    } else if (OB_FAIL(build_tunnel_partition_task_state(task_idx, part_id, part_spec, start, step, capacity))) {
      LOG_WARN("failed to build tunnel task");
    }
  }

  return ret;
}

int ObODPSJNITableRowIterator::build_tunnel_partition_task_state(
    int64_t task_idx, int64_t part_id, const ObString &part_spec, int64_t start, int64_t step, int64_t capacity)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();

  int64_t real_time_partition_row_count = 0;
  if (OB_ISNULL(odps_jni_schema_scanner_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("could not init schema scanner which is null", K(ret));
  } else if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_row_count(
                 task_alloc_, part_spec, real_time_partition_row_count))) {
    LOG_WARN("failed to get partition row count", K(ret));
  } else if (OB_UNLIKELY(-1 == real_time_partition_row_count)) {
    ret = OB_ODPS_ERROR;
    LOG_WARN("odps get invalid partition", K(ret));
  } else {
    if (start >= real_time_partition_row_count) {
      start = real_time_partition_row_count;
      step = 0;
    } else if (INT64_MAX == step || start + step > real_time_partition_row_count) {
      step = real_time_partition_row_count - start;
    }
  }
  if (OB_SUCC(ret)) {
    state_.task_idx_ = task_idx;
    state_.part_id_ = part_id;
    state_.start_ = start;
    state_.step_ = step;
    state_.count_ = 0;
    state_.part_spec_ = part_spec;
    if (obexpr_odps_part_col_idsmap_.count() != 0) {
      bool is_part_table = scan_param_->table_param_->is_partition_table();
      bool is_external_object = is_external_object_id(scan_param_->table_param_->get_table_id());
      if (obexpr_odps_part_col_idsmap_.count() != 0 && is_external_object) {
        // for partitioned external url table,
        // calculate partition values by calc_file_part_list_value_by_array
        if (is_part_table) {
          if (OB_FAIL(calc_file_part_list_value_by_array(
                  part_id, task_alloc_, scan_param_->partition_infos_, state_.part_list_val_))) {
            LOG_WARN("failed to calc parttion list value for mocked table", K(part_id), K(ret));
          }
        }
      } else {
        // for regular external table, calculate partition values by calc_file_partition_list_value
        if (OB_FAIL(calc_file_partition_list_value(part_id, task_alloc_, state_.part_list_val_))) {
          LOG_WARN("failed to calc parttion list value", K(part_id), K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {

    } else if (OB_NOT_NULL(state_.odps_jni_scanner_) && OB_FAIL(state_.odps_jni_scanner_->do_close())) {
      // RELEASE LAST TASK CORRECTLY
      LOG_WARN("failed to close the scanner of previous task", K(ret));
    } else {
      ObPxSqcHandler *sqc = ctx.exec_ctx_.get_sqc_handler();  // if sqc is not NULL, odps read is in px plan
      if (OB_ISNULL(sqc) || !sqc->get_sqc_ctx().gi_pump_.is_odps_scanner_mgr_inited()) {
        state_.odps_jni_scanner_ = create_odps_jni_scanner();
        state_.is_from_gi_pump_ = false;
        LOG_TRACE("succ to create jni scanner without GI", K(ret), K(part_id), KP(sqc), K(state_.is_from_gi_pump_));
      } else {
        state_.odps_jni_scanner_ = create_odps_jni_scanner();
        // NOTE: mock as px mode
        state_.is_from_gi_pump_ = true;
      }
      LOG_TRACE("timer build tunnel reader and open connection");
      if (OB_ISNULL(state_.odps_jni_scanner_.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret), K(state_.is_from_gi_pump_));
      } else if (OB_UNLIKELY(ctx.max_batch_size_ < capacity)) {  // exec once only
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("setup batch size for odps jni iterator", K(ret), K(ctx.max_batch_size_), K(capacity));
      } else if (OB_FAIL(init_data_tunnel_reader_params(start, step, part_spec))) {
        LOG_WARN("failed to init data reader params", K(ret), K(start), K(step), K(part_spec));
      } else if (state_.odps_jni_scanner_->is_inited()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("jni scanner should not be inited", K(ret));
      } else if (OB_FAIL(state_.odps_jni_scanner_->do_init(odps_params_map_))) {
        // If the jni scanner is total new fresh, then init before open.
        LOG_WARN("failed to init next odps jni scanner", K(ret));
      } else if (OB_FAIL(state_.odps_jni_scanner_->do_open())) {
        LOG_WARN(
            "failed to open next odps jni scanner", K(ret), K(task_idx), K(part_id), K(start), K(step), K(part_spec));
      } else { /* do nothing */
        state_.odps_jni_scanner_->total_read_rows_ = step;
      }
      LOG_TRACE("timer build tunnel reader and open connection");
    }  // two branch 1 select count(*) 2 select col
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_data_tunnel_reader_params(int64_t start, int64_t step, const ObString &part_spec)
{
  int ret = OB_SUCCESS;
  if (!odps_params_map_.created() || odps_params_map_.empty() || OB_ISNULL(state_.odps_jni_scanner_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params", K(ret));
  } else {
    // update `batch_size`, `start` and `step` to jni scanner
    ObFastFormatInt fsf(start);
    ObFastFormatInt fss(step);

    ObString start_offset_str = ObString::make_string(fsf.str());
    ObString split_size_str = ObString::make_string(fss.str());

    LOG_INFO("display variables of odps jni iterator", K(ret), K(start_offset_str), K(split_size_str));

    ObString temp_partition_spec = ObString::make_string("partition_spec");
    ObString temp_start_offset = ObString::make_string("start_offset");
    ObString temp_split_size = ObString::make_string("split_size");
    ObString temp_transfer_mode = ObString::make_string("transfer_mode");
    ObString transfer_option = state_.odps_jni_scanner_->get_transfer_mode_str();

    ObString partition_spec;
    ObString start_offset;
    ObString split_size;
    if (OB_FAIL(ob_write_string(task_alloc_, part_spec, partition_spec, true))) {
      LOG_WARN("failed to write c_str style for partition spec", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, start_offset_str, start_offset, true))) {
      LOG_WARN("failed to write c_str style for start offset", K(ret));
    } else if (OB_FAIL(ob_write_string(task_alloc_, split_size_str, split_size, true))) {
      LOG_WARN("failed to write c_str style for split size", K(ret));
    } else if (!start_offset.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("configure start offset is not number", K(ret), K(start_offset));
    } else if (!split_size.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("configure split size is not number", K(ret), K(split_size));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_start_offset, start_offset, 1))) {
      LOG_WARN("failed to add start offset", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_split_size, split_size, 1))) {
      LOG_WARN("failed to add start offset", K(ret));
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_transfer_mode, transfer_option, 1))) {
      LOG_WARN("failed to add transfer mode for tunnel");
    } else if (OB_FAIL(odps_params_map_.set_refactored(temp_partition_spec, partition_spec, 1))) {
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_column_exprs_storage(const ExprFixedArray &column_exprs, ObEvalCtx &ctx, int64_t num_rows)
{
  int ret = OB_SUCCESS;
  if (state_.odps_jni_scanner_->get_transfer_mode() == JniScanner::TransferMode::OFF_HEAP_TABLE) {
    for (int64_t i = 0; OB_SUCC(ret) && i < obexpr_odps_nonpart_col_idsmap_.count(); ++i) {
      int64_t column_idx = obexpr_odps_nonpart_col_idsmap_.at(i).ob_col_idx_;
      ObExpr &expr = *column_exprs.at(column_idx);  // do not check null ptr
      if (expr.type_ == T_PSEUDO_EXTERNAL_FILE_COL){
        // Note: column idx is using for ob side column, target index is using for java side
        int64_t target_index = obexpr_odps_nonpart_col_idsmap_.at(i).odps_col_idx_;
        if (target_index < 0 || target_index >= mirror_nonpart_column_list_.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid target index for mirror column", K(ret), K(target_index), K(column_idx), K(mirror_nonpart_column_list_));
        } else {
          MirrorOdpsJniColumn column = mirror_nonpart_column_list_.at(target_index);
          if (OB_FAIL(fill_column_offheaptable(ctx, expr, column, num_rows, column_idx))) {
            LOG_WARN("failed to fill column", K(ret), K(column), K(column_idx), K(target_index));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < obexpr_odps_part_col_idsmap_.count(); ++i) {
      int64_t column_idx = obexpr_odps_part_col_idsmap_.at(i).ob_col_idx_;
      ObExpr &expr = *column_exprs.at(column_idx);  // do not check null ptr
      if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
        int64_t target_index = obexpr_odps_part_col_idsmap_.at(i).odps_col_idx_;
        if (target_index < 0 || target_index >= mirror_partition_column_list_.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid target index for mirror column", K(ret), K(target_index), K(column_idx));
        } else {
          MirrorOdpsJniColumn column = mirror_partition_column_list_.at(target_index);
          if (OB_FAIL(fill_column_offheaptable(ctx, expr, column, num_rows, column_idx))) {
            LOG_WARN("failed to fill column", K(ret), K(column), K(column_idx), K(target_index));
          }
        }
      }
    }
  } else {
    const std::shared_ptr<arrow::RecordBatch> cur_record_batch = state_.odps_jni_scanner_->get_cur_arrow_batch();
    const std::shared_ptr<arrow::Schema> cur_schema = cur_record_batch->schema();
    if (sorted_column_ids_.empty()) {
      if (cur_schema->fields().size() == 1 && obexpr_odps_part_col_idsmap_.empty() && obexpr_odps_nonpart_col_idsmap_.empty()) {
        // do nothing
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column count", K(ret), K(cur_schema->fields().size()));
      }
    } else {
      if (cur_schema->fields().size() != sorted_column_ids_.count()) {
        // do nothing
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column count", K(ret), K(cur_schema->fields().size()), K(sorted_column_ids_.count()));
      } else {
        for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < cur_schema->fields().size(); ++column_idx) {
          if (column_idx >= sorted_column_ids_.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column idx", K(ret), K(column_idx), K(sorted_column_ids_.count()));
          } else {
            int64_t expr_idx = sorted_column_ids_.at(column_idx).ob_col_idx_;
            int64_t target_index = sorted_column_ids_.at(column_idx).odps_col_idx_;
            if (expr_idx < 0 || expr_idx >= column_exprs.count()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid target index for mirror column", K(ret), K(expr_idx), K(column_idx));
            } else if (target_index < mirror_nonpart_column_list_.count()) {
              ObExpr &expr = *column_exprs.at(expr_idx);  // do not check null ptr

              const std::shared_ptr<arrow::Field> field = cur_schema->field(column_idx);
              const std::shared_ptr<arrow::Array> array = cur_record_batch->column(column_idx);
              MirrorOdpsJniColumn column = mirror_nonpart_column_list_.at(target_index);
              if (OB_FAIL(fill_column_arrow(ctx, expr, array, field, column, num_rows, expr_idx))) {
                LOG_WARN("failed to fill column", K(ret));
              }
            } else {
              ObExpr &expr = *column_exprs.at(expr_idx);  // do not check null ptr
              target_index -= mirror_nonpart_column_list_.count();
              if (target_index < 0 || target_index >= mirror_partition_column_list_.count()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid target index for mirror column", K(ret), K(target_index), K(column_idx));
              } else {
                const std::shared_ptr<arrow::Field> field = cur_schema->field(column_idx);
                const std::shared_ptr<arrow::Array> array = cur_record_batch->column(column_idx);
                MirrorOdpsJniColumn column = mirror_partition_column_list_.at(target_index);
                if (OB_FAIL(fill_column_arrow(ctx, expr, array, field, column, num_rows, expr_idx))) {
                  LOG_WARN("failed to fill column", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
int ObODPSJNITableRowIterator::fill_column_exprs_tunnel(const ExprFixedArray &column_exprs, ObEvalCtx &ctx, int64_t num_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(state_.odps_jni_scanner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid odps jni scanner", K(ret));
  } else if (state_.odps_jni_scanner_->get_transfer_mode() == JniScanner::TransferMode::OFF_HEAP_TABLE) {
    for (int64_t i = 0; OB_SUCC(ret) && i < obexpr_odps_nonpart_col_idsmap_.count(); ++i) {
      int64_t column_idx = obexpr_odps_nonpart_col_idsmap_.at(i).ob_col_idx_;
      ObExpr &expr = *column_exprs.at(column_idx);  // do not check null ptr
      if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
        // do nothing, parititon columns will handle in alone.
      } else {
        // Note: column idx is using for ob side column, target index is using for java side
        int64_t target_index = obexpr_odps_nonpart_col_idsmap_.at(i).odps_col_idx_;
        // NOTE: the column data will only get queried column, so java side will only init
        // quired column and release column should also release queried fields.
        // And the queried fields index will start from 0 to queried column length.
        if (target_index < 0 || target_index >= mirror_nonpart_column_list_.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid target index for mirror column", K(ret), K(target_index), K(column_idx));
        } else {
          MirrorOdpsJniColumn column = mirror_nonpart_column_list_.at(target_index);
          if (OB_FAIL(fill_column_offheaptable(ctx, expr, column, num_rows, column_idx))) {
            LOG_WARN("failed to fill column", K(ret), K(column), K(column_idx), K(target_index));
          }
        }
      }
    }
  } else {
    if (obexpr_odps_nonpart_col_idsmap_.count() == 0) {
      // do nothing
    } else {
      const std::shared_ptr<arrow::RecordBatch> cur_record_batch = state_.odps_jni_scanner_->get_cur_arrow_batch();
      const std::shared_ptr<arrow::Schema> cur_schema = cur_record_batch->schema();
      for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < cur_schema->fields().size(); ++column_idx) {
        if (column_idx >= sorted_column_ids_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column idx", K(ret), K(column_idx), K(sorted_column_ids_.count()));
        } else {
          int64_t expr_idx = sorted_column_ids_.at(column_idx).ob_col_idx_;
          int64_t target_index = sorted_column_ids_.at(column_idx).odps_col_idx_;
          if (expr_idx < 0 || expr_idx >= column_exprs.count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid target index for mirror column", K(ret), K(expr_idx), K(column_idx));
          } else {
            ObExpr &expr = *column_exprs.at(expr_idx);  // do not check null ptr
            if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL || target_index < 0 || target_index >= mirror_nonpart_column_list_.count()) {
              // do nothing, parititon columns will handle in alone.
            } else {
              const std::shared_ptr<arrow::Field> field = cur_schema->field(column_idx);
              const std::shared_ptr<arrow::Array> array = cur_record_batch->column(column_idx);
              MirrorOdpsJniColumn column = mirror_nonpart_column_list_.at(target_index);
              if (OB_FAIL(fill_column_arrow(ctx, expr, array, field, column, num_rows, expr_idx))) {
                LOG_WARN("failed to fill column", K(ret));
              }
            }
          }
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < obexpr_odps_part_col_idsmap_.count(); ++i) {
    int column_idx = obexpr_odps_part_col_idsmap_.at(i).ob_col_idx_;
    ObExpr &expr = *column_exprs.at(column_idx);
    if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
      ObDatum *datums = expr.locate_batch_datums(ctx);
      if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, num_rows))) {
        LOG_WARN("failed to init expr vector", K(ret), K(expr));
      }
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
        int64_t loc_idx = column_exprs.at(column_idx)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || loc_idx >= state_.part_list_val_.get_count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcepted loc_idx",
              K(ret),
              K(loc_idx),
              K(state_.part_list_val_.get_count()),
              KP(&state_.part_list_val_));
        } else if (state_.part_list_val_.get_cell(loc_idx).is_null()) {
          datums[row_idx].set_null();
        } else {
          CK(OB_NOT_NULL(datums[row_idx].ptr_));
          OZ(datums[row_idx].from_obj(state_.part_list_val_.get_cell(loc_idx)));
        }
      }
    }
  }

  return ret;
}

int ObODPSJNITableRowIterator::calc_exprs_for_rowid(const int64_t read_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_NOT_NULL(file_id_expr_)) {
    OZ(file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_id_expr_->get_vector(eval_ctx));
      // file_id_ is mapping to state_.part_id_ in odps.
      vec->set_int(i, state_.part_id_);
    }
    file_id_expr_->set_evaluated_flag(eval_ctx);
  }
  if (OB_NOT_NULL(line_number_expr_)) {
    OZ(line_number_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(line_number_expr_->get_vector(eval_ctx));
      vec->set_int(i, state_.cur_line_number_ + i);
    }
    line_number_expr_->set_evaluated_flag(eval_ctx);
  }
  state_.cur_line_number_ += read_count;
  return ret;
}

int ObODPSJNITableRowIterator::pull_partition_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    ObSEArray<ObString, 8> partition_specs;
    if (odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
      if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_phy_specs(arena_alloc_, partition_specs))) {
        LOG_WARN("failed to get partition specs", K(ret));
      }
    } else {
      if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_specs(arena_alloc_, partition_specs))) {
        LOG_WARN("failed to get partition specs", K(ret));
      }
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(extract_odps_partition_specs(partition_specs))) {
      LOG_WARN("failed to extract partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::extract_odps_partition_specs(ObSEArray<ObString, 8> &partition_specs)
{
  int ret = OB_SUCCESS;
  int count = partition_specs.count();
  // __NaN__ means non-partition name
  if (1 == count && partition_specs.at(0).prefix_match(NON_PARTITION_FLAG)) {
    is_part_table_ = false;
  } else {
    is_part_table_ = true;
  }
  // Although non-partition also has one partition_spec `__NaN__#${record count}`
  for (int idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
    ObString partition_spec = partition_specs.at(idx);
    ObString spec = partition_spec.split_on('#');
    ObString temp_record = partition_spec;
    if (!temp_record.is_numeric()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid recored num", K(ret));
    } else {
      int err = 0;
      int record_num = ObCharset::strntoll(temp_record.ptr(), temp_record.length(), 10, &err);
      if (err != 0) {
        ret = OB_ERR_DATA_TRUNCATED;
        LOG_WARN("invalid record num", K(ret), K(err));
      } else {
        partition_specs_.push_back(OdpsJNIPartition(spec, record_num));
      }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_file_total_row_count(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_file_total_row_count(count))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_file_total_size(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_file_total_size(count))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_split_count(int64_t &count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_split_count(count))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_session_id(ObIAllocator &alloc, ObString &sid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_session_id(alloc, sid))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_serilize_session(ObIAllocator &alloc, ObString &sstr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_serilize_session(alloc, sstr))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

void ObODPSJNITableRowIterator::reset()
{
  LOG_INFO("show read rows", K(read_rows_));
  state_.reuse();  // reset state_ to initial values for rescan
  // Sometimes state_.odps_jni_scanner_ maybe not inited correctly
  task_alloc_.clear();
  column_exprs_alloc_.clear();

  real_row_idx_ = 0;
  read_rounds_ = 0;
  read_rows_ = 0;
  batch_size_ = -1;
}

int ObODPSJNITableRowIterator::StateValues::reuse()
{
  int ret = OB_SUCCESS;
  task_idx_ = -1;
  part_id_ = 0;
  start_ = 0;
  step_ = 0;
  count_ = 0;
  is_from_gi_pump_ = false;
  if (OB_NOT_NULL(odps_jni_scanner_)) {
    (void)odps_jni_scanner_->do_close();
    odps_jni_scanner_.reset();
  }
  return ret;
}

int ObODPSJNITableRowIterator::fetch_partition_row_count(const ObString &part_spec, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(odps_jni_schema_scanner_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexcepted null odps jni scanner", K(ret));
  } else if (!odps_jni_schema_scanner_->is_inited()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not inited", K(ret));
  } else if (!odps_jni_schema_scanner_->is_opened()) {
    ret = OB_JNI_ENV_ERROR;
    LOG_WARN("jni scanner is not opened", K(ret));
  } else {
    if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_row_count(arena_alloc_, part_spec, row_count))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

// =================== LONG FUNC ======================================

template <typename T>
int fill_numeric_type(T array, const std::shared_ptr<arrow::Field> field, const ObExpr &expr, ObEvalCtx &ctx,
    ObObjType type, int64_t num_rows, int64_t column_idx, ObDatum *datums, common::ObArenaAllocator &column_exprs_alloc)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_idx(0);

  if ((ObTinyIntType == type || ObSmallIntType == type || ObMediumIntType == type || ObInt32Type == type ||
          ObIntType == type) &&
      !is_oracle_mode()) {
    for (int row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      if (!array->IsNull(row_idx)) {
        datums[row_idx].set_int(array->Value(row_idx));
      } else {
        datums[row_idx].set_null();
      }
    }
  } else if (ObNumberType == type && is_oracle_mode()) {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      if (!array->IsNull(row_idx)) {
        number::ObNumber nmb;
        int64_t value = array->Value(row_idx);
        if (OB_FAIL(nmb.from(value, column_exprs_alloc))) {
          LOG_WARN("failed to cast int value to number", K(ret), K(array->Value(row_idx)), K(row_idx), K(column_idx));
        } else {
          datums[row_idx].set_number(nmb);
        }
      } else {
        datums[row_idx].set_null();
      }
    }
  } else if (ObDecimalIntType == type && is_oracle_mode()) {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      if (!array->IsNull(row_idx)) {
        int64_t in_val = array->Value(row_idx);

        ObDecimalInt *decint = nullptr;
        int32_t int_bytes = 0;
        ObDecimalIntBuilder tmp_alloc;
        ObScale out_scale = expr.datum_meta_.scale_;
        ObScale in_scale = 0;
        ObPrecision out_prec = expr.datum_meta_.precision_;
        ObPrecision in_prec = ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type].get_precision();
        const static int64_t DECINT64_MAX = get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
        if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
          in_prec = MAX_PRECISION_DECIMAL_INT_64;
        }
        if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes, in_prec))) {
          LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx), K(column_idx));
        } else if (ObDatumCast::need_scale_decimalint(in_scale, in_prec, out_scale, out_prec)) {
          ObDecimalIntBuilder res_val;
          if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                  decint, int_bytes, in_scale, out_scale, out_prec, expr.extra_, res_val))) {
            LOG_WARN("scale decimal int failed", K(ret), K(row_idx), K(column_idx));
          } else {
            datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
          }
        } else {
          datums[row_idx].set_decimal_int(decint, int_bytes);
        }
      } else {
        datums[row_idx].set_null();
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(type), K(field->type()->id()));
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_column_arrow(ObEvalCtx &ctx, const ObExpr &expr,
    const std::shared_ptr<arrow::Array> &array, const std::shared_ptr<arrow::Field> &field, const MirrorOdpsJniColumn& column, int64_t num_rows,
    int64_t column_idx)
{
  int ret = OB_SUCCESS;
  ObDatum *datums = expr.locate_batch_datums(ctx);
  ObObjType type = expr.obj_meta_.get_type();

  if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, num_rows))) {
    LOG_WARN("failed to init expr vector", K(ret), K(expr));
  } else {
    if (field->type()->id() == arrow::Type::BOOL) {
      std::shared_ptr<arrow::BooleanArray> bool_array =
          std::static_pointer_cast<arrow::BooleanArray>(array);
      if (OB_FAIL(fill_numeric_type(
              bool_array, field, expr, ctx, type, num_rows, column_idx, datums, column_exprs_alloc_))) {}
    } else if (field->type()->id() == arrow::Type::INT8) {
      std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> int8_array =
          std::static_pointer_cast<arrow::Int8Array>(array);
      if (OB_FAIL(fill_numeric_type(
              int8_array, field, expr, ctx, type, num_rows, column_idx, datums, column_exprs_alloc_))) {}
    } else if (field->type()->id() == arrow::Type::INT16) {
      std::shared_ptr<arrow::NumericArray<arrow::Int16Type>> int16_array =
          std::static_pointer_cast<arrow::Int16Array>(array);
      if (OB_FAIL(fill_numeric_type(
              int16_array, field, expr, ctx, type, num_rows, column_idx, datums, column_exprs_alloc_))) {}
    } else if (field->type()->id() == arrow::Type::INT32) {
      std::shared_ptr<arrow::NumericArray<arrow::Int32Type>> int32_array =
          std::static_pointer_cast<arrow::Int32Array>(array);
      if (OB_FAIL(fill_numeric_type(
              int32_array, field, expr, ctx, type, num_rows, column_idx, datums, column_exprs_alloc_))) {}
    } else if (field->type()->id() == arrow::Type::INT64) {
      std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> int64_array =
          std::static_pointer_cast<arrow::Int64Array>(array);
      if (OB_FAIL(fill_numeric_type(
              int64_array, field, expr, ctx, type, num_rows, column_idx, datums, column_exprs_alloc_))) {}
    } else if (field->type()->id() == arrow::Type::FLOAT) {
      if (ObFloatType == type) {
        std::shared_ptr<arrow::NumericArray<arrow::FloatType>> f_array =
            std::static_pointer_cast<arrow::FloatArray>(array);

        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!f_array->IsNull(row_idx)) {
            datums[row_idx].set_float(f_array->Value(row_idx));
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::DOUBLE) {
      if (ObDoubleType == type) {
        std::shared_ptr<arrow::NumericArray<arrow::DoubleType>> f_array =
            std::static_pointer_cast<arrow::DoubleArray>(array);

        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!f_array->IsNull(row_idx)) {
            datums[row_idx].set_double(f_array->Value(row_idx));
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::DECIMAL128) {
      std::shared_ptr<arrow::Decimal128Array> d_array = std::static_pointer_cast<arrow::Decimal128Array>(array);
      if (ObDecimalIntType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!d_array->IsNull(row_idx)) {
            ObDecimalIntBuilder res_val;
            ObDecimalInt *decint = nullptr;
            const ObDatumMeta &datum_meta = expr.datum_meta_;
            int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);

            if (4 == int_bytes) {
              int32_t v = *reinterpret_cast<const int128_t *>(d_array->GetValue(row_idx));
              res_val.from(v);
            } else if (8 == int_bytes) {
              int64_t v = *reinterpret_cast<const int128_t *>(d_array->GetValue(row_idx));
              res_val.from(v);
            } else if (16 == int_bytes) {
              int128_t v = *reinterpret_cast<const int128_t *>(d_array->GetValue(row_idx));
              res_val.from(v);
            }
            datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
          } else {
            datums[row_idx].set_null();
          }
        }
      } else if (ObNumberType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);

          if (!d_array->IsNull(row_idx)) {
            const char *v = reinterpret_cast<const char *>(d_array->GetValue(row_idx));
            ObString in_str(v);
            number::ObNumber nmb;
            ObNumStackOnceAlloc tmp_alloc;
            if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(),tmp_alloc, nmb))) {
              LOG_WARN("cast string to number failed", K(ret), K(row_idx), K(column_idx));
            } else {
              datums[row_idx].set_number(nmb);
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::DECIMAL256) {
      std::shared_ptr<arrow::Decimal256Array> d_array = std::static_pointer_cast<arrow::Decimal256Array>(array);
      if (ObDecimalIntType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!d_array->IsNull(row_idx)) {
            ObDecimalIntBuilder res_val;
            ObDecimalInt *decint = nullptr;
            const int256_t *v = reinterpret_cast<const int256_t *>(d_array->GetValue(row_idx));
            res_val.from(*v);
            datums[row_idx].set_decimal_int(res_val.get_decimal_int(), res_val.get_int_bytes());
          } else {
            datums[row_idx].set_null();
          }
        }
      } else if (ObNumberType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);

          if (!d_array->IsNull(row_idx)) {
            const char *v = reinterpret_cast<const char *>(d_array->GetValue(row_idx));
            ObString in_str(v);
            number::ObNumber nmb;
            ObNumStackOnceAlloc tmp_alloc;
            if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(), tmp_alloc, nmb))) {
              LOG_WARN("cast string to number failed", K(ret), K(row_idx), K(column_idx));
            } else {
              datums[row_idx].set_number(nmb);
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::STRING ||
               field->type()->id() == arrow::Type::BINARY) {
      std::shared_ptr<arrow::StringArray> s_array = std::static_pointer_cast<arrow::StringArray>(array);
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_idx(0);
      ObObjType in_type = ObVarcharType;
      ObObjType out_type = type;
      ObCollationType in_cs_type = field->type()->id() == arrow::Type::STRING ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
      ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
      if ((ObCharType == type && column.type_ == OdpsType::CHAR)
        || (ObVarcharType == type && column.type_ == OdpsType::VARCHAR)) {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!s_array->IsNull(row_idx)) {
            int32_t out_length = 0;
            const char *str = reinterpret_cast<const char *>(s_array->GetValue(row_idx, &out_length));
            ObString temp_str(out_length, str);
            ObString in_str;
            if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              bool has_set_res = false;
              if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                datums[row_idx].set_string(in_str);
              } else {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("TYPE NOT MATCH", K(ret));
              }
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else if ((ObVarcharType == type && (column.type_ == OdpsType::STRING || column.type_ == OdpsType::BINARY)) ||
                  ((ObTinyTextType == type || ObTextType == type || ObLongTextType == type || ObMediumTextType == type)
                      && (column.type_ == OdpsType::STRING || column.type_ == OdpsType::BINARY)) ||
                  (ObJsonType == type && column.type_ == OdpsType::JSON)) {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!s_array->IsNull(row_idx)) {
            // out_length是真实长度
            int32_t out_length = 0;
            const char *str = reinterpret_cast<const char *>(s_array->GetValue(row_idx, &out_length));
            // judge length是语义长度
            int32_t judge_length = 0;
            if (CHARSET_UTF8MB4 == out_charset) {
              judge_length = ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, str, out_length);
            } else {
              judge_length = out_length;
            }
            if (!(text_type_length_is_valid_at_runtime(type, judge_length)
                || varchar_length_is_valid_at_runtime(type, out_charset, judge_length, expr.max_length_))) {
              ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
              LOG_WARN("unexpected data length", K(ret), K(out_length), K(judge_length), K(expr.max_length_), K(type));
            } else {
              ObString temp_str(out_length, str);
              ObString in_str;
              if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
                LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
              } else {
                bool has_set_res = false;
                if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                  if (!ob_is_text_tc(type)) {
                    datums[row_idx].set_string(in_str);
                  } else {
                    if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(expr, in_str,
                                  ctx, NULL, datums[row_idx], in_type, in_cs_type))) {
                      LOG_WARN("cast string to string failed", K(ret), K(row_idx), K(column_idx));
                    }
                  }
                } else {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("TYPE NOT MATCH", K(ret));
                }
              }
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else if (ObRawType == type) {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!s_array->IsNull(row_idx)) {
            int32_t out_length = 0;
            const char *str = reinterpret_cast<const char *>(s_array->GetValue(row_idx, &out_length));
            if (out_length > expr.max_length_) {
              ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
              LOG_WARN("unexpected data length", K(ret), K(out_length), K(expr.max_length_), K(type));
            } else {
              ObString temp_str(out_length, str);
              if (OB_SUCC(ret)) {
                bool has_set_res = false;
                if (OB_FAIL(ObDatumHexUtils::hextoraw_string(expr, temp_str, ctx, datums[row_idx], has_set_res))) {
                  LOG_WARN("cast string to raw failed", K(ret), K(row_idx), K(column_idx));
                }
              }
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::DATE32) {
      std::shared_ptr<arrow::NumericArray<arrow::Date32Type>> date_array =
          std::static_pointer_cast<arrow::Date32Array>(array);
      if (ob_is_date_or_mysql_date(type) && is_mysql_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!date_array->IsNull(row_idx)) {
            int32_t v = date_array->Value(row_idx);
            int32_t date = v;
            if (ob_is_mysql_date_tc(type)) {
              ObMySQLDate mdate;
              ObTimeConverter::date_to_mdate(date, mdate);
              datums[row_idx].set_mysql_date(mdate);
            } else {
              datums[row_idx].set_date(date);
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::DATE64) { // tunnel datetime
      std::shared_ptr<arrow::NumericArray<arrow::Date64Type>> date_array =
          std::static_pointer_cast<arrow::Date64Array>(array);
      if (ob_is_datetime_or_mysql_datetime_tc(type) && is_mysql_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!date_array->IsNull(row_idx)) {
            int64_t v = date_array->Value(row_idx);
            int64_t datetime = v * 1000;

            int32_t offset_sec = 0;

            if (OB_ERR_UNKNOWN_TIME_ZONE ==timezone_ret_) {
              ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_ptr_), true);
              ObOTimestampData tmp_ot_data;
              tmp_ot_data.time_ctx_.tz_desc_ = 0;
              tmp_ot_data.time_ctx_.store_tz_id_ = 0;
              tmp_ot_data.time_us_ = datetime;
              if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampLTZType, tmp_ot_data,
                                                                NULL, ob_time_))) {
                LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
              } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time_))) {
                LOG_WARN("failed to convert string to tz_offset", K(ret));
              } else {
                offset_sec = ob_time_.parts_[DT_OFFSET_MIN] * 60;
                LOG_DEBUG("finish str_to_tz_offset", K(ob_time_), K(tmp_ot_data));
              }
            } else {
              offset_sec = timezone_offset_;
            }
            if (OB_SUCC(ret)) {
              datetime += SEC_TO_USEC(offset_sec);
              if (ob_is_mysql_datetime(type)) {
                ObMySQLDateTime mdatetime;
                ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
                datums[row_idx].set_mysql_datetime(mdatetime);
              } else {
                datums[row_idx].set_datetime(datetime);
              }
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::TIMESTAMP) { // storage api datetime
      std::shared_ptr<arrow::NumericArray<arrow::TimestampType>> date_array =
          std::static_pointer_cast<arrow::TimestampArray>(array);
      std::shared_ptr<arrow::TimestampType> timestamp_type = std::dynamic_pointer_cast<arrow::TimestampType>(field->type());
      if (column.type_ != OdpsType::DATETIME && is_mysql_mode()) {
        int64_t res_offset = 0;
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!date_array->IsNull(row_idx)) {
            int64_t v = date_array->Value(row_idx);
            int64_t datetime = 0;
            if (timestamp_type->unit() == arrow::TimeUnit::SECOND) {
              datetime = v * 1000000;
            } else if (timestamp_type->unit() == arrow::TimeUnit::MILLI) {
              datetime = v * 1000;
            } else if (timestamp_type->unit() == arrow::TimeUnit::MICRO) {
              datetime = v;
            } else {
              datetime = v / 1000;
            }
            if (ob_is_mysql_datetime(type)) {
              ObMySQLDateTime mdatetime;
              ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
              datums[row_idx].set_mysql_datetime(mdatetime);
            } else {
              datums[row_idx].set_datetime(datetime);
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else if (is_mysql_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!date_array->IsNull(row_idx)) {
            int64_t v = date_array->Value(row_idx); // us

            int64_t datetime = 0;
            if (timestamp_type->unit() == arrow::TimeUnit::SECOND) {
              datetime = v * 1000000;
            } else if (timestamp_type->unit() == arrow::TimeUnit::MILLI) {
              datetime = v * 1000;
            } else if (timestamp_type->unit() == arrow::TimeUnit::MICRO) {
              datetime = v;
            } else {
              datetime = v / 1000;
            }

            int32_t offset_sec = 0;
            if (OB_ERR_UNKNOWN_TIME_ZONE ==timezone_ret_) {
              ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_ptr_), true);
              ObOTimestampData tmp_ot_data;
              tmp_ot_data.time_ctx_.tz_desc_ = 0;
              tmp_ot_data.time_ctx_.store_tz_id_ = 0;
              tmp_ot_data.time_us_ = datetime;
              if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampLTZType, tmp_ot_data,
                                                                NULL, ob_time_))) {
                LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
              } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time_))) {
                LOG_WARN("failed to convert string to tz_offset", K(ret));
              } else {
                offset_sec = ob_time_.parts_[DT_OFFSET_MIN] * 60;
                LOG_DEBUG("finish str_to_tz_offset", K(ob_time_), K(tmp_ot_data));
              }
            } else {
              offset_sec = timezone_offset_;
            }
            if (OB_SUCC(ret)) {
              datetime += SEC_TO_USEC(offset_sec);
              if (ob_is_mysql_datetime(type)) {
                ObMySQLDateTime mdatetime;
                ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
                datums[row_idx].set_mysql_datetime(mdatetime);
              } else {
                datums[row_idx].set_datetime(datetime);
              }
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (field->type()->id() == arrow::Type::LIST) {
      std::shared_ptr<arrow::ListArray> list_array
        = std::static_pointer_cast<arrow::ListArray>(array);
      ObODPSArrayHelper *array_helper = array_helpers_.at(column_idx);
      if (OB_ISNULL(array_helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null array helper");
      } else if (field->type()->num_fields() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected field number");
      } else if (ObCollectionSQLType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        const std::shared_ptr<arrow::Field> &child_field = field->type()->field(0);
        ObIArrayType *child_array = array_helper->array_;
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          if (!list_array->IsNull(row_idx)) {
            std::shared_ptr<arrow::Array> cur_array = list_array->value_slice(row_idx);
            ObString res_str;
            if (OB_FAIL(fill_array_arrow(cur_array,
                                         child_field,
                                         array_helper))) {
              LOG_WARN("failed to fill array arrow");
            } else if (OB_FAIL(ObArrayExprUtils::set_array_res(child_array,
                                                               child_array->get_raw_binary_len(),
                                                               expr,
                                                               ctx,
                                                               res_str))) {
              LOG_WARN("get array binary string failed", K(ret));
            } else {
              datums[row_idx].set_string(res_str);
            }
          } else {
            datums[row_idx].set_null();
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("not implemented", K(ret), K(field->type()->id()));
    }
  }
  return ret;
}

template<typename T>
int fill_numeric_array_type(T arrow_array,
                            const std::shared_ptr<arrow::Field> field,
                            ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  if (ObTinyIntType == array_helper->element_type_) {
    ObArrayFixedSize<int8_t> *child_array = static_cast<ObArrayFixedSize<int8_t> *>(array_helper->array_);
    for (int row_idx = 0; OB_SUCC(ret) && row_idx < arrow_array->length(); ++row_idx) {
      if (!arrow_array->IsNull(row_idx)) {
        if (OB_FAIL(child_array->push_back(arrow_array->Value(row_idx)))) {
          LOG_WARN("failed to push tinyint to array");
        }
      } else if (OB_FAIL(child_array->push_null())) {
        LOG_WARN("failed to push null to array");
      }
    }
  } else if (ObSmallIntType == array_helper->element_type_) {
    ObArrayFixedSize<int16_t> *child_array = static_cast<ObArrayFixedSize<int16_t> *>(array_helper->array_);
    for (int row_idx = 0; OB_SUCC(ret) && row_idx < arrow_array->length(); ++row_idx) {
      if (!arrow_array->IsNull(row_idx)) {
        if (OB_FAIL(child_array->push_back(arrow_array->Value(row_idx)))) {
          LOG_WARN("failed to push tinyint to array");
        }
      } else if (OB_FAIL(child_array->push_null())) {
        LOG_WARN("failed to push null to array");
      }
    }
  } else if (ObInt32Type == array_helper->element_type_) {
    ObArrayFixedSize<int32_t> *child_array = static_cast<ObArrayFixedSize<int32_t> *>(array_helper->array_);
    for (int row_idx = 0; OB_SUCC(ret) && row_idx < arrow_array->length(); ++row_idx) {
      if (!arrow_array->IsNull(row_idx)) {
        if (OB_FAIL(child_array->push_back(arrow_array->Value(row_idx)))) {
          LOG_WARN("failed to push tinyint to array");
        }
      } else if (OB_FAIL(child_array->push_null())) {
        LOG_WARN("failed to push null to array");
      }
    }
  } else if (ObIntType == array_helper->element_type_) {
    ObArrayFixedSize<int64_t> *child_array = static_cast<ObArrayFixedSize<int64_t> *>(array_helper->array_);
    for (int row_idx = 0; OB_SUCC(ret) && row_idx < arrow_array->length(); ++row_idx) {
      if (!arrow_array->IsNull(row_idx)) {
        if (OB_FAIL(child_array->push_back(arrow_array->Value(row_idx)))) {
          LOG_WARN("failed to push tinyint to array");
        }
      } else if (OB_FAIL(child_array->push_null())) {
        LOG_WARN("failed to push null to array");
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(array_helper->element_type_), K(field->type()->id()));
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_array_arrow(const std::shared_ptr<arrow::Array>& array,
                                                const std::shared_ptr<arrow::Field>& element_field,
                                                ObODPSArrayHelper *array_helper) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null array helper");
  } else {
    array_helper->array_->clear();
  }
  if (OB_SUCC(ret)) {
    if (element_field->type()->id() == arrow::Type::BOOL) {
      std::shared_ptr<arrow::BooleanArray> bool_array =
          std::static_pointer_cast<arrow::BooleanArray>(array);
      if (OB_FAIL(fill_numeric_array_type(bool_array, element_field, array_helper))) {
        LOG_WARN("failed to fill bool numeric array type");
      }
    } else if (element_field->type()->id() == arrow::Type::INT8) {
      std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> int8_array
        = std::static_pointer_cast<arrow::Int8Array>(array);
      if (OB_FAIL(fill_numeric_array_type(int8_array, element_field, array_helper))) {
        LOG_WARN("failed to fill int8 numeric array type");
      }
    } else if (element_field->type()->id() == arrow::Type::INT16) {
      std::shared_ptr<arrow::NumericArray<arrow::Int16Type>> int16_array
        = std::static_pointer_cast<arrow::Int16Array>(array);
      if (OB_FAIL(fill_numeric_array_type(int16_array, element_field, array_helper))) {
        LOG_WARN("failed to fill int16 numeric array type");
      }
    } else if (element_field->type()->id() == arrow::Type::INT32) {
      std::shared_ptr<arrow::NumericArray<arrow::Int32Type>> int32_array
        = std::static_pointer_cast<arrow::Int32Array>(array);
      if (OB_FAIL(fill_numeric_array_type(int32_array, element_field, array_helper))) {
        LOG_WARN("failed to fill int32 numeric array type");
      }
    } else if (element_field->type()->id() == arrow::Type::INT64) {
      std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> int64_array
        = std::static_pointer_cast<arrow::Int64Array>(array);
      if (OB_FAIL(fill_numeric_array_type(int64_array, element_field, array_helper))) {
        LOG_WARN("failed to fill int64 numeric array type");
      }
    } else if (arrow::Type::FLOAT == element_field->type()->id() &&
               ObFloatType == array_helper->element_type_) {
      std::shared_ptr<arrow::NumericArray<arrow::FloatType>> f_array
              = std::static_pointer_cast<arrow::FloatArray>(array);
      ObArrayFixedSize<float> *child_array = static_cast<ObArrayFixedSize<float> *>(array_helper->array_);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < f_array->length(); ++row_idx) {
        if (!f_array->IsNull(row_idx)) {
          if (OB_FAIL(child_array->push_back(f_array->Value(row_idx)))) {
            LOG_WARN("failed to push tinyint to array");
          }
        } else if (OB_FAIL(child_array->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      }
    } else if (arrow::Type::DOUBLE == element_field->type()->id() &&
               ObDoubleType == array_helper->element_type_) {
      std::shared_ptr<arrow::NumericArray<arrow::DoubleType>> d_array
              = std::static_pointer_cast<arrow::DoubleArray>(array);
      ObArrayFixedSize<double> *child_array = static_cast<ObArrayFixedSize<double> *>(array_helper->array_);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < d_array->length(); ++row_idx) {
        if (!d_array->IsNull(row_idx)) {
          if (OB_FAIL(child_array->push_back(d_array->Value(row_idx)))) {
            LOG_WARN("failed to push tinyint to array");
          }
        } else if (OB_FAIL(child_array->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      }
    } else if (arrow::Type::STRING == element_field->type()->id() &&
               ObVarcharType == array_helper->element_type_) {
      std::shared_ptr<arrow::StringArray> s_array
          = std::static_pointer_cast<arrow::StringArray>(array);
      ObArrayBinary *child_array = static_cast<ObArrayBinary *>(array_helper->array_);
      ObCollationType out_cs_type = array_helper->element_collation_;
      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < s_array->length(); ++row_idx) {
        if (!s_array->IsNull(row_idx)) {
          int32_t out_length = 0;
          const char* str = reinterpret_cast<const char *>(s_array->GetValue(row_idx, &out_length));
          int32_t judge_length = 0;
          if (CHARSET_UTF8MB4 == out_charset) {
            judge_length = ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, str, out_length);
          } else {
            judge_length = out_length;
          }
          if (!(varchar_length_is_valid_at_runtime(array_helper->element_type_, out_charset, judge_length, array_helper->element_type_))) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("unexpected data length", K(out_length), K(judge_length), K(array_helper->element_length_), KPC(array_helper));
          } else {
            ObString temp_str(out_length, str);
            ObString in_str;
            if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              bool has_set_res = false;
              if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                if (OB_FAIL(child_array->push_back(in_str))) {
                  LOG_WARN("failed to push back string");
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("string need cast in array<varchar> not support");
              }
            }
          }
        } else if (OB_FAIL(child_array->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      }
    } else if (arrow::Type::LIST == element_field->type()->id() &&
               ObCollectionSQLType == array_helper->element_type_) {
      std::shared_ptr<arrow::ListArray> list_array
              = std::static_pointer_cast<arrow::ListArray>(array);
      ObArrayNested *nested_array = static_cast<ObArrayNested *>(array_helper->array_);
      ObIArrayType *child_array = array_helper->child_helper_->array_;
      if (element_field->type()->num_fields() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected field number");
      } else {
        const std::shared_ptr<arrow::Field> &child_field = element_field->type()->field(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < list_array->length(); ++row_idx) {
          if (!list_array->IsNull(row_idx)) {
            std::shared_ptr<arrow::Array> cur_array = list_array->value_slice(row_idx);
            if (OB_FAIL(SMART_CALL(fill_array_arrow(cur_array,
                                                    child_field,
                                                    array_helper->child_helper_)))) {
              LOG_WARN("failed to fill array arrow");
            } else if (OB_FAIL(child_array->init())) {
              LOG_WARN("failed to init child array");
            } else if (OB_FAIL(nested_array->push_back(*child_array))) {
              LOG_WARN("failed to push back child array", KP(nested_array), KP(child_array));
            }
          } else if (OB_FAIL(nested_array->push_null())) {
            LOG_WARN("failed to push null");
          }
        }
      }
    } else {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("not implemented", K(ret), K(element_field->type()->id()));
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_column_offheaptable(
    ObEvalCtx &ctx, const ObExpr &expr, const MirrorOdpsJniColumn &odps_column, int64_t num_rows, int32_t column_idx)
{
  int ret = OB_SUCCESS;
  OdpsDecoder *decoder = nullptr;
  if (OB_FAIL(create_odps_decoder(ctx, expr, odps_column, true,
                                  array_helpers_.at(column_idx), decoder))) {
    LOG_WARN("failed to create odps decoder");
  } else if (OB_FAIL(decoder->decode(ctx, expr, 0, num_rows))) {
    LOG_WARN("faield to decode");
  }
  return ret;
}

int ObODPSJNITableRowIterator::create_odps_decoder(ObEvalCtx &ctx,
                                                   const ObExpr &expr,
                                                   const MirrorOdpsJniColumn &odps_column,
                                                   bool is_root,
                                                   ObODPSArrayHelper *array_helper,
                                                   OdpsDecoder *&decoder)
{
  int ret = OB_SUCCESS;
  // Column meta is mapping by
  // `com.oceanbase.jni.connector.OffHeapTable#getMetaNativeAddress`
  JniScanner::JniTableMeta& column_meta = state_.odps_jni_scanner_->get_jni_table_meta();
  if (is_fixed_odps_type(odps_column.type_)) {
    void *ptr = column_exprs_alloc_.alloc(sizeof(OdpsFixedTypeDecoder));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for OdpsFixedTypeDecoder failed");
    } else {
      OdpsFixedTypeDecoder *fixed_decoder = new(ptr) OdpsFixedTypeDecoder(column_exprs_alloc_,
        is_root, odps_column,session_ptr_, ob_time_, timezone_ret_, timezone_offset_);
      if (OB_FAIL(fixed_decoder->init(column_meta, ctx, expr, array_helper))) {
        LOG_WARN("failed to init fixed decoder");
      } else {
        decoder = fixed_decoder;
      }
    }
  } else if (is_variety_odps_type(odps_column.type_)) {
    void *ptr = column_exprs_alloc_.alloc(sizeof(OdpsVarietyTypeDecoder));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for OdpsVarietyTypeDecoder failed");
    } else {
      OdpsVarietyTypeDecoder *variety_decoder = new(ptr) OdpsVarietyTypeDecoder(column_exprs_alloc_, is_root, odps_column);
      if (OB_FAIL(variety_decoder->init(column_meta, ctx, expr, array_helper))) {
        LOG_WARN("failed to init fixed decoder");
      } else {
        decoder = variety_decoder;
      }
    }
  } else if (is_array_odps_type(odps_column.type_)) {
    void *ptr = column_exprs_alloc_.alloc(sizeof(OdpsArrayTypeDecoder));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for OdpsArrayTypeDecoder failed");
    } else {
      OdpsArrayTypeDecoder *array_decoder = new(ptr) OdpsArrayTypeDecoder(column_exprs_alloc_, is_root, odps_column);
      if (OB_ISNULL(array_helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (OB_FAIL(array_decoder->init(column_meta, ctx, expr, array_helper))) {
        LOG_WARN("failed to init fixed decoder");
      } else if (OB_UNLIKELY(odps_column.child_columns_.count() != 1)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected child columns count", K(odps_column.child_columns_));
      } else if (!is_root && OB_FALSE_IT(array_helper = array_helper->child_helper_)) {
      } else if (OB_FAIL(create_odps_decoder(ctx, expr, odps_column.child_columns_.at(0),
                                             false, array_helper, array_decoder->child_decoder_))) {
        LOG_WARN("failed to create odps decoder");
      } else {
        decoder = array_decoder;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected odps type", K(ret), K(odps_column.type_));
  }
  return ret;
}

// =================== LONG FUNC END ==================================

int ObODPSJNITableRowIterator::OdpsDecoder::init(JniScanner::JniTableMeta& column_meta,
                                                 ObEvalCtx &ctx,
                                                 const ObExpr &expr,
                                                 ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  null_map_ptr_ = column_meta.next_meta_as_long();
  if (0 == null_map_ptr_) {
    // com.oceanbase.jni.connector.ColumnType.Type#UNSUPPORTED will set column
    // address as 0
    ret = OB_JNI_ERROR;
    LOG_WARN("Unsupported type in java side", K(ret));
  } else if (OB_LIKELY(is_root_)) {
    datums_ = expr.locate_batch_datums(ctx);
    type_ = expr.obj_meta_.get_type();
    type_precision_ = expr.datum_meta_.precision_;
    type_scale_ = expr.datum_meta_.scale_;
    type_collation_ = expr.datum_meta_.cs_type_;
    type_length_ = expr.max_length_;
  } else if (OB_ISNULL(array_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else {
    type_ = array_helper->element_type_;
    type_precision_ = array_helper->element_precision_;
    type_scale_ = array_helper->element_scale_;
    type_collation_ = array_helper->element_collation_;
    type_length_ = array_helper->element_length_;
    array_ = array_helper->array_;
  }
  return ret;
}


int ObODPSJNITableRowIterator::OdpsFixedTypeDecoder::init(JniScanner::JniTableMeta& column_meta,
                                                          ObEvalCtx &ctx,
                                                          const ObExpr &expr,
                                                          ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OdpsDecoder::init(column_meta, ctx, expr, array_helper))) {
    LOG_WARN("failed to init");
  } else {
    // Hold the column address
    column_addr_ = column_meta.next_meta_as_long();
  }
  return ret;
}

int ObODPSJNITableRowIterator::OdpsVarietyTypeDecoder::init(JniScanner::JniTableMeta& column_meta,
                                                            ObEvalCtx &ctx,
                                                            const ObExpr &expr,
                                                            ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OdpsDecoder::init(column_meta, ctx, expr, array_helper))) {
    LOG_WARN("failed to init");
  } else {
    offsets_ = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
    base_addr_ = reinterpret_cast<char *>(column_meta.next_meta_as_ptr());
  }
  return ret;
}

int ObODPSJNITableRowIterator::OdpsArrayTypeDecoder::init(JniScanner::JniTableMeta& column_meta,
                                                          ObEvalCtx &ctx,
                                                          const ObExpr &expr,
                                                          ObODPSArrayHelper *array_helper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OdpsDecoder::init(column_meta, ctx, expr, array_helper))) {
    LOG_WARN("failed to init");
  } else {
    offsets_ = reinterpret_cast<int64_t *>(column_meta.next_meta_as_ptr());
  }
  return ret;
}

int ObODPSJNITableRowIterator::OdpsDecoder::decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_root_ && nullptr == datums_) ||
      OB_UNLIKELY(!is_root_ && nullptr == array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected decoder", K(is_root_), KP(datums_), KP(array_));
  } else if (is_root_ && OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, size))) {
    LOG_WARN("failed to init expr vector", K(ret), K(expr));
  }
  return ret;
}

#define SET_INTERGER_TO_ARRAY(value, type, array)                                           \
if (ObTinyIntType == type) {                                                                \
  if (OB_FAIL(static_cast<ObArrayFixedSize<int8_t> *>(array)->push_back(value))) {          \
    LOG_WARN("failed to push tinyint to array");                                            \
  }                                                                                         \
} else if (ObSmallIntType == type) {                                                        \
  if (OB_FAIL(static_cast<ObArrayFixedSize<int16_t> *>(array)->push_back(value))) {         \
    LOG_WARN("failed to push smallint to array");                                           \
  }                                                                                         \
} else if (ObMediumIntType == type) {                                                       \
  ret = OB_NOT_SUPPORTED;                                                                   \
  LOG_WARN("not supported element type", K(type));                                          \
} else if (ObInt32Type == type) {                                                           \
  if (OB_FAIL(static_cast<ObArrayFixedSize<int32_t> *>(array)->push_back(value))) {         \
    LOG_WARN("failed to push int to array");                                                \
  }                                                                                         \
} else if (ObIntType == type) {                                                             \
  if (OB_FAIL(static_cast<ObArrayFixedSize<int64_t> *>(array)->push_back(value))) {         \
    LOG_WARN("failed to push bigint to array");                                             \
  }                                                                                         \
}

int ObODPSJNITableRowIterator::OdpsFixedTypeDecoder::decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size)
{
  int ret = OB_SUCCESS;
  const OdpsType odps_type = odps_column_.type_;
  int32_t type_size = odps_column_.type_size_;
  if (OB_FAIL(ObODPSJNITableRowIterator::OdpsDecoder::decode(ctx, expr, offset, size))) {
    LOG_WARN("failed to decode");
  } else if (odps_type == OdpsType::BOOLEAN) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    if (ObTinyIntType == type_ && !is_oracle_mode()) {
      for (int row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          if (OB_LIKELY(is_root_)) {
            datums_[row_idx].set_null();
          } else if (OB_FAIL(array_->push_null())) {
            LOG_WARN("failed to push null to array");
          }
        } else {
          bool *v = reinterpret_cast<bool *>(column_addr_ + row_idx);
          if (OB_LIKELY(is_root_)) {
            datums_[row_idx].set_int(*v);
          } else {
            SET_INTERGER_TO_ARRAY(*v, type_, array_);
          }
        }
      }
    } else if (ObNumberType == type_ && is_oracle_mode() && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          bool *v = reinterpret_cast<bool *>(column_addr_ + row_idx);
          int64_t in_val = *v;
          number::ObNumber nmb;
          if (OB_FAIL(nmb.from(in_val, alloc_))) {
            LOG_WARN("failed to cast int value to number", K(ret), K(in_val),
                     K(row_idx));
          } else {
            datums_[row_idx].set_number(nmb);
          }
        }
      }
    } else if (is_oracle_mode() && ObDecimalIntType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          bool *v = reinterpret_cast<bool *>(column_addr_ + row_idx);
          int64_t in_val = *v;

          ObDecimalInt *decint = nullptr;
          int32_t int_bytes = 0;
          ObDecimalIntBuilder tmp_alloc;
          ObScale out_scale = type_scale_;
          ObScale in_scale = 0;
          ObPrecision out_prec = type_precision_;
          ObPrecision in_prec =
              ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type_]
                  .get_precision();
          const static int64_t DECINT64_MAX =
              get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
          if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
            in_prec = MAX_PRECISION_DECIMAL_INT_64;
          }
          if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes,
                                         in_prec))) {
            LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx));
          } else if (ObDatumCast::need_scale_decimalint(
                         in_scale, in_prec, out_scale, out_prec)) {
            ObDecimalIntBuilder res_val;
            if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                    decint, int_bytes, in_scale, out_scale, out_prec,
                    expr.extra_, res_val))) {
              LOG_WARN("scale decimal int failed", K(ret), K(row_idx));
            } else {
              datums_[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                              res_val.get_int_bytes());
            }
          } else {
            datums_[row_idx].set_decimal_int(decint, int_bytes);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type_", K(ret), K(type_), K(is_root_));
    }
  } else if (odps_type == OdpsType::TINYINT ||
             odps_type == OdpsType::SMALLINT ||
             odps_type == OdpsType::INT ||
             odps_type == OdpsType::BIGINT) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    if ((ObTinyIntType == type_ || ObSmallIntType == type_ ||
         ObInt32Type == type_ || ObIntType == type_) && !is_oracle_mode()) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          if (OB_LIKELY(is_root_)) {
            datums_[row_idx].set_null();
          } else if (OB_FAIL(array_->push_null())) {
            LOG_WARN("failed to push null to array");
          }
        } else {
          int64_t v = 0;
          if (odps_type == OdpsType::TINYINT) {
            v = *reinterpret_cast<int8_t *>(column_addr_ + 1L * row_idx);
          } else if (odps_type == OdpsType::SMALLINT) {
            v = *reinterpret_cast<int16_t *>(column_addr_ + 2L * row_idx);
          } else if (odps_type == OdpsType::INT) {
            v = *reinterpret_cast<int32_t *>(column_addr_ + 4L * row_idx);
          } else if (odps_type == OdpsType::BIGINT) {
            v = *reinterpret_cast<int64_t *>(column_addr_ + 8L * row_idx);
          }
          if (OB_LIKELY(is_root_)) {
            datums_[row_idx].set_int(v);
          } else {
            SET_INTERGER_TO_ARRAY(v, type_, array_);
          }
        }
      }
    } else if (is_oracle_mode() && ObNumberType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        }else {
          int64_t in_val = 0;
          if (odps_type == OdpsType::TINYINT) {
            int8_t *v = reinterpret_cast<int8_t *>(column_addr_ + 1L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::SMALLINT) {
            int16_t *v = reinterpret_cast<int16_t *>(column_addr_ + 2L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::INT) {
            int32_t *v = reinterpret_cast<int32_t *>(column_addr_ + 4L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::BIGINT) {
            int64_t *v = reinterpret_cast<int64_t *>(column_addr_ + 8L * row_idx);
            in_val = *v;
          }
          number::ObNumber nmb;
          if (OB_FAIL(nmb.from(in_val, alloc_))) {
            LOG_WARN("failed to cast int value to number", K(ret),
                     K(in_val), K(row_idx));
          } else {
            datums_[row_idx].set_number(nmb);
          }
        }
      }
    } else if (is_oracle_mode() && ObDecimalIntType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          int64_t in_val = 0;
          if (odps_type == OdpsType::TINYINT) {
            int8_t *v = reinterpret_cast<int8_t *>(column_addr_ + 1L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::SMALLINT) {
            int16_t *v = reinterpret_cast<int16_t *>(column_addr_ + 2L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::INT) {
            int32_t *v = reinterpret_cast<int32_t *>(column_addr_ + 4L * row_idx);
            in_val = *v;
          } else if (odps_type == OdpsType::BIGINT) {
            int64_t *v = reinterpret_cast<int64_t *>(column_addr_ + 8L * row_idx);
            in_val = *v;
          }
          ObDecimalInt *decint = nullptr;
          int32_t int_bytes = 0;
          ObDecimalIntBuilder tmp_alloc;
          ObScale out_scale = type_scale_;
          ObScale in_scale = 0;
          ObPrecision out_prec = type_precision_;
          ObPrecision in_prec =
              ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type_]
                  .get_precision();
          const static int64_t DECINT64_MAX =
              get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
          if (in_prec > MAX_PRECISION_DECIMAL_INT_64 &&
              in_val < DECINT64_MAX) {
            in_prec = MAX_PRECISION_DECIMAL_INT_64;
          }
          if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint,
                                         int_bytes, in_prec))) {
            LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx));
          } else if (ObDatumCast::need_scale_decimalint(
                         in_scale, in_prec, out_scale, out_prec)) {
            ObDecimalIntBuilder res_val;
            if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                    decint, int_bytes, in_scale, out_scale, out_prec,
                    expr.extra_, res_val))) {
              LOG_WARN("scale decimal int failed", K(ret), K(row_idx));
            } else {
              datums_[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                              res_val.get_int_bytes());
            }
          } else {
            datums_[row_idx].set_decimal_int(decint, int_bytes);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else if (odps_type == OdpsType::FLOAT && ObFloatType == type_) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
      bool is_null = (*null_value == 1);
      if (is_null) {
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_null();
        } else if (OB_FAIL(array_->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      } else {
        float *v = reinterpret_cast<float *>(column_addr_ + 4L * row_idx);
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_float(*v);
        } else {
          if (OB_FAIL(static_cast<ObArrayFixedSize<float> *>(array_)->push_back(*v))) {
            LOG_WARN("failed to push float to array");
          }
        }
      }
    }
  } else if (odps_type == OdpsType::DOUBLE && ObDoubleType == type_) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      char *null_value = reinterpret_cast<char *>(null_map_ptr_ + row_idx);
      bool is_null = (*null_value == 1);
      if (is_null) {
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_null();
        } else if (OB_FAIL(array_->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      } else {
        double *v = reinterpret_cast<double *>(column_addr_ + 8L * row_idx);
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_double(*v);
        } else {
          if (OB_FAIL(static_cast<ObArrayFixedSize<double> *>(array_)->push_back(*v))) {
            LOG_WARN("failed to push double to array");
          }
        }
      }
    }
  } else if (odps_type == OdpsType::DECIMAL) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    if (ObDecimalIntType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          // Note: the type size is needed to get offset of decimal value and
          // decimal value could be decimal32, decimal64, decimal128
          ObDecimalIntBuilder res_val;
          ObDecimalInt *decint = nullptr;
          if (4 == type_size) {
            int32_t *v = reinterpret_cast<int32_t *>(column_addr_ + 1L * type_size * row_idx);
            res_val.from(*v);
          } else if (8 == type_size) {
            int64_t *v = reinterpret_cast<int64_t *>(column_addr_ + 1L * type_size * row_idx);
            res_val.from(*v);
          } else if (16 == type_size) {
            int128_t *v = reinterpret_cast<int128_t *>(column_addr_ + 1L * type_size * row_idx);
            res_val.from(*v);
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("not supported type size", K(ret), K(type_size));
          }
          datums_[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                          res_val.get_int_bytes());
        }
      }
    } else if (ObNumberType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          // Note: the type size is needed to get offset of decimal value and
          // decimal value could be decimal32, decimal64, decimal128
          char *v = reinterpret_cast<char *>(column_addr_ + 1L * type_size * row_idx);
          ObString in_str(v);
          number::ObNumber nmb;
          ObNumStackOnceAlloc tmp_alloc;
          if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(
                  expr, in_str, ctx.exec_ctx_.get_user_logging_ctx(), tmp_alloc, nmb))) {
            LOG_WARN("cast string to number failed", K(ret), K(row_idx));
          } else {
            datums_[row_idx].set_number(nmb);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else if (odps_type == OdpsType::DATETIME) {
    // TODO(bitao): checking timestamp
    if (ob_is_datetime_or_mysql_datetime_tc(type_) && is_mysql_mode() && is_root_) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_idx(0);
      // NOTE: should not add offset in table scan
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char *null_value = reinterpret_cast<char *>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (!is_null) {
          // Note: datetime value already handled as nanoseconds from
          // 1970-01-01 00:00:00 UTC in java side.
          int64_t *v = reinterpret_cast<int64_t *>(column_addr_ + 8L * row_idx);
          int64_t datetime = *v / 1000;
          int32_t res_offset = 0;

          int32_t offset_sec = 0;
          if (OB_ERR_UNKNOWN_TIME_ZONE == timezone_ret_) {
            ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_ptr_), true);
            ObOTimestampData tmp_ot_data;
            tmp_ot_data.time_ctx_.tz_desc_ = 0;
            tmp_ot_data.time_ctx_.store_tz_id_ = 0;
            tmp_ot_data.time_us_ = datetime;
            if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampLTZType, tmp_ot_data,
                                                              NULL, ob_time_))) {
              LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
            } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time_))) {
              LOG_WARN("failed to convert string to tz_offset", K(ret));
            } else {
              offset_sec = ob_time_.parts_[DT_OFFSET_MIN] * 60;
              LOG_DEBUG("finish str_to_tz_offset", K(ob_time_), K(tmp_ot_data));
            }
          } else {
            offset_sec = timezone_offset_;
          }
          if (OB_SUCC(ret)) {
            datetime += SEC_TO_USEC(offset_sec);
            if (ob_is_mysql_datetime(type_)) {
              ObMySQLDateTime mdatetime;
              ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
              datums_[row_idx].set_mysql_datetime(mdatetime);
            } else {
              datums_[row_idx].set_datetime(datetime);
            }
          }
        } else {
          datums_[row_idx].set_null();
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else if (odps_type == OdpsType::TIMESTAMP_NTZ ||
             odps_type == OdpsType::TIMESTAMP) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    // TODO(bitao): checking timestamp
    if (ob_is_datetime_or_mysql_datetime_tc(type_) && is_mysql_mode() && is_root_) {
      int64_t res_offset = SEC_TO_USEC(0);
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          // Note: datetime value already handled as nanoseconds from
          // 1970-01-01 00:00:00 UTC in java side.
          int64_t *v = reinterpret_cast<int64_t *>(column_addr_ + 8L * row_idx);
          int64_t datetime = *v / 1000 + res_offset;
          if (ob_is_mysql_datetime(type_)) {
            ObMySQLDateTime mdatetime;
            ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
            datums_[row_idx].set_mysql_datetime(mdatetime);
          } else {
            datums_[row_idx].set_datetime(datetime);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else if (odps_type == OdpsType::DATE) {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
    batch_info_guard.set_batch_idx(0);
    // DATE should be after DATETIME
    if (ob_is_date_or_mysql_date(type_) && is_mysql_mode() && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          int *v = reinterpret_cast<int*>(column_addr_ + 4L * row_idx);
          // Note: java side already use epoch day interval, so can set date directly.
          int32_t date = *v;
          if (ob_is_mysql_date_tc(type_)) {
            ObMySQLDate mdate;
            ObTimeConverter::date_to_mdate(date, mdate);
            datums_[row_idx].set_mysql_date(mdate);
          } else {
            datums_[row_idx].set_date(date);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected odps type", K(ret), K(odps_type), K(type_), K(is_root_));
  }

  return ret;
}

#undef SET_INTERGER_TO_ARRAY

int ObODPSJNITableRowIterator::OdpsVarietyTypeDecoder::decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size) {
  int ret = OB_SUCCESS;
  const OdpsType odps_type = odps_column_.type_;
  int32_t type_size = odps_column_.type_size_;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_idx(0);
  if (OB_FAIL(ObODPSJNITableRowIterator::OdpsDecoder::decode(ctx, expr, offset, size))) {
    LOG_WARN("failed to decode");
  } else if ((odps_type == OdpsType::CHAR && ObCharType == type_) ||
             (odps_type == OdpsType::VARCHAR && ObVarcharType == type_)) {
    ObObjType in_type = type_;
    ObObjType out_type = type_;
    ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
    ObCollationType out_cs_type = type_collation_;
    for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
      bool is_null = (*null_value == 1);
      if (is_null) {
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_null();
        } else if (OB_FAIL(array_->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      } else {
        ObString temp_str;
        ObString in_str;
        if (row_idx == 0) {
          int length = offsets_[row_idx];
          temp_str = ObString(length, base_addr_);
          base_addr_ += length; // Note: remember to add length to get next
        } else {
          int length = offsets_[row_idx] - offsets_[row_idx - 1];
          temp_str = ObString(length, base_addr_);
          base_addr_ += length; // Note: remember to add length to get next
        }
        if (OB_FAIL(ob_write_string(alloc_, temp_str, in_str))) {
          LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
        } else {
          bool has_set_res = false;
          ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
          if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
            if (OB_LIKELY(is_root_)) {
              datums_[row_idx].set_string(in_str);
            } else {
              if (OB_FAIL(static_cast<ObArrayBinary *>(array_)->push_back(in_str))) {
                LOG_WARN("failed to push double to array");
              }
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("string need cast in array<varchar> not support");
          }
        }
      }
    }
  } else if (odps_type == OdpsType::STRING || odps_type == OdpsType::BINARY || odps_type == OdpsType::JSON) {
    if (ObVarcharType == type_ ||
          ((ObTinyTextType == type_ ||
             ObTextType == type_ ||
             ObLongTextType == type_ ||
             ObMediumTextType == type_ ||
             ObJsonType == type_) &&
            is_root_)) {
      ObObjType in_type = ObVarcharType;
      ObObjType out_type = type_;
      ObCollationType out_cs_type = type_collation_;
      ObCharsetType out_charset = common::ObCharset::charset_type_by_coll(out_cs_type);
      ObCollationType in_cs_type = odps_type == OdpsType::STRING ? CS_TYPE_UTF8MB4_BIN : CS_TYPE_BINARY;
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset+ size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          if (OB_LIKELY(is_root_)) {
            datums_[row_idx].set_null();
          } else if (OB_FAIL(array_->push_null())) {
            LOG_WARN("failed to push null to array");
          }
        } else {
          int length = 0;
          if (row_idx == 0) {
            length = offsets_[row_idx];
          } else {
            length = offsets_[row_idx] - offsets_[row_idx - 1];
          }
          int judge_length = 0;
          if (CHARSET_UTF8MB4 == out_charset) {
            judge_length = ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN, base_addr_, length);
          } else {
            judge_length = length;
          }
          if (!(text_type_length_is_valid_at_runtime(type_, judge_length)
                || varchar_length_is_valid_at_runtime(type_, out_charset, judge_length, type_length_))) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("unexpected data length", K(ret), K(length), K(type_length_), K(type_));
          } else {
            ObString temp_str = ObString(length, base_addr_);
            base_addr_ += length;
            ObString in_str;
            if (OB_FAIL(ob_write_string(alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              bool has_set_res = false;
              if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                if (OB_LIKELY(is_root_)) {
                  if (!ob_is_text_tc(type_)) {
                    datums_[row_idx].set_string(in_str);
                  } else {
                    if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(
                          expr, in_str, ctx, NULL, datums_[row_idx], in_type,
                          in_cs_type))) {
                      LOG_WARN("cast string to text failed", K(ret), K(row_idx));
                    }
                  }
                } else {
                  if (OB_FAIL(static_cast<ObArrayBinary *>(array_)->push_back(in_str))) {
                    LOG_WARN("failed to push double to array");
                  }
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("string need cast in array<varchar> not support");
              }
            }
          }
        }
      }
    } else if (ObRawType == type_ && is_root_) {
      for (int64_t row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
        bool is_null = (*null_value == 1);

        if (is_null) {
          datums_[row_idx].set_null();
        } else {
          ObString in_str;
          int length = 0;
          if (row_idx == 0) {
            length = offsets_[row_idx];
          } else {
            length = offsets_[row_idx] - offsets_[row_idx - 1];
          }
          if (length > type_length_) {
            ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
            LOG_WARN("unexpected data length", K(ret), K(length), K(type_length_), K(type_));
          } else {
            ObString temp_str{length, base_addr_};
            in_str = temp_str;
            base_addr_ += length;
            bool has_set_res = false;
            if (OB_FAIL(ObDatumHexUtils::hextoraw_string(
                    expr, in_str, ctx, datums_[row_idx], has_set_res))) {
              LOG_WARN("cast string to raw failed", K(ret), K(row_idx));
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret), K(type_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected odps type", K(ret), K(odps_type), K(type_), K(is_root_));
  }

  return ret;
}

int ObODPSJNITableRowIterator::OdpsArrayTypeDecoder::decode(ObEvalCtx &ctx, const ObExpr &expr, int64_t offset, int64_t size) {
  int ret = OB_SUCCESS;
  const OdpsType odps_type = odps_column_.type_;
  int32_t type_size = odps_column_.type_size_;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_idx(0);
  if (OB_FAIL(ObODPSJNITableRowIterator::OdpsDecoder::decode(ctx, expr, offset, size))) {
    LOG_WARN("failed to decode");
  } else if (odps_type == OdpsType::ARRAY && ObCollectionSQLType == type_) {
    for (int row_idx = offset; OB_SUCC(ret) && row_idx < offset + size; ++row_idx) {
      batch_info_guard.set_batch_idx(row_idx);
      char* null_value = reinterpret_cast<char*>(null_map_ptr_ + row_idx);
      bool is_null = (*null_value == 1);
      if (is_null) {
        if (OB_LIKELY(is_root_)) {
          datums_[row_idx].set_null();
        } else if (OB_FAIL(array_->push_null())) {
          LOG_WARN("failed to push null to array");
        }
      } else {
        int64_t array_offset = 0;
        int64_t array_size = 0;
        if (row_idx == 0) {
          array_offset = 0;
          array_size = offsets_[row_idx];
        } else {
          array_offset = offsets_[row_idx - 1];
          array_size = offsets_[row_idx] - offsets_[row_idx - 1];
        }
        ObIArrayType *child_array = child_decoder_->array_;
        if (OB_FAIL(child_decoder_->decode(ctx, expr, array_offset, array_size))) {
          LOG_WARN("failed to decode");
        } else if (OB_LIKELY(is_root_)) {
          ObString res_str;
          if (OB_FAIL(ObArrayExprUtils::set_array_res(child_array, child_array->get_raw_binary_len(), expr, ctx, res_str))) {
            LOG_WARN("get array binary string failed", K(ret));
          } else {
            datums_[row_idx].set_string(res_str);
          }
        } else {
          ObArrayNested *nested_array = static_cast<ObArrayNested *>(array_);
          // 这个init函数需要在child_array中的元素已经填充完毕后调用，即在child_decoder->decode之后调用。否则无法正确初始化类成员。
          if (OB_FAIL(child_array->init())) {
            LOG_WARN("failed to init child array");
          } else if (OB_FAIL(nested_array->push_back(*child_array))) {
            LOG_WARN("failed to push back child array", KP(array_), KP(child_array), KP(child_array->get_data()));
          }
        }
        if (OB_SUCC(ret)) {
          child_array->clear();
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected odps type or ob type", K(ret), K(odps_type), K(type_), K(is_root_));
  }
  return ret;
}

int ObODPSJNITableRowIterator::construct_predicate_using_white_filter(const ObDASScanCtDef &das_ctdef,
                                                                      ObDASScanRtDef *das_rtdef,
                                                                      ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode()) {
  } else if (das_rtdef != nullptr) {
    sql::ObPushdownFilterExecutor *filter = das_rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (filter == nullptr) {
      // do nothing
    } else if (OB_FAIL(construct_predicate_using_white_filter(das_ctdef, exec_ctx, *filter))) {
      LOG_WARN("failed to construct predicate using white filter");
    }
  } else {
    ObEvalCtx eval_ctx(exec_ctx);
    ObPushdownOperator pd_expr_op(eval_ctx, das_ctdef.pd_expr_spec_);
    if (OB_FAIL(pd_expr_op.init_pushdown_storage_filter())) {
      LOG_WARN("failed to init pushdown storage filter failed");
    } else if (pd_expr_op.pd_storage_filters_ == nullptr) {
      // do nothing
    } else if (OB_FAIL(construct_predicate_using_white_filter(das_ctdef, exec_ctx,
                                                              *pd_expr_op.pd_storage_filters_))) {
      LOG_WARN("failed to construct predicate using white filter");
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::construct_predicate_using_white_filter(const ObDASScanCtDef &das_ctdef,
                                                                      ObExecContext &exec_ctx,
                                                                      sql::ObPushdownFilterExecutor &filter)
{
  int ret = OB_SUCCESS;
  bool can_pushdown = true;
  bool is_valid = true;
  ObSqlString predicate;
  ObSEArray<ObExpr*, 16> access_exprs;
  ObObjPrintParams print_params = CREATE_OBJ_PRINT_PARAM(exec_ctx.get_my_session());
  print_params.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  common::ObArenaAllocator arena_alloc;
  char *buf = nullptr;
  int64_t default_length = 1024;
  if (OB_FAIL(filter.init_evaluated_datums(is_valid))) {
    LOG_WARN("failed to init evaluated datums");
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(init_access_exprs(das_ctdef, access_exprs, is_valid))) {
    LOG_WARN("failed to init column exprs");
  } else if (!is_valid) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char*>(arena_alloc.alloc(default_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memeory for print obj");
  } else if (OB_FAIL(print_predicate_string(access_exprs, das_ctdef.pd_expr_spec_.ext_file_column_exprs_,
                                            filter, print_params, arena_alloc, buf, default_length,
                                            predicate, can_pushdown, true))) {
    LOG_WARN("failed to check filter can pushdown");
  } else if (can_pushdown) {
    predicate_buf_len_ = predicate.length() + 1;
    char *alloc_buf = static_cast<char *>(arena_alloc_.alloc(predicate_buf_len_));
    if (OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to extend stmt buf", K(ret), K(predicate.length()));
    } else {
      MEMCPY(alloc_buf, predicate.ptr(), predicate.length());
      alloc_buf[predicate.length()] = '\0';
      predicate_buf_ = alloc_buf;
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_access_exprs(const ObDASScanCtDef &das_ctdef,
                                                 ObIArray<ObExpr*> &access_exprs,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_UNLIKELY(das_ctdef.access_column_ids_.count() != das_ctdef.pd_expr_spec_.access_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids not equal to access expr");
  }
  const ObIArray<ObExpr*> &ext_column_convert_exprs = das_ctdef.pd_expr_spec_.ext_column_convert_exprs_;
  for (int i = 0; is_valid && i < ext_column_convert_exprs.count(); ++i) {
    ObExpr *convert_expr = ext_column_convert_exprs.at(i);
    if (OB_ISNULL(convert_expr) || T_FUN_COLUMN_CONV != convert_expr->type_ ||
        convert_expr->arg_cnt_ != 6) {
      is_valid = false;
    } else {
      ObExpr *ori_expr = convert_expr->args_[4];
      if (T_PSEUDO_EXTERNAL_FILE_COL != ori_expr->type_ &&
          T_PSEUDO_PARTITION_LIST_COL != ori_expr->type_) {
        is_valid = false;
        LOG_TRACE("pushdown is disabled because dependent expr has non pseudo expr", K(ori_expr->type_));
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && is_valid && i < das_ctdef.access_column_ids_.count(); ++i) {
    ObExpr *cur_expr = das_ctdef.pd_expr_spec_.access_exprs_.at(i);
    if (OB_HIDDEN_LINE_NUMBER_COLUMN_ID == das_ctdef.access_column_ids_.at(i) ||
        OB_HIDDEN_FILE_ID_COLUMN_ID == das_ctdef.access_column_ids_.at(i)) {
      // do nothing
    } else if (OB_FAIL(access_exprs.push_back(cur_expr))) {
      LOG_WARN("failed to push back expr");
    }
  }
  if (OB_SUCC(ret) && is_valid &&
      access_exprs.count() != das_ctdef.pd_expr_spec_.ext_file_column_exprs_.count()) {
    // External table may map one odps column to multiple ob column. In this sinario, we can not map
    // an access expr to an odps column.
    // e.g.  create external table t1 (c1 TINYINT as (external$tablecol1),
    //                                 c2 TINYINT(1) as (external$tablecol1));
    is_valid = false;
    LOG_TRACE("pushdown is disabled because multiple columns map to one odps column", K(access_exprs),
                K(das_ctdef.pd_expr_spec_.ext_file_column_exprs_));
  }
  return ret;
}

int ObODPSJNITableRowIterator::get_mirror_column(const ObIArray<ObExpr*> &access_exprs,
                                                 const ObIArray<ObExpr*> &file_column_exprs,
                                                 const ObExpr* column_expr,
                                                 MirrorOdpsJniColumn *&mirror_column)
{
  int ret = OB_SUCCESS;
  ObExpr* column_file_expr = nullptr;
  for (int64_t i = 0; column_file_expr == nullptr && i < access_exprs.count(); ++i) {
    if (column_expr == access_exprs.at(i)) {
      column_file_expr = file_column_exprs.at(i);
    }
  }
  if (OB_ISNULL(column_file_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null column file expr");
  } else if (column_file_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
    mirror_column = &mirror_nonpart_column_list_.at(column_file_expr->extra_ - 1);
  } else if (column_file_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
    mirror_column = &mirror_partition_column_list_.at(column_file_expr->extra_ - 1);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column file expr type", KPC(column_file_expr));
  }
  return ret;
}

int ObODPSJNITableRowIterator::print_predicate_string(const ObIArray<ObExpr*> &access_exprs,
                                                      const ObIArray<ObExpr*> &file_column_exprs,
                                                      sql::ObPushdownFilterExecutor &filter,
                                                      ObObjPrintParams &print_params,
                                                      ObIAllocator &alloc,
                                                      char *&buf,
                                                      int64_t &length,
                                                      ObSqlString &predicate,
                                                      bool &can_pushdown,
                                                      bool is_root /*= false */)
{
  int ret = OB_SUCCESS;
  can_pushdown = true;
  if (filter.is_logic_and_node()) {
    sql::ObPushdownFilterExecutor **children = filter.get_childs();
    ObSEArray<ObSqlString, 4> and_predicates;
    can_pushdown = false;
    for (uint32_t i = 0; OB_SUCC(ret) && i < filter.get_child_count(); ++i) {
      bool cur_can_pushdown = true;
      ObSqlString and_predicate;
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null child filter", K(i));
      } else if (OB_FAIL(print_predicate_string(access_exprs, file_column_exprs, *children[i],
                                                print_params, alloc, buf, length,
                                                and_predicate, cur_can_pushdown))) {
        LOG_WARN("failed to print predicate string", K(i), KP(children[i]));
      } else if (cur_can_pushdown && OB_FAIL(and_predicates.push_back(and_predicate))) {
        LOG_WARN("failed to push back and predicate string");
      }
    }
    if (OB_SUCC(ret) && !and_predicates.empty()) {
      can_pushdown = true;
      if (!is_root && OB_FAIL(predicate.append("("))) {
        LOG_WARN("failed to append string");
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < and_predicates.count(); ++i) {
        if (i != 0 && OB_FAIL(predicate.append(" and "))) {
          LOG_WARN("failed to append string");
        } else if (OB_FAIL(predicate.append(and_predicates.at(i).string()))) {
          LOG_WARN("failed to append string", K(and_predicates.at(i)));
        }
      }
      if (OB_SUCC(ret) && !is_root && OB_FAIL(predicate.append(")"))) {
        LOG_WARN("failed to append string");
      }
    }
  } else if (filter.is_logic_or_node()) {
    sql::ObPushdownFilterExecutor **children = filter.get_childs();
    ObSEArray<ObSqlString, 4> or_predicates;
    for (uint32_t i = 0; OB_SUCC(ret) && can_pushdown && i < filter.get_child_count(); ++i) {
      ObSqlString or_predicate;
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null child filter", K(i));
      } else if (OB_FAIL(print_predicate_string(access_exprs, file_column_exprs, *children[i],
                                                print_params, alloc, buf, length,
                                                or_predicate, can_pushdown))) {
        LOG_WARN("failed to check filter can pushdown", K(i), KP(children[i]));
      } else if (can_pushdown && OB_FAIL(or_predicates.push_back(or_predicate))) {
        LOG_WARN("failed to push back and predicate string");
      }
    }
    if (OB_SUCC(ret) && can_pushdown) {
      if (!is_root && OB_FAIL(predicate.append("("))) {
        LOG_WARN("failed to append string");
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < or_predicates.count(); ++i) {
        if (i != 0 && OB_FAIL(predicate.append(" or "))) {
          LOG_WARN("failed to append string");
        } else if (OB_FAIL(predicate.append(or_predicates.at(i).string()))) {
        LOG_WARN("failed to append string", K(or_predicates.at(i)));
        }
      }
      if (OB_SUCC(ret) && !is_root && OB_FAIL(predicate.append(")"))) {
        LOG_WARN("failed to append string");
      }
    }
  } else if (filter.is_filter_white_node()) {
    sql::ObWhiteFilterExecutor &white_filter = static_cast<sql::ObWhiteFilterExecutor &>(filter);
    ObWhiteFilterOperatorType op_type = white_filter.get_op_type();
    MirrorOdpsJniColumn *mirror_column = nullptr;
    ObString symbol("");
    ObIArray<ObExpr*> &column_exprs = white_filter.get_filter_node().column_exprs_;
    const common::ObIArray<common::ObDatum> &datums = white_filter.get_datums();
    ObObjMeta obj_meta;
    ObObj obj;
    if (WHITE_OP_EQ == op_type) {
      symbol = "=";
    } else if (WHITE_OP_LE == op_type) {
      symbol = "<=";
    } else if (WHITE_OP_LT == op_type) {
      symbol = "<";
    } else if (WHITE_OP_GE == op_type) {
      symbol = ">=";
    } else if (WHITE_OP_GT == op_type) {
      symbol = ">";
    } else if (WHITE_OP_NE == op_type) {
      symbol = "!=";
    } else if (WHITE_OP_IN == op_type) {
      symbol = "in (";
    } else if (WHITE_OP_NU == op_type) {
      symbol = "is null";
    } else if (WHITE_OP_NN == op_type) {
      symbol = "is not null";
    } else {
      can_pushdown = false;
    }
    if (OB_SUCC(ret) && can_pushdown) {
      if (OB_UNLIKELY(column_exprs.count() != 1)){
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid argument for white filter", K(column_exprs), K(datums));
      } else if (OB_FAIL(get_mirror_column(access_exprs, file_column_exprs,
                                           column_exprs.at(0), mirror_column))) {
        LOG_WARN("failed to get mirror column");
      } else if (OB_ISNULL(mirror_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null mirror column");
      } else if (WHITE_OP_NU == op_type || WHITE_OP_NN == op_type) {
        // always pushdown is [not] null
      } else if (WHITE_OP_IN == op_type) {
        if (datums.count() > 1000) {
          can_pushdown = false;
        } else if (OB_FAIL(white_filter.get_filter_node().get_filter_in_val_meta(0, obj_meta))) {
          LOG_WARN("failed to get filter val meta");
        } else if (OB_FAIL(check_type_for_pushdown(*mirror_column, obj_meta,
                                                   op_type, can_pushdown))) {
          LOG_WARN("failed to check type for pushdown");
        }
      } else if (OB_FAIL(white_filter.get_filter_node().get_filter_val_meta(obj_meta))) {
        LOG_WARN("failed to get filter val meta");
      } else if (OB_FAIL(check_type_for_pushdown(*mirror_column, obj_meta,
                                                 op_type, can_pushdown))) {
        LOG_WARN("failed to check type for pushdown");
      }
    }
    if (OB_SUCC(ret) && can_pushdown) {
      const char* prefix = "";
      if (mirror_column->type_ == OdpsType::DATETIME) {
        prefix = DATETIME_PREFIX;
      } else if (mirror_column->type_ == OdpsType::TIMESTAMP) {
        prefix = TIMESTAMP_PREFIX;
      } else if (mirror_column->type_ == OdpsType::TIMESTAMP_NTZ) {
        prefix = TIMESTAMP_NTZ_PREFIX;
      }
      if (OB_FAIL(predicate.append_fmt("`%.*s` %.*s ", mirror_column->name_.length(),
                                                       mirror_column->name_.ptr(),
                                                       symbol.length(), symbol.ptr()))) {
        LOG_WARN("failed to append sql fmt");
      } else if (WHITE_OP_NU == op_type || WHITE_OP_NN == op_type) {
        // always pushdown is [not] null
      } else if (WHITE_OP_IN == op_type) {
        for (int64_t i = 0; OB_SUCC(ret) && can_pushdown && i < datums.count(); ++i) {
          const ObDatum &ref_datum = datums.at(i);
          int64_t pos = 0;
          if (OB_FAIL(white_filter.get_filter_node().get_filter_in_val_meta(i, obj_meta))) {
            LOG_WARN("failed to get filter in val meta");
          } else if (OB_FAIL(ref_datum.to_obj(obj, obj_meta))) {
            LOG_WARN("failed to convert datum to obj");
          } else if (is_zero_time(obj)) {
            can_pushdown = false;
          } else if (mirror_column->type_ == OdpsType::DATETIME && OB_FALSE_IT(obj.set_scale(0))) {
          } else if (OB_FAIL(obj.print_sql_literal(buf, length, pos, alloc, print_params))) {
            LOG_WARN("failed to print obj value");
          } else if (OB_FAIL(predicate.append_fmt(" %s%.*s%s", prefix, static_cast<int32_t>(pos), buf,
                                                  (i < datums.count() -1) ? "," : ")"))) {
            LOG_WARN("failed to append sql fmt");
          }
        }
      } else if (mirror_column->type_ == OdpsType::BOOLEAN) {
        const ObDatum &ref_datum = datums.at(0);
        if (OB_FAIL(predicate.append(ref_datum.get_int() == 0 ? "false" : "true"))) {
          LOG_WARN("failed to append sql fmt");
        }
      } else {
        const ObDatum &ref_datum = datums.at(0);
        int64_t pos = 0;
        if (OB_FAIL(ref_datum.to_obj(obj, obj_meta))) {
          LOG_WARN("failed to convert datum to obj");
        } else if (is_zero_time(obj)) {
          can_pushdown = false;
        } else if (mirror_column->type_ == OdpsType::DATETIME && OB_FALSE_IT(obj.set_scale(0))) {
        } else if (OB_FAIL(obj.print_sql_literal(buf, length, pos, alloc, print_params))) {
          LOG_WARN("failed to print obj value");
        } else if (OB_FAIL(predicate.append_fmt("%s%.*s", prefix, static_cast<int32_t>(pos), buf))) {
          LOG_WARN("failed to append sql fmt");
        }
      }
    }
  } else {
    can_pushdown = false;
  }
  return ret;
}

int ObODPSJNITableRowIterator::check_type_for_pushdown(const MirrorOdpsJniColumn &mirror_column,
                                                       const ObObjMeta &obj_meta,
                                                       ObWhiteFilterOperatorType cmp_type,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  // odps api can not pushdown BOOLEAN for now
  // if (mirror_column.type_ == OdpsType::BOOLEAN) {
  //   if (obj_meta.is_integer_type() &&
  //       (WHITE_OP_EQ == cmp_type || WHITE_OP_NE == cmp_type)) {
  //     is_valid = true;
  //   }
  // } else
  if (mirror_column.type_ == OdpsType::TINYINT ||
             mirror_column.type_ == OdpsType::SMALLINT ||
             mirror_column.type_ == OdpsType::INT ||
             mirror_column.type_ == OdpsType::BIGINT) {
    if (obj_meta.is_integer_type()) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::FLOAT) {
    if (obj_meta.is_float()) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::DOUBLE) {
    if (obj_meta.is_double()) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::DECIMAL) {
    if (obj_meta.is_number() || obj_meta.is_decimal_int()) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::VARCHAR ||
             mirror_column.type_ == OdpsType::CHAR ||
             mirror_column.type_ == OdpsType::STRING) {
    if (obj_meta.is_string_type()) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::DATE) {
    if (ob_is_date_or_mysql_date(obj_meta.get_type())) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::DATETIME) {
    // Scale of timestamp in odps is 3, and ob-mysql is 6.
    // But pushdown predicate can only use datetime with 0 scale.
    // For consistency, we can not pushdown equal condition.

    // TODO: @yibo.tyf check datum value and pushdown equal predicate if
    // datetime doesn't have decimals.
    if (ob_is_datetime_or_mysql_datetime(obj_meta.get_type()) &&
        (WHITE_OP_LT == cmp_type || WHITE_OP_GT == cmp_type)) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::TIMESTAMP) {
    // Scale of timestamp in odps is 9, but ob-mysql only has 6.
    // For consistency, we can not pushdown equal condition.
    if (obj_meta.is_timestamp() &&
        (WHITE_OP_LT == cmp_type || WHITE_OP_GT == cmp_type)) {
      is_valid = true;
    }
  } else if (mirror_column.type_ == OdpsType::TIMESTAMP_NTZ) {
    // Scale of timestamp in odps is 9, but ob-mysql only has 6.
    // For consistency, we can not pushdown equal condition.
    if (ob_is_datetime_or_mysql_datetime(obj_meta.get_type()) &&
        (WHITE_OP_LT == cmp_type || WHITE_OP_GT == cmp_type)) {
      is_valid = true;
    }
  }
  return ret;
}

bool ObODPSJNITableRowIterator::is_zero_time(const ObObj &obj)
{
  bool is_zero = false;
  if (obj.is_date()) {
    if (obj.get_date() == ObTimeConverter::ZERO_DATE) {
      is_zero = true;
    }
  } else if (obj.is_mysql_date()) {
    if (obj.get_mysql_date() == ObTimeConverter::MYSQL_ZERO_DATE) {
      is_zero = true;
    }
  } else if (obj.is_datetime()) {
    if (obj.get_datetime() == ObTimeConverter::ZERO_DATETIME) {
      is_zero = true;
    }
  } else if (obj.is_mysql_datetime()) {
    if (obj.get_mysql_datetime() == ObTimeConverter::MYSQL_ZERO_DATETIME) {
      is_zero = true;
    }
  } else if (obj.is_timestamp()) {
    if (obj.get_timestamp() == ObTimeConverter::ZERO_DATETIME) {
      is_zero = true;
    }
  }
  return is_zero;
}

// =================== LONG FUNC END ==================================

// ------------------- ObOdpsPartitionJNIScannerMgr -------------------
/*
 * 切分任务
 */
int ObOdpsPartitionJNIScannerMgr::fetch_row_count(ObExecContext &exec_ctx, uint64_t tenant_id, const ObString &properties,
    ObIArray<ObExternalFileInfo> &external_table_files, bool &use_partition_gi)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  ObODPSJNITableRowIterator odps_driver;
  common::ObArenaAllocator arena_alloc;
  use_partition_gi = false;
  int64_t uncollect_statistics_part_cnt = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t max_parttition_count_to_collect_statistic = 5;
  if (OB_LIKELY(tenant_config.is_valid())) {
    max_parttition_count_to_collect_statistic = tenant_config->_max_partition_count_to_collect_statistic;
  }
  for (int64_t i = 0; i < external_table_files.count(); ++i) {
    if (external_table_files.at(i).file_size_ < 0 &&
        ++uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
      break;
    }
  }
  if (uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
    use_partition_gi = true;
  } else if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_meta_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init odps jni scanner", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
      const share::ObExternalFileInfo &odps_partition = external_table_files.at(i);
      /*if (0 != odps_partition.file_id_) {
        if (INT64_MAX == odps_partition.file_id_ && 0 == odps_partition.file_url_.compare("#######DUMMY_FILE#######")) {
          // do nothing
          *(const_cast<int64_t *>(&odps_partition.file_size_)) = 0;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected file id", K(ret), K(i), K(odps_partition.file_id_), K(odps_partition.part_id_));
        }
      } else */
      if (odps_partition.file_size_ >= 0) {
        // do nothing
      } else {
        int64_t record_count = 0;
        if (OB_FAIL(odps_driver.fetch_partition_row_count(odps_partition.file_url_, record_count))) {
          LOG_WARN("failed to fetch partition row count", K(ret));
        } else {
          *(const_cast<int64_t *>(&odps_partition.file_size_)) = record_count;
        }
      }
    }
  }

  return ret;
}

/*
 * 刷新数据
 */
int ObOdpsPartitionJNIScannerMgr::fetch_row_count(
    ObExecContext &exec_ctx, const ObString part_spec, const ObString &properties, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  common::ObArenaAllocator arena_alloc;
  ObODPSJNITableRowIterator odps_driver;
  if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_meta_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.fetch_partition_row_count(part_spec, row_count))) {
    LOG_WARN("failed to pull partition infos", K(ret));
  } else {
    /* do nothing */
  }
  return ret;
}

/*
 * 刷新数据
 */
int ObOdpsPartitionJNIScannerMgr::fetch_storage_row_count(
    ObExecContext &exec_ctx, const ObString part_spec, const ObString &properties,  int64_t &row_count)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  common::ObArenaAllocator arena_alloc;
  ObODPSJNITableRowIterator odps_driver;
  if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_empty_require_column())) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_part_spec(part_spec))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_meta_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.get_file_total_row_count(row_count))) {
    LOG_WARN("failed to pull partition infos", K(ret));
  } else {
    /* do nothing */
  }
  return ret;
}

int ObOdpsPartitionJNIScannerMgr::fetch_storage_api_total_task(ObExecContext &exec_ctx,
                                                               const ExprFixedArray &ext_file_column_expr,
                                                               const ObString &part_list_str,
                                                               const ObDASScanCtDef &das_ctdef,
                                                               ObDASScanRtDef *das_rtdef,
                                                               int64_t parallel,
                                                               ObString &session_str,
                                                               int64_t &split_count,
                                                               ObIAllocator &range_allocator)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  common::ObArenaAllocator arena_alloc;
  ObODPSJNITableRowIterator odps_driver;
  if (OB_FAIL(external_odps_format.load_from_string(das_ctdef.external_file_format_str_.str_, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_schema_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni schema scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_storage_api_meta_param(ext_file_column_expr, part_list_str, parallel))) {
    LOG_WARN("failed to init storage config");
  } else if (OB_FAIL(odps_driver.construct_predicate_using_white_filter(das_ctdef, das_rtdef, exec_ctx))) {
    LOG_WARN("failed to construct predicate using white filter");
  } else if (OB_FAIL(odps_driver.close_schema_scanner())) {
    LOG_WARN("failed to close schema scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_meta_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  // } else if (OB_FAIL(odps_driver.get_file_total_size(total_size))) {
  //   LOG_WARN("failed to get total phy size", K(ret));
  } else if (OB_FAIL(odps_driver.get_split_count(split_count))) {
    LOG_WARN("failed to get split count ", K(ret));
  } else if (OB_FAIL(odps_driver.get_serilize_session(range_allocator, session_str))) {
    LOG_WARN("failed to get session id ", K(ret));
  } else {
    LOG_INFO("show storage api total task", K(parallel), K(split_count));
  }

  return ret;
}

int ObOdpsPartitionJNIScannerMgr::fetch_storage_api_split_by_row(ObExecContext &exec_ctx,
                                                                 const ExprFixedArray &ext_file_column_expr,
                                                                 const ObString &part_list_str,
                                                                 const ObDASScanCtDef &das_ctdef,
                                                                 ObDASScanRtDef *das_rtdef,
                                                                 int64_t parallel,
                                                                 ObString &session_str,
                                                                 int64_t &row_count,
                                                                 ObIAllocator &range_allocator)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  common::ObArenaAllocator arena_alloc;
  ObODPSJNITableRowIterator odps_driver;
  if (OB_FAIL(external_odps_format.load_from_string(das_ctdef.external_file_format_str_.str_, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_schema_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni schema scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_storage_api_meta_param(ext_file_column_expr, part_list_str, parallel))) {
    LOG_WARN("failed to init storage config");
  } else if (OB_FAIL(odps_driver.construct_predicate_using_white_filter(das_ctdef, das_rtdef, exec_ctx))) {
    LOG_WARN("failed to construct predicate using white filter");
  } else if (OB_FAIL(odps_driver.close_schema_scanner())) {
    LOG_WARN("failed to close schema scanner", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_meta_scanner(external_odps_format.odps_format_, exec_ctx.get_my_session()))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.get_file_total_row_count(row_count))) {
    LOG_WARN("failed to get total phy size", K(ret));
  } else if (OB_FAIL(odps_driver.get_serilize_session(range_allocator, session_str))) {
    LOG_WARN("failed to get session id ", K(ret));
  } else {
    LOG_INFO("show storage api total task", K(parallel), K(row_count));
  }
  return ret;
}

int ObOdpsPartitionJNIScannerMgr::reset()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    // do nothing
  }
  inited_ = false;
  return ret;
}

// ================================================================================================================

int ObOdpsJniUploaderMgr::create_writer_params_map(ObIAllocator &alloc, const sql::ObODPSGeneralFormat &odps_format,
    const ObString &external_partition, bool is_overwrite, common::hash::ObHashMap<ObString, ObString> &odps_params_map)
{
  int ret = OB_SUCCESS;
  /* create one uploader and get session id */
  ObString access_id;
  ObString access_key;
  ObString project;
  ObString table;
  ObString region;
  ObString external_partition_c_style;
  ObString endpoint;
  ObString tunnel_endpoint;

  if (OB_FAIL(ob_write_string(alloc, odps_format.access_id_, access_id, true))) {
    LOG_WARN("failed to write access id", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.access_key_, access_key, true))) {
    LOG_WARN("failed to write access key", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.project_, project, true))) {
    LOG_WARN("failed to write project", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.table_, table, true))) {
    LOG_WARN("failed to write table", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.region_, region, true))) {
    LOG_WARN("failed to write region", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, external_partition, external_partition_c_style, true))) {
    LOG_WARN("failed to write external partition", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.endpoint_, endpoint, true))) {
    LOG_WARN("failed to write endpoint", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, odps_format.tunnel_endpoint_, tunnel_endpoint, true))) {
    LOG_WARN("failed to write tunnel endpoint", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (!odps_params_map.created() &&
        OB_FAIL(odps_params_map.create(MAX_PARAMS_SIZE, "IntoOdps", "IntoOdps", MTL_ID()))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_id"), access_id))) {
      LOG_WARN("failed to add access id to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_key"), access_key))) {
      LOG_WARN("failed to add access key to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("project"), project))) {
      LOG_WARN("failed to add project to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("table_name"), table))) {
      LOG_WARN("failed to add table name to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("region"), region))) {
      LOG_WARN("failed to add region to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("odps_url"), endpoint))) {
      LOG_WARN("failed to add endpoint to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("tunnel_url"), tunnel_endpoint))) {
      LOG_WARN("failed to add tunnel endpoint to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("public_access"), ObString::make_string("true")))) {
      LOG_WARN("failed to add public access to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("use_epoch_offset"), ObString::make_string("true")))) {
      // Note: set this param to make java side return the date/datetime format which use
      // epoch interval day/microseconds.
      LOG_WARN("failed to add use_epoch_day to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(
                   ObString::make_string("overwrite"), ObString::make_string(is_overwrite ? "true" : "false")))) {
      LOG_WARN("failed to add overwrite to params", K(ret));
    } else { /* do nothing */
    }
  }
  if (OB_SUCC(ret)) {
    if (!external_partition.empty() &&
        OB_FAIL(odps_params_map.set_refactored(ObString::make_string("partition_spec"), external_partition_c_style))) {
      LOG_WARN("failed to add partition spec to params", K(ret));
    }
  }
  return ret;
}

int ObOdpsJniUploaderMgr::init_writer_params_in_px(
    const ObString &properties, const ObString &external_partition, bool is_overwrite, int64_t parallel)
{
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_properties;
  // This entry is first time to setup java env
  if (properties.empty()) {
    // do nohting
  } else if (OB_FAIL(external_properties.load_from_string(properties, write_arena_alloc_))) {
    LOG_WARN("failed to parser external odps format", K(ret));
  } else if (OB_FAIL(external_properties.odps_format_.decrypt())) {
    LOG_WARN("failed to dump odps format", K(ret));
  } else if (sql::ObExternalFileFormat::ODPS_FORMAT != external_properties.format_type_) {
    // do nothing
  } else {
    ObJavaEnv &java_env = ObJavaEnv::getInstance();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!GCONF.ob_enable_java_env) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("java env is not enabled", K(ret));
    } else if (!java_env.is_env_inited()) {
      if (OB_FAIL(java_env.setup_java_env())) {
        LOG_WARN("failed to setup java env", K(ret));
      }
    }
    // open session
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (inited_) {
      // do nothing
    } else if (OB_FAIL(create_writer_params_map(write_arena_alloc_,
                  external_properties.odps_format_,
                  external_partition,
                  is_overwrite,
                  odps_params_map_))) {
      LOG_WARN("failed to init writer params", K(ret));
    } else if (OB_FAIL(get_writer_sid(write_arena_alloc_, odps_params_map_, sid_))) {
      LOG_WARN("failed to session id of the writer");
    } else if (OB_FAIL(odps_params_map_.set_refactored(ObString::make_string("session_id"), sid_))) {
      LOG_WARN("failed to add session id to params", K(ret));
    }

    if (OB_SUCC(ret)) {
      inited_ = true;
      init_parallel_ = parallel;
      ATOMIC_STORE(&ref_, parallel);
      LOG_TRACE("succ to init odps uploader", K(ret), K(ref_));
    }
  }
  //  成功后上层出去之后由ObSelectIntoOp::destroy析构
  // 如果不幸没走到op由析构函数兜底
  return ret;
}

int ObOdpsJniUploaderMgr::get_writer_sid(
    ObIAllocator &alloc, const common::hash::ObHashMap<ObString, ObString> &odps_params_map, ObString &sid)
{
  int ret = OB_SUCCESS;
  session_holder_ptr_ = create_odps_jni_writer();
  ObString tmp_sid;
  if (OB_ISNULL(session_holder_ptr_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create odps jni writer", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->init_params(odps_params_map))) {
    LOG_WARN("failed to init params", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->do_open())) {
    LOG_WARN("failed to do open", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->get_session_id(alloc, tmp_sid))) {
    LOG_WARN("failed to get session id", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, tmp_sid, sid, true))) {
    LOG_WARN("failed to write string", K(ret));
  }
  // 成功和失败后上层出去之后由ObSelectIntoOp::destroy析构
  return ret;
}

int ObOdpsJniUploaderMgr::get_odps_uploader_in_px(
    int task_id, const common::ObFixedArray<ObExpr *, common::ObIAllocator> &select_exprs, OdpsUploader &odps_uploader)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init write sid should after writer params odps uploader", K(ret));
  } else {
    JNIWriterPtr writer_ptr = create_odps_jni_writer();
    if (OB_FAIL(writer_ptr->init_params(odps_params_map_))) {
      LOG_WARN("failed to init params", K(ret));
    } else if (OB_FAIL(writer_ptr->do_open())) {
      LOG_WARN("failed to do open", K(ret));
    } else if (OB_FAIL(writer_ptr->do_open_record(task_id))) {
      LOG_WARN("failed to do open record", K(ret));
    }
    if (OB_SUCC(ret)) {
      odps_uploader.writer_ptr = writer_ptr;
    }
    // 成功和失败后上层出去之后由ObSelectIntoOp::destroy析构
  }
  return ret;
}

int ObOdpsJniUploaderMgr::append_block_id(long block_id) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_holder_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no session holder ", K(ret));
  } else {
    if (OB_FAIL(session_holder_ptr_->append_block_id(block_id))) {
      LOG_WARN("failed to commit ", K(ret));
    }
  }
  return ret;
}

int ObOdpsJniUploaderMgr::commit_session(int64_t num_block) {
  int ret= OB_SUCCESS;
  if (OB_ISNULL(session_holder_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no session holder ", K(ret));
  } else {
    if (OB_FAIL(session_holder_ptr_->commit_session(num_block))) {
      LOG_WARN("failed to commit ", K(ret));
    }
  }
  return ret;
}

void ObOdpsJniUploaderMgr::release_hold_session()
{
  LockGuard guard(lock_);
  if (OB_NOT_NULL(session_holder_ptr_.get())) {
    (void)session_holder_ptr_->do_close();
    session_holder_ptr_.reset();
  }
  return;
}

int ObOdpsJniUploaderMgr::reset()
{
  int ret = OB_SUCCESS;
  odps_params_map_.destroy();
  session_holder_ptr_.reset();
  inited_ = false;
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif