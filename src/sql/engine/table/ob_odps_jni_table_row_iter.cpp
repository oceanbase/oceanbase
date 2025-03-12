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

#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/connector/ob_jni_scanner.h"
#include "sql/engine/connector/ob_java_env.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "lib/charset/ob_charset.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
namespace sql
{

const char* ObODPSJNITableRowIterator::NON_PARTITION_FLAG = "__NaN__";


int ObODPSJNITableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", K(ret));
  } else if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
    LOG_WARN("failed to call ObExternalTableRowIterator::init", K(ret));
  } else if (OB_FAIL(init_jni_schema_scanner(scan_param->external_file_format_.odps_format_))) {
    // Using schema scanner to get columns
    LOG_WARN("failed to init odps jni scanner", K(ret));
  } else if (OB_FAIL(pull_columns())) {
    LOG_WARN("failed to pull column info", K(ret));
  } else if (OB_FAIL(prepare_expr())) {
    LOG_WARN("failed to prepare expr", K(ret));
  } else if (OB_FAIL(init_columns_and_types())) {
    LOG_WARN("failed to init expected columns and related types", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObODPSJNITableRowIterator::init_jni_schema_scanner(const ObODPSGeneralFormat& odps_format) {
  int ret = OB_SUCCESS;
  ObJavaEnv &java_env = ObJavaEnv::getInstance();
  // This entry is first time to setup java env
  if (!java_env.is_env_inited()) {
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
    LOG_TRACE("init odps jni scanner with params", K(ret));
    if (!odps_params_map_.created() &&
             OB_FAIL(odps_params_map_.create(MAX_PARAMS_SIZE,
                                            "JNITableParams"))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(init_required_params())) {
      LOG_WARN("failed to init required params", K(ret));
    } else { /* do nothing */ }
  }

  if (OB_SUCC(ret)) {
    odps_jni_schema_scanner_ = create_odps_jni_scanner(true);
    if (OB_FAIL(odps_jni_schema_scanner_->do_init(odps_params_map_))) {
      LOG_WARN("failed to init odps jni scanner", K(ret));
    } else {
      // NOTE!!! only open schema scanner directly before using
      if (OB_FAIL(odps_jni_schema_scanner_->do_open())) {
        LOG_WARN("failed to open odps jni schema scanner", K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_required_params() {
  int ret = OB_SUCCESS;
  if (!odps_params_map_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params map", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("access_id"),
                  odps_format_.access_id_))) {
    LOG_WARN("failed to add access id to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("access_key"),
                  odps_format_.access_key_))) {
    LOG_WARN("failed to add access key to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("project"), odps_format_.project_))) {
    LOG_WARN("failed to add project to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("table_name"), odps_format_.table_))) {
    LOG_WARN("failed to add table name to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("start_offset"),
                  ObString::make_string("0")))) {
    LOG_WARN("failed to add start offset to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("split_size"),
                  ObString::make_string("256")))) {
    LOG_WARN("failed to add split size to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("region"), odps_format_.region_))) {
    LOG_WARN("failed to add region to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("public_access"),
                  ObString::make_string("true")))) {
    LOG_WARN("failed to add public access to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("use_epoch_offset"),
                  ObString::make_string("true")))) {
    // Note: set this param to make java side return the date/datetime format which use
    // epoch interval day/microseconds.
    LOG_WARN("failed to add use_epoch_day to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("api_mode"),
                  ObString::make_string("tunnel")))) {
    // Note: current only support tunnel api.
    LOG_WARN("failed to add api_mode to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("schema"),
                  odps_format_.schema_))) {
    LOG_WARN("failed to add schema to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("quota"),
                  odps_format_.quota_))) {
    LOG_WARN("failed to add quota to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("odps_url"),
                  odps_format_.endpoint_))) {
    LOG_WARN("failed to add endpoint to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("tunnel_url"),
                  odps_format_.tunnel_endpoint_))) {
    LOG_WARN("failed to add tunnel endpoint to params", K(ret));
  } else if (OB_FAIL(odps_params_map_.set_refactored(
                  ObString::make_string("compress_option"),
                  odps_format_.compression_code_))) {
    LOG_WARN("failed to add compress code to params", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObODPSJNITableRowIterator::extract_odps_partition_specs(
    ObSEArray<ObString, 8> &partition_specs) {
  int ret = OB_SUCCESS;
  int count = partition_specs.count();
  // __NaN__ means non-partition name
  if (1 == count &&
      partition_specs.at(0).prefix_match(NON_PARTITION_FLAG)) {
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
      int record_num = ObCharset::strntoll(temp_record.ptr(),
                                            temp_record.length(), 10, &err);
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

int ObODPSJNITableRowIterator::extract_mirror_odps_columns(
    ObSEArray<ObString, 8> &mirror_columns) {
  int ret = OB_SUCCESS;
  int count = mirror_columns.count();
  if (count == 0) {
    /* do nothing */
  } else {
    for (int idx = 0; OB_SUCC(ret) && idx < count; ++idx) {
      ObString mirror_column = mirror_columns.at(idx);
      ObString name = mirror_column.split_on('#');
      ObString type = mirror_column.split_on('#');
      if (type.prefix_match("CHAR") || type.prefix_match("VARCHAR")) {
        // if type is `CHAR` or `VARCHAR`, then it will be with `length`
        ObString length = mirror_column.split_on('#');
        ObString type_size = mirror_column.split_on('#');
        ObString type_expr = mirror_column;

        int32_t num_len = -1;
        if(!length.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid length", K(ret), K(length));
        } else if (!type_size.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type size", K(ret), K(type_size));
        } else if (OB_ISNULL(type_expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type expression", K(ret), K(type_expr));
        } else {
          int length_err = 0;
          int type_size_err = 0;
          int num_len = ObCharset::strntoll(length.ptr(), length.length(), 10,
                                             &length_err);
          int num_type_size = ObCharset::strntoll(
              type_size.ptr(), type_size.length(), 10, &type_size_err);
          if (0 != length_err) {
            ret = OB_ERR_DATA_TRUNCATED;
            LOG_WARN("invalid length num", K(ret), K(length_err));
          } else if (0 != type_size_err) {
            ret = OB_ERR_DATA_TRUNCATED;
            LOG_WARN("invalid type size num", K(ret), K(type_size_err));
          } else {
            mirror_column_list_.push_back(MirrorOdpsJniColumn(
                name, type, num_len, num_type_size, type_expr));
          }
        }
      } else if (type.prefix_match("DECIMAL")) {
        ObString precision = mirror_column.split_on('#');
        ObString scale = mirror_column.split_on('#');
        ObString type_size = mirror_column.split_on('#');
        ObString type_expr = mirror_column;

        if (!precision.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid precision", K(ret), K(precision));
        } else if (!scale.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid scale", K(ret), K(scale));
        } else if (!type_size.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type size", K(ret), K(type_size));
        } else if (OB_ISNULL(type_expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type expression", K(ret), K(mirror_column));
        } else {
          int err_precision = 0;
          int err_scale = 0;
          int err_type_size = 0;
          int precision_num = ObCharset::strntoll(
              precision.ptr(), precision.length(), 10, &err_precision);
          int scale_num =
              ObCharset::strntoll(scale.ptr(), scale.length(), 10, &err_scale);
          int type_size_num = ObCharset::strntoll(
              type_size.ptr(), type_size.length(), 10, &err_type_size);
          if (0 != err_precision) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid precision num", K(ret), K(err_precision),
                     K(precision));
          } else if (0 != err_scale) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid scale num", K(ret), K(err_scale), K(scale));
          } else if (0 != err_type_size) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type size num", K(ret), K(err_type_size),
                     K(type_size));
          } else {
            mirror_column_list_.push_back(
                MirrorOdpsJniColumn(name, type, precision_num, scale_num,
                                    type_size_num, type_expr));
          }
        }
      } else if (type.prefix_match("ARRARY")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported arrary type now", K(ret));
      } else if (type.prefix_match("STRUCT")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported struct type now", K(ret));
      } else if (type.prefix_match("MAP")) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported map type now", K(ret));
      } else { // simple premitive types
        ObString type_size = mirror_column.split_on('#');
        ObString type_expr = mirror_column;

        if (!type_size.is_numeric()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid type size", K(ret), K(type_size));
        } else {
          int err_type_size = 0;
          int type_size_num = ObCharset::strntoll(
              type_size.ptr(), type_size.length(), 10, &err_type_size);
          if (0 != err_type_size) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type size num", K(ret), K(err_type_size),
                     K(type_size), K(type_expr), K(mirror_column));
          } else {
            mirror_column_list_.push_back(
                MirrorOdpsJniColumn(name, type, type_size_num, type_expr));
          }
        }
      }
    }
  }
  return ret;
}


int ObODPSJNITableRowIterator::pull_columns() {
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

    if (OB_FAIL(pull_partition_info())) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else if (OB_FAIL(odps_jni_schema_scanner_->get_odps_mirror_columns(arena_alloc_, mirror_columns))) {
      LOG_WARN("failed to get mirror columns", K(ret));
    } else if (OB_FAIL(extract_mirror_odps_columns(mirror_columns))) {
      LOG_WARN("failed to extract mirror columns", K(ret));
    } else { /* do nothing */}
  }
  return ret;
}

int ObODPSJNITableRowIterator::check_type_static(MirrorOdpsJniColumn odps_column,
                                                 const ObExpr *ob_type_expr) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_type_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    const ObString odps_type = odps_column.type_;
    const int32_t odps_type_length = odps_column.length_;
    const int32_t odps_type_precision = odps_column.precision_;
    const int32_t odps_type_scale = odps_column.scale_;

    const ObObjType ob_type = ob_type_expr->obj_meta_.get_type();
    const int32_t ob_type_length = ob_type_expr->max_length_;
    const int32_t ob_type_precision = ob_type_expr->datum_meta_.precision_;
    const int32_t ob_type_scale = ob_type_expr->datum_meta_.scale_;

    if (odps_type.prefix_match("TINYINT") ||
        odps_type.prefix_match("BOOLEAN")) {
      if (ObTinyIntType == ob_type) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("SMALLINT")) {
      if (ObSmallIntType == ob_type) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("INT")) {
      if (ObInt32Type == ob_type) {
        // odps_type to ob_type is valid
      } else if (is_oracle_mode() && ObNumberType == ob_type) {
        // odps_type to ob_type is valid
      } else if (is_oracle_mode() && ObDecimalIntType == ob_type) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("BIGINT")) {
      if (ObIntType == ob_type) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("FLOAT")) {
      if (ObFloatType == ob_type && ob_type_length == 12 && ob_type_scale == -1) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("DOUBLE")) {
      if (ObDoubleType == ob_type && ob_type_length == 23 && ob_type_scale == -1) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("DECIMAL")) {
      if (ObDecimalIntType == ob_type ||
          ObNumberType == ob_type) {
        // odps_type to ob_type is valid
        if (ob_type_precision != odps_type_precision || // in ObExpr, max_length_ is decimal type's precision
            ob_type_scale != odps_type_scale) {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                            K(ob_type_length),
                                                            K(ob_type_precision),
                                                            K(ob_type_scale),
                                                            K(odps_type_length),
                                                            K(odps_type_precision),
                                                            K(odps_type_scale));
        }
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("VARCHAR") || odps_type.prefix_match("CHAR")) {
      if (ObCharType == ob_type || ObVarcharType == ob_type) {
        // odps_type to ob_type is valid
        if (ob_type_length != odps_type_length) {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                            K(ob_type_length),
                                                            K(ob_type_precision),
                                                            K(ob_type_scale),
                                                            K(odps_type_length),
                                                            K(odps_type_precision),
                                                            K(odps_type_scale));
        }
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("STRING") || odps_type.prefix_match("BINARY")) {
      if (ObCharType == ob_type || ObVarcharType == ob_type ||
          ObTinyTextType == ob_type || ObTextType == ob_type ||
          ObLongTextType == ob_type || ObMediumTextType == ob_type) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("TIMESTAMP_NTZ")) {
      if (ob_is_datetime_or_mysql_datetime(ob_type)) {
        // odps_type to ob_type is valid
        if (ob_type_scale < 6) {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                            K(ob_type_length),
                                                            K(ob_type_precision),
                                                            K(ob_type_scale),
                                                            K(odps_type_length),
                                                            K(odps_type_precision),
                                                            K(odps_type_scale));
        }
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("TIMESTAMP")) {
      if (ObTimestampType == ob_type) {
        // odps_type to ob_type is valid
        if (ob_type_scale < 6) {
          ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
          LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
                                                            K(ob_type_length),
                                                            K(ob_type_precision),
                                                            K(ob_type_scale),
                                                            K(odps_type_length),
                                                            K(odps_type_precision),
                                                            K(odps_type_scale));
        }
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("DATETIME")) {
      if (ob_is_datetime_or_mysql_datetime(ob_type)) {
        // odps_type to ob_type is valid
        // if (ob_type_scale < 3) {
        //   ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        //   LOG_WARN("invalid precision or scale or length", K(ret), K(odps_type), K(ob_type),
        //                                                     K(ob_type_length),
        //                                                     K(ob_type_precision),
        //                                                     K(ob_type_scale),
        //                                                     K(odps_type_length),
        //                                                     K(odps_type_precision),
        //                                                     K(odps_type_scale));
        // }
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else if (odps_type.prefix_match("DATE")) {
      if (ob_is_date_or_mysql_date(ob_type)) {
        // odps_type to ob_type is valid
      } else {
        ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
        LOG_WARN("invalid odps type map to ob type", K(ret), K(odps_type), K(ob_type));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported odps type", K(ret), K(odps_type), K(ob_type));
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::prepare_expr()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->ext_file_column_exprs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", KP(scan_param_), K(ret));
  } else {
    total_column_ids_.reuse();
    target_column_ids_.reuse();

    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->ext_file_column_exprs_->count(); ++i) {
      const ObExpr *cur_expr = scan_param_->ext_file_column_exprs_->at(i); // do no check is NULL or not
      int target_idx = cur_expr->extra_ - 1;
      if (OB_UNLIKELY(cur_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL &&
                      (target_idx < 0 || target_idx >= mirror_column_list_.count()))) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("unexcepted target_idx", K(ret), K(target_idx), K(mirror_column_list_.count()));
        LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR, "wrong column index point to odps, please check the index of external$tablecol[index] and metadata$partition_list_col[index]");
      } else if (cur_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL &&
                 OB_FAIL(check_type_static(mirror_column_list_.at(target_idx), cur_expr))) {
        LOG_WARN("odps type map ob type not support", K(ret), K(target_idx));
      } else if (OB_FAIL(total_column_ids_.push_back(target_idx))) {
        LOG_WARN("failed to keep target_idx of total col", K(ret), K(target_idx));
      } else if (cur_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL &&
                 OB_FAIL(target_column_ids_.push_back(target_idx))) {
        LOG_WARN("failed to keep target_idx of external col", K(ret), K(target_idx));
      }
    }
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
  }
  return ret;
}

int ObODPSJNITableRowIterator::init_columns_and_types() {
  int ret = OB_SUCCESS;
  if (!odps_params_map_.created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to get odps params", K(ret));
  } else if (inited_columns_and_types_) {
    // do nothing
  } else if (0 == target_column_ids_.count()) {
    is_empty_external_file_exprs_ = true;
    // If external_file_exprs is empty, but total_column_ids_ is not,
    // means that the query is only partition column(s).
    if (0 != total_column_ids_.count()) {
      is_only_part_col_queried_ = true;
    }
  } else {
    ObSqlString required_fields;
    ObSqlString field_types;

    int64_t column_count = target_column_ids_.count();
    if (0 == column_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("init columns with 0 column count", K(ret));
      LOG_USER_ERROR(
          OB_INVALID_ARGUMENT,
          "init column with 0 column count, may be incorrect column");
    } else {
      for (int i = 0; OB_SUCC(ret) && i < column_count; ++i) {
        int32_t target_column_id = target_column_ids_.at(i);
        MirrorOdpsJniColumn column = mirror_column_list_.at(target_column_id);
        ObString temp_column_name(column.name_.length(), column.name_.ptr());
        ObString temp_type_expr(column.type_expr_.length(),
                                column.type_expr_.ptr());

        ObString column_name;
        ObString type_expr;
        // Note: should transfer obstring to be c_str style
        if (OB_FAIL(ob_write_string(arena_alloc_, temp_column_name, column_name,
                                    true))) {
          LOG_WARN("failed to write column name", K(ret), K(column.name_));
        } else if (OB_FAIL(ob_write_string(arena_alloc_, temp_type_expr,
                                           type_expr, true))) {
          LOG_WARN("failed to write type expr", K(ret), K(column.type_expr_));
        } else if (OB_FAIL(required_fields.append(column_name))) {
          LOG_WARN("failed to append required columns", K(ret), K(column_name),
                   K(target_column_id));
        } else if (column_count > 1 && i != column_count - 1 &&
                   OB_FAIL(required_fields.append(","))) {
          LOG_WARN("failed to append comma for column name splittor", K(ret));
        } else if (OB_FAIL(field_types.append(type_expr))) {
          LOG_WARN("failed to append field type", K(ret), K(target_column_id),
                   K(type_expr));
        } else if (column_count > 1 && i != column_count - 1 &&
                   OB_FAIL(field_types.append("#"))) {
          LOG_WARN("failed to append '#' for type expr splittor", K(ret));
        } else {
          /* do nothing */
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_empty_external_file_exprs_ || is_only_part_col_queried_) {
      // Empty external file exprs does not need to init any fields and columns.
      // do nothing
    } else {
      ObString fields;
      ObString types;
      ObString temp_required_fields = ObString::make_string("required_fields");
      ObString temp_columns_types = ObString::make_string("columns_types");
      if (OB_FAIL(ob_write_string(arena_alloc_, required_fields.string(),
                                  fields, true))) {
        LOG_WARN("failed to write required fields", K(ret));
      } else if (OB_FAIL(ob_write_string(arena_alloc_, field_types.string(),
                                         types, true))) {
        LOG_WARN("failed to write column types", K(ret));
      } else if (OB_FAIL(odps_params_map_.set_refactored(temp_required_fields,
                                                         fields, 1))) {
        LOG_WARN("failed to add required fields to params", K(ret));
      } else if (OB_FAIL(odps_params_map_.set_refactored(temp_columns_types,
                                                         types, 1))) {
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

int ObODPSJNITableRowIterator::get_next_rows(int64_t &count, int64_t capacity) {
  int ret = OB_SUCCESS;
  lib::ObMallocHookAttrGuard guard(mem_attr_);
  // column_exprs_alloc_ must not clear
  column_exprs_alloc_.clear();
  ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &file_column_exprs =
      *(scan_param_->ext_file_column_exprs_);

  int64_t read_rows = 0;
  bool eof = false;
  int64_t returned_row_cnt = 0;
  // Firstly, the state_.count_ will equal to state_.step_ which is both `0`.
  if (0 == file_column_exprs.count()) {
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task(capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      read_rounds_ += 1;
      // Means that may input the maybe count(1)/sum(1)/max(1)/min(1) and the `1`
      // is not a real column.
      count = std::min(capacity, state_.step_ - state_.count_);
      total_count_ += count;
      state_.count_ += count;
    }
  } else {
    // Normally executing to fetch data.
    if (state_.count_ >= state_.step_ && OB_FAIL(next_task(capacity))) {
      // It should close remote scanner whatever get next is iter end or other failures.
      if (OB_ITER_END != ret) {
        LOG_WARN("get next task failed", K(ret));
      }
    } else {
      if (is_empty_external_file_exprs_ || is_only_part_col_queried_) {
        read_rounds_ += 1;
        // Means that may input the maybe partition column(s) which is/are not a
        // real queried column(s).
        count = std::min(capacity, state_.step_ - state_.count_);
        total_count_ += count;
        state_.count_ += count;
        returned_row_cnt += count;
        LOG_TRACE("current read partition column(s) rows", K(ret),
                 K(state_), K(read_rounds_), K(is_empty_external_file_exprs_),
                 K(is_only_part_col_queried_), K(total_count_), K(capacity),
                 K(returned_row_cnt));
        if (0 == returned_row_cnt &&
                  (INT64_MAX == state_.step_ || state_.count_ == state_.step_)) {
          LOG_WARN("go to next task", K(ret), K(read_rounds_));
          state_.step_ = state_.count_; // go to next task
          count = 0;
        } else if (0 == returned_row_cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(total_count_),
                  K(returned_row_cnt), K(state_), K(ret));
        }
      } else {
        // Sometimes get next will return 0 row that means it end of read too.
        if (OB_FAIL(state_.odps_jni_scanner_->do_get_next(&read_rows, &eof))) {
          // It should close remote scanner whatever get next is iter end or
          // other failures.
          LOG_WARN("failed to get next rows by jni scanner", K(ret), K(eof),
                     K(read_rows));
        } else if (0 == read_rows && (INT64_MAX == state_.step_ || state_.count_ == state_.step_ || eof)) {
          state_.step_ = state_.count_; // goto get next task, next task do close
          count = 0;
        } else if (0 == read_rows) {
          // do nothing
          count = 0;
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected returned_row_cnt", K(total_count_),
                  K(returned_row_cnt), K(state_), K(ret));
        } else if (!eof) {
          state_.count_ += read_rows;
          total_count_ += read_rows;
          returned_row_cnt += read_rows;
          LOG_TRACE("current read rows", K(ret), K(eof), K(state_),
                    K(read_rows), K(total_count_), K(returned_row_cnt));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 == file_column_exprs.count()) {
    // do nothing
  } else if (!eof) {
    int64_t should_read_rows = 0;
    if (is_empty_external_file_exprs_ || is_only_part_col_queried_) {
      should_read_rows = std::min(returned_row_cnt, capacity);
      read_rounds_ += 1;
      LOG_TRACE("current already read infos in only partition column scenes",
                K(ret), K(read_rounds_), K(should_read_rows), K(read_rows),
                K(is_empty_external_file_exprs_), K(is_only_part_col_queried_),
                K(capacity), K(returned_row_cnt), K(state_), K(total_count_));
    } else {
      // Some times the read batch size not equals to capacity, so should limit
      // read rows with capacity.
      should_read_rows = std::min(read_rows, capacity);
      read_rounds_ += 1;
      LOG_TRACE("current already read infos", K(ret), K(read_rounds_),
                K(should_read_rows), K(read_rows), K(capacity),
                K(returned_row_cnt), K(state_), K(total_count_));
      // Handle data from odps jni scanner
      if (0 == should_read_rows) {
        // if read_rows is 0 means that it is unnecessary to fill column exprs.
      } else if (OB_FAIL(fill_column_exprs_(file_column_exprs, ctx,
                                            should_read_rows))) {
        (void)state_.odps_jni_scanner_->release_table(should_read_rows);
        LOG_WARN("failed to fill columns", K(ret), K(should_read_rows),
                 K(total_count_));
        // 这里fill 失败了也要release
      } else if (OB_FAIL(state_.odps_jni_scanner_->release_table(should_read_rows))) {
        LOG_WARN("failed to release table", K(ret), K(should_read_rows));
      } else {
        /* do nothing */
      }
    }

    // Default handling about partition columns
    if (0 == should_read_rows) {
      // if should_read_rows is 0 means that it is unnecessary to fill partition column
      // exprs.
    } else {
      for (int64_t column_idx = 0;
           OB_SUCC(ret) && column_idx < total_column_ids_.count();
           ++column_idx) {
        ObExpr &expr = *file_column_exprs.at(column_idx);
        if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
          ObDatum *datums = expr.locate_batch_datums(ctx);
          if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, should_read_rows))) {
            LOG_WARN("failed to init expr vector", K(ret), K(expr));
          }
          for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < should_read_rows;
               ++row_idx) {
            int64_t loc_idx = file_column_exprs.at(column_idx)->extra_ - 1;
            if (OB_UNLIKELY(loc_idx < 0 ||
                            loc_idx >= state_.part_list_val_.get_count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexcepted loc_idx", K(ret), K(loc_idx),
                       K(state_.part_list_val_.get_count()),
                       KP(&state_.part_list_val_));
            } else if (state_.part_list_val_.get_cell(loc_idx).is_null()) {
              datums[row_idx].set_null();
            } else {
              CK(OB_NOT_NULL(datums[row_idx].ptr_));
              OZ(datums[row_idx].from_obj(
                  state_.part_list_val_.get_cell(loc_idx)));
            }
          }
        }
      }
    }

    if (0 == should_read_rows) {
      // if should_read_rows is 0 means that it is unnecessary to do data copy.
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_idx(0);
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
        file_column_exprs.at(i)->set_evaluated_flag(ctx);
      }
      for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
        ObExpr *column_expr = column_exprs_.at(i);
        ObExpr *column_convert_expr =
            scan_param_->ext_column_convert_exprs_->at(i);
        if (OB_FAIL(column_convert_expr->eval_batch(ctx, *bit_vector_cache_,
                                                    should_read_rows))) {
          LOG_WARN("failed to eval batch", K(ret), K(i), K(should_read_rows),
                   K(odps_format_.table_), K(target_column_ids_),
                   K(total_column_ids_));
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
  }
  if (!eof && OB_SUCC(ret) && OB_FAIL(calc_exprs_for_rowid(count))) {
    LOG_WARN("failed to calc exprs for rowid", K(ret), K(count));
  }
  if (OB_FAIL(ret)) { // unlikely
    if (OB_ITER_END != ret) {
      // END ABNORMALLY CLEAN TASK IN THIS ROUND
      if (OB_NOT_NULL(state_.odps_jni_scanner_)) {
        (void)state_.odps_jni_scanner_->do_close();
      }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::next_task(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  task_alloc_.reset();
  int64_t task_idx = state_.task_idx_;
  int64_t start = 0;
  int64_t step = 0;
  if (++task_idx >= scan_param_->key_ranges_.count()) {
    ret = OB_ITER_END;
    LOG_INFO("odps table iter end", K(ret), K(total_count_), K(state_),
             K(task_idx), K_(read_rounds));
    // If the external file exprs is empty, then can cal related size direclty.
    // And does not need to open scanner.
    // Note: if enter the last round, then the scanner will open again to check rows which
    // is already end of file.
    if (!(is_empty_external_file_exprs_ || is_only_part_col_queried_) &&
        OB_NOT_NULL(state_.odps_jni_scanner_)) {
      // END NORMALLY
      int tmp_ret = state_.odps_jni_scanner_->do_close();
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("failed to close the jni scanner for this state", K(ret));
      }
    }
  } else {
    if (OB_FAIL(ObExternalTableUtils::resolve_odps_start_step(scan_param_->key_ranges_.at(task_idx),
                                                              ObExternalTableUtils::LINE_NUMBER,
                                                              start,
                                                              step))) {
      LOG_WARN("failed to resolve range in external table", K(ret), K(start), K(step));
    } else {
      const ObString &part_spec =
          scan_param_->key_ranges_.at(task_idx)
              .get_start_key()
              .get_obj_ptr()[ObExternalTableUtils::FILE_URL]
              .get_string();
      int64_t part_id = scan_param_->key_ranges_.at(task_idx)
                            .get_start_key()
                            .get_obj_ptr()[ObExternalTableUtils::PARTITION_ID]
                            .get_int();
      if (part_spec.compare("#######DUMMY_FILE#######") == 0) {
        ret = OB_ITER_END;
        LOG_TRACE("iterator of odps jni scanner is end with dummy file", K(ret));
      } else if (OB_NOT_NULL(state_.odps_jni_scanner_) &&
                             OB_FAIL(state_.odps_jni_scanner_->do_close())) {
        // RELEASE LAST TASK CORRECTLY
        LOG_WARN("failed to close the scanner of previous task", K(ret));
      } else {
        ObEvalCtx &ctx = scan_param_->op_->get_eval_ctx();
        ObPxSqcHandler *sqc = ctx.exec_ctx_.get_sqc_handler();// if sqc is not NULL, odps read is in px plan
        if (OB_ISNULL(sqc) || !sqc->get_sqc_ctx().gi_pump_.is_odps_scanner_mgr_inited()) {
          state_.odps_jni_scanner_ = create_odps_jni_scanner();
          state_.is_from_gi_pump_ = false;
          LOG_TRACE("succ to create jni scanner without GI", K(ret), K(part_id), KP(sqc), K(state_.is_from_gi_pump_));
        } else {
          state_.odps_jni_scanner_ = create_odps_jni_scanner();
          // NOTE: mock as px mode
          state_.is_from_gi_pump_ = true;
        }
      }
      if (OB_FAIL(ret)) {
        /* do nothing */
      } else if (OB_ISNULL(state_.odps_jni_scanner_.get())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret), K(state_.is_from_gi_pump_));
      } else if (OB_FAIL(calc_file_partition_list_value(
                     part_id, task_alloc_, state_.part_list_val_))) {
        LOG_WARN("failed to calc parttion list value", K(part_id), K(ret));
      } else {
        int64_t real_time_partition_row_count = 0;
        if (OB_ISNULL(odps_jni_schema_scanner_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("could not init schema scanner which is null", K(ret));
        } else if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_row_count(
                task_alloc_, part_spec, real_time_partition_row_count))) {
          LOG_WARN("failed to get partition row count", K(ret));
        } else if (start >= real_time_partition_row_count) {
          start = real_time_partition_row_count;
          step = 0;
          LOG_WARN("odps iter end", K(ret), K(part_id), K(state_.start_),
                   K(real_time_partition_row_count));
        } else if (INT64_MAX == step ||
                   start + step > real_time_partition_row_count) {
          step = real_time_partition_row_count - start;
        }
        if (OB_SUCC(ret)) {
          state_.task_idx_ = task_idx;
          state_.part_id_ = part_id;
          state_.start_ = start;
          state_.step_ = step;
          state_.count_ = 0;
          state_.part_spec_ = part_spec;
          LOG_TRACE("get a new task of odps jni iterator", K(ret),
                   K(batch_size_), K(state_));
          if (is_empty_external_file_exprs_ || is_only_part_col_queried_) {
            // If the external file exprs is empty, then can calc related size
            // direclty. And does not need to open scanner. do nothing.
            LOG_TRACE(
                "enter in empty external exprs or only partion column queried",
                K(ret), K(is_empty_external_file_exprs_),
                K(is_only_part_col_queried_), K(state_));
          } else {
            if (OB_SUCC(ret) && -1 == batch_size_) { // exec once only
              batch_size_ = eval_ctx.max_batch_size_;
              LOG_WARN("setup batch size for odps jni iterator", K(ret),
                       K(batch_size_));
              // Because every scanner may change the batch size, not normal
              // default `256`.
              state_.odps_jni_scanner_->set_batch_size(batch_size_);
              // Setup scanner is running in debug mode
              // state_.odps_jni_scanner_->set_debug_mode(true);
            }

            if (OB_SUCC(ret) &&
                (!odps_params_map_.created() || odps_params_map_.empty())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("odps params map should be created or not empty",
                       K(ret));
            } else {
              // update `batch_size`, `start` and `step` to jni scanner
              ObFastFormatInt fsf(start);
              ObFastFormatInt fss(step);
              ObFastFormatInt fsc(capacity);

              ObString start_offset_str = ObString::make_string(fsf.str());
              ObString split_size_str = ObString::make_string(fss.str());
              ObString capacity_str = ObString::make_string(fsc.str());

              LOG_INFO("display variables of odps jni iterator", K(ret),
                       K(start_offset_str), K(split_size_str), K(capacity_str));

              ObString temp_start_offset =
                  ObString::make_string("start_offset");
              ObString temp_split_size = ObString::make_string("split_size");
              ObString temp_partition_spec =
                  ObString::make_string("partition_spec");
              ObString temp_capacity = ObString::make_string("expect_read_rows");

              ObString partition_spec;
              ObString start_offset;
              ObString split_size;
              ObString expect_read_rows;
              if (OB_FAIL(ob_write_string(task_alloc_, part_spec,
                                          partition_spec, true))) {
                LOG_WARN("failed to write c_str style for partition spec", K(ret));
              } else if (OB_FAIL(ob_write_string(task_alloc_, start_offset_str,
                                          start_offset, true))) {
                LOG_WARN("failed to write c_str style for start offset", K(ret));
              } else if (OB_FAIL(ob_write_string(task_alloc_, split_size_str,
                                          split_size, true))) {
                LOG_WARN("failed to write c_str style for split size", K(ret));
              } else if (OB_FAIL(ob_write_string(task_alloc_, capacity_str,
                                          expect_read_rows, true))) {
                LOG_WARN("failed to write c_str style for expect read rows", K(ret));
              } else if (!start_offset.is_numeric()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("configure start offset is not number", K(ret),
                         K(start_offset));
              } else if (!split_size.is_numeric()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("configure split size is not number", K(ret),
                         K(split_size));
              } else if (!expect_read_rows.is_numeric()) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("configure expect read rows is not number", K(ret),
                         K(expect_read_rows));
              } else if (OB_FAIL(odps_params_map_.set_refactored(
                             temp_start_offset, start_offset, 1))) {
                LOG_WARN("failed to add start offset", K(ret));
              } else if (OB_FAIL(odps_params_map_.set_refactored(
                             temp_split_size, split_size, 1))) {
                LOG_WARN("failed to add split size(step)", K(ret));
              } else if (OB_FAIL(odps_params_map_.set_refactored(
                             temp_partition_spec, partition_spec, 1))) {
                LOG_WARN("failed to add partition spec(step)", K(ret));
              } else if (OB_FAIL(odps_params_map_.set_refactored(
                             temp_capacity, expect_read_rows, 1))) {
                LOG_WARN("failed to add expect read rows", K(ret));
              } else { /* do nothing */
              }
            }

            if (OB_SUCC(ret)) {
              if (!state_.odps_jni_scanner_->is_inited()) {
                // If the jni scanner is total new fresh, then init before open.
                if (OB_FAIL(
                        state_.odps_jni_scanner_->do_init(odps_params_map_))) {
                  LOG_WARN("failed to init next odps jni scanner", K(ret));
                } else { /* do nothing */
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("jni scanner should not be inited", K(ret));
              }
            }

            if (OB_SUCC(ret) && OB_FAIL(state_.odps_jni_scanner_->do_open())) {
              LOG_WARN("failed to open next odps jni scanner", K(ret),
                       K(task_idx), K(part_id), K(start), K(step),
                       K(part_spec));
            } else { /* do nothing */
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_column_(ObEvalCtx &ctx, const ObExpr &expr,
                                            const MirrorOdpsJniColumn &odps_column,
                                            int64_t num_rows, int32_t column_idx) {
  int ret = OB_SUCCESS;
  // Column meta is mapping by
  // `com.oceanbase.jni.connector.OffHeapTable#getMetaNativeAddress`
  JniScanner::JniTableMeta& column_meta = state_.odps_jni_scanner_->get_jni_table_meta();
  long null_map_ptr = column_meta.next_meta_as_long();
  const ObString &odps_type = odps_column.type_;
  int32_t type_size = odps_column.type_size_;

  if (0 == null_map_ptr) {
    // com.oceanbase.jni.connector.ColumnType.Type#UNSUPPORTED will set column
    // address as 0
    ret = OB_JNI_ERROR;
    LOG_WARN("Unsupported type in java side", K(ret), K(odps_type));
  } else {
    ObDatum *datums = expr.locate_batch_datums(ctx);
    ObObjType type = expr.obj_meta_.get_type();

    if (OB_FAIL(expr.init_vector_for_write(ctx, VEC_UNIFORM, num_rows))) {
      LOG_WARN("failed to init expr vector", K(ret), K(expr));
    } else if (odps_type.prefix_match("BOOLEAN")) {
      // Hold the column address
      long column_addr = column_meta.next_meta_as_long();

      if ((ObTinyIntType == type || ObSmallIntType == type ||
           ObMediumIntType == type || ObInt32Type == type ||
           ObIntType == type) &&
          !is_oracle_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            bool *v = reinterpret_cast<bool *>(column_addr + row_idx);
            datums[row_idx].set_int(*v);
          }
        }
      } else if (ObNumberType == type && is_oracle_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            bool *v = reinterpret_cast<bool *>(column_addr + row_idx);
            int64_t in_val = *v;
            number::ObNumber nmb;
            if (OB_FAIL(nmb.from(in_val, column_exprs_alloc_))) {
              LOG_WARN("failed to cast int value to number", K(ret), K(in_val),
                       K(row_idx), K(column_idx));
            } else {
              datums[row_idx].set_number(nmb);
            }
          }
        }
      } else if (is_oracle_mode() && ObDecimalIntType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            bool *v = reinterpret_cast<bool *>(column_addr + row_idx);
            int64_t in_val = *v;

            ObDecimalInt *decint = nullptr;
            int32_t int_bytes = 0;
            ObDecimalIntBuilder tmp_alloc;
            ObScale out_scale = expr.datum_meta_.scale_;
            ObScale in_scale = 0;
            ObPrecision out_prec = expr.datum_meta_.precision_;
            ObPrecision in_prec =
                ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                    .get_precision();
            const static int64_t DECINT64_MAX =
                get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
            if (in_prec > MAX_PRECISION_DECIMAL_INT_64 && in_val < DECINT64_MAX) {
              in_prec = MAX_PRECISION_DECIMAL_INT_64;
            }
            if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint, int_bytes,
                                           in_prec))) {
              LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx),
                       K(column_idx));
            } else if (ObDatumCast::need_scale_decimalint(
                           in_scale, in_prec, out_scale, out_prec)) {
              ObDecimalIntBuilder res_val;
              if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                      decint, int_bytes, in_scale, out_scale, out_prec,
                      expr.extra_, res_val))) {
                LOG_WARN("scale decimal int failed", K(ret), K(row_idx),
                         K(column_idx));
              } else {
                datums[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                                res_val.get_int_bytes());
              }
            } else {
              datums[row_idx].set_decimal_int(decint, int_bytes);
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("TINYINT") ||
               odps_type.prefix_match("SMALLINT") ||
               odps_type.prefix_match("INT") ||
               odps_type.prefix_match("BIGINT")) {
      if ((ObTinyIntType == type || ObSmallIntType == type ||
           ObMediumIntType == type || ObInt32Type == type ||
           ObIntType == type) && !is_oracle_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            if (odps_type.prefix_match("TINYINT")) {
              int8_t *v = reinterpret_cast<int8_t *>(column_addr + 1L * row_idx);
              datums[row_idx].set_int(*v);
            } else if (odps_type.prefix_match("SMALLINT")) {
              int16_t *v = reinterpret_cast<int16_t *>(column_addr + 2L * row_idx);
              datums[row_idx].set_int(*v);
            } else if (odps_type.prefix_match("INT")) {
              int32_t *v = reinterpret_cast<int32_t *>(column_addr + 4L * row_idx);
              datums[row_idx].set_int(*v);
            } else if (odps_type.prefix_match("BIGINT")) {
              int64_t *v = reinterpret_cast<int64_t *>(column_addr + 8L * row_idx);
              datums[row_idx].set_int(*v);
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid odps type to get int value", K(ret), K(odps_type));
            }
          }
        }
      } else if (is_oracle_mode() && ObNumberType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          }else {
            int64_t in_val;
            if (odps_type.prefix_match("TINYINT")) {
              int8_t *v = reinterpret_cast<int8_t *>(column_addr + 1L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("SMALLINT")) {
              int16_t *v = reinterpret_cast<int16_t *>(column_addr + 2L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("INT")) {
              int32_t *v = reinterpret_cast<int32_t *>(column_addr + 4L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("BIGINT")) {
              int64_t *v = reinterpret_cast<int64_t *>(column_addr + 8L * row_idx);
              in_val = *v;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invali odps type to get int value", K(ret), K(odps_type));
            }

            if (OB_SUCC(ret)) {
              number::ObNumber nmb;
              if (OB_FAIL(nmb.from(in_val, column_exprs_alloc_))) {
                LOG_WARN("failed to cast int value to number", K(ret),
                         K(in_val), K(row_idx), K(column_idx));
              } else {
                datums[row_idx].set_number(nmb);
              }
            }
          }
        }
      } else if (is_oracle_mode() && ObDecimalIntType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            int64_t in_val;
            if (odps_type.prefix_match("TINYINT")) {
              int8_t *v = reinterpret_cast<int8_t *>(column_addr + 1L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("SMALLINT")) {
              int16_t *v = reinterpret_cast<int16_t *>(column_addr + 2L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("INT")) {
              int32_t *v = reinterpret_cast<int32_t *>(column_addr + 4L * row_idx);
              in_val = *v;
            } else if (odps_type.prefix_match("BIGINT")) {
              int64_t *v = reinterpret_cast<int64_t *>(column_addr + 8L * row_idx);
              in_val = *v;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invali odps type to get int value", K(ret), K(odps_type));
            }

            if (OB_SUCC(ret)) {
              ObDecimalInt *decint = nullptr;
              int32_t int_bytes = 0;
              ObDecimalIntBuilder tmp_alloc;
              ObScale out_scale = expr.datum_meta_.scale_;
              ObScale in_scale = 0;
              ObPrecision out_prec = expr.datum_meta_.precision_;
              ObPrecision in_prec =
                  ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type]
                      .get_precision();
              const static int64_t DECINT64_MAX =
                  get_scale_factor<int64_t>(MAX_PRECISION_DECIMAL_INT_64);
              if (in_prec > MAX_PRECISION_DECIMAL_INT_64 &&
                  in_val < DECINT64_MAX) {
                in_prec = MAX_PRECISION_DECIMAL_INT_64;
              }
              if (OB_FAIL(wide::from_integer(in_val, tmp_alloc, decint,
                                             int_bytes, in_prec))) {
                LOG_WARN("from_integer failed", K(ret), K(in_val), K(row_idx),
                         K(column_idx));
              } else if (ObDatumCast::need_scale_decimalint(
                             in_scale, in_prec, out_scale, out_prec)) {
                ObDecimalIntBuilder res_val;
                if (OB_FAIL(ObDatumCast::common_scale_decimalint(
                        decint, int_bytes, in_scale, out_scale, out_prec,
                        expr.extra_, res_val))) {
                  LOG_WARN("scale decimal int failed", K(ret), K(row_idx),
                           K(column_idx));
                } else {
                  datums[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                                  res_val.get_int_bytes());
                }
              } else {
                datums[row_idx].set_decimal_int(decint, int_bytes);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("FLOAT")) {
      if (ObFloatType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            float *v = reinterpret_cast<float *>(column_addr + 4L * row_idx);
            datums[row_idx].set_float(*v);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("DOUBLE")) {
      if (ObDoubleType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char *null_value = reinterpret_cast<char *>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            double *v = reinterpret_cast<double *>(column_addr + 8L * row_idx);
            datums[row_idx].set_double(*v);
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("DECIMAL")) {
      if (ObDecimalIntType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);
        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            // Note: the type size is needed to get offset of decimal value and
            // decimal value could be decimal32, decimal64, decimal128
            ObDecimalIntBuilder res_val;
            ObDecimalInt *decint = nullptr;
            if (4 == type_size) {
              int32_t *v = reinterpret_cast<int32_t *>(column_addr + 1L * type_size * row_idx);
              res_val.from(*v);
            } else if (8 == type_size) {
              int64_t *v = reinterpret_cast<int64_t *>(column_addr + 1L * type_size * row_idx);
              res_val.from(*v);
            } else if (16 == type_size) {
              int128_t *v = reinterpret_cast<int128_t *>(column_addr + 1L * type_size * row_idx);
              res_val.from(*v);
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("not supported type size", K(ret), K(type_size));
            }
            datums[row_idx].set_decimal_int(res_val.get_decimal_int(),
                                            res_val.get_int_bytes());
          }
        }
      } else if (ObNumberType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);

          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            // Note: the type size is needed to get offset of decimal value and
            // decimal value could be decimal32, decimal64, decimal128
            char *v = reinterpret_cast<char *>(column_addr + 1L * type_size * row_idx);
            ObString in_str(v);
            number::ObNumber nmb;
            ObNumStackOnceAlloc tmp_alloc;
            if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_number_wrap(
                    expr, in_str, tmp_alloc, nmb))) {
              LOG_WARN("cast string to number failed", K(ret), K(row_idx),
                       K(column_idx));
            } else {
              datums[row_idx].set_number(nmb);
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("CHAR") || odps_type.prefix_match("VARCHAR")) {
      if (ObCharType == type || ObVarcharType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        int *offsets = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
        char* base_addr = reinterpret_cast<char*>(column_meta.next_meta_as_ptr());

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            ObObjType in_type;
            ObObjType out_type;
            if (ObCharType == type) {
              in_type = ObCharType;
              out_type = ObCharType;
            } else if (ObVarcharType == type) {
              in_type = ObVarcharType;
              out_type = ObVarcharType;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("Unsupported type to fill column", K(ret), K(type), K(column_idx), K(row_idx));
            }

            ObString temp_str;
            ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN; // odps's collation
            ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
            if (OB_SUCC(ret)) {
              if (num_rows == 0) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid num rows to get char offsets", K(ret));
              } else if (row_idx == 0) {
                int length = offsets[row_idx];
                temp_str = ObString(length, base_addr);
                base_addr += length; // Note: remember to add length to get next
              } else {
                int length = offsets[row_idx] - offsets[row_idx - 1];
                temp_str = ObString(length, base_addr);
                base_addr += length; // Note: remember to add length to get next
              }
            }
            ObString in_str;
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              bool has_set_res = false;
              ObCharsetType out_charset =
                  common::ObCharset::charset_type_by_coll(out_cs_type);
              if (CHARSET_UTF8MB4 == out_charset || CHARSET_BINARY == out_charset) {
                datums[row_idx].set_string(in_str);
              } else if (OB_FAIL(oceanbase::sql::ObOdpsDataTypeCastUtil::
                                     common_string_string_wrap(
                                         expr, in_type, in_cs_type, out_type,
                                         out_cs_type, in_str, ctx,
                                         datums[row_idx], has_set_res))) {
                LOG_WARN("cast string to string failed", K(ret), K(row_idx),
                         K(column_idx));
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("STRING") ||
               odps_type.prefix_match("BINARY")) {
      if (ObVarcharType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        int *offsets = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
        char *base_addr = reinterpret_cast<char *>(column_meta.next_meta_as_ptr());

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            ObObjType in_type = ObVarcharType;
            ObObjType out_type = ObVarcharType;
            ObCollationType in_cs_type = odps_type.prefix_match("STRING")
                                             ? CS_TYPE_UTF8MB4_BIN
                                             : CS_TYPE_BINARY;
            ObCollationType out_cs_type = expr.datum_meta_.cs_type_;

            ObString temp_str;
            if (num_rows == 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid num rows to get string/binary offsets", K(ret), K(row_idx));
            } else if (row_idx == 0) {
              int length = offsets[row_idx];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                temp_str = ObString(length, base_addr);
                base_addr += length;
              }
            } else {
              int length = offsets[row_idx] - offsets[row_idx - 1];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                temp_str = ObString(length, base_addr);
                base_addr += length;
              }
            }
            ObString in_str;
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              bool has_set_res = false;
              ObCharsetType out_charset =
                  common::ObCharset::charset_type_by_coll(out_cs_type);
              if (CHARSET_UTF8MB4 == out_charset ||
                  CHARSET_BINARY == out_charset) {
                datums[row_idx].set_string(in_str);
              } else if (OB_FAIL(
                             ObOdpsDataTypeCastUtil::common_string_string_wrap(
                                 expr, in_type, in_cs_type, out_type,
                                 out_cs_type, in_str, ctx, datums[row_idx],
                                 has_set_res))) {
                LOG_WARN("cast string to string failed", K(ret), K(row_idx),
                         K(column_idx));
              }
            }
          }
        }
      } else if (ObTinyTextType == type || ObTextType == type ||
                 ObLongTextType == type || ObMediumTextType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        int *offsets = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
        char *base_addr = reinterpret_cast<char *>(column_meta.next_meta_as_ptr());

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            ObObjType in_type = ObVarcharType;
            ObString temp_str;
            if (num_rows == 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid num rows to get string/binary offsets", K(ret), K(row_idx));
            } else if (row_idx == 0) {
              int length = offsets[row_idx];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                temp_str = ObString(length, base_addr);
                base_addr += length;
              }
            } else {
              int length = offsets[row_idx] - offsets[row_idx - 1];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                temp_str = ObString(length, base_addr);
                base_addr += length;
              }
            }
            ObString in_str;
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ob_write_string(column_exprs_alloc_, temp_str, in_str))) {
              LOG_WARN("failed to write str", K(ret), K(temp_str), K(in_str));
            } else {
              ObCollationType in_cs_type = odps_type.prefix_match("STRING")
                                               ? CS_TYPE_UTF8MB4_BIN
                                               : CS_TYPE_BINARY;
              if (OB_FAIL(ObOdpsDataTypeCastUtil::common_string_text_wrap(
                      expr, in_str, ctx, NULL, datums[row_idx], in_type,
                      in_cs_type))) {
                LOG_WARN("cast string to text failed", K(ret), K(row_idx),
                         K(column_idx));
              }
            }
          }
        }
      } else if (ObRawType == type) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        int *offsets = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
        char *base_addr = reinterpret_cast<char *>(column_meta.next_meta_as_ptr());

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);

          if (is_null) {
            datums[row_idx].set_null();
          } else {
            ObString in_str;
            if (num_rows == 0) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid num rows to get string/binary offsets", K(ret), K(row_idx));
            } else if (row_idx == 0) {
              int length = offsets[row_idx];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                ObString temp_str{length, base_addr};
                in_str = temp_str;
                base_addr += length;
              }
            } else {
              int length = offsets[row_idx] - offsets[row_idx - 1];
              if (length > expr.max_length_) {
                ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
                LOG_WARN("unexpected data length", K(ret), K(length),
                         K(expr.max_length_), K(type));
              } else {
                ObString temp_str{length, base_addr};
                in_str = temp_str;
                base_addr += length;
              }
            }
            if (OB_SUCC(ret)) {
              bool has_set_res = false;
              if (OB_FAIL(ObDatumHexUtils::hextoraw_string(
                      expr, in_str, ctx, datums[row_idx], has_set_res))) {
                LOG_WARN("cast string to raw failed", K(ret), K(row_idx),
                        K(column_idx));
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("DATETIME") ||
               odps_type.prefix_match("TIMESTAMP_NTZ") ||
               odps_type.prefix_match("TIMESTAMP")) {
      // TODO(bitao): checking timestamp
      if (ob_is_datetime_or_mysql_datetime_tc(type) && is_mysql_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          int64_t res_offset = 0;
          // NOTE: should not add offset in table scan
          // int32_t tmp_offset = 0;
          // if (OB_FAIL(ctx.exec_ctx_.get_my_session()
          //                 ->get_timezone_info()
          //                 ->get_timezone_offset(0, tmp_offset))) {
          //   LOG_WARN("failed to get timezone offset", K(ret));
          // } else {
          //   res_offset = SEC_TO_USEC(tmp_offset);
          // }
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            // Note: datetime value already handled as nanoseconds from
            // 1970-01-01 00:00:00 UTC in java side.
            int64_t *v = reinterpret_cast<int64_t *>(column_addr + 8L * row_idx);
            int64_t datetime = *v / 1000 + res_offset;
            if (ob_is_mysql_datetime(type)) {
              ObMySQLDateTime mdatetime;
              ret = ObTimeConverter::datetime_to_mdatetime(datetime, mdatetime);
              datums[row_idx].set_mysql_datetime(mdatetime);
            } else {
              datums[row_idx].set_datetime(datetime);
            }
          }
        }
      } else if (false && ObTimestampNanoType == type && is_oracle_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            // TODO(bitao): add datums value?
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("DATE")) {
      // DATE should be after DATETIME
      if (ob_is_date_or_mysql_date(type) && is_mysql_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
            int *v = reinterpret_cast<int*>(column_addr + 4L * row_idx);
            // Note: java side already use epoch day interval, so can set date directly.
            int32_t date = *v;
            if (ob_is_mysql_date_tc(type)) {
              ObMySQLDate mdate;
              ObTimeConverter::date_to_mdate(date, mdate);
              datums[row_idx].set_mysql_date(mdate);
            } else {
              datums[row_idx].set_date(date);
            }
          }
        }
      } else if (false && ObDateTimeType == type && is_oracle_mode()) {
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
        batch_info_guard.set_batch_idx(0);

        // Hold the column address
        long column_addr = column_meta.next_meta_as_long();

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
          batch_info_guard.set_batch_idx(row_idx);
          char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
          bool is_null = (*null_value == 1);
          if (is_null) {
            datums[row_idx].set_null();
          } else {
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(type), K(column_idx));
      }
    } else if (odps_type.prefix_match("JSON")) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
      batch_info_guard.set_batch_idx(0);

      int *offsets = reinterpret_cast<int *>(column_meta.next_meta_as_ptr());
      char* base_addr = reinterpret_cast<char*>(column_meta.next_meta_as_ptr());

      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < num_rows; ++row_idx) {
        batch_info_guard.set_batch_idx(row_idx);
        char* null_value = reinterpret_cast<char*>(null_map_ptr + row_idx);
        bool is_null = (*null_value == 1);
        if (is_null) {
          datums[row_idx].set_null();
        } else {
          ObString in_str;
          if (num_rows == 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid num rows to get json offsets", K(ret), K(row_idx));
          } else if (row_idx == 0) {
            int length = offsets[row_idx];
            if (length > expr.max_length_) {
              ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
              LOG_WARN("unexpected data length", K(ret), K(length),
                        K(expr.max_length_), K(type));
            } else {
              ObString temp_str{length, base_addr};
              in_str = temp_str;
              base_addr += length;
            }
          } else {
            int length = offsets[row_idx] - offsets[row_idx - 1];
            if (length > expr.max_length_) {
              ret = OB_EXTERNAL_ODPS_COLUMN_TYPE_MISMATCH;
              LOG_WARN("unexpected data length", K(ret), K(length),
                        K(expr.max_length_), K(type));
            } else {
              ObString temp_str{length, base_addr};
              in_str = temp_str;
              base_addr += length;
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected odps type", K(ret), K(odps_type));
    }
  }
  return ret;
}

int ObODPSJNITableRowIterator::fill_column_exprs_(
    const ExprFixedArray &column_exprs, ObEvalCtx &ctx, int64_t num_rows) {
  int ret = OB_SUCCESS;
  int32_t release_index = 0;
  for (int64_t column_idx = 0;
       OB_SUCC(ret) && column_idx < total_column_ids_.count(); ++column_idx) {
    ObExpr &expr = *column_exprs.at(column_idx); // do not check null ptr
    if (expr.type_ == T_PSEUDO_PARTITION_LIST_COL) {
      // do nothing, parititon columns will handle in alone.
    } else {
      // Note: column idx is using for ob side column, target index is using for java side
      int64_t target_index = total_column_ids_.at(column_idx);

      if (target_index < 0 || target_index >= mirror_column_list_.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid target index for mirror column", K(ret), K(target_index));
      } else {
        MirrorOdpsJniColumn column = mirror_column_list_.at(target_index);

        // NOTE: the column data will only get queried column, so java side will only init
        // quired column and release column should also release queried fields.
        // And the queried fields index will start from 0 to queried column length.

        if (OB_FAIL(fill_column_(ctx, expr, column, num_rows, column_idx))) {
          (void)state_.odps_jni_scanner_->release_column(release_index);
          LOG_WARN("failed to fill column", K(ret), K(column), K(column_idx),
                   K(target_index));
        } else if (OB_FAIL(state_.odps_jni_scanner_->release_column(
                       release_index))) {
          LOG_WARN("failed to release column", K(ret), K(target_index), K(release_index));
        } else {
          release_index += 1;
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
    OZ (file_id_expr_->init_vector_for_write(eval_ctx, VEC_FIXED, read_count));
    for (int i = 0; OB_SUCC(ret) && i < read_count; i++) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_id_expr_->get_vector(eval_ctx));
      // file_id_ is mapping to state_.part_id_ in odps.
      vec->set_int(i, state_.part_id_);
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


int ObODPSJNITableRowIterator::pull_partition_info() {
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
    if (OB_FAIL(odps_jni_schema_scanner_->get_odps_partition_specs(arena_alloc_, partition_specs))) {
      LOG_WARN("failed to get partition specs", K(ret));
    } else if (OB_FAIL(extract_odps_partition_specs(partition_specs))) {
      LOG_WARN("failed to extract partition specs", K(ret));
    } else { /* do nothing */}
  }
  return ret;
}


void ObODPSJNITableRowIterator::reset()
{
  state_.reuse(); // reset state_ to initial values for rescan
  // odps_params_map_.reuse();
  // Sometimes state_.odps_jni_scanner_ maybe not inited correctly
  task_alloc_.clear();
  column_exprs_alloc_.clear();

  real_row_idx_ = 0;
  read_rounds_ = 0;
  total_count_ = 0;
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

int ObODPSJNITableRowIterator::fecth_partition_row_count(const ObString &part_spec, int64_t &row_count)
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
    } else { /* do nothing */}
  }
  return ret;
}

// ------------------- ObOdpsPartitionJNIScannerMgr -------------------

int ObOdpsPartitionJNIScannerMgr::fetch_row_count(uint64_t tenant_id,
                                                  const ObIArray<ObExternalFileInfo> &external_table_files,
                                                  const ObString &properties,
                                                  bool &use_partition_gi)
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
    if (external_table_files.at(i).file_size_ < 0 && ++uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
      break;
    }
  }
  if (uncollect_statistics_part_cnt > max_parttition_count_to_collect_statistic) {
    use_partition_gi = true;
  } else if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_schema_scanner(external_odps_format.odps_format_))) {
    LOG_WARN("failed to init odps jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.pull_partition_info())) {
    LOG_WARN("failed to pull partition infos", K(ret));
  } else {
    ObIArray<sql::ObODPSJNITableRowIterator::OdpsJNIPartition>
          &partition_specs = odps_driver.get_partition_specs();

    int remote_odps_partition_count = partition_specs.count();
    int local_odps_partition_count = external_table_files.count();

    if (odps_driver.is_part_table()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < local_odps_partition_count; ++i) {
        const share::ObExternalFileInfo &odps_partition =
            external_table_files.at(i);
        if (0 != odps_partition.file_id_) {
          ret = OB_INVALID_PARTITION;
          LOG_WARN("unexpected file id", K(ret), K(i),
                  K(odps_partition.file_id_), K(odps_partition.part_id_));
        }

        bool is_found = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < remote_odps_partition_count; ++j) {
          ObODPSJNITableRowIterator::OdpsJNIPartition &partition = partition_specs.at(j);
          if (OB_ISNULL(odps_partition.file_url_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected partition spec", K(ret),
                    K(odps_partition.file_url_), K(partition.partition_spec_));
          } else if (0 == odps_partition.file_url_.compare(partition.partition_spec_)) {
            int64_t record_count = partition.record_count_;
            *(const_cast<int64_t *>(&odps_partition.file_size_)) = record_count;
            is_found = true;
          }
        }

        if (!is_found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find any parition spec on remote", K(ret),
                  K(odps_partition.file_url_));
        }
      }
    } else {
      // non-partition table
      if (1 != remote_odps_partition_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid non-partition count", K(ret),
                K(remote_odps_partition_count));
      } else if (0 != partition_specs.at(0).partition_spec_.compare(
                          ObODPSJNITableRowIterator::NON_PARTITION_FLAG)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("non-partition spec should be __NaN__", K(ret));
      } else {
        int64_t record_count = partition_specs.at(0).record_count_;
        *(const_cast<int64_t *>(&external_table_files.at(0).file_size_)) = record_count;
      }
    }
  }

  return ret;
}

int ObOdpsPartitionJNIScannerMgr::fetch_row_count(const ObString part_spec,
                                                  const ObString &properties,
                                                  int64_t &row_count) {
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_odps_format;
  common::ObArenaAllocator arena_alloc;
  ObODPSJNITableRowIterator odps_driver;
  if (OB_FAIL(external_odps_format.load_from_string(properties, arena_alloc))) {
    LOG_WARN("failed to init external_odps_format", K(ret));
  } else if (OB_FAIL(odps_driver.init_jni_schema_scanner(external_odps_format.odps_format_))) {
    LOG_WARN("failed to init jni scanner", K(ret));
  } else if (OB_FAIL(odps_driver.fecth_partition_row_count(part_spec, row_count))) {
    LOG_WARN("failed to pull partition infos", K(ret));
  } else {
    /* do nothing */
  }

  return ret;
}

int ObOdpsPartitionJNIScannerMgr::reset()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    // do nothing
  } else {
    odps_part_id_to_scanner_map_.reuse();
  }
  inited_ = false;
  return ret;
}

int ObOdpsJniUploaderMgr::create_writer_params_map(ObIAllocator& alloc,
                                                   const sql::ObODPSGeneralFormat &odps_format,
                                                   const ObString &external_partition,
                                                   bool is_overwrite,
                                                   common::hash::ObHashMap<ObString, ObString> &odps_params_map)
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
    if (!odps_params_map.created() && OB_FAIL(odps_params_map.create(MAX_PARAMS_SIZE, "IntoOdps", "IntoOdps", MTL_ID()))) {
      LOG_WARN("failed to create odps params map", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_id"), access_id))) {
      LOG_WARN("failed to add access id to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("access_key"), access_key))) {
      LOG_WARN("failed to add access key to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("project"),    project))) {
      LOG_WARN("failed to add project to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("table_name"), table))) {
      LOG_WARN("failed to add table name to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("region"),     region))) {
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
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("api_mode"), ObString::make_string("tunnel")))) {
      LOG_WARN("failed to add api mode to params", K(ret));
    } else if (OB_FAIL(odps_params_map.set_refactored(ObString::make_string("overwrite"), ObString::make_string(is_overwrite ? "true" : "false")))) {
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
  // open session
  if (inited_) {
    // do nothing
  } else if (properties.empty()) {
    // do nohting
  } else if (OB_FAIL(external_properties.load_from_string(properties, write_arena_alloc_))) {
    LOG_WARN("failed to parser external odps format", K(ret));
  } else if (OB_FAIL(external_properties.odps_format_.decrypt())) {
    LOG_WARN("failed to dump odps format", K(ret));
  } else if (sql::ObExternalFileFormat::ODPS_FORMAT != external_properties.format_type_) {
    // do nothing
  } else if (OB_FAIL(
                 create_writer_params_map(write_arena_alloc_,
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
  return ret;
}

int ObOdpsJniUploaderMgr::get_odps_uploader_in_px(int task_id, const common::ObFixedArray<ObExpr*, common::ObIAllocator>& select_exprs, OdpsUploader &odps_uploader)
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
  }
  return ret;
}

int ObOdpsJniUploaderMgr::get_writer_sid(
  ObIAllocator &alloc, const common::hash::ObHashMap<ObString, ObString> &odps_params_map, ObString &sid)
{
  int ret = OB_SUCCESS;
  session_holder_ptr_ = create_odps_jni_writer();
  if (OB_ISNULL(session_holder_ptr_.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create odps jni writer", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->init_params(odps_params_map))) {
    LOG_WARN("failed to init params", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->do_open())) {
    LOG_WARN("failed to do open", K(ret));
  } else if (OB_FAIL(session_holder_ptr_->get_session_id(alloc, sid))) {
    int tmp_ret = session_holder_ptr_->do_close();
    if (!OB_SUCC(tmp_ret)) {
      LOG_WARN("failed to do close", K(tmp_ret));
    }
    LOG_WARN("failed to get session id", K(ret));
  }
  return ret;
}

void ObOdpsJniUploaderMgr::release_hold_session() {
  LockGuard guard(lock_);
  if (OB_NOT_NULL(session_holder_ptr_.get())) {
    (void) session_holder_ptr_->do_close();
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


} // namespace sql
} // namespace oceanbase
#endif