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

#define USING_LOG_PREFIX  SQL_ENG

#include "ob_load_data_parser.h"
#include "lib/string/ob_hex_utils_base.h"
#include "src/sql/engine/ob_exec_context.h"
#if defined (OB_BUILD_CPP_ODPS) || defined (OB_BUILD_JNI_ODPS)
#include "share/ob_encryption_util.h"
#endif

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
const char INVALID_TERM_CHAR = '\xff';

const char * ObExternalFileFormat::FORMAT_TYPE_STR[] = {
  "CSV",
  "PARQUET",
  "ODPS",
  "ORC",
};
static_assert(array_elements(ObExternalFileFormat::FORMAT_TYPE_STR) == ObExternalFileFormat::MAX_FORMAT, "Not enough initializer for ObExternalFileFormat");

int64_t ObODPSGeneralFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(access_type_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(access_id_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(access_key_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(sts_token_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(endpoint_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(tunnel_endpoint_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(project_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(schema_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(table_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(quota_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(compression_code_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%s", OPTION_NAMES[idx++], STR_BOOL(collect_statistics_on_create_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", OPTION_NAMES[idx++], to_cstring(ObHexStringWrap(region_)));
  return pos;
}

int ObODPSGeneralFormat::encrypt_str(common::ObString &src, common::ObString &dst)
{
  int ret = OB_SUCCESS;
#if defined(OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty()) {
    //do nothing
    dst = src;
  } else {
    char encrypted_string[common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH] = {0};

    char hex_buff[common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1] = {0}; // +1 to reserve space for \0
    int64_t encrypt_len = -1;
    if (OB_FAIL(oceanbase::share::ObEncryptionUtil::encrypt_sys_data(tenant_id,
                                                   src.ptr(),
                                                   src.length(),
                                                   encrypted_string,
                                                   common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH,
                                                   encrypt_len))) {

      LOG_WARN("fail to encrypt_sys_data", KR(ret), K(src));
    } else if (0 >= encrypt_len || common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH < encrypt_len * 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("encrypt_len is invalid", K(ret), K(encrypt_len), K(common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH));
    } else if (OB_FAIL(to_hex_cstr(encrypted_string, encrypt_len, hex_buff, common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1))) {
      LOG_WARN("fail to print to hex str", K(ret));
    } else if (OB_FAIL(deep_copy_str(ObString(hex_buff), dst))) {
      LOG_WARN("failed to deep copy encrypted_string", K(ret));
    } else {
      LOG_TRACE("succ to encrypt src", K(ret));
    }
  }
#else
  dst = src;
#endif
  return ret;
}

int ObODPSGeneralFormat::decrypt_str(common::ObString &src, common::ObString &dst)
{
  int ret = OB_SUCCESS;
#if defined (OB_BUILD_TDE_SECURITY)
  const uint64_t tenant_id = MTL_ID();
  if (src.empty()) {
    // do nothing
    dst = src;
  } else if (0 != src.length() % 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src", K(src.length()), K(ret));
  } else {
    char encrypted_password_not_hex[common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH] = {0};
    char plain_string[common::OB_MAX_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1] = { 0 }; // need +1 to reserve space for \0
    int64_t plain_string_len = -1;
    if (OB_FAIL(hex_to_cstr(src.ptr(),
                            src.length(),
                            encrypted_password_not_hex,
                            common::OB_MAX_ENCRYPTED_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH))) {
      LOG_WARN("failed to hex to cstr", K(src.length()), K(ret));
    } else if (OB_FAIL(share::ObEncryptionUtil::decrypt_sys_data(tenant_id,
                                                          encrypted_password_not_hex,

                                                          src.length() / 2,
                                                          plain_string,
                                                          common::OB_MAX_EXTERNAL_TABLE_PROPERTIES_ITEM_LENGTH + 1,
                                                          plain_string_len))) {
      LOG_WARN("failed to decrypt_sys_data", K(ret), K(src.length()));
    } else if (0 >= plain_string_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decrypt dblink password failed", K(ret), K(plain_string_len));
    } else if (OB_FAIL(deep_copy_str(ObString(plain_string_len, plain_string), dst))) {
      LOG_WARN("failed to deep copy plain_string", K(ret));
    } else {
      LOG_TRACE("succ to decrypt src", K(ret));
    }
  }
#else
  dst = src;
#endif
  return ret;
}

int ObODPSGeneralFormat::encrypt()
{
  int ret = OB_SUCCESS;
  ObString encrypted_access_id;
  ObString encrypted_access_key;
  ObString encrypted_sts_token;
  if (OB_FAIL(encrypt_str(access_id_, encrypted_access_id))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(encrypt_str(access_key_, encrypted_access_key))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(encrypt_str(sts_token_, encrypted_sts_token))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else {
    access_id_ = encrypted_access_id;
    access_key_ = encrypted_access_key;
    sts_token_ = encrypted_sts_token;
  }
  return ret;
}

int ObODPSGeneralFormat::decrypt()
{
  int ret = OB_SUCCESS;
  ObString decrypted_access_id;
  ObString decrypted_access_key;
  ObString decrypted_sts_token;
  if (OB_FAIL(decrypt_str(access_id_, decrypted_access_id))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(access_key_, decrypted_access_key))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else if (OB_FAIL(decrypt_str(sts_token_, decrypted_sts_token))) {
    LOG_WARN("failed to encrypt", K(ret));
  } else {
    access_id_ = decrypted_access_id;
    access_key_ = decrypted_access_key;
    sts_token_ = decrypted_sts_token;
  }
  return ret;
}

int ObODPSGeneralFormat::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (src.length() > 0) {
    int64_t len = src.length() + 1;
    if (OB_ISNULL(buf = static_cast<char*>(arena_alloc_.alloc(len)))) {
      LOG_ERROR("allocate memory fail", K(len));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.ptr(), len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
    }
  } else {
    dest.reset();
  }
  return ret;
}

int ObODPSGeneralFormat::deep_copy(const ObODPSGeneralFormat &src) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_str(src.access_type_, access_type_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.access_id_, access_id_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.access_key_, access_key_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.sts_token_, sts_token_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.endpoint_, endpoint_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.tunnel_endpoint_, tunnel_endpoint_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.project_, project_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.schema_, schema_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.table_, table_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.quota_, quota_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.compression_code_, compression_code_))) {
    LOG_WARN("failed to deep copy", K(ret));
  } else if (OB_FAIL(deep_copy_str(src.region_, region_))) {
    LOG_WARN("failed to deep copy region for odps general format", K(ret));
  } else {
    collect_statistics_on_create_ = src.collect_statistics_on_create_;
  }
  return ret;
}

int ObODPSGeneralFormat::load_from_json_data(json::Pair *&node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      access_type_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      access_id_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      access_key_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      sts_token_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      endpoint_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      tunnel_endpoint_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      project_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      schema_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      table_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      quota_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      compression_code_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      collect_statistics_on_create_ = true;
    } else {
      collect_statistics_on_create_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      region_ = obj.get_string();
    }
    node = node->get_next();
  }
  return ret;
}

int ObCSVGeneralFormat::init_format(const ObDataInFileStruct &format,
                                    int64_t file_column_nums,
                                    ObCollationType file_cs_type)
{
  int ret = OB_SUCCESS;

  if (!ObCharset::is_valid_collation(file_cs_type)) {
    ret = OB_ERR_UNKNOWN_CHARSET;
    LOG_WARN("invalid charset", K(ret), K(file_cs_type));
  } else {
    cs_type_ = ObCharset::charset_type_by_coll(file_cs_type);
    file_column_nums_ = file_column_nums;
    field_enclosed_char_ = format.field_enclosed_char_;
    field_escaped_char_ = format.field_escaped_char_;
    field_term_str_ = format.field_term_str_;
    line_term_str_ = format.line_term_str_;
    line_start_str_ = format.line_start_str_;
    if (line_term_str_.empty() && !field_term_str_.empty()) {
      line_term_str_ = field_term_str_;
    }
  }
  return ret;
}


int ObCSVGeneralParser::init(const ObDataInFileStruct &format,
                             int64_t file_column_nums,
                             ObCollationType file_cs_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(format_.init_format(format, file_column_nums, file_cs_type))) {
    LOG_WARN("fail to init format", K(ret));
  } else if (OB_FAIL(init_opt_variables())) {
    LOG_WARN("fail to init opt values", K(ret));
  }

  return ret;
}

int ObCSVGeneralParser::init(const ObCSVGeneralFormat &format)
{
  int ret = OB_SUCCESS;

  format_ = format;

  if (OB_FAIL(init_opt_variables())) {
    LOG_WARN("fail to init opt values", K(ret));
  }

  return ret;
}

int ObCSVGeneralParser::init_opt_variables()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    opt_param_.line_term_c_ = format_.line_term_str_.empty() ? INVALID_TERM_CHAR : format_.line_term_str_[0];
    opt_param_.field_term_c_ = format_.field_term_str_.empty() ? INVALID_TERM_CHAR : format_.field_term_str_[0];
    opt_param_.max_term_ = std::max(static_cast<unsigned> (opt_param_.field_term_c_),
                                    static_cast<unsigned> (opt_param_.line_term_c_));
    opt_param_.min_term_ = std::min(static_cast<unsigned> (opt_param_.field_term_c_),
                                    static_cast<unsigned> (opt_param_.line_term_c_));
    opt_param_.is_filling_zero_to_empty_field_ = lib::is_mysql_mode();
    opt_param_.is_line_term_by_counting_field_ =
        0 == format_.line_term_str_.compare(format_.field_term_str_);
    opt_param_.is_same_escape_enclosed_ = (format_.field_enclosed_char_ == format_.field_escaped_char_);

    opt_param_.is_simple_format_ =
        !opt_param_.is_line_term_by_counting_field_
        && format_.field_term_str_.length() == 1
        && format_.line_term_str_.length() == 1
        && format_.line_start_str_.length() == 0
        && !opt_param_.is_same_escape_enclosed_
        && format_.field_enclosed_char_ == INT64_MAX;

  }

  if (OB_SUCC(ret) && OB_FAIL(fields_per_line_.prepare_allocate(format_.file_column_nums_))) {
    LOG_WARN("fail to allocate memory", K(ret), K(format_.file_column_nums_));
  }
  return ret;
}

int ObCSVGeneralParser::handle_irregular_line(int field_idx, int line_no,
                                              ObIArray<LineErrRec> &errors)
{
  int ret = OB_SUCCESS;
  LineErrRec rec;
  rec.err_code = field_idx > format_.file_column_nums_ ?
        OB_WARN_TOO_MANY_RECORDS : OB_WARN_TOO_FEW_RECORDS;
  rec.line_no = line_no;
  OX (errors.push_back(rec));
  for (int i = field_idx; OB_SUCC(ret) && i < format_.file_column_nums_; ++i) {
    FieldValue &new_field = fields_per_line_.at(i);
    new_field = FieldValue();
    new_field.is_null_ = 1;
  }
  return ret;
}

int64_t ObCSVGeneralFormat::to_json_kv_string(char *buf, const int64_t buf_len, bool into_outfile) const
{
  int64_t pos = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::LINE_DELIMITER)],
                  to_cstring(ObHexStringWrap(line_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FIELD_DELIMITER)], to_cstring(ObHexStringWrap(field_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%ld)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::ESCAPE)], field_escaped_char_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%ld)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FIELD_OPTIONALLY_ENCLOSED_BY)], field_enclosed_char_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::ENCODING)], ObCharset::charset_name(cs_type_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%ld)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::SKIP_HEADER)], skip_header_lines_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%s)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::SKIP_BLANK_LINES)], STR_BOOL(skip_blank_lines_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%s)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::TRIM_SPACE)], STR_BOOL(trim_space_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":)", OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::NULL_IF_EXETERNAL)]);
    J_ARRAY_START();
      for (int64_t i = 0; i < null_if_.count(); i++) {
        if (i != 0) {
          J_COMMA();
        }
        databuff_printf(buf, buf_len, pos, R"("%s")", to_cstring(ObHexStringWrap(null_if_.at(i))));
      }
    J_ARRAY_END();
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":%s)",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::EMPTY_FIELD_AS_NULL)],
                  STR_BOOL(empty_field_as_null_));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                  OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::COMPRESSION)],
                  compression_algorithm_to_string(compression_algorithm_));
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_0 && into_outfile) {
    J_COMMA();
    databuff_printf(buf, buf_len, pos, R"("%s":%s)",
                    OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::IS_OPTIONAL)], STR_BOOL(is_optional_));
    J_COMMA();
    databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                    OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FILE_EXTENSION)],
                    to_cstring(ObHexStringWrap(file_extension_)));
  }
  if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_5_1) {
    J_COMMA();
    databuff_printf(buf, buf_len, pos, R"("%s":%s)",
                    OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::PARSE_HEADER)], STR_BOOL(parse_header_));
    J_COMMA();
    databuff_printf(buf, buf_len, pos, R"("%s":"%s")",
                    OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::BINARY_FORMAT)],
                    binary_format_to_string(binary_format_));
  }
  return pos;
}

int ObCSVGeneralFormat::load_from_json_data(json::Pair *&node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::LINE_DELIMITER)])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      line_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FIELD_DELIMITER)])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ(ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      field_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::ESCAPE)])
      && json::JT_NUMBER == node->value_->get_type()) {
    field_escaped_char_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FIELD_OPTIONALLY_ENCLOSED_BY)])
      && json::JT_NUMBER == node->value_->get_type()) {
    field_enclosed_char_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::ENCODING)])
      && json::JT_STRING == node->value_->get_type()) {
    cs_type_ = ObCharset::charset_type(node->value_->get_string());
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::SKIP_HEADER)])
      && json::JT_NUMBER == node->value_->get_type()) {
    skip_header_lines_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::SKIP_BLANK_LINES)])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      skip_blank_lines_ = true;
    } else {
      skip_blank_lines_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::TRIM_SPACE)])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      trim_space_ = true;
    } else {
      trim_space_ = false;
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::NULL_IF_EXETERNAL)])
      && json::JT_ARRAY == node->value_->get_type()) {
    const json::Array &it_array = node->value_->get_array();
    int64_t idx = 0;
    if (it_array.get_size() > 0
        && OB_FAIL(null_if_.allocate_array(allocator, it_array.get_size()))) {
      LOG_WARN("allocate array failed", K(ret));
    }
    for (auto it_tmp = it_array.get_first();
         OB_SUCC(ret) && it_tmp != it_array.get_header() && it_tmp != NULL;
         it_tmp = it_tmp->get_next()) {
      if (OB_UNLIKELY(json::JT_STRING != it_tmp->get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null_if_ child is not string", K(ret), "type", it_tmp->get_type());
      } else {
        ObObj obj;
        OZ(ObHexUtilsBase::unhex(it_tmp->get_string(), allocator, obj));
        if (OB_SUCC(ret) && !obj.is_null()) {
          null_if_.at(idx++) = obj.get_string();
        }
      }
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::EMPTY_FIELD_AS_NULL)])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      empty_field_as_null_ = true;
    } else {
      empty_field_as_null_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::COMPRESSION)])
      && json::JT_STRING == node->value_->get_type()) {
    if (OB_FAIL(compression_algorithm_from_string(node->value_->get_string(), compression_algorithm_))) {
      LOG_WARN("failed to convert string to compression", K(ret));
    } else {
      node = node->get_next();
    }
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::IS_OPTIONAL)])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      is_optional_ = true;
    } else {
      is_optional_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::FILE_EXTENSION)])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      file_extension_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::PARSE_HEADER)])) {
    if (json::JT_TRUE == node->value_->get_type()) {
      parse_header_ = true;
    } else {
      parse_header_ = false;
    }
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[static_cast<int32_t>(ObCSVOptionsEnum::BINARY_FORMAT)])
      && json::JT_STRING == node->value_->get_type()) {
    if (OB_FAIL(binary_format_from_string(node->value_->get_string(), binary_format_))) {
      LOG_WARN("failed to convert string to binary format", K(ret));
    } else {
      node = node->get_next();
    }
  }
  return ret;
}

int64_t ObParquetGeneralFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], row_group_size_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], compress_type_index_);
  return pos;
}

int ObParquetGeneralFormat::load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    row_group_size_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    compress_type_index_ = node->value_->get_number();
    node = node->get_next();
  }
  return ret;
}


int64_t ObOrcGeneralFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], stripe_size_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], compress_type_index_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], compression_block_size_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":%ld", OPTION_NAMES[idx++], row_index_stride_);
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":", OPTION_NAMES[idx++]);
  J_ARRAY_START();
  int64_t sz = column_use_bloom_filter_.count() - 1;
  for (int64_t i = 0; i < sz; ++i) {
    databuff_printf(buf, buf_len, pos, "%ld", column_use_bloom_filter_.at(i));
    J_COMMA();
  }
  if (sz > 0) {
    databuff_printf(buf, buf_len, pos, "%ld", column_use_bloom_filter_.at(sz));
  }
  J_ARRAY_END();
  return pos;
}

int ObOrcGeneralFormat::load_from_json_data(json::Pair *&node, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    stripe_size_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    compress_type_index_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    compression_block_size_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_NUMBER == node->value_->get_type()) {
    row_index_stride_ = node->value_->get_number();
    node = node->get_next();
  }
  if (OB_NOT_NULL(node) && 0 == node->name_.case_compare(OPTION_NAMES[idx++])
      && json::JT_ARRAY == node->value_->get_type()) {
    int32_t sz = node->value_->get_array().get_size();
    if (sz == 0) {
      // do no thing
    } else if (OB_FAIL(column_use_bloom_filter_.allocate_array(allocator, sz))) {
      LOG_WARN("allocate array failed", K(ret));
    } else {
      int64_t idx = 0;
      DLIST_FOREACH(it, node->value_->get_array()) {
        if (OB_ISNULL(it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error json value", K(ret), KPC(it));
        } else if (json::JT_NUMBER != it->get_type()) {
          LOG_WARN("invalid node type", K(it->get_type()));
        } else if (OB_FALSE_IT(column_use_bloom_filter_.at(idx++) = it->get_number())) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int64_t ObOriginFileFormat::to_json_kv_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t idx = 0;
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_line_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_term_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_escaped_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_field_enclosed_str_)));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "\"%s\":\"%s\"", ORIGIN_FORMAT_STRING[idx++], to_cstring(ObHexStringWrap(origin_null_if_str_)));
  return pos;
}

int ObOriginFileFormat::load_from_json_data(json::Pair *&node, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_line_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_term_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_escaped_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_field_enclosed_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(node)
      && 0 == node->name_.case_compare(ORIGIN_FORMAT_STRING[idx++])
      && json::JT_STRING == node->value_->get_type()) {
    ObObj obj;
    OZ (ObHexUtilsBase::unhex(node->value_->get_string(), allocator, obj));
    if (OB_SUCC(ret) && !obj.is_null()) {
      origin_null_if_str_ = obj.get_string();
    }
    node = node->get_next();
  }
  return ret;
}

const char *compression_algorithm_to_string(ObCSVGeneralFormat::ObCSVCompression compression_algorithm)
{
  switch (compression_algorithm) {
    case ObCSVGeneralFormat::ObCSVCompression::NONE:    return "NONE";
    case ObCSVGeneralFormat::ObCSVCompression::AUTO:    return "AUTO";
    case ObCSVGeneralFormat::ObCSVCompression::GZIP:    return "GZIP";
    case ObCSVGeneralFormat::ObCSVCompression::DEFLATE: return "DEFLATE";
    case ObCSVGeneralFormat::ObCSVCompression::ZSTD:    return "ZSTD";
    default:                               return "INVALID";
  }
}

int compression_algorithm_from_string(ObString compression_name,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_algorithm)
{
  int ret = OB_SUCCESS;

  if (compression_name.length() == 0 ||
      0 == compression_name.case_compare("none")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::NONE;
  } else if (0 == compression_name.case_compare("gzip")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::GZIP;
  } else if (0 == compression_name.case_compare("deflate")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::DEFLATE;
  } else if (0 == compression_name.case_compare("zstd")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::ZSTD;
  } else if (0 == compression_name.case_compare("auto")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::AUTO;
  } else {
    ret = OB_INVALID_ARGUMENT;
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::INVALID;
  }
  return ret;
}

const char *binary_format_to_string(const ObCSVGeneralFormat::ObCSVBinaryFormat binary_format)
{
  switch (binary_format) {
    case ObCSVGeneralFormat::ObCSVBinaryFormat::HEX:    return "HEX";
    case ObCSVGeneralFormat::ObCSVBinaryFormat::BASE64:    return "BASE64";
    default: return "DEFAULT";
  }
}

int binary_format_from_string(const ObString binary_format_str,
                              ObCSVGeneralFormat::ObCSVBinaryFormat &binary_format) {
  int ret = OB_SUCCESS;

  if (binary_format_str.empty() || 0 == binary_format_str.case_compare("default")) {
    binary_format = ObCSVGeneralFormat::ObCSVBinaryFormat::DEFAULT;
  } else if (0 == binary_format_str.case_compare("hex")) {
    binary_format = ObCSVGeneralFormat::ObCSVBinaryFormat::HEX;
  } else if (0 == binary_format_str.case_compare("base64")) {
    binary_format = ObCSVGeneralFormat::ObCSVBinaryFormat::BASE64;
  } else {
    ret = OB_INVALID_ARGUMENT;
    binary_format = ObCSVGeneralFormat::ObCSVBinaryFormat::DEFAULT;
  }
  return ret;
}

int compression_algorithm_from_suffix(ObString filename,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_algorithm)
{
  int ret = OB_SUCCESS;
  if (filename.suffix_match_ci(".gz")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::GZIP;
  } else if (filename.suffix_match_ci(".deflate")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::DEFLATE;
  } else if (filename.suffix_match_ci(".zst") || filename.suffix_match_ci(".zstd")) {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::ZSTD;
  } else {
    compression_algorithm = ObCSVGeneralFormat::ObCSVCompression::NONE;
  }
  return ret;
}
const char *compression_algorithm_to_suffix(ObCSVGeneralFormat::ObCSVCompression compression_algorithm)
{
  switch (compression_algorithm) {
    case ObCSVGeneralFormat::ObCSVCompression::GZIP:    return ".gz";
    case ObCSVGeneralFormat::ObCSVCompression::DEFLATE: return ".deflate";
    case ObCSVGeneralFormat::ObCSVCompression::ZSTD:    return ".zst";
    default:                                            return "";
  }
}

int64_t ObExternalFileFormat::to_string(char *buf, const int64_t buf_len, bool into_outfile) const
{
  int64_t pos = 0;
  bool is_valid_format = format_type_ > INVALID_FORMAT && format_type_ < MAX_FORMAT;

  J_OBJ_START();

  databuff_print_kv(buf, buf_len, pos, "\"TYPE\"", is_valid_format ? ObExternalFileFormat::FORMAT_TYPE_STR[format_type_] : "INVALID");

  switch (format_type_) {
    case CSV_FORMAT:
      pos += csv_format_.to_json_kv_string(buf + pos, buf_len - pos, into_outfile);
      pos += origin_file_format_str_.to_json_kv_string(buf + pos, buf_len - pos);
      break;
    case ODPS_FORMAT:
      pos += odps_format_.to_json_kv_string(buf + pos, buf_len - pos);
      break;
    case PARQUET_FORMAT:
      pos += parquet_format_.to_json_kv_string(buf + pos, buf_len - pos);
      break;
    case ORC_FORMAT:
      pos += orc_format_.to_json_kv_string(buf + pos, buf_len - pos);
      break;
    default:
      pos += 0;
  }

  J_OBJ_END();
  return pos;
}

int ObExternalFileFormat::load_from_string(const ObString &str, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  json::Value *data = NULL;
  json::Parser parser;
  ObArenaAllocator temp_allocator;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format string is empty", K(ret), K(str));
  } else if (OB_FAIL(parser.init(&temp_allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), data))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (NULL == data || json::JT_OBJECT != data->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json value", K(ret), KPC(data));
  } else {
    auto format_type_node = data->get_object().get_first();
    if (format_type_node->value_->get_type() != json::JT_STRING) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected json format", K(ret), K(str));
    } else {
      ObString format_type_str = format_type_node->value_->get_string();
      for (int i = 0; i < array_elements(ObExternalFileFormat::FORMAT_TYPE_STR); ++i) {
        if (format_type_str.case_compare(ObExternalFileFormat::FORMAT_TYPE_STR[i]) == 0) {
          format_type_ = static_cast<FormatType>(i);
          break;
        }
      }
      format_type_node = format_type_node->get_next();
      switch (format_type_) {
        case CSV_FORMAT:
          OZ (csv_format_.load_from_json_data(format_type_node, allocator));
          OZ (origin_file_format_str_.load_from_json_data(format_type_node, allocator));
          break;
        case ODPS_FORMAT:
          OZ (odps_format_.load_from_json_data(format_type_node, allocator));
          break;
        case PARQUET_FORMAT:
          OZ (parquet_format_.load_from_json_data(format_type_node, allocator));
          break;
        case ORC_FORMAT:
          OZ (orc_format_.load_from_json_data(format_type_node, allocator));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid format type", K(ret), K(format_type_str));
          break;
      }
    }
  }
  return ret;
}

int ObExternalFileFormat::mock_gen_column_def(
    const share::schema::ObColumnSchemaV2 &column,
    ObIAllocator &allocator,
    ObString &def)
{
  int ret = OB_SUCCESS;
  ObSqlString temp_str;
  switch (format_type_) {
    case CSV_FORMAT: {
      uint64_t file_column_idx = column.get_column_id() - OB_APP_MIN_COLUMN_ID + 1;
      if (OB_FAIL(temp_str.append_fmt("%s%lu", N_EXTERNAL_FILE_COLUMN_PREFIX, file_column_idx))) {
        LOG_WARN("fail to append sql str", K(ret));
      }
      break;
    }
    case ODPS_FORMAT: {
      uint64_t odps_column_idx = column.get_column_id() - OB_APP_MIN_COLUMN_ID + 1;
      if (OB_FAIL(temp_str.append_fmt("%s%lu", N_EXTERNAL_TABLE_COLUMN_PREFIX, odps_column_idx))) {
        LOG_WARN("fail to append sql str", K(ret));
      } else {
      }
      break;
    }
    case PARQUET_FORMAT: {
      if (OB_FAIL(temp_str.append_fmt("get_path(%s, '%.*s')",
                                      N_EXTERNAL_FILE_ROW,
                                      column.get_column_name_str().length(),
                                      column.get_column_name_str().ptr()))) {
        LOG_WARN("fail to append sql str", K(ret));
      }
      break;
    }
    case ORC_FORMAT: {
      if (OB_FAIL(temp_str.append_fmt("get_path(%s, '%.*s')",
                                      N_EXTERNAL_FILE_ROW,
                                      column.get_column_name_str().length(),
                                      column.get_column_name_str().ptr()))) {
        LOG_WARN("fail to append sql str", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected format", K(ret), K(format_type_));
    }

  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_write_string(allocator, temp_str.string(), def))) {
       LOG_WARN("fail to write string", K(ret));
     }
  }

  return ret;
}

int ObExternalFileFormat::StringData::store_str(const ObString &str)
{
  return ob_write_string(allocator_, str, str_);
}

int ObExternalFileFormat::get_format_file_extension(FormatType format_type, ObString &file_extension)
{
  int ret  = OB_SUCCESS;
  switch (format_type) {
    case CSV_FORMAT: {
      file_extension.assign_ptr(csv_format_.file_extension_.ptr(), csv_format_.file_extension_.length());
      break;
    }
    case PARQUET_FORMAT: {
      file_extension.assign_ptr(ObParquetGeneralFormat::DEFAULT_FILE_EXTENSION, strlen(ObParquetGeneralFormat::DEFAULT_FILE_EXTENSION));
      break;
    }
    case ORC_FORMAT: {
      file_extension.assign_ptr(ObOrcGeneralFormat::DEFAULT_FILE_EXTENSION, strlen(ObOrcGeneralFormat::DEFAULT_FILE_EXTENSION));
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected to get format file extension", K(ret), K(format_type_));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExternalFileFormat::StringData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, str_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExternalFileFormat::StringData)
{
  int ret = OB_SUCCESS;
  ObString temp_str;
  LST_DO_CODE(OB_UNIS_DECODE, temp_str);
  if (OB_SUCC(ret)) {
    ret = store_str(temp_str);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExternalFileFormat::StringData)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, str_);
  return len;
}

int ObExternalFileFormat::StringList::store_strs(ObIArray<ObString> &strs)
{
  int ret = OB_SUCCESS;
  ObString str;
  OZ(strs_.init(strs.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < strs.count(); i++) {
    str.reset();
    if (OB_FAIL(ob_write_string(allocator_, strs.at(i), str))) {
      LOG_WARN("failed to deep copy string", K(ret));
    } else if (OB_FAIL(strs_.push_back(str))) {
      LOG_WARN("failed to push back string", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExternalFileFormat::StringList)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, strs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExternalFileFormat::StringList)
{
  int ret = OB_SUCCESS;
  ObFixedArray<ObString, ObIAllocator> temp_strs(allocator_);
  LST_DO_CODE(OB_UNIS_DECODE, temp_strs);
  if (OB_SUCC(ret)) {
    ret = store_strs(temp_strs);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExternalFileFormat::StringList)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, strs_);
  return len;
}

}
}
