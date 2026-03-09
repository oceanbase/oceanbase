/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_schema_struct_fts.h"
#include "lib/container/ob_array.h"
#include "share/ob_fts_index_builder_util.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

int ObFTSIndexParams::from_param_str(const ObString &param_str, ObFTSIndexParams &params)
{
  int ret = OB_SUCCESS;
  if (param_str.empty()) {
    params.fts_index_type_ = OB_FTS_INDEX_TYPE_MATCH; // if not set, default to match type
  } else {
    ObString tmp_param_str = param_str;
    ObArray<ObString> tmp_param_strs;
    if (OB_FAIL(split_on(tmp_param_str, ',', tmp_param_strs))) {
      LOG_WARN("fail to split param str", K(ret), K(tmp_param_str));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_param_strs.count(); ++i) {
        ObString tmp_param_str = tmp_param_strs.at(i).trim();
        ObArray<ObString> tmp_param_name_with_value;
        if (OB_FAIL(split_on(tmp_param_str, '=', tmp_param_name_with_value))) {
          LOG_WARN("fail to split param name and value", K(ret), K(tmp_param_str));
        } else if (tmp_param_name_with_value.count() != 2) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid param name and value", K(ret), K(tmp_param_name_with_value));
        } else {
          ObString tmp_param_name = tmp_param_name_with_value.at(0).trim();
          ObString tmp_param_value = tmp_param_name_with_value.at(1).trim();
          if (tmp_param_name.case_compare_equal("fts_index_type")) {
            if (tmp_param_value.case_compare_equal("match")) {
              params.fts_index_type_ = OB_FTS_INDEX_TYPE_MATCH;
            } else if (tmp_param_value.case_compare_equal("phrase_match")) {
              params.fts_index_type_ = OB_FTS_INDEX_TYPE_PHRASE_MATCH;
            } else if (tmp_param_value.case_compare_equal("filter")) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("filter fts index type is not supported", K(ret));
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid fts index type", K(ret), K(tmp_param_value));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFTSIndexParams::append_param_str(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  char tmp [OB_MAX_INDEX_PARAMS_LENGTH] = {0};
  ObString fts_index_type_str;
  if (OB_FAIL(ObFtsIndexTypePrinter::fts_index_type_to_string(fts_index_type_, fts_index_type_str))) {
    LOG_WARN("fail to convert fts index type to string", K(ret));
  }
  int64_t tmp_pos = 0;
  if (FAILEDx(databuff_printf(tmp, OB_MAX_INDEX_PARAMS_LENGTH, tmp_pos, "FTS_INDEX_TYPE = %.*s ", fts_index_type_str.length(), fts_index_type_str.ptr()))) {
    LOG_WARN("fail to append fts index type", K(ret));
  } else if (tmp_pos > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer is not enough", K(ret), K(tmp_pos), K(buf_len));
  } else {
    MEMCPY(buf + pos, tmp, tmp_pos);
    pos += tmp_pos;
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase