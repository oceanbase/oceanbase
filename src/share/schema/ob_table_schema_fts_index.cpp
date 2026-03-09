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
#include "ob_column_schema.h"
#include "ob_table_schema.h"
#include "ob_schema_struct_fts.h"

// part implement of ObTableSchema for fts index

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace oceanbase::common;

int ObTableSchema::set_fts_params_to_index_params(const ObFTSIndexParams &fts_index_params)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_INDEX_PARAMS_LENGTH] = { 0 };
  int64_t pos = 0;
  if (!fts_index_params.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fts index params is not valid", K(ret), K(fts_index_params));
  } else if (OB_FAIL(fts_index_params.append_param_str(buf, sizeof(buf), pos))) {
    LOG_WARN("fail to append fts index params to index params", K(ret), K(fts_index_params));
  } else if (OB_FAIL(deep_copy_str(buf, index_params_))) {
    LOG_WARN("fail to deep copy fts index params to index params", K(ret), K(buf));
  }
  return ret;
}

int ObTableSchema::get_fts_params_from_index_params(ObFTSIndexParams &fts_index_params) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFTSIndexParams::from_param_str(index_params_, fts_index_params))) {
    LOG_WARN("fail to get fts index params from index params", K(ret), K(index_params_));
  } else if (!fts_index_params.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected fts index params", K(ret), K(fts_index_params));
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
