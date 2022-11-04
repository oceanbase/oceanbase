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

#include "sql/engine/sort/ob_base_sort.h"
#include "common/object/ob_obj_compare.h"
#include "common/row/ob_row_util.h"
#include "share/ob_cluster_version.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

DEFINE_SERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  int32_t cs_type_int = static_cast<int32_t>(cs_type_);
  OB_UNIS_ENCODE(index_);
  OB_UNIS_ENCODE(cs_type_int);
  bool is_asc = true;
  if ((extra_info_ & ObSortColumnExtra::SORT_COL_ASC_BIT) > 0) {
    is_asc = true;
  } else {
    is_asc = false;
  }
  if ((extra_info_ & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
    OB_UNIS_ENCODE(extra_info_);
    BASE_SER((ObSortColumn, ObSortColumnExtra));
  } else {
    OB_UNIS_ENCODE(is_asc);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSortColumn)
{
  int ret = OB_SUCCESS;
  int32_t cs_type_int = 0;
  OB_UNIS_DECODE(index_);
  OB_UNIS_DECODE(cs_type_int);
  cs_type_ = static_cast<ObCollationType>(cs_type_int);
  uint8_t extra_char = 0;
  OB_UNIS_DECODE(extra_char);

  if (OB_SUCC(ret)) {
    extra_info_ = extra_char;
    if ((extra_char & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
      BASE_DESER((ObSortColumn, ObSortColumnExtra));
    } else {
      // do nothing
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSortColumn)
{
  int64_t len = 0;
  int32_t cs_type_int = static_cast<int32_t>(cs_type_);
  OB_UNIS_ADD_LEN(index_);
  OB_UNIS_ADD_LEN(cs_type_int);   // for cs_type_.

  OB_UNIS_ADD_LEN(extra_info_);

  if ((extra_info_ & ObSortColumnExtra::SORT_COL_EXTRA_BIT) > 0) {
    BASE_ADD_LEN((ObSortColumn, ObSortColumnExtra));
  }
  return len;
}

OB_SERIALIZE_MEMBER(ObSortColumnExtra, obj_type_, order_type_);
