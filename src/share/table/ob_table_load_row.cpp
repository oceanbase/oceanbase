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

#define USING_LOG_PREFIX SERVER

#include "share/table/ob_table_load_row.h"

namespace oceanbase
{
namespace table
{
using namespace common;

OB_DEF_SERIALIZE_SIMPLE(ObTableLoadTabletObjRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(tablet_id_);
  OB_UNIS_ENCODE(obj_row_);
  return ret;
}

OB_DEF_DESERIALIZE_SIMPLE(ObTableLoadTabletObjRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(tablet_id_);
  OB_UNIS_DECODE(obj_row_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE_SIMPLE(ObTableLoadTabletObjRow)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(tablet_id_);
  OB_UNIS_ADD_LEN(obj_row_);
  return len;
}

} // namespace table
} // namespace oceanbase
