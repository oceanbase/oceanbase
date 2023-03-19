// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <>

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
