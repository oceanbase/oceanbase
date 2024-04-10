/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * ob_das_attach_define.cpp
 *
 *      Author: yuming<>
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_factory.h"
namespace oceanbase
{
namespace sql
{

OB_DEF_SERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = false;
  OB_UNIS_ENCODE(has_attach_ctdef);
  return ret;
}

OB_DEF_DESERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = false;
  OB_UNIS_DECODE(has_attach_ctdef);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASAttachSpec)
{
  int64_t len = 0;
  bool has_attach_ctdef = false;
  OB_UNIS_ADD_LEN(has_attach_ctdef);
  return len;
}

}
}
