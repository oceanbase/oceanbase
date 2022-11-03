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

#define USING_LOG_PREFIX SHARE

#include "rpc/obmysql/ob_mysql_util.h"
#include "share/client_feedback/ob_feedback_int_struct.h"

namespace oceanbase
{
namespace share
{
using namespace common;

int ObFeedbackIntStruct::serialize_struct_content(char *buf, const int64_t len, int64_t &pos) const
{
  OB_FB_SER_START;
  OB_FB_ENCODE_INT(int_value_);
  OB_FB_SER_END;
}

int ObFeedbackIntStruct::deserialize_struct_content(char *buf, const int64_t len, int64_t &pos)
{
  OB_FB_DESER_START;
  OB_FB_DECODE_INT(int_value_, int64_t);
  OB_FB_DESER_END;
}

} // end namespace share
} // end namespace oceanbase
