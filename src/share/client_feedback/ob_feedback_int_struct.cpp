/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

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
