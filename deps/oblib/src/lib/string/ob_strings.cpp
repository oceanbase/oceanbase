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

#define USING_LOG_PREFIX LIB

#include "lib/string/ob_strings.h"
#include "lib/utility/utility.h"
namespace oceanbase
{
namespace common
{
ObStrings::ObStrings()
{
}

ObStrings::~ObStrings()
{
}

int ObStrings::add_string(const ObString &str, int64_t *idx/*=NULL*/)
{
  int ret = OB_SUCCESS;
  ObString stored_str;
  if (OB_FAIL(buf_.write_string(str, &stored_str))) {
    LOG_WARN("failed to write string", K(ret), K(str));
  } else if (OB_FAIL(strs_.push_back(stored_str))) {
    LOG_WARN("failed to push into array", K(ret));
  } else {
    if (NULL != idx) {
      *idx = strs_.count() - 1;
    }
  }
  return ret;
}

int ObStrings::get_string(int64_t idx, ObString &str) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= strs_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), "string count", strs_.count());
  } else if (OB_FAIL(strs_.at(idx, str))) {
    LOG_WARN("failed to get string", K(ret), K(idx));
  }
  return ret;
}

int64_t ObStrings::count() const
{
  return strs_.count();
}

void ObStrings::reuse()
{
  buf_.reset();
  strs_.reset();
}

DEFINE_SERIALIZE(ObStrings)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, strs_.count()))) {
    LOG_WARN("encode int failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < strs_.count(); ++i) {
      if (OB_FAIL(strs_.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("serialize string failed", K(ret));
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObStrings)
{
  int ret = OB_SUCCESS;
  reuse();
  int64_t count = 0;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
    LOG_WARN("decode int failed", K(ret));
  } else {
    ObString str;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(str.deserialize(buf, data_len, pos))) {
        LOG_WARN("deserialize string failed", K(ret));
      } else if (OB_FAIL(add_string(str))) {
        LOG_WARN("add string string failed", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObStrings::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = strs_.to_string(buf, buf_len);
  }
  return pos;
}
} //end common
} //end oceanbase
