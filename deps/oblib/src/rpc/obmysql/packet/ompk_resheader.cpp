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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/packet/ompk_resheader.h"
#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

OMPKResheader::OMPKResheader()
    : field_count_(0)
{

}


OMPKResheader::~OMPKResheader() {}

int OMPKResheader::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (NULL == buffer || len <= 0 || pos < 0) {
    LOG_WARN("invalid argument", KP(buffer), K(len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(ObMySQLUtil::store_length(buffer, len, field_count_, pos))) {
      LOG_WARN("serialize field count fail", KP(buffer), K(len), K(pos), K(field_count_));
    }
  }

  return ret;
}

int64_t OMPKResheader::get_serialize_size() const
{
  int64_t len = 0;
  len += ObMySQLUtil::get_number_store_len(field_count_); // field_count_
  return len;
}
