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

#define USING_LOG_PREFIX LIB_MYSQLC
#include "lib/mysqlclient/ob_dblink_error_trans.h"
#include "share/ob_errno.h"

int __attribute__((weak)) get_oracle_errno(int index)
{
  return oceanbase::OB_SUCCESS;
}

int __attribute__((weak)) get_mysql_errno(int index)
{
  return oceanbase::OB_SUCCESS;
}

const char* __attribute__((weak)) get_oracle_str_error(int index)
{
  return NULL;
}

const char* __attribute__((weak)) get_mysql_str_error(int index)
{
  return NULL;
}


namespace oceanbase
{
namespace common
{

int sqlclient::ObDblinkErrorTrans::external_errno_to_ob_errno(bool is_oracle_err,
                                                   int external_errno,
                                                   const char *external_errmsg,
                                                   int &ob_errno) {
  int ret = OB_SUCCESS;
  external_errno = abs(external_errno);
  if (OB_SUCCESS != external_errno) {
    const char *oracle_msg_prefix = "ORA";
    if (external_errno >= 2000 && // google "Client Error Message Reference"
        external_errno <= 2075 && // you will known errno in [2000, 2075] is client error at dev.mysql.com
        (!is_oracle_err ||
        (is_oracle_err &&
        (OB_NOT_NULL(external_errmsg) && 0 != STRLEN(external_errmsg)) &&
        0 != std::memcmp(oracle_msg_prefix, external_errmsg,
        std::min(STRLEN(oracle_msg_prefix), STRLEN(external_errmsg)))))) {
      ob_errno = external_errno; // do not map, show user client errno directly.
    } else {
      int64_t match_count = 0;
      for (int i = 0; i < oceanbase::common::OB_MAX_ERROR_CODE; ++i) {
        if (external_errno == (is_oracle_err ? get_oracle_errno(i) : get_mysql_errno(i))) {
          ob_errno = -i;
          ++match_count;
        }
      }
      if (1 != match_count) {
        // default ob_errno, if external_errno can not map to any valid ob_errno
        ob_errno = OB_ERR_DBLINK_REMOTE_ECODE;
	const char *errmsg = external_errmsg;
	if (NULL == errmsg) {
		errmsg = "empty error message";
	}
        int msg_len = STRLEN(errmsg);
        LOG_USER_ERROR(OB_ERR_DBLINK_REMOTE_ECODE, external_errno, msg_len, errmsg);
      }
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
