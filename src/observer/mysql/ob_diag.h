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

#ifndef _OB_DIAG_H_
#define _OB_DIAG_H_

#include "lib/string/ob_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace obmysql
{

class ObDiag
{
  static const int64_t EXPIRED_TIME = 10 * 1000L * 1000L;  // 10 seconds
public:
  ObDiag();

  int refresh_passwd(common::ObString &passwd);
  int check_passwd(const common::ObString &passwd, const common::ObString &scramble_str);

private:
  lib::ObMutex lock_;
  char passwd_[64];
  int64_t fresh_timestamp_;
}; // end of class ObDiag

inline ObDiag::ObDiag()
    : lock_(ObLatchIds::DEFAULT_MUTEX), passwd_(), fresh_timestamp_(0L)
{
}

inline int ObDiag::refresh_passwd(common::ObString &passwd)
{
  int ret = common::OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  const int64_t current_ts = common::ObTimeUtility::current_time();

  if (fresh_timestamp_ + EXPIRED_TIME < current_ts) {
    int64_t pos = 0;
    if (OB_FAIL(common::databuff_printf(
                    passwd_, sizeof (passwd_), pos, "%ld", current_ts))) {
      RPC_OBMYSQL_LOG(ERROR, "generate passwd fail", K(ret));
    }
  }
  fresh_timestamp_ = current_ts;

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    if (OB_FAIL(common::databuff_printf(
                    passwd.ptr(), passwd.size(), pos, "%s", passwd_))) {
      RPC_OBMYSQL_LOG(ERROR, "copy passwd fail", K(ret));
    }
  }
  return ret;
}

inline int ObDiag::check_passwd(const common::ObString &passwd, const common::ObString &scramble_str)
{
  using common::ObString;
  int ret = common::OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);

  const int64_t current_ts = common::ObTimeUtility::current_time();
  if (fresh_timestamp_ + EXPIRED_TIME > current_ts) {
    char buf[21] = {};
    int64_t pos = 0;
    if (OB_FAIL(common::ObEncryptedHelper::encrypt_password(
                    ObString(STRLEN (passwd_), passwd_),
                    scramble_str,
                    buf,
                    sizeof(buf),
                    pos))) {
      RPC_OBMYSQL_LOG(ERROR, "encrypt password fail", K(ret));
    } else if (MEMCMP(passwd.ptr(), buf, passwd.length()) != 0) {
      ret = common::OB_ERR_WRONG_PASSWORD;
    }
  } else {
    ret = common::OB_ERR_WRONG_PASSWORD;
  }
  return ret;
}

} // end of namespace obmysql
} // end of namespace oceanbase


#endif /* _OB_DIAG_H_ */
