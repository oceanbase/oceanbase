/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_RETRY_H_
#define OCEANBASE_STORAGE_OB_LOB_RETRY_H_

#include "storage/lob/ob_lob_access_param.h"

namespace oceanbase
{
namespace storage
{

class ObLobRetryUtil
{
public:
  static bool is_remote_ret_can_retry(int ret_code);
  static int check_need_retry(ObLobAccessParam &param, const int error_code, const int retry_cnt, bool &need_retry);

};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_RETRY_H_