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