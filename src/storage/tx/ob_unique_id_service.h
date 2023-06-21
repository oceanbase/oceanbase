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

#ifndef OCEANBASE_TRANSACTION_OB_UNIQUE_ID_SERVICE_
#define OCEANBASE_TRANSACTION_OB_UNIQUE_ID_SERVICE_

#include "ob_trans_service.h"

namespace oceanbase
{

namespace transaction
{

class ObUniqueIDService
{
public:
  ObUniqueIDService() {}
  ~ObUniqueIDService() {}
  static int mtl_init(ObUniqueIDService *&unique_id_service)
  {
    return OB_SUCCESS;
  }
  void destroy() {}
  int gen_unique_id(int64_t &unique_id, const int64_t timeout_ts)
  {
    int ret = OB_SUCCESS;
    ObTransID trans_id;
    int64_t expire_ts = ObTimeUtility::current_time() + timeout_ts;

    do {
      if (OB_SUCC(MTL(transaction::ObTransService *)->gen_trans_id(trans_id))) {
        unique_id = trans_id.get_id();
      } else if (OB_GTI_NOT_READY == ret) {
        if (ObTimeUtility::current_time() > expire_ts) {
          ret = OB_NEED_RETRY;
          TRANS_LOG(WARN, "get unique id not ready", K(ret), K(expire_ts));
        } else {
          ob_usleep(1000);
        }
      } else {
        TRANS_LOG(WARN, "get unique id fail", KR(ret));
      }
    } while (OB_GTI_NOT_READY == ret);
    return ret;
  }
};

}
}
#endif