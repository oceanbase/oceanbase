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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_ID_SERVICE_
#define OCEANBASE_TRANSACTION_OB_TRANS_ID_SERVICE_

#include "ob_id_service.h"
#include "ob_gti_rpc.h"

namespace oceanbase
{

namespace transaction
{

class ObTransIDService :  public ObIDService
{
public:
  ObTransIDService() {}
  ~ObTransIDService() {}
  int init();
  static int mtl_init(ObTransIDService *&trans_id_service);
  void destroy() { reset(); }
  static const int64_t TRANS_ID_PREALLOCATED_RANGE = 1000000; //TransID默认预分配大小
  int handle_request(const ObGtiRequest &request, obrpc::ObGtiRpcResult &result);
};

}
}
#endif