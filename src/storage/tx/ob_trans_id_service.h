/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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