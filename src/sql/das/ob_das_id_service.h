/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_ID_SERVICE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_ID_SERVICE_H_
#include "storage/tx/ob_id_service.h"
#include "ob_das_id_rpc.h"
namespace oceanbase
{
namespace sql
{
class ObDASIDService :  public transaction::ObIDService
{
public:
  ObDASIDService() {}
  ~ObDASIDService() {}
  int init();
  static int mtl_init(ObDASIDService *&das_id_service);
  void destroy() { reset(); }
  static const int64_t DAS_ID_PREALLOCATED_RANGE = 1000000; // 1 million
  int handle_request(const ObDASIDRequest &request, obrpc::ObDASIDRpcResult &result);
};
} // namespace sql
} // namespace oceanbase
#endif // OBDEV_SRC_SQL_DAS_OB_DAS_ID_SERVICE_H_
