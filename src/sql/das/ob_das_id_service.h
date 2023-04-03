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
