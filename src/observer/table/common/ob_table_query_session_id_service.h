/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #ifndef _OB_TABLE_QUERY_SESSION_ID_SERVICE_H
 #define _OB_TABLE_QUERY_SESSION_ID_SERVICE_H

 #include "storage/tx/ob_id_service.h"
 #include "ob_table_query_session_id_rpc.h"

namespace oceanbase
{
namespace observer
{
class ObTableSessIDService :  public transaction::ObIDService
{
public:
  ObTableSessIDService() {}
  ~ObTableSessIDService() {}
  int init();
  static int mtl_init(ObTableSessIDService *&table_sess_id_service);
  void destroy() { reset(); }
  int handle_request(const ObTableSessIDRequest &request, obrpc::ObTableSessIDRpcResult &result);
  static const int64_t OB_TABLE_QUERY_SESSION_RANGE = 1000000; // 1 million
  static const int64_t MAX_SESSION_ID = 1LL << 60;
};

} // end of namespace observer
} // end of namespace oceanbase


#endif // _OB_TABLE_QUERY_SESSION_ID_SERVICE_H