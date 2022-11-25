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

#ifndef OCEANBASE_TRANSACTION_OB_TIMESTAMP_ACCESS_
#define OCEANBASE_TRANSACTION_OB_TIMESTAMP_ACCESS_

#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{

namespace obrpc
{
class ObGtsRpcResult;
}

namespace transaction
{
class ObGtsRequest;

class ObTimestampAccess
{
public:
  ObTimestampAccess() : service_type_(FOLLOWER) {}
  ~ObTimestampAccess() {}
  static int mtl_init(ObTimestampAccess *&timestamp_access)
  {
    timestamp_access->reset();
    return OB_SUCCESS;
  }
  void destroy() { reset();}
  void reset() { service_type_ = FOLLOWER; }
  enum ServiceType {
    FOLLOWER = 0,
    GTS_LEADER,
    STS_LEADER,
  };
  void set_service_type(const ServiceType service_type) { service_type_ = service_type; }
  ServiceType get_service_type() const { return service_type_; }
  int handle_request(const ObGtsRequest &request, obrpc::ObGtsRpcResult &result);
  int get_number(const int64_t base_id, int64_t &gts);
private:
  ServiceType service_type_;
};


}
}
#endif