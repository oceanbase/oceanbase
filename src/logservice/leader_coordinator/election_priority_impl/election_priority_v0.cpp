/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "election_priority_impl.h"

namespace oceanbase
{
namespace logservice
{
namespace coordinator
{

int PriorityV0::compare(const AbstractPriority &rhs, int &result, ObStringHolder &reason) const
{
  int ret = OB_SUCCESS;
  // 这里如果转型失败直接抛异常，但设计上转型不会失败
  const PriorityV0 &rhs_impl = dynamic_cast<const PriorityV0 &>(rhs);
  if (port_number_ > rhs_impl.port_number_) {
    result = 1;
    reason.assign("PORT");
  } else if (port_number_ < rhs_impl.port_number_) {
    result = -1;
    reason.assign("PORT");
  } else {
    result = 0;
  }
  COORDINATOR_LOG(DEBUG, "debug", K(*this), K(rhs), KR(ret), K(MTL_ID()));
  return ret;
}

int PriorityV0::refresh_(const share::ObLSID &ls_id)
{
  UNUSED(ls_id);
  port_number_ = GCTX.self_addr().get_port();
  return OB_SUCCESS;
}

}
}
}