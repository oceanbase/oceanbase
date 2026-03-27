/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_
#define OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_

#include "io/easy_io.h"
#include "share/ob_i_server_auth.h"

namespace oceanbase
{
namespace observer
{
int ob_rpc_intrusion_detect_patch(easy_io_handler_pt* ez_handler, share::ObIServerAuth* auth);
}; // end namespace observer
}; // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_RPC_INTRUSION_DETECT_H_ */

