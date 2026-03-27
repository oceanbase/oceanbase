/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_OBSERVER_OB_SERVER_STRUCT_H_
#define _OCEANBASE_OBSERVER_OB_SERVER_STRUCT_H_

#include "share/ob_lease_struct.h"
#include "lib/net/ob_addr.h"
#include "share/ob_cluster_role.h"              // ObClusterRole
#include "share/ob_rpc_struct.h"
#include "share/ob_server_struct.h"

// This file is replaced by share/ob_server_struct.h,
// PLEASE include "share/ob_server_struct.h" instead
namespace oceanbase
{
namespace observer
{
using ObServiceStatus = share::ObServiceStatus;
using ObServerMode = share::ObServerMode;
using ObGlobalContext = share::ObGlobalContext;
} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_SERVER_STRUCT_H_
