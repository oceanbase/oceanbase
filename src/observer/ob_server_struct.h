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
