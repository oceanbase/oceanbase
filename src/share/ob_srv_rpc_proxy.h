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

#ifndef _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_
#define _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_

#include "sql/engine/cmd/ob_kill_session_arg.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_common_id.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_lonely_table_clean_rpc_struct.h"
#include "share/ob_server_struct.h"
#include "observer/net/ob_net_endpoint_ingress_rpc_struct.h"
#include "observer/net/ob_shared_storage_net_throt_rpc_struct.h"
#include "share/ob_heartbeat_struct.h"
#include "share/resource_limit_calculator/ob_resource_commmon.h"
#include "observer/table_load/control/ob_table_load_control_rpc_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_struct.h"
#include "rpc/obrpc/ob_rpc_reverse_keepalive_struct.h"
#include "share/ob_flashback_standby_log_struct.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_sswriter_msg.h"
#endif
#include "share/ls/ob_alter_ls_struct.h"

namespace oceanbase
{
namespace obrpc
{

class ObSrvRpcProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(ObSrvRpcProxy);
#define OB_RPC_DECLARATIONS
#include "ob_srv_rpc_proxy.ipp"
#undef OB_RPC_DECLARATIONS

}; // end of class ObSrvRpcProxy

} // end of namespace rpc
} // end of namespace oceanbase
// rollback defines
#include "rpc/obrpc/ob_rpc_proxy_macros.h"

#endif /* _OCEABASE_COMMON_OB_SRV_RPC_PROXY_H_ */
