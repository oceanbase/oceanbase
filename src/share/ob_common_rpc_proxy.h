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

#ifndef _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_
#define _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_lease_struct.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_time_zone_info_manager.h"
#include "common/storage/ob_freeze_define.h" // for ObFrozenStatus
#include "rootserver/ob_alter_locality_finish_checker.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "share/ls/ob_ls_info.h"

namespace oceanbase
{
namespace rootserver
{
class ObCommitAlterTenantLocalityArg;
}
namespace obrpc
{
class ObCommonRpcProxy
    : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObCommonRpcProxy);
#define OB_RPC_DECLARATIONS
#include "ob_common_rpc_proxy.ipp"
#undef OB_RPC_DECLARATIONS
public:
  inline void set_rs_mgr(share::ObRsMgr &rs_mgr)
  {
    rs_mgr_ = &rs_mgr;
  }

  //send to rs, only need set rs_mgr, no need set dst_server
  inline ObCommonRpcProxy to_rs(share::ObRsMgr &rs_mgr) const
  {
    ObCommonRpcProxy proxy = this->to();
    proxy.set_rs_mgr(rs_mgr);
    return proxy;
  }

  //send to addr, if failed, it will not retry to send to rs according to rs_mgr
  //the interface emphasize no retry
  inline ObCommonRpcProxy to_addr(const ::oceanbase::common::ObAddr &dst) const
  {
    ObCommonRpcProxy proxy = this->to(dst);
    proxy.reset_rs_mgr();
    return proxy;
  }

private:
  inline void reset_rs_mgr()
  {
    rs_mgr_ = NULL;
  }

protected:

#define CALL_WITH_RETRY(call_stmt)                                      \
  common::ObAddr rs;                                                    \
  do {                                                                  \
    int ret = common::OB_SUCCESS;                                       \
    const bool use_remote_rs = GCONF.cluster_id != dst_cluster_id_ && OB_INVALID_CLUSTER_ID != dst_cluster_id_;\
    rs.reset();                                                         \
    if (NULL != rs_mgr_) {                                              \
      if (use_remote_rs) {                           \
        if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {  \
          if (OB_ENTRY_NOT_EXIST != ret) {                                                      \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          } else if (OB_FAIL(rs_mgr_->renew_master_rootserver(dst_cluster_id_))) {       \
            SHARE_LOG(WARN, "failed to renew remote master rs", KR(ret), K_(dst_cluster_id));   \
          } else if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {    \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          }                                                                                    \
        }                                                               \
      } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {        \
        SHARE_LOG(WARN, "failed to get master rs", KR(ret));            \
      }                                                                 \
      if (OB_FAIL(ret)) {                                               \
      } else {                                                          \
        set_server(rs);                                                 \
      }                                                                 \
    }                                                                   \
    if (OB_SUCC(ret)) {                                                 \
      ret = call_stmt;                                                  \
      const int64_t RETRY_TIMES = 3;                                    \
      for (int64_t i = 0;                                               \
           (common::OB_RS_NOT_MASTER == ret                             \
            || common::OB_SERVER_IS_INIT == ret                         \
            || (use_remote_rs && OB_FAIL(ret)))                         \
           && NULL != rs_mgr_ && i < RETRY_TIMES;                       \
           ++i) {                                                       \
        if (use_remote_rs) {                         \
          if (OB_FAIL(rs_mgr_->renew_master_rootserver(dst_cluster_id_))) {  \
            SHARE_LOG(WARN, "failed to get master rs", K(ret), K(dst_cluster_id_));          \
          } else if (OB_FAIL(rs_mgr_->get_master_root_server(dst_cluster_id_, rs))) {  \
            SHARE_LOG(WARN, "failed to get remote master rs", KR(ret), K_(dst_cluster_id));     \
          }                                                            \
        } else if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {              \
          SHARE_LOG(WARN, "renew_master_rootserver failed", K(ret), "retry", i); \
        } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs))) {      \
          SHARE_LOG(WARN, "get master root service failed", K(ret), "retry", i); \
        }                                                               \
        if (OB_FAIL(ret)) {                                             \
        } else {                                                        \
          set_server(rs);                                               \
          ret = call_stmt;                                              \
        }                                                               \
      }                                                                 \
    }                                                                   \
    return ret;                                                         \
  } while (false)

  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode, const Input &args,
               Out &result, Handle *handle, const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_call(pcode, args, result, handle, opts));
  }

  template <typename Input>
  int rpc_post(ObRpcPacketCode pcode, const Input &args,
               rpc::frame::ObReqTransport::AsyncCB *cb, const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, args, cb, opts));
  }

  template <class pcodeStruct>
  int rpc_post(const typename pcodeStruct::Request &args,
               obrpc::ObRpcProxy::AsyncCB<pcodeStruct> *cb,
               const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(ObRpcProxy::rpc_post<pcodeStruct>(args, cb, opts));
  }

  int rpc_post(
      ObRpcPacketCode pcode,
      rpc::frame::ObReqTransport::AsyncCB *cb,
      const ObRpcOpts &opts)
  {
    CALL_WITH_RETRY(rpc_post(pcode, cb, opts));
  }
#undef CALL_WIRTH_RETRY

private:
  share::ObRsMgr *rs_mgr_;
}; // end of class ObCommonRpcProxy

} // end of namespace share
} // end of namespace oceanbase

// rollback defines
#include "rpc/obrpc/ob_rpc_proxy_macros.h"

#endif /* _OCEABASE_SHARE_OB_COMMON_RPC_PROXY_H_ */
