/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OB_COLLECT_MV_MERGE_INFO_TASK_H
#define OB_COLLECT_MV_MERGE_INFO_TASK_H

#include "lib/ob_define.h"
#include "share/ob_thread_mgr.h"
#include "rootserver/mview/ob_mview_timer_task.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "src/storage/tx_storage/ob_ls_service.h"
#include "src/storage/mview/ob_major_mv_merge_info.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace rootserver
{

#define RPC_MVIEW(code, arg, result, name) \
  typedef obrpc::ObAsyncRpcProxy<code, arg, result, \
    int (obrpc::ObSrvRpcProxy::*)(const arg &, obrpc::ObSrvRpcProxy::AsyncCB<code> *, const obrpc::ObRpcOpts &), obrpc::ObSrvRpcProxy> name
RPC_MVIEW(obrpc::OB_COLLECT_MV_MERGE_INFO, obrpc::ObCollectMvMergeInfoArg, obrpc::ObCollectMvMergeInfoResult, ObCollectMvMergeInfoProxy);

class ObCollectMvMergeInfoTask : public ObMViewTimerTask
{
public:

    static const uint64_t SLEEP_SECONDS = 30 * 1000L * 1000L; // 30s
    explicit ObCollectMvMergeInfoTask() : is_inited_(false),
                                          is_stop_(true),
                                          in_sched_(false),
                                          tenant_id_(OB_INVALID_TENANT_ID)
    {}
    ~ObCollectMvMergeInfoTask() { destroy(); }
    // static int mtl_init(ObMvMergeInfoCollector *&collector);
    int init();
    void reset();
    void destroy();
    int start();
    void stop();
    void wait();
    void runTimerTask(void) override;
    static int get_stable_member_list_and_config_version(const uint64_t tenant_id,
                                                 const share::ObLSID &ls_id,
                                                 common::ObIArray<common::ObAddr> &addr_list,
                                                 palf::LogConfigVersion &log_config_version);
    int check_ls_list_state(const ObLSAttrArray &ls_attr_array);
    int check_ls_list_match(const ObLSAttrArray &ls_attr_array,
                            const ObLSAttrArray &ls_attr_array_new);
    int double_check_ls_list_in_trans(const ObLSAttrArray &ls_attr_array,
                                      common::ObMySQLTransaction &trans);
    int check_and_update_tenant_merge_scn(const ObLSAttrArray &ls_attr_array,
                                          const share::SCN &merge_scn);
    static int collect_ls_member_merge_info(const uint64_t tenant_id,
                                            const ObLSID &ls_id,
                                            share::SCN &merge_scn);
    static int sync_get_ls_member_merge_info(const common::ObAddr &server,
                                             const uint64_t tenant_id,
                                             const ObLSID &ls_id,
                                             storage::ObMajorMVMergeInfo &mv_merge_info,
                                             uint64_t rpc_timeout,
                                             const bool need_check_leader = false,
                                             const bool need_update = false);
    static int get_min_mv_tablet_major_compaction_scn(share::SCN &compaction_scn);
    TO_STRING_KV(K_(is_inited), K_(is_stop), K_(in_sched), K_(tenant_id));
private:
    int check_ls_attr_state_(const ObLSAttr &ls_attr);
private:
    bool is_inited_;
    bool is_stop_;
    bool in_sched_;
    uint64_t tenant_id_;
};

}
}

#endif
