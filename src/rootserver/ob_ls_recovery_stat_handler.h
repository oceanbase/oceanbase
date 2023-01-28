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

#ifndef OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER
#define OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER

#include "lib/ob_define.h"
#include "rootserver/ob_rs_async_rpc_proxy.h" //ObGetLSReplayedScnProxy
#include "share/ls/ob_ls_recovery_stat_operator.h" // ObLSRecoveryStatOperator

namespace oceanbase
{

namespace storage
{
class ObLS;
}

namespace rootserver
{
/**
  * @description:
  *    ObLSRecoveryStatHandler exists on the LS of each observer and is responsible for
  *    the each LS recovery stat
  */
class ObLSRecoveryStatHandler
{
public:
  ObLSRecoveryStatHandler() { reset(); }
  ~ObLSRecoveryStatHandler() { reset(); }
  int init(const uint64_t tenant_id, ObLS *ls);
  void reset();
  /**
   * @description:
   *    get ls readable_scn considering readable scn, sync scn and replayable scn.
   * @param[out] readable_scn ls readable_scn
   * @return return code
   */
  int get_ls_replica_readable_scn(share::SCN &readable_scn);

  TO_STRING_KV(K_(tenant_id), K_(ls));

private:
  int check_inner_stat_();

  /**
   * @description:
   *    increase LS readable_scn when replayable_scn is pushed forward in switchover
   * @param[in/out] readable_scn
   *                  in: actual readable_scn
   *                  out: increased readable_scn
   * @param[out] sync_scn increased sync_scn
   * @return return code
   */
  int increase_ls_replica_readable_scn_(share::SCN &readable_scn, share::SCN &sync_scn);

  DISALLOW_COPY_AND_ASSIGN(ObLSRecoveryStatHandler);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObLS *ls_;
};

}
}

#endif // OCEANBASE_STORAGE_OB_LS_RECOVERY_STAT_HANDLER
