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

#pragma once

#include "rootserver/mview/ob_mview_timer_task.h"
#include "share/ob_ls_id.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/mview/ob_major_mv_merge_info.h"

namespace oceanbase
{
namespace rootserver
{

struct ObMVMergeSCNInfo
{
  ObMVMergeSCNInfo(
      const share::ObLSID &ls_id)
  : ls_id_(ls_id),
    major_mv_merge_scn_publish_(share::SCN::min_scn()),
    major_mv_merge_scn_safe_calc_(share::SCN::min_scn())
  {}
  ObMVMergeSCNInfo()
  : ls_id_(),
    major_mv_merge_scn_publish_(share::SCN::min_scn()),
    major_mv_merge_scn_safe_calc_(share::SCN::min_scn())
  {}
  bool is_valid() const
  { return ls_id_.is_valid() && major_mv_merge_scn_publish_.is_valid()
           && major_mv_merge_scn_safe_calc_.is_valid()
           && major_mv_merge_scn_publish_ >= major_mv_merge_scn_safe_calc_; }


  TO_STRING_KV(K_(ls_id), K_(major_mv_merge_scn_publish), K_(major_mv_merge_scn_safe_calc));

  share::ObLSID ls_id_;
  share::SCN major_mv_merge_scn_publish_;
  share::SCN major_mv_merge_scn_safe_calc_;
};

class ObMVMergeSCNInfoCache
{
public:
  int get_ls_info(
      const share::ObLSID &ls_id,
      ObMVMergeSCNInfo *&ls_info);
  int clear_deleted_ls_info(
      const share::SCN &merge_scn);
  ObArray<ObMVMergeSCNInfo>& get_ls_infos()
  { return arr_; }

  int64_t get_ls_info_cnt()
  { return arr_.count(); }

  void reset()
  { arr_.reset(); }

  TO_STRING_KV(K(arr_.count()), K_(arr));

private:
  ObArray<ObMVMergeSCNInfo> arr_;
};

class ObReplicaSafeCheckTask : public ObMViewTimerTask
{
public:
  ObReplicaSafeCheckTask();
  virtual ~ObReplicaSafeCheckTask();
  DISABLE_COPY_ASSIGN(ObReplicaSafeCheckTask);

  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  // for TimerTask
  void runTimerTask() override;

  static int create_ls_with_tenant_mv_merge_scn(const uint64_t tenant_id,
                                                const share::ObLSID &ls_id,
                                                common::ObMySQLTransaction &trans);

  TO_STRING_KV(K_(status), K_(in_sched), K_(is_stop), K_(is_inited), K_(merge_scn), K_(max_transfer_task_id), K_(ls_cache));
private:
  static const int64_t CHECK_INTERVAL = 30LL * 1000 * 1000;
  static const int64_t ERROR_RETRY_INTERVAL = CHECK_INTERVAL;
  static const int64_t WAIT_END_INTERVAL = 5LL * 1000 * 1000;
  static const int64_t LOCATION_RETRY_INTERVAL = WAIT_END_INTERVAL;

  enum class StatusType
  {
    PUBLISH_SCN = 0,
    CHECK_END = 1,
    NOTICE_SAFE = 2
  };

private:
  void switch_status(StatusType new_status, int64_t delay = 0);

  int publish_scn();
  int check_end();
  int notice_safe();
  // void finish();
  void cleanup();
  int do_multi_trans(
      const transaction::ObTxDataSourceType type,
      const ObUpdateMergeScnArg &arg);
  int check_row_empty(
      const ObSqlString &sql_str,
      bool &is_empty);
  static int get_transfer_task_id(
    share::ObTransferTaskID &max_transfer_task_id);
  
  static int register_mds_in_trans(
      const transaction::ObTxDataSourceType type,
      const ObUpdateMergeScnArg &arg,
      common::ObMySQLTransaction &trans);

private:
  StatusType status_;
  bool in_sched_;
  bool is_stop_;
  bool is_inited_;
  share::SCN merge_scn_;
  share::ObTransferTaskID max_transfer_task_id_;
  ObMVMergeSCNInfoCache ls_cache_;
};

} // namespace rootserver
} // namespace oceanbase
