/**
 * Copyright (c) 2024 OceanBase
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

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/task/ob_timer.h"
#include "rootserver/mview/ob_mview_timer_task.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/ob_table_range.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
}

namespace rootserver
{
// just for check major mv safety
struct ObMajorMVMergeInfo
{
public:
  ObMajorMVMergeInfo() : mview_id_(0),
                         data_table_id_(0),
                         last_refresh_scn_(0),
                         tablet_id_(0),
                         svr_addr_(),
                         ls_id_(),
                         end_log_scn_(0),
                         is_member_(false),
                         is_learner_(false)
                         {}
  ~ObMajorMVMergeInfo() {}
  TO_STRING_KV(K_(mview_id), K_(data_table_id), K_(last_refresh_scn), K_(tablet_id),
      K_(svr_addr), K_(ls_id), K_(end_log_scn), K_(is_member), K_(is_learner));
  bool is_valid() {
    return mview_id_ > 0 && data_table_id_ > 0 && last_refresh_scn_ > 0
      && svr_addr_.is_valid() && ls_id_.is_valid() && tablet_id_ > 0;
  }
  bool is_equal_node(ObMajorMVMergeInfo &other) {
    return mview_id_ == other.mview_id_ && data_table_id_ == other.data_table_id_
      && last_refresh_scn_ == other.last_refresh_scn_ && tablet_id_ == other.tablet_id_
      && svr_addr_ == other.svr_addr_ && ls_id_ == other.ls_id_;
  }
public:
  int64_t mview_id_;
  int64_t data_table_id_;
  uint64_t last_refresh_scn_;
  int64_t tablet_id_;
  ObAddr svr_addr_;
  share::ObLSID ls_id_;
  uint64_t end_log_scn_;
  bool is_member_;
  bool is_learner_;
};

class ObMViewPushRefreshScnTask : public ObMViewTimerTask
{
public:
  ObMViewPushRefreshScnTask();
  virtual ~ObMViewPushRefreshScnTask();
  DISABLE_COPY_ASSIGN(ObMViewPushRefreshScnTask);
  // for Service
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  // for TimerTask
  void runTimerTask() override;

  static int check_major_mv_refresh_scn_safety(const uint64_t tenant_id);
  static const int64_t MVIEW_PUSH_REFRESH_SCN_INTERVAL = 30 * 1000 * 1000; // 30s
private:
  static int get_major_mv_merge_info_(const uint64_t tenant_id,
                               ObISQLClient &sql_client,
                               ObIArray<ObMajorMVMergeInfo> &merge_info_array);
  static int update_major_refresh_mview_scn_(
      const uint64_t tenant_id,
      const share::SCN &major_refresh_mview_scn,
      ObMySQLTransaction &trans);
private:
  bool is_inited_;
  bool in_sched_;
  bool is_stop_;
  uint64_t tenant_id_;
};

struct ObMajorRefreshMViewScnArg final
{
  OB_UNIS_VERSION(1);
public:
  ObMajorRefreshMViewScnArg()
  : major_refresh_mview_scn_(share::ObScnRange::MIN_SCN)
  {}
  ~ObMajorRefreshMViewScnArg() = default;
  bool is_valid() const { return major_refresh_mview_scn_.is_valid(); }
  void reset() { return major_refresh_mview_scn_.set_min(); }

  TO_STRING_KV(K_(major_refresh_mview_scn));

  share::SCN major_refresh_mview_scn_;
};


} // namespace rootserver
} // namespace oceanbase
