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

#ifndef OCEABASE_STORAGE_REBUILD_SERVICE_
#define OCEABASE_STORAGE_REBUILD_SERVICE_

#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_reentrant_thread.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/container/ob_se_array.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/scn.h"
#include "ob_storage_ha_struct.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

struct ObLSRebuildCtx final
{
  ObLSRebuildCtx();
  ~ObLSRebuildCtx() = default;
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(ls_id), K_(type), K_(task_id));
  share::ObLSID ls_id_;
  ObLSRebuildType type_;
  share::ObTaskId task_id_;
};

struct ObLSRebuildInfoHelper
{
public:
  static int check_can_change_info(
      const ObLSRebuildInfo &old_info,
      const ObLSRebuildInfo &new_info,
      bool &can_change);
  static int get_next_rebuild_info(
      const ObLSRebuildInfo &curr_info,
      const ObLSRebuildType &rebuild_type,
      const int32_t result,
      const ObMigrationStatus &status,
      ObLSRebuildInfo &next_info);
};

class ObRebuildService : public lib::ThreadPool
{
public:
  ObRebuildService();
  virtual ~ObRebuildService();
  static int mtl_init(ObRebuildService *&rebuild_service);

  int init(ObLSService *ls_service);
  void destroy();
  void run1() final;
  void wakeup();
  void stop();
  void wait();
  int start();
  int add_rebuild_ls(
      const share::ObLSID &ls_id,
      const ObLSRebuildType &rebuild_type);
  int finish_rebuild_ls(
      const share::ObLSID &ls_id,
      const int32_t result);
  int check_ls_need_rebuild(
      const share::ObLSID &ls_id,
      bool &need_rebuild);
  int remove_rebuild_ls(
      const share::ObLSID &ls_id);
private:
  int build_rebuild_ctx_map_();
  int scheduler_rebuild_mgr_();
  int do_rebuild_mgr_(const ObLSRebuildCtx &rebuild_ctx);
  int inner_remove_rebuild_ls_(
      const share::ObLSID &ls_id);
  int check_rebuild_ctx_map_();
  int get_ls_rebuild_ctx_array_(common::ObIArray<ObLSRebuildCtx> &rebuild_ctx_array);
  int build_ls_rebuild_info_();
  int check_can_rebuild_(
      const ObLSRebuildCtx &rebuild_ctx,
      ObLS *ls,
      bool &can_rebuild);

private:
  static const int64_t SCHEDULER_WAIT_TIME_MS = 5 * 60 * 1000L; // 5min
  static const int64_t WAIT_SERVER_IN_SERVICE_TIME_MS = 1000; //1s
  static const int64_t MAX_BUCKET_NUM = 128;
  typedef common::hash::ObHashMap<share::ObLSID, ObLSRebuildCtx> LSRebuildCtxMap;

  bool is_inited_;
  common::ObThreadCond thread_cond_;
  int64_t wakeup_cnt_;
  ObLSService *ls_service_;
  common::SpinRWLock map_lock_;
  LSRebuildCtxMap rebuild_ctx_map_;
  DISALLOW_COPY_AND_ASSIGN(ObRebuildService);
};

class ObLSRebuildMgr final
{
public:
  ObLSRebuildMgr();
  ~ObLSRebuildMgr();
  int init(
      const ObLSRebuildCtx &rebuild_ctx,
      ObLSService *ls_service);
  int process();
  int finish_ls_rebuild(const int32_t result);

private:
  void wakeup_();
  int do_with_init_status_(const ObLSRebuildInfo &rebuild_info);
  int do_with_doing_status_(const ObLSRebuildInfo &rebuild_info);
  int do_with_cleanup_status_(const ObLSRebuildInfo &rebuild_info);

  int switch_next_status_(
      const ObLSRebuildInfo &curr_rebuild_info,
      const int32_t result);
  int generate_rebuild_task_();
  int get_ls_info_(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::ObLSInfo &ls_info);

private:
  bool is_inited_;
  ObLSRebuildCtx rebuild_ctx_;
  ObLSHandle ls_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRebuildMgr);
};


}
}
#endif
