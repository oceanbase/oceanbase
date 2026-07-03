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

#ifndef OCEANBASE_SHARE_OB_VECTOR_MEM_SYNC_EXECUTOR_H_
#define OCEANBASE_SHARE_OB_VECTOR_MEM_SYNC_EXECUTOR_H_

#include "lib/container/ob_se_array.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/tx/ob_trans_define.h"
#include "logservice/ob_append_callback.h"

namespace oceanbase
{
namespace share
{

class ObVecMemSyncExecutor;
typedef common::ObSEArray<common::ObTabletID, 100> ObVectorIndexTabletIDArray;
typedef common::ObSEArray<uint64_t, 100> ObVectorIndexTableIDArray;

// ObVecMemSyncLogCb - callback for mem sync log submission
class ObVecMemSyncLogCb : public logservice::AppendCb
{
public:
  ObVecMemSyncLogCb();
  ~ObVecMemSyncLogCb();

  void reset();
  void destroy();

  int on_success() override;
  int on_failure() override;

  TO_STRING_KV(K_(is_callback_invoked), K_(is_success), KP_(log_buffer));
  OB_INLINE bool is_invoked() const { return ATOMIC_LOAD(&is_callback_invoked_); }
  OB_INLINE bool is_success() const { return ATOMIC_LOAD(&is_success_); }
  const char *get_cb_name() const override { return "VecMemSyncLogCb"; }

public:
  static const uint32 VECTOR_INDEX_SYNC_LOG_MAX_LENGTH = 16 * 1024; // Max 16KB each log
  static const uint32_t VECTOR_INDEX_MAX_SYNC_COUNT = 512;
  class ObVecMemSyncExecutor *executor_;
  char *log_buffer_;

private:
  bool is_callback_invoked_;
  bool is_success_;
};

class ObVecMemSyncTask : public ObVecIndexIAsyncTask
{
public:
  ObVecMemSyncTask()
    : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "VecMemSyncTask")),
      ls_(nullptr),
      read_snapshot_()
  {}

  explicit ObVecMemSyncTask(const ObMemAttr &mem_attr)
    : ObVecIndexIAsyncTask(mem_attr),
      ls_(nullptr),
      read_snapshot_()
  {}

  virtual ~ObVecMemSyncTask() {}

  int init(uint64_t tenant_id, ObLSID ls_id, ObVecIndexAsyncTaskCtx *task_ctx);

  virtual int do_work() override;

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_id), KPC_(ctx));

private:
  int process_one();

private:
  storage::ObLS *ls_;
  SCN read_snapshot_;

  DISALLOW_COPY_AND_ASSIGN(ObVecMemSyncTask);
};

class ObVecMemSyncExecutor : public ObVecITaskExecutor
{
public:
  ObVecMemSyncExecutor()
    : ObVecITaskExecutor(),
      need_refresh_(true),
      logging_lock_(common::ObLatchIds::OB_PLUGIN_VECTOR_INDEX_SCHEDULER_LOGGING_LOCK),
      is_logging_(false),
      cb_()
  {}

  virtual ~ObVecMemSyncExecutor() {}

  int init(uint64_t tenant_id, storage::ObLSHandle &ls_handle);

  int load_triggered_task(const ObVecIndexTaskStatus &task_row) override;
  virtual int load_task(uint64_t &task_trace_base_num) override;
  virtual int check_and_set_thread_pool() override;
  void set_need_refresh(const bool need_refresh) { need_refresh_ = need_refresh; }

  // clog callback handler
  int handle_submit_callback(const bool success);

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(ls_handle));

private:
  virtual bool check_operation_allow() override;

  int log_tablets_need_memdata_sync(ObPluginVectorIndexMgr *index_ls_mgr);

  int submit_log_(ObVectorIndexTabletIDArray &tablet_id_array,
                  ObVectorIndexTableIDArray &table_id_array);

private:
  bool need_refresh_;
  // clog related members
  common::ObSpinLock logging_lock_;
  bool is_logging_;
  ObVecMemSyncLogCb cb_;

  DISALLOW_COPY_AND_ASSIGN(ObVecMemSyncExecutor);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_VECTOR_MEM_SYNC_EXECUTOR_H_
