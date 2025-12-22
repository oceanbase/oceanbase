/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_H_
#define OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_H_
#include "ob_table_sess_pool.h"
#include "ob_table_object_pool_common.h"
#include "ob_table_system_variable.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace table
{

class ObTableObjectPoolMgr final
{
public:
  ObTableObjectPoolMgr()
      : is_inited_(false),
        allocator_("TbObjPoolMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        sess_pool_(nullptr),
        ls_op_pool_(MTL_ID(), "LsOpPool", static_cast<omt::ObTenant *>(MTL_CTX())->max_worker_cnt(), REQUESE_RESULT_RETIRE_TIME),
        ls_res_pool_(MTL_ID(), "LsOpPool", static_cast<omt::ObTenant *>(MTL_CTX())->max_worker_cnt(), REQUESE_RESULT_RETIRE_TIME)
  {
    ls_op_pool_.set_obj_label("LsOpObj");
    ls_res_pool_.set_obj_label("LsResObj");
  }
  virtual ~ObTableObjectPoolMgr() { destroy(); }
  TO_STRING_KV(K_(is_inited),
               KPC_(sess_pool),
               K_(ls_op_pool),
               K_(ls_res_pool)
               );
public:
  class ObTableSessEliminationTask : public common::ObTimerTask
  {
  public:
    ObTableSessEliminationTask()
        : is_inited_(false),
          obj_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(obj_pool_mgr));
    void runTimerTask(void);
  private:
    // 回收已经淘汰的session
    int run_recycle_retired_sess_task();
    // 淘汰长期未被使用的session
    int run_retire_sess_task();
  public:
    bool is_inited_;
    ObTableObjectPoolMgr *obj_pool_mgr_;
  };
  class ObTableSessSysVarUpdateTask : public common::ObTimerTask
  {
  public:
    ObTableSessSysVarUpdateTask()
        : is_inited_(false),
          obj_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(obj_pool_mgr));
    void runTimerTask(void);
  private:
    int run_update_sys_var_task();
  public:
    bool is_inited_;
    ObTableObjectPoolMgr *obj_pool_mgr_;
  };
  class ObTableReqResEliminationTask : public common::ObTimerTask
  {
  public:
    ObTableReqResEliminationTask()
        : is_inited_(false),
          obj_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(obj_pool_mgr));
    void runTimerTask(void);
  private:
    int run_recycle_retired_request_result_task();
  public:
    bool is_inited_;
    ObTableObjectPoolMgr *obj_pool_mgr_;
  };
  
  class ObTableUserLockStatusRefreshTask : public common::ObTimerTask
  {
  public:
    ObTableUserLockStatusRefreshTask()
        : is_inited_(false),
          obj_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(obj_pool_mgr));
    void runTimerTask(void);
  private:
    int run_refresh_user_lock_status_task();
  public:
    bool is_inited_;
    ObTableObjectPoolMgr *obj_pool_mgr_;
  };
  
public:
  static int mtl_init(ObTableObjectPoolMgr *&mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  int init();
  int get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard);
  int update_sess(ObTableApiCredential &credential);
  int update_sys_vars(bool only_update_dynamic_vars)
  {
    return sys_vars_.update_sys_vars(only_update_dynamic_vars);
  }
public:
  OB_INLINE int64_t get_binlog_row_image() const
  {
    return sys_vars_.dynamic_vars_.get_binlog_row_image();
  }
  OB_INLINE ObKvModeType get_kv_mode() const
  {
    return sys_vars_.static_vars_.get_kv_mode();
  }
  OB_INLINE int64_t get_kv_group_commit_batch_size() const
  {
    return sys_vars_.dynamic_vars_.get_kv_group_commit_batch_size();
  }
  OB_INLINE ObTableGroupRwMode get_group_rw_mode() const
  {
    return sys_vars_.dynamic_vars_.get_group_rw_mode();
  }
  OB_INLINE int64_t get_query_record_size_limit() const
  {
    return sys_vars_.dynamic_vars_.get_query_record_size_limit();
  }
  OB_INLINE bool is_enable_query_response_time_stats() const
  {
    return sys_vars_.dynamic_vars_.is_enable_query_response_time_stats();
  }
  OB_INLINE bool is_support_distributed_execute() const
  {
    return sys_vars_.dynamic_vars_.is_support_distributed_execute();
  }
  OB_INLINE int alloc_ls_op(ObTableLSOp *&op) { return ls_op_pool_.get_object(op); }
  OB_INLINE void free_ls_op(ObTableLSOp *op) { ls_op_pool_.free_object(op); }
  OB_INLINE int alloc_res(ObTableLSOpResult *&res) { return ls_res_pool_.get_object(res); }
  OB_INLINE void free_res(ObTableLSOpResult *res) { ls_res_pool_.free_object(res); }
private:
  int create_session_pool_safe();
  int create_session_pool_unsafe();
private:
  static const int64_t ELIMINATE_SESSION_DELAY = 5 * 1000 * 1000; // 5s
  static const int64_t SYS_VAR_REFRESH_DELAY = 5 * 1000 * 1000; // 5s
  static const int64_t ELIMINATE_RES_RESULT_DELAY = 5 * 1000 * 1000; // 5s
  static const int64_t USER_LOCK_STATUS_REFRESH_DELAY = 5 * 1000 * 1000; // 30s
  static const int64_t REQUESE_RESULT_RETIRE_TIME = 60 * 1000 * 1000 ; // 60s
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObTableApiSessPool *sess_pool_;
  ObSpinLock lock_; // for double check sess pool creating
  ObTableSessEliminationTask sess_elimination_task_;
  ObTableSessSysVarUpdateTask sys_var_update_task_;
  ObTableReqResEliminationTask req_res_elimination_task_;
  ObTableUserLockStatusRefreshTask user_lock_status_refresh_task_;
  ObTableRelatedSysVars sys_vars_;
  ObTableObjectPool<ObTableLSOp> ls_op_pool_;
  ObTableObjectPool<ObTableLSOpResult> ls_res_pool_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableObjectPoolMgr);
};

#define TABLEAPI_OBJECT_POOL_MGR (MTL(ObTableObjectPoolMgr*))

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_OBJECT_POOL_H_ */
