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


#ifndef OB_SQL_UDR_OB_UDR_MGR_H_
#define OB_SQL_UDR_OB_UDR_MGR_H_
#include "sql/udr/ob_udr_sql_service.h"
#include "sql/udr/ob_udr_item_mgr.h"
#include "lib/task/ob_timer.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"


namespace oceanbase
{
namespace sql
{
class ObUDRMgr;

class UDRBackupRecoveryGuard
{
public:
  UDRBackupRecoveryGuard(ObSqlCtx &sql_ctx, ObPlanCacheCtx &pc_ctx)
    : sql_ctx_(sql_ctx),
      pc_ctx_(pc_ctx),
      is_prepare_protocol_(false),
      cur_sql_()
  {
    backup();
  }

  ~UDRBackupRecoveryGuard()
  {
    recovery();
  }
  void backup();
  void recovery();

private:
  ObSqlCtx &sql_ctx_;
  ObPlanCacheCtx &pc_ctx_;
  bool is_prepare_protocol_;
  common::ObString cur_sql_;
  PlanCacheMode mode_;
};

class UDRTmpAllocatorGuard
{
public:
  UDRTmpAllocatorGuard()
  : mem_context_(nullptr),
    inited_(false) {}
  ~UDRTmpAllocatorGuard();
  int init(const uint64_t tenant_id);
  ObIAllocator* get_allocator();
private:
  lib::MemoryContext mem_context_;
  bool inited_;
};

class ObUDRRefreshTask : public common::ObTimerTask
{
public:
  ObUDRRefreshTask() : rule_mgr_(NULL) {}
  void runTimerTask(void);

public:
  const static int64_t REFRESH_INTERVAL = 5L * 1000L * 1000L; // 5s
  ObUDRMgr* rule_mgr_;
};

class ObUDRMgr
{
  friend class ObUDRRefreshTask;
public:
  ObUDRMgr()
  : mutex_(),
    inner_allocator_(NULL),
    inited_(false),
    destroyed_(false),
    tenant_id_(OB_INVALID_ID),
    tg_id_(-1),
    rule_version_(OB_INIT_REWRITE_RULE_VERSION) {}
  ~ObUDRMgr();
  static int mtl_init(ObUDRMgr* &udr_mgr);
  static void mtl_stop(ObUDRMgr* &udr_mgr);
  void destroy();
  int insert_rule(ObUDRInfo &arg);
  int remove_rule(ObUDRInfo &arg);
  int alter_rule_status(ObUDRInfo &arg);
  int sync_rule_from_inner_table();
  int get_udr_item(const ObUDRContext &rule_ctx,
                   ObUDRItemMgr::UDRItemRefGuard &item_guard,
                   PatternConstConsList *cst_cons_list = NULL);
  int fuzzy_check_by_pattern_digest(const uint64_t pattern_digest, bool &is_exists);
  void set_rule_version(const int64_t version) { ATOMIC_STORE(&rule_version_, version); }
  int64_t get_rule_version() const { return ATOMIC_LOAD(&rule_version_); }

private:
  int init(uint64_t tenant_id);
  int init_mem_context(uint64_t tenant_id);

private:
  lib::ObMutex mutex_;
  lib::MemoryContext mem_context_;
  common::ObIAllocator* inner_allocator_;
  ObUDRSqlService sql_service_;
  ObUDRRefreshTask refresh_task_;
  ObUDRItemMgr rule_item_mgr_;
  bool inited_;
  bool destroyed_;
  uint64_t tenant_id_;
  int tg_id_;
  int64_t rule_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDRMgr);
};


} // namespace sql end
} // namespace oceanbase end
#endif
