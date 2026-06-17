/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_partition_random_distribution_helper.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_random_partition_helper.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/ddl/ob_ddl_lock.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace storage
{

int ObTenantLSCntInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(tenant_ls_cnt_map_.create(10, "TenantLSCnt"))) {
    LOG_WARN("fail to create tenant_ls_cnt_map", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObTenantLSCntInfo::reset()
{
  inited_ = false;
  tenant_ls_cnt_map_.destroy();
}

// this will also remove keys other than tenant_ids from tenant_ls_cnt_map_
int ObTenantLSCntInfo::get_changed_tenant_ids(const ObIArray<uint64_t> &tenant_ids,
                                              const ObIArray<int64_t> &tenant_ls_arrays,
                                              ObIArray<uint64_t> &changed_tenant_ids)
{
  int ret = OB_SUCCESS;
  changed_tenant_ids.reset();
  if (OB_UNLIKELY(!inited_ || tenant_ids.count() != tenant_ls_arrays.count())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init or invalid arg", K(ret), K(inited_), K(tenant_ids), K(tenant_ls_arrays));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      int64_t ls_count = tenant_ls_arrays.at(i);
      if (ls_count > 0) { // empty means ls balance unfinished
        int64_t old_cnt = -1;
        bool found = false;
        if (OB_SUCC(tenant_ls_cnt_map_.get_refactored(tenant_id, old_cnt))) {
          found = true;
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get old ls cnt", K(ret), K(tenant_id));
        }
        if (OB_FAIL(ret)) {
        } else if (!found || old_cnt != ls_count) {
          if (OB_FAIL(changed_tenant_ids.push_back(tenant_id))) {
            LOG_WARN("fail to push changed tenant_id", K(ret), K(tenant_id));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<uint64_t> to_remove;
      for (hash::ObHashMap<uint64_t, int64_t>::iterator it = tenant_ls_cnt_map_.begin();
           OB_SUCC(ret) && it != tenant_ls_cnt_map_.end(); ++it) {
        const uint64_t tenant_id = it->first;
        if (!is_contain(tenant_ids, tenant_id)) {
          if (OB_FAIL(to_remove.push_back(tenant_id))) {
            LOG_WARN("fail to push to remove array", K(ret), K(tenant_id));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < to_remove.count(); ++i) {
        if (OB_FAIL(tenant_ls_cnt_map_.erase_refactored(to_remove.at(i)))) {
          LOG_WARN("fail to erase tenant from map", K(ret), K(to_remove.at(i)));
        }
      }
      LOG_INFO("erase ls cnt", K(ret), K(to_remove));
    }
  }
  return ret;
}

int ObTenantLSCntInfo::set_tenant_ls_cnt(const uint64_t tenant_id, const ObLSAttrArray &ls_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(inited_));
  } else {
    int64_t ls_count = 0;
    ARRAY_FOREACH(ls_attr_array, idx) {
      const share::ObLSAttr &ls_attr = ls_attr_array.at(idx);
      if (ls_attr.ls_is_normal() && SYS_LS != ls_attr.get_ls_id()
          && !ls_attr.get_ls_flag().is_block_tablet_in() && !ls_attr.get_ls_flag().is_duplicate_ls()) {
        ++ls_count;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_ls_cnt_map_.set_refactored(tenant_id, ls_count, 1/*overwrite*/))) {
      LOG_WARN("fail to erase tenant from map", K(ret), K(tenant_id));
    } else {
      LOG_INFO("update ls cnt", K(tenant_id), K(ls_count), K(tenant_ls_cnt_map_.size()), K(tenant_ls_cnt_map_.bucket_count()));
    }
  }
  return ret;
}

int ObRsRandomPartitionScheduler::mtl_init(ObRsRandomPartitionScheduler *&scheduler)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (!is_sys_tenant(tenant_id)) {
    LOG_INFO("new rs random partition scheduler should run on SYS tenant", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(scheduler)) {
    if (OB_FAIL(scheduler->init())) {
      LOG_WARN("failed to init scheduler", KR(ret));
    }
  }
  FLOG_INFO("finish mtl_init for rs random partition scheduler", KR(ret), K(tenant_id));
  return ret;
}

void ObRsRandomPartitionScheduler::mtl_stop(ObRsRandomPartitionScheduler *&scheduler)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (!is_sys_tenant(tenant_id)) {
    LOG_INFO("new rs random partition scheduler should run on SYS tenant", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(scheduler)) {
    scheduler->reset();
  }
  FLOG_INFO("finish mtl_stop for rs random partition scheduler", KR(ret), K(tenant_id));
}

void ObRsRandomPartitionScheduler::mtl_wait(ObRsRandomPartitionScheduler *&scheduler)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (!is_sys_tenant(tenant_id)) {
    LOG_INFO("new rs random partition scheduler should run on SYS tenant", KR(ret), K(tenant_id));
  } else if (OB_NOT_NULL(scheduler)) {
    // do nothing
  }
  FLOG_INFO("finish mtl_wait for rs random partition scheduler", KR(ret), K(tenant_id));
}

int ObRsRandomPartitionScheduler::switch_to_leader()
{
  int ret = OB_SUCCESS;
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("new ddl scheduler should run on SYS tenant", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to start rs random partition scheduler", K(ret));
  }
  FLOG_INFO("ObRsRandomPartitionScheduler switch leader finish",
            KR(ret), "tenant_id", MTL_ID(), K_(is_inited));
  return ret;
}

int ObRsRandomPartitionScheduler::resume_leader()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(switch_to_leader())) {
    LOG_WARN("resume leader failed", KR(ret));
  }
  FLOG_INFO("ObRsRandomPartitionScheduler resume leader finish",
            KR(ret), "tenant_id", MTL_ID(), K_(is_inited));
  return ret;
}

void ObRsRandomPartitionScheduler::switch_to_follower_forcedly()
{
  switch_to_follower_gracefully();
}

int ObRsRandomPartitionScheduler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("new ddl scheduler should run on SYS tenant", KR(ret), "tenant_id", MTL_ID());
  } else {
    reset();
  }
  FLOG_INFO("ObRsRandomPartitionScheduler switch follower finish",
            KR(ret), "tenant_id", MTL_ID(), K_(is_inited));
  return ret;
}

int ObRsRandomPartitionScheduler::init()
{
  int ret = OB_SUCCESS;
  {
    lib::ObMutexGuard guard(mutex_);
    if (is_inited_) {
      LOG_INFO("rs random partition scheduler already init", K(ret));
    } else if (OB_FAIL(ObRsSplitScheduler::init())) {
      LOG_WARN("failed to init rs split scheduler", K(ret));
    } else if (OB_FAIL(ls_cnt_info_.init())) {
      LOG_WARN("failed to init ls cnt info", K(ret));
    } else {
      reset_direct_cache();
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObRsRandomPartitionScheduler::reset()
{
  lib::ObMutexGuard guard(mutex_);
  ObRsSplitScheduler::reset();
  reset_direct_cache();
  ls_cnt_info_.reset();
  is_inited_ = false;
}

int ObRsRandomPartitionScheduler::push_tasks(const ObArray<ObAutoSplitTask> &task_array)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("random partition scheduler not rs", K(ret), K(is_inited_));
  } else if (OB_FAIL(ObRsSplitScheduler::push_tasks(task_array))) {
    LOG_WARN("failed to push task", K(ret));
  }
  return ret;
}

int ObRsRandomPartitionScheduler::pop_tasks(const int64_t num_tasks_can_pop, const bool throttle_by_table, ObArray<ObAutoSplitTask> &task_array)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("random partition scheduler not rs", K(ret), K(is_inited_));
  } else if (OB_FAIL(ObRsSplitScheduler::pop_tasks(num_tasks_can_pop, throttle_by_table, task_array))) {
    LOG_WARN("failed to push task", K(ret));
  }
  return ret;
}

int ObRsRandomPartitionScheduler::get_changed_tenant_ids(const ObIArray<uint64_t> &tenant_ids,
                                                         const ObIArray<int64_t> &tenant_ls_arrays,
                                                         ObIArray<uint64_t> &changed_tenant_ids)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("random partition scheduler not rs", K(ret), K(is_inited_));
  } else if (OB_FAIL(ls_cnt_info_.get_changed_tenant_ids(tenant_ids, tenant_ls_arrays, changed_tenant_ids))) {
    LOG_WARN("failed to get changed tenant ids", K(ret), K(tenant_ids), K(tenant_ls_arrays));
  }
  return ret;
}

int ObRsRandomPartitionScheduler::set_tenant_ls_cnt(const uint64_t tenant_id, const ObLSAttrArray &ls_attr_array)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("random partition scheduler not rs", K(ret), K(is_inited_));
  } else if (OB_FAIL(ls_cnt_info_.set_tenant_ls_cnt(tenant_id, ls_attr_array))) {
    LOG_WARN("failed to get changed tenant ids", K(ret), K(tenant_id), K(ls_attr_array));
  }
  return ret;
}

int ObRsRandomPartitionScheduler::get_random_distributed_table(const ObIArray<uint64_t> &changed_tenant_ids,
                                                               ObIArray<ObArray<int64_t>> &tenant_table_ids)
{
  int ret = OB_SUCCESS;
  ObSqlString query_sql;
  ObSqlString tenant_id_list_sql;
  tenant_table_ids.reuse();
  if (OB_UNLIKELY(changed_tenant_ids.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("changed tenant ids is empty", K(ret));
  } else if (OB_FAIL(tenant_table_ids.prepare_allocate(changed_tenant_ids.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret), K(changed_tenant_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < changed_tenant_ids.count(); ++i) {
    const uint64_t tenant_id = changed_tenant_ids.at(i);
    if (OB_INVALID_ID != tenant_id) {
      if (0 == i) {
        if (OB_FAIL(tenant_id_list_sql.assign_fmt("%lu", tenant_id))) {
          LOG_WARN("failed to assign sql", K(ret), K(tenant_id));
        }
      } else if (OB_FAIL(tenant_id_list_sql.append_fmt(", %lu", tenant_id))) {
        LOG_WARN("failed to append sql", K(ret), K(tenant_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(tenant_id_list_sql.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_id_list_sql is empty after processing", K(ret), K(changed_tenant_ids));
    } else if (OB_FAIL(query_sql.assign_fmt(
        "select tenant_id, table_id from oceanbase.__all_virtual_table "
        "where tenant_id in (%s) and auto_part = 1 and auto_part_size > 0 and part_func_type = 8 and database_id not in (%ld)",
        tenant_id_list_sql.ptr(), common::OB_RECYCLEBIN_SCHEMA_ID))) {
      LOG_WARN("failed to assign sql", K(ret), K(query_sql));
    } else {
      sqlclient::ObMySQLResult *scan_result = nullptr;
      SMART_VAR(ObMySQLProxy::MySQLResult, scan_res) {
        if (OB_UNLIKELY(!query_sql.is_valid()) || OB_ISNULL(GCTX.sql_proxy_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(query_sql), KP(GCTX.sql_proxy_));
        } else if (OB_FAIL(GCTX.sql_proxy_->read(scan_res, common::OB_SYS_TENANT_ID, query_sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(ret), K(query_sql));
        } else if (OB_ISNULL(scan_result = scan_res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, query result must not be NULL", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(scan_result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("failed to get next row", K(ret));
              }
            } else {
              uint64_t tenant_id = 0;
              int64_t table_id = 0;
              EXTRACT_INT_FIELD_MYSQL(*scan_result, "tenant_id", tenant_id, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*scan_result, "table_id", table_id, int64_t);
              if (OB_SUCC(ret)) {
                int64_t tenant_idx = 0;
                for (; OB_SUCC(ret) && tenant_idx < changed_tenant_ids.count(); tenant_idx++) {
                  if (changed_tenant_ids.at(tenant_idx) == tenant_id) {
                    break;
                  }
                }
                if (OB_UNLIKELY(tenant_idx >= changed_tenant_ids.count() || tenant_idx >= tenant_table_ids.count())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("sql return unexpected tenant_id", K(ret), K(tenant_idx), K(tenant_id), K(changed_tenant_ids), K(tenant_table_ids));
                } else if (OB_FAIL(tenant_table_ids.at(tenant_idx).push_back(table_id))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRsRandomPartitionScheduler::schedule_random_part_task()
{
  int ret = OB_SUCCESS;
  ObRsRandomPartitionScheduler *random_task_scheduler = MTL_WITH_CHECK_TENANT(ObRsRandomPartitionScheduler*, OB_SYS_TENANT_ID);
  ObArray<ObAutoSplitTask> task_array;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(random_task_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid rs random partition scheduler", K(ret), K(MTL_ID()));
  } else if (OB_TMP_FAIL(random_task_scheduler->gc_deleted_tenant_caches())) {
    LOG_WARN("failed to gc split tasks", K(tmp_ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(random_task_scheduler->pop_tasks(ObRsSplitScheduler::MAX_SPLIT_TASKS_ONE_ROUND/*num_tasks_to_pop*/, false/*throttle_by_table*/, task_array))) {
    LOG_WARN("fail to pop tasks from random_split_task_tree");
  } else if (task_array.count() == 0) {
    //do nothing
  } else {
    ObRandomPartitionArgBuilder random_part_helper;
    ObArray<ObAutoSplitTask> failed_task;
    common::ObMalloc allocator(common::ObMemAttr(OB_SERVER_TENANT_ID, "split_sched"));
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      tmp_ret = OB_SUCCESS;
      ObAutoSplitTask &task = task_array.at(i);
      void *buf = nullptr;
      obrpc::ObAlterTableArg *single_arg = nullptr;
      obrpc::ObAlterRandomPartitionRes res;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(obrpc::ObAlterTableArg)))) {
        //ignore ret
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(tmp_ret), K(task));
      } else if (FALSE_IT(single_arg = new (buf) obrpc::ObAlterTableArg())) {
      } else if (OB_TMP_FAIL(random_part_helper.build_arg(task.tenant_id_, task.table_id_, task.inactive_tablet_ids_, 0/*specified_value*/, 0/*available_ls_cnt*/, *single_arg))) {
        LOG_WARN("fail to build arg", K(tmp_ret), K(task));
      } else if (!single_arg->is_random_partition()) {
        //do nothing
      } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(tmp_ret), KP(GCTX.rs_rpc_proxy_));
      } else {
        if (OB_TMP_FAIL(GCTX.rs_rpc_proxy_->to(GCTX.self_addr()).timeout(GCONF._ob_ddl_timeout).alter_random_distribution_partition(*single_arg, res))) {
          LOG_WARN("alter table failed", K(tmp_ret), K(single_arg));
        }
      }
      if (OB_TMP_FAIL(tmp_ret) && ObRsSplitScheduler::can_retry(task, tmp_ret)) {
        failed_task.reuse();
        task.increment_retry_times();
        if (OB_TMP_FAIL(failed_task.push_back(task))) {
          LOG_WARN("fail to push back into failed task", K(tmp_ret), K(task));
        } else if (OB_TMP_FAIL(random_task_scheduler->push_tasks(failed_task))) {
          LOG_WARN("fail to push tasks", K(tmp_ret), K(failed_task));
        }
      }
      if (OB_NOT_NULL(single_arg)) {
        single_arg->~ObAlterTableArg();
        allocator.free(single_arg);
        single_arg = nullptr;
      }
    }
  }
  return ret;
}

int ObRsRandomPartitionScheduler::refresh_auto_random_part()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> all_tenant_ids;
  ObArray<uint64_t> changed_tenant_ids;
  ObArray<ObArray<int64_t>> auto_random_tenant_table_ids;
  ObArray<int64_t> tenant_ls_arrays;
  ObRsRandomPartitionScheduler *scheduler = MTL_WITH_CHECK_TENANT(ObRsRandomPartitionScheduler*, OB_SYS_TENANT_ID);
  if (OB_ISNULL(scheduler) || OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema service or sql proxy", K(ret), K(MTL_ID()), KP(scheduler), KP(GCTX.schema_service_),
        KP(GCTX.sql_proxy_), KP(GCTX.rs_rpc_proxy_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_ids(all_tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else if (OB_FAIL(tenant_ls_arrays.prepare_allocate(all_tenant_ids.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant_ids.count(); i++) {
      const uint64_t tenant_id = all_tenant_ids.at(i);
      bool is_primary = false;
      if (OB_FAIL(ObAllTenantInfoProxy::is_primary_tenant(GCTX.sql_proxy_, tenant_id, is_primary))) {
        LOG_WARN("fail to execute is_primary_tenant", KR(ret), K(tenant_id));
      } else if (is_primary && is_user_tenant(tenant_id)) {
        bool is_finished = true;
        if (OB_FAIL(rootserver::ObTenantBalanceService::is_ls_balance_finished(tenant_id, is_finished))) {
          LOG_WARN("failed to check is ls balance finished", K(ret), K(tenant_id));
        } else if (is_finished) {
          if (OB_FAIL(rootserver::ObRandomPartitionHelper::get_available_ls_cnt(tenant_id, tenant_ls_arrays.at(i)))) {
            LOG_WARN("fail to load all ls", KR(ret), K(tenant_id));
          }
        } else {
          tenant_ls_arrays.at(i) = 0;
          LOG_INFO("tenant ls balance not finished", K(tenant_id));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(scheduler->get_changed_tenant_ids(all_tenant_ids, tenant_ls_arrays, changed_tenant_ids))) {
    LOG_WARN("fail to get changed tenant ids", K(ret));
  } else if (changed_tenant_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(get_random_distributed_table(changed_tenant_ids, auto_random_tenant_table_ids))) {
    LOG_WARN("fail to get random distributed table", K(ret));
  } else if (OB_UNLIKELY(changed_tenant_ids.count() != auto_random_tenant_table_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tenant table ids cnt", K(ret), K(changed_tenant_ids), K(auto_random_tenant_table_ids));
  } else {
    for (int64_t tenant_idx = 0; tenant_idx < changed_tenant_ids.count(); tenant_idx++) { // overwrite ret
      const uint64_t tenant_id = changed_tenant_ids.at(tenant_idx);
      common::ObArenaAllocator allocator(common::ObMemAttr(OB_SERVER_TENANT_ID, "DDLRandomDist"));
      obrpc::ObAlterTableArg arg;
      ObRandomPartitionArgBuilder random_part_helper;

      int64_t tenant_ls_array_idx = 0;
      int64_t available_ls_cnt = 0;
      for (; OB_SUCC(ret) && tenant_ls_array_idx < all_tenant_ids.count(); tenant_ls_array_idx++) {
        if (all_tenant_ids.at(tenant_ls_array_idx) == tenant_id) {
          break;
        }
      }
      if (OB_UNLIKELY(tenant_ls_array_idx >= all_tenant_ids.count() || tenant_ls_array_idx >= tenant_ls_arrays.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid idx", K(ret), K(tenant_ls_array_idx), K(all_tenant_ids), K(tenant_ls_arrays));
      } else {
        available_ls_cnt = tenant_ls_arrays.at(tenant_ls_array_idx);
      }

      if (OB_SUCC(ret)) {
        int first_failed_ret = OB_SUCCESS;
        for (int64_t i = 0; i < auto_random_tenant_table_ids.at(tenant_idx).count(); i++) { // overwrite ret
          const int64_t table_id = auto_random_tenant_table_ids.at(tenant_idx).at(i);
          arg.reset();
          obrpc::ObAlterRandomPartitionRes res;
          if (OB_FAIL(random_part_helper.build_arg(tenant_id, table_id, ObArray<ObTabletID>(), 0/*specified_value*/, available_ls_cnt, arg))) {
            LOG_WARN("fail to build arg", K(ret), K(tenant_id), K(table_id));
          } else if (!arg.is_random_partition()) {
            // do nothing
          } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(GCTX.self_addr())
                                 .timeout(GCONF._ob_ddl_timeout)
                                 .alter_random_distribution_partition(arg, res))) {
            LOG_WARN("alter table failed", K(ret), K(tenant_id), K(table_id), K(arg));
          }
          if (OB_FAIL(ret) && OB_SUCCESS == first_failed_ret) {
            first_failed_ret = ret;
          }
        }
        ret = first_failed_ret;
        LOG_INFO("refresh random partition tables for tenant", K(ret), K(tenant_id), K(auto_random_tenant_table_ids.at(tenant_idx)));
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(scheduler->set_tenant_ls_cnt(tenant_id, available_ls_cnt))) {
          LOG_WARN("failed to update tenant ls cnt", K(ret), K(tenant_id), K(available_ls_cnt));
        }
      }
    }
  }
  return ret;
}

ObServerRandomPartitionScheduler &ObServerRandomPartitionScheduler::get_instance()
{
  static ObServerRandomPartitionScheduler instance;
  return instance;
}

int ObServerRandomPartitionScheduler::check_and_fetch_tablet_split_info(const storage::ObTabletHandle &tablet_handle,
                                                                  storage::ObLS &ls,
                                                                  bool &need_random_part,
                                                                  ObAutoSplitTask &task)
{
  int ret = OB_SUCCESS;
  int64_t used_disk_space = OB_INVALID_SIZE;
  int64_t random_part_tablet_size = OB_INVALID_SIZE;
  int64_t real_random_part_size = OB_INVALID_SIZE;
  bool is_active = false;
  ObTablet *tablet = nullptr;
  ObRole role = INVALID_ROLE;
  const share::ObLSID ls_id = ls.get_ls_id();
  need_random_part = false;
  task.reset();
  ObTabletRandomMdsUserData rand_part_data;
  mds::MdsWriter writer;// will be removed later
  mds::TwoPhaseCommitState trans_stat;// will be removed later
  share::SCN trans_version;// will be removed later
  ObSEArray<uint64_t, 1> table_ids;
  ObTabletRandomMdsUserData random_part_data;
  if (OB_UNLIKELY(!tablet_handle.is_valid() || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle), K(ls_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer to tablet is nullptr", K(ret), KP(tablet));
  } else if (OB_FAIL(ls.get_ls_role(role))) {
    LOG_WARN("get role failed", K(ret), K(MTL_ID()), K(ls_id));
  } else if (0 >= tablet->get_major_table_count() || common::ObRole::LEADER != role || OB_SYS_TENANT_ID == MTL_ID()) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(tablet->ObITabletMdsCustomizedInterface::get_latest_random_part_data(
      random_part_data, writer, trans_stat, trans_version))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
      random_part_tablet_size = OB_INVALID_SIZE;
    } else {
      LOG_WARN("fail to get split data", K(ret), KP(tablet));
    }
  } else if (OB_FAIL(random_part_data.get_random_part_data(random_part_tablet_size, is_active))) {
    need_random_part = false;
    random_part_tablet_size = OB_INVALID_SIZE;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_SIZE == random_part_tablet_size || !is_active) {
    need_random_part = false;
  } else if (OB_FAIL(check_tablet_creation_limit(1 /*inc_tablet_cnt*/, 0.8/*safe_ratio*/, random_part_tablet_size, real_random_part_size))) {
    LOG_WARN("check_create_new_tablets fail", K(ret));
    if (OB_TOO_MANY_PARTITIONS_ERROR == ret) {
      need_random_part = false;
      ret = OB_SUCCESS;
    }
  } else if (real_random_part_size != OB_INVALID_SIZE) {
    used_disk_space = OB_MAX(tablet->get_tablet_meta().space_usage_.all_sstable_data_required_size_, used_disk_space);
    need_random_part = used_disk_space > real_random_part_size;
    if (OB_SUCC(ret) && need_random_part) {
      ObTabletCreateDeleteMdsUserData user_data;
      common::ObArenaAllocator allocator;
      const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
      if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(),
          user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
        LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
        need_random_part = false;
      } else if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_info_list))) {
        LOG_WARN("failed to get medium info list", K(ret), KPC(tablet));
        need_random_part = false;
      } else if ((need_random_part = user_data.get_tablet_status() == ObTabletStatus::Status::NORMAL && (medium_info_list->size() == 0))) {
        task.tenant_id_ = MTL_ID();
        task.ls_id_ = ls_id;
        task.tablet_id_ = tablet->get_data_tablet_id();
        task.auto_split_tablet_size_ = random_part_tablet_size;
        task.used_disk_space_ = used_disk_space;
        task.retry_times_ = 0;
        task.is_random_part_ = true;
        if (OB_FAIL(task.inactive_tablet_ids_.push_back(tablet->get_data_tablet_id()))) {
          LOG_WARN("failed to push back bablets_id", K(ret), KPC(tablet));
        } else if (OB_FAIL(ObAutoSplitArgBuilder::acquire_table_id_of_tablets(MTL_ID(), task.inactive_tablet_ids_, table_ids))) {
          LOG_WARN("failed to acquire table id of tablets", K(ret), K(task));
        } else if (1 != table_ids.count() || table_ids.at(0) == OB_INVALID_ID) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table id", K(ret), K(table_ids));
        } else {
          task.table_id_ = table_ids.at(0);
        }
      }
    }
  }
  return ret;
}

// non-zero available_ls_cnt is for optimization: when active tablet cnt is equal to available_ls_cnt, no need to change active tablet ids
// and return arg.is_random_partition() = false
int ObRandomPartitionArgBuilder::build_arg(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<ObTabletID> &inactive_tablet_ids,
                                     const uint64_t specified_value,
                                     const int64_t available_ls_cnt,
                                     obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = nullptr;
  const share::schema::ObSimpleDatabaseSchema *db_schema = nullptr;
  ObSplitSampler sampler;
  common::ObArenaAllocator range_allocator;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  share::schema::ObSchemaGetterGuard guard;
  arg.reset();
  bool need_alter = true;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_ID || table_id == OB_INVALID_ID || OB_ISNULL(schema_service) || available_ls_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(available_ls_cnt),
                                 K(inactive_tablet_ids), K(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(acquire_schema_info_of_table_(tenant_id, table_id, table_schema, db_schema, guard, arg))) {
    LOG_WARN("fail to acquire schema info of tablet", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(db_schema) || table_schema->is_in_recyclebin()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(table_id), KPC(table_schema), KPC(db_schema));
  } else if (OB_UNLIKELY(!table_schema->is_random_part())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("table is not a random partition table", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(rootserver::ObRandomPartitionHelper::check_enable_random_partition(*table_schema))) {
    LOG_WARN("table is not valid for random partition", KR(ret), K(tenant_id), K(table_id), KPC(table_schema));
  } else if (available_ls_cnt != 0) {
    const ObArray<ObTabletID> empty_inactive_tablet_ids;
    ObArray<int64_t> marked_inactive_part_indexs;
    ObArray<ObTabletID> active_tablet_ids;
    if (OB_FAIL(rootserver::ObRandomPartitionHelper::get_active_tablets(*table_schema,
                                                                        empty_inactive_tablet_ids,
                                                                        marked_inactive_part_indexs,
                                                                        active_tablet_ids))) {
      LOG_WARN("fail to get active tablets", KR(ret));
    } else {
      need_alter = available_ls_cnt != active_tablet_ids.count();
    }
  }
  if (OB_SUCC(ret) && need_alter) {
    if (OB_FAIL(build_arg_(tenant_id, db_schema->get_database_name_str(),
                            *table_schema, table_id, inactive_tablet_ids, specified_value, arg))) {
      LOG_WARN("fail to build split arg", KR(ret), K(tenant_id), KPC(db_schema),
                                          KPC(table_schema), K(table_id), K(inactive_tablet_ids));
    }
  }
  return ret;
}

int ObRandomPartitionArgBuilder::acquire_schema_info_of_table_(const uint64_t tenant_id,
                                                          const uint64_t table_id,
                                                          const share::schema::ObTableSchema *&table_schema,
                                                          const share::schema::ObSimpleDatabaseSchema *&db_schema,
                                                          share::schema::ObSchemaGetterGuard &guard,
                                                          obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  if (tenant_id == OB_INVALID_ID || table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(tenant_id), K(table_id), K(table_id));
  } else if (OB_FAIL(arg.based_schema_object_infos_.push_back(ObBasedSchemaObjectInfo(table_schema->get_table_id(),
      schema::TABLE_SCHEMA, table_schema->get_schema_version(), table_schema->get_tenant_id())))) {
    LOG_WARN("fail to push back into based_schema_object_infos_", K(ret));
  } else if (FALSE_IT(db_id = table_schema->get_database_id())){
  } else if (OB_FAIL(guard.get_database_schema(tenant_id, db_id, db_schema))) {
    LOG_WARN("fail to get database schema", KR(ret), K(tenant_id), K(table_id), K(table_id));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(tenant_id), K(db_id), K(table_id), K(table_id));
  }
  return ret;
}

int ObRandomPartitionArgBuilder::build_arg_(const uint64_t tenant_id,
                                      const ObString &db_name,
                                      const share::schema::ObTableSchema &table_schema,
                                      const uint64_t table_id,
                                      const ObIArray<ObTabletID> &inactive_tablet_ids,
                                      const uint64_t specified_value,
                                      obrpc::ObAlterTableArg &arg)
{
  int ret = OB_SUCCESS;
  ObTZMapWrap tz_map_wrap;
  share::schema::AlterTableSchema& alter_table_schema = arg.alter_table_schema_;
  if (tenant_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(build_alter_table_schema_(tenant_id, db_name, table_schema,
                                               table_id,
                                               alter_table_schema))) {
    LOG_WARN("fail to build alter_table_schema", KR(ret), K(tenant_id), K(db_name),
                                                 K(table_schema), K(table_id));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap))) {
    LOG_WARN("get tenant timezone map failed", KR(ret), K(tenant_id));
  } else {
    arg.is_inner_ = true;
    arg.exec_tenant_id_ = tenant_id;
    arg.table_id_ = table_id;
    arg.is_add_to_scheduler_ = false;
    arg.tz_info_wrap_.set_tz_info_offset(0);
    arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT;
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
    arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
    arg.set_tz_info_map(tz_map_wrap.get_tz_map());
    arg.is_alter_random_partition_ = true;
    arg.alter_random_partition_arg_.specified_value_ = specified_value;
    if (OB_FAIL(arg.alter_random_partition_arg_.inactive_tablets_.assign(inactive_tablet_ids))) {
      LOG_WARN("fail to assign inactive tablets", KR(ret), K(inactive_tablet_ids));
    }
  }
  return ret;
}

int ObRandomPartitionArgBuilder::build_alter_table_schema_(const uint64_t tenant_id,
                                                     const ObString &db_name,
                                                     const share::schema::ObTableSchema &table_schema,
                                                     const uint64_t table_id,
                                                     share::schema::AlterTableSchema &alter_table_schema)
{
  int ret = OB_SUCCESS;
  const ObString& table_name = table_schema.get_table_name_str();
  const ObString& part_func_expr = table_schema.get_part_option().get_part_func_expr_str();
  const ObPartitionFuncType part_func_type = table_schema.get_part_option().get_part_func_type();
  const ObPartitionLevel target_part_level = table_schema.get_part_level();

  if (OB_FAIL(alter_table_schema.set_origin_database_name(db_name))) {
    LOG_WARN("fail to set origin database name", KR(ret), K(db_name));
  } else if (OB_FAIL(alter_table_schema.set_origin_table_name(table_name))) {
    LOG_WARN("fail to set origin table name", KR(ret), K(table_name));
  } else if (FALSE_IT(alter_table_schema.set_table_type(table_schema.get_table_type()))) {
  } else if (FALSE_IT(alter_table_schema.set_index_type(table_schema.get_index_type()))) {
  } else if (FALSE_IT(alter_table_schema.set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(alter_table_schema.set_part_level(target_part_level))) {
  } else if (FALSE_IT(alter_table_schema.get_part_option().set_part_func_type(part_func_type))) {
  } else if (FALSE_IT(alter_table_schema.get_part_option().set_part_expr(part_func_expr))) {
  }
  return ret;
}

}//storage
}//oceanbase
