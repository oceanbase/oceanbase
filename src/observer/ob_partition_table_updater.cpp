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

#define USING_LOG_PREFIX SERVER

#include "ob_partition_table_updater.h"

#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/ob_running_mode.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/ob_running_mode.h"
#include "share/ob_debug_sync.h"
#include "common/ob_timeout_ctx.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_task_define.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/ob_i_partition_report.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_schema_status_proxy.h"
#include "storage/ob_pg_mgr.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
namespace observer {

using namespace common;
using namespace share;

const ObPartitionKey ObPTUpdateTask::report_merge_finish_pkey_(INT64_MAX, INT32_MAX, INT32_MAX);
const ObPartitionKey ObPTUpdateTask::sync_pt_key_(
    INT64_MAX, INT32_MAX - 1, INT32_MAX - 1);  // TODO make sure this function
const int64_t ObPTUpdateTask::rpc_task_group_id_(INT64_MAX >> OB_TENANT_ID_SHIFT);

ObPTUpdateTask::~ObPTUpdateTask()
{}

// this func is used to check if this task can be executed using batch mode
bool ObPTUpdateTask::need_process_alone() const
{
  bool bret = false;
  if (OB_ALL_CORE_TABLE_TID == extract_pure_id(part_key_.table_id_)) {
    bret = true;
  }
  return bret;
}

int ObPTUpdateTask::set_update_task(
    const ObPartitionKey& part_key, const int64_t data_version, const int64_t first_submit_time, const bool with_role)
{
  int ret = OB_SUCCESS;
  if (!part_key.is_valid() || data_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(data_version));
  } else {
    part_key_ = part_key;
    data_version_ = data_version;
    first_submit_time_ = first_submit_time;
    is_remove_ = false;
    with_role_ = is_sys_table(part_key.get_table_id()) ? true : with_role;
  }
  return ret;
}

int ObPTUpdateTask::set_remove_task(const ObPartitionKey& part_key, const int64_t first_submit_time)
{
  int ret = OB_SUCCESS;
  if (!part_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key));
  } else {
    part_key_ = part_key;
    data_version_ = 0;
    first_submit_time_ = first_submit_time;
    is_remove_ = true;
    // no need to set with role
  }
  return ret;
}

bool ObPTUpdateTask::is_valid() const
{
  return part_key_.is_valid();
}

void ObPTUpdateTask::check_task_status() const
{
  int64_t now = ObTimeUtility::current_time();
  // 30 minutes by default
  int64_t interval = GCONF.partition_table_check_interval;
  // need to print an error log if this task is not executed correctly since two minuts ago
  int64_t safe_interval = interval / 15;
  if (now - first_submit_time_ > safe_interval) {
    LOG_ERROR("partition table update task cost too much time to execute",
        K(*this),
        K(safe_interval),
        "cost_time",
        now - first_submit_time_,
        K(interval));
  }
}

int64_t ObPTUpdateTask::hash() const
{
  int64_t hash_val = 0;
  if (!is_valid()) {
    LOG_WARN("invalid argument", "self", *this);
  } else {
    hash_val = part_key_.hash();
  }
  return hash_val;
}

bool ObPTUpdateTask::operator==(const ObPTUpdateTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else if (&other == this) {
    equal = true;
  } else {
    equal = (part_key_ == other.part_key_ && other.data_version_ == data_version_ && other.is_remove_ == is_remove_ &&
             other.with_role_ == with_role_);
  }
  return equal;
}

bool ObPTUpdateTask::operator<(const ObPTUpdateTask& other) const
{
  return (part_key_ < other.part_key_);
}

bool ObPTUpdateTask::compare_without_version(const ObPTUpdateTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_ERROR("invalid argument", "self", *this, K(other));
  } else if (&other == this) {
    equal = true;
  } else {
    equal = (part_key_ == other.part_key_ && with_role_ == other.with_role_);
  }
  return equal;
}

bool ObPTUpdateTask::is_barrier() const
{
  return is_barrier(part_key_);
}

bool ObPTUpdateTask::is_barrier(const common::ObPartitionKey& pkey)
{
  return report_merge_finish_pkey_ == pkey || sync_pt_key_ == pkey;
}

ObPTUpdateRoleTask::~ObPTUpdateRoleTask()
{}

bool ObPTUpdateRoleTask::is_valid() const
{
  return pkey_.is_valid();
}

// this func is used to check if this task can be executed using batch mode
bool ObPTUpdateRoleTask::need_process_alone() const
{
  bool bool_ret = false;
  return bool_ret;
}

void ObPTUpdateRoleTask::check_task_status() const
{
  int64_t now = ObTimeUtility::current_time();
  // 30 minutes by default
  int64_t interval = GCONF.partition_table_check_interval;
  // need to print an error log if this task is not executed correctly since two minuts ago
  int64_t safe_interval = interval / 15;
  if (now - first_submit_time_ > safe_interval) {
    LOG_ERROR("partition table update task cost too much time to execute",
        K(*this),
        K(safe_interval),
        "cost_time",
        now - first_submit_time_,
        K(interval));
  }
}

int64_t ObPTUpdateRoleTask::hash() const
{
  return pkey_.hash();
}

bool ObPTUpdateRoleTask::operator==(const ObPTUpdateRoleTask& other) const
{
  bool equal = false;
  if (!is_valid() || !other.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, K(other));
  } else if (this == &other) {
    equal = true;
  } else {
    equal = (pkey_ == other.pkey_ && data_version_ == other.data_version_);
  }
  return equal;
}

bool ObPTUpdateRoleTask::operator<(const ObPTUpdateRoleTask& other) const
{
  return (pkey_ < other.pkey_);
}

bool ObPTUpdateRoleTask::compare_without_version(const ObPTUpdateRoleTask& other) const
{
  bool equal = false;
  if (this == &other) {
    equal = true;
  } else {
    equal = (pkey_ == other.pkey_);
  }
  return equal;
}

int ObPTUpdateRoleTask::set_update_partition_role_task(
    const common::ObPartitionKey& pkey, const int64_t data_version, const int64_t first_submit_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || data_version < 0 || first_submit_time < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pkey), K(data_version), K(first_submit_time));
  } else {
    pkey_ = pkey;
    data_version_ = data_version;
    first_submit_time_ = first_submit_time;
  }
  return ret;
}

int ObPartitionTableUpdater::get_queue(const ObPartitionKey& key, ObPTUpdateTaskQueue*& queue)
{
  queue = NULL;
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else {
    if (OB_ALL_CORE_TABLE_TID == extract_pure_id(key.table_id_) ||
        OB_ALL_ROOT_TABLE_TID == extract_pure_id(key.table_id_)) {
      queue = &core_table_queue_;
    } else if (is_sys_table(key.table_id_)) {
      if (OB_SYS_TENANT_ID == extract_tenant_id(key.table_id_)) {
        queue = &sys_table_queue_;
      } else {
        queue = &tenant_space_queue_;
      }
    } else {
      queue = &user_table_queue_;
    }
  }
  return ret;
}

int ObPartitionTableUpdater::init()
{
  int ret = OB_SUCCESS;
  // expand task queue length twice except update_role_queue
  const int64_t max_partition_cnt = !lib::is_mini_mode() ? MAX_PARTITION_CNT : MINI_MODE_MAX_PARTITION_CNT;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_FAIL(core_table_queue_.init(this, UPDATER_THREAD_CNT, max_partition_cnt / 100, "CoreTblUp"))) {
    LOG_WARN("init core table updater queue failed",
        K(ret),
        LITERAL_K(UPDATER_THREAD_CNT),
        "queue_size",
        max_partition_cnt / 100);
  } else if (OB_FAIL(sys_table_queue_.init(this, UPDATER_THREAD_CNT, max_partition_cnt / 100, "SysTblUp"))) {
    LOG_WARN("init system table updater queue failed",
        K(ret),
        LITERAL_K(UPDATER_THREAD_CNT),
        "queue_size",
        max_partition_cnt / 100);
  } else if (OB_FAIL(
                 tenant_space_queue_.init(this, UPDATER_TENANT_SPACE_THREAD_CNT, max_partition_cnt * 2, "TSpaceUp"))) {
    LOG_WARN("init tenant space table updater queue failed",
        K(ret),
        LITERAL_K(UPDATER_TENANT_SPACE_THREAD_CNT),
        "queue_size",
        max_partition_cnt / 100);
  } else if (OB_FAIL(user_table_queue_.init(this,
                 !lib::is_mini_mode() ? UPDATE_USER_TABLE_CNT : MINI_MODE_UPDATE_USER_TABLE_CNT,
                 MAX_PARTITION_CNT * 2,
                 "UserTblUp"))) {
    LOG_WARN("init user table updater queue failed",
        K(ret),
        LITERAL_K(MAX_PARTITION_REPORT_CONCURRENCY),
        K(max_partition_cnt));
  } else if (OB_FAIL(update_role_queue_.init(this,
                 !lib::is_mini_mode() ? UPDATE_USER_TABLE_CNT : MINI_MODE_UPDATE_USER_TABLE_CNT,
                 MAX_PARTITION_CNT,
                 "UsrTbRoleUp"))) {
    LOG_WARN(
        "init update role queue failed", K(ret), LITERAL_K(MAX_PARTITION_REPORT_CONCURRENCY), K(max_partition_cnt));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableUpdater::get_pg_key(const ObPartitionKey& key, ObPGKey& pg_key)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("observer is null", K(ret));
  } else if (ObPTUpdateTask::is_barrier(key)) {
    pg_key = key;
  } else if (OB_FAIL(GCTX.ob_service_->get_pg_key(key, pg_key))) {
    LOG_WARN("fail to get pg_key", K(ret), K(key));
  }
  return ret;
}

int ObPartitionTableUpdater::async_update_partition_role(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  const int64_t data_version = 0;
  const int64_t now = ObTimeUtility::current_time();
  const uint64_t pure_table_id = extract_pure_id(pkey.get_table_id());
  if (is_sys_table(pure_table_id)) {
    if (OB_FAIL(add_task(pkey, data_version, now, true /*with role*/))) {
      LOG_WARN("fail to add to task", KR(ret), K(pkey));
    }
  } else {
    if (OB_FAIL(add_update_partition_role_task(pkey, data_version, now))) {
      LOG_WARN("fail to add update partition role task", KR(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionTableUpdater::async_update(const ObPartitionKey& key, const bool with_role)
{
  int64_t data_version = 0;
  int64_t now = ObTimeUtility::current_time();
  return add_task(key, data_version, now, with_role);
}

int ObPartitionTableUpdater::add_update_partition_role_task(
    const ObPartitionKey& key, const int64_t data_version, const int64_t first_submit_time)
{
  int ret = OB_SUCCESS;
  ObPTUpdateRoleTask task;
  ObPGKey pg_key;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else if (OB_FAIL(get_pg_key(key, pg_key))) {
    LOG_WARN("fail to get pg key", K(ret), KR(ret), K(key));
  } else if (OB_UNLIKELY(!pg_key.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pg_key is invalid", KR(ret), K(pg_key));
  } else if (is_sys_table(extract_pure_id(pg_key.get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shall never be here", KR(ret), K(pg_key));
  } else if (OB_FAIL(task.set_update_partition_role_task(pg_key, data_version, first_submit_time))) {
    LOG_WARN("fail to set update partition role task", KR(ret), K(pg_key), K(key), K(first_submit_time));
  } else if (OB_FAIL((update_role_queue_.add(task)))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to do add update partition leader task", KR(ret), K(task));
    }
  }
  return ret;
}

int ObPartitionTableUpdater::add_task(
    const ObPartitionKey& key, const int64_t data_version, const int64_t first_submit_time, const bool with_role)
{
  int ret = OB_SUCCESS;
  ObPTUpdateTask task;
  ObPGKey pg_key;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(get_pg_key(key, pg_key))) {
    LOG_WARN("fail to get pg_key", K(ret), K(key), K(pg_key));
  } else if (OB_FAIL(task.set_update_task(pg_key, data_version, first_submit_time, with_role))) {
    LOG_WARN("set update task failed", K(ret), K(key), K(pg_key));
  } else {
    if (!task.is_barrier()) {
      ObPTUpdateTaskQueue* queue = NULL;
      if (OB_FAIL(get_queue(pg_key, queue))) {
        LOG_WARN("get queue failed", K(ret), K(key), K(pg_key));
      } else if (NULL == queue) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL queue", K(ret));
      } else if (OB_FAIL(queue->add(task))) {
        if (OB_EAGAIN == ret) {
          LOG_DEBUG("partition table update task exist", K(task));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("add partition table update task failed", K(ret), K(task));
        }
      } else {
        ObTenantStatEstGuard guard;
        EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_TOTAL_COUNT);
      }
    } else {
      ObPTUpdateTaskQueue* inner_table_queues[] = {
          &core_table_queue_, &sys_table_queue_, &tenant_space_queue_, &user_table_queue_};
      STATIC_ASSERT(ARRAYSIZEOF(inner_table_queues) == ObIPartitionTable::PARTITION_TABLE_REPORT_LEVELS,
          "wrong partition table report levels");
      for (int64_t i = 0; (OB_SUCCESS == ret || OB_EAGAIN == ret) && i < ARRAYSIZEOF(inner_table_queues); ++i) {
        ret = inner_table_queues[i]->add(task);
        if (OB_SUCCESS != ret && OB_EAGAIN != ret) {
          LOG_WARN("add barrier task failed", K(ret), K(key), K(data_version));
        } else {
          ObTenantStatEstGuard guard;
          EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_TOTAL_COUNT);
        }
      }
    }
  }
  return ret;
}

int ObPartitionTableUpdater::process_barrier(const ObPTUpdateRoleTask& task, bool& stopped)
{
  UNUSED(task);
  UNUSED(stopped);
  return OB_NOT_SUPPORTED;
}

int ObPartitionTableUpdater::do_batch_execute(
    const common::ObIArray<ObPartitionReplica>& tasks, const common::ObRole new_role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tasks.count() <= 0)) {
    // empty task
  } else if (OB_UNLIKELY(nullptr == GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pt operator is null", KR(ret));
  } else {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(GCTX.pt_operator_->batch_report_partition_role(tasks, new_role))) {
        LOG_WARN("fail to batch report partition role", KR(ret), K(tasks));
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("batch execute report role failed", KR(ret), "cnt", tasks.count());
    } else if (tasks.count() > 0) {
      LOG_INFO("batch execute report role success", KR(ret), "cnt", tasks.count());
    }
  }
  return ret;
}

int ObPartitionTableUpdater::reput_to_queue(const common::ObIArray<ObPTUpdateRoleTask>& tasks)
{
  int ret = OB_SUCCESS;
  // try to push back to the task queue again, ignore ret code
  for (int64_t i = 0; i < tasks.count(); ++i) {
    int tmp_ret = OB_SUCCESS;
    const ObPTUpdateRoleTask& this_task = tasks.at(i);
    if (!this_task.is_valid()) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid task", KR(tmp_ret), K(this_task));
    } else if (OB_SUCCESS != (tmp_ret = add_update_partition_role_task(
                                  this_task.pkey_, this_task.data_version_, this_task.first_submit_time_))) {
      LOG_WARN("add update partition role task", KR(tmp_ret), K(this_task));
    }
  }
  return ret;
}

int ObPartitionTableUpdater::batch_process_tasks(const ObIArray<ObPTUpdateRoleTask>& batch_tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(stopped);
  ObCurTraceId::init(GCONF.self_addr_);
  ObSEArray<ObPTUpdateRoleTask, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> tasks;
  uint64_t tenant_id = OB_INVALID_ID;
  bool tenant_dropped = false;
  if (OB_UNLIKELY(nullptr == GCTX.pt_operator_ || nullptr == GCTX.ob_service_ || nullptr == GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument",
        KR(ret),
        "pt_operator",
        GCTX.pt_operator_,
        "ob_service",
        GCTX.ob_service_,
        "ob_service",
        GCTX.ob_service_);
  } else if (OB_UNLIKELY(batch_tasks.count() <= 0)) {
    // bypass
  } else if (OB_FAIL(tasks.assign(batch_tasks))) {
    LOG_WARN("fail to assign task", K(ret));
    // sorted by pkey to avoid deadlock when update partition table
  } else if (FALSE_IT(std::sort(tasks.begin(), tasks.end()))) {
    // shall never be here
  } else if (FALSE_IT(tenant_id = tasks.at(0).pkey_.get_tenant_id())) {
    // shall never be here
  } else {
    // try to check if tenant has been dropped before execute, ignore ret code
    (void)check_if_tenant_has_been_dropped(tenant_id, tenant_dropped);
  }

  if (OB_SUCC(ret) && !tenant_dropped && tasks.count() > 0) {
    ObArray<ObPTUpdateRoleTask> leader_tasks;
    ObArray<ObPTUpdateRoleTask> standby_leader_tasks;
    ObArray<ObPTUpdateRoleTask> restore_leader_tasks;
    ObArray<ObPTUpdateRoleTask> follower_tasks;
    ObArray<ObPartitionReplica> leader_r_array;
    ObArray<ObPartitionReplica> standby_leader_r_array;
    ObArray<ObPartitionReplica> restore_leader_r_array;
    ObArray<ObPartitionReplica> follower_r_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObPTUpdateRoleTask& this_task = tasks.at(i);
      this_task.check_task_status();
      share::ObPartitionReplica replica;
      int tmp_ret = GCTX.ob_service_->fill_partition_replica(this_task.pkey_, replica);
      if (OB_SUCCESS != tmp_ret) {
        // bypass
      } else if (LEADER == replica.role_) {
        if (OB_FAIL(leader_tasks.push_back(this_task))) {
          LOG_WARN("fail to push back leader task", KR(ret), "array_cnt", leader_tasks.count());
        } else if (OB_FAIL(leader_r_array.push_back(replica))) {
          LOG_WARN("fail to push back leader replica array", KR(ret), "array_cnt", leader_r_array.count());
        }
      } else if (FOLLOWER == replica.role_) {
        if (OB_FAIL(follower_tasks.push_back(this_task))) {
          LOG_WARN("fail to push back follower task", KR(ret), "array_cnt", follower_tasks.count());
        } else if (OB_FAIL(follower_r_array.push_back(replica))) {
          LOG_WARN("fail to push back follower replica array", KR(ret), "array_cnt", follower_r_array.count());
        }
      } else if (STANDBY_LEADER == replica.role_) {
        if (OB_FAIL(standby_leader_tasks.push_back(this_task))) {
          LOG_WARN("fail to push back standby leader tasks", KR(ret), "array_cnt", standby_leader_tasks.count());
        } else if (OB_FAIL(standby_leader_r_array.push_back(replica))) {
          LOG_WARN(
              "fail to push back standby leader replica array", KR(ret), "array_cnt", standby_leader_r_array.count());
        }
      } else if (RESTORE_LEADER == replica.role_) {
        if (OB_FAIL(restore_leader_tasks.push_back(this_task))) {
          LOG_WARN("fail to push back restore leader tasks", KR(ret), "array_cnt", restore_leader_tasks.count());
        } else if (OB_FAIL(restore_leader_r_array.push_back(replica))) {
          LOG_WARN(
              "fail to push back restore leader replica array", KR(ret), "array_cnt", restore_leader_r_array.count());
        }
      } else {
        // print an error log, but ignore ret code to make this routine go
        LOG_ERROR("unexpected replica role value", K(replica));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = do_batch_execute(leader_r_array, LEADER);
      if (OB_SUCCESS == tmp_ret) {
        // good
      } else if (OB_SUCCESS != (tmp_ret = reput_to_queue(leader_tasks))) {
        LOG_WARN("update info fail to reput to queue", KR(tmp_ret), K(leader_tasks));
      } else {
        LOG_INFO("batch update partition table failed, reput to queue", K(leader_tasks));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = do_batch_execute(standby_leader_r_array, STANDBY_LEADER);
      if (OB_SUCCESS == tmp_ret) {
        // good
      } else if (OB_SUCCESS != (tmp_ret = reput_to_queue(standby_leader_tasks))) {
        LOG_WARN("update info fail to reput to queue", KR(tmp_ret), K(standby_leader_tasks));
      } else {
        LOG_INFO("batch update partition table failed, reput to queue", K(standby_leader_tasks));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = do_batch_execute(restore_leader_r_array, RESTORE_LEADER);
      if (OB_SUCCESS == tmp_ret) {
        // good
      } else if (OB_SUCCESS != (tmp_ret = reput_to_queue(restore_leader_tasks))) {
        LOG_WARN("update info fail to reput to queue", KR(tmp_ret), K(restore_leader_tasks));
      } else {
        LOG_INFO("batch update partition table failed, reput to queue", K(restore_leader_tasks));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = reput_to_queue(batch_tasks))) {
        LOG_WARN("update info failed to reput to queue", KR(tmp_ret), K(batch_tasks));
      } else {
        LOG_INFO("batch update partition table failed, reput to queue", K(batch_tasks));
      }
    }
  }
  return ret;
}

int ObPartitionTableUpdater::process_barrier(const ObPTUpdateTask& task, bool& stopped)
{
  UNUSED(stopped);
  return do_rpc_task(task);
}

int ObPartitionTableUpdater::batch_process_tasks(const ObIArray<ObPTUpdateTask>& batch_tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(stopped);
  DEBUG_SYNC(BEFORE_BATCH_PROCESS_TASK);
  ObSEArray<ObPTUpdateTask, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> tasks;
  bool skip_to_reput_tasks = false;
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_ISNULL(GCTX.pt_operator_) || OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(GCTX.pt_operator_));
  } else if (OB_UNLIKELY(batch_tasks.count() <= 0)) {
    // nothing to do
  } else if (OB_FAIL(tasks.assign(batch_tasks))) {
    LOG_WARN("fail to assgign task", K(ret));
  } else {
    // distinguish the batch task based on the role and task type
    std::sort(tasks.begin(), tasks.end());
    ObPartitionReplica replica;
    ObArray<ObPartitionReplica> leader_replicas;
    ObArray<ObPartitionReplica> with_role_report_replicas;
    ObArray<ObPartitionReplica> without_role_report_replicas;
    ObPTUpdateTask tmp_task;
    ObSEArray<ObPTUpdateTask, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> leader_tasks;
    ObSEArray<ObPTUpdateTask, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> with_role_report_tasks;
    ObSEArray<ObPTUpdateTask, UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> without_role_report_tasks;
    FOREACH_CNT_X(task, tasks, OB_SUCC(ret))
    {
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task", K(ret), K(task));
      } else {
        task->check_task_status();
        replica.reset();
        bool need_skip = false;
        if (!task->is_remove_) {
          if (OB_FAIL(GCTX.ob_service_->fill_partition_replica(task->part_key_, replica))) {
            LOG_WARN("fail to fill partition replica", K(ret), K(task->part_key_));
            if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
              ObTaskController::get().allow_next_syslog();
              LOG_INFO("try update not exist or invalid partition, turn to remove partition table",
                  K(task->part_key_),
                  K(ret));
              ret = OB_SUCCESS;
              replica.table_id_ = task->part_key_.table_id_;
              replica.partition_id_ = task->part_key_.get_partition_id();
              replica.partition_cnt_ = task->part_key_.get_partition_cnt();
              replica.set_remove_flag();
            }
          } else {
            storage::ObIPartitionGroupGuard pg_guard;
            if (OB_FAIL(GCTX.par_ser_->get_partition(task->part_key_, pg_guard))) {
              LOG_WARN("fail to get partition", K(ret), K(task->part_key_));
            } else if (OB_ISNULL(pg_guard.get_partition_group())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("pg is null", K(ret), K(task->part_key_));
            } else {
              if (REPLICA_STATUS_OFFLINE == replica.status_) {
                replica.set_remove_flag();
              }
              LOG_DEBUG("fill partition success",
                  K(task->part_key_),
                  K(replica.data_version_),
                  K(replica.table_id_),
                  K(replica.partition_id_),
                  K(replica.server_),
                  K(replica.role_),
                  K(replica.to_leader_time_));
            }
          }
        } else {
          replica.table_id_ = task->part_key_.table_id_;
          replica.partition_id_ = task->part_key_.get_partition_id();
          replica.partition_cnt_ = task->part_key_.get_partition_cnt();
          replica.set_remove_flag();
        }
        if (OB_SUCC(ret) && !need_skip) {
          tmp_task = *task;
          const uint64_t table_id = replica.table_id_;
          if ((replica.is_leader_like() && (task->with_role_ || replica.need_force_full_report())) ||
              replica.is_remove_) {
            if (OB_FAIL(leader_replicas.push_back(replica))) {
              LOG_WARN("fail to push back replica", K(ret), K(replica));
            } else if (OB_FAIL(leader_tasks.push_back(tmp_task))) {
              LOG_WARN("fail to push back task", K(ret), KPC(task));
            }
          } else if (task->with_role_ || replica.need_force_full_report()) {
            if (OB_FAIL(with_role_report_replicas.push_back(replica))) {
              LOG_WARN("fail to push back replica", K(ret), K(replica));
            } else if (OB_FAIL(with_role_report_tasks.push_back(tmp_task))) {
              LOG_WARN("fail to push back task", K(ret), KPC(task));
            }
          } else {
            if (OB_FAIL(without_role_report_replicas.push_back(replica))) {
              LOG_WARN("fail to push back replica", K(ret), K(replica));
            } else if (OB_FAIL(without_role_report_tasks.push_back(tmp_task))) {
              LOG_WARN("fail to push back task", K(ret), KPC(task));
            }
          }
        }
      }
    }  // foreach

    if (OB_SUCC(ret)) {
      skip_to_reput_tasks = true;
      // execute leader or removed replica report tasks
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = do_batch_execute(leader_tasks, leader_replicas, true /*with role*/))) {
        LOG_WARN("batch execute leader replicas failed", K(ret), "cnt", leader_tasks.count());
      }
      // execute follower replica report tasks with role
      if (OB_SUCCESS !=
          (tmp_ret = do_batch_execute(with_role_report_tasks, with_role_report_replicas, true /*with role*/))) {
        LOG_WARN("batch execute with role replicas failed",
            K(ret),
            "cnt",
            with_role_report_tasks.count(),
            "with_role",
            true);
      }
      // execute follower replica report tasks without role
      if (OB_SUCCESS != (tmp_ret = do_batch_execute(
                             without_role_report_tasks, without_role_report_replicas, false /*without role*/))) {
        LOG_WARN("batch execute without role replicas failed",
            K(ret),
            "cnt",
            without_role_report_tasks.count(),
            "with_role",
            false);
      }
    }
  }
  if (OB_FAIL(ret) && !skip_to_reput_tasks) {
    int tmp_ret = reput_to_queue(batch_tasks);
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR("update info fail to reput to queue", K(ret), K(batch_tasks.count()));
    } else {
      LOG_INFO("batch update partition table fail, reput to queue", K(batch_tasks.count()));
    }
    ObTenantStatEstGuard guard;
    EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_FAIL_TIMES);
  }
  return ret;
}

// after oceanbase 2.0 version, partition table is splitted to become a tenant level sys table.
// if a specific tenant is dropped, its corresponding tenant level partition table is dropped at the same time.
// a task trying to update the partition table of this tenant shall always fail because the partition table
// doesn't exist any more. this check_if_tenant_has_been_dropped func can detect
// the existence of the tenant partition table to determine whether the task need to be executed again.
int ObPartitionTableUpdater::check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool& has_dropped)
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
  share::schema::ObSchemaGetterGuard guard;
  has_dropped = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, has_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tenant_id));
  }
  return ret;
}

int ObPartitionTableUpdater::do_batch_execute(
    const ObIArray<ObPTUpdateTask>& tasks, const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  int ret = OB_SUCCESS;
  bool skip_to_reput_tasks = false;
  // temporary variable success_idx is used to record the success offset in replica array(replicas)
  int64_t success_idx = OB_INVALID_ID;
  if (tasks.count() != replicas.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tasks num not match", K(ret), "task_cnt", tasks.count(), "replica_cnt", replicas.count());
  } else if (0 == tasks.count()) {
    // skip
  } else {
    DEBUG_SYNC(BEFORE_ASYNC_PT_UPDATE_TASK_EXECUTE);
    // need to push back to task queue if failed
    const bool is_sys = is_sys_table(tasks.at(0).part_key_.get_table_id());
    const uint64_t tenant_id = tasks.at(0).part_key_.get_tenant_id();
    const int64_t start_time = ObTimeUtility::current_time();
    if ((replicas.at(0).is_leader_like() && (replicas.at(0).need_force_full_report() || with_role)) ||
        replicas.at(0).is_remove_) {
      ObSEArray<ObPartitionReplica, 1> tmp_replicas;
      for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); i++) {
        tmp_replicas.reset();
        if (OB_FAIL(tmp_replicas.push_back(replicas.at(i)))) {
          LOG_WARN("fail to push back replica", K(ret), K(replicas.at(i)));
        } else if (OB_FAIL(GCTX.pt_operator_->batch_execute(tmp_replicas))) {
          LOG_WARN(
              "do partition table update failed", K(ret), "escape time", ObTimeUtility::current_time() - start_time);
        } else {
          success_idx++;
        }
      }
    } else if (replicas.at(0).need_force_full_report() || with_role) {
      if (OB_FAIL(GCTX.pt_operator_->batch_report_with_optimization(replicas, true /*with role*/))) {
        LOG_WARN("fail to batch report replica",
            K(ret),
            K(with_role),
            "first_replica_status",
            replicas.at(0),
            "escape time",
            ObTimeUtility::current_time() - start_time);
      } else {
        success_idx = replicas.count() - 1;
      }
    } else {
      if (OB_FAIL(GCTX.pt_operator_->batch_report_with_optimization(replicas, false /*without role*/))) {
        LOG_WARN("fail to batch report replica",
            K(ret),
            K(with_role),
            "first_replica_status",
            replicas.at(0),
            "escape time",
            ObTimeUtility::current_time() - start_time);
      } else {
        success_idx = replicas.count() - 1;
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = check_if_tenant_has_been_dropped(tenant_id, skip_to_reput_tasks))) {
        LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tmp_ret), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("batch process update success",
          K(ret),
          K(replicas.count()),
          "use_time",
          ObTimeUtility::current_time() - start_time);
      EVENT_ADD(OBSERVER_PARTITION_TABLE_UPDATER_FINISH_COUNT, replicas.count());
    }
    int tmp_ret = throttle(is_sys, ret, ObTimeUtility::current_time() - start_time, stopped_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("throttle failed", K(tmp_ret));
    }
    DEBUG_SYNC(AFTER_ASYNC_PT_UPDATE_TASK_EXECUTE);
  }
  if (OB_FAIL(ret) && !skip_to_reput_tasks) {
    int tmp_ret = reput_to_queue(tasks, success_idx);
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR("update info fail to reput to queue", K(ret), K(tasks.count()));
    } else {
      LOG_INFO("batch update partition table fail, reput to queue", K(tasks.count()));
    }
    ObTenantStatEstGuard guard;
    EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_FAIL_TIMES);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("batch execute replica failed", K(ret), "cnt", tasks.count());
  } else if (tasks.count() > 0) {
    LOG_INFO("batch execute replica success", K(ret), "cnt", tasks.count());
  }
  return ret;
}

int ObPartitionTableUpdater::reput_to_queue(
    const ObIArray<ObPTUpdateTask>& tasks, const int64_t success_idx /*= OB_INVALID_ID*/)
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard guard;
  // try to push task back to queue, ignore ret code
  for (int64_t i = success_idx + 1; i < tasks.count(); i++) {
    const ObPTUpdateTask& task = tasks.at(i);
    if (!task.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid task", K(ret), K(task));
    } else if (OB_FAIL(add_task(task.part_key_, task.data_version_, task.first_submit_time_, task.with_role_))) {
      LOG_ERROR("fail to reput to queue", K(ret), "partition key", task.part_key_);
      EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_DROP_COUNT);
    } else {
      EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_REPUT_COUNT);
    }
  }
  return ret;
}

int ObPartitionTableUpdater::throttle(
    const bool is_sys_table, const int return_code, const int64_t execute_time_us, bool& stopped)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  if (OB_SUCCESS != return_code) {
    sleep_us = RETRY_INTERVAL_US;
    if (is_sys_table) {
      sleep_us = SYS_RETRY_INTERVAL_US;
    }
  } else {
    if (execute_time_us > SLOW_UPDATE_TIME_US) {
      if (is_sys_table) {
        // won't limit update for sys table
        sleep_us = 0;
        LOG_WARN("detected slow update for sys table", K(ret));
      } else {
        sleep_us = MIN(RETRY_INTERVAL_US, (execute_time_us - SLOW_UPDATE_TIME_US));
        LOG_WARN("detected slow update, may be too many concurrent updating", K(sleep_us));
      }
    }
  }
  const static int64_t sleep_step_us = 20 * 1000;  // 20ms
  for (; !stopped && sleep_us > 0; sleep_us -= sleep_step_us) {
    usleep(static_cast<int32_t>(std::min(sleep_step_us, sleep_us)));
  }
  return ret;
}

int ObPartitionTableUpdater::do_rpc_task(const ObPTUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task));
  } else if (ObPTUpdateTask::report_merge_finish_pkey_ == task.part_key_) {
    if (OB_FAIL(do_report_merge_finish(task.data_version_))) {
      LOG_WARN("report merge finished failed", K(ret), K(task.data_version_));
      // no need to retry merge finish report task, ignore this error
      ret = OB_SUCCESS;
    }
  } else if (ObPTUpdateTask::sync_pt_key_ == task.part_key_) {
    if (OB_FAIL(do_sync_pt_finish(task.data_version_))) {
      LOG_WARN("report sync pt finish failed", K(ret));
      // no need to retry sync partition finish, ignore this error
      ret = OB_SUCCESS;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid task type", K(ret), K(task));
  }
  return ret;
}

int ObPartitionTableUpdater::sync_update(const ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  ObPGKey pg_key;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(get_pg_key(key, pg_key))) {
    LOG_WARN("fail to get pg_key", K(ret), K(key), K(pg_key));
  } else {
    ObPartitionReplica replica;
    if (OB_FAIL(GCTX.ob_service_->fill_partition_replica(pg_key, replica))) {
      LOG_WARN("fill partition replica failed", K(ret), K(key), K(pg_key));
      if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
        LOG_DEBUG(
            "try update not exist or invalid partition, turn to remove partition table", K(key), K(pg_key), K(ret));
        const ObAddr addr = GCTX.self_addr_;
        if (OB_FAIL(GCTX.pt_operator_->remove(pg_key.table_id_, pg_key.get_partition_id(), addr))) {
          LOG_WARN("remove partition failed", K(ret), K(key), K(pg_key), K(addr));
        }
      }
    } else if (OB_FAIL(GCTX.pt_operator_->update(replica))) {
      LOG_WARN("update replica failed", K(ret), K(replica));
    }
  }
  return ret;
}

int ObPartitionTableUpdater::async_remove(const ObPartitionKey& key)
{
  int ret = OB_SUCCESS;
  ObPTUpdateTask task;
  ObPTUpdateTaskQueue* queue = NULL;
  int64_t now = ObTimeUtility::current_time();
  ObPGKey pg_key;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(get_pg_key(key, pg_key))) {
    LOG_WARN("fail to get pg_key", K(ret), K(key), K(pg_key));
  } else if (OB_FAIL(task.set_remove_task(pg_key, now))) {
    LOG_WARN("get remove task failed", K(ret), K(key), K(pg_key));
  } else if (OB_FAIL(get_queue(pg_key, queue))) {
    LOG_WARN("get queue failed", K(ret), K(key), K(pg_key));
  } else if (NULL == queue) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL queue", K(ret));
  } else if (OB_FAIL(queue->add(task))) {
    if (OB_SUCCESS != ret) {
      if (OB_EAGAIN == ret) {
        LOG_DEBUG("partition table remove task exist", K(task));
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("add partition table remove task failed", K(ret), K(task));
      }
    }
  } else {
    ObTenantStatEstGuard guard;
    EVENT_INC(OBSERVER_PARTITION_TABLE_UPDATER_TOTAL_COUNT);
  }
  LOG_INFO("submit partition table remove task", K(key), K(pg_key), K(ret));
  return ret;
}

int ObPartitionTableUpdater::sync_merge_finish(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObPTUpdateTask task;
  int64_t now = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(version));
  } else if (OB_FAIL(add_task(ObPTUpdateTask::report_merge_finish_pkey_, version, now, true /*with role*/))) {
    LOG_WARN("submit async merge finish task failed",
        K(ret),
        "part_key",
        ObPTUpdateTask::report_merge_finish_pkey_,
        K(version));
  }
  return ret;
}

int ObPartitionTableUpdater::sync_pt(const int64_t version)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  ObPTUpdateTask task;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(version));
  } else if (OB_FAIL(add_task(ObPTUpdateTask::sync_pt_key_, version, now, true /*with role*/))) {
    LOG_WARN("submit async partition table update task failed",
        K(ret),
        "part_key",
        ObPTUpdateTask::sync_pt_key_,
        K(version));
  }
  return ret;
}

int ObPartitionTableUpdater::do_report_merge_finish(const int64_t data_version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (data_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_version));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret), K(GCTX));
  } else {
    LOG_INFO("report merge finish", K(data_version));
    obrpc::ObMergeFinishArg arg;
    ObAddr rs_addr;
    arg.frozen_version_ = data_version;
    arg.server_ = GCTX.self_addr_;
    if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
      STORAGE_LOG(WARN, "fail to get rootservice address", K(ret));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).merge_finish(arg))) {
      LOG_WARN("report merge finish failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObPartitionTableUpdater::do_sync_pt_finish(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(version));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid global context", K(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get rootservice address failed", K(ret));
  } else {
    LOG_INFO("sync partition table finish", K(version));
    obrpc::ObSyncPartitionTableFinishArg arg;
    arg.server_ = GCTX.self_addr_;
    arg.version_ = version;
    // invoke not_rs_mgr() to disable rpc proxy retry logic.
    if (OB_FAIL(GCTX.rs_rpc_proxy_->to_addr(rs_addr).timeout(GCONF.rpc_timeout).sync_pt_finish(arg))) {
      LOG_WARN("call sync partition table finish rpc failed", K(ret), K(arg));
    }
  }
  return ret;
}

void ObPartitionTableUpdater::stop()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    stopped_ = true;
    core_table_queue_.stop();
    sys_table_queue_.stop();
    tenant_space_queue_.stop();
    user_table_queue_.stop();
    update_role_queue_.stop();
  }
}

void ObPartitionTableUpdater::wait()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    core_table_queue_.wait();
    sys_table_queue_.wait();
    tenant_space_queue_.wait();
    user_table_queue_.wait();
    update_role_queue_.wait();
  }
}

void ObPartitionTableUpdater::destroy()
{
  stop();
  wait();
}

}  // end namespace observer
}  // end namespace oceanbase
