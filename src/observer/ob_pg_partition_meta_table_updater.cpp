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

#include "ob_pg_partition_meta_table_updater.h"
#include "lib/ob_running_mode.h"
#include "share/ob_task_define.h"
#include "share/ob_pg_partition_meta_table_operator.h"
#include "observer/ob_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::obsys;

const ObPartitionKey ObPGPartitionMTUpdateTask::sync_pt_key_(INT64_MAX, INT32_MAX - 1, INT32_MAX - 1);

ObPGPartitionMTUpdateTask::ObPGPartitionMTUpdateTask()
    : pkey_(), add_timestamp_(0), update_type_(ObPGPartitionMTUpdateType::INSERT_ON_UPDATE), version_(0), svr_()
{}

int ObPGPartitionMTUpdateTask::init(const common::ObPartitionKey& pkey, const ObPGPartitionMTUpdateType type,
    const int64_t version, const common::ObAddr& addr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pkey.is_valid()) || !addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey), K(type), K(addr));
  } else {
    pkey_ = pkey;
    update_type_ = type;
    version_ = version;
    svr_ = addr;
    add_timestamp_ = ObTimeUtility::current_time();
  }
  return ret;
}

bool ObPGPartitionMTUpdateTask::is_valid() const
{
  return pkey_.is_valid() && svr_.is_valid();
}

int64_t ObPGPartitionMTUpdateTask::hash() const
{
  return pkey_.hash();
}

bool ObPGPartitionMTUpdateTask::operator==(const ObPGPartitionMTUpdateTask& other) const
{
  return pkey_ == other.pkey_
         //&& update_type_ == other.update_type_
         && version_ == other.version_ && svr_ == other.svr_;
}

bool ObPGPartitionMTUpdateTask::operator!=(const ObPGPartitionMTUpdateTask& other) const
{
  return !(operator==(other));
}

bool ObPGPartitionMTUpdateTask::compare_without_version(const ObPGPartitionMTUpdateTask& other) const
{
  return pkey_ == other.pkey_ && update_type_ == other.update_type_;
}

bool ObPGPartitionMTUpdateTask::is_barrier() const
{
  return pkey_ == sync_pt_key_;
}

const int64_t ObPGPartitionMTUpdater::UPDATER_THREAD_CNT;

int ObPGPartitionMTUpdater::init()
{
  int ret = OB_SUCCESS;

  const int64_t max_partition_cnt =
      !lib::is_mini_mode() ? OB_MAX_PARTITION_NUM_PER_SERVER : OB_MINI_MODE_MAX_PARTITION_NUM_PER_SERVER;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGPartitionMTUpdater has already been inited", K(ret));
  } else if (OB_FAIL(task_queue_.init(this,
                 !lib::is_mini_mode() ? UPDATER_THREAD_CNT : MINI_MODE_UPDATER_THREAD_CNT,
                 max_partition_cnt,
                 "PGPTMTUp"))) {
    LOG_WARN("fail to init task queue", K(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPGPartitionMTUpdater already running", K(ret));
  } else {
    is_inited_ = true;
    is_running_ = true;
    LOG_INFO("ObPGPartitionMTUpdater init success", K(lbt()));
  }
  return ret;
}

void ObPGPartitionMTUpdater::reset()
{
  is_inited_ = false;
  is_running_ = false;
}

void ObPGPartitionMTUpdater::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObPGPartitionMTUpdater has not been inited");
  } else if (!is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPGPartitionMTUpdater already not running", K(ret));
  } else {
    (void)task_queue_.stop();
    is_running_ = false;
    LOG_INFO("ObPGPartitionMTUpdater stop success");
  }
  UNUSED(ret);
}

void ObPGPartitionMTUpdater::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObPGPartitionMTUpdater has not been inited");
  } else if (!is_running_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObPGPartitionMTUpdater already not running", K(ret));
  } else {
    (void)task_queue_.wait();
    LOG_INFO("ObPGPartitionMTUpdater wait success");
  }
  UNUSED(ret);
}

void ObPGPartitionMTUpdater::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      (void)task_queue_.stop();
      (void)task_queue_.wait();
    }
    is_inited_ = false;
    is_running_ = false;
    LOG_INFO("ObPGPartitionMTUpdater wait success");
  }
}

ObPGPartitionMTUpdater& ObPGPartitionMTUpdater::get_instance()
{
  static ObPGPartitionMTUpdater instance;
  return instance;
}

int ObPGPartitionMTUpdater::process_barrier(const ObPGPartitionMTUpdateTask& task, bool& stopped)
{
  UNUSED(stopped);
  return do_sync_pt_finish_(task.get_version());
}

int ObPGPartitionMTUpdater::sync_pg_pt(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObPGPartitionMTUpdateTask task;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(version));
  } else if (OB_FAIL(add_task(ObPGPartitionMTUpdateTask::sync_pt_key_,
                 ObPGPartitionMTUpdateType::INSERT_ON_UPDATE,
                 version,
                 GCTX.self_addr_))) {
    LOG_WARN("submit async pg partition meta table update task failed",
        K(ret),
        "part_key",
        ObPGPartitionMTUpdateTask::sync_pt_key_,
        K(version));
  }
  return ret;
}

int ObPGPartitionMTUpdater::do_sync_pt_finish_(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;

  if (!is_inited_) {
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
    obrpc::ObSyncPGPartitionMTFinishArg arg;
    arg.server_ = GCTX.self_addr_;
    arg.version_ = version;
    if (OB_FAIL(GCTX.rs_rpc_proxy_->to_addr(rs_addr).timeout(GCONF.rpc_timeout).sync_pg_pt_finish(arg))) {
      LOG_WARN("call sync pg partition meta table finish rpc failed", K(ret), K(arg));
    }
  }
  return ret;
}

int ObPGPartitionMTUpdater::add_task(
    const ObPartitionKey& pkey, const ObPGPartitionMTUpdateType type, const int64_t version, const common::ObAddr& svr)
{
  int ret = OB_SUCCESS;
  ObPGPartitionMTUpdateTask task;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGPartitionMTUpdater has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else if (pkey.is_pg()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add task error", K(pkey), K(type), K(version));
  } else if (is_sys_table(pkey.get_table_id())) {
    // system table no need to report
  } else if (OB_FAIL(task.init(pkey, type, version, svr))) {
    LOG_WARN("fail to init task", K(ret), K(pkey), K(type));
  } else {
    add_tasks_to_queue_(task);
  }
  return ret;
}

void ObPGPartitionMTUpdater::add_tasks_to_queue_(const ObPGPartitionMTUpdateTask& task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_queue_.add(task))) {
    if (OB_LIKELY(OB_EAGAIN == ret)) {
      LOG_DEBUG("task has been added already", K(task));
    } else {
      LOG_WARN("failed to add task to queue", K(ret), K(task));
    }
  }
}

int ObPGPartitionMTUpdater::batch_process_tasks(const ObIArray<ObPGPartitionMTUpdateTask>& tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  bool skip_to_reput_tasks = false;
  bool update_succ = false;
  bool delete_succ = false;
  UNUSED(stopped);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGPartitionMTUpdater has not been inited", K(ret));
  } else if (OB_UNLIKELY(tasks.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    ObArray<ObPGPartitionMTUpdateItem> pt_update_items;
    ObArray<ObPGPartitionMTUpdateItem> pt_delete_items;
    int64_t start_time = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObPGPartitionMTUpdateTask& task = tasks.at(i);
      if (!task.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ObPGPartitionMTUpdateTask is not valid", K(ret), K(task));
      } else {
        if (ObPGPartitionMTUpdateType::DELETE != task.update_type_) {
          ObPGPartitionMTUpdateItem item;
          if (OB_FAIL(GCTX.ob_service_->fill_partition_table_update_task(task.pkey_, item))) {
            if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
              ObTaskController::get().allow_next_syslog();
              LOG_INFO("try update not exist or valid partition", K(ret), K(task.pkey_));
              ret = OB_SUCCESS;
              if (OB_FAIL(add_task(task.pkey_, ObPGPartitionMTUpdateType::DELETE, task.version_, task.svr_))) {
                LOG_ERROR("fail to add task", K(ret), K(task.pkey_));
              }
            } else if (OB_EAGAIN == ret) {
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(pt_update_items.push_back(item))) {
            LOG_WARN("pt update items push back error", K(ret), K(item));
          } else {
            LOG_INFO("fill partition table update task success", K(ret), K(task.pkey_));
          }
        } else {
          ObPGPartitionMTUpdateItem pt_delete_item;
          pt_delete_item.tenant_id_ = extract_tenant_id(task.pkey_.get_table_id());
          pt_delete_item.table_id_ = task.pkey_.get_table_id();
          pt_delete_item.partition_id_ = task.pkey_.get_partition_id();
          pt_delete_item.svr_ip_ = task.svr_;
          if (OB_FAIL(pt_delete_items.push_back(pt_delete_item))) {
            LOG_WARN("fail to push back data checksum item", K(ret));
          }
        }
      }
    }

    start_time = ObTimeUtility::current_time();
    if (OB_SUCC(ret)) {
      if (pt_update_items.count() > 0) {
        if (OB_FAIL(ObPGPartitionMTUpdateOperator::batch_update(pt_update_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch update pg partition meta table", K(ret), K(pt_update_items));
        } else {
          update_succ = true;
          LOG_INFO("batch update pg partition meta table success", K(pt_update_items));
        }
      }

      if (OB_SUCC(ret) && pt_delete_items.count() > 0) {
        if (OB_FAIL(ObPGPartitionMTUpdateOperator::batch_delete(pt_delete_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch delete pg partition meta table", K(ret), K(pt_update_items));
        } else {
          delete_succ = true;
          LOG_INFO("batch delete pg partition meta table success", K(pt_update_items));
        }
      }
      if (OB_FAIL(ret)) {
        // no need to retry when tenant dropped;
        int tmp_ret = OB_SUCCESS;
        share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
        share::schema::ObSchemaGetterGuard guard;
        const uint64_t tenant_id = tasks.at(0).pkey_.get_tenant_id();
        bool tenant_has_been_dropped = false;
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is null", K(ret), K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
          LOG_WARN("fail to get schema guard", K(ret), K(tmp_ret), K(tenant_id));
        } else if (OB_SUCCESS !=
                   (tmp_ret = guard.check_if_tenant_has_been_dropped(tenant_id, tenant_has_been_dropped))) {
          LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tmp_ret), K(tenant_id));
        } else if (tenant_has_been_dropped) {
          skip_to_reput_tasks = true;
          LOG_INFO("skip to reput tasks", K(ret), K(tenant_id), "count", tasks.count());
        }
      }
    }

    int tmp_ret = ObPartitionTableUpdater::throttle(
        false /*is system table*/, ret, ObTimeUtility::current_time() - start_time, is_running_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("throttle failed", K(tmp_ret));
    }

    // ignore retcode, reput into task queue
    if (OB_FAIL(ret) && !skip_to_reput_tasks) {
      if (update_succ) {
        for (int64_t i = 0; !delete_succ && i < pt_delete_items.count(); ++i) {
          for (int64_t j = 0; j < tasks.count(); ++j) {
            if (pt_delete_items.at(i).table_id_ == tasks.at(j).pkey_.get_table_id() &&
                pt_delete_items.at(i).partition_id_ == tasks.at(j).pkey_.get_partition_id()) {
              add_tasks_to_queue_(tasks.at(j));
              break;
            }
          }
        }
      } else {
        for (int64_t i = 0; i < tasks.count(); ++i) {
          add_tasks_to_queue_(tasks.at(i));
        }
      }
    }
  }
  return ret;
}
