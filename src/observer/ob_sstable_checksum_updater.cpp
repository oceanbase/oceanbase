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

#include "ob_sstable_checksum_updater.h"
#include "lib/ob_running_mode.h"
#include "share/ob_task_define.h"
#include "share/ob_sstable_checksum_operator.h"
#include "observer/ob_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_schema_status_proxy.h"
#include "share/ob_thread_mgr.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::obsys;

ObSSTableChecksumUpdateTask::ObSSTableChecksumUpdateTask()
    : pkey_(),
      sstable_id_(OB_INVALID_ID),
      sstable_type_(-1),
      is_remove_(false),
      need_batch_(true),
      add_timestamp_(0),
      update_type_(ObSSTableChecksumUpdateType::UPDATE_ALL)
{}

int ObSSTableChecksumUpdateTask::set_info(const common::ObPartitionKey& pkey, const uint64_t sstable_id,
    const int sstable_type, const bool is_remove, const int64_t timestamp,
    const ObSSTableChecksumUpdateType update_type, const bool need_batch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey), K(sstable_id), K(sstable_type), K(is_remove), K(update_type));
  } else {
    pkey_ = pkey;
    sstable_id_ = sstable_id;
    sstable_type_ = sstable_type;
    is_remove_ = is_remove;
    add_timestamp_ = timestamp;
    update_type_ = update_type;
    need_batch_ = need_batch;
  }
  return ret;
}

bool ObSSTableChecksumUpdateTask::is_valid() const
{
  return pkey_.is_valid();
}

int64_t ObSSTableChecksumUpdateTask::hash() const
{
  int64_t hash_val = 0;
  hash_val = pkey_.hash();
  hash_val = murmurhash(&sstable_id_, sizeof(sstable_id_), hash_val);
  hash_val = murmurhash(&sstable_type_, sizeof(sstable_type_), hash_val);
  hash_val = murmurhash(&is_remove_, sizeof(is_remove_), hash_val);
  hash_val = murmurhash(&update_type_, sizeof(update_type_), hash_val);
  return hash_val;
}

bool ObSSTableChecksumUpdateTask::operator==(const ObSSTableChecksumUpdateTask& other) const
{
  return pkey_ == other.pkey_ && sstable_id_ == other.sstable_id_ && sstable_type_ == other.sstable_type_ &&
         is_remove_ == other.is_remove_ && update_type_ == other.update_type_;
}

bool ObSSTableChecksumUpdateTask::operator!=(const ObSSTableChecksumUpdateTask& other) const
{
  return !(operator==(other));
}

bool ObSSTableChecksumUpdateTask::compare_without_version(const ObSSTableChecksumUpdateTask& other) const
{
  return pkey_ == other.pkey_ && sstable_id_ == other.sstable_id_ && sstable_type_ == other.sstable_type_ &&
         is_remove_ == other.is_remove_;
}

bool ObSSTableChecksumUpdateTask::is_barrier() const
{
  return pkey_ == ObPTUpdateTask::report_merge_finish_pkey_;
}

const int64_t ObSSTableChecksumUpdater::UPDATER_THREAD_CNT;
const int64_t ObSSTableChecksumUpdater::MINI_MODE_UPDATER_THREAD_CNT;
const int64_t ObSSTableChecksumUpdater::EXPECT_BATCH_SIZE;
const int64_t ObSSTableChecksumUpdater::MAX_WAIT_TIME_US;
const int64_t ObSSTableChecksumUpdater::COND_WAIT_US;

ObSSTableChecksumUpdater::ObSSTableChecksumUpdater() : is_inited_(false), stopped_(false), task_queue_(), tg_id_(-1)
{}

int ObSSTableChecksumUpdater::init()
{
  int ret = OB_SUCCESS;
  const int64_t max_partition_cnt = !lib::is_mini_mode() ? MAX_PARTITION_CNT : MINI_MODE_MAX_PARTITION_CNT;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSTableChecksumUpdater has already been inited", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::STableChecksumUp, tg_id_))) {
    LOG_WARN("ObSSTableChecksumUpdater create tg failed", K(ret));
  } else if (OB_FAIL(task_queue_.init(this,
                 !lib::is_mini_mode() ? UPDATER_THREAD_CNT : MINI_MODE_UPDATER_THREAD_CNT,
                 max_partition_cnt,
                 "ChecksumTask"))) {
    LOG_WARN("fail to init task queue", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("failed to init cond", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_WARN("failed to start thread", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableChecksumUpdater::batch_process_tasks(const ObIArray<ObSSTableChecksumUpdateTask>& tasks, bool& stopped)
{
  int ret = OB_SUCCESS;
  bool skip_to_reput_tasks = false;
  UNUSED(stopped);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableChecksumUpdater has not been inited", K(ret));
  } else if (OB_UNLIKELY(tasks.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret));
  } else {
    ObArray<ObSSTableDataChecksumItem> data_checksum_items;
    ObArray<ObSSTableDataChecksumItem> to_delete_data_checksum_items;
    ObArray<ObSSTableColumnChecksumItem> column_checksum_items;
    ObArray<ObSSTableColumnChecksumItem> to_delete_column_checksum_items;
    int64_t start_time = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObSSTableChecksumUpdateTask& task = tasks.at(i);
      if (!task.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ObSSTableChecksumUpdateTask is not valid", K(ret), K(task));
      } else {
        if (!task.is_remove_) {
          if (OB_FAIL(GCTX.ob_service_->fill_checksum(task.pkey_,
                  task.sstable_id_,
                  task.sstable_type_,
                  task.update_type_,
                  data_checksum_items,
                  column_checksum_items))) {
            if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
              ObTaskController::get().allow_next_syslog();
              LOG_INFO("try update not exist or valid partition", K(ret), K(task.pkey_));
              ret = OB_SUCCESS;
              if (OB_FAIL(add_task(task.pkey_, true /*is remove*/))) {
                LOG_ERROR("fail to add task", K(ret), K(task.pkey_));
              }
            } else {
              LOG_WARN("fail to fill checksum", K(ret));
            }
          }
        } else {
          bool need_remove = false;
          ObIPartitionGroupGuard guard;
          if (OB_FAIL(ObPartitionService::get_instance().get_partition(task.pkey_, guard))) {
            if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
              need_remove = true;
              ret = OB_SUCCESS;
            }
          } else {
            need_remove = false;
            LOG_INFO("partition exist again, do not need to remove", K(task.pkey_));
          }
          if (OB_SUCC(ret) && need_remove) {
            ObSSTableDataChecksumItem data_checksum_item;
            ObSSTableColumnChecksumItem column_checksum_item;
            data_checksum_item.tenant_id_ = extract_tenant_id(task.pkey_.get_table_id());
            data_checksum_item.data_table_id_ = task.pkey_.get_table_id();
            data_checksum_item.partition_id_ = task.pkey_.get_partition_id();
            data_checksum_item.sstable_type_ = task.sstable_type_;
            data_checksum_item.server_ = GCTX.self_addr_;
            column_checksum_item.tenant_id_ = extract_tenant_id(task.pkey_.get_table_id());
            column_checksum_item.data_table_id_ = task.pkey_.get_table_id();
            column_checksum_item.partition_id_ = task.pkey_.get_partition_id();
            column_checksum_item.sstable_type_ = task.sstable_type_;
            column_checksum_item.server_ = GCTX.self_addr_;
            if (OB_FAIL(to_delete_data_checksum_items.push_back(data_checksum_item))) {
              LOG_WARN("fail to push back data checksum item", K(ret));
            } else if (OB_FAIL(to_delete_column_checksum_items.push_back(column_checksum_item))) {
              LOG_WARN("fail to push back column checksum item", K(ret));
            }
          }
        }
      }
    }

    start_time = ObTimeUtility::current_time();
    if (OB_SUCC(ret)) {
      if (data_checksum_items.count() > 0) {
        if (OB_FAIL(ObSSTableDataChecksumOperator::batch_report_checksum(data_checksum_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch report checksum", K(ret));
        }
      }

      if (OB_SUCC(ret) && column_checksum_items.count() > 0) {
        if (OB_FAIL(ObSSTableColumnChecksumOperator::batch_report_checksum(column_checksum_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch report checksum", K(ret));
        }
      }

      if (OB_SUCC(ret) && to_delete_data_checksum_items.count() > 0) {
        if (OB_FAIL(ObSSTableDataChecksumOperator::batch_remove_checksum(
                to_delete_data_checksum_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch remove data checksum", K(ret));
        }
      }

      if (OB_SUCC(ret) && to_delete_column_checksum_items.count() > 0) {
        if (OB_FAIL(ObSSTableColumnChecksumOperator::batch_remove_checksum(
                to_delete_column_checksum_items, *GCTX.sql_proxy_))) {
          LOG_WARN("fail to batch remove column checksum", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        // if tenant has been dropped, do not execute these tasks
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
        false /*is system table*/, ret, ObTimeUtility::current_time() - start_time, stopped_);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("throttle failed", K(tmp_ret));
    }

    if (OB_FAIL(ret) && !skip_to_reput_tasks) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = reput_to_queue(tasks))) {
        LOG_WARN("fail to reput to queue", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableChecksumUpdater::reput_to_queue(const ObIArray<ObSSTableChecksumUpdateTask>& tasks)
{
  int ret = OB_SUCCESS;
  // ignore ret code, try best to put back as many tasks as we can
  for (int64_t i = 0; i < tasks.count(); ++i) {
    const ObSSTableChecksumUpdateTask& task = tasks.at(i);
    if (!task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(task));
    } else if (OB_FAIL(task_queue_.add(task))) {
      if (OB_LIKELY(OB_EAGAIN == ret)) {
        LOG_DEBUG("task has been added already", K(task));
      } else {
        LOG_ERROR("failed to add task to queue", K(ret), K(task));
      }
    }
  }
  return ret;
}

int ObSSTableChecksumUpdater::process_barrier(const ObSSTableChecksumUpdateTask& task, bool& stopped)
{
  int ret = OB_SUCCESS;
  UNUSED(task);
  UNUSED(stopped);
  return ret;
}

int ObSSTableChecksumUpdater::add_task(const ObPartitionKey& pkey, const bool is_remove,
    const ObSSTableChecksumUpdateType update_type, const uint64_t sstable_id, const int sstable_type,
    const bool task_need_batch)
{
  int ret = OB_SUCCESS;
  ObSSTableChecksumUpdateTask task;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableChecksumUpdater has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey));
  } else if (OB_FAIL(task.set_info(pkey,
                 sstable_id,
                 sstable_type,
                 is_remove,
                 ObTimeUtility::current_time(),
                 update_type,
                 task_need_batch))) {
    LOG_WARN("fail to set task info", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(buffered_tasks_.push_back(task))) {
      LOG_WARN("failed to push task to buffered_tasks_", K(ret), K(task));
    } else {
      int64_t time_to_wait = 0;
      try_submit_task(time_to_wait);
    }
  }
  return ret;
}

void ObSSTableChecksumUpdater::stop()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObSSTableChecksumUpdater has not been inited");
  } else {
    stopped_ = true;
    TG_STOP(tg_id_);
    task_queue_.stop();
  }
}

void ObSSTableChecksumUpdater::wait()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN("ObSSTableChecksumUpdater has not been inited");
  } else {
    TG_WAIT(tg_id_);
    task_queue_.wait();
  }
}

void ObSSTableChecksumUpdater::destroy()
{
  stop();
  wait();
  TG_DESTROY(tg_id_);
}

void ObSSTableChecksumUpdater::try_submit_task(int64_t& time_to_wait)
{
  time_to_wait = COND_WAIT_US;
  const int64_t task_cnt = buffered_tasks_.count();
  if (task_cnt >= EXPECT_BATCH_SIZE) {
    add_tasks_to_queue();
  } else if (task_cnt > 0) {
    if (!buffered_tasks_.at(task_cnt - 1).need_batch_) {
      add_tasks_to_queue();
    } else {
      const int64_t diff = buffered_tasks_.at(0).get_add_timestamp() + MAX_WAIT_TIME_US - ObTimeUtility::current_time();
      if (0 >= diff) {
        add_tasks_to_queue();
      } else {
        time_to_wait = diff;
      }
    }
  }
}

// cond_ must be held before calling this method
void ObSSTableChecksumUpdater::add_tasks_to_queue()
{
  int ret = OB_SUCCESS;
  // ignore ret code, put as many tasks as we can
  for (int64_t i = 0; i < buffered_tasks_.count(); ++i) {
    const ObSSTableChecksumUpdateTask& task = buffered_tasks_.at(i);
    if (OB_FAIL(task_queue_.add(task))) {
      if (OB_LIKELY(OB_EAGAIN == ret)) {
        LOG_DEBUG("task has been added already", K(task));
      } else {
        LOG_ERROR("failed to add task to queue", K(ret), K(task));
      }
    }
  }
  buffered_tasks_.reuse();
}

void ObSSTableChecksumUpdater::run1()
{
  int64_t time_to_wait = COND_WAIT_US;
  lib::set_thread_name("STableChecksumUp");
  while (!stopped_ && !has_set_stop()) {
    ObThreadCondGuard guard(cond_);
    try_submit_task(time_to_wait);
    cond_.wait_us(std::min(time_to_wait, COND_WAIT_US));
  }
}
