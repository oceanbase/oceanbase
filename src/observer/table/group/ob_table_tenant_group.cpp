/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_tenant_group.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::common::hash;

namespace oceanbase
{
namespace table
{

void ObTableGroupCommitMgr::ObTableGroupStatisAndTriggerTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  run_statistics_task();

  if (OB_FAIL(run_trigger_task())) {
    LOG_WARN("fail to run trigger task", K(ret));
  }
}

void ObTableGroupCommitMgr::ObTableGroupStatisAndTriggerTask::run_statistics_task()
{
  int ret = OB_SUCCESS;
  ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid() && tenant_config->enable_kv_group_commit) {
    ObTableMovingAverage<int64_t> &queue_times = group_mgr_.get_queue_times();
    int64_t cur_queue_time = group_mgr_.get_queue_time();
    if (OB_FAIL(queue_times.add(cur_queue_time))) {
      LOG_WARN("fail to add queue time", K(ret), K(cur_queue_time));
    }
  }
}

int ObTableGroupCommitMgr::ObTableGroupStatisAndTriggerTask::run_trigger_task()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(trigger_other_group())) {
    LOG_WARN("fail to trigger other group", K(ret));
  } else if (OB_FAIL(trigger_failed_group())) {
    LOG_WARN("fail to trigger failed group", K(ret));
  }

  return ret;
}

int ObTableGroupCommitMgr::ObTableGroupStatisAndTriggerTask::trigger_other_group()
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitMgr::ObTableGroupCommitMap &groups = group_mgr_.get_groups();
  ObTableTriggerGroupsGetter getter;

  if (OB_FAIL(group_mgr_.get_groups().foreach_refactored(getter))) {
    LOG_WARN("fail to foreach groups", K(ret));
  } else {
    for (int64_t i = 0; i < getter.trigger_requests_.count() && OB_SUCC(ret); i++) {
      ObTableGroupTriggerRequest *request = getter.trigger_requests_.at(i);
      if (OB_ISNULL(request)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("trigger request is null", K(ret));
      } else if (OB_FAIL(ObTableGroupUtils::trigger(*request))) {
        LOG_WARN("fail to trigger", K(ret), KPC(request));
      }
    }
  }

  return ret;
}

int ObTableGroupCommitMgr::ObTableGroupStatisAndTriggerTask::trigger_failed_group()
{
  int ret = OB_SUCCESS;
  ObTableFailedGroups &failed_groups = group_mgr_.get_failed_groups();

  if (!failed_groups.empty()) {
    ObArenaAllocator tmp_allocator;
    tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "KvFailedGroup", ObCtxIds::DEFAULT_CTX_ID));
    ObSEArray<ObTableGroupTriggerRequest*, 32> trigger_requests;
    trigger_requests.set_attr(ObMemAttr(MTL_ID(), "GroupTrigger"));
    if (OB_FAIL(failed_groups.construct_trigger_requests(tmp_allocator, trigger_requests))) {
      LOG_WARN("fail to construct trigger requests", K(ret));
    } else {
      for (int64_t i = 0; i < trigger_requests.count() && OB_SUCC(ret); i++) {
        ObTableGroupTriggerRequest *request = trigger_requests.at(i);
        if (OB_ISNULL(request)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("trigger request is null", K(ret));
        } else if (OB_FAIL(ObTableGroupUtils::trigger(*request))) {
          LOG_WARN("fail to trigger", K(ret), KPC(request));
        }
      }
    }
  }

  return ret;
}

int64_t ObTableGroupCommitMgr::ObTableGroupSizeAndOpsTask::get_new_group_size(int64_t cur, double persent)
{
  double delta = persent * DEFAULT_GROUP_CHANGE_SIZE;
  if (delta > 0 && delta < 0.5) {
    delta = 1;
  } else if (delta > 0.5) {
    delta = DEFAULT_GROUP_CHANGE_SIZE;
  }
  int64_t new_group_size = cur + delta;
  if (new_group_size > DEFAULT_MAX_GROUP_SIZE) {
    new_group_size = DEFAULT_MAX_GROUP_SIZE;
  } else if (new_group_size < DEFAULT_MIN_GROUP_SIZE) {
    new_group_size = DEFAULT_MIN_GROUP_SIZE;
  }
  return new_group_size;
}

void ObTableGroupCommitMgr::ObTableGroupSizeAndOpsTask::runTimerTask()
{
  ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid() && tenant_config->enable_kv_group_commit) {
    ObTableMovingAverage<int64_t> &queue_times = group_mgr_.get_queue_times();
    int64_t cur_avg_queue_time = queue_times.get_average();
    int64_t cur_queue_time = group_mgr_.get_queue_time();
    int64_t last_avg_queue_time = group_mgr_.get_last_queue_time();
    int64_t last_put_ops = group_mgr_.get_last_put_ops();
    int64_t last_get_ops = group_mgr_.get_last_get_ops();

    // enable group commit if queue time >= DEFAULT_ENABLE_GROUP_COMMIT_QUEUE_TIME
    if (cur_queue_time >= DEFAULT_ENABLE_GROUP_COMMIT_QUEUE_TIME && group_mgr_.is_group_commit_disable()) {
      group_mgr_.set_group_commit_disable(false);
      LOG_INFO("enable group commit", K(cur_queue_time));
    }

    if (!group_mgr_.is_group_commit_disable()) {
      const ObTableGroupOpsCounter &ops = group_mgr_.get_ops_counter();
      int64_t cur_get_op_ops = ops.get_get_op_ops();
      int64_t cur_put_op_ops = ops.get_put_op_ops();
      int64_t total_ops = cur_get_op_ops + cur_put_op_ops;
      double get_op_persent = 0.5;
      double put_op_persent = 0.5;
      if (total_ops != 0) {
        get_op_persent = cur_get_op_ops / total_ops;
        put_op_persent = cur_put_op_ops / total_ops;
      }

      int64_t cur_get_op_group_size = group_mgr_.get_get_op_group_size();
      int64_t cur_put_op_group_size = group_mgr_.get_put_op_group_size();
      bool is_qt_decing = cur_avg_queue_time < (last_avg_queue_time - last_avg_queue_time * DEFAULT_FLUCTUATION);
      bool is_put_ops_rising = cur_put_op_ops > (last_put_ops + last_put_ops * DEFAULT_FLUCTUATION);
      bool is_put_ops_decing = cur_put_op_ops < (last_put_ops - last_put_ops * DEFAULT_FLUCTUATION);
      bool is_get_ops_rising = cur_get_op_ops > (last_get_ops + last_get_ops * DEFAULT_FLUCTUATION);
      bool is_get_ops_decing = cur_get_op_ops < (last_get_ops - last_get_ops * DEFAULT_FLUCTUATION);
      // inc put group size
      if (is_put_ops_rising && is_qt_decing) {
        int64_t new_group_size = get_new_group_size(cur_put_op_group_size, put_op_persent);
        group_mgr_.set_put_op_group_size(new_group_size);
        LOG_INFO("inc put group size", K(cur_avg_queue_time), K(last_avg_queue_time), K(cur_put_op_ops),
            K(last_put_ops), K(cur_put_op_group_size), K(new_group_size));
      }

      // dec put group size
      if (is_put_ops_decing && is_qt_decing) {
        int64_t new_group_size = get_new_group_size(cur_put_op_group_size, put_op_persent);
        group_mgr_.set_put_op_group_size(new_group_size);
        LOG_INFO("dec group size", K(cur_avg_queue_time), K(last_avg_queue_time), K(cur_put_op_ops),
            K(last_put_ops), K(cur_put_op_group_size), K(new_group_size));
      }

      // inc get group size
      if (is_get_ops_rising && is_qt_decing) {
        int64_t new_group_size = get_new_group_size(cur_get_op_group_size, get_op_persent);
        group_mgr_.set_get_op_group_size(new_group_size);
        LOG_INFO("inc get group size", K(cur_avg_queue_time), K(last_avg_queue_time), K(cur_get_op_ops),
            K(last_get_ops), K(cur_get_op_group_size), K(new_group_size));
      }

      // dec get group size
      if (is_get_ops_decing && is_qt_decing) {
        int64_t new_group_size = get_new_group_size(cur_get_op_group_size, get_op_persent);
        group_mgr_.set_get_op_group_size(new_group_size);
        LOG_INFO("dec get group size", K(cur_avg_queue_time), K(last_avg_queue_time), K(cur_get_op_ops),
            K(last_get_ops), K(cur_get_op_group_size), K(new_group_size));
      }

      // set minimum group size if ops is too low
      if (cur_put_op_ops <= DEFAULT_MIN_OPS && group_mgr_.get_put_op_group_size() != DEFAULT_MIN_GROUP_SIZE) {
        group_mgr_.set_put_op_group_size(DEFAULT_MIN_GROUP_SIZE);
        LOG_INFO("set put min group size", K(cur_put_op_ops));
      }

      // set minimum group size if ops is too low
      if (cur_get_op_ops <= DEFAULT_MIN_OPS && group_mgr_.get_get_op_group_size() != DEFAULT_MIN_GROUP_SIZE) {
        group_mgr_.set_get_op_group_size(DEFAULT_MIN_GROUP_SIZE);
        LOG_INFO("set get min group size", K(cur_get_op_ops));
      }

      group_mgr_.set_last_queue_time(cur_avg_queue_time);
      group_mgr_.set_last_put_ops(cur_put_op_ops);
      group_mgr_.set_last_get_ops(cur_get_op_ops);
    }

    group_mgr_.get_ops_counter().reset_ops(); // reset ops counter per second
  }
}

int ObTableGroupCommitMgr::start_timer()
{
  int ret = OB_SUCCESS;

  if (!timer_.inited()) {
    timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(timer_.init("TableGroupCommitMgr"))) {
      LOG_WARN("fail to init kv group commit timer", KR(ret));
    } else if (OB_FAIL(timer_.schedule(statis_and_trigger_task_,
                                       ObTableGroupStatisAndTriggerTask::TASK_SCHEDULE_INTERVAL,
                                       true))) {
      LOG_WARN("fail to schedule group commit statis and trigger task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(group_size_and_ops_task_,
                                       ObTableGroupSizeAndOpsTask::TASK_SCHEDULE_INTERVAL,
                                       true))) {
      LOG_WARN("fail to schedule group commit ops and group size task", KR(ret));
    } else {
      LOG_INFO("successfully schedule kv group commit timer");
    }
  }

  return ret;
}

int ObTableGroupCommitMgr::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_FAIL(groups_.create(DEFAULT_GROUP_SIZE,
                               "HashBucApiGroup",
                               "HasNodApiGroup",
                               MTL_ID()))) {
      LOG_WARN("fail to init sess pool", K(ret), K(MTL_ID()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroupCommitMgr::start()
{
  int ret = OB_SUCCESS;
  LOG_INFO("successfully to start ObTableGroupCommitMgr");
  return ret;
}

void ObTableGroupCommitMgr::stop()
{
  if (timer_.inited()) {
    timer_.stop();
  }
}

void ObTableGroupCommitMgr::wait()
{
  if (timer_.inited()) {
    timer_.wait();
  }
}

void ObTableGroupCommitMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // 1. destroy timer task
    if (timer_.inited()) {
      timer_.destroy();
    }

    // 2. check whether there are any remaining operations in the groups_ that have not been executed
    ObTableGroupForeacher foreacher;
    if (OB_FAIL(groups_.foreach_refactored(foreacher))) {
      LOG_WARN("fail to foreach groups", K(ret));
    } else {
      bool add_failed_group = false;
      for (int64_t i = 0; i < foreacher.groups_.count() && OB_SUCC(ret); i++) {
        ObTableGroupCommitOps *group = foreacher.groups_.at(i);
        if (OB_ISNULL(group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group is null", K(ret));
        } else if (OB_FAIL(ObTableGroupExecuteService::execute(*group,
                                                               &TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups(),
                                                               &TABLEAPI_GROUP_COMMIT_MGR->get_group_factory(),
                                                               &TABLEAPI_GROUP_COMMIT_MGR->get_op_factory(),
                                                               add_failed_group))) {
          LOG_WARN("fail to execute group", K(ret), KPC(group));
        }
      }
    }

    // 3. clear resource
    groups_.clear();
    group_factory_.free_all();
    op_factory_.free_all();
    is_inited_ = false;
    LOG_INFO("ObTableGroupCommitMgr destroy successfully");
  }
}

int ObTableGroupCommitMgr::create_and_add_group(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitOps *tmp_group = nullptr;

  if (OB_ISNULL(ctx.key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key is null", K(ret));
  } else if (OB_ISNULL(tmp_group = group_factory_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableGroupCommitOps", K(ret), K_(group_factory));
  } else if (OB_FAIL(tmp_group->init(ctx.credential_,
                                     ctx.key_->ls_id_,
                                     ctx.key_->table_id_,
                                     ctx.tablet_id_,
                                     ctx.entity_type_,
                                     ctx.timeout_ts_))) {
    LOG_WARN("fail to init group", K(ret), K(ctx));
    group_factory_.free(tmp_group);
    tmp_group = nullptr;
  } else if (OB_FAIL(groups_.set_refactored(ctx.key_->hash_, tmp_group))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to set group to hash map", K(ret), K(ctx.key_->hash_));
    } else {
      ret = OB_SUCCESS; // replace error code
    }
    // this group has been set by other thread, free it
    group_factory_.free(tmp_group);
  }

  return ret;
}

int ObTableGroupFeeder::operator()(MapKV &entry)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(group_ = entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null op", K(ret), K(entry.first));
  } else if (OB_FAIL(group_->add_op(op_))) {
    LOG_WARN("fail to add op to group", K(ret), KPC_(group), KPC_(op));
  } else if (group_->need_execute(group_mgr_.get_group_size(group_->is_get()))) {
    need_execute_ = true;
    ObTableGroupCommitOps *new_group = nullptr;
    if (OB_ISNULL(new_group = group_mgr_.alloc_group())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObTableGroupCommitOps", K(ret));
    } else if (OB_FAIL(new_group->init(group_->credential_,
                                       group_->ls_id_,
                                       group_->table_id_,
                                       group_->tablet_id_,
                                       group_->entity_type_,
                                       INT64_MAX))) {
      LOG_WARN("fail to init group", K(ret), KPC_(group));
      group_mgr_.free_group(new_group);
      new_group = nullptr;
    } else {
      entry.second = new_group;
    }
  }

  return ret;
}

int ObTableExecuteGroupsGetter::operator()(HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitOps *group = kv.second;

  if (OB_NOT_NULL(group) && group->need_execute(TABLEAPI_GROUP_COMMIT_MGR->get_group_size(group->is_get()))
      && OB_FAIL(can_execute_keys_.push_back(kv.first))) {
    LOG_WARN("fail to push back key", K(ret), K_(can_execute_keys));
  }

  return ret;
}

int ObTableTriggerGroupsGetter::operator()(HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitOps *group = kv.second;

  if (OB_NOT_NULL(group) && group->need_execute(TABLEAPI_GROUP_COMMIT_MGR->get_group_size(group->is_get()))) {
    ObTableGroupTriggerRequest *request = OB_NEWx(ObTableGroupTriggerRequest, &allocator_);
    if (OB_ISNULL(request)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc request", K(ret));
    } else if (OB_FAIL(request->init(group->credential_))) {
      LOG_WARN("fail to init request", K(ret));
    } else if (OB_FAIL(trigger_requests_.push_back(request))) {
      LOG_WARN("fail to push back request");
    }
  }

  return ret;
}

int ObTableTimeoutGroupCounter::operator()(HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv)
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitOps *group = kv.second;

  if (OB_ISNULL(group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null group", K(ret), K(kv.first));
  } else if (group->is_timeout()) {
    count_++;
  }

  return ret;
}

int ObTableGroupForeacher::operator()(HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(groups_.push_back(kv.second))) {
    LOG_WARN("fail to push back group", K(ret));
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase
