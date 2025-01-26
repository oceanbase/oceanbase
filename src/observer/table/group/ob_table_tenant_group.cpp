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
#include "ob_table_group_service.h"
#include "ob_table_tenant_group.h"
#include "observer/table/ob_table_session_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::common::hash;

namespace oceanbase
{
namespace table
{

ObGetExpiredGroupOp::ObGetExpiredGroupOp(int64_t max_active_ts)
    : max_active_ts_(max_active_ts),
      expired_keys_()
{
  cur_ts_ = common::ObTimeUtility::fast_current_time();
}

int ObGetExpiredGroupOp::operator()(common::hash::HashMapPair<ObITableGroupKey*, ObITableGroupValue*> &entry)
{
  int ret = OB_SUCCESS;
  ObITableGroupValue *group = entry.second;
  if (OB_ISNULL(group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group is NULL", K(ret), K(entry.first));
  } else if ((cur_ts_ - group->group_info_.gmt_modified_) > max_active_ts_) {
    if (OB_FAIL(expired_keys_.push_back(entry.first))) {
      LOG_WARN("fail to push back expired group key", K(ret), KPC(entry.first));
    }
  }
  return ret;
}

bool ObEraseGroupIfEmptyOp::operator()(common::hash::HashMapPair<ObITableGroupKey*, ObITableGroupValue*> &entry)
{
  bool is_erase = (cur_ts_ - entry.second->group_info_.gmt_modified_) > max_active_ts_ && entry.second->get_group_size() == 0;
  return is_erase;
}

int ObGetAllGroupInfoOp::operator()(common::hash::HashMapPair<ObITableGroupKey*, ObITableGroupValue*> &entry) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObITableGroupValue is NULL", KPC(entry.first));
  } else if (OB_FAIL(group_infos_.push_back(entry.second->group_info_))) {
    LOG_WARN("fail to push back group info", K(ret), K(entry.second));
  }
  return ret;
}

int ObGetAllGroupValueOp::operator()(common::hash::HashMapPair<ObITableGroupKey*, ObITableGroupValue*> &entry) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObITableGroupValue is NULL", KPC(entry.first));
  } else if (OB_FAIL(group_values_.push_back(entry.second))) {
    LOG_WARN("fail to push back group info", K(ret), K(entry.second));
  }
  return ret;
}

int ObTableGroupCommitMgr::get_all_group_info(ObGetAllGroupInfoOp &get_op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_map_.foreach_refactored(get_op))) {
    LOG_WARN("fail to get group info from map", K(ret));
  } else if (OB_FAIL(get_op.group_infos_.push_back(failed_groups_.get_group_info()))) {
    LOG_WARN("fail to get group info from failed_groups", K(ret));
  }
  return ret;
}

void ObTableGroupCommitMgr::ObTableGroupTriggerTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_trigger_task())) {
    LOG_WARN("fail to run trigger task", K(ret));
  }
}

int ObTableGroupCommitMgr::ObTableGroupTriggerTask::run_trigger_task()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(trigger_other_group())) {
    LOG_WARN("fail to trigger other group", K(ret));
  } else if (OB_FAIL(trigger_failed_group())) {
    LOG_WARN("fail to trigger failed group", K(ret));
  } else if (OB_FAIL(trigger_expire_group())) {
    LOG_WARN("fail to triggrt expired group", K(ret));
  }

  return ret;
}

int ObTableGroupCommitMgr::ObTableGroupTriggerTask::trigger_other_group()
{
  int ret = OB_SUCCESS;
  ObTableGroupCommitMap &group_map = group_mgr_.get_group_map();
  ObSEArray<ObITableGroupValue*, 32> group_values;
  group_values.set_attr(ObMemAttr(MTL_ID(), "GetAllGrpVals"));
  ObGetAllGroupValueOp all_group_vals_op(group_values);
  if (OB_FAIL(group_map.foreach_refactored(all_group_vals_op))) {
    LOG_WARN("fail to get all group values", K(ret));
  } else {
    for (int i = 0; i < group_values.count() && OB_SUCC(ret); i++) {
      ObITableGroupValue *group = group_values.at(i);
      if (OB_ISNULL(group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group is null", K(ret));
      } else if (!group->has_executable_batch()) {
        // do nothing
        LOG_DEBUG("[group commit debug] group is empty");
      } else {
        ObTableGroupTriggerRequest request;
        ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue*>(group)->group_meta_;
        const ObTableApiCredential &credential = group_meta.credential_;
        if (OB_FAIL(request.init(credential))) {
          LOG_WARN("fail to init request", K(ret));
        } else if (OB_FAIL(ObTableGroupUtils::trigger(request))) {
          LOG_WARN("fail to trigger", K(ret), K(request));
        }
        LOG_DEBUG("[group commit debug] trigger other group", K(ret), K(request), K(credential));
      }
    }
  }
  return ret;
}

int ObTableGroupCommitMgr::ObTableGroupTriggerTask::trigger_expire_group()
{
  int ret = OB_SUCCESS;
  ObTableExpiredGroups &expire_groups = group_mgr_.get_expired_groups();
  if (!expire_groups.get_expired_groups().empty()) {
    ObArenaAllocator tmp_allocator;
    tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "KvExpiredGroup", ObCtxIds::DEFAULT_CTX_ID));
    ObSEArray<ObTableGroupTriggerRequest*, 32> trigger_requests;
    trigger_requests.set_attr(ObMemAttr(MTL_ID(), "GroupTrigger"));
    if (OB_FAIL(expire_groups.construct_trigger_requests(tmp_allocator, trigger_requests))) {
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
      LOG_DEBUG("[group commit debug] trigger expired group", K(ret), K(trigger_requests.count()));
    }
  }
  return ret;
}

int ObTableGroupCommitMgr::ObTableGroupTriggerTask::trigger_failed_group()
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
      LOG_DEBUG("[group commit debug] trigger failed group", K(ret), K(trigger_requests.count()));
    }
  }

  return ret;
}

void ObTableGroupCommitMgr::ObTableGroupUpdateTask::runTimerTask()
{
  clean_expired_group_task();
  update_group_info_task();
}

void ObTableGroupCommitMgr::ObTableGroupUpdateTask::clean_expired_group_task()
{
  int ret = OB_SUCCESS;
  int64_t clean_group_count = group_mgr_.expired_groups_.get_clean_group_counts();
  int64_t clean_count = clean_group_count > MAX_CLEAN_GROUP_SIZE_EACH_TASK ? MAX_CLEAN_GROUP_SIZE_EACH_TASK : clean_group_count;
  // 1. get all the expired group in group map and move to expired_group
  ObGetExpiredGroupOp get_op(GROUP_VALUE_MAX_ACTIVE_TS);
  if (OB_FAIL(group_mgr_.group_map_.foreach_refactored(get_op))) {
    LOG_WARN("fail to scan expired group", K(ret));
  } else {
    // overwrite ret
    for (int64_t i = 0; i < get_op.expired_keys_.count(); i++) {
      ObITableGroupKey *key= get_op.expired_keys_.at(i);
      ObITableGroupValue *group = nullptr;
      ObEraseGroupIfEmptyOp erase_op(GROUP_VALUE_MAX_ACTIVE_TS);
      bool is_erased = false;
      if (OB_FAIL(group_mgr_.group_map_.erase_if(key, erase_op, is_erased, &group))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to erase expired group", K(ret), KPC(key));
        }
      } else if (is_erased && OB_NOT_NULL(group)) {
        if (OB_FAIL(group_mgr_.expired_groups_.add_expired_group(group))) {
          LOG_WARN("fail to add exired group", K(ret), KPC(group), KPC(key));
        } else {
          key->~ObITableGroupKey();
          group_mgr_.allocator_.free(key);
          key = nullptr;
        }
        LOG_DEBUG("[group commit debug] add expired group", K(group_mgr_.expired_groups_.get_expired_groups().size()));
      }
    } // end for
  }

  // 2. clean the group which is ready to be free in expire_groups
  while (clean_count--) { // ignore ret
    ObITableGroupValue *group = nullptr;
    if (OB_FAIL(group_mgr_.expired_groups_.pop_clean_group(group))) {
      // overwrite ret
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to get clean group", K(ret));
      }
    } else if (OB_NOT_NULL(group)) {
      group->~ObITableGroupValue();
      group_mgr_.allocator_.free(group);
      group = nullptr;
    }
    LOG_DEBUG("[group commit debug] free clean group", K(group_mgr_.expired_groups_.get_clean_group_counts()));
  } // end while
}

void ObTableGroupCommitMgr::ObTableGroupUpdateTask::update_group_info_task()
{
  int ret = OB_SUCCESS;
  bool is_group_commit_config_enable = ObTableGroupUtils::is_group_commit_config_enable();
  if (is_group_commit_config_enable || (!is_group_commit_config_enable && need_update_group_info_)) {
    // 1. check and refresh all group info status in group_map
    ObTableGroupCommitMap &group_map = group_mgr_.get_group_map();
    ObTableGroupCommitMap::iterator iter = group_map.begin();
    // ignore ret
    for (;iter != group_map.end(); iter++) {
      ObITableGroupKey *key = iter->first;
      ObITableGroupValue *group = iter->second;
      if (OB_NOT_NULL(group)) {
        ObTableGroupInfo &group_info = group->group_info_;
        group_info.queue_size_ = group->get_group_size();
        group_info.batch_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
      }
    } // end for

    // 2. update group info in failed_groups
    {
      ObTableGroupInfo &group_info = group_mgr_.failed_groups_.get_group_info();
      group_info.queue_size_ = group_mgr_.get_failed_groups().count();
      group_info.batch_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
    }

    need_update_group_info_ = is_group_commit_config_enable ? true : false;
  }
}

void ObTableGroupCommitMgr::ObTableGroupOpsTask::runTimerTask()
{
  update_ops_task();
}

void ObTableGroupCommitMgr::ObTableGroupOpsTask::update_ops_task()
{
  int ret = OB_SUCCESS;
  // only use to record ops count currently and self-adaptive strategy is coming soon
  if (ObTableGroupUtils::is_group_commit_config_enable()) {
    ObTableGroupOpsCounter &ops = group_mgr_.get_ops_counter();
    int64_t enable_threshold = group_mgr_.get_enable_ops_threshold();
    if (TABLEAPI_GROUP_COMMIT_MGR->get_last_ops() >= enable_threshold &&
        TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()) {
      TABLEAPI_GROUP_COMMIT_MGR->set_group_commit_disable(false);
      LOG_INFO("enable group commit", K(TABLEAPI_GROUP_COMMIT_MGR->get_last_ops()), K(enable_threshold));
    } else if (TABLEAPI_GROUP_COMMIT_MGR->get_last_ops() < enable_threshold &&
              !TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()) {
      TABLEAPI_GROUP_COMMIT_MGR->set_group_commit_disable(true);
      LOG_INFO("disable group commit", K(TABLEAPI_GROUP_COMMIT_MGR->get_last_ops()), K(enable_threshold));
    }
    if (TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups().count() >= DEFAULT_DISABLE_MAX_FAILED_GROUP_SIZE) {
       TABLEAPI_GROUP_COMMIT_MGR->set_group_commit_disable(true);
      LOG_INFO("disable group commit due to too much failed groups", K(TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups().count()),
                K(TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()));
    }
    int64_t cur_read_op_ops = ops.get_read_ops();
    int64_t cur_write_op_ops = ops.get_write_ops();
    group_mgr_.set_last_write_ops(cur_write_op_ops);
    group_mgr_.set_last_read_ops(cur_read_op_ops);
    ops.reset_ops(); // reset ops counter per second
  }
}

int ObTableGroupCommitMgr::start_timer()
{
  int ret = OB_SUCCESS;

  if (!timer_.inited()) {
    timer_.set_run_wrapper(MTL_CTX());
    if (OB_FAIL(timer_.init("TableGroupCommitMgr"))) {
      LOG_WARN("fail to init kv group commit timer", KR(ret));
    } else if (OB_FAIL(timer_.schedule(group_trigger_task_,
                                       ObTableGroupTriggerTask::TASK_SCHEDULE_INTERVAL,
                                       true))) {
      LOG_WARN("fail to schedule group commit trigger task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(group_update_task_,
                                       ObTableGroupUpdateTask::TASK_SCHEDULE_INTERVAL,
                                       true))) {
      LOG_WARN("fail to schedule group commit update task", KR(ret));
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
    ObTableGroupRegister::register_group_objs();
    const ObMemAttr attr(MTL_ID(), "TbGroupComMgr");
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      LOG_WARN("fail to init allocator", K(ret));
    } else if (OB_FAIL(group_map_.create(DEFAULT_GROUP_SIZE, "HashBucApiGroup", "HasNodApiGroup", MTL_ID()))) {
      LOG_WARN("fail to init group map", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(expired_groups_.init())) {
      LOG_WARN("fail to init expired groups", K(ret));
    } else if (OB_FAIL(failed_groups_.init())) {
      LOG_WARN("fail to init failed groups", K(ret));
    } else {
        enable_ops_threshold_ = get_enable_ops_threshold();
        is_inited_ = true;
    }
  }

  return ret;
}

int ObTableGroupCommitMgr::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table group commit mgr not init", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                     group_ops_task_,
                                     ObTableGroupOpsTask::TASK_SCHEDULE_INTERVAL,
                                     true))) {
    LOG_WARN("fail to schedule group ops task", K(ret));
  } else {
    LOG_INFO("successfully to start ObTableGroupCommitMgr");
  }
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

int ObTableGroupCommitMgr::clean_group_map()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITableOp*, 16> ops;
  ObHashMap<ObITableGroupKey*, ObITableGroupValue*>::iterator iter = group_map_.begin();
  for (; iter != group_map_.end(); iter++) {
    ObITableGroupKey *key = iter->first;
    ObITableGroupValue *group = iter->second;
    int64_t batch_size = get_group_size();
    ops.reuse();
    if (OB_NOT_NULL(group)) {
      while(group->has_executable_batch()) {
        if (OB_FAIL(group->get_executable_group(batch_size, ops, false))) {
          LOG_WARN("fail to get executable batch", K(ret));
        } else if (ops.count() == 0) {
          // do nothing
        } else {
          ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue *>(group)->group_meta_;
          ObTableGroup *exec_group = nullptr;
          if (OB_ISNULL(exec_group = group_factory_.alloc())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc execute group", K(ret));
          } else if (OB_FAIL(exec_group->init(group_meta, ops))) {
            LOG_WARN("fail to init exec group", K(ret), K(group_meta), K(ops));
          } else if (OB_FAIL(ObTableGroupExecuteService::execute(*exec_group, true /*add failed group*/))) {
            LOG_WARN("fail to execute group", K(ret));
          }
        }
      } // end while

      // free group whether succes or not
      group->~ObITableGroupValue();
      allocator_.free(group);
      group = nullptr;
    }

    if (OB_NOT_NULL(key)) {
      key->~ObITableGroupKey();
      allocator_.free(key);
      key = nullptr;
    }

  }

  return ret;
}

int ObTableGroupCommitMgr::clean_expired_groups()
{
  int ret = OB_SUCCESS;
  LOG_INFO("clean expired groups:", K(expired_groups_.get_expired_groups().size()),
            K(expired_groups_.get_clean_group_counts()));
  // 1. clean all remaining expired groups
  while (!expired_groups_.get_expired_groups().empty()) {
    ObITableGroupValue *group = nullptr;
    if (OB_FAIL(expired_groups_.pop_expired_group(group))) {
      LOG_WARN("fail to pop expired group", K(ret));
    } else if (OB_NOT_NULL(group)) {
      int64_t batch_size = get_group_size();
      ObSEArray <ObITableOp*, 16> ops;
      while(group->has_executable_batch()) {
        if (OB_FAIL(group->get_executable_group(batch_size, ops, false))) {
          LOG_WARN("fail to get executable queue", K(ret));
        } else if (ops.count() == 0) {
          // do nothing
          LOG_DEBUG("ops count is 0");
        } else {
          ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue *>(group)->group_meta_;
          ObTableGroup *exec_group = nullptr;
          if (OB_ISNULL(exec_group = group_factory_.alloc())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc execute group", K(ret));
          } else if (OB_FAIL(exec_group->init(group_meta, ops))) {
            LOG_WARN("fail to init exec group", K(ret), K(group_meta), K(ops));
          } else if (OB_FAIL(ObTableGroupExecuteService::execute(*exec_group, false))) {
            LOG_WARN("fail to execute group", K(ret));
          }
        }
      } // end while
      // free group whether succes or not
      group->~ObITableGroupValue();
      allocator_.free(group);
      group = nullptr;
    }
  }

  // 2. clean all remaining clean group
  while (!expired_groups_.get_clean_groups().empty()) {
    ObITableGroupValue *group = nullptr;
    if (OB_FAIL(expired_groups_.pop_clean_group(group))) {
      // overwrite ret
      LOG_WARN("fail to pop clean group", K(ret));
    } else if (OB_NOT_NULL(group)) {
      group->~ObITableGroupValue();
      allocator_.free(group);
      group = nullptr;
    }
  }
  return ret;
}

int ObTableGroupCommitMgr::clean_failed_groups()
{
  int ret = OB_SUCCESS;
  LOG_INFO("clean failed groups:", K(failed_groups_.get_failed_groups().size()));
  while (!failed_groups_.empty()) {
    ObTableGroup *group = failed_groups_.get();
    if (OB_NOT_NULL(group)) {
      if (OB_FAIL(ObTableGroupService::process_one_by_one(*group))) {
        LOG_WARN("fail to process group one by one", K(ret));
      }
    }
    if (OB_NOT_NULL(group)) {
      group_factory_.free(group);
    }
  }
  return ret;
}

void ObTableGroupCommitMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // 1. destroy timer task
    if (timer_.inited()) {
      timer_.destroy();
    }
    // 2. clean remian groups in group_map
    if (OB_FAIL(clean_group_map())) {
      LOG_WARN("fail to clean group map", K(ret));
    }
    // 3. clean remain groups in expired_groups
    if (OB_FAIL(clean_expired_groups())) {
      // overwrite ret
      LOG_WARN("fail to clean expired groups", K(ret));
    }
    // 4. clean remian groups in failed groups
    if (OB_FAIL(clean_failed_groups())) {
      // overwrite ret
      LOG_WARN("fail to clean failed groups", K(ret));
    }
    // 5. clear resource
    group_map_.clear();
    group_factory_.free_all();
    op_factory_.free_all();
    is_inited_ = false;
    LOG_INFO("ObTableGroupCommitMgr destroy successfully");
  }
}

int64_t ObTableGroupCommitMgr::get_group_size() const
{
  int64_t batch_size = 1;
  batch_size = TABLEAPI_SESS_POOL_MGR->get_kv_group_commit_batch_size();
  return batch_size > 1 ? batch_size : 1;
}

int64_t ObTableGroupCommitMgr::get_enable_ops_threshold() const
{
  int64_t enable_ops_threshold = enable_ops_threshold_;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    enable_ops_threshold = tenant_config->_enable_kv_group_commit_ops;
    enable_ops_threshold = enable_ops_threshold >= 0 ? enable_ops_threshold : enable_ops_threshold_;
  }
  return enable_ops_threshold;
}

int ObTableGroupCommitMgr::create_and_add_group(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTableGroupType group_type = ctx.group_type_;
  ObITableGroupKey *key = nullptr;
  ObITableGroupValue *group = nullptr;
  if (group_type < 0 || group_type >= ObTableGroupType::TYPE_MAX) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid op type", K(ret), K(group_type));
  } else if (OB_FAIL(GROUP_KEY_ALLOC[group_type](allocator_, key))) {
    LOG_WARN("fail to alloc ObTableGroupKey", K(ret), K(group_type));
  } else if (OB_FAIL(GROUP_VAL_ALLOC[group_type](allocator_, group))) {
    LOG_WARN("fail to alloc ObTableGroupValue", K(ret), K(group_type));
  } else {
    if (OB_FAIL(key->deep_copy(allocator_, *ctx.key_))) {
      LOG_WARN("fail to deep copy group key", K(ret), KPC(ctx.key_));
    } else if (OB_FAIL(group->init(ctx))) {
      LOG_WARN("fail to init group", K(ret), K(ctx));
    } else if (OB_FAIL(group_map_.set_refactored(key, group))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to set group to hash map", K(ret), KPC(key));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(key)) {
      key->~ObITableGroupKey();
      allocator_.free(key);
      key = nullptr;
    }
    if (OB_NOT_NULL(group)) {
      group->~ObITableGroupValue();
      allocator_.free(group);
      group = nullptr;
    }
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTableGroupCommitMgr::get_or_create_group(const ObTableGroupCtx &ctx, ObITableGroupValue *&group)
{
  int ret = OB_SUCCESS;
  group = nullptr;
  if (OB_FAIL(group_map_.get_refactored(ctx.key_, group))) {
    if (ret != OB_HASH_NOT_EXIST) {
      LOG_WARN("fail to replace group", K(ret), KPC(ctx.key_));
    } else {
      // not exist, create it
      if (OB_FAIL(create_and_add_group(ctx))) { // create and add group
        LOG_WARN("fail to create and add group", K(ret), K(ctx));
      } else if (OB_FAIL(group_map_.get_refactored(ctx.key_, group))) { // again
        LOG_WARN("fail to get group", K(ret), KPC(ctx.key_));
      }
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
