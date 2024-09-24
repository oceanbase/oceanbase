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
#include "ob_table_group_service.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::common::hash;

namespace oceanbase
{
namespace table
{

ObGetExpiredLsGroupOp::ObGetExpiredLsGroupOp(int64_t max_active_ts)
    : max_active_ts_(max_active_ts),
      expired_ls_groups_()
{
  cur_ts_ = common::ObTimeUtility::fast_current_time();
}

int ObGetExpiredLsGroupOp::operator()(common::hash::HashMapPair<uint64_t, ObTableLsGroup*> &entry)
{
  int ret = OB_SUCCESS;
  ObTableLsGroup *ls_group = entry.second;
  if (OB_ISNULL(ls_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls group is NULL", K(ret), K(entry.first));
  } else if ((cur_ts_ - ls_group->last_active_ts_) > max_active_ts_) {
    if (OB_FAIL(expired_ls_groups_.push_back(entry.first))) {
      LOG_WARN("fail to push back expired ls group key", K(ret), K(entry.first));
    }
  }
  return ret;
}

int ObCreateLsGroupOp::operator()(const common::hash::HashMapPair<uint64_t, ObTableLsGroup*> &entry)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = entry.first;
  ObTableLsGroupInfo group_info;
  if (OB_FAIL(group_info.init(commit_key_))) {
    LOG_WARN("fail to init group info", K(commit_key_), K(ret));
  } else if (OB_FAIL(group_info_map_.set_refactored(hash_val, group_info))) {
    if (ret != OB_HASH_EXIST) {
      LOG_WARN("fail to set group info", K(ret), K(group_info));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

bool ObEraseLsGroupIfEmptyOp::operator()(common::hash::HashMapPair<uint64_t, ObTableLsGroup*> &entry)
{
  bool is_erase = (cur_ts_ - entry.second->last_active_ts_) > max_active_ts_ && entry.second->get_queue_size() == 0;
  int ret = OB_SUCCESS;
  if (is_erase) {
    if (OB_FAIL(group_info_map_.erase_refactored(entry.first))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to erase expired ls info group", K(ret), K(entry.first));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return (ret == OB_SUCCESS) && is_erase;
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
  ObHashMap<uint64_t, ObTableLsGroup*>::iterator iter = group_map.begin();

  for (; OB_SUCC(ret) && iter != group_map.end(); iter++) {
    ObTableLsGroup *ls_group = iter->second;
    if (OB_ISNULL(ls_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group is null", K(ret));
    } else if (!ls_group->has_executable_ops()) {
      // do nothing
    } else {
      ObTableGroupTriggerRequest request;
      if (OB_FAIL(request.init(ls_group->meta_.credential_))) {
        LOG_WARN("fail to init request", K(ret));
      } else if (OB_FAIL(ObTableGroupUtils::trigger(request))) {
        LOG_WARN("fail to trigger", K(ret), K(request));
      }
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

void ObTableGroupCommitMgr::ObTableGroupInfoTask::runTimerTask()
{
  update_ops_task();
  clean_expired_group_task();
  update_group_info_task();
}

void ObTableGroupCommitMgr::ObTableGroupInfoTask::clean_expired_group_task()
{
  int ret = OB_SUCCESS;
  int64_t clean_group_count = group_mgr_.expired_groups_.get_clean_group_counts();
  int64_t clean_count = clean_group_count > MAX_CLEAN_GROUP_SIZE_EACH_TASK ? MAX_CLEAN_GROUP_SIZE_EACH_TASK : clean_group_count;
  // 1. get all the expired ls group in group map and move to expired_group
  ObGetExpiredLsGroupOp get_op(LS_GROUP_MAX_ACTIVE_TS);
  if (OB_FAIL(group_mgr_.group_map_.foreach_refactored(get_op))) {
    LOG_WARN("fail to scan expired ls group", K(ret));
  } else {
    // overwrite ret
    for (int64_t i = 0; i < get_op.expired_ls_groups_.count(); i++) {
      uint64_t hash_val = get_op.expired_ls_groups_.at(i);
      ObTableLsGroup *ls_group = nullptr;
      ObEraseLsGroupIfEmptyOp erase_op(group_mgr_.group_info_map_, LS_GROUP_MAX_ACTIVE_TS);
      bool is_erased = false;
      if (OB_FAIL(group_mgr_.group_map_.erase_if(hash_val, erase_op, is_erased, &ls_group))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to erase expired ls group", K(ret), K(hash_val));
        }
      } else if (is_erased && OB_NOT_NULL(ls_group)) {
        if (OB_FAIL(group_mgr_.expired_groups_.add_expired_group(ls_group))) {
          LOG_WARN("fail to add exired ls group", K(ret), KPC(ls_group), K(hash_val));
        }
      }
    } // end for
  }

  // 2. clean the ls group which is ready to be free in expire_groups
  while (clean_count--) { // ignore ret
    ObTableLsGroup *ls_group = nullptr;
    if (OB_FAIL(group_mgr_.expired_groups_.pop_clean_group(ls_group))) {
      // overwrite ret
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to get clean ls group", K(ret));
      }
    } else if (OB_NOT_NULL(ls_group)) {
      ls_group->~ObTableLsGroup();
      group_mgr_.allocator_.free(ls_group);
      ls_group = nullptr;
    }
  } // end while
}

void ObTableGroupCommitMgr::ObTableGroupInfoTask::update_ops_task()
{
  int ret = OB_SUCCESS;
  // only use to record ops count currently and self-adaptive strategy is coming soon
  if (ObTableGroupUtils::is_group_commit_config_enable()) {
    ObTableGroupOpsCounter &ops = group_mgr_.get_ops_counter();
    if (TABLEAPI_GROUP_COMMIT_MGR->get_last_ops() >= DEFAULT_ENABLE_GROUP_COMMIT_OPS &&
        TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()) {
      TABLEAPI_GROUP_COMMIT_MGR->set_group_commit_disable(false);
      LOG_INFO("enable group commit");
    } else if (TABLEAPI_GROUP_COMMIT_MGR->get_last_ops() < DEFAULT_ENABLE_GROUP_COMMIT_OPS &&
              !TABLEAPI_GROUP_COMMIT_MGR->is_group_commit_disable()) {
      TABLEAPI_GROUP_COMMIT_MGR->set_group_commit_disable(true);
      LOG_INFO("disable group commit");
    }
    int64_t cur_read_op_ops = ops.get_read_ops();
    int64_t cur_write_op_ops = ops.get_write_ops();
    group_mgr_.set_last_write_ops(cur_write_op_ops);
    group_mgr_.set_last_read_ops(cur_read_op_ops);
    ops.reset_ops(); // reset ops counter per second
  }
}

void ObTableGroupCommitMgr::ObTableGroupInfoTask::update_group_info_task()
{
  int ret = OB_SUCCESS;
  bool is_group_commit_config_enable = ObTableGroupUtils::is_group_commit_config_enable();
  if (is_group_commit_config_enable || (!is_group_commit_config_enable && need_update_group_info_)) {
    // 1. check and refresh all group info status
    int64_t cur_ts = ObClockGenerator::getClock();
    ObTableGroupInfoMap &group_info_map = TABLEAPI_GROUP_COMMIT_MGR->get_group_info_map();
    ObTableGroupCommitMap &group_map = TABLEAPI_GROUP_COMMIT_MGR->get_group_map();
    ObTableGroupInfoMap::iterator iter = group_info_map.begin();
    // ignore ret
    for (;iter != group_info_map.end(); iter++) {
      uint64_t hash_key = iter->first;
      ObTableLsGroupInfo &group_info = iter->second;
      if (group_info.is_normal_group()) {
        ObTableLsGroup *ls_group = nullptr;
        if (OB_FAIL(group_map.get_refactored(hash_key, ls_group))) {
          LOG_WARN("fail to get ls group", K(ret), K(hash_key));
        } else if (OB_NOT_NULL(ls_group)) {
          group_info.queue_size_ = ls_group->get_queue_size();
        }
      } else if (group_info.is_fail_group()) {
        group_info.queue_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups().count();
      }
      group_info.batch_size_ = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(group_info.is_read_group());
      group_info.gmt_modified_ = cur_ts;
    } // end for

    need_update_group_info_ = is_group_commit_config_enable ? true : false;
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
                                       ObTableGroupTriggerTask::TASK_SCHEDULE_INTERVAL,
                                       true))) {
      LOG_WARN("fail to schedule group commit statis and trigger task", KR(ret));
    } else if (OB_FAIL(timer_.schedule(group_size_and_ops_task_,
                                       ObTableGroupInfoTask::TASK_SCHEDULE_INTERVAL,
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
    const ObMemAttr attr(MTL_ID(), "TbGroupComMgr");
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      LOG_WARN("fail to init allocator", K(ret));
    } else if (OB_FAIL(group_map_.create(DEFAULT_GROUP_SIZE, "HashBucApiGroup", "HasNodApiGroup", MTL_ID()))) {
      LOG_WARN("fail to init ls group map", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(expired_groups_.init())) {
      LOG_WARN("fail to init expired groups", K(ret));
    } else if (OB_FAIL(group_info_map_.create(DEFAULT_GROUP_SIZE, "HashBucGrpInfo", "HasNodGrpInfo", MTL_ID()))) {
      LOG_WARN("fail to init group info map", K(ret), K(MTL_ID()));
    } else {
      // add fail group to group info map
      ObTableGroupCommitKey key;
      ObTableLsGroupInfo fail_group_info;
      key.is_fail_group_key_ = true;
      if (OB_FAIL(key.init())) {
        LOG_WARN("fail to init key", K(ret), K(key));
      } else if (OB_FAIL(fail_group_info.init(key))) {
        LOG_WARN("fail to init fail group info", K(ret), K(key));
      } else if (OB_FAIL(group_info_map_.set_refactored(key.hash_, fail_group_info))) {
        LOG_WARN("fail to set fail group info to map", K(ret));
      } else {
        is_inited_ = true;
      }
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

int ObTableGroupCommitMgr::clean_group_map()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableGroupCommitSingleOp*, 16> ops;
  ObHashMap<uint64_t, ObTableLsGroup*>::iterator iter = group_map_.begin();
  for (; iter != group_map_.end(); iter++) {
    ObTableLsGroup *ls_group = iter->second;
    int64_t batch_size = -1;
    ops.reuse();
    if (OB_NOT_NULL(ls_group)) {
      batch_size = get_group_size(ls_group->meta_.is_get_);
      while(ls_group->has_executable_ops()) {
        if (OB_FAIL(ls_group->get_executable_batch(batch_size, ops, false))) {
          LOG_WARN("fail to get executable batch", K(ret));
        } else if (ops.count() == 0) {
          // do nothing
        } else {
          ObTableGroupCommitOps *group = group_factory_.alloc();
          if (OB_ISNULL(group)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc group", K(ret));
          } else if (OB_FAIL(group->init(ls_group->meta_, ops))) {
            LOG_WARN("fail to init group", K(ret), K(ls_group->meta_), K(ops));
          } else if (OB_FAIL(ObTableGroupExecuteService::execute(*group,
                                                                  &failed_groups_,
                                                                  &group_factory_,
                                                                  &op_factory_,
                                                                  false /*add_failed_group */))) {
            LOG_WARN("fail to execute group", K(ret));
          }
        }
      } // end while

      // free ls_group whether succes or not
      ls_group->~ObTableLsGroup();
      allocator_.free(ls_group);
      ls_group = nullptr;
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
    ObTableLsGroup *ls_group = nullptr;
    if (OB_FAIL(expired_groups_.pop_expired_group(ls_group))) {
      LOG_WARN("fail to pop expired group", K(ret));
    } else if (OB_NOT_NULL(ls_group)) {
      int64_t batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(ls_group->meta_.is_get_);
      ObSEArray <ObTableGroupCommitSingleOp*, 16> ops;
      while(ls_group->has_executable_ops()) {
        if (OB_FAIL(ls_group->get_executable_batch(batch_size, ops, false))) {
          LOG_WARN("fail to get executable queue", K(ret));
        } else if (ops.count() == 0) {
          // do nothing
          LOG_DEBUG("ops count is 0");
        } else {
          ObTableGroupCommitOps *group = group_factory_.alloc();
          if (OB_ISNULL(group)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc group", K(ret));
          } else if (OB_FAIL(group->init(ls_group->meta_, ops))) {
            LOG_WARN("fail to init group", K(ret), K(ls_group->meta_), K(ops));
          } else if (OB_FAIL(ObTableGroupExecuteService::execute(*group,
                                                                &failed_groups_,
                                                                &group_factory_,
                                                                &op_factory_))) {
            LOG_WARN("fail to execute group", K(ret));
          }
        }
      } // end while
      // free ls_group whether succes or not
      ls_group->~ObTableLsGroup();
      allocator_.free(ls_group);
      ls_group = nullptr;
    }
  }

  // 2. clean all remaining clean group
  while (!expired_groups_.get_clean_groups().empty()) {
    ObTableLsGroup *ls_group = nullptr;
    if (OB_FAIL(expired_groups_.pop_clean_group(ls_group))) {
      // overwrite ret
      LOG_WARN("fail to pop clean group", K(ret));
    } else if (OB_NOT_NULL(ls_group)) {
      ls_group->~ObTableLsGroup();
      allocator_.free(ls_group);
      ls_group = nullptr;
    }
  }
  return ret;
}

int ObTableGroupCommitMgr::clean_failed_groups()
{
  int ret = OB_SUCCESS;
  LOG_INFO("clean failed groups:", K(failed_groups_.get_failed_groups().size()));
  while (!failed_groups_.empty()) {
    ObTableGroupCommitOps *group = failed_groups_.get();
    if (OB_NOT_NULL(group)) {
      if (OB_FAIL(ObTableGroupService::process_one_by_one(*group))) {
        LOG_WARN("fail to process group one by one", K(ret));
      }
      if (OB_NOT_NULL(group)) {
        group_factory_.free(group);
      }
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
    group_info_map_.clear();
    group_factory_.free_all();
    op_factory_.free_all();
    is_inited_ = false;
    LOG_INFO("ObTableGroupCommitMgr destroy successfully");
  }
}

int64_t ObTableGroupCommitMgr::get_group_size(bool is_read) const
{
  UNUSED(is_read);
  int64_t batch_size = 1;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    batch_size = tenant_config->kv_group_commit_batch_size;
    batch_size = batch_size > 1 ? batch_size : 1;
  }
  return batch_size;
}

int ObTableGroupCommitMgr::create_and_add_ls_group(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTableLsGroup *tmp_ls_group = nullptr;
  ObMemAttr memattr(MTL_ID(), "ObTableLsGroup");
  if (OB_ISNULL(ctx.key_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key is null", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableLsGroup), memattr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObTableLsGroup", K(ret), K(sizeof(ObTableLsGroup)));
  } else {
    ObCreateLsGroupOp create_group_op(group_info_map_, *ctx.key_); // add group info atomic
    ObTableLsGroup *tmp_ls_group = new(buf) ObTableLsGroup();
    if (OB_FAIL(tmp_ls_group->init(ctx.credential_,
                                   ctx.key_->ls_id_,
                                   ctx.key_->table_id_,
                                   ctx.entity_type_,
                                   ctx.key_->op_type_))) {
      LOG_WARN("fail to init ls group", K(ret), K(ctx));
      tmp_ls_group->~ObTableLsGroup();
      allocator_.free(tmp_ls_group);
      tmp_ls_group = nullptr;
    } else if (OB_FAIL(group_map_.set_refactored(ctx.key_->hash_,
                                                 tmp_ls_group,
                                                 0/*flag*/,
                                                 0/*broadcast*/,
                                                 0/*overwrite_key*/,
                                                 &create_group_op))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to set group to hash map", K(ret), K(ctx.key_));
      } else {
        ret = OB_SUCCESS; // replace error code
      }
      // this group has been set by other thread, free it
      tmp_ls_group->~ObTableLsGroup();
      allocator_.free(tmp_ls_group);
      tmp_ls_group = nullptr;
    }
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase
