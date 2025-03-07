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
#include "ob_table_group_service.h"
#include "observer/table/ob_table_move_response.h"
#include "ob_table_group_factory.h"

using namespace oceanbase::omt;
using namespace oceanbase::common::hash;

void __attribute__((weak)) request_finish_callback();

namespace oceanbase
{
namespace table
{
int ObTableGroupService::check_legality(const ObTableGroupCtx &ctx, const ObITableGroupKey *key, const ObITableOp *op)
{
  int ret = OB_SUCCESS;
  if (!(ctx.group_type_ > ObTableGroupType::TYPE_INVALID && ctx.group_type_ < ObTableGroupType::TYPE_MAX)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "group op type for group commit");
    LOG_WARN("operation type for group commit is not supported yet", K(ret), K(ctx.group_type_));
  } else if (OB_ISNULL(key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key is NULL", K(ret));
  } else if (OB_FAIL(key->check_legality())) {
    LOG_WARN("fail to check key legality", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is NULL", K(ret));
  } else if (OB_FAIL(op->check_legality())) {
    LOG_WARN("fail to check key legality", K(ret));
  }

  return ret;
}

// it means only one operation in a batch operation
int ObTableGroupService::process_one_by_one(ObTableGroup &group)
{
  int ret = OB_SUCCESS;
  ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObIArray<ObITableOp*> &ops = group.ops_;
  ObTableGroupMeta group_meta = group.group_meta_;
  bool add_failed_group = false;
  for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
    ObITableOp *op = ops.at(i);
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op is null", K(ret));
    } else {
      ObTableGroup *exec_group = group_factory.alloc();
      if (OB_ISNULL(exec_group)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc execute group", K(ret));
      } else if (OB_FAIL(exec_group->init(group_meta, op))) {
        LOG_WARN("fail to init executable group", K(ret));
        group_factory.free(exec_group);
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(*exec_group, add_failed_group))) {
        LOG_WARN("fail to execute group", K(ret));
      }
    }
  }

  return ret;
}

// only execute one group once
int ObTableGroupService::process_failed_group()
{
  int ret = OB_SUCCESS;

  ObTableFailedGroups &groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
  if (groups.empty()) {
    // do nothing
  } else {
    ObTableGroup *group = groups.get();
    if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group is null", K(ret));
    } else if (OB_FAIL(process_one_by_one(*group))) {
      LOG_WARN("fail to process group one by one", K(ret));
    }

    if (OB_NOT_NULL(group)) {
      TABLEAPI_GROUP_COMMIT_MGR->get_group_factory().free(group);
    }
  }

  return ret;
}

int ObTableGroupService::process_other_group()
{
  int ret = OB_SUCCESS;
  ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupCommitMap &group_map = TABLEAPI_GROUP_COMMIT_MGR->get_group_map();
  ObHashMap<ObITableGroupKey *, ObITableGroupValue*>::iterator iter = group_map.begin();
  ObSEArray <ObITableOp*, 16> ops;
  bool is_executed = false;
  for (; OB_SUCC(ret) && iter != group_map.end() && !is_executed; iter++) {
    ObITableGroupValue *group = iter->second;
    int64_t batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
    ops.reuse();
    if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group is null", K(ret));
    } else if (OB_FAIL(group->get_executable_group(batch_size, ops, false))) {
      LOG_WARN("fail to get executable queue", K(ret));
    } else if (ops.count() == 0) {
      // do nothing
      LOG_DEBUG("ops count is 0");
    } else {
      ObTableGroup *exec_group = group_factory.alloc();
      // currently, we only have class ObTableGroupValue and its meta type bind to ObTableGroupMeta
      ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue *>(group)->group_meta_;
      if (OB_ISNULL(exec_group)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc execute group", K(ret));
      } else if (OB_FAIL(exec_group->init(group_meta, ops))) {
        LOG_WARN("fail to init executable group", K(ret), K(group_meta), K(ops));
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(*exec_group, true /* add_fail_group */))) {
        LOG_WARN("fail to execute group", K(ret));
      } else {
        is_executed = true;
      }
      LOG_DEBUG("[group commit debug] process other group", K(ops.count()), K(group->get_group_size()));
    }
  }

  return ret;
}

int ObTableGroupService::process_expired_group()
{
  int ret = OB_SUCCESS;
  ObTableExpiredGroups &expired_groups = TABLEAPI_GROUP_COMMIT_MGR->get_expired_groups();
  ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObITableGroupValue *exp_group = nullptr;
  if (OB_FAIL(expired_groups.pop_expired_group(exp_group))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to pop expired group", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(exp_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expired ls_group is NULL", K(ret));
  } else {
    int64_t batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
    ObSEArray <ObITableOp*, 16> ops;
    while(exp_group->has_executable_batch()) {
      if (OB_FAIL(exp_group->get_executable_group(batch_size, ops, false))) {
        LOG_WARN("fail to get executable group", K(ret));
      } else if (ops.count() == 0) {
        // do nothing
        LOG_DEBUG("ops count is 0");
      } else {
        ObTableGroup *exec_group = group_factory.alloc();
        ObTableGroupMeta &group_meta = static_cast<ObTableGroupValue *>(exp_group)->group_meta_;
        if (OB_ISNULL(exec_group)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc group", K(ret));
        } else if (OB_FAIL(exec_group->init(group_meta, ops))) {
          LOG_WARN("fail to init executable group", K(ret), K(group_meta), K(ops));
        } else if (OB_FAIL(ObTableGroupExecuteService::execute(*exec_group, true /* add_fail_group */))) {
          LOG_WARN("fail to execute group", K(ret));
        }
      }
    } // end while
  }

  // expired ls group has been cleared and move to clean group
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(exp_group) && OB_TMP_FAIL(expired_groups.add_clean_group(exp_group))) {
    // overwrite ret
    LOG_WARN("fail to add expired ls group to clean group", K(tmp_ret), KPC(exp_group));
  } else {
    LOG_DEBUG("[group commit debug] add clean group", K(ret), KP(exp_group), K(expired_groups.get_clean_group_counts()));
  }

  return ret;
}

int ObTableGroupService::process(ObTableGroupCtx &ctx, ObITableOp *op, bool is_direct_execute)
{
  int ret = OB_SUCCESS;
  ObITableGroupKey *key = ctx.key_;
  ObSEArray<ObITableOp*, 32> ops;
  // check the group commit if is started in each process
  if (!TABLEAPI_GROUP_COMMIT_MGR->is_timer_enable() &&
      TABLEAPI_GROUP_COMMIT_MGR->check_and_enable_timer()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(TABLEAPI_GROUP_COMMIT_MGR->start_timer())) {
      LOG_WARN("fail to start group commit timer", K(tmp_ret));
    }
  }

  if (OB_FAIL(check_legality(ctx, key, op))) {
    LOG_WARN("fail to check legality", K(ret));
  } else {
    bool need_execute_batch = false;
    if (is_direct_execute) {
      if (OB_FAIL(ops.push_back(op))) {
        LOG_WARN("fail to push back op", KPC(op), K(ret));
      } else {
        need_execute_batch = true;
      }
    } else {
      // get group, create if not exist
      ObITableGroupValue *group = nullptr;
      if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_or_create_group(ctx, group))) {
        LOG_WARN("fail to get group", K(ret));
      } else if (OB_FAIL(add_and_try_to_get_batch(op, group, ops))) {
        LOG_WARN("fail to add and try to get executable batch", K(ret));
      } else if (ops.count() > 0) {
        need_execute_batch = true;
      }
    }

    if (OB_SUCC(ret) && need_execute_batch) {
      // execute and response if had_do_response = false
      if (OB_FAIL(execute_batch(ctx, ops, is_direct_execute, true /* add failed group */))) {
        LOG_WARN("fail to execute batch", K(ret), K(is_direct_execute));
      }
    }
    LOG_DEBUG("[group commit debug] process group", K(ret), K(need_execute_batch), K(is_direct_execute), K(ops.count()));
  }

  return ret;
}

int ObTableGroupService::add_and_try_to_get_batch(ObITableOp *op, ObITableGroupValue *group, ObIArray<ObITableOp *> &ops)
{
  int ret = OB_SUCCESS;
  int64_t batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size();
  if (OB_ISNULL(group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls group is null", K(ret));
  } else if (OB_FAIL(group->get_executable_group(batch_size - 1, ops))) { // Get executable batch from group and execute it
    LOG_WARN("fail to get executable batch", K(ret));
  } else if (ops.count() == 0) {
    // clear thread local variables used to wait in queue
    request_finish_callback();
    if (OB_FAIL(group->add_op_to_group(op))) {
      LOG_WARN("fail to add op to group", K(ret));
    }
  } else if (OB_FAIL(ops.push_back(op))) {
    LOG_WARN("fail to push back op ", K(ret), KPC(op));
  }
  return ret;
}

int ObTableGroupService::execute_batch(ObTableGroupCtx &ctx,
                                       ObIArray<ObITableOp *> &ops,
                                       bool is_direct_execute,
                                       bool add_fail_group)
{
  int ret = OB_SUCCESS;
  ObTableGroupFactory<ObTableGroup> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableFailedGroups &failed_groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
  ObTableGroupOpFactory &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  ObITableOpProcessor *op_processor = nullptr;
  ObTableGroup *group = nullptr;
  ObTableGroupCommitCreateCbFunctor functor;
  ObArenaAllocator tmp_allocator("TbGroupExec", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(GROUP_PROC_ALLOC[ctx.group_type_](tmp_allocator, op_processor))) {
    LOG_WARN("fail to alloc op processor", K(ret));
  } else if (OB_ISNULL(group = group_factory.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc group", K(ret));
  } else if (OB_FAIL(group->init(ctx, ops))) {
    LOG_WARN("fail to init executable group", K(ret));
  } else if (OB_ISNULL(ctx.create_cb_functor_)) {
    if (is_direct_execute) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("callback functor is NULL when is directly execution", K(ret));
    } else {
      ctx.create_cb_functor_ = &functor;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_direct_execute && OB_FAIL(functor.init(group, add_fail_group,
              &failed_groups, &group_factory, &op_factory))) {
    LOG_WARN("fail to init group commit callback functor", K(ret));
  } else if (OB_FAIL(op_processor->init(ctx, &group->ops_))) {
    LOG_WARN("fail to init op processor", K(ret));
  } else if (OB_FAIL(op_processor->process())) {
    LOG_WARN("fail to process op", K(ret));
  }

  // clear thread local variables used to wait in queue
  request_finish_callback();

  int tmp_ret = ret;
  if (OB_ISNULL(ctx.trans_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error before process batch", K(ret), K(tmp_ret));
  } else if (is_direct_execute && ctx.trans_param_->had_do_response_) {
    // if is is_direct_execute. the cb functor has deep copy the result and response,
    // we need to free this op to prevent memory of op_factory expanding
    for (int64_t i = 0; i < ops.count(); i++) {
      op_factory.free(ops.at(i));
    }
    group_factory.free(group);
  } else if (!ctx.trans_param_->had_do_response_
      && OB_NOT_NULL(group)
      && group->is_inited_
      && OB_FAIL(ObTableGroupExecuteService::process_result(tmp_ret,
                                                            *group,
                                                            is_direct_execute,
                                                            add_fail_group))) {
    LOG_WARN("fail to process result", K(ret), K(tmp_ret), K(is_direct_execute));
  }
  if (OB_NOT_NULL(op_processor)) {
    op_processor->~ObITableOpProcessor();
    op_processor = nullptr;
  }
  LOG_DEBUG("[group commit debug] execute batch", K(ret), K(tmp_ret), K(is_direct_execute), K(add_fail_group), KPC(ctx.trans_param_));
  return ret;
}

int ObTableGroupService::process_trigger()
{
  int ret = OB_SUCCESS;

  if (TABLEAPI_GROUP_COMMIT_MGR->has_failed_groups()) {
    if (OB_FAIL(process_failed_group())) {
      LOG_WARN("fail to process failed group", K(ret));
    }
  } else if (OB_FAIL(process_other_group())) {
    LOG_WARN("fail to process other group", K(ret));
  } else if (TABLEAPI_GROUP_COMMIT_MGR->has_expired_groups() && OB_FAIL(process_expired_group())) {
    LOG_WARN("fail to process expired group", K(ret));
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
