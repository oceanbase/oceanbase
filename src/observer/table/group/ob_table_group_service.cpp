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

using namespace oceanbase::omt;
using namespace oceanbase::common::hash;

namespace oceanbase
{
namespace table
{

int ObTableGroupService::check_legality(const ObTableGroupCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObTableGroupCommitKey *key = ctx.key_;

  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key is null", K(ret));
  } else if (!ObTableOperationType::is_group_support_type(key->op_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation type for group commit");
    LOG_WARN("operation type for group commit is not supported yet", K(ret), KPC(key));
  }

  return ret;
}

// it meads only one operation in a batch operation
int ObTableGroupService::process_one_by_one(ObTableGroupCommitOps &group)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableGroupCommitSingleOp*> &ops = group.ops_;
  bool add_failed_group = false;
  ObTableGroupFactory<ObTableGroupCommitOps> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  ObTableFailedGroups &failed_group = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();

  for (int64_t i = 0; i < ops.count() && OB_SUCC(ret); i++) {
    ObTableGroupCommitSingleOp *op = ops.at(i);
    if (OB_ISNULL(op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op is null", K(ret));
    } else {
      ObTableGroupCommitOps *one_op_group = group_factory.alloc();
      int64_t timeout = op->timeout_ts_ - ObClockGenerator::getClock();
      if (OB_ISNULL(one_op_group)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc group", K(ret));
      } else if (OB_FAIL(one_op_group->init(group.meta_, op, timeout, op->timeout_ts_))) {
        LOG_WARN("fail to init group", K(ret), K(group.meta_), K(ops));
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(*one_op_group,
                                                             &failed_group,
                                                             &group_factory,
                                                             &op_factory,
                                                             add_failed_group))) {
        LOG_WARN("fail to execute tablet groups", K(ret));
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
    ObTableGroupCommitOps *group = groups.get();
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
  ObTableGroupFactory<ObTableGroupCommitOps> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  ObTableFailedGroups &failed_group = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
  ObTableGroupCommitMap &group_map = TABLEAPI_GROUP_COMMIT_MGR->get_group_map();
  ObHashMap<uint64_t, ObTableLsGroup*>::iterator iter = group_map.begin();
  ObSEArray <ObTableGroupCommitSingleOp*, 16> ops;
  bool is_executed = false;
  for (; OB_SUCC(ret) && iter != group_map.end() && !is_executed; iter++) {
    ObTableLsGroup *ls_group = iter->second;
    int64_t batch_size = -1;
    ops.reuse();
    if (OB_ISNULL(ls_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group is null", K(ret));
    } else if (FALSE_IT(batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(ls_group->meta_.is_get_))) {
    } else if (OB_FAIL(ls_group->get_executable_batch(batch_size, ops, false))) {
      LOG_WARN("fail to get executable queue", K(ret));
    } else if (ops.count() == 0) {
      // do nothing
      LOG_DEBUG("ops count is 0");
    } else {
      ObTableGroupCommitOps *group = group_factory.alloc();
      if (OB_ISNULL(group)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc group", K(ret));
      } else if (OB_FAIL(group->init(ls_group->meta_, ops))) {
        LOG_WARN("fail to init group", K(ret), K(ls_group->meta_), K(ops));
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(*group,
                                                             &failed_group,
                                                             &group_factory,
                                                             &op_factory))) {
        LOG_WARN("fail to execute group", K(ret));
      } else {
        is_executed = true;
      }
      LOG_DEBUG("[group commit] process other group", K(ops.count()), K(ls_group->meta_), K(ls_group->get_queue_size()));
    }
  }

  return ret;
}

int ObTableGroupService::process_expired_group()
{
  int ret = OB_SUCCESS;
  ObTableExpiredGroups &expired_group = TABLEAPI_GROUP_COMMIT_MGR->get_expired_groups();
  ObTableFailedGroups &failed_group = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
  ObTableGroupFactory<ObTableGroupCommitOps> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  ObTableLsGroup *ls_group = nullptr;
  if (OB_FAIL(expired_group.pop_expired_group(ls_group))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("fail to pop expired group", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(ls_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expired ls_group is NULL", K(ret));
  } else {
    int64_t batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(ls_group->meta_.is_get_);
    ObSEArray <ObTableGroupCommitSingleOp*, 16> ops;
    while(ls_group->has_executable_ops()) {
      if (OB_FAIL(ls_group->get_executable_batch(batch_size, ops, false))) {
        LOG_WARN("fail to get executable queue", K(ret));
      } else if (ops.count() == 0) {
        // do nothing
        LOG_DEBUG("ops count is 0");
      } else {
        ObTableGroupCommitOps *group = group_factory.alloc();
        if (OB_ISNULL(group)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc group", K(ret));
        } else if (OB_FAIL(group->init(ls_group->meta_, ops))) {
          LOG_WARN("fail to init group", K(ret), K(ls_group->meta_), K(ops));
        } else if (OB_FAIL(ObTableGroupExecuteService::execute(*group,
                                                              &failed_group,
                                                              &group_factory,
                                                              &op_factory))) {
          LOG_WARN("fail to execute group", K(ret));
        }
      }
    } // end while
  }

  // expired ls group has been cleared and move to clean group
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_group) && OB_TMP_FAIL(expired_group.add_clean_group(ls_group))) {
    // overwrite ret
    LOG_WARN("fail to add expired ls group to clean group", K(tmp_ret), KPC(ls_group));
  } else {
    LOG_DEBUG("[group commit debug] add clean group", K(ret), KP(ls_group), K(expired_group.get_clean_group_counts()));
  }

  return ret;
}

int ObTableGroupService::process(ObTableGroupCtx &ctx, ObTableGroupCommitSingleOp *op)
{
  int ret = OB_SUCCESS;
  bool add_failed_group = true;
  bool had_do_response = false;
  ObTableGroupCommitKey *key = ctx.key_;

  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key is null", K(ret));
  } else if (OB_FAIL(check_legality(ctx))) {
    LOG_WARN("ctx is invalid", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is null", K(ret));
  } else if (!op->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("op is invalid", K(ret), KPC(op));
  } else {
    // Get ls group, create if not exist
    ObTableLsGroup *ls_group = nullptr;
    if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_group_map().get_refactored(key->hash_, ls_group))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("fail to replace group", K(ret), KPC(key));
      } else {
        // not exist, create it
        if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->create_and_add_ls_group(ctx))) { // create and add ls group
          LOG_WARN("fail to create and add ls group", K(ret), K(ctx));
        } else if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_group_map().get_refactored(key->hash_, ls_group))) { // again
          LOG_WARN("fail to get ls group", K(ret), KPC(key));
        }
      }
    }

    ObSEArray<ObTableGroupCommitSingleOp*, 32> ops;
    int64_t batch_size = -1;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls group is null", K(ret));
    } else if (FALSE_IT(ls_group->last_active_ts_ = ObTimeUtility::fast_current_time())) {
    } else if (FALSE_IT(batch_size = TABLEAPI_GROUP_COMMIT_MGR->get_group_size(ls_group->meta_.is_get_))) {
    } else if (OB_FAIL(ls_group->get_executable_batch(batch_size - 1, ops))) { // Get executable batch from ls group and execute it
      LOG_WARN("fail to get executable batch", K(ret));
    } else if (ops.count() == 0) {
      if (OB_FAIL(ls_group->add_op_to_queue(op, ctx.add_group_success_))) {
        LOG_WARN("fail to add op to queue", K(ret));
      }
    } else if (OB_FAIL(ops.push_back(op))) {
      LOG_WARN("fail to push back op to the executable batch", K(ret), KPC(op));
    } else {
      ctx.add_group_success_ = true;
      ObTableGroupFactory<ObTableGroupCommitOps> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
      ObTableGroupCommitOps *group = group_factory.alloc();
      if (OB_ISNULL(group)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc group", K(ret));
      } else if (OB_FAIL(group->init(ls_group->meta_, ops))) {
        LOG_WARN("fail to init group", K(ret), K(ls_group->meta_), K(ops));
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(ctx, *group))) {
        LOG_WARN("fail to execute", K(ret));
      }
    }
  }

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
