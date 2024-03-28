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
  } else if (key->op_type_ != ObTableOperationType::Type::PUT
      && key->op_type_ != ObTableOperationType::Type::GET) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "operation type for group commit");
    LOG_WARN("operation type for group commit is not supported yet", K(ret), KPC(key));
  }

  return ret;
}

// only execute one group once
int ObTableGroupService::process_failed_group()
{
  int ret = OB_SUCCESS;
  ObTableGroupFactory<ObTableGroupCommitOps> &group_factory = TABLEAPI_GROUP_COMMIT_MGR->get_group_factory();
  ObTableGroupFactory<ObTableGroupCommitSingleOp> &op_factory = TABLEAPI_GROUP_COMMIT_MGR->get_op_factory();
  ObTableFailedGroups &failed_groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();
  ObTableFailedGroups &groups = TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups();

  if (groups.empty()) {
    // do nothing
  } else {
    ObTableGroupCommitOps *group = groups.get();
    if (OB_ISNULL(group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group is null", K(ret));
    } else if (OB_FAIL(ObTableGroupExecuteService::execute_one_by_one(*group,
                                                                       &failed_groups,
                                                                       &group_factory,
                                                                       &op_factory))) {
      LOG_WARN("fail to execute group one by one", K(ret));
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
  bool add_failed_group = true;
  bool had_do_response = false;
  ObTableGroupCommitMgr::ObTableGroupCommitMap &groups = TABLEAPI_GROUP_COMMIT_MGR->get_groups();
  ObTableExecuteGroupsGetter getter;

  if (OB_FAIL(groups.foreach_refactored(getter))) {
    LOG_WARN("fail to foreach groups", K(ret));
  } else {
    bool has_execute_one = false;
    for (int64_t i = 0; i < getter.can_execute_keys_.count() && OB_SUCC(ret) && !has_execute_one; i++) {
      ObTableGroupCommitOps *execute_group = nullptr;
      const uint64_t &key = getter.can_execute_keys_.at(i);
      if (OB_FAIL(groups.erase_refactored(key, &execute_group))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to erase sess from sess hash map", K(ret), K(key));
        } else {
          // maybe this group has been execute by ohter thread, get next group
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(execute_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute group is null", K(ret));
      } else if (OB_FAIL(ObTableGroupExecuteService::execute(*execute_group,
                                                             &TABLEAPI_GROUP_COMMIT_MGR->get_failed_groups(),
                                                             &TABLEAPI_GROUP_COMMIT_MGR->get_group_factory(),
                                                             &TABLEAPI_GROUP_COMMIT_MGR->get_op_factory(),
                                                             add_failed_group,
                                                             had_do_response))) {
        LOG_WARN("fail to execute group", K(ret), KPC(execute_group));
      } else {
        has_execute_one = true;
      }
    }
  }

  return ret;
}

/*
  1. add operation
  2. execute if group needs execute
    2.1 create new group
    2.2 execute old group
*/
int ObTableGroupService::process(const ObTableGroupCtx &ctx, ObTableGroupCommitSingleOp *op)
{
  int ret = OB_SUCCESS;
  bool add_failed_group = true;
  bool had_do_response = false;
  const ObTableGroupCommitKey *key = ctx.key_;

  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key is null", K(ret));
  } else if (OB_FAIL(check_legality(ctx))) {
    LOG_WARN("ctx is invalid", K(ret));
  } else if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operation is null", K(ret));
  } else {
    // feeder will do two things:
    // 1. add op to group
    // 2. will remove the old group and create a new group to replace the old one when group need execute.
    ObTableGroupFeeder feeder(*TABLEAPI_GROUP_COMMIT_MGR, op);
    if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_groups().atomic_refactored(key->hash_, feeder))) {
      if (ret != OB_HASH_NOT_EXIST) {
        LOG_WARN("fail to replace group", K(ret), K(key));
      } else {
        // not exist, create it
        if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->create_and_add_group(ctx))) { // create and add group
          LOG_WARN("fail to create and add group", K(ret), K(ctx));
        } else if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_groups().atomic_refactored(key->hash_, feeder))) { // again
          LOG_WARN("fail to replace group", K(ret), K(key));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (feeder.need_execute()) {
      if (OB_FAIL(ObTableGroupExecuteService::execute(ctx,
                                                      *feeder.get_group(),
                                                      add_failed_group,
                                                      had_do_response))) {
        LOG_WARN("fail to execute group", K(ret), K(ctx), KPC(feeder.get_group()));
      }
    } else if (TABLEAPI_GROUP_COMMIT_MGR->has_failed_groups()) {
      if (OB_FAIL(process_failed_group())) {
        LOG_WARN("fail to process failed group", K(ret));
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
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase
