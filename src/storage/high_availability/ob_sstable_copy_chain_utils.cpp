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

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_copy_chain_utils.h"
#include "ob_physical_copy_task.h"
#include "ob_sstable_copy_finish_task.h"
#include "ob_sstable_copy_start_task.h"
#include "ob_tablet_copy_finish_task.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_struct.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

int ObSSTableCopyChainScanner::validate_copy_table_key(const ObITable::TableKey &copy_table_key)
{
  int ret = OB_SUCCESS;
  if (!copy_table_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy table key info is invalid", K(ret), K(copy_table_key));
  } else if (!is_ha_copy_sstable_type(copy_table_key.table_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sstable type in copy table key array", K(ret), K(copy_table_key));
  }
  return ret;
}

int ObSSTableCopyChainScanner::check_one_key(
    const ObITable::TableKey &copy_table_key,
    ObISSTableCopyScanPolicy &scan_policy,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  need_copy = true;
  if (OB_FAIL(validate_copy_table_key(copy_table_key))) {
    LOG_WARN("invalid copy table key", K(ret), K(copy_table_key));
  } else if (OB_FAIL(scan_policy.check_need_copy_sstable(copy_table_key, need_copy))) {
    LOG_WARN("failed to check need copy sstable", K(ret), K(copy_table_key));
  }
  return ret;
}

int ObSSTableCopyChainScanner::find_next_copy_table_key(
    const common::ObIArray<ObITable::TableKey> &copy_table_key_array,
    ObISSTableCopyScanPolicy &scan_policy,
    int64_t &next_index,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  for (int64_t i = next_index; OB_SUCC(ret) && !found && i < copy_table_key_array.count(); ++i) {
    const ObITable::TableKey &copy_table_key = copy_table_key_array.at(i);
    bool need_copy = true;
    if (OB_FAIL(check_one_key(copy_table_key, scan_policy, need_copy))) {
      LOG_WARN("failed to check copy table key", K(ret), K(copy_table_key));
    } else if (!need_copy) {
      LOG_DEBUG("no need copy the sstable, skip it", K(copy_table_key));
    } else {
      found = true;
      next_index = i;
    }
  }
  return ret;
}

/******************ObSSTableCopyUnit*********************/
ObSSTableCopyUnit::ObSSTableCopyUnit(const uint64_t tenant_id)
  : consumed_count_(0),
    batch_keys_()
{
  batch_keys_.set_attr(ObMemAttr(tenant_id, "CopyUnitKeys"));
}

void ObSSTableCopyUnit::reset()
{
  consumed_count_ = 0;
  batch_keys_.reset();
}

/******************ObITabletCopyChainDriverOps*********************/
int ObITabletCopyChainDriverOps::plan_copy_unit(
    const int64_t start_index,
    ObSSTableCopyUnit &unit)
{
  UNUSED(start_index);
  // one single-sstable copy chain per driver round
  unit.reset();
  unit.consumed_count_ = 1;
  return OB_SUCCESS;
}

int ObITabletCopyChainDriverOps::generate_batch_task(
    share::ObIDag *dag,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    const common::ObIArray<ObITable::TableKey> &batch_keys,
    share::ObITask *parent_task,
    share::ObITask *child_task)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(dag);
  UNUSED(tablet_copy_finish_task);
  UNUSED(parent_task);
  UNUSED(child_task);
  LOG_WARN("this copy chain driver does not support batch sstable copy task", K(ret),
      "batch_key_count", batch_keys.count());
  return ret;
}

// Inserts one middle task of type T between parent_task and finish_task:
// parent -> T -> finish. COPY_MACRO_BLOCKS and START_THEN_FINISH only differ in
// the type of this middle task (ObPhysicalCopyTask actually transfers the macro
// block data, ObSSTableCopyStartTask only fetches the macro block id array of a
// shared sstable), so the wiring is shared here.
template <typename T>
static int insert_middle_task(
    share::ObIDag *dag,
    share::ObITask *parent_task,
    ObSSTableCopyFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  T *middle_task = nullptr;
  if (OB_FAIL(dag->alloc_task(middle_task))) {
    LOG_WARN("failed to alloc middle task", K(ret));
  } else if (OB_FAIL(middle_task->init(finish_task->get_copy_ctx(), finish_task))) {
    LOG_WARN("failed to init middle task", K(ret));
  } else if (OB_FAIL(parent_task->add_child(*middle_task))) {
    LOG_WARN("failed to add middle task as child of parent", K(ret));
  } else if (OB_FAIL(middle_task->add_child(*finish_task))) {
    LOG_WARN("failed to add finish task as child of middle task", K(ret));
  } else if (OB_FAIL(dag->add_task(*middle_task))) {
    LOG_WARN("failed to add middle task to dag", K(ret));
  }
  return ret;
}

int ObSSTableCopyChainBuilder::build_sstable_copy_chain(
    share::ObIDag *dag,
    share::ObITask *parent_task,
    share::ObITask *child_task,
    ObSSTableCopyFinishTask *finish_task,
    const ObSSTableCopyTopology topology)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(dag) || OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build sstable copy chain get invalid argument", K(ret),
        KP(dag), KP(parent_task), KP(child_task), K(topology));
  } else if (BYPASS_TO_NEXT == topology) {
    // no chain to build: the parent bypasses the copy chain and drives the child directly
    if (OB_FAIL(parent_task->add_child(*child_task))) {
      LOG_WARN("failed to add child task", K(ret));
    }
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("finish task should not be NULL", K(ret), K(topology));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  } else {
    switch (topology) {
      case COPY_MACRO_BLOCKS: // parent -> physical copy -> finish -> child
        ret = insert_middle_task<ObPhysicalCopyTask>(dag, parent_task, finish_task);
        break;
      case START_THEN_FINISH: // parent -> copy start -> finish -> child
        ret = insert_middle_task<ObSSTableCopyStartTask>(dag, parent_task, finish_task);
        break;
      case DIRECT_TO_FINISH: // parent -> finish -> child
        ret = parent_task->add_child(*finish_task);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to build sstable copy chain", K(ret), K(topology));
    } else if (OB_FAIL(dag->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    }
  }
  return ret;
}

/******************ObTabletCopyChainDriverTaskBase*********************/
ObTabletCopyChainDriverTaskBase::ObTabletCopyChainDriverTaskBase()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ops_(nullptr),
    tablet_copy_finish_task_(nullptr),
    next_index_(0)
{
}

ObTabletCopyChainDriverTaskBase::~ObTabletCopyChainDriverTaskBase()
{
}

int ObTabletCopyChainDriverTaskBase::inner_init(
    ObITabletCopyChainDriverOps *ops,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    const int64_t next_index)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet copy chain driver task init twice", K(ret));
  } else if (OB_ISNULL(ops)
      || OB_ISNULL(tablet_copy_finish_task)
      || next_index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet copy chain driver task init get invalid argument", K(ret),
        KP(ops), KP(tablet_copy_finish_task), K(next_index));
  } else {
    ops_ = ops;
    tablet_copy_finish_task_ = tablet_copy_finish_task;
    next_index_ = next_index;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletCopyChainDriverTaskBase::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_DEBUG("start do tablet copy chain driver task", KPC(this));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy chain driver task do not init", K(ret));
  } else if (ops_->is_ctx_failed()) {
    ret = OB_CANCELED;
    LOG_WARN("ctx already failed", K(ret));
  } else if (OB_FAIL(generate_next_sstable_chain_())) {
    LOG_WARN("failed to generate next sstable chain", K(ret), K_(next_index));
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObTabletCopyChainDriverTaskBase::generate_next_sstable_chain_()
{
  int ret = OB_SUCCESS;
  bool found = false;
  int64_t next_index = next_index_;
  ObTabletCopyChainDriverTaskBase *next_driver_task = nullptr;
  ObSSTableCopyUnit unit(get_ha_mem_tenant_id());
  const common::ObIArray<ObITable::TableKey> &copy_table_key_array = ops_->get_copy_table_key_array();

  if (OB_FAIL(ObSSTableCopyChainScanner::find_next_copy_table_key(
      copy_table_key_array, *ops_ /* scan_policy */, next_index, found))) {
    LOG_WARN("failed to find next copy table key", K(ret), K_(next_index));
  } else if (!found) {
    // no more sstable to copy: the sink (tablet copy finish task) is only hung under this
    // driver, finishing this driver makes its indegree drop to 0 and triggers the final work
    LOG_INFO("no more sstable to copy, driver chain finished", KPC(this));
  } else if (OB_FAIL(ops_->plan_copy_unit(next_index, unit))) {
    LOG_WARN("failed to plan copy unit", K(ret), K(next_index));
  } else if (unit.consumed_count_ <= 0
      || unit.consumed_count_ < unit.batch_keys_.count()
      || (!unit.is_batch() && 1 != unit.consumed_count_)
      || next_index + unit.consumed_count_ > copy_table_key_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("planned copy unit is unexpected", K(ret), K(unit), K(next_index),
        "key_count", copy_table_key_array.count());
    // the next driver starts right after the sstables this unit consumes
  } else if (OB_FAIL(alloc_and_init_next_driver_(next_index + unit.consumed_count_, next_driver_task))) {
    LOG_WARN("failed to alloc and init next driver task", K(ret), K(next_index), K(unit));
  } else if (OB_FAIL(generate_copy_unit_tasks_(unit, next_index, next_driver_task /*child_task*/))) {
    LOG_WARN("failed to generate copy unit tasks", K(ret), K(next_index), K(unit));
    // hang the sink under the next driver, so it always has exactly one active parent;
    // the old edge (this driver -> sink) is removed automatically when this driver finishes
  } else if (OB_FAIL(next_driver_task->add_child(*tablet_copy_finish_task_))) {
    LOG_WARN("failed to add tablet copy finish task as child of next driver", K(ret));
  } else if (OB_FAIL(dag_->add_task(*next_driver_task))) {
    LOG_WARN("failed to add next driver task to dag", K(ret));
  } else {
    FLOG_INFO("succeed to generate next sstable copy unit", K(next_index), K(unit),
        "copy_table_key", copy_table_key_array.at(next_index));
  }
  return ret;
}

int ObTabletCopyChainDriverTaskBase::generate_copy_unit_tasks_(
    const ObSSTableCopyUnit &unit,
    const int64_t start_index,
    share::ObITask *child_task)
{
  int ret = OB_SUCCESS;
  if (unit.is_batch()) {
    if (OB_FAIL(ops_->generate_batch_task(dag_, tablet_copy_finish_task_, unit.batch_keys_,
        this /*parent_task*/, child_task))) {
      LOG_WARN("failed to generate batch sstable copy task", K(ret), K(unit), K(start_index));
    }
  } else {
    const ObITable::TableKey &copy_table_key = ops_->get_copy_table_key_array().at(start_index);
    if (OB_FAIL(ops_->generate_physical_task(dag_, tablet_copy_finish_task_, copy_table_key,
        this /*parent_task*/, child_task))) {
      LOG_WARN("failed to generate physical task", K(ret), K(copy_table_key), K(start_index));
    }
  }
  return ret;
}

}
}
