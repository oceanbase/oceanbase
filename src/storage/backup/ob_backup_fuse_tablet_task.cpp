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
#include "storage/backup/ob_backup_fuse_tablet_dag.h"
#include "storage/backup/ob_backup_fuse_tablet_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_tablet_reorganize_helper.h"

using namespace oceanbase::share;

namespace oceanbase
{
namespace backup
{

static int deal_with_fo(ObBackupTabletGroupFuseCtx *ctx, const int64_t result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx->set_result(result, false/*need_retry*/))) {
    LOG_WARN("failed to set result", K(ret));
  }
  return ret;
}

ObInitialBackupTabletGroupFuseTask::ObInitialBackupTabletGroupFuseTask()
  : ObITask(TASK_TYPE_BACKUP_INITIAL_FUSE),
    is_inited_(false),
    dag_net_(NULL),
    ctx_(NULL)
{
}

ObInitialBackupTabletGroupFuseTask::~ObInitialBackupTabletGroupFuseTask()
{
}

int ObInitialBackupTabletGroupFuseTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = NULL;
  ObBackupTabletFuseDagNet *fuse_dag_net = NULL;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial tablet group fuse task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be null", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (OB_ISNULL(fuse_dag_net = static_cast<ObBackupTabletFuseDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be null", K(ret), KP(dag_net));
  } else {
    dag_net_ = dag_net;
    ctx_ = fuse_dag_net->get_fuse_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init initial backup tablet group fuse task");
  }
  return ret;
}

int ObInitialBackupTabletGroupFuseTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial backup tablet group fuse task do not init", K(ret));
  } else if (OB_FAIL(build_tablet_group_ctx_())) {
    LOG_WARN("failed to build tablet group ctx", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_tablet_fuse_dags_())) {
    LOG_WARN("failed to generate tablet fuse dags", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObInitialBackupTabletGroupFuseTask::generate_tablet_fuse_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialBackupTabletGroupFuseDag *initial_dag = NULL;
  ObStartBackupTabletGroupFuseDag   *start_dag   = NULL;
  ObFinishBackupTabletGroupFuseDag  *finish_dag  = NULL;
  ObTenantDagScheduler *scheduler = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inital tablet group fuse init task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (ObDagType::DAG_TYPE_INITIAL_BACKUP_FUSE != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is not initial backup fuse", K(ret), "dag_type", this->get_dag()->get_type());
  } else if (OB_ISNULL(initial_dag = static_cast<ObInitialBackupTabletGroupFuseDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial backup tablet group fuse dag should not be NULL", K(ret), KP(initial_dag));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(start_dag))) {
      LOG_WARN("failed to alloc start fuse dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag(finish_dag))) {
      LOG_WARN("failed to alloc finish fuse dag", K(ret));
    } else if (OB_FAIL(start_dag->init(dag_net_, finish_dag))) {
      LOG_WARN("failed to init start fuse dag", K(ret));
    } else if (OB_FAIL(finish_dag->init(dag_net_))) {
      LOG_WARN("failed to init finish fuse dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*start_dag))) {
      LOG_WARN("failed to add start fuse dag as child", K(ret), KPC(start_dag));
    } else if (OB_FAIL(start_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(start_dag->add_child(*finish_dag))) {
      LOG_WARN("failed to add finish fuse dag as child", K(ret));
    } else if (OB_FAIL(finish_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(finish_dag))) {
      LOG_WARN("failed to add finish fuse dag", K(ret), K(*finish_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(start_dag))) {
      LOG_WARN("failed to add dag", K(ret), K(*start_dag));
      if (OB_TMP_FAIL(scheduler->cancel_dag(finish_dag))) {
        LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(start_dag));
      } else {
        finish_dag = NULL;
      }
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule start fuse dag", K(*start_dag));
    }

    if (OB_FAIL(ret)) {

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_dag)) {
        scheduler->free_dag(*finish_dag);
        finish_dag = NULL;
      }

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_dag)) {
        scheduler->free_dag(*start_dag);
        start_dag = NULL;
      }

      const bool need_retry = true;
      if (OB_TMP_FAIL(ctx_->set_result(ret, need_retry, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set fuse result", K(ret), K(tmp_ret), K(*ctx_));
      }
    }
  }
  return ret;
}

int ObInitialBackupTabletGroupFuseTask::build_tablet_group_ctx_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group fuse task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fuse ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->init())) {
    LOG_WARN("failed to build tablet group ctx", K(ret));
  }
  return ret;
}

int ObInitialBackupTabletGroupFuseTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_fuse", "initial_backup_tablet_group_fuse_task",
                     "tenant_id", ctx_->param_.tenant_id_,
                     "backup_set_id", ctx_->param_.backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->param_.ls_id_.id(),
                     "turn_id", ctx_->param_.turn_id_,
                     "retry_id", ctx_->param_.retry_id_,
                     "result", result);
  }
  return ret;
}

/******************ObStartBackupTabletGroupFuseTask*********************/
ObStartBackupTabletGroupFuseTask::ObStartBackupTabletGroupFuseTask()
  : ObITask(TASK_TYPE_BACKUP_START_FUSE),
    is_inited_(false),
    finish_dag_(NULL),
    group_ctx_(NULL)
{
}

ObStartBackupTabletGroupFuseTask::~ObStartBackupTabletGroupFuseTask()
{
}

int ObStartBackupTabletGroupFuseTask::init(share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = NULL;
  ObBackupTabletFuseDagNet *fuse_dag_net = NULL;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start tablet group fuse task init twice", K(ret));
  } else if (OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start tablet group fuse task init get invalid argument", K(ret), KP(finish_dag));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (OB_ISNULL(fuse_dag_net = static_cast<ObBackupTabletFuseDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else {
    finish_dag_ = finish_dag;
    group_ctx_ = fuse_dag_net->get_fuse_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init start tablet group fuse task");
  }
  return ret;
}

int ObStartBackupTabletGroupFuseTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_fuse = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group retore task do not init", K(ret));
  } else if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (group_ctx_->is_failed()) {
    // do nothing
  } else if (OB_FAIL(check_need_fuse_tablet_(need_fuse))) {
    LOG_WARN("failed to check need fuse tablet", K(ret));
  } else if (!need_fuse) {
    LOG_INFO("no need fuse tablet meta", KPC_(group_ctx));
  } else if (OB_FAIL(group_ctx_->do_fuse())) {
    LOG_WARN("failed to do fuse", K(ret));
  } else if (OB_FAIL(generate_tablet_fuse_dag_())) {
    LOG_WARN("failed to generate tablet fuse dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(group_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  return ret;
}

int ObStartBackupTabletGroupFuseTask::check_need_fuse_tablet_(bool &need_fuse)
{
  int ret = OB_SUCCESS;
  need_fuse = false;
  bool has_sys_data = false;
  if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret), KP_(group_ctx));
  } else if (OB_FAIL(ObLSBackupOperator::check_ls_has_sys_data(*group_ctx_->report_ctx_.sql_proxy_,
                                                          group_ctx_->param_.tenant_id_,
                                                          group_ctx_->param_.job_desc_.task_id_,
                                                          group_ctx_->param_.ls_id_,
                                                          has_sys_data))) {
    LOG_WARN("failed to check has sys ls", K(ret), KPC_(group_ctx));
  } else {
    need_fuse = has_sys_data;
  }
  return ret;
}

int ObStartBackupTabletGroupFuseTask::generate_tablet_fuse_dag_()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = NULL;
  ObIDagNet *dag_net = NULL;
  ObIDag *parent = this->get_dag();
  ObStartBackupTabletGroupFuseDag *group_fuse_dag = NULL;
  ObBackupTabletFuseDag *tablet_fuse_dag = NULL;
  ObBackupTabletFuseItem fuse_item;
  ObInitBackupTabletFuseParam param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group fuse task do not init", K(ret));
  } else if (ObDagType::DAG_TYPE_START_BACKUP_FUSE != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is not start backup fuse", K(ret), "dag_type", this->get_dag()->get_type());
  } else if (OB_ISNULL(group_fuse_dag = static_cast<ObStartBackupTabletGroupFuseDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start tablet group fuse dag should not be NULL", K(ret), KP(group_fuse_dag));
  } else if (OB_ISNULL(dag_net = group_fuse_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group fuse dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(group_ctx_->get_next_tablet_item(fuse_item))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("no tablets need fuse", KPC_(group_ctx));
    } else {
      LOG_WARN("failed to get next tablet id", K(ret), KPC(group_ctx_));
    }
  } else {
    if (OB_FAIL(scheduler->alloc_dag(tablet_fuse_dag))) {
      LOG_WARN("failed to alloc tablet fuse dag ", K(ret));
    } else if (OB_FAIL(tablet_fuse_dag->init(group_ctx_->param_, fuse_item, *group_ctx_))) {
      LOG_WARN("failed to init tablet fuse dag", K(ret), "param", group_ctx_->param_, K(fuse_item));
    } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_fuse_dag))) {
      LOG_WARN("failed to add dag into dag net", K(ret), KPC_(group_ctx));
    } else if (OB_FAIL(parent->add_child_without_inheritance(*tablet_fuse_dag))) {
      LOG_WARN("failed to add child dag", K(ret), KPC_(group_ctx));
    } else if (OB_FAIL(tablet_fuse_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC_(group_ctx));
    } else if (OB_FAIL(tablet_fuse_dag->add_child_without_inheritance(*finish_dag_))) {
      LOG_WARN("failed to add finish dag as child", K(ret), KPC_(group_ctx));
    } else if (OB_FAIL(scheduler->add_dag(tablet_fuse_dag))) {
      LOG_WARN("failed to add tablet fuse dag", K(ret), K(*tablet_fuse_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule tablet fuse dag", K(*tablet_fuse_dag));
      tablet_fuse_dag = NULL;
    }
  }

  if (OB_NOT_NULL(tablet_fuse_dag)) {
    scheduler->free_dag(*tablet_fuse_dag);
    tablet_fuse_dag = NULL;
  }
  return ret;
}

int ObStartBackupTabletGroupFuseTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(group_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_fuse", "start_backup_tablet_group_fuse_task",
                     "tenant_id", group_ctx_->param_.tenant_id_,
                     "backup_set_id", group_ctx_->param_.backup_set_desc_.backup_set_id_,
                     "ls_id", group_ctx_->param_.ls_id_.id(),
                     "turn_id", group_ctx_->param_.turn_id_,
                     "retry_id", group_ctx_->param_.retry_id_,
                     "result", result);
  }
  return ret;
}

/******************ObFinishBackupTabletGroupFuseTask*********************/
ObFinishBackupTabletGroupFuseTask::ObFinishBackupTabletGroupFuseTask()
  : ObITask(TASK_TYPE_BACKUP_FINISH_FUSE),
    is_inited_(false),
    dag_net_(NULL),
    group_ctx_(NULL)
{
}

ObFinishBackupTabletGroupFuseTask::~ObFinishBackupTabletGroupFuseTask()
{
}

int ObFinishBackupTabletGroupFuseTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = NULL;
  ObBackupTabletFuseDagNet *fuse_dag_net = NULL;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish tablet group fuse task init twice", K(ret));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (OB_ISNULL(fuse_dag_net = static_cast<ObBackupTabletFuseDagNet *>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else {
    dag_net_ = dag_net;
    group_ctx_ = fuse_dag_net->get_fuse_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init finish tablet group fuse task");
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do finish tablet group fuse task");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group fuse task do not init", K(ret));
  } else if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (group_ctx_->is_failed()) {
    if (OB_TMP_FAIL(group_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), K(ret));
    }
  } else if (OB_FAIL(close_extern_writer_())) {
    LOG_WARN("failed to close extern writer", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(group_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }
  if (OB_TMP_FAIL(report_task_result_())) {
    LOG_WARN("failed to report task result", K(tmp_ret), K(ret));
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseTask::close_extern_writer_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(group_ctx_->close_extern_writer())) {
    LOG_WARN("failed to close extern writer", K(ret));
  } else {
    LOG_INFO("close extern tablet meta writer", K(ret));
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseTask::generate_init_dag_()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = NULL;
  ObInitialBackupTabletGroupFuseDag *initial_dag = NULL;
  ObFinishBackupTabletGroupFuseDag  *finish_dag  = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group task do not init", K(ret));
  } else if (ObDagType::DAG_TYPE_FINISH_BACKUP_FUSE != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is not finish backup fuse", K(ret), "dag_type", this->get_dag()->get_type());
  } else if (OB_ISNULL(finish_dag = static_cast<ObFinishBackupTabletGroupFuseDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish tablet group fuse dag should not be NULL", K(ret), KP(finish_dag));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(initial_dag))) {
      LOG_WARN("failed to alloc initial fuse dag ", K(ret));
    } else if (OB_FAIL(initial_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial fuse dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_dag))) {
      LOG_WARN("failed to add initial fuse dag as chiild", K(ret), KPC(initial_dag));
    } else if (OB_FAIL(initial_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
      LOG_WARN("failed to add initial fuse dag", K(ret), K(*initial_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create tablet group initial fuse dag", K(ret));
      initial_dag = NULL;
    }

    if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_dag);
      initial_dag = NULL;
    }
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_fuse", "finish_backup_tablet_group_fuse_task",
                     "tenant_id", group_ctx_->param_.tenant_id_,
                     "backup_set_id", group_ctx_->param_.backup_set_desc_.backup_set_id_,
                     "ls_id", group_ctx_->param_.ls_id_.id(),
                     "turn_id", group_ctx_->param_.turn_id_,
                     "retry_id", group_ctx_->param_.retry_id_);
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseTask::report_task_result_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(group_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(ObBackupUtils::report_task_result(group_ctx_->param_.job_desc_.job_id_,
                                                       group_ctx_->param_.job_desc_.task_id_,
                                                       group_ctx_->param_.tenant_id_,
                                                       group_ctx_->param_.ls_id_,
                                                       group_ctx_->param_.turn_id_,
                                                       group_ctx_->param_.retry_id_,
                                                       group_ctx_->param_.job_desc_.trace_id_,
                                                       this->get_dag()->get_dag_id(),
                                                       result,
                                                       group_ctx_->report_ctx_))) {
    LOG_WARN("failed to report task result", K(ret));
  }
  return ret;
}

/******************ObBackupTabletFuseTask*********************/
ObBackupTabletFuseTask::ObBackupTabletFuseTask()
  : ObITask(TASK_TYPE_BACKUP_TABLET_FUSE),
    is_inited_(false),
    sql_proxy_(NULL),
    fuse_ctx_(NULL),
    group_ctx_(NULL),
    fuse_type_(ObBackupFuseTabletType::MAX)
{
}

ObBackupTabletFuseTask::~ObBackupTabletFuseTask()
{
}

int ObBackupTabletFuseTask::init(
    ObBackupTabletFuseCtx &fuse_ctx, ObBackupTabletGroupFuseCtx &group_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet fuse task init twice", K(ret));
  } else if (!fuse_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(fuse_ctx));
  } else {
    fuse_ctx_ = &fuse_ctx;
    group_ctx_ = &group_ctx;
    sql_proxy_ = group_ctx.report_ctx_.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletFuseTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("do backup tablet fuse task", KPC_(fuse_ctx));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet fuse task do not init", K(ret));
  } else if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (group_ctx_->is_failed()) {
    // do nothing
  } else if (OB_FAIL(inner_process_(fuse_ctx_->fuse_item_))) {
    LOG_WARN("failed to inner process", K(ret), KPC_(fuse_ctx));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(group_ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }

  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupTabletFuseTask::inner_process_(const ObBackupTabletFuseItem &fuse_item)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = fuse_item.tablet_id_;
  ObMigrationTabletParam output_param;
  if (OB_FAIL(fuse_tablet_item_(fuse_item, output_param))) {
    LOG_WARN("failed to fuse tablet item", K(ret), K(fuse_item));
  } else if (OB_FAIL(write_new_tablet_info_(output_param))) {
    LOG_WARN("failed to write new tablet info", K(ret), K(output_param));
  }
  return ret;
}

int ObBackupTabletFuseTask::check_tablet_deleted_(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    bool &tablet_deleted)
{
  int ret = OB_SUCCESS;
  int64_t tmp_ls_id = 0;
  int64_t tablet_count = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else if (OB_FAIL(ObLSBackupOperator::get_tablet_to_ls_info(
      *sql_proxy_, tenant_id, tablet_id, tablet_count, tmp_ls_id))) {
    LOG_WARN("failed to check tablet deleted", K(ret), K(tenant_id), K(tablet_id));
  } else {
    tablet_deleted = 0 == tablet_count;
  }
  return ret;
}

int ObBackupTabletFuseTask::check_tablet_reorganized_(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    bool &tablet_reoragnized)
{
  int ret = OB_SUCCESS;
  share::ObLSID tmp_ls_id;
  int64_t tablet_count = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else if (OB_FAIL(ObBackupTabletReorganizeHelper::check_tablet_has_reorganized(
      *sql_proxy_, tenant_id, tablet_id, tmp_ls_id, tablet_reoragnized))) {
    LOG_WARN("failed to check tablet has reoragnized", K(ret), K(tenant_id), K(tablet_id));
  } else {
    LOG_INFO("check tablet has reorganized", K(tenant_id), K(tablet_id), K(tmp_ls_id), K(tablet_reoragnized));
  }
  return ret;
}

int ObBackupTabletFuseTask::fetch_tablet_meta_in_user_data_(
    const ObBackupMetaIndex &meta_index,
    ObMigrationTabletParam &tablet_param)
{
  int ret = OB_SUCCESS;
  tablet_param.reset();
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  share::ObBackupPath backup_path;
  ObBackupTabletMeta backup_tablet_meta;
  common::ObStorageIdMod mod;

  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;

  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(meta_index));
  } else if (OB_ISNULL(fuse_ctx_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("fuse ctx should not be null", K(ret));
  } else if (FALSE_IT(mod.storage_id_ = fuse_ctx_->param_.dest_id_)) {
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(fuse_ctx_->param_.backup_dest_,
                                                                          fuse_ctx_->param_.backup_set_desc_,
                                                                          meta_index.ls_id_,
                                                                          backup_data_type,
                                                                          meta_index.turn_id_,
                                                                          meta_index.retry_id_,
                                                                          meta_index.file_id_,
                                                                          backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(meta_index));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::read_tablet_meta(backup_path.get_obstr(),
                                                             fuse_ctx_->param_.backup_dest_.get_storage_info(),
                                                             mod,
                                                             meta_index,
                                                             backup_tablet_meta))) {
    LOG_WARN("failed to read tablet meta", K(ret), K(backup_path), K(meta_index));
  } else if (OB_FAIL(tablet_param.assign(backup_tablet_meta.tablet_meta_))) {
    LOG_WARN("failed to assign tablet param", K(ret));
  }
  return ret;
}

int ObBackupTabletFuseTask::fuse_tablet_item_(
    const ObBackupTabletFuseItem &fuse_item,
    ObMigrationTabletParam &output_param)
{
  int ret = OB_SUCCESS;
  if (fuse_item.has_tablet_meta_index_) {
    const ObBackupMetaIndex &meta_index = fuse_item.tablet_meta_index_v2_;
    ObMigrationTabletParam param_v2;
    if (OB_FAIL(fetch_tablet_meta_in_user_data_(meta_index, param_v2))) {
      LOG_WARN("failed to fetch tablet meta in user data", K(ret));
    } else if (OB_FAIL(inner_fuse_tablet_item_(fuse_item.tablet_param_v1_, param_v2, output_param))) {
      LOG_WARN("failed to fuse tablet item", K(ret));
    }
  } else {
    if (OB_FAIL(output_param.assign(fuse_item.tablet_param_v1_))) {
      LOG_WARN("failed to assign param", K(ret), K(fuse_item));
    } else if (fuse_item.tablet_id_.is_ls_inner_tablet()) {
      fuse_type_ = ObBackupFuseTabletType::FUSE_TABLET_LS_INNER_TABLET;
    } else {
      bool is_deleted = false;
      bool is_reorganized = false;
      if (OB_ISNULL(group_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group ctx should not be null", K(ret));
      } else if (OB_FAIL(check_tablet_reorganized_(
          group_ctx_->param_.tenant_id_, fuse_item.tablet_id_, is_reorganized))) {
        LOG_WARN("failed to check tablet has reorganized", K(ret));
      } else if (is_reorganized) {
        fuse_type_ = ObBackupFuseTabletType::FUSE_TABLET_META_REORGANIZED;
        output_param.ha_status_.set_restore_status(ObTabletRestoreStatus::UNDEFINED);
      } else if (OB_FAIL(check_tablet_deleted_(
          group_ctx_->param_.tenant_id_, fuse_item.tablet_id_, is_deleted))) {
        LOG_WARN("failed to check tablet deleted", K(ret));
      } else if (OB_UNLIKELY(!is_deleted)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet not deleted", K(ret), K(fuse_item));
      } else {
        fuse_type_ = ObBackupFuseTabletType::FUSE_TABLET_META_DELETED;
        output_param.ha_status_.set_restore_status(ObTabletRestoreStatus::UNDEFINED);
      }
    }
  }
  return ret;
}

int ObBackupTabletFuseTask::inner_fuse_tablet_item_(
    const ObMigrationTabletParam &param_v1,
    const ObMigrationTabletParam &param_v2,
    ObMigrationTabletParam &output_param)
{
  int ret = OB_SUCCESS;
  if (!param_v1.is_valid() || !param_v2.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param_v1), K(param_v2));
  } else if (param_v1.transfer_info_.transfer_seq_ != param_v2.transfer_info_.transfer_seq_) {
    if (OB_FAIL(output_param.assign(param_v1))) {
      LOG_WARN("failed to assign param", K(ret), K(param_v1));
    } else {
      fuse_type_ = ObBackupFuseTabletType::FUSE_TABLET_META_USE_V1;
      output_param.ha_status_.set_restore_status(ObTabletRestoreStatus::UNDEFINED);
    }
  } else {
    if (OB_FAIL(output_param.assign(param_v2))) {
      LOG_WARN("failed to assign param", K(ret), K(param_v1));
    } else {
      fuse_type_ = ObBackupFuseTabletType::FUSE_TABLET_META_USE_V2;
      output_param.ha_status_.set_restore_status(ObTabletRestoreStatus::EMPTY);
    }
  }
  return ret;
}

int ObBackupTabletFuseTask::write_new_tablet_info_(
    const ObMigrationTabletParam &output_param)
{
  int ret = OB_SUCCESS;
  if (!output_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(output_param));
  } else if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(group_ctx_->write_tablet_meta(output_param))) {
    LOG_WARN("failed to write tablet meta", K(ret), K(output_param));
  }
  return ret;
}

int ObBackupTabletFuseTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  char buf[OB_MAX_COLUMN_NAME_BUF_LENGTH] = "";
  int64_t pos = 0;
  if (OB_ISNULL(group_ctx_) || OB_ISNULL(fuse_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KP_(group_ctx), KP_(fuse_ctx));
  } else if (OB_FAIL(group_ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos,
      "turn_id_%ld_retry_id_%ld", group_ctx_->param_.turn_id_, group_ctx_->param_.retry_id_))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_SYNC_ADD("backup_fuse", "backup_tablet_fuse_task",
                          "tenant_id", group_ctx_->param_.tenant_id_,
                          "backup_set_id", group_ctx_->param_.backup_set_desc_.backup_set_id_,
                          "ls_id", group_ctx_->param_.ls_id_.id(),
                          "turn_id_and_retry_id", buf,
                          "tablet_id", fuse_ctx_->fuse_item_.tablet_id_.id(),
                          "result", result,
                          fuse_type_);
#endif
  }
  return ret;
}

}
}
