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
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/ob_server.h"

using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace backup
{

ObBackupTabletFuseDagNet::ObBackupTabletFuseDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_BACKUP),
    is_inited_(false),
    ctx_(NULL),
    compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

ObBackupTabletFuseDagNet::~ObBackupTabletFuseDagNet()
{
  free_fuse_ctx_();
}

int ObBackupTabletFuseDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSBackupDagNetInitParam* init_param = static_cast<const ObLSBackupDagNetInitParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group fuse dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid() || !OB_BACKUP_INDEX_CACHE.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_fuse_ctx_())) {
    LOG_WARN("failed to alloc tablet group fuse ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->job_desc_.trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), K(init_param));
  } else if (OB_FAIL(ctx_->param_.init(*init_param))) {
    LOG_WARN("failed to init param", K(ret), KPC(init_param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(init_param->tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), KPC(init_param));
  } else {
    ctx_->report_ctx_ = init_param->report_ctx_;
    is_inited_ = true;
  }
  return ret;
}

bool ObBackupTabletFuseDagNet::is_valid() const
{
  return OB_NOT_NULL(ctx_) && ctx_->is_valid();
}

int ObBackupTabletFuseDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = NULL;
  ObInitialBackupTabletGroupFuseDag *initial_dag = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_->start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_dag))) {
    LOG_WARN("failed to alloc inital fuse dag ", K(ret));
  } else if (OB_FAIL(initial_dag->init(this))) {
    LOG_WARN("failed to init initial fuse dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_dag))) {
    LOG_WARN("failed to add initial fuse dag into dag net", K(ret));
  } else if (OB_FAIL(initial_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
    LOG_WARN("failed to add initial fuse dag", K(ret), K(*initial_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_dag = NULL;
  }

  if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
    scheduler->free_dag(*initial_dag);
  }
  return ret;
}

bool ObBackupTabletFuseDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    is_same = true;
  } else {
    is_same = false;
  }
  return is_same;
}

int64_t ObBackupTabletFuseDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fuse ctx is NULL", KPC(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_->param_, sizeof(ctx_->param_), hash_value);
  }
  return hash_value;
}

int ObBackupTabletFuseDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group fuse dag net do not init ", K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "ObBackupTabletFuseDagNet:",
      ", ls_id = ", ctx_->param_.ls_id_,
      ", backup_set_id = ", ctx_->param_.backup_set_desc_.backup_set_id_,
      ", turn_id = ", ctx_->param_.turn_id_,
      ", retry_id = ", ctx_->param_.retry_id_))) {
    LOG_WARN("failed to databuff printf", K(ret), "param", ctx_->param_);
  }
  return ret;
}

int ObBackupTabletFuseDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group fuse dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "ObBackupTabletFuseDagNet: ls_id = ", ctx_->param_.ls_id_,
      ", backup_set_id = ", ctx_->param_.backup_set_desc_.backup_set_id_))) {
    LOG_WARN("failed to databuff printf", K(ret), "param", ctx_->param_);
  }
  return ret;
}

int ObBackupTabletFuseDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group fuse dag net do not init", K(ret));
  } else {
    ctx_->finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_ts_ - ctx_->start_ts_;
    FLOG_INFO("finish tablet group fuse dag net", K(cost_ts), KPC_(ctx));
  }
  return ret;
}

int ObBackupTabletFuseDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group fuse dag net do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KP_(ctx));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

int ObBackupTabletFuseDagNet::alloc_fuse_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group fuse ctx init twice", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObBackupTabletGroupFuseCtx), "TGFuseCtx"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ctx_ = new (buf) ObBackupTabletGroupFuseCtx())) {
  }
  return ret;
}

void ObBackupTabletFuseDagNet::free_fuse_ctx_()
{
  if (OB_ISNULL(ctx_)) {
    //do nothing
  } else {
    ctx_->~ObBackupTabletGroupFuseCtx();
    mtl_free(ctx_);
    ctx_ = NULL;
  }
}

/******************ObBackupTabletGroupFuseDag*********************/
ObBackupTabletGroupFuseDag::ObBackupTabletGroupFuseDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : share::ObIDag(dag_type),
    ctx_(NULL),
    compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

ObBackupTabletGroupFuseDag::~ObBackupTabletGroupFuseDag()
{
}

bool ObBackupTabletGroupFuseDag::operator == (const ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObBackupTabletGroupFuseDag &other_dag = static_cast<const ObBackupTabletGroupFuseDag&>(other);
    if (OB_ISNULL(get_ctx()) || OB_ISNULL(other_dag.get_ctx())) {
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "tablet group fuse ctx should not be NULL");
    } else {
      is_same = get_ctx()->param_ == other_dag.get_ctx()->param_;
    }
  }
  return is_same;
}

int64_t ObBackupTabletGroupFuseDag::hash() const
{
  int64_t hash_value = 0;
  ObBackupTabletGroupFuseCtx *ctx = get_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "tablet group fuse ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(&ctx->param_.ls_id_, sizeof(ctx->param_.ls_id_), hash_value);
    hash_value = common::murmurhash(&ctx->param_.backup_set_desc_.backup_set_id_, sizeof(ctx->param_.backup_set_desc_.backup_set_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(&dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObBackupTabletGroupFuseDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KP_(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             ctx_->param_.ls_id_.id(),
                                             ctx_->param_.backup_set_desc_.backup_set_id_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

/******************ObInitialBackupTabletGroupFuseDag*********************/
ObInitialBackupTabletGroupFuseDag::ObInitialBackupTabletGroupFuseDag()
  : ObBackupTabletGroupFuseDag(ObDagType::DAG_TYPE_INITIAL_BACKUP_FUSE),
    is_inited_(false)
{
}

ObInitialBackupTabletGroupFuseDag::~ObInitialBackupTabletGroupFuseDag()
{
}

int ObInitialBackupTabletGroupFuseDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObBackupTabletFuseDagNet *tablet_fuse_dag_net = NULL;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial tablet group fuse dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (OB_ISNULL(tablet_fuse_dag_net = static_cast<ObBackupTabletFuseDagNet*>(dag_net))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else {
    ctx_ = tablet_fuse_dag_net->get_fuse_ctx();
    compat_mode_ = tablet_fuse_dag_net->get_compat_mode();
    is_inited_ = true;
  }
  return ret;
}

int ObInitialBackupTabletGroupFuseDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObBackupTabletGroupFuseCtx *ctx = NULL;

  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group fuse dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group fuse ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "ObInitialBackupTabletGroupFuseDag: ls_id = ", ctx_->param_.ls_id_,
      ", backup_set_id = ", ctx_->param_.backup_set_desc_.backup_set_id_))) {
    LOG_WARN("failed to databuff printf", K(ret), "param", ctx_->param_);
  }
  return ret;
}

int ObInitialBackupTabletGroupFuseDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialBackupTabletGroupFuseTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group fuse dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial tablet group fuse task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartBackupTabletGroupFuseDag*********************/
ObStartBackupTabletGroupFuseDag::ObStartBackupTabletGroupFuseDag()
  : ObBackupTabletGroupFuseDag(ObDagType::DAG_TYPE_START_BACKUP_FUSE),
    is_inited_(false),
    finish_dag_(NULL)
{
}

ObStartBackupTabletGroupFuseDag::~ObStartBackupTabletGroupFuseDag()
{
}

int ObStartBackupTabletGroupFuseDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObBackupTabletGroupFuseCtx *ctx = NULL;

  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group fuse dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group fuse ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "ObStartBackupTabletGroupFuseDag: ls_id = ", ctx_->param_.ls_id_,
      ", backup_set_id = ", ctx_->param_.backup_set_desc_.backup_set_id_))) {
    LOG_WARN("failed to databuff printf", K(ret), "param", ctx_->param_);
  }
  return ret;
}

int ObStartBackupTabletGroupFuseDag::init(
    share::ObIDagNet *dag_net, share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObBackupTabletFuseDagNet *fuse_dag_net = NULL;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start tablet group fuse dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start tablet group fuse dag init get invalid argument", K(ret), KP(dag_net), KP(finish_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (FALSE_IT(fuse_dag_net = static_cast<ObBackupTabletFuseDagNet*>(dag_net))) {
  } else {
    ctx_ = fuse_dag_net->get_fuse_ctx();
    finish_dag_ = finish_dag;
    compat_mode_ = fuse_dag_net->get_compat_mode();
    is_inited_ = true;
  }
  return ret;
}

int ObStartBackupTabletGroupFuseDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartBackupTabletGroupFuseTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group fuse dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(finish_dag_))) {
    LOG_WARN("failed to init start tablet group fuse task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishBackupTabletGroupFuseDag*********************/
ObFinishBackupTabletGroupFuseDag::ObFinishBackupTabletGroupFuseDag()
  : ObBackupTabletGroupFuseDag(ObDagType::DAG_TYPE_FINISH_BACKUP_FUSE),
    is_inited_(false)
{
}

ObFinishBackupTabletGroupFuseDag::~ObFinishBackupTabletGroupFuseDag()
{
}

int ObFinishBackupTabletGroupFuseDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObBackupTabletGroupFuseCtx *ctx = NULL;

  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group fuse dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group fuse ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
      "ObFinishBackupTabletGroupFuseDag: ls_id = ", ctx_->param_.ls_id_,
      ", backup_set_id = ", ctx_->param_.backup_set_desc_.backup_set_id_))) {
    LOG_WARN("failed to databuff printf", K(ret), "param", ctx_->param_);
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObBackupTabletFuseDagNet *fuse_dag_net = NULL;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish tablet group fuse dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is not backup", K(ret), "dag_net_type", dag_net->get_type());
  } else if (FALSE_IT(fuse_dag_net = static_cast<ObBackupTabletFuseDagNet*>(dag_net))) {
  } else {
    ctx_ = fuse_dag_net->get_fuse_ctx();
    compat_mode_ = fuse_dag_net->get_compat_mode();
    is_inited_ = true;
  }
  return ret;
}

int ObFinishBackupTabletGroupFuseDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishBackupTabletGroupFuseTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group fuse dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish tablet group fuse task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObBackupTabletFuseDag*********************/
ObBackupTabletFuseDag::ObBackupTabletFuseDag()
  : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_TABLET_FUSE),
    is_inited_(false),
    fuse_ctx_(),
    group_ctx_(NULL),
    compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

ObBackupTabletFuseDag::~ObBackupTabletFuseDag()
{
}

int ObBackupTabletFuseDag::init(
    const ObInitBackupTabletFuseParam &param,
    const ObBackupTabletFuseItem &fuse_item,
    ObBackupTabletGroupFuseCtx &group_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet fuse dag init twice", K(ret));
  } else if (!param.is_valid() || !fuse_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet fuse dag init get invalid argument", K(ret), K(param), K(fuse_item));
  } else if (OB_FAIL(fuse_ctx_.param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(fuse_ctx_.fuse_item_.assign(fuse_item))) {
    LOG_WARN("failed to assign param", K(ret), K(fuse_item));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param.tenant_id_, compat_mode_))) {
    LOG_WARN("failed to get_compat_mode", K(ret), K(param));
  } else {
    group_ctx_ = &group_ctx;
    fuse_ctx_.group_ctx_ = group_ctx_;
    is_inited_ = true;
  }
  return ret;
}

bool ObBackupTabletFuseDag::operator==(const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObBackupTabletFuseDag &other_dag = static_cast<const ObBackupTabletFuseDag&>(other);
    is_same = fuse_ctx_.param_ == other_dag.fuse_ctx_.param_
           && fuse_ctx_.fuse_item_.tablet_id_ == other_dag.fuse_ctx_.fuse_item_.tablet_id_;
  }
  return is_same;
}

int64_t ObBackupTabletFuseDag::hash() const
{
  int64_t hash_value = 0;
  const ObDagType::ObDagTypeEnum type = get_type();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&fuse_ctx_.param_.ls_id_, sizeof(fuse_ctx_.param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&fuse_ctx_.fuse_item_.tablet_id_, sizeof(fuse_ctx_.fuse_item_.tablet_id_), hash_value);
  return hash_value;
}

int ObBackupTabletFuseDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet fuse dag do not init", K(ret));
  } else if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos,
       "ObBackupTabletFuseDag: ls_id = ", fuse_ctx_.param_.ls_id_,
       ", backup_set_id = ", fuse_ctx_.param_.backup_set_desc_.backup_set_id_,
       ", tablet_id = ", fuse_ctx_.fuse_item_.tablet_id_))) {
    LOG_WARN("failed to fill comment", K(ret), K(fuse_ctx_));
  }
  return ret;
}

int ObBackupTabletFuseDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet fuse dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                fuse_ctx_.param_.ls_id_.id(),
                                static_cast<int64_t>(fuse_ctx_.fuse_item_.tablet_id_.id())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObBackupTabletFuseDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupTabletFuseTask *task = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet fuse dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(fuse_ctx_, *group_ctx_))) {
    LOG_WARN("failed to init sys tablets fuse task", K(ret), K(fuse_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObBackupTabletFuseDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  dag = NULL;
  ObTenantDagScheduler *scheduler = NULL;
  ObBackupTabletFuseDag *tablet_fuse_dag = NULL;
  ObBackupTabletFuseItem fuse_item;
  ObDagId dag_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet fuse dag do not init", K(ret));
  } else if (OB_ISNULL(group_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("tablet fuse dag not has next dag", KPC(this));
  } else if (group_ctx_->is_failed()) {
    // do nothing
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(group_ctx_->get_next_tablet_item(fuse_item))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next tablet id", K(ret), KPC(this));
    }
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_fuse_dag))) {
    LOG_WARN("failed to alloc tablet fuse dag", K(ret));
  } else if (OB_FAIL(tablet_fuse_dag->init(group_ctx_->param_, fuse_item, *group_ctx_))) {
    LOG_WARN("failed to init tablet fuse dag", K(ret), "param", group_ctx_->param_, K(fuse_item));
  } else if (FALSE_IT(dag_id.init(MYADDR))) {
  } else if (OB_FAIL(tablet_fuse_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret), K(fuse_item));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_fuse_dag));
    dag = tablet_fuse_dag;
    tablet_fuse_dag = NULL;
  }

  if (OB_NOT_NULL(tablet_fuse_dag)) {
    scheduler->free_dag(*tablet_fuse_dag);
    tablet_fuse_dag = NULL;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (OB_TMP_FAIL(group_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret));
    }
  }
  return ret;
}

}
}
