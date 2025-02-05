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
#include "storage/backup/ob_backup_complement_log.h"

namespace oceanbase
{
namespace backup
{

static int deal_with_fo(ObBackupComplementLogCtx *ctx, const int64_t result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx->set_result(result, false/*need_retry*/))) {
    LOG_WARN("failed to set result", K(ret));
  } else {
    LOG_INFO("deal with fo", K(ret), K(result));
  }
  return ret;
}

// ObBackupPieceFile

ObBackupPieceFile::ObBackupPieceFile()
    : dest_id_(-1), round_id_(-1), piece_id_(-1), ls_id_(), file_id_(-1), start_scn_(), checkpoint_scn_(), path_()
{}

void ObBackupPieceFile::reset()
{
  dest_id_ = -1;
  round_id_ = -1;
  piece_id_ = -1;
  ls_id_.reset();
  file_id_ = -1;
  start_scn_.reset();
  checkpoint_scn_.reset();
  path_.reset();
}

int ObBackupPieceFile::set(const int64_t dest_id, const int64_t round_id,
    const int64_t piece_id, const share::ObLSID &ls_id, const int64_t file_id,
    const share::SCN &start_scn, const share::SCN &checkpoint_scn, const ObBackupPathString &path)
{
  int ret = OB_SUCCESS;
  if (dest_id <= 0 || round_id <= 0 || piece_id <= 0 || !ls_id.is_valid() || file_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(dest_id), K(round_id), K(piece_id), K(ls_id), K(file_id));
  } else if (OB_FAIL(path_.assign(path))) {
    LOG_WARN("failed to assign path", K(ret), K(path));
  } else {
    dest_id_ = dest_id;
    round_id_ = round_id;
    piece_id_ = piece_id;
    ls_id_ = ls_id;
    file_id_ = file_id;
    start_scn_ = start_scn;
    checkpoint_scn_ = checkpoint_scn;
  }
  return ret;
}

// ObBackupComplementLogCtx

bool ObBackupComplementLogCtx::is_valid() const
{
  return job_desc_.is_valid()
      && backup_dest_.is_valid()
      && OB_INVALID_ID != tenant_id_
      && dest_id_ >= 0
      && backup_set_desc_.is_valid()
      && ls_id_.is_valid()
      && compl_start_scn_.is_valid()
      && compl_end_scn_.is_valid()
      && turn_id_ > 0
      && retry_id_ >= 0;
}

bool ObBackupComplementLogCtx::operator==(const ObBackupComplementLogCtx &other) const
{
  bool bret = false;
  bret = job_desc_ == other.job_desc_
      && backup_dest_ == other.backup_dest_
      && tenant_id_ == other.tenant_id_
      && dest_id_ == other.dest_id_
      && backup_set_desc_ == other.backup_set_desc_
      && ls_id_ == other.ls_id_
      && compl_start_scn_ == other.compl_start_scn_
      && compl_end_scn_ == other.compl_end_scn_
      && turn_id_ == other.turn_id_
      && retry_id_ == other.retry_id_
      && is_only_calc_stat_ == other.is_only_calc_stat_;
  return bret;
}

uint64_t ObBackupComplementLogCtx::calc_hash(uint64_t seed) const
{
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), seed);
  hash_code = murmurhash(&backup_set_desc_.backup_set_id_, sizeof(backup_set_desc_.backup_set_id_), hash_code);
  hash_code = murmurhash(&dest_id_, sizeof(dest_id_), hash_code);
  hash_code = murmurhash(&ls_id_, sizeof(ls_id_), hash_code);
  hash_code = murmurhash(&compl_start_scn_, sizeof(compl_start_scn_), hash_code);
  hash_code = murmurhash(&compl_end_scn_, sizeof(compl_end_scn_), hash_code);
  hash_code = murmurhash(&turn_id_, sizeof(turn_id_), hash_code);
  hash_code = murmurhash(&retry_id_, sizeof(retry_id_), hash_code);
  hash_code = murmurhash(&is_only_calc_stat_, sizeof(is_only_calc_stat_), hash_code);
  return hash_code;
}

int ObBackupComplementLogCtx::set_result(
    const int32_t result,
    const bool need_retry,
    const enum share::ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (OB_FAIL(result_mgr_.set_result(result, need_retry, type))) {
    LOG_WARN("failed to set result", K(ret), K(result), K(*this));
  }
  return ret;
}

bool ObBackupComplementLogCtx::is_failed() const
{
  lib::ObMutexGuard guard(mutex_);
  return result_mgr_.is_failed();
}

int ObBackupComplementLogCtx::check_allow_retry(bool &allow_retry)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  allow_retry = false;
  if (OB_FAIL(result_mgr_.check_allow_retry(allow_retry))) {
    LOG_WARN("failed to check need retry", K(ret), K(*this));
  }
  return ret;
}

int ObBackupComplementLogCtx::get_result(int32_t &result)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  result = OB_SUCCESS;
  if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

// ObBackupPieceOp

ObBackupPieceOp::ObBackupPieceOp() : file_id_list_()
{}

int ObBackupPieceOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  int64_t file_id = -1;
  int32_t len = 0;
  ObString entry_suffix;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", K(ret));
  } else if (FALSE_IT(len = strlen(entry->d_name) - strlen(OB_ARCHIVE_SUFFIX))) {
  } else if (len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file name without a unified suffix", K(ret), K(entry->d_name), K(OB_ARCHIVE_SUFFIX));
  } else if (FALSE_IT(entry_suffix.assign_ptr(entry->d_name + len, strlen(OB_ARCHIVE_SUFFIX)))) {
  } else if (!entry_suffix.prefix_match(OB_ARCHIVE_SUFFIX)) {
    // not ended with archive suffix
    ObString file_name_str(entry->d_name);
    LOG_INFO("skip file which is not archive file", K(file_name_str), K(entry_suffix));
  } else if (OB_FAIL(databuff_printf(file_name, sizeof(file_name), "%.*s", len, entry->d_name))) {
    LOG_WARN("fail to save tmp file name", K(ret), K(file_name));
  } else if (0 == ObString::make_string(file_name).case_compare(OB_STR_LS_FILE_INFO)) {
    LOG_INFO("skip ls file info");
  } else if (OB_FAIL(ob_atoll(file_name, file_id))) {
    LOG_WARN("failed to change string to number", K(ret), K(file_name));
  } else if (OB_FAIL(file_id_list_.push_back(file_id))) {
    LOG_WARN("failed to push back", K(ret), K(file_id));
  }
  return ret;
}

int ObBackupPieceOp::get_file_id_list(common::ObIArray<int64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_id_list_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

// CompareArchivePiece

bool CompareArchivePiece::operator()(
    const ObTenantArchivePieceAttr &lhs, const ObTenantArchivePieceAttr &rhs) const
{
  return lhs.key_.piece_id_ < rhs.key_.piece_id_;
}

// ObBackupComplementLogDagNet

ObBackupComplementLogDagNet::ObBackupComplementLogDagNet()
    : ObBackupDagNet(ObBackupDagNetSubType::LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG_NET),
      is_inited_(false),
      ctx_(),
      bandwidth_throttle_(NULL)
{}

ObBackupComplementLogDagNet::~ObBackupComplementLogDagNet()
{}

int ObBackupComplementLogDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  ObLSBackupDagNetInitParam init_param;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag net init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(param));
  } else if (OB_FAIL(init_param.assign(*(static_cast<const ObLSBackupDagNetInitParam *>(param))))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param.job_desc_.trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), K(init_param));
  } else if (OB_FAIL(ctx_.backup_dest_.deep_copy(init_param.backup_dest_))) {
    LOG_WARN("failed to deep copy backup dest", K(ret), K(init_param));
  } else {
    ctx_.job_desc_ = init_param.job_desc_;
    ctx_.tenant_id_ = init_param.tenant_id_;
    ctx_.dest_id_ = init_param.dest_id_;
    ctx_.backup_set_desc_ = init_param.backup_set_desc_;
    ctx_.ls_id_ = init_param.ls_id_;
    ctx_.compl_start_scn_ = init_param.compl_start_scn_;
    ctx_.compl_end_scn_ = init_param.compl_end_scn_;
    ctx_.turn_id_ = init_param.turn_id_;
    ctx_.retry_id_ = init_param.retry_id_;
    ctx_.report_ctx_ = init_param.report_ctx_;
    ctx_.is_only_calc_stat_ = init_param.is_only_calc_stat_;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupComplementLogDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObBackupLSLogGroupDag *complement_dag = NULL;
  ObBackupLSLogGroupFinishDag *finish_dag = NULL;
  ObTenantDagScheduler *dag_scheduler = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag net not init", K(ret));
  } else if (OB_FAIL(guard.switch_to(ctx_.tenant_id_))) {
    LOG_WARN("failed to switch to tenant", K(ret), K_(ctx));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(complement_dag))) {
    LOG_WARN("failed to alloc rebuild index dag", K(ret));
  } else if (OB_FAIL(complement_dag->init(ctx_.ls_id_, &ctx_, GCTX.bandwidth_throttle_))) {
    LOG_WARN("failed to init child dag", K(ret), K_(ctx));
  } else if (OB_FAIL(complement_dag->create_first_task())) {
    LOG_WARN("failed to create first task for child dag", K(ret), KPC(complement_dag));
  } else if (OB_FAIL(add_dag_into_dag_net(*complement_dag))) {
    LOG_WARN("failed to add dag into dag_net", K(ret), KPC(complement_dag));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(finish_dag))) {
    LOG_WARN("failed to create dag", K(ret));
  } else if (OB_FAIL(finish_dag->init(&ctx_))) {
    LOG_WARN("failed to init finish dag", K(ret), K_(ctx));
  } else if (OB_FAIL(finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(complement_dag->add_child(*finish_dag))) {
    LOG_WARN("failed to add child", K(ret), KPC(complement_dag), KPC(finish_dag));
  } else {
    bool add_finish_dag_success = false;
    bool add_complement_dag_success = false;
    if (OB_FAIL(dag_scheduler->add_dag(finish_dag))) {
      LOG_WARN("failed to add dag into dag_scheduler", K(ret), KP(finish_dag));
    } else {
      add_finish_dag_success = true;
      LOG_INFO("success to add finish dag into dag_net", K(ret), KP(finish_dag));
    }
    if (FAILEDx(dag_scheduler->add_dag(complement_dag))) {
      LOG_WARN("failed to add dag into dag_scheduler", K(ret), KP(complement_dag));
    } else {
      add_complement_dag_success = true;
      LOG_INFO("success to add complement log dag into dag_net", K(ret), KP(complement_dag));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(finish_dag)) {
      // add finish dag success and add complement log dag failed, need cancel finish dag
      if (add_finish_dag_success && !add_complement_dag_success) {
        if (OB_TMP_FAIL(dag_scheduler->cancel_dag(finish_dag))) {
          LOG_ERROR("failed to cancel backup dag", K(tmp_ret), KP(dag_scheduler), KP(finish_dag));
        } else {
          finish_dag = NULL;
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(complement_dag)) {
    dag_scheduler->free_dag(*complement_dag);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler) && OB_NOT_NULL(finish_dag)) {
    dag_scheduler->free_dag(*finish_dag);
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ObBackupUtils::report_task_result(ctx_.job_desc_.job_id_,
                ctx_.job_desc_.task_id_,
                ctx_.tenant_id_,
                ctx_.ls_id_,
                ctx_.turn_id_,
                ctx_.retry_id_,
                ctx_.job_desc_.trace_id_,
                this->get_dag_id(),
                ret,
                ctx_.report_ctx_))) {
      LOG_WARN("failed to report task result", K(ret), K_(ctx));
    }
  }
  return ret;
}

bool ObBackupComplementLogDagNet::operator==(const share::ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupDagNet &backup_dag_net = static_cast<const ObBackupDagNet &>(other);
    if (backup_dag_net.get_sub_type() != get_sub_type()) {
      bret = false;
    } else {
      const ObBackupComplementLogDagNet &dag = static_cast<const ObBackupComplementLogDagNet &>(other);
      bret = dag.ctx_ == ctx_;
    }
  }
  return bret;
}

bool ObBackupComplementLogDagNet::is_valid() const
{
  return ctx_.is_valid();
}

int64_t ObBackupComplementLogDagNet::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  const int64_t type = ObBackupDagNetSubType::LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG_NET;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = ctx_.calc_hash(hash_value);
  return hash_value;
}

int ObBackupComplementLogDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "[BACKUP_COMPLEMENT_LOG_DAG_NET]: tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
                 ctx_.tenant_id_,
                 ctx_.backup_set_desc_.backup_set_id_,
                 ctx_.ls_id_.id()))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  };
  return ret;
}

int ObBackupComplementLogDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf,
          buf_len,
          "tenant_id=%lu, backup_set_id=%ld, ls_id=%ld",
          ctx_.tenant_id_,
          ctx_.backup_set_desc_.backup_set_id_,
          ctx_.ls_id_.id()))) {
    LOG_WARN("failed to fill dag net key", K(ret), K(ctx_));
  };
  return ret;
}

// ObBackupLSLogGroupDag

ObBackupLSLogGroupDag::ObBackupLSLogGroupDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_LS_LOG_GROUP),
      is_inited_(false),
      ls_id_(),
      ctx_(NULL),
      bandwidth_throttle_(NULL)
{}

ObBackupLSLogGroupDag::~ObBackupLSLogGroupDag()
{}

int ObBackupLSLogGroupDag::init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx,
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup complement log task init twice", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(ls_id), KP(ctx), KP(bandwidth_throttle));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    bandwidth_throttle_ = bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogGroupDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupLSLogGroupTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls group dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(ls_id_, ctx_, bandwidth_throttle_))) {
    LOG_WARN("failed to init task", K(ret), K_(ls_id), K_(ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add complement log task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObBackupLSLogGroupDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup complement log dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(ctx_->tenant_id_), ctx_->backup_set_desc_.backup_set_id_,
                                  ls_id_.id()))){
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObBackupLSLogGroupDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos, "ls_id=", ctx_->ls_id_))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(ls_id));
  }
  return ret;
}

bool ObBackupLSLogGroupDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupLSLogGroupDag &other_dag = static_cast<const ObBackupLSLogGroupDag &>(other);
    bret = ctx_ == other_dag.ctx_;
  }
  return bret;
}

int64_t ObBackupLSLogGroupDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup ls group dag do not init", K(ret));
  } else {
    int64_t type = get_type();
    hash_value = common::murmurhash(&type, sizeof(type), hash_value);
    hash_value = ctx_->calc_hash(hash_value);
  }
  return hash_value;
}

// ObBackupLSLogGroupTask

ObBackupLSLogGroupTask::ObBackupLSLogGroupTask()
  : ObITask(ObITask::ObITaskType::TASK_TYPE_BACKUP_LS_LOG_GROUP),
    is_inited_(false),
    ls_id_(),
    ctx_(NULL),
    current_idx_(0),
    ls_ids_(),
    bandwidth_throttle_(NULL)
{}

ObBackupLSLogGroupTask::~ObBackupLSLogGroupTask()
{
}

int ObBackupLSLogGroupTask::init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx, common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls group task is not init", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KPC(ctx), K(ls_id), KP(bandwidth_throttle));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    bandwidth_throttle_ = bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogGroupTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls group task is not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    // do nothing
  } else if (OB_FAIL(generate_ls_dag_())) {
    LOG_WARN("failed to generate ls backup dag", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupLSLogGroupTask::get_next_ls_id_(share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  if (current_idx_ >= ls_ids_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("ls iterator end", K(ret), K_(current_idx), K_(ls_ids));
  } else {
    ls_id = ls_ids_.at(current_idx_);
    current_idx_++;
  }
  return ret;
}

int ObBackupLSLogGroupTask::generate_ls_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObIDag *> ls_dag_array;
  ObTenantDagScheduler *scheduler = nullptr;
  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(ls_ids_.push_back(ls_id_))) {
    LOG_WARN("failed to push back", K(ret), K_(ls_id));
  }
  while (OB_SUCC(ret)) {
    share::ObLSID ls_id;
    if (OB_FAIL(get_next_ls_id_(ls_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next ls id", K(ret));
      }
      break;
    } else {
      ObIDagNet *dag_net = nullptr;
      ObIDag *parent = this->get_dag();
      ObBackupLSLogDag *ls_dag = nullptr;

      if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls migration dag net should not be NULL", K(ret), KP(dag_net));
      } else if (OB_FAIL(scheduler->alloc_dag(ls_dag))) {
        LOG_WARN("failed to alloc ls dag ", K(ret));
      } else if (OB_FAIL(ls_dag_array.push_back(ls_dag))) {
        LOG_WARN("failed to push tablet restore dag into array", K(ret), K(*ctx_));
      } else if (OB_FAIL(ls_dag->init(ls_id, ctx_, bandwidth_throttle_))) {
        LOG_WARN("failed to init ls dag", K(ret), K(ls_id), KPC_(ctx));
      } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*ls_dag))) {
        LOG_WARN("failed to add dag into dag net", K(ret), KPC_(ctx));
      } else if (OB_FAIL(parent->add_child(*ls_dag))) {
        LOG_WARN("failed to add child dag", K(ret), KPC_(ctx));
      } else if (OB_FAIL(ls_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), KPC_(ctx));
      } else if (OB_FAIL(scheduler->add_dag(ls_dag))) {
        LOG_WARN("failed to add ls dag", K(ret), KPC(ls_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule ls backup dag", KPC(ls_dag));
        ls_dag = nullptr;
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(ls_dag)) {
        if (!ls_dag_array.empty()) {
          ObIDag *last = ls_dag_array.at(ls_dag_array.count() - 1);
          if (last == ls_dag) {
            ls_dag_array.pop_back();
          }
        }

        scheduler->free_dag(*ls_dag);
        ls_dag = nullptr;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler)) {
    for (int64_t idx = 0; idx < ls_dag_array.count(); ++idx) {
      if (OB_TMP_FAIL(scheduler->cancel_dag(ls_dag_array.at(idx)))) {
        LOG_WARN("failed to cancel ls dag", K(tmp_ret), K(idx));
      }
    }
    ls_dag_array.reset();
  }
  return ret;
}

int ObBackupLSLogGroupTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_complement_log", "backup_ls_log_group_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "turn_id", ctx_->turn_id_,
                     "retry_id", ctx_->retry_id_,
                     "is_only_calc_stat", ctx_->is_only_calc_stat_);
  }
  return ret;
}

// ObBackupLSLogDag

ObBackupLSLogDag::ObBackupLSLogDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_LS_LOG),
      is_inited_(false),
      ls_id_(),
      ctx_(NULL),
      bandwidth_throttle_(NULL)
{}

ObBackupLSLogDag::~ObBackupLSLogDag()
{}

int ObBackupLSLogDag::init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx,
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup ls dag do not init", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), KP(ctx), KP(bandwidth_throttle));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    bandwidth_throttle_ = bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupLSLogTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(ls_id_, ctx_, bandwidth_throttle_))) {
    LOG_WARN("failed to init task", K(ret), K_(ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add backup ls log task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObBackupLSLogDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup complement log dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(ctx_->tenant_id_), ctx_->backup_set_desc_.backup_set_id_,
                                  ls_id_.id()))){
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObBackupLSLogDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_print_multi_objs(buf, buf_len, pos, "ls_id=", ls_id_))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(ls_id));
  }
  return ret;
}

bool ObBackupLSLogDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupLSLogDag &other_dag = static_cast<const ObBackupLSLogDag &>(other);
    bret = ctx_ == other_dag.ctx_;
  }
  return bret;
}

int64_t ObBackupLSLogDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup ls group dag do not init", K(ret));
  } else {
    int64_t type = get_type();
    hash_value = common::murmurhash(&type, sizeof(type), hash_value);
    hash_value = ctx_->calc_hash(hash_value);
    hash_value = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_value);
  }
  return hash_value;
}

// ObBackupLSLogTask

ObBackupLSLogTask::ObBackupLSLogTask()
  : ObITask(ObITask::ObITaskType::TASK_TYPE_BACKUP_LS_LOG),
    is_inited_(false),
    ls_id_(),
    ctx_(NULL),
    bandwidth_throttle_(NULL),
    file_list_(),
    archive_dest_()
{
}

ObBackupLSLogTask::~ObBackupLSLogTask()
{
}

int ObBackupLSLogTask::init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx,
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx) || OB_ISNULL(bandwidth_throttle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), KP(ctx), KP(bandwidth_throttle));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    bandwidth_throttle_ = bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t dest_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    // do nothing
  } else if (OB_FAIL(get_active_round_dest_id_(ctx_->tenant_id_, dest_id))) {
    LOG_WARN("failed to get active round dest id", K(ret), K(dest_id));
  } else if (OB_FAIL(inner_process_(dest_id, ls_id_))) {
    LOG_WARN("failed to inner process", K(ret), K_(ls_id));
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupLSLogTask::get_active_round_dest_id_(const uint64_t tenant_id, int64_t &dest_id)
{
  int ret = OB_SUCCESS;
  dest_id = 0;
  ObArchivePersistHelper helper;
  ObArray<ObTenantArchiveRoundAttr> rounds;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_all_active_rounds(*ctx_->report_ctx_.sql_proxy_, rounds))) {
    LOG_WARN("failed to get all active rounds", K(ret), K(tenant_id), K(dest_id));
  } else if (1 != rounds.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("round count is not one", K(ret), K(tenant_id));
  } else {
    dest_id = rounds.at(0).dest_id_;
  }
  return ret;
}

int ObBackupLSLogTask::inner_process_(
    const int64_t archive_dest_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath backup_path;
  ObBackupIoAdapter util;
  ObArray<ObTenantArchivePieceAttr> piece_list;
  if (OB_FAIL(get_complement_log_dir_path_(backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret));
  } else if (OB_FAIL(util.mkdir(backup_path.get_obstr(), ctx_->backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to make parent dir", K(ret), K(backup_path), KPC_(ctx));
  } else if (OB_FAIL(calc_backup_file_range_(archive_dest_id, ls_id, piece_list, file_list_))) {
    LOG_WARN("failed to calc backup file range", K(ret), K(archive_dest_id), K(ls_id));
  } else {
    if (ctx_->is_only_calc_stat_) {
      if (OB_FAIL(report_complement_log_stat_(file_list_))) {
        LOG_WARN("failed to report complement log stat", K(ret), K_(file_list));
      }
    } else {
      if (OB_FAIL(deal_with_piece_meta_(piece_list))) {
        LOG_WARN("failed to deal with piece meta", K(ret), K(piece_list), K_(file_list));
      }
    }

    if (FAILEDx(generate_ls_copy_task_(ctx_->is_only_calc_stat_, ls_id))) {
      LOG_WARN("failed to generate ls copy task", K(ret), K(ls_id));
    }
  }
  return ret;
}

int ObBackupLSLogTask::deal_with_piece_meta_(
    const common::ObIArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(piece_list, idx, cnt, OB_SUCC(ret)) {
    const ObTenantArchivePieceAttr &piece_attr = piece_list.at(idx);
    if (OB_FAIL(inner_deal_with_piece_meta_(piece_attr))) {
      LOG_WARN("failed to backup complement log for piece", K(ret), K(piece_attr));
    } else {
      LOG_INFO("inner deal with piece meta", K(piece_attr));
    }
  }
  return ret;
}

int ObBackupLSLogTask::inner_deal_with_piece_meta_(const ObTenantArchivePieceAttr &piece_attr)
{
  int ret = OB_SUCCESS;
  ObBackupDest src;
  ObBackupDest dest;
  if (OB_FAIL(transform_and_copy_meta_file_(piece_attr))) {
    LOG_WARN("failed to transform and copy meta file", K(ret), K(piece_attr));
  } else if (OB_FAIL(get_copy_src_and_dest_(piece_attr, src, dest))) {
    LOG_WARN("failed to get copy src and dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(copy_piece_start_file_(piece_attr, src, dest))) {
    LOG_WARN("failed to copy piece start file", K(ret), K(piece_attr));
  } else if (OB_FAIL(copy_piece_end_file_(piece_attr, src, dest))) {
    LOG_WARN("failed to copy piece start file", K(ret), K(piece_attr));
  }
  return ret;
}

int ObBackupLSLogTask::generate_ls_copy_task_(const bool is_only_calc_stat, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (!is_only_calc_stat) {
    ObBackupLSLogFileTask *copy_task = NULL;
    ObBackupLSLogFinishTask *finish_task = NULL;
    if (OB_FAIL(dag_->alloc_task(finish_task))) {
      LOG_WARN("failed to alloc finish task", K(ret));
    } else if (OB_FAIL(finish_task->init(ls_id, file_list_, ctx_))) {
      LOG_WARN("failed to init finish task", K(ret), K(ls_id), KPC_(ctx));
    } else if (OB_FAIL(dag_->alloc_task(copy_task))) {
      LOG_WARN("failed to alloc copy task", K(ret));
    } else if (OB_FAIL(copy_task->init(ls_id, bandwidth_throttle_, ctx_, finish_task))) {
      LOG_WARN("failed to init copy task", K(ret), K(ls_id), KPC_(ctx), KPC(finish_task));
    } else if (OB_FAIL(this->add_child(*copy_task))) {
      LOG_WARN("failed to add child", K(ret));
    } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
      LOG_WARN("failed to add child finish task", K(ret));
    } else if (OB_FAIL(dag_->add_task(*copy_task))) {
      LOG_WARN("failed to add copy task to dag", K(ret));
    } else if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    } else {
      LOG_INFO("generate ls copy task", K(ls_id));
    }
  } else {
    ObBackupLSLogFinishTask *finish_task = NULL;
    if (OB_FAIL(dag_->alloc_task(finish_task))) {
      LOG_WARN("failed to alloc finish task", K(ret));
    } else if (OB_FAIL(finish_task->init(ls_id, file_list_, ctx_))) {
      LOG_WARN("failed to init finish task", K(ret), K(ls_id), KPC_(ctx));
    } else if (OB_FAIL(this->add_child(*finish_task))) {
      LOG_WARN("failed to add child", K(ret));
    } else if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    } else {
      LOG_INFO("generate ls copy task", K(ls_id));
    }
  }
  return ret;
}

int ObBackupLSLogTask::get_complement_log_dir_path_(ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  if (OB_FAIL(ObBackupPathUtil::get_complement_log_dir_path(
      ctx_->backup_dest_, ctx_->backup_set_desc_, backup_path))) {
    LOG_WARN("failed to get backup file path", K(ret));
  }
  return ret;
}

int ObBackupLSLogTask::get_ls_replay_start_scn_if_not_newly_created_(const share::ObLSID &ls_id, share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataStore store;
  storage::ObLSMetaPackage ls_meta_package;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", KPC_(ctx));
  } else if (OB_FAIL(store.init(ctx_->backup_dest_, ctx_->backup_set_desc_))) {
    LOG_WARN("failed to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_id, ls_meta_package))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("should be newly created ls", K(ls_id));
    } else {
      LOG_WARN("failed to read ls meta infos", K(ret), K(ls_id));
    }
  } else {
    start_scn = ls_meta_package.palf_meta_.prev_log_info_.scn_;
    LOG_INFO("get ls replay start scn if not newly created", K(ls_id), K(start_scn));
  }
  return ret;
}

int ObBackupLSLogTask::calc_backup_file_range_(const int64_t dest_id, const share::ObLSID &ls_id,
    common::ObArray<ObTenantArchivePieceAttr> &piece_list, common::ObIArray<ObBackupPieceFile> &file_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  file_list.reset();
  uint64_t tenant_id = OB_INVALID_ID;
  SCN start_scn = SCN::invalid_scn();
  SCN end_scn = SCN::invalid_scn();
  int64_t start_piece_id = 0;
  int64_t end_piece_id = 0;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KP_(ctx));
  } else if (FALSE_IT(tenant_id = ctx_->tenant_id_)) {
  } else if (FALSE_IT(start_scn = ctx_->compl_start_scn_)) {
  } else if (FALSE_IT(end_scn = ctx_->compl_end_scn_)) {
  } else if (OB_FAIL(get_ls_replay_start_scn_if_not_newly_created_(ls_id, start_scn))) {
    LOG_WARN("failed to get ls replay start scn if newly created", K(ret), K(ls_id));
  } else if (OB_FAIL(get_piece_id_by_scn_(tenant_id, dest_id, start_scn, start_piece_id))) {
    LOG_WARN("failed to get piece id by ts", K(ret), K(tenant_id), K(start_scn));
  } else if (OB_FAIL(get_piece_id_by_scn_(tenant_id, dest_id, end_scn, end_piece_id))) {
    LOG_WARN("failed to get piece id by ts", K(ret), K(tenant_id), K(end_scn));
  } else if (OB_FAIL(get_all_pieces_(tenant_id, dest_id, start_piece_id, end_piece_id, piece_list))) {
    LOG_WARN("failed to get all round pieces", K(ret), K(tenant_id), K(start_piece_id), K(end_piece_id));
  } else if (OB_FAIL(wait_pieces_frozen_(piece_list))) {
    LOG_WARN("failed to wait pieces frozen", K(ret), K(piece_list));
  } else if (OB_FAIL(get_all_piece_file_list_(tenant_id, ls_id, piece_list, start_scn, end_scn, file_list))) {
    LOG_WARN("failed to get all piece file list", K(ret), K(tenant_id), K(ls_id), K(start_piece_id), K(end_piece_id));
  } else {
    LOG_INFO("get all piece file list", K(tenant_id), K(start_piece_id), K(end_piece_id), K(piece_list), K(file_list));
  }
  return ret;
}

int ObBackupLSLogTask::update_ls_task_stat_(const share::ObBackupStats &old_backup_stat,
    const int64_t compl_log_file_count, share::ObBackupStats &new_backup_stat)
{
  int ret = OB_SUCCESS;
  if (!old_backup_stat.is_valid() || compl_log_file_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(old_backup_stat), K(compl_log_file_count));
  } else {
    new_backup_stat.input_bytes_ = old_backup_stat.input_bytes_;
    new_backup_stat.output_bytes_ = old_backup_stat.output_bytes_;
    new_backup_stat.tablet_count_ = old_backup_stat.tablet_count_;
    new_backup_stat.macro_block_count_ = old_backup_stat.macro_block_count_;
    new_backup_stat.finish_macro_block_count_ = old_backup_stat.finish_macro_block_count_;
    new_backup_stat.finish_tablet_count_ = old_backup_stat.finish_tablet_count_;
    new_backup_stat.finish_macro_block_count_ = new_backup_stat.finish_macro_block_count_;
    new_backup_stat.log_file_count_ += compl_log_file_count;
  }
  return ret;
}

int ObBackupLSLogTask::report_complement_log_stat_(const common::ObIArray<ObBackupPieceFile> &file_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t compl_log_file_count = file_list.count();
  ObMySQLTransaction trans;
  int64_t max_file_id = 0;
  const bool for_update = true;
  const int64_t job_id = ctx_->job_desc_.job_id_;
  const int64_t task_id = ctx_->job_desc_.task_id_;
  const uint64_t tenant_id = ctx_->tenant_id_;
  const share::ObLSID &ls_id = ls_id_;
  if (OB_FAIL(trans.start(ctx_->report_ctx_.sql_proxy_, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    share::ObBackupLSTaskAttr old_ls_task_attr;
    share::ObBackupStats new_ls_task_stat;
    if (OB_FAIL(ObBackupLSTaskOperator::get_ls_task(trans, for_update,
              task_id, tenant_id, ls_id, old_ls_task_attr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("ls task should not exist, should be newly create ls", K(ret), K(ls_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get ls task", K(ret), K(task_id), K(tenant_id), K(ls_id));
      }
    } else if (OB_FAIL(update_ls_task_stat_(old_ls_task_attr.stats_, compl_log_file_count, new_ls_task_stat))) {
      LOG_WARN("failed to update ls task stat", K(ret));
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_stats(trans, task_id, tenant_id, ls_id, new_ls_task_stat))) {
      LOG_WARN("failed to update stat", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", K(ret));
      }
    } else {
      if (OB_TMP_FAIL(trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupLSLogTask::get_piece_id_by_scn_(const uint64_t tenant_id,
    const int64_t dest_id, const SCN &scn, int64_t &piece_id)
{
  int ret = OB_SUCCESS;
  piece_id = 0;
  ObArchivePersistHelper helper;
  ObTenantArchivePieceAttr piece;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_piece_by_scn(*ctx_->report_ctx_.sql_proxy_, dest_id, scn, piece))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(tenant_id), K(dest_id), K(scn));
  } else {
    piece_id = piece.key_.piece_id_;
    LOG_INFO("get piece id by scn", K(tenant_id), K(scn), K(piece_id));
  }
  return ret;
}

int ObBackupLSLogTask::get_all_pieces_(const uint64_t tenant_id, const int64_t dest_id,
    const int64_t start_piece_id, const int64_t end_piece_id, common::ObArray<ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  piece_list.reset();
  ObArchivePersistHelper helper;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_pieces_by_range(*ctx_->report_ctx_.sql_proxy_, dest_id, start_piece_id, end_piece_id, piece_list))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(tenant_id), K(dest_id), K(start_piece_id), K(end_piece_id));
  } else {
    LOG_INFO("get pieces by range", K(tenant_id), K(start_piece_id), K(end_piece_id), K(piece_list));
  }
  return ret;
}

int ObBackupLSLogTask::wait_pieces_frozen_(const common::ObArray<share::ObTenantArchivePieceAttr> &piece_list)
{
  int ret = OB_SUCCESS;
  if (0 == piece_list.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("piece list should not be empty", K(ret));
  } else if (1 == piece_list.count()) {
    LOG_INFO("only one piece, no need wait frozen", K(piece_list));
  } else {
    int64_t start_idx = piece_list.count() - 2;
    for (int64_t i = start_idx; OB_SUCC(ret) && i >= 0; i--) {
      const ObTenantArchivePieceAttr &piece = piece_list.at(i);
      if (OB_FAIL(wait_piece_frozen_(piece))) {
        LOG_WARN("failed to wait piece frozen", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupLSLogTask::wait_piece_frozen_(const share::ObTenantArchivePieceAttr &piece)
{
  int ret = OB_SUCCESS;
  static const int64_t CHECK_TIME_INTERVAL = 1_s;
  static const int64_t MAX_CHECK_INTERVAL = 60_s;
  const int64_t start_ts = ObTimeUtility::current_time();
  if (!piece.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(piece));
  } else {
    do {
      bool is_frozen = false;
      const int64_t cur_ts = ObTimeUtility::current_time();
      if (cur_ts - start_ts > MAX_CHECK_INTERVAL) {
        ret = OB_TIMEOUT;
        LOG_WARN("backup advance checkpoint by flush timeout", K(ret), K(piece));
      } else if (OB_FAIL(check_piece_frozen_(piece, is_frozen))) {
        LOG_WARN("failed to get ls meta", K(ret), K(piece));
      } else if (is_frozen) {
        LOG_INFO("piece already frozen", K(piece), "wait_time", cur_ts - start_ts);
        break;
      } else {
        LOG_INFO("wait piece frozen", K(piece), "wait_time", cur_ts - start_ts);
        ob_usleep(CHECK_TIME_INTERVAL);
        share::dag_yield();
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

int ObBackupLSLogTask::check_piece_frozen_(const share::ObTenantArchivePieceAttr &piece, bool &is_frozen)
{
  int ret = OB_SUCCESS;
  is_frozen = false;
  ObArchivePersistHelper helper;
  ObTenantArchivePieceAttr cur_piece;
  const uint64_t tenant_id = ctx_->tenant_id_;
  const int64_t dest_id = piece.key_.dest_id_;
  const int64_t round_id = piece.key_.round_id_;
  const int64_t piece_id = piece.key_.piece_id_;
  if (OB_FAIL(helper.init(tenant_id))) {
    LOG_WARN("failed to init archive persist helper", K(ret), K(tenant_id));
  } else if (OB_FAIL(helper.get_piece(*ctx_->report_ctx_.sql_proxy_, dest_id, round_id, piece_id, false/*need_lock*/, cur_piece))) {
    LOG_WARN("failed to get pieces by range", K(ret), K(piece));
  } else {
    is_frozen = cur_piece.status_.is_frozen();
  }
  return ret;
}

int ObBackupLSLogTask::get_all_piece_file_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObIArray<ObTenantArchivePieceAttr> &piece_list, const SCN &start_scn, const SCN &end_scn,
    common::ObIArray<ObBackupPieceFile> &piece_file_list)
{
  int ret = OB_SUCCESS;
  piece_file_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < piece_list.count(); ++i) {
    const ObTenantArchivePieceAttr &round_piece = piece_list.at(i);
    ObArray<ObBackupPieceFile> tmp_file_list;
    if (OB_FAIL(inner_get_piece_file_list_(ls_id, round_piece, tmp_file_list))) {
      LOG_WARN("failed to inner get piece file list", K(ret), K(ls_id), K(round_piece));
    } else {
      if (1 == piece_list.count()) {
        int64_t start_file_id = 0;
        int64_t end_file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, start_scn, start_file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(start_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(start_scn));
          }
        } else if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, end_scn, end_file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (start_file_id > end_file_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("start file id should not greater than end file id", K(ret), K(tenant_id), K(ls_id), K(round_piece),
              K(start_scn), K(end_scn), K(start_file_id), K(end_file_id));
        } else if (OB_FAIL(get_file_in_between_(start_file_id, end_file_id, tmp_file_list))) {
          LOG_WARN("failed to get file in between", K(ret), K(start_file_id), K(end_file_id));
        }
      } else if (0 == i) {
        int64_t file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, start_scn, file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (OB_FAIL(filter_file_id_smaller_than_(file_id, tmp_file_list))) {
          LOG_WARN("failed to filter file id smaller than", K(ret), K(file_id));
        }
      } else if (piece_list.count() - 1 == i) {
        int64_t file_id = 0;
        if (OB_FAIL(locate_archive_file_id_by_scn_(round_piece, ls_id, end_scn, file_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            LOG_INFO("path may not exist", K(ret), K(round_piece), K(ls_id), K(end_scn));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to locate archive file id by scn", K(ret), K(round_piece), K(ls_id), K(end_scn));
          }
        } else if (OB_FAIL(filter_file_id_larger_than_(file_id, tmp_file_list))) {
          LOG_WARN("failed to filter file id smaller than", K(ret), K(file_id));
        }
      } else {
      }  // do nothing
    }
    if (FAILEDx(append(piece_file_list, tmp_file_list))) {
      LOG_WARN("failed to append list", K(ret));
    }
  }
  return ret;
}

int ObBackupLSLogTask::inner_get_piece_file_list_(const share::ObLSID &ls_id,
    const ObTenantArchivePieceAttr &piece_attr, common::ObIArray<ObBackupPieceFile> &file_list)
{
  int ret = OB_SUCCESS;
  ObBackupPieceFile piece_file;
  ObBackupIoAdapter util;
  ObBackupPath src_piece_dir_path;
  ObBackupPieceOp op;
  ObArray<int64_t> file_id_list;
  const int64_t dest_id = piece_attr.key_.dest_id_;
  const int64_t round_id = piece_attr.key_.round_id_;
  const int64_t piece_id = piece_attr.key_.piece_id_;
  const share::SCN &start_scn = piece_attr.start_scn_;
  const share::SCN &checkpoint_scn = piece_attr.checkpoint_scn_;
  if (OB_FAIL(get_src_backup_piece_dir_(ls_id, piece_attr, src_piece_dir_path))) {
    LOG_WARN("failed to get src backup piece dir", K(ret), K(round_id), K(piece_id), K(ls_id), K(piece_attr));
  } else if (OB_FAIL(util.adaptively_list_files(src_piece_dir_path.get_obstr(), archive_dest_.get_storage_info(), op))) {
    LOG_WARN("failed to list files", K(ret), K(src_piece_dir_path));
  } else if (OB_FAIL(op.get_file_id_list(file_id_list))) {
    LOG_WARN("failed to get files", K(ret));
  } else {
    ObBackupPieceFile piece_file;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_id_list.count(); ++i) {
      const int64_t file_id = file_id_list.at(i);
      piece_file.reset();
      if (OB_FAIL(piece_file.set(dest_id, round_id, piece_id, ls_id, file_id, start_scn, checkpoint_scn, piece_attr.path_))) {
        LOG_WARN("failed to set piece files", K(ret), K(round_id), K(piece_id));
      } else if (OB_FAIL(file_list.push_back(piece_file))) {
        LOG_WARN("failed to push back", K(ret), K(piece_file));
      }
    }
  }
  return ret;
}

int ObBackupLSLogTask::locate_archive_file_id_by_scn_(
    const ObTenantArchivePieceAttr &piece_attr, const share::ObLSID &ls_id, const SCN &scn, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  const int64_t dest_id = piece_attr.key_.dest_id_;
  const int64_t round_id = piece_attr.key_.round_id_;
  const int64_t piece_id = piece_attr.key_.piece_id_;
  if (OB_FAIL(get_archive_backup_dest_(piece_attr.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(archive::ObArchiveFileUtils::locate_file_by_scn(archive_dest,
      dest_id, round_id, piece_id, ls_id, scn, file_id))) {
    LOG_WARN("failed to locate file by scn", K(ret), K(archive_dest), K(ls_id), K(scn), K(piece_attr));
  } else {
    LOG_INFO("locate archive file id by scn", K(piece_attr), K(ls_id), K(scn), K(file_id));
  }
  return ret;
}

int ObBackupLSLogTask::get_file_in_between_(
    const int64_t start_file_id, const int64_t end_file_id, common::ObIArray<ObBackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupPieceFile &file = list.at(i);
    if (file.file_id_ >= start_file_id && file.file_id_ <= end_file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;
}

int ObBackupLSLogTask::filter_file_id_smaller_than_(
    const int64_t file_id, common::ObIArray<ObBackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupPieceFile &file = list.at(i);
    if (file.file_id_ >= file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;
}

int ObBackupLSLogTask::filter_file_id_larger_than_(
    const int64_t file_id, common::ObIArray<ObBackupPieceFile> &list)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupPieceFile> tmp_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupPieceFile &file = list.at(i);
    if (file.file_id_ <= file_id) {
      if (OB_FAIL(tmp_list.push_back(file))) {
        LOG_WARN("failed to push back", K(ret), K(file));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(tmp_list))) {
      LOG_WARN("failed to assign list", K(ret), K(tmp_list));
    }
  }
  return ret;
}

int ObBackupLSLogTask::get_src_backup_piece_dir_(const share::ObLSID &ls_id,
    const ObTenantArchivePieceAttr &piece_attr, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  if (OB_FAIL(get_archive_backup_dest_(piece_attr.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_ls_log_dir_path(archive_dest,
          piece_attr.key_.dest_id_,
          piece_attr.key_.round_id_,
          piece_attr.key_.piece_id_,
          ls_id,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret), K(ls_id), K(piece_attr));
  }
  return ret;
}

int ObBackupLSLogTask::write_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore store;
  share::ObBackupFormatDesc format_desc;
  bool is_exist = false;
  ObBackupDest new_backup_dest;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      ctx_->backup_dest_, ctx_->backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret));
  } else if (OB_FAIL(store.init(new_backup_dest))) {
    LOG_WARN("fail to init store", K(ret), K(new_backup_dest));
  } else if (OB_FAIL(store.is_format_file_exist(is_exist))) {
    LOG_WARN("fail to check format file exist", K(ret));
  } else if (is_exist) {
    // do not recreate the format file
  } else if (OB_FAIL(generate_format_desc_(format_desc))) {
    LOG_WARN("fail to generate format desc", K(ret));
  } else if (OB_FAIL(store.write_format_file(format_desc))) {
    LOG_WARN("fail to write format file", K(ret), K(format_desc));
  }
  return ret;
}

int ObBackupLSLogTask::generate_format_desc_(share::ObBackupFormatDesc &format_desc)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  share::ObBackupPathString root_path;
  const schema::ObTenantSchema *tenant_schema = nullptr;
  ObBackupDest new_backup_dest;
  const ObBackupDestType::TYPE &dest_type = ObBackupDestType::DEST_TYPE_ARCHIVE_LOG;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      ctx_->backup_dest_, ctx_->backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(ctx_->tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), KPC_(ctx));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant schema", K(ret));
  } else if (OB_FAIL(format_desc.cluster_name_.assign(GCONF.cluster.str()))) {
    LOG_WARN("failed to assign cluster name", K(ret));
  } else if (OB_FAIL(format_desc.tenant_name_.assign(tenant_schema->get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", K(ret));
  } else if (OB_FAIL(new_backup_dest.get_backup_path_str(format_desc.path_.ptr(), format_desc.path_.capacity()))) {
    LOG_WARN("failed to get backup path", K(ret));
  } else {
    format_desc.tenant_id_ = ctx_->tenant_id_;
    format_desc.incarnation_ = OB_START_INCARNATION;
    format_desc.dest_id_ = ctx_->dest_id_;
    format_desc.dest_type_ = dest_type;
    format_desc.cluster_id_ = GCONF.cluster_id;
  }
  return ret;
}

int ObBackupLSLogTask::transform_and_copy_meta_file_(const ObTenantArchivePieceAttr &piece_attr)
{
  int ret = OB_SUCCESS;
  ObBackupDest src;
  ObBackupDest dest;
  ObArchiveStore src_store;
  ObArchiveStore dest_store;
  const share::ObLSID &ls_id = ls_id_;
  if (OB_FAIL(get_copy_src_and_dest_(piece_attr, src, dest))) {
    LOG_WARN("failed to get copy src and dest", K(ret), K(piece_attr));
  } else if (OB_FAIL(src_store.init(src))) {
    LOG_WARN("failed to init src store", K(ret), K(src));
  } else if (OB_FAIL(dest_store.init(dest))) {
    LOG_WARN("failed to init dest store", K(ret), K(dest));
  } else if (OB_FAIL(copy_ls_file_info_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy ls file info", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_piece_file_info_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy piece file info", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_single_piece_info_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy single piece info", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_tenant_archive_piece_infos_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy tenant archive piece infos", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_checkpoint_info_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy checkpoint info", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_round_start_file_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy round start file", K(ret), K(src), K(dest), K(piece_attr));
  } else if (OB_FAIL(copy_round_end_file_(piece_attr, src_store, dest_store))) {
    LOG_WARN("failed to copy round end file", K(ret), K(src), K(dest), K(piece_attr));
  }
  return ret;
}

int ObBackupLSLogTask::copy_ls_file_info_(
    const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  const share::ObLSID &ls_id = ls_id_;
  ObSingleLSInfoDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_single_ls_info_file_exist(src_dest_id, round_id, piece_id, ls_id, is_exist))) {
    LOG_WARN("failed to check is single ls info file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_single_ls_info(src_dest_id, round_id, piece_id, ls_id, desc))) {
    LOG_WARN("failed to read single ls info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_single_ls_info(dest_dest_id, round_id, piece_id, ls_id, desc))) {
    LOG_WARN("failed to write single ls info", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_piece_file_info_(
    const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObPieceInfoDesc piece_info_desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_piece_info_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is piece info file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_info(src_dest_id, round_id, piece_id, piece_info_desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_info(dest_dest_id, round_id, piece_id, piece_info_desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_single_piece_info_(
    const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObSinglePieceDesc single_piece_desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_single_piece_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is single piece file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_single_piece(src_dest_id, round_id, piece_id, single_piece_desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_single_piece(dest_dest_id, round_id, piece_id, single_piece_desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_tenant_archive_piece_infos_(
    const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObTenantArchivePieceInfosDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_tenant_archive_piece_infos_file_exist(src_dest_id, round_id, piece_id, is_exist))) {
    LOG_WARN("failed to check is tenant archive piece infos file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_tenant_archive_piece_infos(src_dest_id, round_id, piece_id, desc))) {
    LOG_WARN("failed to read piece info", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_tenant_archive_piece_infos(dest_dest_id, round_id, piece_id, desc))) {
    LOG_WARN("failed to write piece info", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_checkpoint_info_(const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObPieceCheckpointDesc desc;
  bool is_exist = false;
  const int64_t file_id = 0;
  const share::SCN old_ckpt_scn = SCN::min_scn(); //set 0, but will not delete
  if (OB_FAIL(src_store.is_piece_checkpoint_file_exist(src_dest_id, round_id, piece_id, file_id, is_exist))) {
    LOG_WARN("failed to check is piece checkpoint file file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_checkpoint(src_dest_id, round_id, piece_id, file_id, desc))) {
    LOG_WARN("failed to read piece checkpoint", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_checkpoint(dest_dest_id, round_id, piece_id, file_id, old_ckpt_scn, desc))) {
    LOG_WARN("failed to write piece checkpoint", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_round_start_file_(const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObRoundStartDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_round_start_file_exist(src_dest_id, round_id, is_exist))) {
    LOG_WARN("failed to check is round start file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_round_start(src_dest_id, round_id, desc))) {
    LOG_WARN("failed to read round start", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_round_start(dest_dest_id, round_id, desc))) {
    LOG_WARN("failed to write round start", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_round_end_file_(const ObTenantArchivePieceAttr &piece_file, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store)
{
  int ret = OB_SUCCESS;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  ObRoundEndDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.is_round_end_file_exist(src_dest_id, round_id, is_exist))) {
    LOG_WARN("failed to check is round end file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_round_end(src_dest_id, round_id, desc))) {
    LOG_WARN("failed to read round end", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_round_end(dest_dest_id, round_id, desc))) {
    LOG_WARN("failed to write round end", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_piece_start_file_(const ObTenantArchivePieceAttr &piece_file, const share::ObBackupDest &src, const share::ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  ObArchiveStore src_store;
  ObArchiveStore dest_store;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  const share::SCN &create_scn = piece_file.start_scn_;
  ObPieceStartDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.init(src))) {
    LOG_WARN("failed to init src store", K(ret), K(src));
  } else if (OB_FAIL(dest_store.init(dest))) {
    LOG_WARN("failed to init dest store", K(ret), K(dest));
  } else if (OB_FAIL(src_store.is_piece_start_file_exist(src_dest_id, round_id, piece_id, create_scn, is_exist))) {
    LOG_WARN("failed to check is piece start file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_start(src_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to read round start", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_start(dest_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to write round start", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::copy_piece_end_file_(const ObTenantArchivePieceAttr &piece_file, const share::ObBackupDest &src, const share::ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  ObArchiveStore src_store;
  ObArchiveStore dest_store;
  const int64_t src_dest_id = piece_file.key_.dest_id_;
  const int64_t dest_dest_id = ctx_->dest_id_;
  const int64_t round_id = piece_file.key_.round_id_;
  const int64_t piece_id = piece_file.key_.piece_id_;
  const share::SCN &create_scn = piece_file.checkpoint_scn_;
  ObPieceEndDesc desc;
  bool is_exist = false;
  if (OB_FAIL(src_store.init(src))) {
    LOG_WARN("failed to init src store", K(ret), K(src));
  } else if (OB_FAIL(dest_store.init(dest))) {
    LOG_WARN("failed to init dest store", K(ret), K(dest));
  } else if (OB_FAIL(src_store.is_piece_end_file_exist(src_dest_id, round_id, piece_id, create_scn, is_exist))) {
    LOG_WARN("failed to check is piece end file exist", K(ret), K(src_dest_id), K(round_id), K(piece_id));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(src_store.read_piece_end(src_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to read piece end", K(ret), K(piece_file));
  } else if (OB_FAIL(dest_store.write_piece_end(dest_dest_id, round_id, piece_id, create_scn, desc))) {
    LOG_WARN("failed to write piece end", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::get_archive_backup_dest_(
    const ObBackupPathString &path, share::ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(
      *ctx_->report_ctx_.sql_proxy_, ctx_->tenant_id_, path, archive_dest))) {
    LOG_WARN("failed to get archive dest", K(ret), K(path));
  } else if (OB_FAIL(archive_dest_.deep_copy(archive_dest))) {
    LOG_WARN("failed to deep copy archive dest", K(ret));
  } else {
    LOG_INFO("succ get backup dest", K(path));
  }
  return ret;
}

int ObBackupLSLogTask::get_copy_src_and_dest_(
    const ObTenantArchivePieceAttr &piece_file, share::ObBackupDest &src, share::ObBackupDest &dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_archive_backup_dest_(piece_file.path_, src))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      ctx_->backup_dest_, ctx_->backup_set_desc_, dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_complement_log", "backup_ls_log_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "turn_id", ctx_->turn_id_,
                     "retry_id", ctx_->retry_id_,
                     "is_only_calc_stat", ctx_->is_only_calc_stat_);
  }
  return ret;
}

// ObBackupLSLogFileTask

ObBackupLSLogFileTask::ObBackupLSLogFileTask()
  : ObITask(ObITask::ObITaskType::TASK_TYPE_BACKUP_LS_LOG_FILE),
    is_inited_(false),
    ls_id_(),
    ctx_(NULL),
    finish_task_(NULL),
    backup_piece_file_(),
    last_active_time_(),
    bandwidth_throttle_(),
    archive_dest_()
{
}

ObBackupLSLogFileTask::~ObBackupLSLogFileTask()
{
}

int ObBackupLSLogFileTask::init(const share::ObLSID &ls_id, common::ObInOutBandwidthThrottle *bandwidth_throttle,
    ObBackupComplementLogCtx *ctx, ObBackupLSLogFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is not init", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(bandwidth_throttle) || OB_ISNULL(ctx) || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), KP(bandwidth_throttle), KP(ctx), KP(finish_task));
  } else if (OB_FAIL(build_backup_piece_file_info_(finish_task))) {
    LOG_WARN("failed to build backup piece file info", K(ret), KP(finish_task));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    finish_task_ = finish_task;
    bandwidth_throttle_ = bandwidth_throttle;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogFileTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup file copy task is not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->is_failed()) {
    ret = OB_CANCELED;
    LOG_WARN("ctx already failed", K(ret));
  } else if (OB_FAIL(inner_process_(backup_piece_file_))) {
    LOG_WARN("failed to inner process", K(ret), K_(backup_piece_file));
  } else {
    LOG_INFO("inner process backup log file", K_(backup_piece_file));
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(deal_with_fo(ctx_, ret))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret));
    }
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupLSLogFileTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = NULL;
  ObBackupLSLogFileTask *tmp_task = NULL;
  bool is_iter_end = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(finish_task_->check_is_iter_end(is_iter_end))) {
    LOG_WARN("failed to check is iter end", K(ret));
  } else if (is_iter_end) {
    ret = OB_ITER_END;
    LOG_WARN("backup ls file meet end", K_(ls_id));
  } else if (OB_FAIL(dag_->alloc_task(tmp_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_task->init(ls_id_, bandwidth_throttle_, ctx_, finish_task_))) {
    LOG_WARN("failed to init next task", K(ret), K_(ls_id), KPC_(ctx), KPC_(finish_task));
  } else {
    next_task = tmp_task;
    tmp_task = NULL;
  }
  return ret;
}

int ObBackupLSLogFileTask::build_backup_piece_file_info_(ObBackupLSLogFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(finish_task));
  } else if (OB_FAIL(finish_task->get_copy_file_info(backup_piece_file_))) {
    LOG_WARN("failed to get copy file info", K(ret));
  }
  return ret;
}

int ObBackupLSLogFileTask::inner_process_(const ObBackupPieceFile &piece_file)
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path;
  ObBackupPath dst_path;
  ObBackupIoAdapter util;
  if (OB_FAIL(get_src_backup_file_path_(piece_file, src_path))) {
    LOG_WARN("failed to get src backup file path", K(ret), K(piece_file));
  } else if (OB_FAIL(get_dst_backup_file_path_(piece_file, dst_path))) {
    LOG_WARN("failed to get dst backup file path", K(ret), K(piece_file));
  } else if (OB_FAIL(util.mk_parent_dir(dst_path.get_obstr(), ctx_->backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to mk parent dir", K(ret), K(dst_path), KPC_(ctx));
  } else if (OB_FAIL(inner_backup_complement_log_(src_path, dst_path))) {
    LOG_WARN("failed to inner backup complement log", K(ret), K(src_path), K(dst_path));
  } else if (OB_FAIL(report_progress_())) {
    LOG_WARN("failed to make progress", K(ret));
  } else {
    SERVER_EVENT_ADD("backup", "backup_ls_log_file_task",
                    "tenant_id", ctx_->tenant_id_,
                    "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                    "dest_id", piece_file.dest_id_,
                    "round_id", piece_file.round_id_,
                    "piece_id", piece_file.piece_id_,
                    "file_id", piece_file.file_id_);
    LOG_INFO("inner backup complement log", K(piece_file), K(src_path), K(dst_path));
  }
  return ret;
}

int ObBackupLSLogFileTask::inner_backup_complement_log_(
    const ObBackupPath &src_path, const ObBackupPath &dst_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_clog_file_(src_path, dst_path))) {
    LOG_WARN("failed to transfer clog file", K(ret), K(src_path), K(dst_path));
  } else {
    LOG_INFO("backup complement log", K(src_path), K(dst_path));
  }
  return ret;
}

int ObBackupLSLogFileTask::get_src_backup_file_path_(
    const ObBackupPieceFile &piece_file, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest archive_dest;
  if (OB_FAIL(get_archive_backup_dest_(piece_file.path_, archive_dest))) {
    LOG_WARN("failed to get archive backup dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(archive_dest,
          piece_file.dest_id_,
          piece_file.round_id_,
          piece_file.piece_id_,
          piece_file.ls_id_,
          piece_file.file_id_,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret), K(archive_dest));
  }
  return ret;
}

int ObBackupLSLogFileTask::get_archive_backup_dest_(
    const ObBackupPathString &path, share::ObBackupDest &archive_dest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(
      *ctx_->report_ctx_.sql_proxy_, ctx_->tenant_id_, path, archive_dest))) {
    LOG_WARN("failed to get archive dest", K(ret), K(path));
  } else if (OB_FAIL(archive_dest_.deep_copy(archive_dest))) {
    LOG_WARN("failed to deep copy archive dest", K(ret));
  } else {
    LOG_INFO("succ get backup dest", K(path));
  }
  return ret;
}

int ObBackupLSLogFileTask::get_dst_backup_file_path_(
    const ObBackupPieceFile &piece_file, share::ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  ObBackupDest new_backup_dest;
  if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(
      ctx_->backup_dest_, ctx_->backup_set_desc_, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(piece_file));
  } else if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(new_backup_dest,
          ctx_->dest_id_,
          piece_file.round_id_,
          piece_file.piece_id_,
          piece_file.ls_id_,
          piece_file.file_id_,
          backup_path))) {
    LOG_WARN("failed to get ls log archive path", K(ret));
  }
  return ret;
}

int ObBackupLSLogFileTask::transfer_clog_file_(const ObBackupPath &src_path, const ObBackupPath &dst_path)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t dst_len = 0;
  int64_t transfer_len = 0;
  ObIOFd fd;
  ObBackupIoAdapter util;
  ObIODevice *device_handle = NULL;
  ObStorageIdMod mod;
  mod.storage_id_ = ctx_->dest_id_;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
  if (OB_FAIL(util.open_with_access_type(
          device_handle, fd, ctx_->backup_dest_.get_storage_info(), dst_path.get_obstr(), OB_STORAGE_ACCESS_MULTIPART_WRITER, mod))) {
    LOG_WARN("failed to open with access type", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inner_transfer_clog_file_(src_path, dst_path, device_handle, fd, dst_len, transfer_len))) {
        LOG_WARN("failed to inner transfer clog file", K(ret), K(src_path), K(dst_path));
      } else {
        dst_len += transfer_len;
      }
      if (0 == transfer_len) { //at this point, last part is still held in memory
        LOG_INFO("transfer ended", K(ret), K(src_path), K(dst_path));
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(device_handle->complete(fd))) {
        LOG_WARN("fail to complete multipart upload", K(ret), K(device_handle), K(fd));
      }
    } else {
      if (OB_NOT_NULL(device_handle) && OB_TMP_FAIL(device_handle->abort(fd))) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("fail to abort multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd));
      }
    }
    if (OB_SUCCESS != (tmp_ret = util.close_device_and_fd(device_handle, fd))) {
      LOG_WARN("fail to close file", K(ret), K(dst_path));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObBackupLSLogFileTask::inner_transfer_clog_file_(const ObBackupPath &src_path, const ObBackupPath &dst_path,
    ObIODevice *&device_handle, ObIOFd &fd, const int64_t dst_len, int64_t &transfer_len)
{
  int ret = OB_SUCCESS;
  transfer_len = 0;
  ObBackupIoAdapter util;
  int64_t write_size = -1;
  ObArenaAllocator allocator;
  int64_t src_len = 0;
  char *buf = NULL;
  int64_t read_len = 0;
  if (OB_FAIL(get_file_length_(src_path.get_obstr(), archive_dest_.get_storage_info(), src_len))) {
    LOG_WARN("failed to get file length", K(ret), K(src_path));
  } else if (dst_len == src_len) {
    transfer_len = 0;
  } else if (dst_len > src_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dst len should not be greater then src len", K(ret), K(src_len), K(dst_len));
  } else if (OB_FAIL(get_transfer_length_(src_len - dst_len, transfer_len))) {
    LOG_WARN("failed to get transfer len", K(ret), K(src_len), K(dst_len));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(transfer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(transfer_len));
  } else if (OB_FAIL(util.adaptively_read_part_file(src_path.get_obstr(), archive_dest_.get_storage_info(), buf, transfer_len, dst_len, read_len,
      ObStorageIdMod::get_default_backup_id_mod()))) {
    LOG_WARN("failed to read part file", K(ret), K(src_path));
  } else if (read_len != transfer_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read len not expected", K(ret), K(read_len), K(transfer_len));
  } else if (OB_FAIL(device_handle->pwrite(fd, dst_len, transfer_len, buf, write_size))) {
    LOG_WARN("failed to write multipart upload file", K(ret));
  } else if (OB_FAIL(bandwidth_throttle_->limit_out_and_sleep(write_size, last_active_time_, INT64_MAX))) {
    LOG_WARN("failed to limit out and sleep", K(ret));
  } else {
    last_active_time_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObBackupLSLogFileTask::get_transfer_length_(const int64_t delta_len, int64_t &transfer_len)
{
  int ret = OB_SUCCESS;
  transfer_len = 0;
  const int64_t MAX_PART_FILE_SIZE = 2 * 1024 * 1024;  // 2MB
  if (delta_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(delta_len));
  } else {
    transfer_len = std::min(delta_len, MAX_PART_FILE_SIZE);
  }
  return ret;
}

int ObBackupLSLogFileTask::get_file_length_(
    const common::ObString &file_path, const share::ObBackupStorageInfo *storage_info, int64_t &length)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  if (OB_FAIL(util.adaptively_get_file_length(file_path, storage_info, length))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      length = 0;
    } else {
      LOG_WARN("failed to get file length", K(ret), K(file_path));
    }
  }
  return ret;
}

int ObBackupLSLogFileTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_complement_log", "backup_ls_file_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "round_id", backup_piece_file_.round_id_,
                     "piece_id", backup_piece_file_.piece_id_,
                     "is_only_calc_stat", ctx_->is_only_calc_stat_,
                     backup_piece_file_.file_id_);
  }
  return ret;
}

int ObBackupLSLogFileTask::report_progress_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  const bool for_update = true;
  if (OB_FAIL(trans.start(ctx_->report_ctx_.sql_proxy_, gen_meta_tenant_id(ctx_->tenant_id_)))) {
    LOG_WARN("failed to start transaction", K(ret));
  } else {
    const int64_t job_id = ctx_->job_desc_.job_id_;
    const int64_t task_id = ctx_->job_desc_.task_id_;
    const uint64_t tenant_id = ctx_->tenant_id_;
    const share::ObLSID &ls_id = ls_id_;
    share::ObBackupSetTaskAttr old_set_task_attr;
    share::ObBackupLSTaskAttr old_ls_task_attr;
    share::ObBackupStats new_ls_task_stat;
    share::ObBackupStats new_backup_set_stats;
    if (OB_FAIL(ObBackupTaskOperator::get_backup_task(trans, job_id, tenant_id, for_update, old_set_task_attr))) {
      LOG_WARN("failed to get backup task", K(ret));
    } else if (OB_FAIL(ObBackupLSTaskOperator::get_ls_task(trans, for_update,
              task_id, tenant_id, ls_id, old_ls_task_attr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_WARN("ls task do not exist, should be newly created ls", K(ret), K(ls_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get ls task", K(ret), K(task_id), K(tenant_id), K(ls_id));
      }
    } else if (OB_FALSE_IT(new_backup_set_stats = old_set_task_attr.stats_)) {
    } else if (OB_FALSE_IT(new_backup_set_stats.finish_log_file_count_++)) {
    } else if (OB_FALSE_IT(new_ls_task_stat = old_ls_task_attr.stats_)) {
    } else if (OB_FALSE_IT(new_ls_task_stat.finish_log_file_count_++)) {
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_stats(trans, task_id, tenant_id, ls_id, new_ls_task_stat))) {
      LOG_WARN("failed to update stat", K(ret));
    } else if (OB_FAIL(ObBackupTaskOperator::update_stats(trans, task_id, tenant_id, new_backup_set_stats))) {
      LOG_WARN("failed to update stats", K(ret), K(task_id), K(tenant_id));
    } else {
      LOG_INFO("make progress", K(new_backup_set_stats), K(new_ls_task_stat));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", K(ret));
      }
    } else {
      if (OB_TMP_FAIL(trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}



// ObBackupLSLogFinishTask

ObBackupLSLogFinishTask::ObBackupLSLogFinishTask()
  : ObITask(ObITask::ObITaskType::TASK_TYPE_BACKUP_LS_LOG_FINISH),
    is_inited_(false),
    mutex_(),
    idx_(),
    ctx_(NULL),
    file_list_()
{
}

ObBackupLSLogFinishTask::~ObBackupLSLogFinishTask()
{
}

int ObBackupLSLogFinishTask::init(const share::ObLSID &ls_id,
    const common::ObIArray<ObBackupPieceFile> &file_list, ObBackupComplementLogCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup ls log copy finish task init twice", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(ls_id), KP(ctx));
  } else if (OB_FAIL(file_list_.assign(file_list))) {
    LOG_WARN("failed to assign file list", K(ret), K(file_list));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls log copy finish task do not init", K(ret));
  } else {
    // do nothing
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupLSLogFinishTask::check_is_iter_end(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  ObMutexGuard guard(mutex_);
  if (idx_ == file_list_.count()) {
    is_iter_end = true;
  } else {
    is_iter_end = false;
  }
  return ret;
}

int ObBackupLSLogFinishTask::get_copy_file_info(ObBackupPieceFile &piece_file)
{
  int ret = OB_SUCCESS;
  piece_file.reset();
  ObMutexGuard guard(mutex_);
  if (idx_ == file_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("backup ls finish task reach end", K_(idx), K_(file_list));
  } else {
    piece_file = file_list_.at(idx_);
    idx_++;
    LOG_INFO("succeed get backup piece file info", K_(idx), K(piece_file));
  }
  return ret;
}

int ObBackupLSLogFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_complement_log", "backup_ls_log_finish_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "turn_id", ctx_->turn_id_,
                     "retry_id", ctx_->retry_id_,
                     "is_only_calc_stat", ctx_->is_only_calc_stat_);
  }
  return ret;
}

// ObBackupLSLogGroupFinishDag

ObBackupLSLogGroupFinishDag::ObBackupLSLogGroupFinishDag()
    : share::ObIDag(ObDagType::DAG_TYPE_BACKUP_LS_LOG_GROUP_FINISH),
      is_inited_(false),
      ctx_(NULL)
{}

ObBackupLSLogGroupFinishDag::~ObBackupLSLogGroupFinishDag()
{}

int ObBackupLSLogGroupFinishDag::init(ObBackupComplementLogCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls backup complement log task init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), KP(ctx));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogGroupFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupLSLogGroupFinishTask *task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls group dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(ctx_))) {
    LOG_WARN("failed to init task", K(ret), K_(ctx));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add task", K(ret));
  } else {
    LOG_INFO("success to add backup ls log group finish task", K(ret), KPC(this), KPC(task));
  }
  return ret;
}

int ObBackupLSLogGroupFinishDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup complement log dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  static_cast<int64_t>(ctx_->tenant_id_), ctx_->backup_set_desc_.backup_set_id_,
                                  ctx_->ls_id_.id()))){
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObBackupLSLogGroupFinishDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(ctx_->ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), KPC_(ctx));
  }
  return ret;
}

bool ObBackupLSLogGroupFinishDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupLSLogGroupFinishDag &other_dag = static_cast<const ObBackupLSLogGroupFinishDag &>(other);
    bret = ctx_->job_desc_ == other_dag.ctx_->job_desc_
        && ctx_->backup_dest_ == other_dag.ctx_->backup_dest_
        && ctx_->tenant_id_ == other_dag.ctx_->tenant_id_
        && ctx_->dest_id_ == other_dag.ctx_->dest_id_
        && ctx_->backup_set_desc_ == other_dag.ctx_->backup_set_desc_
        && ctx_->ls_id_ == other_dag.ctx_->ls_id_;
  }
  return bret;
}

int64_t ObBackupLSLogGroupFinishDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("backup ls group dag do not init", K(ret));
  } else {
    int64_t type = get_type();
    hash_value = common::murmurhash(&type, sizeof(type), hash_value);
    hash_value = ctx_->calc_hash(hash_value);
  }
  return hash_value;
}

// ObBackupLSLogGroupFinishTask

ObBackupLSLogGroupFinishTask::ObBackupLSLogGroupFinishTask()
  : ObITask(ObITask::ObITaskType::TASK_TYPE_BACKUP_LS_LOG_GROUP_FINISH),
    is_inited_(false),
    ctx_(NULL)
{
}

ObBackupLSLogGroupFinishTask::~ObBackupLSLogGroupFinishTask()
{
}

int ObBackupLSLogGroupFinishTask::init(ObBackupComplementLogCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup ls log copy finish task init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), KP(ctx));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupLSLogGroupFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup ls log copy finish task do not init", K(ret));
  } else {
    // do nothing
  }
  if (OB_TMP_FAIL(report_task_result_())) {
    LOG_WARN("failed to report task result", K(tmp_ret), K(ret));
  }
  if (OB_TMP_FAIL(record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObBackupLSLogGroupFinishTask::report_task_result_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group ctx should not be null", K(ret));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else if (OB_FAIL(ObBackupUtils::report_task_result(ctx_->job_desc_.job_id_,
                                                       ctx_->job_desc_.task_id_,
                                                       ctx_->tenant_id_,
                                                       ctx_->ls_id_,
                                                       ctx_->turn_id_,
                                                       ctx_->retry_id_,
                                                       ctx_->job_desc_.trace_id_,
                                                       this->get_dag()->get_dag_id(),
                                                       result,
                                                       ctx_->report_ctx_))) {
    LOG_WARN("failed to report task result", K(ret));
  }
  return ret;
}

int ObBackupLSLogGroupFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("backup_complement_log", "backup_ls_log_group_finish_task",
                     "tenant_id", ctx_->tenant_id_,
                     "backup_set_id", ctx_->backup_set_desc_.backup_set_id_,
                     "ls_id", ctx_->ls_id_.id(),
                     "turn_id", ctx_->turn_id_,
                     "retry_id", ctx_->retry_id_,
                     "is_only_calc_stat", ctx_->is_only_calc_stat_);
  }
  return ret;
}

}
}
