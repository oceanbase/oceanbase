// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_instance.h"
#include "share/table/ob_table_load_define.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace table;
using namespace storage;
namespace observer
{
ObTableLoadInstance::ObTableLoadInstance()
  : allocator_(nullptr),
    execute_ctx_(nullptr),
    table_ctx_(nullptr),
    coordinator_(nullptr),
    job_stat_(nullptr),
    px_mode_(false),
    online_opt_stat_gather_(false),
    sql_mode_(0),
    is_inited_(false)
{
}

ObTableLoadInstance::~ObTableLoadInstance()
{
  destroy();
}

int ObTableLoadInstance::init(ObTableLoadParam &param,
    const oceanbase::common::ObIArray<int64_t> &idx_array,
    ObTableLoadExecCtx *execute_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadInstance init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == execute_ctx->exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(execute_ctx));
  } else {
    bool is_new = false;
    allocator_ = execute_ctx->allocator_;
    execute_ctx_ = execute_ctx;
    px_mode_ = param.px_mode_;
    online_opt_stat_gather_ = param.online_opt_stat_gather_;
    sql_mode_ = param.sql_mode_;
    ObTableLoadSegmentID segment_id(DEFAULT_SEGMENT_ID);
    if (OB_FAIL(param.normalize())) {
      LOG_WARN("fail to normalize param", KR(ret));
    } else if (OB_FAIL(next_sequence_no_array_.create(param.session_count_, *allocator_))) {
      LOG_WARN("fail to create next sequence no array", KR(ret));
    }
    // create table ctx
    else if (OB_FAIL(ObTableLoadService::create_ctx(param, table_ctx_, is_new))) {
      LOG_WARN("fail to create table load ctx", KR(ret), K(param));
    } else if (OB_UNLIKELY(!is_new)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("table load ctx exists", KR(ret));
    } else if (OB_FAIL(
                 ObTableLoadCoordinator::init_ctx(table_ctx_, idx_array, execute_ctx_->exec_ctx_->get_my_session()))) {
      LOG_WARN("fail to coordinator init ctx", KR(ret));
    }
    // new coordinator
    else if (OB_ISNULL(coordinator_ = OB_NEWx(ObTableLoadCoordinator, allocator_, table_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadCoordinator", KR(ret));
    } else if (OB_FAIL(coordinator_->init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    }
    // begin
    else if (OB_FAIL(coordinator_->begin())) {
      LOG_WARN("fail to coordinator begin", KR(ret));
    }
    // start trans
    else if (!px_mode_ && OB_FAIL(coordinator_->start_trans(segment_id, trans_id_))) {
      LOG_WARN("fail to coordinator start trans", KR(ret));
    }
    // init succ
    else {
      for (int64_t i = 0; i < param.session_count_; ++i) {
        next_sequence_no_array_[i] = 1;
      }
      job_stat_ = table_ctx_->job_stat_;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObTableLoadInstance::destroy()
{
  if (nullptr != coordinator_) {
    if (coordinator_->is_valid()) {
      ObTableLoadCoordinator::abort_ctx(table_ctx_);
    }
    coordinator_->~ObTableLoadCoordinator();
    allocator_->free(coordinator_);
    coordinator_ = nullptr;
  }
  if (nullptr != table_ctx_) {
    ObTableLoadService::remove_ctx(table_ctx_);
    ObTableLoadService::put_ctx(table_ctx_);
    table_ctx_ = nullptr;
    job_stat_ = nullptr;
  }
}

int ObTableLoadInstance::check_trans_committed()
{
  int ret = OB_SUCCESS;
  ObTableLoadTransStatusType trans_status = ObTableLoadTransStatusType::NONE;
  int error_code = OB_SUCCESS;
  while (OB_SUCC(ret) && ObTableLoadTransStatusType::COMMIT != trans_status &&
         OB_SUCC(execute_ctx_->check_status())) {
    if (OB_FAIL(coordinator_->get_trans_status(trans_id_, trans_status, error_code))) {
      LOG_WARN("fail to coordinator get trans status", KR(ret));
    } else{
      switch (trans_status) {
        case ObTableLoadTransStatusType::FROZEN:
          usleep(WAIT_INTERVAL_US);
          break;
        case ObTableLoadTransStatusType::COMMIT:
          break;
        case ObTableLoadTransStatusType::ERROR:
          ret = error_code;
          LOG_WARN("trans has error", KR(ret));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected trans status", KR(ret), K(trans_status));
          break;
      }
    }
  }
  return ret;
}

int ObTableLoadInstance::check_merged()
{
  int ret = OB_SUCCESS;
  ObTableLoadStatusType status = ObTableLoadStatusType::NONE;
  int error_code = OB_SUCCESS;
  while (OB_SUCC(ret) && ObTableLoadStatusType::MERGED != status &&
         OB_SUCC(execute_ctx_->check_status())) {
    if (OB_FAIL(coordinator_->get_status(status, error_code))) {
      LOG_WARN("fail to coordinator get status", KR(ret));
    } else {
      switch (status) {
        case ObTableLoadStatusType::FROZEN:
        case ObTableLoadStatusType::MERGING:
          usleep(WAIT_INTERVAL_US);
          break;
        case ObTableLoadStatusType::MERGED:
          break;
        case ObTableLoadStatusType::ERROR:
          ret = error_code;
          LOG_WARN("table load has error", KR(ret));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status", KR(ret), K(status));
          break;
      }
    }
  }
  return ret;
}

// commit() = px_commit_data() + px_commit_ddl()
// used in non px_mode
int ObTableLoadInstance::commit(ObTableLoadResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    // finish trans
    if (OB_FAIL(coordinator_->finish_trans(trans_id_))) {
      LOG_WARN("fail to finish trans", KR(ret));
    }
    // wait trans commit
    else if (OB_FAIL(check_trans_committed())) {
      LOG_WARN("fail to check trans committed", KR(ret));
    }
    // finish
    else if (OB_FAIL(coordinator_->finish())) {
      LOG_WARN("fail to finish", KR(ret));
    }
    // wait merge
    else if (OB_FAIL(check_merged())) {
      LOG_WARN("fail to check merged", KR(ret));
    }
    // commit
    else if (OB_FAIL(coordinator_->commit(*execute_ctx_->exec_ctx_, result_info))) {
      LOG_WARN("fail to commit", KR(ret));
    }
    else {
      // Setting coordinator_ to NULL to mark a normal termination
      coordinator_->~ObTableLoadCoordinator();
      allocator_->free(coordinator_);
      coordinator_ = nullptr;
    }
  }
  return ret;
}

// used in insert /*+ append */ into select clause
int ObTableLoadInstance::px_commit_data()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    // finish
    if (OB_FAIL(coordinator_->finish())) {
      LOG_WARN("fail to finish", KR(ret));
    }
    // wait merge
    else if (OB_FAIL(check_merged())) {
      LOG_WARN("fail to check merged", KR(ret));
    }
    // commit
    else if (OB_FAIL(coordinator_->px_commit_data(*execute_ctx_->exec_ctx_))) {
      LOG_WARN("fail to do px_commit_data", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadInstance::px_commit_ddl()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(coordinator_->px_commit_ddl())) {
      LOG_WARN("fail to do px_commit_ddl", KR(ret));
    } else {
      // Setting coordinator_ to NULL to mark a normal termination
      coordinator_->~ObTableLoadCoordinator();
      allocator_->free(coordinator_);
      coordinator_ = nullptr;
    }
  }
  return ret;
}

int ObTableLoadInstance::write(int32_t session_id,
                                const table::ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 0 || session_id > table_ctx_->param_.session_count_ ||
                         obj_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(obj_rows.count()));
  } else {
    uint64_t &next_sequence_no = next_sequence_no_array_[session_id - 1];
    if (OB_FAIL(coordinator_->write(trans_id_, session_id, next_sequence_no++, obj_rows))) {
      LOG_WARN("fail to write coordinator", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
