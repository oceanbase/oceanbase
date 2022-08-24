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

#define USING_LOG_PREFIX SERVER
#include "ob_table_ttl_task.h"
#include "ob_table_ttl_common.h"
#include "ob_table_ttl_manager.h"
#include "ob_table_service.h"
#include "share/table/ob_table.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::sql;
using namespace oceanbase::transaction;
using namespace oceanbase::observer;
using namespace oceanbase::storage;
using namespace oceanbase::share;


/**
 * ---------------------------------------- ObTableTTLDeleteTask ----------------------------------------
 */
ObTableTTLDeleteTask::ObTableTTLDeleteTask():
    ObITask(ObITaskType::TASK_TYPE_TTL_DELETE), is_inited_(false), param_(NULL), info_(NULL),
    allocator_(ObModIds::TABLE_PROC), trans_state_(), trans_desc_(),
    participants_leaders_(), part_epoch_list_()
{}

ObTableTTLDeleteTask::~ObTableTTLDeleteTask()
{}

int ObTableTTLDeleteTask::init(const ObTTLPara &ttl_para, ObTTLTaskInfo &ttl_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ttl ctx twice", KR(ret));
  } else if (OB_UNLIKELY(!ttl_para.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl para", KR(ret), K(ttl_para));
  } else if (OB_UNLIKELY(!ttl_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl info", KR(ret), K(ttl_info));
  } else {
    // todo@dazhi: maybe move to table service
    if (!ttl_info.row_key_.empty()) {
      ObRowkey rowkey;
      int64_t pos = 0;
      if (OB_FAIL(rowkey.deserialize(allocator_, ttl_info.row_key_.ptr(), ttl_info.row_key_.length(), pos))) {
        LOG_WARN("fail to deserialize rowkey",  K(ret), K(ttl_info.row_key_));
      } else if (rowkey.get_obj_cnt() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(rowkey), K(ttl_info.row_key_));
      } else {
        first_key_ = rowkey.get_obj_ptr()[0].get_string();
        second_key_ = rowkey.get_obj_ptr()[1].get_string();
      }
    } else {
      first_key_.reset();
      second_key_.reset();
    }

    if (OB_SUCC(ret)) {
      param_ = &ttl_para;
      info_ = &ttl_info;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableTTLDeleteTask::start_trans()
{
  int ret = OB_SUCCESS;
  NG_TRACE(T_start_trans_begin);
  ObPartitionKey pkey = info_->pkey_;
  uint64_t tenant_id = pkey.get_tenant_id();
  const int64_t trans_timeout_ts = ONE_TASK_TIMEOUT + ObTimeUtility::current_time();
  const int64_t trans_consistency_level = ObTransConsistencyLevel::STRONG;
  const int32_t trans_consistency_type = ObTransConsistencyType::CURRENT_READ;
  storage::ObPartitionService* partition_service = GCTX.par_ser_;
  if (OB_ISNULL(partition_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null partition service", K(ret));
  } else if (OB_FAIL(participants_leaders_.push(pkey, GCTX.self_addr_))) {
    LOG_WARN("fail to add leader address", K(ret));
  }

  // 1. start transaction
  if (OB_SUCC(ret)) {
    transaction::ObStartTransParam start_trans_param;
    int32_t access_mode = transaction::ObTransAccessMode::READ_WRITE;
    start_trans_param.set_access_mode(access_mode);
    start_trans_param.set_type(transaction::ObTransType::TRANS_USER);
    start_trans_param.set_isolation(transaction::ObTransIsolation::READ_COMMITED);
    start_trans_param.set_autocommit(true);
    start_trans_param.set_consistency_type(trans_consistency_type);
    start_trans_param.set_read_snapshot_type(transaction::ObTransReadSnapshotType::STATEMENT_SNAPSHOT);
    start_trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());
    const uint32_t session_id = 1;  // ignore
    const uint64_t proxy_session_id = 1;  // ignore
    const uint64_t org_cluster_id = ObServerConfig::get_instance().cluster_id;

    if (true == trans_state_.is_start_trans_executed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start_trans is executed", K(ret));
    } else {
      if (OB_FAIL(partition_service->start_trans(tenant_id,
                                                 org_cluster_id,
                                                 start_trans_param,
                                                 trans_timeout_ts,
                                                 session_id,
                                                 proxy_session_id, trans_desc_))) {
        LOG_WARN("fail start trans", K(ret), K(start_trans_param));
      }
      trans_state_.set_start_trans_executed(OB_SUCC(ret));
    }
  }
  NG_TRACE(T_start_trans_end);
  // 2. start stmt
  if (OB_SUCC(ret)) {
    transaction::ObStmtDesc &stmt_desc = trans_desc_.get_cur_stmt_desc();
    const bool is_sfu = false;
    stmt_desc.stmt_tenant_id_ = tenant_id;
    stmt_desc.phy_plan_type_ = sql::OB_PHY_PLAN_LOCAL;
    stmt_desc.stmt_type_ = sql::stmt::T_DELETE;
    stmt_desc.is_sfu_ = is_sfu;
    stmt_desc.execution_id_ = 1;
    stmt_desc.inner_sql_ = false;
    stmt_desc.consistency_level_ = trans_consistency_level;
    stmt_desc.is_contain_inner_table_ = false;
    const int64_t stmt_timeout_ts = trans_timeout_ts;
    const bool is_retry_sql = false;
    transaction::ObStmtParam stmt_param;
    ObPartitionArray unreachable_partitions;
    if (true == trans_state_.is_start_stmt_executed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start_stmt is executed", K(ret));
    } else if (OB_FAIL(stmt_param.init(tenant_id, stmt_timeout_ts, is_retry_sql))) {
      LOG_WARN("ObStmtParam init error", K(ret), K(tenant_id), K(is_retry_sql));
    } else if (OB_FAIL(partition_service->start_stmt(stmt_param,
                                                     trans_desc_,
                                                     participants_leaders_, unreachable_partitions))) {
      LOG_WARN("failed to start stmt", K(ret), K(stmt_param));
    }
    trans_state_.set_start_stmt_executed(OB_SUCC(ret));
  }

  // 3. start participant
  NG_TRACE(T_start_part_begin);
  if (OB_SUCC(ret)) {
    if (true == trans_state_.is_start_participant_executed()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("start_participant is executed", K(ret));
    } else if (OB_FAIL(partition_service->start_participant(trans_desc_,
                                                        participants_leaders_.get_partitions(),
                                                        part_epoch_list_))) {
      LOG_WARN("fail start participants", K(ret));
    }
    trans_state_.set_start_participant_executed(OB_SUCC(ret));
  }
  NG_TRACE(T_start_part_end);

  return ret;
}

int ObTableTTLDeleteTask::end_trans(bool is_rollback)
{
  int ret = OB_SUCCESS;
  NG_TRACE(T_end_part_begin);
  storage::ObPartitionService* partition_service = GCTX.par_ser_;
  int end_ret = OB_SUCCESS;
  if (trans_state_.is_start_participant_executed() && trans_state_.is_start_participant_success()) {
    if (OB_SUCCESS != (end_ret = partition_service->end_participant(
                           is_rollback, trans_desc_, participants_leaders_.get_partitions()))) {
      ret = end_ret;
      LOG_WARN("fail to end participant", K(ret), K(end_ret),
               K(is_rollback));
    }
    trans_state_.clear_start_participant_executed();
  }
  NG_TRACE(T_end_part_end);
  if (trans_state_.is_start_stmt_executed() && trans_state_.is_start_stmt_success()) {
    is_rollback = (is_rollback || OB_SUCCESS != ret);
    bool is_incomplete = false;
    ObPartitionArray discard_partitions;
    if (OB_SUCCESS != (end_ret = partition_service->end_stmt(
                           is_rollback, is_incomplete, participants_leaders_.get_partitions(),
                           part_epoch_list_, discard_partitions, participants_leaders_, trans_desc_))) {
      ret = (OB_SUCCESS == ret) ? end_ret : ret;
      LOG_WARN("fail to end stmt", K(ret), K(end_ret), K(is_rollback));
    }
    trans_state_.clear_start_stmt_executed();
  }
  NG_TRACE(T_end_trans_begin);
  const bool use_sync = true;
  if (trans_state_.is_start_trans_executed() && trans_state_.is_start_trans_success()) {
    if (trans_desc_.is_readonly() || use_sync) {
      ret = sync_end_trans(is_rollback, ONE_TASK_TIMEOUT);
    }
    trans_state_.clear_start_trans_executed();
  }
  trans_state_.reset();
  NG_TRACE(T_end_trans_end);
  return ret;
}

int ObTableTTLDeleteTask::sync_end_trans(bool is_rollback, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout_ts);
  sql::ObEndTransSyncCallback callback;
  if (OB_FAIL(callback.init(&trans_desc_, NULL))) {
    LOG_WARN("fail init callback", K(ret));
  } else {
    int wait_ret = OB_SUCCESS;

    storage::ObPartitionService* partition_service = GCTX.par_ser_;
    callback.set_is_need_rollback(is_rollback);
    callback.set_end_trans_type(sql::ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
    callback.handout();
    const int64_t stmt_timeout_ts = ONE_TASK_TIMEOUT;
    // whether end_trans is success or not, the callback MUST be invoked
    if (OB_FAIL(partition_service->end_trans(is_rollback, trans_desc_, callback, stmt_timeout_ts))) {
      LOG_WARN("fail end trans when session terminate", K(ret), K_(trans_desc), K(stmt_timeout_ts));
    }
    // MUST wait here
    if (OB_UNLIKELY(OB_SUCCESS != (wait_ret = callback.wait()))) {
      LOG_WARN("sync end trans callback return an error!", K(ret),
               K(wait_ret), K_(trans_desc), K(stmt_timeout_ts));
    }
    ret = OB_SUCCESS != ret? ret : wait_ret;
    bool has_called_txs_end_trans = false;
    if (callback.is_txs_end_trans_called()) { // enter into the transaction layer
      has_called_txs_end_trans = true;
    } else {
      //something wrong before enter into the transaction layer
      has_called_txs_end_trans = false;
      LOG_WARN("fail before trans service end trans, may disconnct", K(ret));
      if (OB_UNLIKELY(OB_SUCCESS == ret)) {
        LOG_ERROR("callback before trans service end trans, but ret is OB_SUCCESS, it is BUG!!!",
                  K(callback.is_txs_end_trans_called()));
      }
    }
    UNUSED(has_called_txs_end_trans);
    ret = OB_SUCCESS != ret? ret : wait_ret;
  }
  return ret;
}

int ObTableTTLDeleteTask::process()
{
  int ret = OB_SUCCESS;
  bool is_stop = false;
  while(!is_stop) {
    if (OB_FAIL(process_one())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to process one", K(ret));
      }
    }
    if (OB_FAIL(ObTTLManager::get_instance().report_task_status(
                              const_cast<ObTTLTaskInfo&>(*info_), const_cast<ObTTLPara&>(*param_), is_stop))) { // todo@dazhi: reduce report times
      LOG_WARN("fail to report ttl task status", K(ret));
    }
  }
  return ret;
}

// 1. start_tx
// 2. execute ttl delete
// 3. end_tx and update context
int ObTableTTLDeleteTask::process_one()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtil::current_time();
  ObPartitionKey pkey = info_->pkey_;
  uint64_t tenant_id = pkey.get_tenant_id();
  uint64_t table_id = pkey.get_table_id();
  uint64_t partition_id = pkey.get_partition_id();
  ObTableTTLOperation op(tenant_id,
                         table_id,
                         param_->max_version_,
                         param_->ttl_,
                         PER_TASK_DEL_ROWS,
                         first_key_,
                         second_key_);
  ObTableTTLOperationResult result;
  ObTableServiceTTLCtx ctx(allocator_);
  ctx.init_param(ONE_TASK_TIMEOUT + ObTimeUtility::current_time(), trans_desc_, &allocator_,
                 false /* returning_affected_rows */,
                 table::ObTableEntityType::ET_KV,
                 table::ObBinlogRowImageType::MINIMAL);
  ctx.param_table_id() = table_id;
  ctx.param_partition_id() = partition_id;
  if (OB_FAIL(start_trans())) {
    LOG_WARN("fail to start tx", K(ret));
  } else if (OB_FAIL(GCTX.table_service_->execute_ttl_delete(ctx, op, result))) {
    LOG_WARN("fail to execute ttl delete, need rollback", K(ret));
  } else {/* do nothing */}

  ctx.reset_ttl_ctx(GCTX.par_ser_);
  bool need_rollback = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  if (OB_FAIL(end_trans(need_rollback))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;
  ObString result_key;
  if (OB_SUCC(ret)) {
    ObString first_key = result.get_end_rowkey();
    ObString second_key = result.get_end_qualifier();
    ObObj row_key_objs[2];
    row_key_objs[0].set_varchar(first_key);
    row_key_objs[1].set_varchar(second_key);
    ObRowkey row_key;
    row_key.assign(row_key_objs, 2);
    uint64_t buf_len = row_key.get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(buf_len));
    int64_t pos = 0;
    if (OB_FAIL(row_key.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize", K(ret));
    } else {
      result_key.assign_ptr(buf, buf_len);
    }
  }
  info_->max_version_del_cnt_ += result.get_max_version_del_row();
  info_->ttl_del_cnt_ += result.get_ttl_del_row();
  info_->scan_cnt_ += result.get_scan_row();
  info_->err_code_ = ret; 
  if (OB_SUCC(ret) && result.get_del_row() < PER_TASK_DEL_ROWS) {
    ret = OB_ITER_END; // finsh task
    info_->err_code_ = ret; 
    LOG_DEBUG("finish delete", K(ret), KPC_(info));
  }
  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_DEBUG("finish process one", K(ret), K(cost), K(result.get_max_version_del_row()), K(result.get_ttl_del_row()), K(result.get_del_row()));
  return ret;
}

/**
 * ---------------------------------------- ObTableTTLDag ----------------------------------------
 */
ObTableTTLDag::ObTableTTLDag()
  : ObIDag(ObIDagType::DAG_TYPE_TTL, ObIDagPriority::DAG_PRIO_TTL),
    is_inited_(false), param_(), info_(), compat_mode_(share::ObWorker::CompatMode::INVALID)
{
}

ObTableTTLDag::~ObTableTTLDag()
{
}

int ObTableTTLDag::init(const ObTTLPara &param, ObTTLTaskInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableTTLDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task para", K(ret), K(info));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task info", K(ret), K(param));
  } else if (OB_FAIL(get_compat_mode_with_table_id(info.pkey_.table_id_, compat_mode_))) {
    LOG_WARN("fail to get compat mode", K(ret), K(info.pkey_.table_id_));
  } else {
    is_inited_ = true;
    param_ = param;
    info_ = info;
  }
  return ret;
}

bool ObTableTTLDag::operator==(const ObIDag& other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObTableTTLDag &dag = static_cast<const ObTableTTLDag &>(other);
    if (OB_UNLIKELY(!param_.is_valid() || !dag.param_.is_valid() ||
                    !dag.info_.is_valid() || !dag.info_.is_valid())) {
      tmp_ret = OB_ERR_SYS;
      LOG_ERROR("invalid argument", K(tmp_ret), K_(param), K_(info), K(dag.param_), K(dag.info_));
    } else {
      is_equal = (param_.ttl_ == dag.param_.ttl_) &&
                 (param_.max_version_ == dag.param_.max_version_) &&
                 (info_.pkey_ == dag.info_.pkey_);
    }
  }
  return is_equal;
}

int64_t ObTableTTLDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid() || !info_.is_valid())) {
    tmp_ret = OB_ERR_SYS;
    LOG_ERROR("invalid argument", K(tmp_ret), K(is_inited_), K(param_));
  } else {
    hash_val += info_.pkey_.hash();
    hash_val = common::murmurhash(&param_.ttl_, sizeof(param_.ttl_), hash_val);
    hash_val = common::murmurhash(&param_.max_version_, sizeof(param_.max_version_), hash_val);
  }
  return hash_val;
}

int64_t ObTableTTLDag::get_tenant_id() const
{ 
  int tmp_ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_ || !info_.is_valid())) {
    tmp_ret = OB_ERR_SYS;
    LOG_ERROR("invalid argument", K(tmp_ret), K_(is_inited), K_(info));
  } else {
    tenant_id = info_.pkey_.get_tenant_id();
  }
  return tenant_id;
};

int ObTableTTLDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableTTLDag has not been initialized", K(ret));
  } else if (OB_UNLIKELY(!param_.is_valid() || !info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K_(param), K_(info));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "ttl task: table_id = %ld, "
                                     "partition_id=%ld, max_version=%d, time_to_live=%d",
                                     info_.pkey_.get_table_id(),
                                     info_.pkey_.get_partition_id(),
                                     param_.max_version_,
                                     param_.ttl_))) {
    LOG_WARN("fail to fill comment", K(ret), K(param_), K(info_));
  }
  return ret;
}
