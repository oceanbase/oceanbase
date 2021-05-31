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

#define USING_LOG_PREFIX TRANS
#include "lib/stat/ob_diagnose_info.h"

#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/transaction/ob_trans_define.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/ob_partition_service.h"
#include "storage/transaction/ob_trans_part_ctx.h"

namespace oceanbase {
using namespace storage;
using namespace transaction;
using namespace common;
namespace memtable {

ObMultiVersionValueIterator::ObMultiVersionValueIterator()
    : is_inited_(false),
      value_(NULL),
      version_iter_(NULL),
      multi_version_iter_(NULL),
      ctx_(NULL),
      max_committed_trans_version_(-1),
      cur_trans_version_(0),
      compacted_row_dml_(T_DML_UNKNOWN),
      is_node_compacted_(false),
      trans_table_guard_(NULL),
      merge_log_ts_(INT64_MAX),
      iter_mode_(storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT),
      meet_non_overflow_node_flag_(false)
{}

ObMultiVersionValueIterator::~ObMultiVersionValueIterator()
{}

// need not detect row latch when minor freeze
int ObMultiVersionValueIterator::init(const ObIMvccCtx& ctx, const ObTransSnapInfo& snapshot_info,
    const ObMemtableKey* key, ObMvccRow* value, ObTransStateTableGuard& trans_table_guard)
{
  int ret = OB_SUCCESS;
  reset();
  UNUSEDx(key, snapshot_info);
  if (OB_ISNULL(value)) {
    is_inited_ = true;
    // TODO() do not lock for read to avoid deadlock
    //} else if (OB_FAIL(value->lock_for_read(key, const_cast<ObIMvccCtx&>(ctx)))) {
    //  TRANS_LOG(WARN, "wait row lock fail", K(ret), K(ctx), KP(value), K(*value));
  } else {
    // need not detect row latch when minor freeze,
    // just traverse trasaction node list where list head points to
    // because all transaction have already apply to memtable before minor freeze

    version_iter_ = value->get_list_head();
    value_ = value;
    is_inited_ = true;
    ctx_ = &ctx;
    trans_table_guard_ = &trans_table_guard;
  }
  return ret;
}

void ObMultiVersionValueIterator::init_multi_version_iter()
{
  ObMvccTransNode* iter = version_iter_;
  // find the first non-compacted trans_node and use its dml_type for the compacted row
  while (NULL != iter && NDT_COMPACT == iter->type_) {
    iter = iter->prev_;
  }

  if (NULL != iter) {
    compacted_row_dml_ = iter->get_dml_type();
  }
  max_committed_trans_version_ = (NULL != version_iter_) ? version_iter_->trans_version_ : -1;
  cur_trans_version_ = max_committed_trans_version_;
  if (max_committed_trans_version_ <= ctx_->get_multi_version_start()) {
    // iterate all transaction node if start version is greater than or equal to
    // max committed transaction version
    multi_version_iter_ = NULL;
  } else {
    multi_version_iter_ = version_iter_;
    // skip current version because the max committed transaction version has been compacted
    while (NULL != multi_version_iter_ && max_committed_trans_version_ > 0) {
      if (multi_version_iter_->trans_version_ < max_committed_trans_version_) {
        break;
      } else if (multi_version_iter_->trans_version_ > max_committed_trans_version_) {
        TRANS_LOG(ERROR,
            "meet trans node with larger trans version",
            K_(max_committed_trans_version),
            KPC_(multi_version_iter));
      }
      multi_version_iter_ = multi_version_iter_->prev_;
    }
  }
}

int ObMultiVersionValueIterator::should_ignore_cur_node(const ObMvccTransNode& node, bool& ignore_flag)
{
  int ret = OB_SUCCESS;
  ignore_flag = false;
  if (storage::ObTableIterParam::OIM_ITER_FULL == iter_mode_) {
    // should iter all trans node
  } else if (storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT == iter_mode_) {
    if (NDT_COMPACT == node.type_) {
      ignore_flag = true;
    } else {
      ignore_flag = !node.is_overflow();
      if (ignore_flag) {  // meet non-overflow trans node
        meet_non_overflow_node_flag_ = true;
      } else if (meet_non_overflow_node_flag_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet overflow node after non-overflow node", K(ret), K(node));
        print_cur_status();
      }
    }
  } else {  // OIM_ITER_NON_OVERFLOW_TO_MINI == iter_mode_
    ignore_flag = node.is_overflow();
  }
  return ret;
}

void ObMultiVersionValueIterator::print_cur_status()
{
  ObMvccTransNode* node = value_->get_list_head();
  while (OB_NOT_NULL(node)) {
    TRANS_LOG(ERROR, "print_cur_status", KPC(node));
    node = node->prev_;
  }
}

bool ObMultiVersionValueIterator::contain_overflow_trans_node()
{
  bool bret = false;
  if (OB_NOT_NULL(value_)) {
    bret = NULL != value_->first_overflow_node_;
  }
  return bret;
}

/*
 * TAIL <- NODE <- NODE(first_overflow_node_) <- NODE <- NODE <- NODE <- HEAD
 *                   	overflow       		      |     non-overflow
 * ObMvccRow::first_overflow_node_ point to first overflow_node
 * set version iterator to first_overflow_node_->prev_ if need skip overflow node
 * when iterate from link tail to link head
 * */
void ObMultiVersionValueIterator::jump_overflow_trans_node()
{
  ObMvccTransNode* first_overflow_node = OB_NOT_NULL(value_) ? value_->first_overflow_node_ : NULL;
  if (OB_NOT_NULL(first_overflow_node)) {
    version_iter_ = first_overflow_node->prev_;
  }
}

int ObMultiVersionValueIterator::get_next_uncommitted_node(
    const void*& tnode, transaction::ObTransID& trans_id, int64_t& trans_version, int64_t& sql_sequence)
{
  int ret = OB_SUCCESS;
  transaction::ObTransTableStatusType trans_status;
  tnode = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMultiVersionValueIterator is not inited", K(ret));
  } else {
    bool ignore_flag = false;
    while (OB_SUCC(ret) && OB_ISNULL(tnode)) {
      if (OB_ISNULL(version_iter_)) {
        ret = OB_ITER_END;
      } else if (INT64_MAX == version_iter_->log_timestamp_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "current trans node has not submit clog yet", K(ret), KPC_(version_iter));
      } else if (NDT_COMPACT == version_iter_->type_) {  // ignore compact node
        version_iter_ = version_iter_->prev_;
      } else if (OB_FAIL(should_ignore_cur_node(*version_iter_, ignore_flag))) {
        TRANS_LOG(ERROR, "not status invalid", K(ret), KPC(this));
      } else if (ignore_flag) {
        version_iter_ = version_iter_->prev_;
      } else if (OB_FAIL(get_status_of_curr_trans_node(trans_id, trans_status))) {
        TRANS_LOG(WARN, "failed to get status of curr trans node", K(ret), K(merge_log_ts_));
      } else if (ObTransTableStatusType::RUNNING == trans_status) {  // return this node
        if (!trans_id.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "trans_id is invalid", K(ret), K(trans_id), KPC(version_iter_), K(merge_log_ts_));
        } else {
          tnode = static_cast<const void*>(version_iter_);
          trans_version = INT64_MAX;
          sql_sequence = version_iter_->sql_sequence_;
          version_iter_ = version_iter_->prev_;
        }
      } else if (ObTransTableStatusType::ABORT == trans_status) {
        ret = OB_EAGAIN;
        TRANS_LOG(
            WARN, "meet abort trans node when mini merge, need retry", K(ret), KPC(version_iter_), K(trans_status));
      } else {  // COMMIT
        if (!version_iter_->is_committed()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "TransNode status is invalid", K(ret), KPC(version_iter_), K(trans_status));
        }
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::check_next_sql_sequence(
    const ObTransID& input_trans_id, const int64_t input_sql_sequence, bool& same_sql_sequence_flag)
{
  int ret = OB_SUCCESS;
  same_sql_sequence_flag = false;
  transaction::ObTransTableStatusType trans_status;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMultiVersionValueIterator is not inited", K(ret));
  } else {
    while (nullptr != version_iter_ && NDT_COMPACT == version_iter_->type_) {
      version_iter_ = version_iter_->prev_;
    }
    if (nullptr != version_iter_) {
      ObTransID trans_id;
      if (OB_FAIL(get_status_of_curr_trans_node(trans_id, trans_status))) {
        if (OB_TRANS_CTX_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(WARN, "failed to get status of curr trans node", K(ret), K(merge_log_ts_));
        }
      } else if (ObTransTableStatusType::RUNNING == trans_status) {  // return this node
        if (input_trans_id == trans_id && version_iter_->sql_sequence_ == input_sql_sequence) {
          same_sql_sequence_flag = true;
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_status_of_curr_trans_node(
    ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(version_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans node is null", K(ret), K(version_iter_));
  } else {
    int64_t sql_sequence = INT64_MAX;
    {
      ObRebuildListener rebuild_listener(*(trans_table_guard_->get_trans_state_table().get_partition_trans_ctx_mgr()));

      if (rebuild_listener.on_partition_rebuild()) {
        // the rebuild is concurrent with the merge, so the merge should be
        // aborted for safe usage of the transaction pointer on transaction node
        // without reference count
        ret = OB_REPLICA_NOT_READABLE;
      } else {
        ObRowLatchGuard guard(value_->latch_);
        ret = version_iter_->get_trans_id_and_sql_no(trans_id, sql_sequence);
      }
    }
    if (OB_FAIL(ret)) {
      if (OB_TRANS_HAS_DECIDED == ret) {
        ret = OB_SUCCESS;
        // call get node in determined status
        if (OB_FAIL(get_trans_status_in_determined_status(trans_id, trans_status))) {
          TRANS_LOG(WARN, "failed to get trans status in determined status", K(ret), K(merge_log_ts_));
        }
      } else {
        TRANS_LOG(WARN, "failed to get trans id and sql no", K(ret), K(merge_log_ts_));
      }
    } else if (OB_FAIL(get_trans_status(trans_id, trans_status))) {  // in running status
      TRANS_LOG(
          WARN, "failed to get trans status in running status", K(ret), K(trans_id), K(sql_sequence), K(merge_log_ts_));
    } else if (ObTransTableStatusType::RUNNING != trans_status) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR,
          "get commit/abort status in RUNNING branch",
          K(ret),
          KPC(version_iter_),
          K(trans_id),
          K(trans_status),
          K(merge_log_ts_));
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_trans_status_in_determined_status(
    transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status)
{
  int ret = OB_SUCCESS;
  transaction::ObTransCtx* trans_ctx_ptr = NULL;
  {
    ObRowLatchGuard guard(value_->latch_);
    ret = version_iter_->get_trans_ctx(trans_ctx_ptr);
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "failed to get trans ctx from trans node", K(ret), K(*version_iter_));
  } else if (OB_ISNULL(trans_ctx_ptr)) {  // not a dirty trans
    trans_status = version_iter_->is_committed() ? ObTransTableStatusType::COMMIT : ObTransTableStatusType::ABORT;
  } else if (version_iter_->is_committed()) {  // is a dirty COMMIT trans
    trans_id = trans_ctx_ptr->get_trans_id();  // get trans id from trans ctx
    if (OB_FAIL(get_trans_status(trans_id, trans_status))) {
      TRANS_LOG(WARN, "failed to get transaction status", K(ret), K(trans_table_guard_), K(merge_log_ts_), K(trans_id));
    }
  } else {  // is a dirty ABORT trans
    trans_status = ObTransTableStatusType::ABORT;
  }
  return ret;
}

int ObMultiVersionValueIterator::get_trans_status(
    const ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status)
{
  int ret = OB_SUCCESS;
  int64_t trans_version = INT64_MAX;

  if (OB_ISNULL(trans_table_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObPartitionTransCtxMgr is null", K(ret), K(trans_table_guard_), K(merge_log_ts_), K(trans_id));
  } else if (OB_FAIL(trans_table_guard_->get_trans_state_table().get_transaction_status_with_log_ts(
                 trans_id, merge_log_ts_, trans_status, trans_version))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "failed to get transaction status", K(ret), K(trans_table_guard_), K(merge_log_ts_), K(trans_id));
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_node(const void*& tnode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(this));
  } else if (OB_ISNULL(version_iter_) || version_iter_->trans_version_ <= ctx_->get_base_version()) {
    version_iter_ = NULL;
    ret = OB_ITER_END;
  } else {
    if (!version_iter_->is_committed() && !version_iter_->is_aborted()) {
      ret = OB_EAGAIN;
      TRANS_LOG(WARN, "TransNode status is invalid, need retry", K(ret), KPC(version_iter_));
    }
    if (NDT_COMPACT == version_iter_->type_) {
      tnode = static_cast<const void*>(version_iter_);
      version_iter_ = NULL;
    } else {
      ObMvccTransNode* cur_iter = version_iter_;
      tnode = static_cast<const void*>(version_iter_);
      version_iter_ = version_iter_->prev_;
      if (OB_NOT_NULL(version_iter_) && cur_iter->trans_version_ < version_iter_->trans_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), KPC(cur_iter), KPC_(version_iter));
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_node_for_compact(const void*& tnode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(this));
  } else if (OB_FAIL(get_next_right_node(version_iter_))) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "get next node failed", K(ret), KP(version_iter_));
    }
  } else if (OB_ISNULL(version_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "version_iter_ is null", K(ret), KP(version_iter_));
  } else if (!version_iter_->is_committed() && !version_iter_->is_aborted()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "TransNode status is invalid, need retry", K(ret), KPC(version_iter_));
  } else if (NDT_COMPACT == version_iter_->type_) {  // meet compact node, end loop TransNode list
    tnode = static_cast<const void*>(version_iter_);
    version_iter_ = NULL;
  } else {
    ObMvccTransNode* cur_iter = version_iter_;
    tnode = static_cast<const void*>(version_iter_);
    version_iter_ = version_iter_->prev_;
    if (OB_NOT_NULL(version_iter_) && cur_iter->trans_version_ < version_iter_->trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), KPC(cur_iter), KPC_(version_iter));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(version_iter_) && OB_ISNULL(tnode)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_right_node(ObMvccTransNode*& node)
{
  int ret = OB_SUCCESS;
  bool ignore_flag = false;
  while (OB_SUCC(ret) && OB_NOT_NULL(node)) {
    // stop iterating when the transaction node version is less than base version
    if (node->trans_version_ <= ctx_->get_base_version()) {
      node = NULL;
      break;
    } else if (OB_FAIL(should_ignore_cur_node(*node, ignore_flag))) {
      TRANS_LOG(ERROR, "node status invalid", K(ret), KPC(this));
    } else if (!ignore_flag) {
      // stop iterating when the transaction node meet requirement
      break;
    }
    node = node->prev_;
  }
  if (OB_SUCC(ret) && OB_ISNULL(node)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_multi_version_node(const void*& tnode)
{
  int ret = OB_SUCCESS;
  bool is_compacted = false;
  tnode = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(this));
  } else if (OB_ISNULL(multi_version_iter_)) {
    ret = OB_ITER_END;
  } else {
    const int64_t cur_trans_version = multi_version_iter_->trans_version_;
    ObMvccTransNode* record_node = nullptr;
    bool ignore_flag = false;
    while (OB_SUCC(ret) && OB_NOT_NULL(multi_version_iter_) && OB_ISNULL(record_node)) {
      if (multi_version_iter_->trans_version_ <= ctx_->get_base_version()) {
        multi_version_iter_ = NULL;
        break;
      } else if (OB_FAIL(should_ignore_cur_node(*multi_version_iter_, ignore_flag))) {
        TRANS_LOG(ERROR, "not status invalid", K(ret), KPC(this));
      } else if (!ignore_flag) {
        if (NDT_COMPACT == multi_version_iter_->type_) {  // meet compacted node
          if (multi_version_iter_->trans_version_ > ctx_->get_multi_version_start()) {
            // ignore compact node
            is_compacted = true;
          } else {  // multi_version_iter_->trans_version_ <= ctx_->get_multi_version_start()
            is_node_compacted_ = true;
            record_node = multi_version_iter_;
            multi_version_iter_ = NULL;
            break;
          }
        } else {  // not compacted node
          if (multi_version_iter_->trans_version_ <= ctx_->get_multi_version_start()) {
            is_node_compacted_ = true;
          }
          record_node = multi_version_iter_;
        }
      }
      multi_version_iter_ = multi_version_iter_->prev_;
      if (is_compacted) {
        // unexpected if compacted transaction node founded and when the next node is null
        if (OB_ISNULL(multi_version_iter_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "multi version iter should not be NULL", K(ret), K(is_compacted), KP(multi_version_iter_));
        } else {
          is_compacted = false;
        }
      }
    }  // end while
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(record_node)) {
        ret = OB_ITER_END;
      } else if (!record_node->is_committed() && !record_node->is_aborted()) {
        ret = OB_EAGAIN;
        TRANS_LOG(WARN,
            "TransNode status is invalid, meet running trans node in iterate "
            "committed row phase, need retry",
            K(ret),
            KPC(record_node));
      } else {
        tnode = static_cast<const void*>(record_node);
        cur_trans_version_ = record_node->trans_version_;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(multi_version_iter_) && cur_trans_version < multi_version_iter_->trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(
          ERROR, "meet trans node with larger trans version", K(ret), K(cur_trans_version), KPC_(multi_version_iter));
    }
  }
  return ret;
}

bool ObMultiVersionValueIterator::is_cur_multi_version_row_end() const
{
  bool ret = true;
  if (OB_ISNULL(multi_version_iter_)) {
    ret = true;
  } else {
    if (cur_trans_version_ == multi_version_iter_->trans_version_) {
      ret = false;
    } else {
      ret = true;
    }
  }
  return ret;
}

void ObMultiVersionValueIterator::reset()
{
  is_inited_ = true;
  value_ = NULL;
  version_iter_ = NULL;
  multi_version_iter_ = NULL;
  ctx_ = NULL;
  max_committed_trans_version_ = -1;
  cur_trans_version_ = 0;
  is_node_compacted_ = false;
  iter_mode_ = storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT;
  meet_non_overflow_node_flag_ = false;
}

enum ObRowDml ObMultiVersionValueIterator::get_first_dml()
{
  return OB_ISNULL(value_) ? T_DML_UNKNOWN : value_->get_first_dml();
}

bool ObMultiVersionValueIterator::is_multi_version_iter_end() const
{
  return OB_ISNULL(multi_version_iter_) || multi_version_iter_->trans_version_ <= ctx_->get_base_version() ||
         (storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT == iter_mode_ &&
             !multi_version_iter_->is_overflow());
}

bool ObMultiVersionValueIterator::is_trans_node_iter_null() const
{
  return OB_ISNULL(version_iter_);
}

bool ObMultiVersionValueIterator::is_compact_iter_end() const
{
  return OB_ISNULL(version_iter_) || version_iter_->trans_version_ <= ctx_->get_base_version() ||
         (storage::ObTableIterParam::OIM_ITER_OVERFLOW_TO_COMPLEMENT == iter_mode_ && !version_iter_->is_overflow());
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMultiVersionRowIterator::ObMultiVersionRowIterator()
    : is_inited_(false),
      ctx_(NULL),
      snapshot_info_(),
      value_iter_(),
      query_engine_(NULL),
      query_engine_iter_(NULL),
      trans_table_guard_(NULL)
{}

ObMultiVersionRowIterator::~ObMultiVersionRowIterator()
{
  reset();
}

int ObMultiVersionRowIterator::init(ObQueryEngine& query_engine, const ObIMvccCtx& ctx,
    const ObTransSnapInfo& snapshot_info, const ObMvccScanRange& range, ObTransStateTableGuard& trans_table_guard)
{
  int ret = OB_SUCCESS;
  ObTransService* trans_service = nullptr;
  if (is_inited_) {
    reset();
  }

  if (OB_FAIL(query_engine.scan(range.start_key_,
          !range.border_flag_.inclusive_start(),
          range.end_key_,
          !range.border_flag_.inclusive_end(),
          snapshot_info.get_snapshot_version(),
          query_engine_iter_))) {
    TRANS_LOG(WARN, "query engine scan fail", K(ret));
  } else {
    query_engine_ = &query_engine;
    query_engine_iter_->set_version(snapshot_info.get_snapshot_version());
    ctx_ = &ctx;
    snapshot_info_ = snapshot_info;
    trans_table_guard_ = &trans_table_guard;
    is_inited_ = true;
  }
  return ret;
}

int ObMultiVersionRowIterator::get_next_row(const ObMemtableKey*& key, ObMultiVersionValueIterator*& value_iter)
{
  int ret = OB_SUCCESS;
  const bool skip_purge_memtable = true;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  }

  while (OB_SUCC(ret)) {
    const ObMemtableKey* tmp_key = NULL;
    ObMvccRow* value = NULL;
    if (OB_FAIL(query_engine_iter_->next(skip_purge_memtable))) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query engine iter next fail", K(ret), "ctx", *ctx_);
      }
    } else if (NULL == (tmp_key = query_engine_iter_->get_key())) {
      TRANS_LOG(ERROR, "unexpected key null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (value = query_engine_iter_->get_value())) {
      TRANS_LOG(ERROR, "unexpected value null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(value_iter_.init(*ctx_, snapshot_info_, tmp_key, value, *trans_table_guard_))) {
      TRANS_LOG(WARN, "value iter init fail", K(ret), "ctx", *ctx_, KP(value), K(*value));
    } else if (!value_iter_.is_exist()) {
      // continue
    } else {
      key = tmp_key;
      value_iter = &value_iter_;
      break;
    }
  }
  return ret;
}

void ObMultiVersionRowIterator::reset()
{
  is_inited_ = false;
  ctx_ = NULL;
  snapshot_info_.reset();
  value_iter_.reset();
  if (NULL != query_engine_iter_) {
    query_engine_iter_->reset();
    query_engine_->revert_iter(query_engine_iter_);
    query_engine_iter_ = NULL;
  }
  query_engine_ = NULL;
}

}  // namespace memtable
}  // namespace oceanbase
