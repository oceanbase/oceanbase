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
#include "storage/memtable/mvcc/ob_mvcc_acc_ctx.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace transaction;
using namespace common;
using namespace blocksstable;
using namespace palf;

namespace memtable
{

ObMultiVersionValueIterator::ObMultiVersionValueIterator()
    : is_inited_(false),
      ctx_(NULL),
      version_range_(),
      value_(NULL),
      version_iter_(NULL),
      multi_version_iter_(NULL),
      max_committed_trans_version_(-1),
      cur_trans_version_(SCN::min_scn()),
      is_node_compacted_(false),
      has_multi_commit_trans_(false),
      merge_scn_(SCN::max_scn())
{
}

ObMultiVersionValueIterator::~ObMultiVersionValueIterator()
{
}

//用于对冻结的memtable的row，所以不用判断行锁和设置barrier
int ObMultiVersionValueIterator::init(ObMvccAccessCtx *ctx,
                                      const ObVersionRange &version_range,
                                      const ObMemtableKey *key,
                                      ObMvccRow *value)
{
  int ret = OB_SUCCESS;
  reset();
  UNUSEDx(key);
  if (OB_ISNULL(value)) {
    is_inited_ = true;
  } else {
    //多版本开始转储时，需要先等已经提交的事务都已经apply到memtable中，然后指定冻结版本来读取
    //多版本，读取时不用再判断行锁，在遍历trans_node链表时，需要从指定的trans_version开始读取，
    //我们先做已提交的数据转储，所以遍历list_head就可以了

    version_iter_ = value->get_list_head();
    value_ = value;
    is_inited_ = true;
    version_range_ = version_range;
    ctx_ = ctx;
  }
  return ret;
}

int ObMultiVersionValueIterator::init_multi_version_iter()
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *iter = version_iter_;
  has_multi_commit_trans_ = false;
  // find the first non-compacted trans_node and use its dml_type for the compacted row
  while (NULL != iter && NDT_COMPACT == iter->type_) {
    iter = iter->prev_;
  }

  max_committed_trans_version_ = (NULL != version_iter_) ? version_iter_->trans_version_.get_val_for_tx() : -1;
  // NB: It will assign -1 to cur_trans_version_, while it will not
  // cause any wrong logic, but take care of it
  multi_version_iter_ = iter;
  if (OB_FAIL(cur_trans_version_.convert_for_tx(max_committed_trans_version_))) {
    TRANS_LOG(ERROR, "failed to convert scn", K(ret), K_(max_committed_trans_version));
  } else if (max_committed_trans_version_ <= version_range_.multi_version_start_) {
    //如果多版本的开始版本大于等于当前以提交的最大版本
    //则只迭代出所有trans node compact结果
  } else {
    while (OB_SUCC(ret) && NULL != iter && max_committed_trans_version_ > 0) {
      // TODO: we need handle INT64_MAX and -1
      if (iter->trans_version_.get_val_for_tx() < max_committed_trans_version_) {
        has_multi_commit_trans_ = true;
        break;
      } else if (iter->trans_version_.get_val_for_tx() > max_committed_trans_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), K_(max_committed_trans_version),
            KPC(iter));
      }
      iter = iter->prev_;
    }
  }
  if (OB_SUCC(ret) && !has_multi_commit_trans_) {
    // no multi commit trans row, only need to output the first compact row
    multi_version_iter_ = NULL;
  }
  return ret;
}

void ObMultiVersionValueIterator::print_cur_status()
{
  ObMvccTransNode *node = value_->get_list_head();
  while (OB_NOT_NULL(node)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "print_cur_status", KPC(node));
    node = node->prev_;
  }
}

int ObMultiVersionValueIterator::get_next_uncommitted_node(
    const void *&tnode,
    transaction::ObTransID &trans_id,
    SCN &trans_version,
    transaction::ObTxSEQ &sql_sequence)
{
  int ret = OB_SUCCESS;
  int64_t state = -1;
  uint64_t cluster_version = 0;
  tnode = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMultiVersionValueIterator is not inited", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_ISNULL(tnode)) {
      if (OB_ISNULL(version_iter_)) {
        ret = OB_ITER_END;
      } else if (SCN::max_scn() == version_iter_->scn_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "current trans node has not submit clog yet", K(ret), KPC_(version_iter));
      } else if (NDT_COMPACT == version_iter_->type_) { // ignore compact node
        version_iter_ = version_iter_->prev_;
      } else if (version_iter_->scn_ > merge_scn_) {
        // skip tx node which not log succ
        if (REACH_TIME_INTERVAL(100_ms)) {
          TRANS_LOG(INFO, "skip txn-node log sync failed", KPC(version_iter_), K(merge_scn_));
        }
        version_iter_ = version_iter_->prev_;
      } else {
        bool need_get_state = version_iter_->get_tx_end_scn() > merge_scn_;
        if (need_get_state) {
          if (OB_FAIL(get_state_of_curr_trans_node(trans_id, state, cluster_version))) {
            TRANS_LOG(WARN, "failed to get status of curr trans node", K(ret), K(merge_scn_));
          }
        }
        // If trans is running, tx_end_scn must be INT64_MAX
        // and trans state has been gotten from tx_table before
        // so need to check tx_end_scn <= memtable_end_scn
        if (ObTxData::RUNNING == state) { // return this node
          if (!trans_id.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "trans_id is invalid", K(ret), K(trans_id), KPC(version_iter_), K(merge_scn_));
          } else {
            tnode = static_cast<const void *>(version_iter_);
            trans_version = SCN::max_scn();
            sql_sequence = version_iter_->seq_no_;
            version_iter_ = version_iter_->prev_;
          }
        // when tx_end_scn <= memtable_end_scn, trans is end
        // so need to get trans state from trans node
        // otherwise get trans state from tx_table
        } else if ((!need_get_state && version_iter_->is_aborted())
                   || ObTxData::ABORT == state) {
          // CAN NOT just jump over abort trans node!!!
          // if have output rows before and following abort trans nodes are all skipped,
          // no row will be output then, there will be no last flag.
          ret = OB_EAGAIN;
          TRANS_LOG(WARN, "meet abort trans node when mini merge, need retry", K(ret),
              KPC(version_iter_), K(state));
        } else if ((!need_get_state && version_iter_->is_committed())
                    || ObTxData::COMMIT == state) { // COMMIT
          if (!version_iter_->is_committed()) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "TransNode status is invalid", K(ret), KPC(version_iter_), K(state));
          }
          ret = OB_ITER_END;
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::check_next_sql_sequence(
    const ObTransID &input_trans_id,
    const ObTxSEQ input_sql_sequence,
    bool &same_sql_sequence_flag)
{
  int ret = OB_SUCCESS;
  same_sql_sequence_flag = false;
  int64_t state;
  uint64_t cluster_version = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObMultiVersionValueIterator is not inited", K(ret));
  } else {
    while (nullptr != version_iter_
        && NDT_COMPACT == version_iter_->type_) {
      version_iter_ = version_iter_->prev_;
    }
    if (nullptr != version_iter_) {
      ObTransID trans_id;
      if (OB_FAIL(get_state_of_curr_trans_node(trans_id, state, cluster_version))) {
        if (OB_TRANS_CTX_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(WARN, "failed to get status of curr trans node", K(ret), K(merge_scn_));
        }
      } else if (ObTxData::RUNNING == state) { // return this node
        if (input_trans_id == trans_id) {
          if (version_iter_->seq_no_ == input_sql_sequence) {
            same_sql_sequence_flag = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_state_of_curr_trans_node(
    ObTransID &trans_id,
    int64_t &state,
    uint64_t &cluster_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(version_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans node is null", K(ret), K(version_iter_));
  } else {
    transaction::ObTxSEQ sql_sequence;
    version_iter_->get_trans_id_and_seq_no(trans_id, sql_sequence);

    if (version_iter_->is_aborted()) {
      state = ObTxData::ABORT;
    } else if (OB_FAIL(get_trans_status(trans_id, state, cluster_version))) {
      TRANS_LOG(WARN, "failed to get trans status in running status",
                K(ret), K(trans_id), K(sql_sequence), K(merge_scn_));
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_trans_status(const transaction::ObTransID &trans_id,
                                                  int64_t &state,
                                                  uint64_t &cluster_version)
{
  UNUSED(cluster_version);
  int ret = OB_SUCCESS;
  SCN trans_version = SCN::max_scn();
  storage::ObTxTableGuards &tx_table_guards = ctx_->get_tx_table_guards();
  if (OB_FAIL(tx_table_guards.get_tx_state_with_scn(trans_id,
                                                    merge_scn_,
                                                    state,
                                                    trans_version))) {
    STORAGE_LOG(WARN, "check_with_tx_data fail.", K(trans_id));
  }

  return ret;
}

int ObMultiVersionValueIterator::get_next_node(const void *&tnode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(this));
  } else if (OB_ISNULL(version_iter_)
      || version_iter_->trans_version_.get_val_for_tx() <= version_range_.base_version_) {
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
      ObMvccTransNode *cur_iter = version_iter_;
      tnode = static_cast<const void*>(version_iter_);
      version_iter_ = version_iter_->prev_;
      if (OB_NOT_NULL(version_iter_) && cur_iter->trans_version_ < version_iter_->trans_version_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), KPC(cur_iter),
            KPC_(version_iter));
      }
    }
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_node_for_compact(const void *&tnode)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), KP(this));
  } else if ((OB_NOT_NULL(version_iter_)
              && version_iter_->trans_version_.get_val_for_tx() <= version_range_.base_version_) ||
      OB_ISNULL(version_iter_)) {
    version_iter_ = nullptr;
    ret = OB_ITER_END;
  } else if (!version_iter_->is_committed() && !version_iter_->is_aborted()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "TransNode status is invalid, need retry", K(ret), KPC(version_iter_));
  } else if (NDT_COMPACT == version_iter_->type_) { // meet compact node, end loop TransNode list
    tnode = static_cast<const void*>(version_iter_);
    version_iter_ = NULL;
  } else {
    ObMvccTransNode *cur_iter = version_iter_;
    tnode = static_cast<const void*>(version_iter_);
    version_iter_ = version_iter_->prev_;
    if (OB_NOT_NULL(version_iter_)
        && cur_iter->trans_version_ < version_iter_->trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), KPC(cur_iter), KPC_(version_iter));
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(version_iter_) && OB_ISNULL(tnode)) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObMultiVersionValueIterator::get_next_multi_version_node(const void *&tnode)
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
    const SCN cur_trans_version = multi_version_iter_->trans_version_;
    ObMvccTransNode *record_node = nullptr;
    while (OB_SUCC(ret) && OB_NOT_NULL(multi_version_iter_) && OB_ISNULL(record_node)) {
      if (multi_version_iter_->trans_version_.get_val_for_tx() <= version_range_.base_version_) {
        multi_version_iter_ = NULL;
        break;
      } else if (NDT_COMPACT == multi_version_iter_->type_) { // meet compacted node
        if (multi_version_iter_->trans_version_.get_val_for_tx() > version_range_.multi_version_start_) {
          // ignore compact node
          is_compacted = true;
        } else { // multi_version_iter_->trans_version_ <= multi_version_start
          is_node_compacted_ = true;
          record_node = multi_version_iter_;
          multi_version_iter_ = NULL;
          break;
        }
      } else { // not compacted node
        if (multi_version_iter_->trans_version_.get_val_for_tx() <= version_range_.multi_version_start_) {
          is_node_compacted_ = true;
        }
        record_node = multi_version_iter_;
      }
      multi_version_iter_ = multi_version_iter_->prev_;
      if (is_compacted) {
        //添加一层防御，对于多版本，如果出现了compacted 的trans node
        //则下一个node一定不为NULL，是NULL则报错
        if (OB_ISNULL(multi_version_iter_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "multi version iter should not be NULL", K(ret), K(is_compacted),
              KP(multi_version_iter_));
        } else {
          is_compacted = false;
        }
      }
    } // end while
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(record_node)) {
        ret = OB_ITER_END;
      } else if (!record_node->is_committed() && !record_node->is_aborted()) {
        ret = OB_EAGAIN;
        TRANS_LOG(WARN, "TransNode status is invalid, meet running trans node in iterate "
            "committed row phase, need retry", K(ret), KPC(record_node));
      } else {
        tnode = static_cast<const void*>(record_node);
        cur_trans_version_ = record_node->trans_version_;
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(multi_version_iter_)
        && cur_trans_version < multi_version_iter_->trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "meet trans node with larger trans version", K(ret), K(cur_trans_version),
          KPC_(multi_version_iter));
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
  max_committed_trans_version_ = -1;
  cur_trans_version_ = SCN::min_scn();
  is_node_compacted_ = false;
  has_multi_commit_trans_ = false;
  ctx_ = NULL;
  version_range_.reset();
}

bool ObMultiVersionValueIterator::is_multi_version_iter_end() const
{
  return OB_ISNULL(multi_version_iter_)
      || multi_version_iter_->trans_version_.get_val_for_tx() <= version_range_.base_version_;
}

bool ObMultiVersionValueIterator::is_trans_node_iter_null() const
{
  return OB_ISNULL(version_iter_);
}

bool ObMultiVersionValueIterator::is_compact_iter_end() const
{
  return OB_ISNULL(version_iter_)
      || version_iter_->trans_version_.get_val_for_tx() <= version_range_.base_version_;
}

DEF_TO_STRING(ObMultiVersionValueIterator) {
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(value), KPC_(version_iter), KPC_(multi_version_iter),
      K_(max_committed_trans_version), K_(cur_trans_version), K_(is_node_compacted),
      K_(ctx), K_(merge_scn), K_(version_range), K_(has_multi_commit_trans));
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObMultiVersionRowIterator::ObMultiVersionRowIterator()
    : is_inited_(false),
      ctx_(NULL),
      version_range_(),
      value_iter_(),
      query_engine_(NULL),
      query_engine_iter_(NULL),
      insert_row_count_(0),
      update_row_count_(0),
      delete_row_count_(0)
{
}

ObMultiVersionRowIterator::~ObMultiVersionRowIterator()
{
  reset();
}

int ObMultiVersionRowIterator::init(
    ObQueryEngine &query_engine,
    ObMvccAccessCtx &ctx,
    const ObVersionRange &version_range,
    const ObMvccScanRange &range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }

  if (OB_FAIL(query_engine.scan(
      range.start_key_,  !range.border_flag_.inclusive_start(),
      range.end_key_,    !range.border_flag_.inclusive_end(),
      query_engine_iter_))) {
    TRANS_LOG(WARN, "query engine scan fail", K(ret));
  } else {
    query_engine_ = &query_engine;
    ctx_ = &ctx;
    version_range_ = version_range;
    is_inited_ = true;
  }
  return ret;
}

int ObMultiVersionRowIterator::get_next_row(
    const ObMemtableKey *&key,
    ObMultiVersionValueIterator *&value_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  }

  while (OB_SUCC(ret)) {
    const ObMemtableKey *tmp_key = NULL;
    ObMvccRow *value = NULL;
    if (OB_FAIL(query_engine_iter_->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "query engine iter next fail", K(ret), "ctx", *ctx_);
      }
    } else if (NULL == (tmp_key = query_engine_iter_->get_key())) {
      TRANS_LOG(ERROR, "unexpected key null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (value = query_engine_iter_->get_value())) {
      TRANS_LOG(ERROR, "unexpected value null pointer", "ctx", *ctx_);
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(try_cleanout_mvcc_row_(value))) {
      TRANS_LOG(WARN, "try cleanout mvcc row failed", K(ret), "ctx", *ctx_);
    } else if (OB_FAIL(value_iter_.init(ctx_,
                                        version_range_,
                                        tmp_key,
                                        value))) {
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

int ObMultiVersionRowIterator::try_cleanout_mvcc_row_(ObMvccRow *value)
{
  int ret = OB_SUCCESS;

  if (NULL == value) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "try cleanout mvcc row failed", K(ret), KPC(value));
  } else {
    ObRowLatchGuard guard(value->latch_);
    ObMvccTransNode *iter = value->get_list_head();
    while (NULL != iter && OB_SUCC(ret)) {
      if (OB_FAIL(try_cleanout_tx_node_(value, iter))) {
        TRANS_LOG(WARN, "try cleanout tx state failed", K(ret), KPC(value), KPC(iter));
      } else {
        iter = iter->prev_;
      }
    }
  }

  return ret;
}

int ObMultiVersionRowIterator::try_cleanout_tx_node_(ObMvccRow *value, ObMvccTransNode *tnode)
{
  int ret = OB_SUCCESS;
  const ObTransID data_tx_id = tnode->tx_id_;
  if (!(tnode->is_committed() || tnode->is_aborted())
      && tnode->is_delayed_cleanout()
      && OB_FAIL(ctx_->get_tx_table_guards().cleanout_tx_node(data_tx_id,
                                            *value,
                                            *tnode,
                                            false     /*need_row_latch*/))) {
    TRANS_LOG(WARN, "cleanout tx state failed", K(ret), K(*value), K(*tnode));
  } else if (!tnode->is_aborted()) {
    const blocksstable::ObDmlFlag dml_flag = tnode->get_dml_flag();

    if (blocksstable::ObDmlFlag::DF_INSERT == dml_flag) {
      ++insert_row_count_;
    } else if (blocksstable::ObDmlFlag::DF_UPDATE == dml_flag) {
      ++update_row_count_;
    } else if (blocksstable::ObDmlFlag::DF_DELETE == dml_flag) {
      ++delete_row_count_;
    }
  }
  return ret;
}

void ObMultiVersionRowIterator::get_tnode_dml_stat(storage::ObTransNodeDMLStat &stat) const
{
  stat.insert_row_count_ += insert_row_count_;
  stat.update_row_count_ += update_row_count_;
  stat.delete_row_count_ += delete_row_count_;
}

void ObMultiVersionRowIterator::reset()
{
  is_inited_ = false;
  ctx_ = NULL;
  version_range_.reset();
  value_iter_.reset();
  if (NULL != query_engine_iter_) {
    query_engine_iter_->reset();
    query_engine_->revert_iter(query_engine_iter_);
    query_engine_iter_ = NULL;
  }
  query_engine_ = NULL;

  insert_row_count_ = 0;
  update_row_count_ = 0;
  delete_row_count_ = 0;
}


} // namespace memtable
} // namespace oceanbase
