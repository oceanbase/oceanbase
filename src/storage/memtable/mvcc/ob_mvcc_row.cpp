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

#include "ob_mvcc_row.h"
#include "common/ob_tablet_id.h"
#include "lib/ob_errno.h"
#include "ob_mvcc_ctx.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_row_compactor.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/time/ob_time_utility.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_trans_event.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/access/ob_rows_info.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
using namespace transaction;
using namespace common;
using namespace share;
namespace memtable
{

const uint8_t ObMvccTransNode::F_INIT = 0x0;
const uint8_t ObMvccTransNode::F_WEAK_CONSISTENT_READ_BARRIER = 0x1;
const uint8_t ObMvccTransNode::F_STRONG_CONSISTENT_READ_BARRIER = 0x2;
const uint8_t ObMvccTransNode::F_COMMITTED = 0x4;
const uint8_t ObMvccTransNode::F_ELR = 0x8;
const uint8_t ObMvccTransNode::F_ABORTED = 0x10;
const uint8_t ObMvccTransNode::F_DELAYED_CLEANOUT = 0x40;
const uint8_t ObMvccTransNode::F_MUTEX = 0x80;

void ObMvccTransNode::checksum(ObBatchChecksum &bc) const
{
  bc.fill(&modify_count_, sizeof(modify_count_));
  bc.fill(&type_, sizeof(type_));
}

uint32_t ObMvccTransNode::m_cal_acc_checksum(const uint32_t last_acc_checksum) const
{
  uint32_t acc_checksum = 0;
  ObBatchChecksum bc;
  bc.fill(&last_acc_checksum, sizeof(last_acc_checksum));
  ((ObMemtableDataHeader *)buf_)->checksum(bc);
  acc_checksum = static_cast<uint32_t>((bc.calc() ? : 1) & 0xffffffff);
  return acc_checksum;
}

void ObMvccTransNode::cal_acc_checksum(const uint32_t last_acc_checksum)
{
  acc_checksum_ = m_cal_acc_checksum(last_acc_checksum);
  if (0 == last_acc_checksum) {
    TRANS_LOG(DEBUG, "calc first trans node checksum", K(last_acc_checksum), K(*this));
  }
}

int ObMvccTransNode::verify_acc_checksum(const uint32_t last_acc_checksum) const
{
  int ret = OB_SUCCESS;
  if (0 != last_acc_checksum) {
    const uint32_t acc_checksum = m_cal_acc_checksum(last_acc_checksum);
    if (acc_checksum_ != acc_checksum) {
      ret = OB_CHECKSUM_ERROR;
      TRANS_LOG(ERROR, "row checksum error", K(ret), K(last_acc_checksum),
          "save_acc_checksum", acc_checksum_, "cal_acc_checksum", acc_checksum, K(*this));
      const uint32_t test_checksum = 0;
      if (acc_checksum_ == m_cal_acc_checksum(test_checksum)) {
        TRANS_LOG(INFO, "test checksum success", K(*this));
      }
    }
  }
  return ret;
}

blocksstable::ObDmlFlag ObMvccTransNode::get_dml_flag() const
{
  return reinterpret_cast<const ObMemtableDataHeader *>(buf_)->dml_flag_;
}

int64_t ObMvccTransNode::get_data_size() const
{
  return reinterpret_cast<const ObMemtableDataHeader *>(buf_)->buf_len_;
}

void ObMvccTransNode::set_safe_read_barrier(const bool is_weak_consistent_read)
{
  uint8_t consistent_flag = F_STRONG_CONSISTENT_READ_BARRIER;
  if (is_weak_consistent_read) {
    consistent_flag = F_WEAK_CONSISTENT_READ_BARRIER;
  }
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag | consistent_flag);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::clear_safe_read_barrier()
{
  const uint8_t consistent_flag = (F_WEAK_CONSISTENT_READ_BARRIER | F_STRONG_CONSISTENT_READ_BARRIER);
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag & (~consistent_flag));
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

bool ObMvccTransNode::is_safe_read_barrier() const
{
  const uint8_t flag = flag_;
  return ((flag & F_WEAK_CONSISTENT_READ_BARRIER)
          || (flag & F_STRONG_CONSISTENT_READ_BARRIER));
}

void ObMvccTransNode::set_snapshot_version_barrier(const SCN scn_version,
                                                   const int64_t flag)
{
  ATOMIC_STORE(&snapshot_version_barrier_, scn_version.get_val_for_tx() | flag);
}

void ObMvccTransNode::get_snapshot_version_barrier(int64_t &version,
                                                   int64_t &flag)
{
  int64_t flaged_version = ATOMIC_LOAD(&snapshot_version_barrier_);
  version = flaged_version & (~SNAPSHOT_VERSION_BARRIER_BIT);
  flag = flaged_version & SNAPSHOT_VERSION_BARRIER_BIT;
}

void ObMvccTransNode::get_trans_id_and_seq_no(ObTransID &tx_id,
                                              ObTxSEQ &seq_no)
{
  tx_id = tx_id_;
  seq_no = seq_no_;
}

int ObMvccTransNode::fill_trans_version(const SCN version)
{
  trans_version_.atomic_store(version);
  return OB_SUCCESS;
}

int ObMvccTransNode::fill_scn(const SCN scn)
{
  scn_.atomic_store(scn);
  return OB_SUCCESS;
}

void ObMvccTransNode::trans_commit(const SCN commit_version, const SCN tx_end_scn)
{
  // NB: we need set commit version before set committed
  fill_trans_version(commit_version);
  set_committed();
  set_tx_end_scn(tx_end_scn);
}

void ObMvccTransNode::trans_abort(const SCN tx_end_scn)
{
  set_aborted();
  set_tx_end_scn(tx_end_scn);
}

void ObMvccTransNode::remove_callback()
{
  set_delayed_cleanout(true);
}

int ObMvccTransNode::is_lock_node(bool &is_lock) const
{
  int ret = common::OB_SUCCESS;
  const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(buf_);
  if (NULL == mtd) {
    ret = common::OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, mtd is NULL", K(ret));
  } else if (blocksstable::ObDmlFlag::DF_LOCK == mtd->dml_flag_) {
    is_lock = true;
  } else {
    is_lock = false;
  }
  return ret;
}

int64_t ObMvccTransNode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(buf_);
  common::databuff_printf(buf, buf_len, pos,
                          "this=%p "
                          "trans_version=%s "
                          "scn=%s "
                          "tx_id=%s "
                          "prev=%p "
                          "next=%p "
                          "modify_count=%u "
                          "acc_checksum=%u "
                          "version=%ld "
                          "type=%d "
                          "flag=%d "
                          "snapshot_barrier=%ld "
                          "snapshot_barrier_flag=%ld "
                          "mtd=%s "
                          "seq_no=%s",
                          this,
                          to_cstring(trans_version_),
                          to_cstring(scn_),
                          to_cstring(tx_id_),
                          prev_,
                          next_,
                          modify_count_,
                          acc_checksum_,
                          version_,
                          type_,
                          flag_,
                          snapshot_version_barrier_
                          & (~SNAPSHOT_VERSION_BARRIER_BIT),
                          snapshot_version_barrier_ >> 62,
                          to_cstring(*mtd),
                          to_cstring(seq_no_));
  return pos;
}

void ObMvccRow::ObMvccRowIndex::reset()
{
  if (!is_empty_) {
    MEMSET(&replay_locations_, 0, sizeof(replay_locations_));
    is_empty_ = true;
  }
}

bool ObMvccRow::ObMvccRowIndex::is_valid_queue_index(const int64_t queue_index)
{
  return (queue_index >= 0 && queue_index < REPLAY_TASK_QUEUE_SIZE);
}

ObMvccTransNode *ObMvccRow::ObMvccRowIndex::get_index_node(const int64_t index) const
{
  ObMvccTransNode *ret_node = NULL;
  if (is_valid_queue_index(index)) {
    ret_node = replay_locations_[index];
  }
  return ret_node;
}

void ObMvccRow::ObMvccRowIndex::set_index_node(const int64_t index, ObMvccTransNode *node)
{
  if (is_valid_queue_index(index)) {
    is_empty_ = false;
    ATOMIC_STORE(&(replay_locations_[index]), node);
  }
}

void ObMvccRow::reset()
{
  update_since_compact_ = 0;
  flag_ = F_INIT;
  first_dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  last_dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  list_head_ = NULL;
  max_trans_version_ = SCN::min_scn();
  max_elr_trans_version_ = SCN::min_scn();
  latest_compact_node_ = NULL;
  latest_compact_ts_ = 0;
  index_ = NULL;
  total_trans_node_cnt_ = 0;
  last_compact_cnt_ = 0;
  max_modify_scn_.set_invalid();
  min_modify_scn_.set_invalid();
}

int64_t ObMvccRow::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos,
                          "{this=%p "
                          "latch_=%s "
                          "flag=%hhu "
                          "first_dml=%s "
                          "last_dml=%s "
                          "update_since_compact=%d "
                          "list_head=%p "
                          "latest_compact_node=%p "
                          "max_trans_version=%s "
                          "max_trans_id=%ld "
                          "max_elr_trans_version=%s "
                          "max_elr_trans_id=%ld "
                          "latest_compact_ts=%ld "
                          "last_compact_cnt=%ld "
                          "total_trans_node_cnt=%ld "
                          "max_modify_scn=%s "
                          "min_modify_scn=%s}",
                          this,
                          (latch_.is_locked() ? "locked" : "unlocked"),
                          flag_,
                          get_dml_str(first_dml_flag_),
                          get_dml_str(last_dml_flag_),
                          update_since_compact_,
                          list_head_,
                          latest_compact_node_,
                          to_cstring(max_trans_version_),
                          max_trans_id_.get_id(),
                          to_cstring(max_elr_trans_version_),
                          max_elr_trans_id_.get_id(),
                          latest_compact_ts_,
                          last_compact_cnt_,
                          total_trans_node_cnt_,
                          to_cstring(max_modify_scn_),
                          to_cstring(min_modify_scn_));
  return pos;
}

int64_t ObMvccRow::to_string(char *buf, const int64_t buf_len, const bool verbose) const
{
  int64_t pos = 0;
  pos = to_string(buf, buf_len);
  if (verbose) {
    common::databuff_printf(buf, buf_len, pos, " list=[");
    ObMvccTransNode *iter = list_head_;
    while (NULL != iter) {
      common::databuff_printf(buf, buf_len, pos, "%p:[%s],", iter, common::to_cstring(*iter));
      iter = iter->prev_;
    }
    common::databuff_printf(buf, buf_len, pos, "]");
  }
  return pos;
}

int ObMvccRow::unlink_trans_node(const ObMvccTransNode &node)
{
  int ret = OB_SUCCESS;
  const bool is_server_serving = false;
  ObMvccTransNode **prev = &list_head_;
  ObMvccTransNode *tmp = ATOMIC_LOAD(prev);

  if (!is_server_serving) {
    //处于宕机重启阶段，为了优化热点行的快速回滚的性能，直接操作node的prev和next node
    if (&node == ATOMIC_LOAD(&list_head_)) {
      prev = &list_head_;
    } else if (NULL == node.next_ || NULL == list_head_) {
      ret = OB_ERR_UNEXPECTED;
      // TODO(handora.qc): teemproary remove
      // TRANS_LOG(ERROR, "unexpected transaciton node", K(ret), K(node), K(*this));
    } else {
      prev = &(node.next_->prev_);
    }
  } else {
    //本机已经正常提供服务，摘链表的操作依然从list_head开始，方便做异常校验
    while (OB_SUCCESS == ret && NULL != tmp && (&node) != tmp) {
      if (NDT_COMPACT == tmp->type_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet compact node when unlink trans node",
                  K(ret), K(*this), K(*tmp), K(node));
        //T2->T1,如果T2提前解锁，是可能被设置上barrier的，后续T1先回滚，可能触发这里的防御
      } else {
        if (!tmp->is_elr() && tmp->is_safe_read_barrier()) {
          // ignore ret
          TRANS_LOG(ERROR, "meet safe read barrier when unlink trans node",
                    K(*this), K(*tmp), K(node));
        }
        prev = &(tmp->prev_);
        tmp = ATOMIC_LOAD(prev);
      }
    }
    if (OB_SUCC(ret) && (&node) != tmp) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "can not find trans node", K(ret), K(node), K(*this));
    }
  }
  if (OB_SUCC(ret)) {
    if (&node == ATOMIC_LOAD(&list_head_)) {
      // pass
    } else {
      if (ObDmlFlag::DF_LOCK == node.get_dml_flag()) {
      } else if (EXECUTE_COUNT_PER_SEC(100)) {
        TRANS_LOG(WARN, "unlink middle trans node", K(node), K(*this));
      }
    }
    ATOMIC_STORE(prev, ATOMIC_LOAD(&(node.prev_)));
    if (NULL != ATOMIC_LOAD(&(node.prev_))) {
      ATOMIC_STORE(&(node.prev_->next_), ATOMIC_LOAD(&(node.next_)));
    }
    if (NULL != index_) {
      //修改凡是执行该node的index node位置
      for (int64_t i = 0; i < common::REPLAY_TASK_QUEUE_SIZE; ++i) {
        if (&node == index_->get_index_node(i)) {
          index_->set_index_node(i, ATOMIC_LOAD(&(node.prev_)));
          if (NULL == node.prev_ && TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
            TRANS_LOG(INFO, "reset index node success", K(i), K(node), K(*this));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      //减少当前行上trans_node的总数量
      total_trans_node_cnt_--;
    }
  }
  return ret;
}

bool ObMvccRow::need_compact(const bool for_read, const bool for_replay)
{
  bool bool_ret = false;
  const int32_t updates = ATOMIC_LOAD(&update_since_compact_);
  const int32_t compact_trigger = (for_read || for_replay)
      ? ObServerConfig::get_instance().row_compaction_update_limit * 3
      : ObServerConfig::get_instance().row_compaction_update_limit;

  //备机热点行row compact频率需要降低
  if (NULL != index_ && for_replay) {
    // 热点行场景
    if (updates >= max(2048, ObServerConfig::get_instance().row_compaction_update_limit * 10)) {
      bool_ret = ATOMIC_BCAS(&update_since_compact_, updates, 0);
    }
  } else if (updates >= compact_trigger) {
    bool_ret = ATOMIC_BCAS(&update_since_compact_, updates, 0);
  } else {
    // do nothing
  }

  return bool_ret;
}

int ObMvccRow::row_compact(ObMemtable *memtable,
                           const SCN snapshot_version,
                           ObIAllocator *node_alloc)
{
  int ret = OB_SUCCESS;
  if (!snapshot_version.is_valid() || NULL == node_alloc || NULL == memtable) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(snapshot_version),
              KP(node_alloc), KP(memtable));
  } else {
    ObMemtableRowCompactor row_compactor;
    if (OB_FAIL(row_compactor.init(this, memtable, node_alloc))) {
      TRANS_LOG(WARN, "row compactor init error", K(ret));
    } else if (OB_FAIL(row_compactor.compact(snapshot_version,
                                             ObMvccTransNode::COMPACT_READ_BIT))) {
      TRANS_LOG(WARN, "row compact error", K(ret), K(snapshot_version));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObMvccRow::insert_trans_node(ObIMvccCtx &ctx,
                                 ObMvccTransNode &node,
                                 common::ObIAllocator &allocator,
                                 ObMvccTransNode *&next_node)
{
  int ret = OB_SUCCESS;
  if (NULL != latest_compact_node_
      && OB_UNLIKELY(node.scn_ <= latest_compact_node_->scn_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid node", K(ret), K(node), K(*latest_compact_node_), K(*this));
  } else {
    next_node = NULL;
    int64_t search_steps = 0;
    const int64_t replay_queue_index = get_replay_queue_index();
    const bool is_re_thread = ObMvccRowIndex::is_valid_queue_index(replay_queue_index);
    if (!is_re_thread && NULL != index_) {
      index_->reset();
      if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        TRANS_LOG(INFO, "reset index node success", K(replay_queue_index), K(node), K(*this));
      }
    }
    ObMvccTransNode *index_node = NULL;
    if (is_re_thread
        && NULL != index_
        && NULL != (index_node = (index_->get_index_node(replay_queue_index)))) {
      if (index_node->is_aborted()) {
        index_->set_index_node(replay_queue_index, NULL);
        index_node = NULL;
        TRANS_LOG(ERROR, "unexpected transaction node state", K(ctx), K(replay_queue_index), K(node));
      }
    }
    if (NULL != index_node && is_re_thread) {
      if (index_node->is_aborted()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected index node", K(ret), K(*index_node), K(node), K(ctx), K(*this));
      } else {
        ObMvccTransNode *prev = index_node;
        ObMvccTransNode *next = index_node->next_;
        while (OB_SUCC(ret) && NULL != next && next->scn_ < node.scn_) {
          prev = next;
          next = next->next_;
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(prev->scn_ > node.scn_ || prev->trans_version_ > node.trans_version_)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "meet unexpected index_node", KR(ret), K(*prev), K(node), K(*index_node), K(*this));
            abort_unless(0);
          } else if (prev->tx_id_ == node.tx_id_ && OB_UNLIKELY(prev->seq_no_ > node.seq_no_)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "prev node seq_no > this node", KR(ret), KPC(prev), K(node), KPC(this));
            usleep(1000);
            ob_abort();
          } else {
            next_node = next;
            ATOMIC_STORE(&(node.next_), next);
            ATOMIC_STORE(&(node.prev_), prev);
            ATOMIC_STORE(&(prev->next_), &node);
            if (NULL != next) {
              ATOMIC_STORE(&(next->prev_), &node);
            }
            if (prev == list_head_) {
              ATOMIC_STORE(&(list_head_), &node);
            }
            node.clear_aborted();
            index_->set_index_node(replay_queue_index, &node);
          }
        }
      }
    } else {
      ObMvccTransNode **prev = &list_head_;
      ObMvccTransNode *tmp = ATOMIC_LOAD(prev);
      while (OB_SUCC(ret) && NULL != tmp && tmp->scn_ > node.scn_) {
        ++search_steps;
        if (NDT_COMPACT == tmp->type_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "meet compact node when insert trans node",
                    K(ret), K(*this), K(*tmp), K(node));
        } else {
          if (tmp->is_safe_read_barrier()) {
            // ignore ret
            TRANS_LOG(ERROR, "meet safe read barrier when insert trans node",
                      K(*this), K(*tmp), K(node));
          }
          next_node = tmp;
          prev = &(tmp->prev_);
          tmp = ATOMIC_LOAD(prev);
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(tmp) && tmp->tx_id_ == node.tx_id_) {
        if (OB_UNLIKELY(tmp->seq_no_ > node.seq_no_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "prev node seq_no > this node", KR(ret), "prev", PC(tmp), K(node), KPC(this));
          usleep(1000);
          ob_abort();
        }
      }
      if (OB_SUCC(ret)) {
        ATOMIC_STORE(&(node.prev_), tmp);
        ATOMIC_STORE(prev, &node);
        ATOMIC_STORE(&(node.next_), next_node);
        node.clear_aborted();
        if (NULL != tmp) {
          ATOMIC_STORE(&(tmp->next_), &node);
        }

        if (OB_SUCC(ret) && is_re_thread) {
          int tmp_ret = OB_SUCCESS;
          if (NULL != index_ || search_steps > INDEX_TRIGGER_COUNT) {
            if (NULL == index_) {
              void *buf = NULL;
              if (OB_UNLIKELY(NULL == (buf = allocator.alloc(sizeof(*index_))))) {
                tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
                TRANS_LOG(WARN, "failed to alloc ObMvccRowIndex", K(ret));
              } else if (NULL == (index_ = new(buf) ObMvccRowIndex())) {
                TRANS_LOG(WARN, "failed to construct ObMvccRowIndex", K(ret));
                tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {/*do nothing*/}
            }
            if (OB_SUCC(ret) && OB_SUCCESS == tmp_ret) {
              index_->set_index_node(replay_queue_index, &node);
              if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
                TRANS_LOG(INFO, "set index node success", K(replay_queue_index), K(node), K(*this));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      total_trans_node_cnt_++;
    }
  }
  return ret;
}

bool ObMvccRow::is_transaction_set_violation(const SCN snapshot_version)
{
  return max_trans_version_.atomic_load() > snapshot_version
    || max_elr_trans_version_.atomic_load() > snapshot_version;
}

int ObMvccRow::elr(const ObTransID &tx_id,
                   const SCN elr_commit_version,
                   const ObTabletID &tablet_id,
                   const ObMemtableKey* key)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *iter = get_list_head();
  if (NULL != iter
      && !iter->is_elr()
      && !iter->is_committed()
      && !iter->is_aborted()) {
    while (NULL != iter && OB_SUCC(ret)) {
      if (tx_id != iter->tx_id_) {
        break;
      } else if (SCN::max_scn() != iter->trans_version_ && iter->trans_version_ > elr_commit_version) {
        // leader revoke
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected transaction version", K(*iter), K(elr_commit_version));
      } else {
        iter->trans_version_ = elr_commit_version;
        iter->set_elr();
        iter = iter->prev_;
      }
    }
    max_elr_trans_version_.inc_update(elr_commit_version);
    // TODO shanyan.g
    if (NULL != key) {
      wakeup_waiter(tablet_id, *key);
    } else {
      ObLockWaitMgr *lwm = NULL;
      if (OB_ISNULL(lwm = MTL(ObLockWaitMgr*))) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "MTL(LockWaitMgr) is null", K(ret), KPC(this));
      } else {
        lwm->wakeup(tx_id);
      }
    }
  }
  return ret;
}

SCN ObMvccRow::get_max_trans_version() const
{
  const SCN max_elr_commit_version = max_elr_trans_version_.atomic_get();
  const SCN max_trans_version = max_trans_version_.atomic_get();
  return MAX(max_elr_commit_version, max_trans_version);
}

void ObMvccRow::update_max_trans_version(const SCN max_trans_version,
                                         const transaction::ObTransID &tx_id)
{
  SCN v = max_trans_version_.inc_update(max_trans_version);
  if (v == max_trans_version) { max_trans_id_ = tx_id; }
}

void ObMvccRow::update_max_elr_trans_version(const SCN max_trans_version,
                                             const transaction::ObTransID &tx_id)
{
  SCN v = max_elr_trans_version_.inc_update(max_trans_version);
  if (v == max_trans_version) { max_elr_trans_id_ = tx_id; }
}

int ObMvccRow::trans_commit(const SCN commit_version, ObMvccTransNode &node)
{
  int ret = OB_SUCCESS;

  if (!commit_version.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(commit_version), K(*this));
  } else {
    // Check safety condition for ELR
    if (NULL != node.prev_ && node.prev_->is_safe_read_barrier()) {
      int64_t snapshot_version_barrier = 0;
      int64_t flag = 0;
      (void)node.prev_->get_snapshot_version_barrier(snapshot_version_barrier, flag);
      if (commit_version.get_val_for_tx() <= snapshot_version_barrier) {
        // TODO (yangyifei.yyf): Modify the defensive from trans_node to mvcc_row
        if (node.prev_->type_ == NDT_COMPACT) {
          // do nothing
        } else {
          // ignore ret
          TRANS_LOG(ERROR, "unexpected commit version", K(snapshot_version_barrier),
                    "cur_node", node, "prev_node", *(node.prev_), K(flag), K(*this),
                    K(commit_version));
        }
      }
    }

    update_dml_flag_(node.get_dml_flag(),
                     node.get_scn());
    update_max_trans_version(commit_version, node.tx_id_);
    update_max_elr_trans_version(commit_version, node.tx_id_);
  }

  return ret;
}

void ObMvccRow::update_dml_flag_(const blocksstable::ObDmlFlag flag, const share::SCN modify_scn)
{
  if (blocksstable::ObDmlFlag::DF_LOCK != flag) {
    if (!max_modify_scn_.is_valid() || max_modify_scn_ <= modify_scn) {
      max_modify_scn_ = modify_scn;
      last_dml_flag_ = flag;
    }

    if (!min_modify_scn_.is_valid() || min_modify_scn_ > modify_scn) {
      min_modify_scn_ = modify_scn;
      first_dml_flag_ = flag;
    }
  }
}

int ObMvccRow::remove_callback(ObMvccRowCallback &cb)
{
  int ret = OB_SUCCESS;

  ObMvccTransNode *node = cb.get_trans_node();
  if (OB_NOT_NULL(node)) {
    node->remove_callback();
    if (OB_ISNULL(MTL(ObLockWaitMgr*))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "MTL(LockWaitMgr) is null", K(ret), KPC(this));
    } else {
      auto tx_ctx = cb.get_trans_ctx();
      ObAddr tx_scheduler;
      if (OB_ISNULL(tx_ctx)) {
        int tmp_ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "trans ctx is null", KR(tmp_ret), K(cb));
      } else {
        tx_scheduler = static_cast<transaction::ObPartTransCtx*>(tx_ctx)->get_scheduler();
      }
      MTL(ObLockWaitMgr*)->transform_row_lock_to_tx_lock(cb.get_tablet_id(), *cb.get_key(), ObTransID(node->tx_id_), tx_scheduler);
      if (cb.is_non_unique_local_index_cb()) {
        // row lock holder is no need to set for non-unique local index, so the reset can be skipped
      } else {
        MTL(ObLockWaitMgr*)->reset_hash_holder(cb.get_tablet_id(), *cb.get_key(), ObTransID(node->tx_id_));
      }
    }
  }
  return ret;
}

/*
 * wakeup_waiter - wakeup whom waiting to acquire the
 *                 ownership of this row to write
 */
int ObMvccRow::wakeup_waiter(const ObTabletID &tablet_id,
                             const ObMemtableKey &key)
{
  int ret = OB_SUCCESS;
  ObLockWaitMgr *lwm = NULL;
  if (OB_ISNULL(lwm = MTL(ObLockWaitMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "MTL(LockWaitMgr) is null", K(ret), KPC(this));
  } else {
    lwm->wakeup(tablet_id, key);
  }
  return ret;
}

int ObMvccRow::mvcc_write_(ObStoreCtx &ctx,
                           ObMvccTransNode &writer_node,
                           const transaction::ObTxSnapshot &snapshot,
                           ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(latch_);
  ObMvccTransNode *iter = ATOMIC_LOAD(&list_head_);
  ObTransID writer_tx_id = ctx.mvcc_acc_ctx_.get_tx_id();
  const SCN snapshot_version = snapshot.version_;
  const ObTxSEQ reader_seq_no = snapshot.scn_;
  bool &can_insert = res.can_insert_;
  bool &need_insert = res.need_insert_;
  bool &is_new_locked = res.is_new_locked_;
  ObStoreRowLockState &lock_state = res.lock_state_;
  ObExistFlag &exist_flag = lock_state.exist_flag_;
  bool need_retry = true;

  while (OB_SUCC(ret) && need_retry) {
    if (OB_ISNULL(iter)) {
      // Case 1: head is empty, so we set node to be the new head
      can_insert = true;
      need_insert = true;
      is_new_locked = true;
      exist_flag = ObExistFlag::UNKNOWN;
      need_retry = false;
    } else {
      // Tip 1: The newest node is either delayed cleanout or not depending on
      //        whether the node's callback has been removed. If it is delayed
      //        cleanout and not decided, the lock state of the node cannot be
      //        updated synchronously, so we need cleanout it using tx_table.
      //        Otherwise the lock state must be updated synchronously(through
      //        committing the node after submitting commit log or aborting the
      //        node after abort), so we can rely on the lock state of the node
      //        directly.
      //
      // NB: You need notice the lock state for write operation(mvcc_write) and
      // read operation(lock_for_read) is different. And we can not directly rely
      // on the lock state of the node even the node is not delayed cleanout for
      // read operation.(If you are intereted in it, read ObMvccRow::mvcc_write)
      ObTransID data_tx_id = iter->get_tx_id();

      if (iter->is_delayed_cleanout() && !(iter->is_committed() || iter->is_aborted()) &&
          OB_FAIL(ctx.mvcc_acc_ctx_.get_tx_table_guards()
                     .tx_table_guard_
                     .cleanout_tx_node(data_tx_id, *this, *iter, false /*need_row_latch*/))) {
        TRANS_LOG(WARN, "cleanout tx state failed", K(ret), K(*this));
      } else if (iter->is_committed() || iter->is_elr()) {
        // Case 2: the newest node is decided, so we can insert into it
        can_insert = true;
        need_insert = true;
        is_new_locked = true;
        exist_flag =
          extract_exist_flag_from_dml_flag(iter->get_dml_flag());
        need_retry = false;
      } else if (iter->is_aborted()) {
        // Case 3: the newest node is aborted and the node must be unlinked,
        //         so we need look for the next one
        iter = iter->prev_;
        need_retry = true;
      } else if (data_tx_id == writer_tx_id) {
        // Case 4: the newest node is not decided and locked by itself, so we
        //         can insert into it
        bool is_lock_node = false;
        if (OB_FAIL(writer_node.is_lock_node(is_lock_node))) {
          TRANS_LOG(ERROR, "get is lock node failed", K(ret), K(writer_node));
        } else if (is_lock_node) {
          // Case 4.1: the writer node is lock node, so we do not insert into it
          // bacause it has already been locked
          can_insert = true;
          need_insert = false;
          is_new_locked = false;
          exist_flag =
            extract_exist_flag_from_dml_flag(iter->get_dml_flag());
          need_retry = false;
        } else {
          // Case 4.2: the writer node is not lock node, so we do not insert into it
          can_insert = true;
          need_insert = true;
          is_new_locked = false;
          exist_flag =
            extract_exist_flag_from_dml_flag(iter->get_dml_flag());
          need_retry = false;
        }
      } else {
        // Case 5: the newest node is not decided and locked by other, so we
        //         cannot insert into it
        can_insert = false;
        need_insert = false;
        is_new_locked = false;
        need_retry = false;
        lock_state.is_locked_ = true;
        lock_state.lock_trans_id_ = data_tx_id;
        lock_state.lock_data_sequence_ = iter->get_seq_no();
        lock_state.lock_dml_flag_ = iter->get_dml_flag();
        lock_state.is_delayed_cleanout_ = iter->is_delayed_cleanout();
        lock_state.mvcc_row_ = this;
        lock_state.trans_scn_ = iter->get_scn();
        exist_flag =
          extract_exist_flag_from_dml_flag(iter->get_dml_flag());
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (can_insert && need_insert) {
      if (nullptr != list_head_ &&
          OB_FAIL(concurrent_control::check_sequence_set_violation(ctx.mvcc_acc_ctx_.write_flag_,
                                                                   reader_seq_no,
                                                                   writer_tx_id,
                                                                   writer_node.get_dml_flag(),
                                                                   writer_node.get_seq_no(),
                                                                   list_head_->get_tx_id(),
                                                                   list_head_->get_dml_flag(),
                                                                   list_head_->get_seq_no()))) {
        TRANS_LOG(WARN, "check sequence set violation failed", K(ret), KPC(this));
      } else if (nullptr != list_head_ && FALSE_IT(res.is_checked_ = true)) {
      } else if (OB_SUCC(check_double_insert_(snapshot_version,
                                              writer_node,
                                              list_head_))) {
        ATOMIC_STORE(&(writer_node.prev_), list_head_);
        ATOMIC_STORE(&(writer_node.next_), NULL);
        if (NULL != list_head_) {
          ATOMIC_STORE(&(list_head_->next_), &writer_node);
        }
        ATOMIC_STORE(&(list_head_), &writer_node);

        if (NULL != writer_node.prev_) {
          writer_node.modify_count_ = writer_node.prev_->modify_count_ + 1;
        } else {
          writer_node.modify_count_ = 0;
        }

        res.tx_node_ = &writer_node;
        total_trans_node_cnt_++;
      }
      if (NULL != writer_node.prev_
          && writer_node.prev_->is_elr()) {
        if (NULL != ctx.mvcc_acc_ctx_.tx_ctx_) {
          TX_STAT_READ_ELR_ROW_COUNT_INC(ctx.mvcc_acc_ctx_.tx_ctx_->get_tenant_id());
        }
      }
    }
  }

  return ret;
}

__attribute__((noinline))
int ObMvccRow::check_double_insert_(const SCN snapshot_version,
                                    ObMvccTransNode &node,
                                    ObMvccTransNode *prev)
{
  int ret = OB_SUCCESS;

  if (NULL != prev) {
    if (blocksstable::ObDmlFlag::DF_INSERT == node.get_dml_flag()
        && blocksstable::ObDmlFlag::DF_DELETE != prev->get_dml_flag()
        && prev->is_committed()
        && snapshot_version >= prev->trans_version_) {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      TRANS_LOG(WARN, "find double insert node", K(ret), K(node), KPC(prev), K(snapshot_version), K(*this));
    }
  }

  return ret;
}

void ObMvccRow::mvcc_undo()
{
  ObRowLatchGuard guard(latch_);
  ObMvccTransNode *iter = ATOMIC_LOAD(&list_head_);

  if (OB_ISNULL(iter)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "mvcc undo with no mvcc data");
  } else {
    iter->set_aborted();
    ATOMIC_STORE(&(list_head_), iter->prev_);
    if (NULL != iter->prev_) {
      ATOMIC_STORE(&(iter->prev_->next_), NULL);
    }
    total_trans_node_cnt_--;
  }
}

int ObMvccRow::mvcc_write(ObStoreCtx &ctx,
                          const transaction::ObTxSnapshot &snapshot,
                          ObMvccTransNode &node,
                          ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  const SCN snapshot_version = snapshot.version_;
  if (max_trans_version_.atomic_load() > snapshot_version
      || max_elr_trans_version_.atomic_load() > snapshot_version) {
    // Case 3. successfully locked while tsc
    ret = OB_TRANSACTION_SET_VIOLATION;
    TRANS_LOG(WARN, "transaction set violation", K(ret),
              K(snapshot_version), "txNode_to_write", node,
              "memtableCtx", ctx, "mvccRow", PC(this));
  } else if (OB_FAIL(mvcc_write_(ctx,
                                 node,
                                 snapshot,
                                 res))) {
    TRANS_LOG(WARN, "mvcc write failed", K(ret), K(node), K(ctx));
  } else if (!res.can_insert_) {
    // Case1: Cannot insert because of write-write conflict
    ret = OB_TRY_LOCK_ROW_CONFLICT;
    TRANS_LOG(WARN, "mvcc write conflict", K(ret), K(ctx), K(node), K(res), K(*this));
  } else if (max_trans_version_.atomic_load() > snapshot_version
             || max_elr_trans_version_.atomic_load() > snapshot_version) {
    // Case 3. successfully locked while tsc
    ret = OB_TRANSACTION_SET_VIOLATION;
    TRANS_LOG(WARN, "transaction set violation", K(ret), K(ctx), K(node), K(*this));
    if (!res.has_insert()) {
      TRANS_LOG(ERROR, "TSC will occurred when already inserted", K(ctx), K(node), KPC(this));
    } else {
      // Tip1: mvcc_write guarantee the tnode will not be inserted if error is reported
      (void)mvcc_undo();
    }
  } else if (node.get_dml_flag() == blocksstable::ObDmlFlag::DF_INSERT &&
             res.lock_state_.row_exist()) {
    // Case 4. successfully locked while insert into exist row
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    TRANS_LOG(WARN, "duplicated primary key found", K(ret), K(ctx), K(node),
              K(*this), K(res));
    if (!res.has_insert()) {
      // It may not inserted due to primary key duplicated
    } else {
      // Tip1: mvcc_write guarantee the tnode will not be inserted if error is reported
      (void)mvcc_undo();
    }
  }
  return ret;
}

/*
 * check_row_locked - check row was locked by an active txn
 *
 * @ctx: the current txn's context
 * @lock_state: information feedback about row's lock state
 *              - is_locked: indicate row's whether locked or not by some txn
 *              - lock_trans_id: the txn who hold the lock
 *              - is_delayed_cleanout: used to decide waiting on row or txn(if true)
 *              - lock_data_sequence: the TxNode's seq_no, used to recheck lockstate
 *                                    when is_delayed_cleanout is true
 *              - mvcc_row: the ObMvccRow which used to recheck lockstate when
 *                          is_delayed_cleanout is false
 * return:
 * - OB_SUCCESS
 */
int ObMvccRow::check_row_locked(ObMvccAccessCtx &ctx,
                                ObStoreRowLockState &lock_state,
                                ObRowState &row_state)
{
  int ret = OB_SUCCESS;
  ObRowLatchGuard guard(latch_);
  transaction::ObTxSnapshot &snapshot = ctx.snapshot_;
  const SCN snapshot_version = snapshot.version_;
  const ObTransID checker_tx_id = ctx.get_tx_id();
  ObMvccTransNode *iter = ATOMIC_LOAD(&list_head_);
  bool need_retry = true;

  while (OB_SUCC(ret) && need_retry) {
    if (OB_ISNULL(iter)) {
      // Case 1: head is empty, so node currently is not be locked
      lock_state.is_locked_ = false;
      lock_state.trans_version_.set_min();
      lock_state.lock_trans_id_.reset();
      lock_state.exist_flag_ = ObExistFlag::UNKNOWN;
      need_retry = false;
    } else {
      auto data_tx_id = iter->tx_id_;
      if (!(iter->is_committed() || iter->is_aborted())
          && iter->is_delayed_cleanout()
          && OB_FAIL(ctx.get_tx_table_guards().cleanout_tx_node(data_tx_id,
                                                                *this,
                                                                *iter,
                                                                false  /*need_row_latch*/))) {
        TRANS_LOG(WARN, "cleanout tx state failed", K(ret), K(*this));
      } else if (iter->is_committed() || iter->is_elr()) {
        // Case 2: the newest node is decided, so node currently is not be locked
        lock_state.is_locked_ = false;
        lock_state.trans_version_ = get_max_trans_version();
        lock_state.lock_trans_id_.reset();
        lock_state.exist_flag_ =
          extract_exist_flag_from_dml_flag(iter->get_dml_flag());
        need_retry = false;
      } else if (iter->is_aborted()) {
        iter = iter->prev_;
        need_retry = true;
      } else {
        lock_state.is_locked_ = true;
        lock_state.trans_version_.set_min();
        lock_state.lock_trans_id_= data_tx_id;
        lock_state.lock_data_sequence_ = iter->get_seq_no();
        lock_state.lock_dml_flag_ = iter->get_dml_flag();
        lock_state.is_delayed_cleanout_ = iter->is_delayed_cleanout();
        lock_state.trans_scn_ = iter->get_scn();
        lock_state.exist_flag_ =
          extract_exist_flag_from_dml_flag(iter->get_dml_flag());
        need_retry = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    lock_state.mvcc_row_ = this;

    // just for temporary enable the batch insert, so the following code will be
    // optimized in the future
    if (!lock_state.is_lock_decided()) {
      // row is not exist
    } else if (lock_state.is_locked(checker_tx_id) ||
               lock_state.trans_version_ > snapshot_version) {
      // row is locked or tsc
    } else {
      if (OB_NOT_NULL(iter)) {
        row_state.row_dml_flag_ = iter->get_dml_flag();
      }
    }
  }
  return ret;
}

void ObMvccRow::print_row()
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow datum_row;
  blocksstable::ObRowReader row_reader;
  ObMvccRow *row = this;
  TRANS_LOG(INFO, "qianchen print row", K(*row));
  for (ObMvccTransNode *node = row->get_list_head(); OB_SUCC(ret) && OB_NOT_NULL(node); node = node->prev_) {
    const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(node->buf_);
    TRANS_LOG(INFO, "qianchen row: ", K(*node), K(mtd));
    if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, datum_row))) {
      CLOG_LOG(WARN, "Failed to read datum row", K(ret));
    } else {
      TRANS_LOG(INFO, "    qianchen datum row: ", K(datum_row));
    }
  }
}
}; // end namespace mvcc
}; // end namespace oceanbase
