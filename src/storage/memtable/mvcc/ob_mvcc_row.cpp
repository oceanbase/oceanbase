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
#include "storage/transaction/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/transaction/ob_trans_ctx.h"

namespace oceanbase {
using namespace storage;
using namespace transaction;
using namespace common;
using namespace share;
namespace memtable {

void ObMvccTransNode::checksum(ObBatchChecksum& bc) const
{
  bc.fill(&modify_count_, sizeof(modify_count_));
  bc.fill(&type_, sizeof(type_));
}

uint32_t ObMvccTransNode::m_cal_acc_checksum(const uint32_t last_acc_checksum) const
{
  uint32_t acc_checksum = 0;
  ObBatchChecksum bc;
  bc.fill(&last_acc_checksum, sizeof(last_acc_checksum));
  ((ObMemtableDataHeader*)buf_)->checksum(bc);
  acc_checksum = static_cast<uint32_t>((bc.calc() ?: 1) & 0xffffffff);
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
      TRANS_LOG(ERROR,
          "row checksum error",
          K(ret),
          K(last_acc_checksum),
          "save_acc_checksum",
          acc_checksum_,
          "cal_acc_checksum",
          acc_checksum,
          K(*this));
      const uint32_t test_checksum = 0;
      if (acc_checksum_ == m_cal_acc_checksum(test_checksum)) {
        TRANS_LOG(INFO, "test checksum success", K(*this));
      }
    }
  }
  return ret;
}

ObRowDml ObMvccTransNode::get_dml_type() const
{
  return reinterpret_cast<const ObMemtableDataHeader*>(buf_)->dml_type_;
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

void ObMvccTransNode::set_elr()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag | F_ELR);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_committed()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag | F_COMMITTED);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_relocated()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag | F_RELOCATED);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_mvcc_row_cb(ObMvccRowCallback* mvcc_row_cb)
{
  ATOMIC_STORE(&mvcc_row_cb_, mvcc_row_cb);
}

const ObMvccRowCallback* ObMvccTransNode::get_mvcc_row_cb() const
{
  ObMvccRowCallback* mvcc_row_cb = ATOMIC_LOAD(&mvcc_row_cb_);
  if ((uint64_t)mvcc_row_cb & 1UL) {
    mvcc_row_cb = NULL;
  }
  return mvcc_row_cb;
}

// thread unsafe, need lock ObMvccRow before
// return OB_TRANS_HAS_DECIDED when transaction is decided
int ObMvccTransNode::get_trans_id_and_sql_no(ObTransID& trans_id, int64_t& sql_no)
{
  int ret = OB_SUCCESS;

  if (NULL == get_mvcc_row_cb() || is_committed() || is_aborted()) {
    ret = OB_TRANS_HAS_DECIDED;
  } else if (is_delayed_cleanout()) {
    ret = get_trans_info_from_ctx_(trans_id, sql_no);
  } else {
    ret = get_trans_info_from_cb_(trans_id, sql_no);
  }

  return ret;
}

int ObMvccTransNode::get_trans_info_from_cb_(ObTransID& trans_id, int64_t& sql_no)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(get_mvcc_row_cb()->get_trans_id(trans_id))) {
    TRANS_LOG(WARN, "get trans id error", K(ret));
  } else {
    sql_no = get_mvcc_row_cb()->get_sql_no();
  }

  return ret;
}

int ObMvccTransNode::get_trans_info_from_ctx_(ObTransID& trans_id, int64_t& sql_no)
{
  int ret = OB_SUCCESS;

  ObTransCtx* ctx = NULL;
  if (OB_FAIL(get_trans_ctx(ctx))) {
    TRANS_LOG(ERROR, "fail to get trans ctx", K(ret), K(*this));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_NULL_VALUE;
    TRANS_LOG(ERROR, "ctx is NULL", K(ret), K(*this));
  } else {
    trans_id = ctx->get_trans_id();
    sql_no = get_sql_sequence();
  }

  return ret;
}

int ObMvccTransNode::get_trans_ctx(ObTransCtx*& ctx)
{
  int ret = OB_SUCCESS;

  if (is_aborted()) {
    ctx = NULL;
  } else if ((!is_committed() && !is_delayed_cleanout())) {
    ret = OB_EAGAIN;
  } else {
    while (NULL == (ctx = trans_ctx_)) {
      PAUSE();
      WEAK_BARRIER();
    }

    if ((uint64_t)ctx & 1UL) {
      ctx = (ObTransCtx*)((uint64_t)ctx ^ 1UL);
    } else {
      ctx = NULL;
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "meet cb when get trx ctx", K(ret));
    }
  }

  return ret;
}

void ObMvccTransNode::clear_aborted()
{
  const uint8_t consistent_flag = F_ABORTED;
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag & (~consistent_flag));
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_aborted()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag | F_ABORTED);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_overflow(const bool overflow)
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = overflow ? flag | F_OVERFLOW : flag & ~F_OVERFLOW;
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

void ObMvccTransNode::set_delayed_cleanout(const bool delayed_cleanout)
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = delayed_cleanout ? flag | F_DELAYED_CLEANOUT : flag & ~F_DELAYED_CLEANOUT;
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

bool ObMvccTransNode::is_delayed_cleanout() const
{
  return ATOMIC_LOAD(&flag_) & F_DELAYED_CLEANOUT;
}

int ObMvccTransNode::set_ctx_descriptor(const uint32_t ctx_descriptor)
{
  ctx_descriptor_ = ctx_descriptor;
  return OB_SUCCESS;
}

uint32_t ObMvccTransNode::get_ctx_descriptor() const
{
  return ctx_descriptor_;
}

int ObMvccTransNode::fill_trans_version(const int64_t version)
{
  trans_version_ = version;
  return OB_SUCCESS;
}

int ObMvccTransNode::is_lock_node(bool& is_lock) const
{
  int ret = OB_SUCCESS;

  const ObMemtableDataHeader* mtd = reinterpret_cast<const ObMemtableDataHeader*>(buf_);
  if (NULL == mtd) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, mtd is NULL", K(ret));
  } else if (T_DML_LOCK == mtd->dml_type_) {
    is_lock = true;
  } else {
    is_lock = false;
  }

  return ret;
}

void ObMvccTransNode::trans_commit()
{
  ObTransCtx* tmp = get_mvcc_row_cb()->get_trans_ctx();
  if (OB_NOT_NULL(tmp) && !((ObPartTransCtx*)tmp)->is_dirty_trans()) {
    tmp = NULL;
  }

  set_mvcc_row_cb(NULL);
  set_committed();
  set_trans_ctx_(tmp);
}

void ObMvccTransNode::remove_callback()
{
  ObTransCtx* tmp = get_mvcc_row_cb()->get_trans_ctx();
  set_mvcc_row_cb(NULL);
  set_delayed_cleanout(true);
  set_trans_ctx_(tmp);
}

void ObMvccTransNode::set_trans_ctx_(ObTransCtx* ctx)
{
  ATOMIC_STORE(&trans_ctx_, (ObTransCtx*)((uint64_t)ctx | 1UL));
}

int ObMvccTransNode::try_cleanout(const ObMvccRow* value)
{
  int ret = OB_SUCCESS;

  if (!GCONF._enable_fast_commit) {
  } else if (is_delayed_cleanout()) {
    TransNodeMutexGuard guard(*this);
    if (is_delayed_cleanout()) {

      bool cleanout_finish = false;
      ObTransCtx* ctx = NULL;

      if (OB_FAIL(get_trans_ctx(ctx))) {
        TRANS_LOG(ERROR, "fail to get trans ctx", K(ret), K(*this));
      } else if (OB_ISNULL(ctx)) {
        ret = OB_ERR_NULL_VALUE;
        TRANS_LOG(ERROR, "ctx is NULL", K(ret), K(*this));
      } else if (OB_FAIL(((ObPartTransCtx*)ctx)
                             ->cleanout_transnode(*this, *const_cast<ObMvccRow*>(value), cleanout_finish))) {
        TRANS_LOG(ERROR, "cleanout transnode failed", K(ret), K(*this));
      }

      if (OB_SUCC(ret) && cleanout_finish) {
        set_delayed_cleanout(false);
      }
    }
  }

  return ret;
}

int ObMvccTransNode::is_running(bool& is_running)
{
  int ret = OB_SUCCESS;

  if (is_committed() || is_aborted()) {
    is_running = false;
  } else if (is_delayed_cleanout()) {
    ObTransCtx* ctx = NULL;

    if (OB_FAIL(get_trans_ctx(ctx))) {
      TRANS_LOG(ERROR, "fail to get trans ctx", K(ret), K(*this));
    } else if (OB_ISNULL(ctx)) {
      ret = OB_ERR_NULL_VALUE;
      TRANS_LOG(ERROR, "ctx is NULL", K(ret), K(*this));
    } else if (OB_FAIL(((ObPartTransCtx*)ctx)->is_running(is_running))) {
      TRANS_LOG(WARN, "fail to check trans status", K(ret));
    }
  } else {
    is_running = true;
  }

  return ret;
}

void ObMvccTransNode::lock()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    if (!(flag & F_RELOCATED)) {
      const uint8_t tmp = (flag | F_RELOCATED);
      if (ATOMIC_BCAS(&flag_, flag, tmp)) {
        break;
      }
    }

    PAUSE();
  }
}

void ObMvccTransNode::unlock()
{
  while (true) {
    const uint8_t flag = ATOMIC_LOAD(&flag_);
    const uint8_t tmp = (flag & ~F_RELOCATED);
    if (ATOMIC_BCAS(&flag_, flag, tmp)) {
      break;
    }
  }
}

int64_t ObMvccTransNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const ObMemtableDataHeader* mtd = reinterpret_cast<const ObMemtableDataHeader*>(buf_);
  common::databuff_printf(buf,
      buf_len,
      pos,
      "this=%p "
      "trans_version=%ld "
      "log_timestamp=%ld "
      "prev=%p "
      "next=%p "
      "modify_count=%u "
      "ctx_descriptor=%d "
      "acc_checksum=%u "
      "version=%ld "
      "type=%d "
      "inc_num=%ld "
      "flag=%d "
      "snapshot_version_barrier=%ld "
      "mtd=%s",
      this,
      trans_version_,
      log_timestamp_,
      prev_,
      next_,
      modify_count_,
      ctx_descriptor_,
      acc_checksum_,
      version_,
      type_,
      inc_num_,
      flag_,
      snapshot_version_barrier_,
      to_cstring(*mtd));
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

ObMvccTransNode* ObMvccRow::ObMvccRowIndex::get_index_node(const int64_t index) const
{
  ObMvccTransNode* ret_node = NULL;
  if (is_valid_queue_index(index)) {
    ret_node = replay_locations_[index];
  }
  return ret_node;
}

void ObMvccRow::ObMvccRowIndex::set_index_node(const int64_t index, ObMvccTransNode* node)
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
  first_dml_ = T_DML_UNKNOWN;
  last_dml_ = T_DML_UNKNOWN;
  list_head_ = NULL;
  modify_count_ = 0;
  acc_checksum_ = 0;
  max_trans_version_ = 0;
  max_elr_trans_version_ = 0;
  ctx_descriptor_ = 0;
  elr_trans_count_ = 0;
  latest_compact_node_ = NULL;
  latest_compact_ts_ = 0;
  index_ = NULL;
  total_trans_node_cnt_ = 0;
  first_overflow_node_ = NULL;
}

int64_t ObMvccRow::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf,
      buf_len,
      pos,
      "this=%p "
      "lock=%s "
      "latch_=%s "
      "flag=%hhu "
      "first_dml=%hhd "
      "last_dml=%hhd "
      "update_since_compact=%d "
      "list_head=%p "
      "latest_compact_node=%p "
      "modify_count=%u "
      "acc_checksum=%u "
      "ctx_descriptor=%d "
      "max_trans_version=%ld "
      "max_elr_trans_version=%ld "
      "elr_trans_count=%d "
      "latest_compact_ts=%ld "
      "total_trans_node_cnt=%ld ",
      this,
      to_cstring(row_lock_),
      (latch_.is_locked() ? "locked" : "unlocked"),
      flag_,
      first_dml_,
      last_dml_,
      update_since_compact_,
      list_head_,
      latest_compact_node_,
      modify_count_,
      acc_checksum_,
      ctx_descriptor_,
      max_trans_version_,
      max_elr_trans_version_,
      elr_trans_count_,
      latest_compact_ts_,
      total_trans_node_cnt_);
  return pos;
}

int64_t ObMvccRow::to_string(char* buf, const int64_t buf_len, const bool verbose) const
{
  int64_t pos = 0;
  pos = to_string(buf, buf_len);
  if (verbose) {
    common::databuff_printf(buf, buf_len, pos, " list=[");
    ObMvccTransNode* iter = list_head_;
    while (NULL != iter) {
      common::databuff_printf(buf, buf_len, pos, "%p:[%s],", iter, common::to_cstring(*iter));
      iter = iter->prev_;
    }
    common::databuff_printf(buf, buf_len, pos, "]");
  }
  return pos;
}

ObMvccRow& ObMvccRow::operator=(const ObMvccRow& o)
{
  update_since_compact_ = o.update_since_compact_;
  first_dml_ = o.first_dml_;
  last_dml_ = o.last_dml_;
  modify_count_ = o.modify_count_;
  acc_checksum_ = o.acc_checksum_;
  max_trans_version_ = o.max_trans_version_;
  max_elr_trans_version_ = o.max_elr_trans_version_;
  ctx_descriptor_ = o.ctx_descriptor_;

  return *this;
}

void ObMvccRow::undo(const ObMvccRow& undo_value)
{
  update_since_compact_ = undo_value.update_since_compact_;
  // can not undo flag
  // flag_ = undo_value.flag_;
  first_dml_ = undo_value.first_dml_;
  last_dml_ = undo_value.last_dml_;
  modify_count_ = undo_value.modify_count_;
  acc_checksum_ = undo_value.acc_checksum_;
  max_trans_version_ = undo_value.max_trans_version_;
  max_elr_trans_version_ = undo_value.max_elr_trans_version_;
  ctx_descriptor_ = undo_value.ctx_descriptor_;
}

int ObMvccRow::unlink_trans_node(ObIMvccCtx& ctx, const ObMvccTransNode& node, const ObMvccRow& undo_value)
{
  int ret = OB_SUCCESS;
  // const bool is_server_serving = (GCTX.status_ == observer::SS_SERVING);
  const bool is_server_serving = false;
  ObMvccTransNode** prev = &list_head_;
  ObMvccTransNode* tmp = ATOMIC_LOAD(prev);

  if (!is_server_serving) {
    // operate prev node and next node directly for ELR performance when booting
    if (&node == ATOMIC_LOAD(&list_head_)) {
      prev = &list_head_;
    } else if (NULL == node.next_ || NULL == list_head_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected transaciton node", K(ret), K(node), K(*this));
    } else {
      prev = &(node.next_->prev_);
    }
  } else {
    // operate list pointed by list_head for checking exception when server is serving
    while (OB_SUCCESS == ret && NULL != tmp && (&node) != tmp) {
      if (NDT_COMPACT == tmp->type_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "meet compact node when unlink trans node", K(ret), K(ctx), K(*this), K(*tmp), K(node));
      } else {
        // ELR maybe meet safe read barrier
        if (!tmp->is_elr() && tmp->is_safe_read_barrier()) {
          // ignore ret
          TRANS_LOG(ERROR, "meet safe read barrier when unlink trans node", K(ctx), K(*this), K(*tmp), K(node));
        }
        prev = &(tmp->prev_);
        tmp = ATOMIC_LOAD(prev);
      }
    }
    if (OB_SUCC(ret) && (&node) != tmp) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "can not find trans node", K(ret), K(ctx), K(node), K(*this));
    }
  }
  if (OB_SUCC(ret)) {
    if (&node == ATOMIC_LOAD(&list_head_)) {
      undo(undo_value);
    } else {
      if (EXECUTE_COUNT_PER_SEC(100)) {
        TRANS_LOG(WARN, "unlink middle trans node", K(ctx), K(node), K(*this), K(undo_value));
      }
    }
    ATOMIC_STORE(prev, ATOMIC_LOAD(&(node.prev_)));
    if (NULL != ATOMIC_LOAD(&(node.prev_))) {
      ATOMIC_STORE(&(node.prev_->next_), ATOMIC_LOAD(&(node.next_)));
    }
    const_cast<ObMvccTransNode&>(node).set_aborted();
    if (NULL != index_) {
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
      total_trans_node_cnt_--;
    }
  }
  return ret;
}

bool ObMvccRow::is_del(const int64_t version) const
{
  ObMvccTransNode* last = NULL;
  return !row_lock_.is_locked() && NULL != (last = ATOMIC_LOAD(&list_head_)) && T_DML_DELETE == last->get_dml_type() &&
         last->trans_version_ <= version;
}

int64_t ObMvccRow::get_del_version() const
{
  ObMvccTransNode* last = NULL;
  return (!row_lock_.is_locked() && NULL != (last = ATOMIC_LOAD(&list_head_)) && T_DML_DELETE == last->get_dml_type())
             ? last->trans_version_
             : INT64_MAX;
}

bool ObMvccRow::is_partial(const int64_t version) const
{
  bool bool_ret = false;
  bool is_locked = row_lock_.is_locked();
  if (!is_locked && version > max_trans_version_) {
    bool_ret = false;
  } else {
    ObMvccTransNode* last = NULL;
    bool_ret = is_locked || (NULL != (last = ATOMIC_LOAD(&list_head_)) && last->trans_version_ > version);
  }
  return bool_ret;
}

bool ObMvccRow::need_compact(const bool for_read, const bool for_replay)
{
  bool bool_ret = false;
  const int32_t updates = ATOMIC_LOAD(&update_since_compact_);
  const int32_t compact_trigger = (for_read || for_replay)
                                      ? ObServerConfig::get_instance().row_compaction_update_limit * 3
                                      : ObServerConfig::get_instance().row_compaction_update_limit;

  // step down compaction when replaying
  if (NULL != index_ && for_replay) {
    // hot line
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

int ObMvccRow::row_compact(const int64_t snapshot_version, ObIAllocator* node_alloc)
{
  int ret = OB_SUCCESS;
  if (0 >= snapshot_version || NULL == node_alloc) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(snapshot_version), KP(node_alloc));
  } else {
    ObMemtableRowCompactor row_compactor;
    if (OB_FAIL(row_compactor.init(this, node_alloc))) {
      TRANS_LOG(WARN, "row compactor init error", K(ret));
    } else if (OB_FAIL(row_compactor.compact(snapshot_version))) {
      TRANS_LOG(WARN, "row compact error", K(ret), K(snapshot_version));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObMvccRow::insert_trans_node(
    ObIMvccCtx& ctx, ObMvccTransNode& node, common::ObIAllocator& allocator, ObMvccTransNode*& next_node)
{
  int ret = OB_SUCCESS;
  if (NULL != latest_compact_node_ && OB_UNLIKELY(node.trans_version_ <= latest_compact_node_->trans_version_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid node", K(ret), K(node), K(*latest_compact_node_), K(*this));
  } else {
    node.set_ctx_descriptor(ctx.get_ctx_descriptor());
    next_node = NULL;
    int64_t search_steps = 0;
    const int64_t replay_queue_index = get_replay_queue_index();
    const bool is_re_thread = ObMvccRowIndex::is_valid_queue_index(replay_queue_index);
    const bool is_follower = ctx.is_for_replay();
    if (!is_re_thread && NULL != index_) {
      if (is_follower && node.is_relocated()) {
        // do nothing
      } else {
        index_->reset();
        if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          TRANS_LOG(INFO, "reset index node success", K(replay_queue_index), K(node), K(*this));
        }
      }
    }
    ObMvccTransNode* index_node = NULL;
    if (is_re_thread && NULL != index_ && NULL != (index_node = (index_->get_index_node(replay_queue_index)))) {
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
        ObMvccTransNode* prev = index_node;
        ObMvccTransNode* next = index_node->next_;
        while (OB_SUCC(ret) && NULL != next && next->trans_version_ < node.trans_version_) {
          prev = next;
          next = next->next_;
        }

        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(prev->trans_version_ > node.trans_version_)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "meet unexpected index_node", KR(ret), K(*prev), K(node), K(*index_node), K(*this));
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
      ObMvccTransNode** prev = &list_head_;
      ObMvccTransNode* tmp = ATOMIC_LOAD(prev);
      while (OB_SUCC(ret) && NULL != tmp && tmp->trans_version_ > node.trans_version_) {
        ++search_steps;
        if (NDT_COMPACT == tmp->type_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "meet compact node when insert trans node", K(ret), K(*this), K(*tmp), K(node));
        } else {
          if (tmp->is_safe_read_barrier()) {
            // ignore ret
            TRANS_LOG(ERROR, "meet safe read barrier when insert trans node", K(*this), K(*tmp), K(node));
          }
          next_node = tmp;
          prev = &(tmp->prev_);
          tmp = ATOMIC_LOAD(prev);
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
              void* buf = NULL;
              if (OB_UNLIKELY(NULL == (buf = allocator.alloc(sizeof(*index_))))) {
                tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
                TRANS_LOG(WARN, "failed to alloc ObMvccRowIndex", K(ret));
              } else if (NULL == (index_ = new (buf) ObMvccRowIndex())) {
                TRANS_LOG(WARN, "failed to construct ObMvccRowIndex", K(ret));
                tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
              } else { /*do nothing*/
              }
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

void ObMvccRow::set_head(ObMvccTransNode* node)
{
  update_since_compact_++;
  list_head_ = node;
  first_dml_ = node->get_dml_type();
  modify_count_++;
  max_trans_version_ = node->trans_version_;
}

void ObMvccRow::update_row_meta()
{
  modify_count_++;
}

int ObMvccRow::try_wait_row_lock_for_read(ObIMvccCtx& ctx, const ObMemtableKey* key) const
{
  int ret = OB_SUCCESS;
  int64_t version = ctx.get_read_snapshot();
  if (ctx.get_is_safe_read()) {
    // no need wait
  } else if (!row_lock_.is_locked() || ATOMIC_LOAD(&max_trans_version_) > version) {
    // no need wait
  } else {
    uint32_t uid = row_lock_.get_exclusive_uid();
    if (ctx.is_can_elr()) {
      ret = ctx.wait_trans_version_v2(uid, version, key, *this);
    } else {
      ret = ctx.wait_trans_version(uid, version);
    }
    if (OB_FAIL(ret)) {
      // count for trying lock for read
      ctx.inc_lock_for_read_retry_count();
    }
  }
  return ret;
}

bool ObMvccRow::is_transaction_set_violation(const int64_t snapshot_version)
{
  return max_trans_version_ > snapshot_version || max_elr_trans_version_ > snapshot_version;
}

int ObMvccRow::row_elr(const uint32_t descriptor, const int64_t elr_commit_version, const ObMemtableKey* key)
{
  int ret = OB_SUCCESS;
  ObRowLatchGuard guard(latch_);

  if (elr_commit_version <= 0 || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(elr_commit_version), KP(key));
  } else if (ctx_descriptor_ != descriptor) {
    TRANS_LOG(DEBUG, "early release lock by other trans", K(descriptor), K(*key), K(elr_commit_version), K(*this));
  } else if (!row_lock_.is_exclusive_locked_by(descriptor)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected lock state", K(ret), K(descriptor), K(elr_commit_version));
  } else if (OB_FAIL(elr(descriptor, elr_commit_version, key))) {
    TRANS_LOG(WARN, "early lock release error", K(ret), K(descriptor), K(elr_commit_version));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "elr trans error", K(ret), K(key), K(descriptor), K(*this));
  } else {
    TRANS_LOG(DEBUG, "elr trans success", K(key), K_(row_lock), K(descriptor), K(*this));
  }

  return ret;
}

int ObMvccRow::elr(const uint32_t descriptor, const int64_t elr_commit_version, const ObMemtableKey* key)
{
  int ret = OB_SUCCESS;

  if (ctx_descriptor_ != descriptor) {
    TRANS_LOG(DEBUG, "early release lock by other trans", K(descriptor), K(*key), K(elr_commit_version), K(*this));
  } else {
    ObMvccTransNode* iter = get_list_head();
    while (NULL != iter && OB_SUCC(ret)) {
      if (descriptor != iter->ctx_descriptor_) {
        break;
      } else if (INT64_MAX != iter->trans_version_ && iter->trans_version_ > elr_commit_version) {
        // leader revoke
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexected transaction version", K(*iter), K(descriptor), K(elr_commit_version));
      } else if (iter->is_elr()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "unexpected transaction node status", K(*iter), K(*this));
      } else {
        iter->trans_version_ = elr_commit_version;
        iter->set_elr();
        iter = iter->prev_;
      }
    }
    inc_elr_trans_count();
    set_ctx_descriptor(0);
    inc_update(&max_elr_trans_version_, elr_commit_version);
    row_lock_.exclusive_unlock(key, descriptor);
  }

  return ret;
}

void ObMvccRow::set_row_header(const uint32_t modify_count, const uint32_t acc_checksum)
{
  ObRowLatchGuard guard(latch_);
  modify_count_ = modify_count;
  acc_checksum_ = acc_checksum;
}

void ObMvccRow::lock_begin(ObIMvccCtx& ctx) const
{
  if (GCONF.enable_sql_audit) {
    ctx.set_lock_start_time(ObTimeUtility::current_time());
  }
}

void ObMvccRow::lock_for_read_end(ObIMvccCtx& ctx, int64_t ret) const
{
  if (!ctx.is_can_elr() && GCONF.enable_sql_audit) {
    const int64_t lock_use_time = ObTimeUtility::current_time() - ctx.get_lock_start_time();
    EVENT_ADD(MEMSTORE_WAIT_READ_LOCK_TIME, lock_use_time);
    if (OB_FAIL(ret)) {
      EVENT_INC(MEMSTORE_READ_LOCK_FAIL_COUNT);
    } else {
      EVENT_INC(MEMSTORE_READ_LOCK_SUCC_COUNT);
    }
    if (lock_use_time >= WARN_TIME_US && TC_REACH_TIME_INTERVAL(LOG_INTERVAL)) {
      const uint32_t conflict_id = row_lock_.get_exclusive_uid();
      const char* conflict_ctx = ctx.log_conflict_ctx(conflict_id);
      TRANS_LOG(WARN, "wait lock for read use too much time", K(ctx), K(ret), K(lock_use_time), K(conflict_ctx));
    }
  }
}

void ObMvccRow::lock_for_write_end(ObIMvccCtx& ctx, int64_t ret) const
{
  if (!ctx.is_can_elr() && GCONF.enable_sql_audit) {
    const int64_t lock_use_time = ObTimeUtility::current_time() - ctx.get_lock_start_time();
    EVENT_ADD(MEMSTORE_WAIT_WRITE_LOCK_TIME, lock_use_time);
    if (OB_FAIL(ret)) {
      EVENT_INC(MEMSTORE_WRITE_LOCK_FAIL_COUNT);
    } else {
      EVENT_INC(MEMSTORE_WRITE_LOCK_SUCC_COUNT);
    }
    if (lock_use_time >= WARN_TIME_US && TC_REACH_TIME_INTERVAL(LOG_INTERVAL)) {
      TRANS_LOG(WARN, "wait lock for write use too much time", K(ctx), K(ret), K(lock_use_time));
    }
  }
}

int ObMvccRow::lock_for_read(const ObMemtableKey* key, ObIMvccCtx& ctx) const
{
  lock_begin(ctx);
  int ret = OB_ERR_SHARED_LOCK_CONFLICT;
  const int64_t MAX_SLEEP_US = 1000;
  const int64_t lock_wait_start_ts = common::ObClockGenerator::getClock();

  for (int32_t i = 0; OB_ERR_SHARED_LOCK_CONFLICT == ret; i++) {
    // leave this check here to make sure no lock is held
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ctx.read_lock_yield())) {
      ret = tmp_ret;
      break;
    }
    if (OB_FAIL(try_wait_row_lock_for_read(ctx, key))) {
      int64_t abs_stmt_timeout = ctx.get_trx_lock_timeout() < 0
                                     ? ctx.get_abs_expired_time()
                                     : MIN(lock_wait_start_ts + ctx.get_trx_lock_timeout(), ctx.get_abs_expired_time());
      int64_t wait_lock_timeout = 2000;
      int64_t abs_lock_timeout = common::ObClockGenerator::getClock() + wait_lock_timeout;
      if (abs_stmt_timeout > 0) {
        abs_lock_timeout = min(abs_stmt_timeout, abs_lock_timeout);
      }
      // leave timeout checkout here to make sure try lock atleast once
      if (abs_lock_timeout >= abs_stmt_timeout) {
        ret = OB_ERR_SHARED_LOCK_CONFLICT;
        break;
      } else if (i < 10) {
        PAUSE();
      } else {
        usleep((i < MAX_SLEEP_US ? i : MAX_SLEEP_US));
      }
    }
  }
  if (OB_FAIL(ret) && !ctx.is_can_elr()) {
    const uint32_t conflict_id = row_lock_.get_exclusive_uid();
    const char* conflict_ctx = ctx.log_conflict_ctx(conflict_id);
    TRANS_LOG(WARN, "lock_for_read fail", K(ret), K(ctx), K(*key), K(conflict_id), K(conflict_ctx));
  }
  lock_for_read_end(ctx, ret);
  return ret;
}

int ObMvccRow::relocate_lock(ObIMvccCtx& ctx, const bool is_sequential_relocate)
{
  lock_begin(ctx);
  int ret = OB_SUCCESS;
  const uint32_t uid = ctx.get_ctx_descriptor();
  if (is_sequential_relocate) {
    ObRowLatchGuard guard(latch_);

    if (OB_FAIL(row_lock_.try_exclusive_lock(uid))) {
      if (ctx.is_can_elr() && ctx.is_elr_prepared()) {
        ret = OB_SUCCESS;
        if (EXECUTE_COUNT_PER_SEC(32)) {
          TRANS_LOG(INFO,
              "no need to relocate lock, because of elr transaction",
              K(ctx),
              K(is_sequential_relocate),
              K(*this));
        }
      } else {
        // rewrite ret
        ret = OB_TRY_LOCK_ROW_CONFLICT;
      }
    } else {
      set_ctx_descriptor(uid);
    }
  } else {
    if (latch_.try_lock()) {
      if (OB_FAIL(row_lock_.try_exclusive_lock(uid))) {
        if (ctx.is_can_elr() && ctx.is_elr_prepared()) {
          ret = OB_SUCCESS;
          if (EXECUTE_COUNT_PER_SEC(32)) {
            TRANS_LOG(INFO,
                "no need to relocate lock, because of elr transaction",
                K(ctx),
                K(is_sequential_relocate),
                K(*this));
          }
        } else {
          // rewrite ret
          ret = OB_TRY_LOCK_ROW_CONFLICT;
        }
      } else {
        set_ctx_descriptor(uid);
      }
      latch_.unlock();
    }
  }
  if (OB_FAIL(ret)) {
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(WARN, "relocate lock error", K(ret), K(ctx), K(*this));
    }
  }

  lock_for_write_end(ctx, ret);
  return ret;
}

/*
 * return OB_SUCCESS if row not locked or locked by myself.
 * return OB_ERR_EXCLUSIVE_LOCK_CONFLICT or OB_TRY_LOCK_ROW_CONFLICT if locked by other.
 *
 * is_locked, is the locked status. true means is locked by myself or other.
 * lock_descriptor, the descriptor of who has locked the row.
 * max_trans_version, set only locked by myself or not locked.
 * */
int ObMvccRow::check_row_locked(
    const ObMemtableKey* key, ObIMvccCtx& ctx, bool& is_locked, uint32_t& lock_descriptor, int64_t& max_trans_version)
{
  int ret = OB_SUCCESS;

  is_locked = row_lock_.is_locked();

  if (is_locked) {
    lock_descriptor = row_lock_.get_exclusive_uid();
  }
  // locked by other
  if (is_locked && lock_descriptor != ctx.get_ctx_descriptor()) {
    lock_descriptor = row_lock_.get_exclusive_uid();
    int64_t lock_wait_start_ts =
        ctx.get_lock_wait_start_ts() > 0 ? ctx.get_lock_wait_start_ts() : common::ObClockGenerator::getClock();
    int64_t query_abs_lock_wait_timeout = ctx.get_query_abs_lock_wait_timeout(lock_wait_start_ts);
    int64_t cur_ts = common::ObClockGenerator::getClock();

    if (cur_ts >= query_abs_lock_wait_timeout) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    } else {
      ret = OB_TRY_LOCK_ROW_CONFLICT;

      const uint32_t uid = ctx.get_ctx_descriptor();
      int tmp_ret = OB_SUCCESS;
      const int64_t is_remote_sql =
          (static_cast<ObMemtableCtx&>(ctx).get_trans_ctx()->get_trans_id().get_server() != OBSERVER.get_self());
      if (OB_SUCCESS != (tmp_ret = get_global_lock_wait_mgr().post_lock(ret,
                             row_lock_,
                             *key,
                             query_abs_lock_wait_timeout,
                             is_remote_sql,
                             total_trans_node_cnt_,
                             ctx.is_can_elr(),
                             uid))) {}
      ctx.set_lock_wait_start_ts(lock_wait_start_ts);
    }
  } else {
    // no lock or locked by myself
    max_trans_version = get_max_trans_version();
  }

  return ret;
}

int ObMvccRow::lock_for_write(const ObMemtableKey* key, ObIMvccCtx& ctx)
{
  lock_begin(ctx);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint32_t uid = ctx.get_ctx_descriptor();
  const int64_t lock_wait_start_ts =
      ctx.get_lock_wait_start_ts() > 0 ? ctx.get_lock_wait_start_ts() : common::ObClockGenerator::getClock();

  int64_t query_abs_lock_wait_timeout = ctx.get_query_abs_lock_wait_timeout(lock_wait_start_ts);

  if (0 > ctx.get_abs_lock_wait_timeout()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "row lock wait timeout is illegal", K(ret), K(*this), K(ctx.get_abs_lock_wait_timeout()));
  } else if (0 == ctx.get_abs_lock_wait_timeout()) {
    if (OB_FAIL(row_lock_.try_exclusive_lock(uid))) {
      // rewrite ret
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    } else if (max_trans_version_ > ctx.get_read_snapshot() || max_elr_trans_version_ > ctx.get_read_snapshot()) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      row_lock_.exclusive_unlock(key, ctx.get_ctx_descriptor());
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "transaction set violation", K(ret), K_(max_trans_version), K_(max_elr_trans_version), K(ctx));
      }
    } else {
      set_ctx_descriptor(uid);
    }
  } else {
    const int64_t cur_ts = common::ObClockGenerator::getClock();
    int64_t abs_lock_timeout = MIN(query_abs_lock_wait_timeout, cur_ts);
    if (abs_lock_timeout > cur_ts) {
      if (OB_FAIL(row_lock_.exclusive_lock(uid, abs_lock_timeout))) {
        TRANS_LOG(DEBUG, "exclusive lock error", K(ret), K(abs_lock_timeout), K(ctx));
      }
    } else {
      // abs_lock_timeout==cur_ts
      if (OB_FAIL(row_lock_.try_exclusive_lock(uid))) {
        TRANS_LOG(DEBUG, "try exclusive lock error", K(ret), K(abs_lock_timeout), K(ctx));
      }
    }

    if (OB_FAIL(ret)) {
      if (abs_lock_timeout >= query_abs_lock_wait_timeout) {
        ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
      } else {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
      }
    } else if (max_trans_version_ > ctx.get_read_snapshot() || max_elr_trans_version_ > ctx.get_read_snapshot()) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      row_lock_.exclusive_unlock(key, ctx.get_ctx_descriptor());
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "transaction set violation", K(ret), K_(max_trans_version), K_(max_elr_trans_version), K(ctx));
      }
    } else {
      set_ctx_descriptor(uid);
    }
    if (!ctx.is_can_elr()) {
      const int64_t lock_ms = (common::ObClockGenerator::getClock() - cur_ts) / 1000;
      ObWaitEventGuard wait_guard(ObWaitEventIds::MT_WRITE_LOCK_WAIT,
          lock_ms,
          (int64_t)&row_lock_,
          (int64_t)uid,
          (int64_t)row_lock_.get_exclusive_uid());
    } else if (!static_cast<ObMemtableCtx&>(ctx).get_trans_ctx()->get_trans_param().is_serializable_isolation() &&
               OB_TRANSACTION_SET_VIOLATION == ret) {
      ret = OB_TRY_LOCK_ROW_CONFLICT;
    } else {
      // do nothing
    }
  }

  // remote sql?
  const int64_t is_remote_sql =
      (static_cast<ObMemtableCtx&>(ctx).get_trans_ctx()->get_trans_id().get_server() != OBSERVER.get_self());
  if (OB_SUCCESS != (tmp_ret = get_global_lock_wait_mgr().post_lock(ret,
                         row_lock_,
                         *key,
                         query_abs_lock_wait_timeout,
                         is_remote_sql,
                         total_trans_node_cnt_,
                         ctx.is_can_elr(),
                         uid))) {}
  const bool enable_perf_event = GCONF.enable_perf_event;
  ctx.set_lock_wait_start_ts(0);
  if (OB_SUCC(ret)) {
    // do nothing
  } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
    if (enable_perf_event && !ctx.is_can_elr()) {
      ctx.on_tsc_retry(*key);
    }
    if (EXECUTE_COUNT_PER_SEC(16)) {
      TRANS_LOG(WARN, "transaction set violation", K(ret), K(*this), K(ctx));
    }
  } else {
    const uint32_t conflict_id = row_lock_.get_exclusive_uid();
    if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      if (enable_perf_event && !ctx.is_can_elr()) {
        const char* conflict_ctx = ctx.log_conflict_ctx(conflict_id);
        ctx.on_wlock_retry(*key, conflict_ctx);
        ctx.set_lock_wait_start_ts(lock_wait_start_ts);
      }
      ctx.set_lock_wait_start_ts(lock_wait_start_ts);
    } else if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
      TRANS_LOG(WARN, "lock_for_write fail", K(ret), K(ctx), K(*key), K(conflict_id));
    } else {
      // do nothing
    }
  }
  lock_for_write_end(ctx, ret);
  return ret;
}

int ObMvccRow::unlock_for_write(const ObMemtableKey* key, ObIMvccCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObRowLatchGuard guard(latch_);

  if (ctx_descriptor_ != ctx.get_ctx_descriptor()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected ctx descriptor or row lock id", K(ret), K(*this));
  } else if (!row_lock_.is_exclusive_locked_by(ctx_descriptor_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected lock state", K(ret), K(*this));
  } else {
    set_ctx_descriptor(0);
    ret = row_lock_.exclusive_unlock(key, ctx.get_ctx_descriptor());
  }

  return ret;
}

int ObMvccRow::revert_lock_for_write(ObIMvccCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObRowLatchGuard guard(latch_);

  if (ctx_descriptor_ != ctx.get_ctx_descriptor()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected ctx descriptor or row lock id", K(ret), K(*this));
  } else if (!row_lock_.is_exclusive_locked_by(ctx_descriptor_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected lock state", K(ret), K(*this));
  } else {
    set_ctx_descriptor(0);
    ret = row_lock_.revert_lock(ctx.get_ctx_descriptor());
  }

  return ret;
}

int64_t ObMvccRow::get_max_trans_version() const
{
  const int64_t max_elr_commit_version = ATOMIC_LOAD(&max_elr_trans_version_);
  const int64_t max_trans_version = ATOMIC_LOAD(&max_trans_version_);
  return std::max(max_elr_commit_version, max_trans_version);
}

void ObMvccRow::update_max_trans_version(const int64_t max_trans_version)
{
  if (max_trans_version > INT64_MAX / 2) {
    TRANS_LOG(ERROR, "unexpected trans version", K(*this), K(max_trans_version));
  }
  inc_update(&max_trans_version_, max_trans_version);
}

int ObMvccRow::set_ctx_descriptor(const uint32_t ctx_descriptor)
{
  ATOMIC_STORE(&ctx_descriptor_, ctx_descriptor);
  return OB_SUCCESS;
}

int ObMvccRow::trans_commit(const int64_t commit_version, ObMvccTransNode& node)
{
  int ret = OB_SUCCESS;

  if (commit_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(commit_version), K(*this));
  } else {
    if (NULL != node.prev_ && node.prev_->is_safe_read_barrier()) {
      if (commit_version <= node.prev_->snapshot_version_barrier_) {
        if (node.is_elr() && node.prev_->type_ == NDT_COMPACT) {
          // do nothing
        } else {
          // ignore ret
          TRANS_LOG(ERROR,
              "unexpected commit version",
              K(commit_version),
              K(*this),
              "cur_node",
              node,
              "prev_node",
              *(node.prev_));
        }
      }
    }
    update_max_trans_version(commit_version);
    inc_update(&max_elr_trans_version_, commit_version);
    node.trans_commit();
  }

  return ret;
}

int ObMvccRow::remove_callback(ObMvccRowCallback& cb)
{
  int ret = OB_SUCCESS;

  ObMvccTransNode* node = cb.get_trans_node();
  if (OB_NOT_NULL(node)) {
    node->remove_callback();
  }

  if (cb.is_write_locked()) {
    set_lock_delayed_cleanout(true);
    get_global_lock_wait_mgr().delegate_waiting_querys_to_trx(*cb.get_key(), cb.get_ctx());
  }

  return ret;
}

void ObMvccRow::set_lock_delayed_cleanout(bool delayed_cleanout)
{
  flag_ = delayed_cleanout ? flag_ | F_LOCK_DELAYED_CLEANOUT : flag_ & ~F_LOCK_DELAYED_CLEANOUT;
}

void ObMvccRow::cleanout_rowlock()
{
  if (is_lock_delayed_cleanout()) {
    ObRowLatchGuard guard(latch_);
    if (is_lock_delayed_cleanout()) {

      uint32_t writer_uid = get_writer_uid();
      uint32_t owner_uid = row_lock_.get_exclusive_uid_();

      if (row_lock_.is_locked_()) {
        if (0 == writer_uid || owner_uid != writer_uid) {
          row_lock_.revert_lock(owner_uid);
        }
      }

      if (owner_uid != writer_uid) {
        // lock row exclusively
        int ret = OB_SUCCESS;
        if (OB_FAIL(row_lock_.try_exclusive_lock_(writer_uid))) {
          TRANS_LOG(ERROR, "unlock inside cleanout should not fail", K(ret), K(owner_uid), K(writer_uid));
        }
      }

      if (0 == writer_uid) {
        set_lock_delayed_cleanout(false);
      }
    }
  }
}

uint32_t ObMvccRow::get_writer_uid()
{
  int ret = OB_SUCCESS;
  uint32_t writer_uid = 0;
  bool met_ended_trans = false;
  ObMvccTransNode* list_head = get_list_head();

  if (list_head) {
    bool is_running = false;
    if (OB_FAIL(list_head->is_running(is_running))) {
      TRANS_LOG(WARN, "fail to check trans status", K(ret));
    } else if (is_running) {
      writer_uid = list_head->get_ctx_descriptor();
    }
  }

  return writer_uid;
}

};  // namespace memtable
};  // end namespace oceanbase
