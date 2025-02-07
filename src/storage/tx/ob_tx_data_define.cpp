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

#include "storage/tx/ob_tx_data_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tx_table/ob_tx_table.h"
#include "share/rc/ob_tenant_base.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/tx/ob_tx_data_op.h"

using namespace oceanbase::share;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{

int ObUndoStatusList::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  SpinRLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  const int64_t len = get_serialize_size_();
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "serialize ObUndoStatusList failed.", KR(ret), KP(buf), K(buf_len),
                K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, UNIS_VERSION))) {
    STORAGE_LOG(WARN, "encode UNIS_VERSION of undo status list failed.", KR(ret), KP(buf),
                K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, len))) {
    STORAGE_LOG(WARN, "encode length of undo status list failed.", KR(ret), KP(buf), K(buf_len),
                K(pos));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize_ undo status list failed.", KR(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

int ObUndoStatusList::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObUndoStatusNode *, 32> node_arr;
  node_arr.reuse();
  ObUndoStatusNode *node = head_;
  // generate undo status node stack
  while (OB_NOT_NULL(node)) {
    if (OB_FAIL(node_arr.push_back(node))) {
      STORAGE_LOG(WARN, "push back undo status node failed", KR(ret), K(node_arr.count()));
    } else {
      node = node->next_;
    }
  }

  LST_DO_CODE(OB_UNIS_ENCODE, undo_node_cnt_);
  // pop undo status node to serialize
  for (int i = node_arr.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    node = node_arr[i];
    for (int i = 0; OB_SUCC(ret) && i < node->size_; i++) {
      LST_DO_CODE(OB_UNIS_ENCODE, node->undo_actions_[i]);
    }
  }

  return ret;
}

int ObUndoStatusList::deserialize(const char *buf,
                                  const int64_t data_len,
                                  int64_t &pos,
                                  ObTenantTxDataAllocator &tx_data_allocator)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t undo_status_list_len = 0;
  SpinWLockGuard guard(lock_);

  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &version))) {
    STORAGE_LOG(WARN, "decode version fail", K(version), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &undo_status_list_len))) {
    STORAGE_LOG(WARN, "decode data len fail", K(undo_status_list_len), K(data_len), K(pos), K(ret));
  } else if (version != UNIS_VERSION) {
    ret = OB_VERSION_NOT_MATCH;
    STORAGE_LOG(WARN, "object version mismatch", K(ret), K(version));
  } else if (OB_UNLIKELY(undo_status_list_len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "can't decode object with negative length", K(undo_status_list_len));
  } else if (OB_UNLIKELY(data_len < undo_status_list_len + pos)) {
    ret = OB_DESERIALIZE_ERROR;
    STORAGE_LOG(WARN, "buf length not correct", K(undo_status_list_len), K(pos), K(data_len));
  } else {
    int64_t original_pos = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + original_pos, undo_status_list_len, pos, tx_data_allocator))) {
      STORAGE_LOG(WARN, "deserialize_ fail", "slen", undo_status_list_len, K(pos), K(ret));
    }
    pos += original_pos;
  }

  return ret;
}

int ObUndoStatusList::deserialize_(const char *buf,
                                   const int64_t data_len,
                                   int64_t &pos,
                                   ObTenantTxDataAllocator &tx_data_allocator)
{
  int ret = OB_SUCCESS;
  ObUndoStatusNode *cur_node = nullptr;
  transaction::ObUndoAction action;
  LST_DO_CODE(OB_UNIS_DECODE, undo_node_cnt_);
  while (OB_SUCC(ret) && pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE, action);
    // allcate new undo status node if needed
    if (OB_ISNULL(cur_node) || cur_node->size_ >= TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE) {
      void *undo_node_buf = nullptr;
      if (OB_ISNULL(undo_node_buf = tx_data_allocator.alloc(false/* enable_throttle */))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "allocate memory when deserialize ObTxData failed.", KR(ret));
      } else {
        cur_node = new (undo_node_buf) ObUndoStatusNode;

        // update undo status list link after allocated new node
        ObUndoStatusNode *tmp_node = head_;
        head_ = cur_node;
        cur_node->next_ = tmp_node;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_NOT_NULL(cur_node)) {
        cur_node->undo_actions_[cur_node->size_++] = action;
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "unexpected nullptr when deserialize undo status list", KR(ret), KP(buf), K(pos), K(data_len));
      }
    }
  }

  return ret;
}

int64_t ObUndoStatusList::get_serialize_size() const
{
  SpinRLockGuard guard(lock_);
  int64_t data_len = get_serialize_size_();
  int64_t len = 0;
  len += serialization::encoded_length_vi64(UNIS_VERSION);
  len += serialization::encoded_length_vi64(data_len);
  len += data_len;
  return len;
}

int64_t ObUndoStatusList::get_serialize_size_() const
{
  int64_t len = 0;
  ObUndoStatusNode *node_ptr = head_;
  LST_DO_CODE(OB_UNIS_ADD_LEN, undo_node_cnt_);
  while (OB_NOT_NULL(node_ptr)) {
    for (int i = 0; i < node_ptr->size_; i++) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, node_ptr->undo_actions_[i]);
    }
    node_ptr = node_ptr->next_;
  }
  return len;
}

bool ObUndoStatusList::is_contain(const transaction::ObTxSEQ seq_no, const int32_t tx_data_state) const
{
  if (OB_LIKELY(ObTxData::COMMIT == tx_data_state || ObTxData::ABORT == tx_data_state)) {
    return is_contain_(seq_no);
  } else {
    SpinRLockGuard guard(lock_);
    return is_contain_(seq_no);
  }
}

bool ObUndoStatusList::is_contain_(const transaction::ObTxSEQ seq_no) const
{
  bool bool_ret = false;
  ObUndoStatusNode *node_ptr = head_;
  while (OB_NOT_NULL(node_ptr)) {
    for (int i = 0; i < node_ptr->size_; i++) {
      if (true == node_ptr->undo_actions_[i].is_contain(seq_no)) {
        bool_ret = true;
        break;
      }
    }
    if (bool_ret) break;
    node_ptr = node_ptr->next_;
  }
  return bool_ret;
}

DEF_TO_STRING(ObUndoStatusList)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(head), K_(undo_node_cnt));

  J_OBJ_START();
  ObUndoStatusNode *node_ptr = head_;
  while (nullptr != node_ptr) {
    for (int i = 0; i < node_ptr->size_; i++) {
      transaction::ObUndoAction undo_action = node_ptr->undo_actions_[i];
      J_OBJ_START();
      J_KV(K(undo_action));
      J_OBJ_END();
    }
    node_ptr = node_ptr->next_;
  }
  J_OBJ_END();

  J_OBJ_END();
  return pos;
}

void ObUndoStatusList::dump_2_text(FILE *fd) const
{
  if (OB_ISNULL(fd)) {
    return;
  }

  ObUndoStatusNode *node = head_;
  bool has_undo = false;
  if (OB_NOT_NULL(node)) {
    has_undo = true;
    fprintf(fd, "    UNDO_STATUS:{");
  }
  while (OB_NOT_NULL(node)) {
    for (int64_t i = node->size_ - 1; i >= 0; i--) {
      fprintf(fd, "{from:%ld, to:%ld}", node->undo_actions_[i].undo_from_.cast_to_int(), node->undo_actions_[i].undo_to_.cast_to_int());
    }
    node = node->next_;
  }
  if (has_undo) {
    fprintf(fd, "}");
  }
}

void ObTxCommitData::reset()
{
  tx_id_ = INT64_MAX;
  state_ = RUNNING;
  commit_version_.reset();
  start_scn_.reset();
  end_scn_.reset();
}

const char* ObTxCommitData::get_state_string(int32_t state)
{
  STATIC_ASSERT(RUNNING == 0, "Invalid State Enum");
  STATIC_ASSERT(COMMIT == 1, "Invalid State Enum");
  STATIC_ASSERT(ELR_COMMIT == 2, "Invalid State Enum");
  STATIC_ASSERT(ABORT == 3, "Invalid State Enum");
  STATIC_ASSERT(MAX_STATE_CNT == 4, "Invalid State Enum");
  const static int cnt = MAX_STATE_CNT;
  const static char STATE_TO_CHAR[cnt][20] = {"RUNNING", "COMMIT", "ELR_COMMIT", "ABORT"};
  return STATE_TO_CHAR[state];
}

int ObTxData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t pos_tmp = 0;
  const int64_t len = get_serialize_size_();

  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "serialize of ObTxDat failed.", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, UNIS_VERSION))) {
    STORAGE_LOG(WARN, "encode UNIS_VERSION of ObTxData failed.", KR(ret), KP(buf), K(buf_len),
                K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, len))) {
    STORAGE_LOG(WARN, "encode length of ObTxData failed.", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (FALSE_IT(pos_tmp = pos)) {
  } else if (OB_FAIL(serialize_(buf, pos + len, pos))) {
    STORAGE_LOG(WARN, "serialize_ of ObTxData failed.", KR(ret), KP(buf), K(buf_len), K(pos), K(pos_tmp));
  }
  return ret;
}

int ObTxData::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // LST_DO_CODE(OB_UNIS_ENCODE, state_, commit_version_, start_scn_, end_scn_);

  if (OB_FAIL(tx_id_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize tx_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, state_))) {
    STORAGE_LOG(WARN, "serialize state fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(commit_version_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize commit_version fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(start_scn_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize start_scn fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(end_scn_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize end_scn fail.", KR(ret), K(pos), K(buf_len));
  }
  uint64_t data_version = 0;
  if (FAILEDx(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    STORAGE_LOG(WARN, "fail to get data version", KR(ret));
  } else if (data_version < DATA_VERSION_4_3_2_0) {
    if (op_guard_.is_valid()) {
      if (OB_FAIL(op_guard_->get_undo_status_list().serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "serialize undo_status_list fail.", KR(ret), K(pos), K(buf_len));
      }
    } else {
      ObUndoStatusList dummy_undo;
      if (OB_FAIL(dummy_undo.serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "serialize undo_status_list fail.", KR(ret), K(pos), K(buf_len));
      }
    }
  } else if (op_guard_.is_valid()) {
    if (OB_FAIL(op_guard_->get_undo_status_list().serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "serialize undo_status_list fail.", KR(ret), K(pos), K(buf_len));
    } else if (OB_FAIL(op_guard_->get_tx_op_list().serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "serialize tx_op_list fail.", KR(ret), K(pos), K(buf_len));
    }
  }
  return ret;
}

int64_t ObTxData::get_serialize_size() const
{
  int64_t data_len = get_serialize_size_();
  int64_t len = 0;
  len += serialization::encoded_length_vi64(UNIS_VERSION);
  len += serialization::encoded_length_vi64(data_len);
  len += data_len;
  return len;
}

int64_t ObTxData::get_serialize_size_() const
{
  int64_t len = 0;
  // LST_DO_CODE(OB_UNIS_ADD_LEN, state_, commit_version_, start_scn_, end_scn_);
  len += tx_id_.get_serialize_size();
  len += serialization::encoded_length_vi32(state_);
  len += commit_version_.get_serialize_size();
  len += start_scn_.get_serialize_size();
  len += end_scn_.get_serialize_size();
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    STORAGE_LOG(ERROR, "get data_version failed", KR(ret));
  }
  if (data_version < DATA_VERSION_4_3_2_0) {
    if (op_guard_.is_valid()) {
      len += op_guard_->get_undo_status_list().get_serialize_size();
    } else {
      ObUndoStatusList dummy_undo;
      len += dummy_undo.get_serialize_size();
    }
  } else if (op_guard_.is_valid()) {
    len += op_guard_->get_undo_status_list().get_serialize_size();
    len += op_guard_->get_tx_op_list().get_serialize_size();
  }
  return len;
}

int64_t ObTxData::size_need_cache() const
{
  int64_t len = TX_DATA_SLICE_SIZE;
  if (op_guard_.is_valid()) {
    len += TX_DATA_SLICE_SIZE; // tx_op
    len += TX_DATA_SLICE_SIZE * op_guard_->get_undo_status_list().undo_node_cnt_;
  }
  return len;
}

int ObTxData::deserialize(const char *buf,
                          const int64_t data_len,
                          int64_t &pos,
                          ObTenantTxDataAllocator &slice_allocator)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  int64_t pos_tmp = 0;

  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &version))) {
    STORAGE_LOG(WARN, "deserialize version of tx data failed.", KR(ret), K(version));
  } else if (version != UNIS_VERSION) {
    ret = OB_VERSION_NOT_MATCH;
    STORAGE_LOG(WARN, "deserialize version of tx data failed.", KR(ret), K(version));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &len))) {
    STORAGE_LOG(WARN, "length from deserialize is invalid.", KR(ret), K(pos), K(len), K(data_len));
  } else if (OB_UNLIKELY(pos + len > data_len)) {
    ret = OB_INVALID_SIZE;
    STORAGE_LOG(WARN, "length from deserialize is invalid.", KR(ret), K(pos), K(len), K(data_len));
  } else if (FALSE_IT(pos_tmp = pos)) {
  } else if (OB_FAIL(deserialize_(buf, pos + len, pos, slice_allocator))) {
    STORAGE_LOG(WARN, "deserialize tx data failed.", KR(ret), K(buf), K(pos), K(len), K(pos_tmp), K(data_len));
  }

  return ret;
}

int ObTxData::deserialize_(const char *buf,
                           const int64_t data_len,
                           int64_t &pos,
                           ObTenantTxDataAllocator &tx_data_allocator)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_id_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize tx_id fail.", KR(ret), K(pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &state_))) {
    STORAGE_LOG(WARN, "deserialize state fail.", KR(ret), K(pos), K(data_len));
  } else if (OB_FAIL(commit_version_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize commit_version fail.", KR(ret), K(pos), K(data_len));
  } else if (OB_FAIL(start_scn_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize start_scn fail.", KR(ret), K(pos), K(data_len));
  } else if (OB_FAIL(end_scn_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "deserialize end_scn fail.", KR(ret), K(pos), K(data_len));
  }
  if (OB_SUCC(ret) && pos < data_len) {
    if (OB_FAIL(init_tx_op())) {
      STORAGE_LOG(WARN, "init tx op fail", KR(ret));
    } else if (OB_FAIL(op_guard_->get_undo_status_list().deserialize(buf, data_len, pos, tx_data_allocator))) {
      STORAGE_LOG(WARN, "deserialize undo_status_list fail.", KR(ret), K(pos), K(data_len));
    } else if (pos < data_len && OB_FAIL(op_guard_->get_tx_op_list().deserialize(buf, data_len, pos,
            MTL(ObSharedMemAllocMgr*)->tx_data_op_allocator()))) {
      STORAGE_LOG(WARN, "deserialize tx_op_list fail.", KR(ret), K(pos), K(data_len));
    }
  }
  return ret;
}

void ObTxData::reset()
{
  if (OB_NOT_NULL(tx_data_allocator_) || ref_cnt_ != 0) {
    int ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "this tx data should not be reset", KR(ret), KP(this), KP(tx_data_allocator_), K(ref_cnt_));
    // TODO : @gengli remove ob_abort
    ob_abort();
  }
  ObTxCommitData::reset();
  op_guard_.reset();
  tx_data_allocator_ = nullptr;
  ref_cnt_ = 0;
}

ObTxData::ObTxData(const ObTxData &rhs)
{
  *this = rhs;
}

ObTxData &ObTxData::operator=(const ObTxData &rhs)
{
  tx_id_ = rhs.tx_id_;
  state_ = rhs.state_;
  commit_version_ = rhs.commit_version_;
  start_scn_ = rhs.start_scn_;
  end_scn_ = rhs.end_scn_;
  if (rhs.op_guard_.is_valid()) {
    op_guard_.init(rhs.op_guard_.ptr());
  }
  return *this;
}

ObTxData &ObTxData::operator=(const ObTxCommitData &rhs)
{
  tx_id_ = rhs.tx_id_;
  state_ = rhs.state_;
  commit_version_ = rhs.commit_version_;
  start_scn_ = rhs.start_scn_;
  end_scn_ = rhs.end_scn_;
  op_guard_.reset();
  return *this;
}

const ObTxData &ObTxData::assign_without_undo(const ObTxData &rhs)
{
  return operator=(static_cast<ObTxCommitData>(rhs));
}

bool ObTxData::is_valid_in_tx_data_table() const
{
  bool bool_ret = true;

  if (ObTxData::RUNNING == state_) {
    if (!end_scn_.is_valid()) {
      bool_ret = false;
      STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data end log ts is invalid", KPC(this));
    } else {
      bool_ret = true;
    }
  } else if (state_ < 0 || state_ >= MAX_STATE_CNT) {
    bool_ret = false;
    STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data state is invalid", KPC(this));
  } else if (!start_scn_.is_valid()) {
    bool_ret = false;
    STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data start_scn is invalid", KPC(this));
  } else if (!end_scn_.is_valid()) {
    bool_ret = false;
    STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data end_scn is invalid", KPC(this));
  } else if (end_scn_ < start_scn_) {
    bool_ret = false;
    STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data end_scn is less than start_scn", KPC(this));
  } else if (!commit_version_.is_valid() && state_ != RUNNING && state_ != ABORT) {
    bool_ret = false;
    STORAGE_LOG_RET(ERROR, OB_INVALID_ERROR, "tx data commit_version is invalid but state is not running or abort",
                KPC(this));
  }

  return bool_ret;
}

int ObTxData::reserve_undo(ObTxTable *tx_table)
{
  int ret = OB_SUCCESS;
  ObTxDataTable *tx_data_table = nullptr;
  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx table is nullptr.", KR(ret));
  } else if (OB_ISNULL(tx_data_table = tx_table->get_tx_data_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data table in tx table is nullptr.", KR(ret));
  } else if (!op_guard_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data op is nullptr", KR(ret));
  } else {
    SpinWLockGuard lock_guard(op_guard_->get_lock());
    SpinWLockGuard guard(op_guard_->get_undo_status_list().lock_);
    ObUndoStatusNode *node = op_guard_->get_undo_status_list().head_;
    if (OB_ISNULL(node) || node->size_ >= TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE) {
      ObUndoStatusNode *new_node = nullptr;
      if (OB_FAIL(tx_data_table->alloc_undo_status_node(new_node))) {
        STORAGE_LOG(WARN, "alloc_undo_status_node() fail", KR(ret));
      } else {
        new_node->next_ = node;
        op_guard_->get_undo_status_list().head_ = new_node;
        op_guard_->get_undo_status_list().undo_node_cnt_++;
      }
    }
  }
  return ret;
}

int ObTxData::add_undo_action(ObTxTable *tx_table, transaction::ObUndoAction &new_undo_action, ObUndoStatusNode *&undo_node)
{
  // STORAGE_LOG(DEBUG, "do add_undo_action");
  int ret = OB_SUCCESS;
  ObTxDataTable *tx_data_table = nullptr;
  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx table is nullptr.", KR(ret));
  } else if (OB_ISNULL(tx_data_table = tx_table->get_tx_data_table())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data table in tx table is nullptr.", KR(ret));
  } else if (!op_guard_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data op is nullptr", KR(ret));
  } else {
    SpinWLockGuard lock_guard(op_guard_->get_lock());
    SpinWLockGuard guard(op_guard_->get_undo_status_list().lock_);
    ObUndoStatusNode *node = op_guard_->get_undo_status_list().head_;
    if (OB_FAIL(merge_undo_actions_(tx_data_table, node, new_undo_action))) {
      STORAGE_LOG(WARN, "merge undo actions fail.", KR(ret), K(new_undo_action));
    } else if (!new_undo_action.is_valid()) {
      // if new_undo_action is merged, it will be set to invalid and skip insert
    } else {
      // generate new node if current node cannot be inserted
      if (OB_ISNULL(node) || node->size_ >= TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE) {
        ObUndoStatusNode *new_node = nullptr;
        if (OB_NOT_NULL(undo_node)) {
          new_node = undo_node;
          undo_node = NULL;
        } else if (OB_FAIL(tx_data_table->alloc_undo_status_node(new_node))) {
          STORAGE_LOG(WARN, "alloc_undo_status_node() fail", KR(ret));
        }

        if (OB_SUCC(ret)) {
          new_node->next_ = node;
          op_guard_->get_undo_status_list().head_ = new_node;
          node = new_node;
          op_guard_->get_undo_status_list().undo_node_cnt_++;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_NOT_NULL(node)) {
          node->undo_actions_[node->size_++] = new_undo_action;
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "node is unexpected nullptr", KR(ret), KPC(this));
        }
      }
    }
  }
  return ret;
}

int ObTxData::merge_undo_actions_(ObTxDataTable *tx_data_table,
                                   ObUndoStatusNode *&node,
                                   transaction::ObUndoAction &new_undo_action)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_NOT_NULL(node)) {
    for (int i = node->size_ - 1; i >= 0; i--) {
      if (new_undo_action.is_contain(node->undo_actions_[i])) {
        node->size_--; // pop merged
      } else if (node->undo_actions_[i].is_contain(new_undo_action)) {
        // new undo is merged, reset it
        new_undo_action.reset();
        break;
      } else {
        break;
      }
    }

    if (0 == node->size_) {
      // fprintf(stdout, "free undo node, node ptr = %p \n", node);
      // all undo actions in this node are merged, free it
      // STORAGE_LOG(DEBUG, "current node is empty, now free it");
      ObUndoStatusNode *node_to_free = node;
      op_guard_->get_undo_status_list().head_ = node->next_;
      node = op_guard_->get_undo_status_list().head_;
      tx_data_table->free_undo_status_node(node_to_free);
      if (op_guard_->get_undo_status_list().undo_node_cnt_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "invalid undo node count int undo status list.", KR(ret),
                    K(op_guard_->get_undo_status_list()));
      } else {
        op_guard_->get_undo_status_list().undo_node_cnt_--;
      }
    } else {
      // merge undo actions done
      break;
    }
  }

  return ret;
}

bool ObTxData::equals_(ObTxData &rhs)
{
  bool bool_ret = true;
  if (tx_id_ != rhs.tx_id_) {
    bool_ret = false;
    STORAGE_LOG(INFO, "tx_id is not equal.");
  } else if (state_ != rhs.state_) {
    bool_ret = false;
    STORAGE_LOG(INFO, "state is not equal.");
  } else if (commit_version_ != rhs.commit_version_) {
    bool_ret = false;
    STORAGE_LOG(INFO, "commit_version is not equal.");
  } else if (start_scn_ != rhs.start_scn_) {
    bool_ret = false;
    STORAGE_LOG(INFO, "start_scn is not equal.");
  } else if (end_scn_ != rhs.end_scn_) {
    bool_ret = false;
    STORAGE_LOG(INFO, "end_scn is not equal.");
  } else {
    ObUndoStatusNode *l_node = NULL;
    if (op_guard_.is_valid()) {
      l_node = op_guard_->get_undo_status_list().head_;
    }
    ObUndoStatusNode *r_node = NULL;
    if (rhs.op_guard_.is_valid()) {
      r_node = rhs.op_guard_->get_undo_status_list().head_;
    }

    while ((nullptr != l_node) && (nullptr != r_node)) {
      if (l_node->size_ != r_node->size_) {
        bool_ret = false;
        break;
      }
      for (int i = 0; i < l_node->size_; i++) {
        if ((l_node->undo_actions_[i].undo_from_ != r_node->undo_actions_[i].undo_from_)
            || (l_node->undo_actions_[i].undo_to_ != r_node->undo_actions_[i].undo_to_)) {
          bool_ret = false;
          break;
        }
      }

      if (false == bool_ret) {
        break;
      } else {
        l_node = l_node->next_;
        r_node = r_node->next_;
      }
    }

    if (true == bool_ret) {
      if (nullptr != l_node || nullptr != r_node) {
        bool_ret = false;
      }
    }
    if (false == bool_ret) {
      STORAGE_LOG(INFO, "undo status is not equal.");
    }
  }

  return bool_ret;
}

void ObTxData::print_to_stderr(const ObTxData &tx_data)
{
  ObCStringHelper helper;
  fprintf(stderr,
          "TX_DATA:{tx_id=%-20ld start_log_scn=%-20s end_log_scn=%-20s commit_version=%-20s "
          "state=%s",
          tx_data.tx_id_.get_id(),
          helper.convert(tx_data.start_scn_),
          helper.convert(tx_data.end_scn_),
          helper.convert(tx_data.commit_version_),
          get_state_string(tx_data.state_));

  if (tx_data.op_guard_.is_valid()) {
    tx_data.op_guard_->get_undo_status_list().dump_2_text(stderr);
  }
}

void ObTxData::dump_2_text(FILE *fd) const
{
  if (OB_ISNULL(fd)) {
    return;
  }

  ObCStringHelper helper;
  fprintf(fd,
          "TX_DATA:\n{\n    tx_id=%-20ld\n    start_log_scn=%-20s\n    end_log_scn=%-20s\n  "
          "  commit_version=%-20s\n    state=%s\n",
          tx_id_.get_id(),
          helper.convert(start_scn_),
          helper.convert(end_scn_),
          helper.convert(commit_version_),
          get_state_string(state_));

  if (op_guard_.is_valid()) {
    op_guard_->get_undo_status_list().dump_2_text(fd);
  }

  fprintf(fd, "\n}\n");
}

DEF_TO_STRING(ObTxData)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(tx_id),
       K_(ref_cnt),
       "state", get_state_string(state_),
       K_(commit_version),
       K_(start_scn),
       K_(end_scn),
       K_(op_guard));
  if (op_guard_.is_valid()) {
    J_KV("UndoStatusList", op_guard_->get_undo_status_list());
  }
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObUndoStatusNode)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(size_), KP(next_));
  J_OBJ_END();
  return pos;
}

int ObTxData::init_tx_op()
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;
  if (!op_guard_.is_valid()) {
    if (OB_ISNULL(tx_data_allocator_)) {
      tx_data_allocator_ = &MTL(ObSharedMemAllocMgr*)->tx_data_allocator();
    }
    if (OB_ISNULL(op_allocator_)) {
      op_allocator_ = &MTL(ObSharedMemAllocMgr*)->tx_data_op_allocator();
    }
    if (OB_ISNULL(ptr = tx_data_allocator_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate memory from slice_allocator fail.", KR(ret), KP(this));
    } else {
      ObTxDataOp *tx_data_op = new (ptr) ObTxDataOp(tx_data_allocator_, op_allocator_);
      op_guard_.init(tx_data_op);
    }
  }
  return ret;
}

int ObTxData::check_tx_op_exist(share::SCN op_scn, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (op_guard_.is_valid()) {
    ObTxOpVector &tx_op_list = op_guard_->get_tx_op_list();
    if (tx_op_list.get_count() > 0 && op_scn <= tx_op_list.at(tx_op_list.get_count() - 1)->get_op_scn()) {
      exist = true;
    }
  }
  return ret;
}

int ObTxDataOpGuard::init(ObTxDataOp *tx_data_op)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(tx_data_op)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "init ObTxDataOpGuard with invalid arguments", KR(ret));
  } else if (tx_data_op->inc_ref() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected ref cnt on tx data op", KR(ret), KP(tx_data_op), KPC(tx_data_op));
    ob_abort();
  } else {
    tx_data_op_ = tx_data_op;
  }
  return ret;
}

void ObTxDataOpGuard::reset()
{
  if (OB_NOT_NULL(tx_data_op_)) {
    tx_data_op_->dec_ref();
    tx_data_op_ = nullptr;
  }
}

}  // namespace storage
}  // namespace oceanbase
