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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_OP
#define OCEANBASE_STORAGE_OB_TX_DATA_OP

#include "share/scn.h"
#include "share/allocator/ob_tx_data_allocator.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"

namespace oceanbase
{

namespace transaction
{
namespace tablelock
{
  struct ObTableLockOp;
}
}
namespace storage
{

// record tx.op has no value
class ObTxDummyOp
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_EMPTY();
};

extern ObTxDummyOp DEFAULT_TX_DUMMY_OP;

/*
 * tx.op is designed to store tx operations that need retain after tx_ctx exit
 * so tx.op need to support various data types
 *
 * we use class ObTxOp to describe every tx operation
 * users just need to add new data types to TX_OP_MEMBERS, we can create ObTxOp and put it into ObTxDataOp
 */
enum class ObTxOpCode : int64_t
{
  INVALID = 0,
  MDS_OP = 1,
  LOCK_OP = 2,
  ABORT_OP = 3
};

#define TX_OP_LIST(...) __VA_ARGS__
#define TX_OP_MEMBERS                       \
  TX_OP_LIST(                               \
      (ObTxOpCode::MDS_OP, transaction::ObTxBufferNodeWrapper),  \
      (ObTxOpCode::LOCK_OP, transaction::tablelock::ObTableLockOp),  \
      (ObTxOpCode::ABORT_OP, ObTxDummyOp) \
  )

class ObTxOp
{
public:
  ObTxOp() { reset(); }
  ~ObTxOp() { reset(); }
  void reset() {
    op_code_ = ObTxOpCode::INVALID;
    op_scn_.reset();
    op_val_ = nullptr;
    val_size_ = 0;
  }
  template <typename T>
  int init(ObTxOpCode op_code, share::SCN op_scn, T *val, int64_t val_size);
  ObTxOpCode get_op_code() { return op_code_; }
  share::SCN get_op_scn() { return op_scn_; }
  void set_op_scn(share::SCN scn) { op_scn_ = scn; }
  void* get_op_val() { return op_val_; }
  int64_t get_val_size() { return val_size_; }
  template <typename T>
  T *get();
  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObIAllocator &allocator);
  void release();
  template <typename T>
  void release(T &op_val) {
    op_val.~T();
  }
  TO_STRING_KV(K_(op_code), K_(op_scn), K_(op_val));
private:
  ObTxOpCode op_code_;
  share::SCN op_scn_;
  void *op_val_;
  int64_t val_size_; // for tx_data memory
};

typedef ObSEArray<ObTxOp, 1> ObTxOpArray;

class ObTxOpVector
{
public:
  ObTxOpVector() { reset(); }
  ~ObTxOpVector() { reset(); }
  void reset() {
    capacity_ = 0;
    count_ = 0;
    tx_op_ = nullptr;
  }
  int64_t get_capacity() { return capacity_; }
  int64_t get_count() { return count_; }
  ObTxOp* get_ptr() { return tx_op_; }
  int try_extend_space(int64_t count, ObIAllocator &allocator);
  ObTxOp *at(int64_t idx);
  int push_back(ObTxOp &tx_op);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObIAllocator &allocator);
  int64_t get_serialize_size() const;
  int check_stat();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t capacity_;
  int64_t count_;
  ObTxOp *tx_op_;
};

class ObTxDataOp
{
public:
  ObTxDataOp(share::ObTenantTxDataAllocator *allocator, share::ObTenantTxDataOpAllocator *op_allocator) :
    ref_cnt_(0),
    undo_status_list_(),
    tx_op_list_(),
    tx_data_allocator_(allocator),
    op_allocator_(op_allocator) {}
  ~ObTxDataOp() {}
  int64_t inc_ref() { return ATOMIC_AAF(&ref_cnt_, 1); }
  void dec_ref();
  ObTxOpVector &get_tx_op_list() { return tx_op_list_; }
  int64_t get_tx_op_size();
  ObUndoStatusList &get_undo_status_list() { return undo_status_list_; }
  common::SpinRWLock &get_lock() { return lock_; }
  int64_t get_ref() { return ref_cnt_; }
  int add_tx_op(ObTxOp &tx_op);
  int add_tx_op_batch(transaction::ObTransID tx_id, share::ObLSID ls_id, share::SCN op_scn, ObTxOpArray &tx_op_batch);
  int reserve_tx_op_space(int64_t count);

  TO_STRING_KV(K_(ref_cnt), K_(undo_status_list), K_(tx_op_list));
private:
  int64_t ref_cnt_;
  ObUndoStatusList undo_status_list_;
  ObTxOpVector tx_op_list_;
  share::ObTenantTxDataAllocator *tx_data_allocator_;
  share::ObTenantTxDataOpAllocator *op_allocator_;
  common::SpinRWLock lock_;
};

static_assert(sizeof(ObTxDataOp) < TX_DATA_SLICE_SIZE, "ObTxDataOp too large!");

template <typename T>
int ObTxOp::init(ObTxOpCode op_code, share::SCN op_scn, T* op_val, int64_t val_size)
{
  int ret = OB_SUCCESS;
  if (op_code == ObTxOpCode::INVALID || !op_scn.is_valid() || OB_ISNULL(op_val) || val_size < 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  #define INIT_TX_OP_TMP(OP_CODE, OP_TYPE)                                            \
  if (OB_SUCC(ret) && op_code_ ==  OP_CODE && !(std::is_same<T, OP_TYPE>::value)) {   \
    ret = OB_INVALID_ARGUMENT;                                                        \
  }
  #define INIT_TX_OP(TYPE, UNUSED) INIT_TX_OP_TMP TYPE

  LST_DO2(INIT_TX_OP, (), TX_OP_MEMBERS);
  #undef INIT_TX_OP_TMP
  #undef INIT_TX_OP
  if (OB_SUCC(ret)) {
    op_code_ = op_code;
    op_scn_ = op_scn;
    op_val_ = op_val;
    val_size_ = val_size;
  }
  return ret;
}

template <typename T>
T* ObTxOp::get()
{
  T* val = nullptr;

  #define GET_TX_OP_TMP(OP_CODE, OP_TYPE)  \
  if (std::is_same<T, OP_TYPE>::value) {   \
    val = (T*)op_val_;                     \
  }
  #define GET_TX_OP(TYPE, UNUSED) GET_TX_OP_TMP TYPE

  LST_DO2(GET_TX_OP, (), TX_OP_MEMBERS);
  #undef GET_TX_OP_TMP
  #undef GET_TX_OP

  return val;
}


}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_OP_
