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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_DEFINE
#define OCEANBASE_STORAGE_OB_TX_DATA_DEFINE

#include "share/scn.h"
#include "share/allocator/ob_tx_data_allocator.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_table/ob_tx_data_hash_map.h"
#include "storage/tx/ob_trans_factory.h"

namespace oceanbase
{
namespace storage
{
class ObTxData;
class ObTxTable;
class ObTxDataTable;
class ObTxDataMemtable;
class ObTxDataMemtableMgr;
class ObTxDataOp;


// The memory structures associated with tx data are shown below. They are designed for several
// reasons:
// 1. Use the entire fixed-length memory block as much as possible and reuse the memory block to
// avoid memory fragmentation caused by frequent allocation of non-fixed-length memory
// 2. Avoid dumping failure caused by memory allocation failure
//
// The tx data table uses ObTenantTxDataAllocator to allocate multiple memory slices. There are three kinds of
// slice. The first kind of slice is divided into three areas. This kind of slice is used in link
// hash map of tx data memtable. :
// 1. HashNodes that ObLinkHashMap needs
// 2. Tx data
// 3. The linked list pointer for sorting, which points to another tx data
//
///                                    A Piece of Memory Slice
//                                            ObTxData
//  ------------------------------> +-------------------------+      +----->+----------------+
//                                  |                         |      |      |                |
//                                  |                         |      |      |                |
//                                  |      ObTxCommitData     |      |      |                |
//                                  |                         |      |      |                |
//                                  |                         |      |      |                |
//                                  +-------------------------+      |      +----------------+
//                                  |                         |      |      |                |
//                                  |       ObTxDataLink      |      |      |                |
//        TX_DATA_SLICE_SIZE        |     (next_hash_node_)   |------+      |                |
//                                  |  (next_sort_list_node_) |             |                |
//                                  |                         |             |                |
//                                  +-------------------------+             +----------------+
//                                  |                         |             |                |
//                                  |     ObUndoStatusList    |             |                |
//                                  |   (next_undo_status_)   |             |                |
//                                  |        (node_cnt_)      |             |                |
//                                  |                         |             |                |
//  ------------------------------> +-------------------------+             +----------------+/
//
//
// The second kind of slice is an ObUndoStatusNode, which is allocated when the transaction has some
// undo actions. It is divided into three areas too:
// 1. size, which means it contains how many undo action in this node
// 2. next pointer, which points to the next ObUndoStatusNode if exists
// 3. An array of ObUndoActions
//
//    A Piece of Memory Slice
//       (ObUndoStatusNode)
//  +-------------------------+     +------> +----------------+
//  |          size_          |     |        |                |
//  +-------------------------+     |        +----------------+
//  |     ObUndoStatusNode    |     |        |                |
//  |          *next_         |-----+        |                |
//  +-------------------------+              +----------------+
//  |                         |              |                |
//  |      ObUndoAction       |              |                |
//  |                         |              |                |
//  +-------------------------+              +----------------+
//  |            *            |              |                |
//  |            *            |              |                |
//  |            *            |              |                |
//  +-------------------------+              +----------------+
//  |                         |              |                |
//  |      ObUndoAction       |              |                |
//  |                         |              |                |
//  +-------------------------+              +----------------+
//
//

static const int TX_DATA_SLICE_SIZE = 128;
static const int UNDO_ACTION_SZIE = 16;
static const int TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE = (TX_DATA_SLICE_SIZE / UNDO_ACTION_SZIE) - 1;
static const int MAX_TX_DATA_MEMTABLE_CNT = 2;

using TxDataMap = ObTxDataHashMap;

// DONT : Modify this definition
struct ObUndoStatusNode
{
  int64_t size_;
  struct ObUndoStatusNode *next_;
  transaction::ObUndoAction undo_actions_[TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE];
  DECLARE_TO_STRING;
  ObUndoStatusNode() : size_(0), next_(nullptr) {}

  const ObUndoStatusNode &assign_value(const ObUndoStatusNode &rhs)
  {
    size_ = rhs.size_;
    for (int i = 0; i < size_; i++) {
      undo_actions_[i] = rhs.undo_actions_[i];
    }
    return *this;
  }
};

struct ObTxDataLinkNode
{
  ObTxData* next_;

  ObTxDataLinkNode() : next_(nullptr) {}
  void reset() { next_ = nullptr; }

  TO_STRING_KV(KP_(next));
};

struct ObUndoStatusList
{
private:
  static const int64_t UNIS_VERSION = 1;

public:
  ObUndoStatusList() : head_(nullptr), undo_node_cnt_(0) {}
  ObUndoStatusList &operator= (const ObUndoStatusList &rhs)
  {
    head_ = rhs.head_;
    undo_node_cnt_ = rhs.undo_node_cnt_;
    return *this;
  }
  ~ObUndoStatusList() { reset(); }

  void dump_2_text(FILE *fd) const;
  DECLARE_TO_STRING;

public:
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, share::ObTenantTxDataAllocator &tx_data_allocator);
  int64_t get_serialize_size() const;
  bool is_contain(const  transaction::ObTxSEQ seq_no, int32_t tx_data_state) const;
  void reset()
  {
    head_ = nullptr;
    undo_node_cnt_ = 0;
  }

private:
  bool is_contain_(const transaction::ObTxSEQ seq_no) const;
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf,
                   const int64_t data_len,
                   int64_t &pos,
                   share::ObTenantTxDataAllocator &tx_data_allocator);
  int64_t get_serialize_size_() const;

public:
  ObUndoStatusNode *head_;
  int32_t undo_node_cnt_;
  common::SpinRWLock lock_;
};

class ObTxCCCtx
{
public:
  // For Tx Ctx Table
  ObTxCCCtx(transaction::ObTxState state, share::SCN prepare_version)
    : state_(state), prepare_version_(prepare_version) {}
  // For Tx Data Table
  ObTxCCCtx() : state_(transaction::ObTxState::MAX), prepare_version_() {}
  TO_STRING_KV(K_(state),  K_(prepare_version));
public:
  transaction::ObTxState state_;
  share::SCN prepare_version_;
};

class ObTxCommitData
{
public:
  ObTxCommitData() { reset(); }
  void reset();
  TO_STRING_KV(K_(tx_id),
               K_(state),
               K_(commit_version),
               K_(start_scn),
               K_(end_scn));

public:
  enum : int32_t {
    RUNNING = 0,
    COMMIT = 1,
    ELR_COMMIT = 2,
    ABORT = 3,
    MAX_STATE_CNT
  };

  static const char *get_state_string(int32_t state);

public:
  transaction::ObTransID tx_id_;
  int32_t state_;
  share::SCN commit_version_;
  share::SCN start_scn_;
  share::SCN end_scn_;
};

class ObTxDataLink
{
public:
  ObTxDataLink() : sort_list_node_(), hash_node_() {}
  // used for mini merge
  ObTxDataLinkNode sort_list_node_;
  // used for hash conflict
  ObTxDataLinkNode hash_node_;
};

class ObTxDataOpGuard
{
public:
  ObTxDataOpGuard() : tx_data_op_(nullptr) {}
  ~ObTxDataOpGuard() { reset(); }
  int init(ObTxDataOp *tx_data_op);
  bool is_valid() const { return tx_data_op_ != nullptr; }
  void reset();
  ObTxDataOp *ptr() const { return tx_data_op_; }
  ObTxDataOp &operator*() {
    return *tx_data_op_;
  }
  ObTxDataOp* operator->() {
    return tx_data_op_;
  }
  ObTxDataOp* operator->() const {
    return tx_data_op_;
  }
  TO_STRING_KV(KP(tx_data_op_));
private:
  ObTxDataOp *tx_data_op_;
};

// DONT : Modify this definition
class ObTxData : public ObTxCommitData, public ObTxDataLink
{
public:
  enum ExclusiveType {
    NORMAL = 0,
    EXCLUSIVE,
    DELETED
  };
private:
  const static int64_t UNIS_VERSION = 1;
public:
  ObTxData()
      : ObTxCommitData(),
        ObTxDataLink(),
        tx_data_allocator_(nullptr),
        op_allocator_(nullptr),
        ref_cnt_(0),
        exclusive_flag_(ExclusiveType::NORMAL) {}
  ObTxData(const ObTxData &rhs);
  ObTxData &operator=(const ObTxData &rhs);
  ObTxData &operator=(const ObTxCommitData &rhs);
  const ObTxData &assign_without_undo(const ObTxData &rhs);

  ~ObTxData() {}
  void reset();
  OB_INLINE bool contain(const transaction::ObTransID &tx_id) { return tx_id_ == tx_id; }

  int init_tx_op();
  int reserve_undo(ObTxTable *tx_table);
  int64_t inc_ref()
  {
    int64_t ref_cnt = ATOMIC_AAF(&ref_cnt_, 1);
    return ref_cnt;
  }

  void dec_ref()
  {
#ifdef UNITTEST
  return;
#endif
    if (nullptr == tx_data_allocator_) {
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid slice allocator", KPC(this));
      ob_abort();
    } else if (0 == ATOMIC_SAF(&ref_cnt_, 1)) {
      op_guard_.reset();
      tx_data_allocator_->free(this);
    }
  }

  int check_tx_op_exist(share::SCN op_scn, bool &exist);

  /**
   * @brief Add a undo action with dynamically memory allocation.
   * See more details in alloc_undo_status_node() function of class ObTxDataTable
   *
   * @param[in] tx_table, the tx table contains this tx data
   * @param[in & out] undo_action, the undo action which is waiting to be added. If this undo action contains exsiting undo actions, the existing undo actions will be deleted and this undo action will be modified to contain all the deleted undo actions.
   * @param[in] undo_node, the undo status node can be used to extend undo status list if required, otherwise it will be released
   */
  OB_NOINLINE int add_undo_action(ObTxTable *tx_table,
                                  transaction::ObUndoAction &undo_action,
                                  ObUndoStatusNode *&undo_node);
  OB_NOINLINE int add_undo_action(ObTxTable *tx_table,
                                  transaction::ObUndoAction &undo_action) {
    ObUndoStatusNode *undo_status_node = nullptr;
    return add_undo_action(tx_table, undo_action, undo_status_node);
  }
  /**
   * @brief Check if this tx data is valid
   */
  bool is_valid_in_tx_data_table() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, share::ObTenantTxDataAllocator &tx_data_allocator);
  int64_t get_serialize_size() const;
  int64_t size_need_cache() const;

  void dump_2_text(FILE *fd) const;
  static void print_to_stderr(const ObTxData &tx_data);

  DECLARE_TO_STRING;

private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf,
                   const int64_t data_len,
                   int64_t &pos,
                   share::ObTenantTxDataAllocator &tx_data_allocator);
  int64_t get_serialize_size_() const;
  bool equals_(ObTxData &rhs);
  int merge_undo_actions_(ObTxDataTable *tx_data_table,
                          ObUndoStatusNode *&node,
                          transaction::ObUndoAction &undo_action);

public:

  OB_INLINE static ObTxData *get_tx_data_by_sort_list_node(ObTxDataLinkNode *sort_list_node)
  {
    if (nullptr == sort_list_node) {
      return nullptr;
    }
    ObTxData *tx_data = static_cast<ObTxData*>(reinterpret_cast<ObTxDataLink*>(sort_list_node));
    return tx_data;
  }

public:
  share::ObTenantTxDataAllocator *tx_data_allocator_;
  share::ObTenantTxDataOpAllocator *op_allocator_;
  int64_t ref_cnt_;
  ExclusiveType exclusive_flag_;
  ObTxDataOpGuard op_guard_;
};

static_assert(sizeof(ObTxData) < storage::TX_DATA_SLICE_SIZE, "ObTxData exceed slice_allocator fixed length");

class ObTxDataGuard
{
public:
  ObTxDataGuard() : tx_data_(nullptr) {}
  ~ObTxDataGuard() { reset(); }
  ObTxDataGuard &operator=(ObTxDataGuard &rhs) = delete;
  ObTxDataGuard(const ObTxDataGuard &other) = delete;

  int init(ObTxData *tx_data)
  {
    int ret = OB_SUCCESS;
    reset();
    if (OB_ISNULL(tx_data)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "init ObTxDataGuard with invalid arguments", KR(ret));
    } else if (tx_data->inc_ref() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected ref cnt on tx data", KR(ret), KP(tx_data), KPC(tx_data));
      ob_abort();
    } else {
      tx_data_ = tx_data;
    }
    return ret;
  }

  void reset()
  {
    if (OB_NOT_NULL(tx_data_)) {
      tx_data_->dec_ref();
      tx_data_ = nullptr;
    }
  }

  ObTxData *tx_data() { return tx_data_; }
  const ObTxData *tx_data() const { return tx_data_; }

  TO_STRING_KV(KPC_(tx_data));

private:
  ObTxData *tx_data_;
};

class ObTxDataMiniCache
{
private:
  static const int32_t TX_DATA_MINI_LRU_ITEM_CNT = 1 << 2; /* 4 */
  static const int32_t MINI_LRU_CONCURRENCY_MOD_MASK = TX_DATA_MINI_LRU_ITEM_CNT - 1;

  struct CacheItem {
    ObTxCommitData tx_data_;
    bool is_valid_;
    common::SpinRWLock lock_;

    CacheItem() : tx_data_(), is_valid_(false) {}

    void reset()
    {
      tx_data_.reset();
      is_valid_ = false;
    }

    TO_STRING_KV(K_(tx_data), K_(is_valid));
  };

public:
  int get(const transaction::ObTransID tx_id, ObTxCommitData &tx_commit_data)
  {
    int ret = OB_SUCCESS;
    int64_t thread_idx = get_itid() & MINI_LRU_CONCURRENCY_MOD_MASK;
    SpinRLockGuard guard(cache_items_[thread_idx].lock_);
    if (cache_items_[thread_idx].is_valid_) {
      if (tx_id == cache_items_[thread_idx].tx_data_.tx_id_) {
        tx_commit_data = cache_items_[thread_idx].tx_data_;
      } else {
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
    }
    return ret;
  }

  void set(const ObTxCommitData &tx_commit_data)
  {
    int64_t thread_idx = get_itid() & MINI_LRU_CONCURRENCY_MOD_MASK;
    SpinWLockGuard guard(cache_items_[thread_idx].lock_);
    if (cache_items_[thread_idx].tx_data_.tx_id_ != tx_commit_data.tx_id_) {
      cache_items_[thread_idx].tx_data_ = tx_commit_data;
      cache_items_[thread_idx].is_valid_ = true;
    }
  }

  void reset() {
    for (int i = 0; i < TX_DATA_MINI_LRU_ITEM_CNT; i++) {
      cache_items_[i].reset();
    }
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    for (int i = 0; i < TX_DATA_MINI_LRU_ITEM_CNT; i++) {
      if (i == 0) {
        databuff_printf(buf, buf_len, pos, "%d:", i);
      } else {
        databuff_printf(buf, buf_len, pos, ", %d:", i);
      }

      if (OB_UNLIKELY(cache_items_[i].is_valid_)) {
        databuff_print_obj(buf, buf_len, pos, cache_items_[i]);
      } else {
        databuff_printf(buf, buf_len, pos, "{}");
      }
    }
    J_ARRAY_END();
    return pos;
  }

private:
  CacheItem cache_items_[TX_DATA_MINI_LRU_ITEM_CNT];
};

struct ObReadTxDataArg{
  const transaction::ObTransID tx_id_;
  const int64_t read_epoch_;
  ObTxDataMiniCache &tx_data_mini_cache_;
  const bool skip_cache_;

  ObReadTxDataArg(const transaction::ObTransID tx_id,
                  const int64_t read_epoch,
                  ObTxDataMiniCache &mini_cache,
                  const bool skip_cache = false)
      : tx_id_(tx_id), read_epoch_(read_epoch),
        tx_data_mini_cache_(mini_cache), skip_cache_(skip_cache) {}

  TO_STRING_KV(K_(tx_id), K_(read_epoch), K_(tx_data_mini_cache), K_(skip_cache));
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_DEFINE_
