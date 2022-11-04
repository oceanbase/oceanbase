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

#include "lib/allocator/ob_slice_alloc.h"
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/ob_i_table.h"


namespace oceanbase
{

namespace storage
{

class ObTxData;
class ObTxTable;
class ObTxDataTable;
class ObTxDataMemtable;
class ObTxDataMemtableMgr;
class TxDataHashMapAllocHandle;

// The memory structures associated with tx data are shown below. They are designed for several
// reasons:
// 1. Use the entire fixed-length memory block as much as possible and reuse the memory block to
// avoid memory fragmentation caused by frequent allocation of non-fixed-length memory
// 2. Avoid dumping failure caused by memory allocation failure
//
// The tx data table uses ObSliceAlloc to allocate multiple memory slices. There are three kinds of
// slice. The first kind of slice is divided into three areas. This kind of slice is used in link
// hash map of tx data memtable. :
// 1. HashNodes that ObLinkHashMap needs
// 2. Tx data
// 3. The linked list pointer for sorting, which points to another tx data
//
//
//                                    A Piece of Memory Slice
//  ------------------------------> +-------------------------+             +----------------+
//                                  |                         |             |                |
//       TX_DATA_HASH_NODE_SIZE     |      TxDataHashNode     |             |                |
//                                  |                         |             |                |
//  ------------------------------> +-------------------------+             +----------------+
//                                  |                         |             |                |
//                                  |                         |             |                |
//           TX_DATA_SIZE           |         ObTxData        |             |                |
//                                  |                         |             |                |
//                                  |                         |             |                |
//  ------------------------------> +-------------------------+      +----->+----------------|
//                                  |                         |      |      |                |
//    TX_DATA_SORT_LIST_NODE_SIZE   |    TxDataSortListNode   |      |      |                |
//                                  |         (*next)         |------+      |                |
//  ------------------------------> +-------------------------+             +----------------+
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
// The third kind of slice is almost identical to the first. It used in tx data sstable cache. The
// lastest_used_time_stamp is used to clean the tx data sstable cache periodically.
//
//                                    A Piece of Memory Slice
//  ------------------------------> +-------------------------+
//                                  |                         |
//       TX_DATA_HASH_NODE_SIZE     |      TxDataHashNode     |
//                                  |                         |
//  ------------------------------> +-------------------------+
//                                  |                         |
//                                  |                         |
//           TX_DATA_SIZE           |         ObTxData        |
//                                  |                         |
//                                  |                         |
//  ------------------------------> +-------------------------+
//                                  |                         |
//    TX_DATA_SORT_LIST_NODE_SIZE   |  latest_used_time_stamp |
//                                  |                         |
//  ------------------------------> +-------------------------+
//

static const int TX_DATA_HASH_NODE_SIZE = 56;
static const int TX_DATA_SIZE = 72;
static const int TX_DATA_SORT_LIST_NODE_SIZE = 8;
static const int TX_DATA_SLICE_SIZE = 136;
static const int TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE = (TX_DATA_SLICE_SIZE / 16) - 1;
static const int TX_DATA_OFFSET_BETWEEN_DATA_AND_SORT_NODE
  = TX_DATA_SLICE_SIZE - TX_DATA_HASH_NODE_SIZE - TX_DATA_SORT_LIST_NODE_SIZE;
// Reserve 5KB to store the fields in tx data except undo_status
static const int OB_MAX_TX_SERIALIZE_SIZE = OB_MAX_USER_ROW_LENGTH - 5120;
static const int MAX_TX_DATA_MEMTABLE_CNT = 2;

using TxDataHashNode = common::LinkHashNode<transaction::ObTransID>;
using TxDataHashValue = common::LinkHashValue<transaction::ObTransID>;
using TxDataMap = common::ObLinkHashMap<transaction::ObTransID, ObTxData, TxDataHashMapAllocHandle>;

// DONT : Modify this definition
struct ObUndoStatusNode
{
  int64_t size_;
  struct ObUndoStatusNode *next_;
  transaction::ObUndoAction undo_actions_[TX_DATA_UNDO_ACT_MAX_NUM_PER_NODE];
  DECLARE_TO_STRING;
  ObUndoStatusNode() : size_(0), next_(nullptr) {}
};

struct ObTxDataSortListNode
{
  struct ObTxDataSortListNode* next_;

  ObTxDataSortListNode() : next_(nullptr) {}
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
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObSliceAlloc &slice_allocator);
  int64_t get_serialize_size() const;

  bool is_contain(const int64_t seq_no) const;

  void reset() 
  { 
    head_ = nullptr;
    undo_node_cnt_ = 0;
  }

private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf, const int64_t data_len, int64_t &pos, ObSliceAlloc &slice_allocator);
  int64_t get_serialize_size_() const;

public:
  ObUndoStatusNode *head_;
  int32_t undo_node_cnt_;
  common::SpinRWLock lock_;
};

// TODO: Redefine it
class ObTxCCCtx
{
public:
  // For Tx Ctx Table
  ObTxCCCtx(transaction::ObTxState state, int64_t prepare_version)
    : state_(state), prepare_version_(prepare_version) {}
  // For Tx Data Table
  ObTxCCCtx() : state_(transaction::ObTxState::MAX), prepare_version_(-1) {}
  TO_STRING_KV(K_(state),  K_(prepare_version));
public:
  transaction::ObTxState state_;
  int64_t prepare_version_;
};

class ObTxCommitData
{
public:
  ObTxCommitData() { reset(); }
  void reset();
  TO_STRING_KV(K_(tx_id), K_(state), K_(is_in_tx_data_table),
      K_(commit_version), K_(start_log_ts), K_(end_log_ts));
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
  bool is_in_tx_data_table_;
  int64_t commit_version_;
  int64_t start_log_ts_;
  int64_t end_log_ts_;
};

// DONT : Modify this definition
class ObTxData : public ObTxCommitData, public TxDataHashValue
{
  friend TxDataHashMapAllocHandle;
private:
  const static int64_t UNIS_VERSION = 1;
public:
  ObTxData() { reset(); }
  ObTxData(const ObTxData &rhs);
  ObTxData &operator=(const ObTxData &rhs);
  ObTxData &operator=(const ObTxCommitData &rhs);
  ~ObTxData() {}
  void reset();
  /**
   * @brief Add a undo action with dynamically memory allocation.
   * See more details in alloc_undo_status_node() function of class ObTxDataTable
   * 
   * @param[in] tx_table, the tx table contains this tx data
   * @param[in & out] undo_action, the undo action which is waiting to be added. If this undo action contains exsiting undo actions, the existing undo actions will be deleted and this undo action will be modified to contain all the deleted undo actions.
   * @param[in] undo_node, the undo status node can be used to extend undo status list if required, otherwise it will be released
   */
  int add_undo_action(ObTxTable *tx_table, transaction::ObUndoAction &undo_action, ObUndoStatusNode *undo_node = nullptr);
  /**
   * @brief Check if this tx data is valid
   */
  bool is_valid_in_tx_data_table() const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, ObSliceAlloc &slice_allocator);
  int64_t get_serialize_size() const;

  void dump_2_text(FILE *fd) const;
  static void print_to_stderr(const ObTxData &tx_data);
  

  DECLARE_TO_STRING;

private:
  int serialize_(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize_(const char *buf, const int64_t data_len, int64_t &pos, ObSliceAlloc &slice_allocator);
  int64_t get_serialize_size_() const;
  bool equals_(ObTxData &rhs);
  int merge_undo_actions_(ObTxDataTable *tx_data_table,
                           ObUndoStatusNode *&node,
                           transaction::ObUndoAction &undo_action);

public:
  /**
   * @brief The latest used time stamp use the same memeory as the sort list node. This function
   * only used when reading tx data in sstable cache because we need update the time stamp to decide
   * which tx data should be deleted.
   *
   * @param tx_data the pointer of tx data
   * @return int64_t* the pointer of latest used time stamp
   */
  OB_INLINE static int64_t *get_latest_used_ts_by_tx_data(ObTxData *tx_data) {
    if (nullptr == tx_data) {
      return nullptr;
    }
    char *sort_list_node_char_ptr
      = reinterpret_cast<char *>(tx_data) + TX_DATA_OFFSET_BETWEEN_DATA_AND_SORT_NODE;
    int64_t *latest_use_ts
      = reinterpret_cast<int64_t*>(sort_list_node_char_ptr);
    return latest_use_ts;
  }

  OB_INLINE static ObTxDataSortListNode *get_sort_list_node_by_tx_data(ObTxData *tx_data)
  {
    if (nullptr == tx_data) {
      return nullptr;
    }
    char *sort_list_node_char_ptr
      = reinterpret_cast<char *>(tx_data) + TX_DATA_OFFSET_BETWEEN_DATA_AND_SORT_NODE;
    ObTxDataSortListNode *sort_list_node
      = reinterpret_cast<ObTxDataSortListNode *>(sort_list_node_char_ptr);
    return sort_list_node;
  }

  OB_INLINE static TxDataHashNode *get_hash_node_by_tx_data(ObTxData *tx_data)
  {
    if (nullptr == tx_data) {
      return nullptr;
    }
    char *hash_node_char_ptr = reinterpret_cast<char *>(tx_data) - TX_DATA_HASH_NODE_SIZE;
    TxDataHashNode *hash_node = reinterpret_cast<TxDataHashNode *>(hash_node_char_ptr);
    return hash_node;
  }

  OB_INLINE static ObTxData *get_tx_data_by_hash_node(TxDataHashNode *hash_node)
  {
    if (nullptr == hash_node) {
      return nullptr;
    }
    char *tx_data_char_ptr = reinterpret_cast<char *>(hash_node) + TX_DATA_HASH_NODE_SIZE;
    ObTxData *tx_data = reinterpret_cast<ObTxData *>(tx_data_char_ptr);
    return tx_data;
  }

  OB_INLINE static ObTxData *get_tx_data_by_sort_list_node(ObTxDataSortListNode *sort_list_node)
  {
    if (nullptr == sort_list_node) {
      return nullptr;
    }
    char *tx_data_char_ptr
      = reinterpret_cast<char *>(sort_list_node) - TX_DATA_OFFSET_BETWEEN_DATA_AND_SORT_NODE;
    ObTxData *tx_data = reinterpret_cast<ObTxData *>(tx_data_char_ptr);
    return tx_data;
  }

public:
  ObUndoStatusList undo_status_list_;
};


class TxDataHashMapAllocHandle
{
  using SliceAllocator = ObSliceAlloc;

public:
  explicit TxDataHashMapAllocHandle(SliceAllocator *slice_allocator)
    : slice_allocator_(slice_allocator)
  {}

  TxDataHashMapAllocHandle(const TxDataHashMapAllocHandle &rhs)
  {
    slice_allocator_ = rhs.slice_allocator_;
  }

  // do nothing
  ObTxData *alloc_value() { return nullptr; }
  // do nothing
  void free_value(ObTxData *tx_data) { UNUSED(tx_data); }

  // construct TxDataHashNode with the memory allocated by slice allocator
  TxDataHashNode *alloc_node(ObTxData *tx_data);

  // the memory allocated by slice allocator in tx data table is freed in link hash map
  void free_node(TxDataHashNode *node);
  void set_slice_allocator(SliceAllocator *slice_allocator) { slice_allocator_ = slice_allocator; }

private:
  void free_undo_list_(ObUndoStatusNode *node_ptr);

private:
  SliceAllocator *slice_allocator_;
};

class ObTxDataGuard
{
public:
  ObTxDataGuard() : is_inited_(false), tx_data_(nullptr), tx_data_map_(nullptr) {}
  virtual ~ObTxDataGuard() { reset(); }

  int init(ObTxData *tx_data, TxDataMap *tx_data_map)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      reset();
    }
    if (OB_ISNULL(tx_data) || OB_ISNULL(tx_data_map)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "init ObTxDataGuard with invalid arguments", KR(ret));
    } else {
      tx_data_ = tx_data;
      tx_data_map_ = tx_data_map;
      is_inited_ = true;
    }

    return ret;
  }

  void reset()
  {
    tx_data_map_->revert(tx_data_);
    tx_data_ = nullptr;
    tx_data_map_ = nullptr;
    is_inited_ = false;
  }

  ObTxData &tx_data() { return *tx_data_; }

private:
  bool is_inited_;
  ObTxData *tx_data_;
  TxDataMap *tx_data_map_;
};

class ObTxDataMemtableWriteGuard
{
public:
  ObTxDataMemtableWriteGuard() : size_(0)
  {
  }
  ~ObTxDataMemtableWriteGuard() { reset(); }

  void reset();

  TO_STRING_KV(K(size_), K(handles_[0]), K(handles_[1]));

public:
  int64_t size_;
  ObTableHandleV2 handles_[MAX_TX_DATA_MEMTABLE_CNT];
};


}  // namespace storage

}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_DEFINE_
