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

#ifndef __OCEANBASE_KEYBTREE_DEPS_H_
#define __OCEANBASE_KEYBTREE_DEPS_H_

#include "lib/ob_abort.h"
#include "lib/allocator/ob_retire_station.h"

#define BTREE_ASSERT(x) if (OB_UNLIKELY(!(x))) { ob_abort(); }

namespace oceanbase
{
namespace keybtree
{

template<typename BtreeKey, typename BtreeVal>
struct BtreeKV;
template<typename BtreeKey, typename BtreeVal>
class ScanHandle;
template<typename BtreeKey, typename BtreeVal>
class ObKeyBtree;


using RawType = uint64_t;
enum
{
  MAX_CPU_NUM = 64,
  RETIRE_LIMIT = 1024,
  NODE_KEY_COUNT = 15,
  NODE_COUNT_PER_ALLOC = 128
};

template<typename BtreeKey, typename BtreeVal>
struct CompHelper
{
  OB_INLINE int compare(const BtreeKey search_key, const BtreeKey idx_key, int &cmp) const
  {
    return search_key.compare(idx_key, cmp);
  }
};

class RWLock
{
public:
  RWLock(): lock_(0) {}
  ~RWLock() {}
  void set_spin() { UNUSED(ATOMIC_AAF(&read_ref_, 1)); }
  bool try_rdlock() {
    bool lock_succ = true;
    uint32_t lock = ATOMIC_LOAD(&lock_);
    if (0 != (lock >> 16)) {
      lock_succ = false;
    } else {
      lock_succ = ATOMIC_BCAS(&lock_, lock, (lock + 2));
    }
    return lock_succ;
  }
  bool is_hold_wrlock(const uint16_t uid) const { return ATOMIC_LOAD(&writer_id_) == uid; }
  bool try_wrlock(const uint16_t uid) {
    bool lock_succ = true;
    while (!ATOMIC_BCAS(&writer_id_, 0, uid)) {
      if (1 == (ATOMIC_LOAD(&read_ref_) & 0x1)) {
        //sched_yield();
      } else {
        lock_succ = false;
        break;
      }
    }
    if (lock_succ) {
      while (ATOMIC_LOAD(&read_ref_) > 1) {
        sched_yield();
      }
    }
    return lock_succ;
  }
  void rdunlock() { UNUSED(ATOMIC_FAA(&read_ref_, -2)); }
  void wrunlock() { ATOMIC_STORE(&lock_, 0); }
private:
  union {
    struct {
      int16_t read_ref_;
      uint16_t writer_id_;
    };
    uint32_t lock_;
  };
};

class MultibitSet
{
  enum {
    LEN_OF_COUNTER = 4,
    LEN_OF_PER_INDEX = 4,
    LEN_OF_TOTAL_INDEX = 64
  };
public:
  MultibitSet(): data_(0) {}
  MultibitSet(RawType d): data_(d) {}
  OB_INLINE void reset() { data_ = 0; }
  OB_INLINE uint8_t at(const int index) const
  {
    MultibitSet temp;
    uint8_t ret = 0;
    temp.load(*this);
    BTREE_ASSERT(0 <= index && index < temp.count_);
    ret = ((uint8_t)(
      (*((uint64_t*)(buf_ + index * LEN_OF_PER_INDEX / 8))) >> (LEN_OF_COUNTER + (index * LEN_OF_PER_INDEX) % 8)
    )) & (0xff >> (8 - LEN_OF_PER_INDEX));
    return ret;
  }
  OB_INLINE void unsafe_insert(int index, uint8_t value) { data_ = cal_index_(*this, index, value); }
  OB_INLINE void inc_count(int16_t s) { count_ += s; }
  OB_INLINE int8_t size() const { return ATOMIC_LOAD((int16_t*)(&data_)) & ((1 << LEN_OF_COUNTER) - 1); }
  OB_INLINE bool free_insert(int index, uint8_t value)
  {
    MultibitSet old_v;
    old_v.load(*this);
    MultibitSet new_v(cal_index_(old_v, index, value));
    return ATOMIC_BCAS(&data_, old_v.data_, new_v.data_);
  }
  OB_INLINE void load(const MultibitSet& index) { data_ = ATOMIC_LOAD(&index.data_); }
  OB_INLINE RawType get() const { return ATOMIC_LOAD(&data_); }
private:
  OB_INLINE RawType cal_index_(MultibitSet &old, int index, uint8_t value)
  {
    uint8_t offset = LEN_OF_COUNTER + index * LEN_OF_PER_INDEX;
    RawType v = value;
    RawType mask = ((~0ULL) << offset);
    return (((old.data_ & mask) << LEN_OF_PER_INDEX) | (v << offset) | (old.data_ & ~mask)) + 1;
  }
  union {
    RawType data_;
    uint8_t buf_[0];
    struct {
      uint8_t count_:LEN_OF_COUNTER;
      RawType pending_data_:(LEN_OF_TOTAL_INDEX - LEN_OF_COUNTER);
    };
  };
public:
};

class WeightEstimate
{
public:
  enum { MAX_LEVEL = 64 };
  WeightEstimate(int64_t node_cnt) {
    for(int64_t i = 0, k = 1; i < MAX_LEVEL; i++) {
      weight_[i] = k;
      k *= node_cnt/2;
    }
  }
  int64_t get_weight(int64_t level) { return weight_[level]; }
private:
  int64_t weight_[MAX_LEVEL];
};

static int64_t estimate_level_weight(int64_t level)
{
  static WeightEstimate weight_estimate(NODE_KEY_COUNT);
  return weight_estimate.get_weight(level);
}

template<typename BtreeKey, typename BtreeVal>
class BtreeNode: public common::ObLink
{
private:
  friend class ScanHandle<BtreeKey, BtreeVal>;
  typedef BtreeKV<BtreeKey, BtreeVal> BtreeKV;
  typedef ObKeyBtree<BtreeKey, BtreeVal> ObKeyBtree;
  typedef CompHelper<BtreeKey, BtreeVal> CompHelper;
private:
  enum {
    MAGIC_NUM = 0xb7ee //47086
  };
public:
  BtreeNode(): host_(nullptr), max_del_version_(0), level_(0), magic_num_(MAGIC_NUM), lock_(), index_() {}
  ~BtreeNode() {}
  void reset();
  OB_INLINE void *get_host() { return host_; }
  OB_INLINE void set_host(void *host) { host_ = host; }
  OB_INLINE MultibitSet& get_index() { return this->index_; }
  OB_INLINE bool is_hold_wrlock(const uint16_t uid) const { return lock_.is_hold_wrlock(uid); }
  OB_INLINE int try_rdlock() { return lock_.try_rdlock() ? OB_SUCCESS : OB_EAGAIN; }
  OB_INLINE int try_wrlock(const uint16_t uid) { return lock_.try_wrlock(uid) ? OB_SUCCESS : OB_EAGAIN; }
  OB_INLINE void rdunlock() { lock_.rdunlock(); }
  OB_INLINE void wrunlock() { lock_.wrunlock(); }
  OB_INLINE void set_spin() { lock_.set_spin(); }
  OB_INLINE bool is_leaf() const { return 0 == level_; }
  OB_INLINE int16_t get_level() const { return level_; }
  // Only leaf nodes use index
  OB_INLINE int get_real_pos(int pos, MultibitSet *index = nullptr) const
  {
    if (is_leaf()) {
      pos = OB_NOT_NULL(index) ? index->at(pos) : index_.at(pos);
    }
    return pos;
  }
  OB_INLINE int size(MultibitSet *index = nullptr) const
  {
    return (is_leaf() && OB_NOT_NULL(index)) ? index->size() : index_.size();
  }
  OB_INLINE BtreeKey &get_key(int pos, MultibitSet *index = nullptr)
  {
    return kvs_[get_real_pos(pos, index)].key_;
  }
  OB_INLINE const BtreeKey &get_key(int pos, MultibitSet *index = nullptr) const
  {
    return kvs_[get_real_pos(pos, index)].key_;
  }
  OB_INLINE BtreeVal get_val_with_tag(int pos, MultibitSet *index = nullptr) const
  {
    return ATOMIC_LOAD(&kvs_[get_real_pos(pos, index)].val_);
  }
  OB_INLINE void set_val(int pos, BtreeVal val, MultibitSet *index = nullptr)
  {
    ATOMIC_STORE(&kvs_[get_real_pos(pos, index)].val_, val);
  }
  OB_INLINE BtreeVal get_val(int pos, MultibitSet *index = nullptr) const
  {
    return BtreeVal((uint64_t)get_val_with_tag(pos, index) & (~1ULL));
  }
  OB_INLINE BtreeVal get_val_with_tag(int pos, int64_t version, MultibitSet *index = nullptr) const {
    return version >= max_del_version_? get_val_with_tag(pos, index): get_val(pos, index);
  }
  int make_new_root(BtreeKey key1, BtreeNode *node_1, BtreeKey key2, BtreeNode *node_2, int16_t level, int64_t version);
  OB_INLINE int64_t get_max_del_version() const { return max_del_version_; }
  OB_INLINE void set_max_del_version(int64_t version) { ATOMIC_STORE(&max_del_version_, version); }
  bool is_overflow(const int64_t delta, MultibitSet *index = nullptr) { return size(index) + delta > NODE_KEY_COUNT; }
  void print(FILE *file, const int depth) const;
  OB_INLINE int find_pos(CompHelper &nh, BtreeKey key, bool &is_equal, int &pos, MultibitSet *index = nullptr)
  {
    int ret = binary_search_upper_bound(nh, key, is_equal, pos, index);
    pos -= 1;
    return ret;
  }
  int get_next_active_child(int pos, int64_t version, int64_t* cnt, MultibitSet *index = nullptr);
  int get_prev_active_child(int pos, int64_t version, int64_t* cnt, MultibitSet *index = nullptr);
  OB_INLINE void set_key_value(int pos, BtreeKey key, BtreeVal val)
  {
    kvs_[pos].key_ = key;
    ATOMIC_STORE(&kvs_[pos].val_, val);
  }
  OB_INLINE void insert_into_node(int pos, BtreeKey key, BtreeVal val)
  {
    // Upper stack should check if there is spliting, and here we don't check overflow.
    int count = size();
    if (pos < count) {
      if (is_leaf()) {
        pos = index_.at(pos);
      }
    } else if (is_leaf()) {
      index_.unsafe_insert(pos, count);
    } else {
      // Only inc count if it's not child.
      index_.inc_count(1);
    }
    set_key_value(pos, key, val);
  }
protected:
  OB_INLINE int binary_search_upper_bound(CompHelper &nh, BtreeKey key, bool &is_equal, int &pos, MultibitSet *index = nullptr)
  {
    // find first item > key
    // valid value to compare is within [start, end)
    // return idx is within [start, end], return end if all valid value < key
    int start = 0;
    int end = 0;
    int ret = OB_SUCCESS;
    // Only leaf node try append directly, other scence do nothign with index.
    if (is_leaf()) {
      index->load(index_);
      end = index->size();
    } else {
      end = size();
    }
    is_equal = false;
    while (OB_SUCC(ret) && start < end && !is_equal) {
      int mid = start + (end - start) / 2;
      int cmp_ret = 0;
      if (OB_FAIL(nh.compare(key, get_key(mid, index), cmp_ret))) {
        OB_LOG(ERROR, "failed to compare", K(key), K(get_key(mid, index)));
      } else if (0 == cmp_ret) {
        is_equal = true;
        end = mid + 1;
      } else if (cmp_ret < 0) {
        end = mid;
      } else {
        start = mid + 1;
      }
    }
    pos = end;
    return ret;
  }
  void copy(BtreeNode &dest, const int dest_start, const int start, const int end);
  void copy_and_insert(BtreeNode &dest_node, const int start, const int end, int pos,
                       BtreeKey key_1, BtreeVal val_1, BtreeKey key_2, BtreeVal val_2);
public:
  uint64_t get_tag(int pos, MultibitSet *index = nullptr) const
  {
    return (uint64_t)ATOMIC_LOAD(&kvs_[get_real_pos(pos, index)].val_) & 1ULL;
  }
  uint64_t check_tag(MultibitSet *index = nullptr) const;
  void replace_child(BtreeNode *new_node, const int pos, BtreeNode *child, int64_t del_version);
  void replace_child_and_key(BtreeNode *new_node, const int pos, BtreeKey key, BtreeNode *child, int64_t del_version);
  void split_child_no_overflow(BtreeNode *new_node, const int pos, BtreeKey key_1, BtreeVal val_1,
                               BtreeKey key_2, BtreeVal val_2, int64_t del_version);
  void split_child_cause_recursive_split(BtreeNode *new_node_1, BtreeNode *new_node_2, const int pos,
                                         BtreeKey key_1, BtreeVal val_1, BtreeKey key_2, BtreeVal val_2, int64_t del_version);
private:
  void *host_;  // 8byte
  int64_t max_del_version_; // 8byte
  int16_t level_; // 2byte
  uint16_t magic_num_; // 2byte
  RWLock lock_; // 4byte
  MultibitSet index_; // 8byte this is the real position of kv.
  BtreeKV kvs_[NODE_KEY_COUNT]; // 16 * 15 = 240byte
};

template<typename BtreeKey, typename BtreeVal>
class Path
{
private:
  typedef BtreeNode<BtreeKey, BtreeVal> BtreeNode;
  enum { MAX_DEPTH = 16 };
  struct Item
  {
    Item(): node_(nullptr), pos_(-1) {}
    ~Item() {}
    BtreeNode *node_;
    int pos_;
  };
public:
  Path(): depth_(0), is_found_(false) {}
  ~Path() {}
  void reset();
  int push(BtreeNode *node, const int pos);
  OB_INLINE int pop(BtreeNode *&node, int &pos)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(depth_ <= 0)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      node = nullptr;
      pos = -1;
    } else {
      depth_--;
      node = path_[depth_].node_;
      pos = path_[depth_].pos_;
    }
    return ret;
  }
  int top(BtreeNode *&node, int &pos);
  int top_k(int k, BtreeNode*& node, int& pos);
  int get_root_level() { return depth_; }
  bool is_empty() const { return 0 == depth_; }
  OB_INLINE int get(int64_t index, BtreeNode *&node, int &pos)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(index < 0 || depth_ <= index)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
      node = nullptr;
      pos = -1;
    } else {
      node = path_[index].node_;
      pos = path_[index].pos_;
    }
    return ret;
  }
  OB_INLINE void resize(int64_t depth) { depth_ = std::max(depth, 0l); }
  OB_INLINE void set_is_found(bool is_found = true) { is_found_ = is_found; }
  bool get_is_found() { return is_found_; }
private:
  int64_t depth_;
  Item path_[MAX_DEPTH];
  bool is_found_;
};

template<typename BtreeKey, typename BtreeVal>
class BaseHandle
{
private:
  typedef CompHelper<BtreeKey, BtreeVal> CompHelper;
public:
  BaseHandle(QClock& qclock): index_(), qclock_(qclock), qc_slot_(UINT64_MAX), comp_() {}
  ~BaseHandle() { release_ref(); }
  int acquire_ref();
  void release_ref()
  {
    qclock_.leave_critical(qc_slot_);
    qc_slot_ = UINT64_MAX;
  }
  OB_INLINE CompHelper &get_comp() { return comp_; }
protected:
  // record leaf index
  MultibitSet index_;
private:
  QClock& qclock_;
  uint64_t qc_slot_;
  CompHelper comp_;
};

template<typename BtreeKey, typename BtreeVal>
class GetHandle: public BaseHandle<BtreeKey, BtreeVal>
{
private:
  typedef BaseHandle<BtreeKey, BtreeVal> BaseHandle;
  typedef BtreeNode<BtreeKey, BtreeVal> BtreeNode;
  typedef ObKeyBtree<BtreeKey, BtreeVal> ObKeyBtree;
public:
  GetHandle(ObKeyBtree &tree): BaseHandle(tree.get_qclock()) { UNUSED(tree); }
  ~GetHandle() {}
  int get(BtreeNode *root, BtreeKey key, BtreeVal &val);
};

template<typename BtreeKey, typename BtreeVal>
class ScanHandle: public BaseHandle<BtreeKey, BtreeVal>
{
private:
  typedef BaseHandle<BtreeKey, BtreeVal> BaseHandle;
  typedef Path<BtreeKey, BtreeVal> Path;
  typedef BtreeNode<BtreeKey, BtreeVal> BtreeNode;
  typedef ObKeyBtree<BtreeKey, BtreeVal> ObKeyBtree;
private:
  Path path_;
  int64_t version_;
public:
  explicit ScanHandle(ObKeyBtree &tree): BaseHandle(tree.get_qclock()), version_(INT64_MAX) { UNUSED(tree); }
  ~ScanHandle() {}
  void reset()
  {
    this->release_ref();
    path_.reset();
  }
  int64_t get_root_level() { return path_.get_root_level(); }
  int get(BtreeKey &key, BtreeVal &val);
  int get(BtreeKey &key, BtreeVal &val, bool is_backward, BtreeKey*& last_key);
  // for estimate row count in range, leaf node is level 0
  int pop_level_node(const bool is_backward, const int64_t level, const double ratio,
      const int64_t gap_limit, int64_t &element_count, int64_t &phy_element_count,
      BtreeKey*& last_key, int64_t &gap_size);
  int pop_level_node(const int64_t level);
  int find_path(BtreeNode *root, BtreeKey key, int64_t version);
  int scan_forward(const int64_t level);
  int scan_backward(const int64_t level);
  int scan_forward(bool skip_inactive=false, int64_t* skip_cnt=NULL);
  int scan_backward(bool skip_inactive=false, int64_t* skip_cnt=NULL);
};

template<typename BtreeKey, typename BtreeVal>
class WriteHandle: public BaseHandle<BtreeKey, BtreeVal>
{
private:
  typedef BaseHandle<BtreeKey, BtreeVal> BaseHandle;
  typedef Path<BtreeKey, BtreeVal> Path;
  typedef BtreeNode<BtreeKey, BtreeVal> BtreeNode;
  typedef ObKeyBtree<BtreeKey, BtreeVal> ObKeyBtree;
private:
  ObKeyBtree &base_;
  Path path_;
  HazardList retire_list_;
  HazardList alloc_list_;
public:
  explicit WriteHandle(ObKeyBtree &tree): BaseHandle(tree.get_qclock()), base_(tree) {}
  ~WriteHandle() {}
  OB_INLINE bool &get_is_in_delete()
  {
    RLOCAL_INLINE(bool, is_in_delete);
    return is_in_delete;
  }
  BtreeNode *alloc_node();
  void free_node(BtreeNode *p);
  void free_list();
  void retire(const int btree_err);
  int find_path(BtreeNode *root, BtreeKey key)
  {
    int ret = OB_SUCCESS;
    int pos = -1;
    bool may_exist = true;
    bool is_found = false;
    BtreeNode *node = nullptr;
    MultibitSet *index = &this->index_;
    index->reset();
    if (OB_SUCC(path_.get(0, node, pos)) && node == root) {
      // find locked nodes, and remove them from path.
      while (OB_SUCC(path_.pop(node, pos)) && !is_hold_wrlock(node));
      while (OB_SUCC(path_.pop(node, pos)) && is_hold_wrlock(node));
      root = OB_NOT_NULL(node) ? node : root;
    } else {
      path_.resize(0);
    }
    ret = OB_SUCCESS;
    while (OB_NOT_NULL(root) && OB_SUCCESS == ret) {
      if (!may_exist) {
        pos = -1;
      } else if (is_found) {
        pos = 0;
      } else if (OB_FAIL(root->find_pos(this->get_comp(), key, is_found, pos, index))) {
        break;
      }
      if (pos < 0) {
        may_exist = false;
        if (get_is_in_delete()) {
          ret = OB_ENTRY_NOT_EXIST;
          break;
        }
      }
      if (OB_FAIL(path_.push(root, pos))) {
        // do nothing
      } else if (root->is_leaf()) {
        if (0 == index->size()) {
          index->load(root->get_index());
        }
        path_.set_is_found(is_found);
        root = nullptr;
      } else {
        root = (BtreeNode *)root->get_val(std::max(pos, 0));
      }
    }
    return ret;
  }
public:
  int insert_and_split_upward(BtreeKey key, BtreeVal &val, BtreeNode *&new_root);
  int tag_delete(BtreeKey key, BtreeVal& val, int64_t version, BtreeNode *&new_root);
  int tag_insert(BtreeKey key, BtreeNode *&new_root);
private:
  uint64_t check_tag(BtreeNode* node) { return OB_ISNULL(node) ? 0: node->check_tag(); }
  BtreeNode* add_tag(BtreeNode* node, uint64_t tag) { return (BtreeNode*)((uint64_t)node | tag); }
  int insert_into_node(BtreeNode *old_node, int pos, BtreeKey key, BtreeVal val, BtreeNode *&new_node_1, BtreeNode *&new_node_2);
  // judge whether it's wrlocked
  bool is_hold_wrlock(BtreeNode *node) { return !node->is_hold_wrlock(0); }
  int try_wrlock(BtreeNode *node);
  int tag_delete_on_leaf(BtreeNode* old_node, int pos, BtreeKey key, BtreeVal& val, int64_t version, BtreeNode*& new_node);
  int tag_insert_on_leaf(BtreeNode* old_node, int pos, BtreeKey key, BtreeNode*& new_node);
  int tag_upward(BtreeNode* new_node, BtreeNode*& new_root);
  int tag_leaf(BtreeNode *old_node, const int pos, BtreeNode*& new_node, uint64_t tag, int64_t version);
  int replace_child_by_copy(BtreeNode *old_node, const int pos, BtreeVal val, int64_t version, BtreeNode*& new_node);
  int replace_child(BtreeNode *old_node, const int pos, BtreeVal val);
  int replace_child_and_key(BtreeNode *old_node, const int pos, BtreeKey key, BtreeNode *child, BtreeNode *&new_node, int64_t version);
  int make_new_root(BtreeNode *&root, BtreeKey key1, BtreeNode *node_1, BtreeKey key2, BtreeNode *node_2, int16_t level, int64_t version);
  int split_child(BtreeNode *old_node, const int pos, BtreeKey key_1, BtreeVal val_1, BtreeKey key_2,
                  BtreeVal val_2, BtreeNode *&new_node_1, BtreeNode *&new_node_2, int64_t version);
};

// when modify this, pls modify buf_size in ob_keybtree.h.
template<typename BtreeKey, typename BtreeVal>
class Iterator
{
private:
  typedef ObKeyBtree<BtreeKey, BtreeVal> ObKeyBtree;
  typedef CompHelper<BtreeKey, BtreeVal> CompHelper;
  typedef ScanHandle<BtreeKey, BtreeVal> ScanHandle;
public:
  explicit Iterator(ObKeyBtree &btree): btree_(btree), scan_handle_(btree), jump_key_(nullptr),
                                        cmp_result_(0), comp_(scan_handle_.get_comp()),
                                        start_key_(), end_key_(), start_exclude_(false), end_exclude_(false),
                                        scan_backward_(false), is_iter_end_(false), iter_count_(0) {}
  ~Iterator() { scan_handle_.reset(); }
  void reset();
  bool is_reverse_scan() const { return scan_backward_; }
  CompHelper& get_comp() { return comp_; }
  int set_key_range(const BtreeKey min_key, const bool start_exclude,
                    const BtreeKey max_key, const bool end_exclude, int64_t version);
  int get_next(BtreeKey &key, BtreeVal &value);
  int64_t get_root_level() { return scan_handle_.get_root_level(); }
  int next_on_level(const int64_t level, BtreeKey& key, BtreeVal& value);
  int estimate_one_level(const int64_t level, const int64_t start_batch_count, const int64_t end_batch_count,
                         const int64_t max_node_count, const int64_t skip_range_limit, const double gap_ratio,
                         int64_t &level_physical_row_count, int64_t &level_element_count, int64_t &node_count);
  // physical_row_count is delta row count after purge
  // while element_count is the count of btree node
  int estimate_element_count(int64_t &physical_row_count, int64_t &element_count, const double ratio);
private:
  int iter_next(BtreeKey &key, BtreeVal &value);
  // for estimate row count in range
  int iter_next_batch_level_node(int64_t &element_count, const int64_t batch_count,
                                 const int64_t level, int64_t &gap_size,
                                 const int64_t gap_limit, int64_t &phy_element_count,
                                 const double ratio);
  int comp(BtreeKey& cur_key, BtreeKey* jump_key, int &cmp);
private:
  ObKeyBtree &btree_;
  ScanHandle scan_handle_;
  BtreeKey* jump_key_;
  int cmp_result_;
  CompHelper &comp_;
  BtreeKey start_key_;
  BtreeKey end_key_;
  bool start_exclude_;
  bool end_exclude_;
  bool scan_backward_;
  bool is_iter_end_;
  int64_t iter_count_;
};

}; // end namespace common
}; // end namespace oceanbase

#endif /* __OCEANBASE_KEYBTREE_DEPS_H_ */
