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

#ifndef OCEANBASE_COMMON_HASH_POINTER_HASHMAP_
#define OCEANBASE_COMMON_HASH_POINTER_HASHMAP_

#include "lib/hash_func/ob_hash_func.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/utility.h"
#include "lib/hash/ob_hashutils.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
/**
 * not thread safe hash map only for pointer
 */
template <class K, class V, template <class, class> class GetKey>
class ObPointerHashArray
{
public:
  struct Iterator
  {
    Iterator() : ha_(NULL), pos_(NULL), end_(NULL) {}
    Iterator(const ObPointerHashArray *ha, const V *pos, const V *end)
        : ha_(ha), pos_(pos), end_(end)
    {
      advance_past_empty_and_deleted();
    }
    V &operator*() const { return const_cast<V &>(*pos_); }
    V *operator->() const { return &(operator*()); }

    void advance_past_empty_and_deleted()
    {
      while (pos_ != end_ && (ha_->test_empty(*this) || ha_->test_erased(*this))) {
        ++pos_;
      }
    }

    Iterator &operator++()
    {
      if (pos_ == end_) {
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "pos is equal to end, unexpected error!");
      } else {
        ++pos_;
        advance_past_empty_and_deleted();
      }
      return *this;
    }

    bool operator==(const Iterator &it) const { return pos_ == it.pos_; }
    bool operator!=(const Iterator &it) const { return pos_ != it.pos_; }

    const ObPointerHashArray *ha_;
    const V *pos_;
    const V *end_;
  };

public:
  explicit ObPointerHashArray(const int64_t array_mem_size,
                              const double load_factor = 0.7)
      : capacity_((array_mem_size - sizeof(ObPointerHashArray)) / sizeof(V)),
        max_entries_((int64_t)((double)capacity_ * load_factor)),
        anchor_mask_(next_pow2(capacity_) - 1),
        empty_v_((V)0),
        erased_v_((V) - 1),
        count_(0),
        item_count_(0)
  {
    memset(cells_, 0, capacity_ * sizeof(V));
  }

  ~ObPointerHashArray() {}
  static int64_t get_hash_array_mem_size(const int64_t ele_cnt)
  {
    return (std::max((static_cast<int64_t>(MIN_HASH_ARRAY_ITEM_COUNT)),
                     ele_cnt * 2) * sizeof(void *) + sizeof(ObPointerHashArray));
  }

  ObPointerHashArray &operator =(const ObPointerHashArray &other)
  {
    if (this != &other) {
      MEMCPY(this, &other, sizeof(ObPointerHashArray) + other.capacity_ * sizeof(V));
    }
    return *this;
  }

  ObPointerHashArray(const ObPointerHashArray &other) : capacity_(0), max_entries_(0),
                                                        anchor_mask_(0), empty_v_((V)0), erased_v_((V) - 1), count_(0), item_count_(0)
  {
    memset(cells_, 0, capacity_ * sizeof(V));
    *this = other;
  }

  Iterator begin() { return Iterator(this, cells_, cells_ + capacity_); }
  Iterator end() { return Iterator(this, cells_ + capacity_, cells_ + capacity_); }

  /**
   * put a key value pair into HashMap
   * when overwrite = 0, do not overwrite existing <key,value> pair
   * when overwrite != 0 overwrite existing value
   * @retval OB_SUCCESS  success
   * @retval OB_HASH_EXIST key exist when overwrite = 0
   * @retval other errors
   */
  int set_refactored(const K &key, const V &value, V &over_write_value, int overwrite = 0)
  {
    int ret = OB_SUCCESS;
    int64_t pos = -1;
    int64_t erased_pos = -1;
    over_write_value = (V(0));

    bool exist = false;
    ret = find_set_pos(key, value, pos, erased_pos, exist, overwrite);
    if (OB_SUCC(ret)) {
      ret = set_value(pos, value, over_write_value);
    } else {
      //HASH_EXIST, HASH_NOT_EXIST or HASH_FULL
    }

    return ret;
  }

  /**
   * put a key value pair into HashMap
   * when overwrite = 0, do not overwrite existing <key,value> pair
   * when overwrite != 0 overwrite existing value
   * @retval OB_SUCCESS  success
   * @retval OB_HASH_EXIST key exist when overwrite = 0
   * @retval other errors
   */
  int set_refactored(const K &key, const V &value, int overwrite = 0)
  {
    V over_write_value = empty_v_;
    return set_refactored(key, value, over_write_value, overwrite);
  }


  int find_set_pos(const K &key, const V &value, int64_t &pos,
                   int64_t &erased_pos, bool &exist, int overwrite)
  {
    int ret = OB_SUCCESS;
    pos = -1;
    erased_pos = -1;

    ret = placement_hash_find_set_pos(key, overwrite, pos, exist);
    if (OB_SUCC(ret) && !exist) {
      if (OB_UNLIKELY(value == erased_v_)) {
        //not allow to insert erased value,
        //but it can overwrite with erased value
        ret = OB_HASH_NOT_EXIST;
      } else if (is_full()) {
        // full but we found one erased pos
        if (pos < 0 || pos >= capacity_) {
          COMMON_LOG(ERROR, "unexpected error", K(pos));
        } else if (cells_[pos] == erased_v_) {
          erased_pos = pos;
        } else {
          // do nothing
        }
        ret = OB_HASH_FULL;
      }
    } else {
      //HASH_EXIST, HASH_OVERWRITE_SUCC or HASH_FULL
    }

    return ret;
  }

  int set_value(const int64_t pos, const V &value, V &over_write_value)
  {
    int ret = OB_SUCCESS;
    over_write_value = empty_v_;

    if (pos < 0 || pos >= capacity_) {
      COMMON_LOG(WARN, "invalid argument", K(pos));
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (cells_[pos] == empty_v_) {
        ++count_;
        ++item_count_;
      } else if (cells_[pos] == erased_v_) {
        ++item_count_;
      } else {
        over_write_value = cells_[pos];
        // overwrite
        if (OB_UNLIKELY(value == erased_v_)) {
          --item_count_;
        }
      }
      cells_[pos] = value;
      if (item_count_ > count_) {
        COMMON_LOG(ERROR, "unexpected error, item_count_ is less to count_");
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

  int set_value(const int64_t pos, const V &value)
  {
    V over_write_value = empty_v_;
    return set_value(pos, value, over_write_value);
  }

  /**
   * @retval OB_SUCCESS get the corresponding value of key
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other errors
   */
  int get_refactored(const K &key, V &value) const
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    if (OB_SUCC(placement_hash_search(key, pos))) {
      if (pos < 0 || pos >= capacity_) {
        COMMON_LOG(ERROR, "unexpected error", K(pos), K_(capacity));
        ret = OB_ERR_UNEXPECTED;
      } else {
        value = cells_[pos];
      }
    }
    return ret;
  }

  /**
   * @retval value get the corresponding value of key
   * @retval NULL key does not exist
   */
  const V *get(const K &key) const
  {
    const V *ret = NULL;
    int64_t pos = 0;
    if (OB_SUCCESS == placement_hash_search(key, pos)) {
      if (pos < 0 || pos >= capacity_) {
        COMMON_LOG(ERROR, "unexpected error", K(pos), K_(capacity));
      } else {
        ret = &cells_[pos];
      }
    }
    return ret;
  }

  V *get(const K &key)
  {
    V *ret = NULL;
    int64_t pos = 0;
    if (OB_SUCCESS == placement_hash_search(key, pos)) {
      if (pos < 0 || pos >= capacity_) {
        COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error", K(pos), K_(capacity));
      } else {
        ret = &cells_[pos];
      }
    }
    return ret;
  }

  // @retval OB_SUCCESS success
  // @retval OB_HASH_NOT_EXIST key not found
  // @retval other errors
  inline int erase_refactored(const K &key, V &erased_value)
  {
    return set_refactored(key, erased_v_, erased_value, 1);
  }

  // @retval OB_SUCCESS success
  // @retval OB_HASH_NOT_EXIST key not found
  // @retval other errors
  inline int erase_refactored(const K &key)
  {
    V erased_value = empty_v_;
    return set_refactored(key, erased_v_, erased_value, 1);
  }

  inline int erase(Iterator it)
  {
    int ret = OB_SUCCESS;
    if ((erased_v_ != (*it)) && (empty_v_ != (*it))) {
     (*it) = erased_v_;
     --item_count_;
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid iterator", K(ret));
    }
    return ret;
  }

  // clears all keys and values in the map and resets all counters.
  // Not thread safe.
  inline void clear()
  {
    memset(cells_, 0, capacity_ * sizeof(V));
    count_ = 0;
    item_count_ = 0;
  }

  inline int64_t count() const { return count_; }
  inline int64_t item_count() const { return item_count_; }
  inline bool is_full() const { return count_ >= max_entries_; }

  // all position is used, and erase_count > useful slot of one sub_map * 10%
  inline bool need_rebuild() const
  {
    return (((count_ - item_count_) > (max_entries_ / 10)) && is_full());
  }

  int64_t get_max_entries() const { return max_entries_; }

  inline void dump_keys(void) const
  {
    for (int i = 0; i < capacity_; i++) {
      if (cells_[i] != empty_v_ && cells_[i] != erased_v_) {
        COMMON_LOG(INFO, "", K(get_key_(cells_[i])));
      }
    }
  }

  K get_key(Iterator &it) const { return get_key_(*it); }

  bool test_empty(Iterator &it) const { return *it == empty_v_; }
  bool test_erased(Iterator &it) const { return *it == erased_v_; }

private:
  uint64_t key_to_anchor_idx(const K &key) const
  {
    uint64_t hash_val = do_hash(key);
    uint64_t probe = hash_val & anchor_mask_;
    return OB_LIKELY(probe < static_cast<uint64_t>(capacity_)) ? probe : hash_val % capacity_;
  }

  int64_t probe_next(int64_t idx) const
  {
    // linear probing
    idx += 1;
    // Avoid modulus because it's slow
    return OB_LIKELY(idx < capacity_) ? idx : (idx - capacity_);
  }

  int placement_hash_search(const K &key_in, int64_t &pos) const
  {
    int hash_ret = OB_SUCCESS;
    pos = -1;

    if (item_count_ <= 0) {
      hash_ret = OB_HASH_NOT_EXIST;
    } else {
      for (int64_t i = key_to_anchor_idx(key_in), num_probes = 0; ; i = probe_next(i)) {
        if (OB_UNLIKELY(cells_[i] == empty_v_)) {
          // if we hit an empty element, this key does not exist
          hash_ret = OB_HASH_NOT_EXIST;
          break;
        } else if (OB_UNLIKELY(cells_[i] == erased_v_)) {
          // erased element, skip it
        } else if (OB_LIKELY(get_key_(cells_[i]) == key_in)) {
          pos = i;
          break;
        }
        ++num_probes;
        if (OB_UNLIKELY(num_probes >= capacity_)) {
          // probed every cell...fail
          hash_ret = OB_HASH_NOT_EXIST;
          break;
        }
      }
    }

    return hash_ret;
  }

  int placement_hash_find_set_pos(const K &key_in, int overwrite, int64_t &pos, bool &exist) const
  {
    int ret = OB_SUCCESS;
    pos = -1;
    exist = false;

    for (int64_t i = key_to_anchor_idx(key_in), num_probes = 0; ; i = probe_next(i)) {
      if (OB_UNLIKELY(cells_[i] == empty_v_)) {
        // if we hit an empty element, this key does not exist, can insert
        if (-1 == pos) {
          // found no erased place to insert
          pos = i;
        }
        break;
      } else if (OB_UNLIKELY(cells_[i] == erased_v_)) {
        if (-1 == pos) {
          // found the first one erased place, continue
          pos = i;
        }
      } else if (get_key_(cells_[i]) == key_in) {
        exist = true;
        if (overwrite == 0) {
          ret = OB_HASH_EXIST;
        }
        // found existent key
        pos = i;
        break;
      } else {
        // do nothing
      }

      ++num_probes;
      if (OB_UNLIKELY(num_probes >= capacity_)) {
        // probed every cell...fail
        ret = OB_HASH_FULL;
        break;
      }
    }

    return ret;
  }

public:
  static const int64_t MIN_HASH_ARRAY_ITEM_COUNT = 16;
private:
  int64_t capacity_;
  int64_t max_entries_;
  int64_t anchor_mask_;
  V empty_v_;
  V erased_v_;
  // position count that have been used(include used by V and erase_v_)
  int64_t count_;
  // actual meaningful item count(not include erase_v)
  int64_t item_count_;
  GetKey<K, V>  get_key_;
  // This must be the last field of this class
  V cells_[0];
};

template < class K, class V, template <class, class> class GetKey,
           int64_t default_size = OB_MALLOC_NORMAL_BLOCK_SIZE, class Allocator = ModulePageAllocator >
class ObPointerHashMap
{
  typedef ObPointerHashArray<K, V, GetKey> SubMap;
public:
  typedef typename SubMap::Iterator iterator;

  explicit ObPointerHashMap(const lib::ObLabel &label = ObModIds::OB_HASH_NODE)
    : ObPointerHashMap(lib::ObMemAttr(OB_SERVER_TENANT_ID, label))
  {
  }

  explicit ObPointerHashMap(const lib::ObMemAttr &attr)
      : sub_map_count_(0), sub_map_mem_size_(default_size), allocator_(attr)
  {
    memset(sub_maps_, 0, sizeof(sub_maps_));
  }

  explicit ObPointerHashMap(const Allocator &alloc)
      : sub_map_count_(0), sub_map_mem_size_(default_size), allocator_(alloc)
  {
    memset(sub_maps_, 0, sizeof(sub_maps_));
  }

  ~ObPointerHashMap()
  {
    destroy();
  }

  int assign(const ObPointerHashMap &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      destroy();
      sub_map_count_ = other.sub_map_count_;
      sub_map_mem_size_ = other.sub_map_mem_size_;
      allocator_ = other.allocator_;
      for (int64_t i = 0; OB_SUCC(ret) && i < other.sub_map_count_; ++i) {
        if (NULL == (sub_maps_[i] = create_sub_map(other.sub_maps_[i]))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(ERROR, "create sub map failed", K(ret), K(*this));
        }
      }
    }
    return ret;
  }

  ObPointerHashMap &operator =(const ObPointerHashMap &other)
  {
    int ret = assign(other);
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "assign failed", K(ret), K(*this));
    }
    return *this;
  }

  explicit ObPointerHashMap(const ObPointerHashMap &other)
      : sub_map_count_(0), sub_map_mem_size_(default_size)
  {
    memset(sub_maps_, 0, sizeof(SubMap *) * MAX_SUB_MAP_COUNT);
    *this = other;
  }

  void destroy()
  {
    for (int64_t i = 0; i < sub_map_count_; ++i) {
      if (NULL != sub_maps_[i]) {
        allocator_.free(sub_maps_[i]);
        sub_maps_[i] = NULL;
      }
    }
    sub_map_count_ = 0;
    sub_map_mem_size_ = 0;
  }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      sub_maps_[0] = create_sub_map();
      if (NULL != sub_maps_[0]) {
        sub_map_count_ = 1;
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    return ret;
  }

  /**
   * put a key value pair into HashMap
   * when overwrite = 0, do not overwrite existing <key,value> pair
   * when overwrite != 0 and overwrite_key = 0, overwrite existing value
   * when overwrite != 0 and overwrite_key != 0, overwrite existing <key,value> pair
   * @retval OB_SUCCESS  success
   * @retval OB_HASH_EXIST key exist when overwrite = 0
   * @retval other errors
   */
  int set_refactored(const K &key, const V &value, V &over_write_value, int overwrite = 0, int overwrite_key = 0)
  {
    int ret = OB_SUCCESS;
    int64_t sub_map_idx = -1;
    int64_t pos = -1;
    UNUSED(overwrite_key);
    over_write_value = (V(0));

    if (OB_UNLIKELY(OB_SUCCESS != (ret = init()))) {
      COMMON_LOG(WARN, "not initialize pointer hash map", K(ret), K(*this));
    } else {
      ret = find_set_pos(key, value, sub_map_idx, pos, overwrite);
      if (OB_SUCC(ret)) {
        if (sub_map_idx < 0 || sub_map_idx >= sub_map_count_) {
          COMMON_LOG(ERROR, "unexpected error", K(*this), K(sub_map_idx), K_(sub_map_count));
          ret = OB_ERR_UNEXPECTED;
        } else {
          ret = sub_maps_[sub_map_idx]->set_value(pos, value, over_write_value);
        }
      }

      if (OB_HASH_FULL == ret) {
        SubMap *sub_map = resize();
        if (NULL != sub_map) {
          ret = sub_map->set_refactored(key, value, over_write_value, overwrite);
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }

    return ret;
  }

  /**
   * put a key value pair into HashMap
   * when overwrite = 0, do not overwrite existing <key,value> pair
   * when overwrite != 0 and overwrite_key = 0, overwrite existing value
   * when overwrite != 0 and overwrite_key != 0, overwrite existing <key,value> pair
   * @retval OB_SUCCESS  success
   * @retval OB_HASH_EXIST key exist when overwrite = 0
   * @retval other errors
   */
  int set_refactored(const K &key, const V &value, int overwrite = 0, int overwrite_key = 0)
  {
    V over_write_value = (V(0));
    return set_refactored(key, value, over_write_value, overwrite, overwrite_key);
  }

  /**
   * @retval OB_SUCCESS get the corresponding value of key
   * @retval OB_HASH_NOT_EXIST key does not exist
   * @retval other errors
   */
  int get_refactored(const K &key, V &value) const
  {
    int hash_ret = OB_HASH_NOT_EXIST;

    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      hash_ret = OB_NOT_INIT;
      COMMON_LOG_RET(WARN, hash_ret, "not initialize pointer hash map", K(*this), K(lbt()));
    } else {
      for (int64_t i = 0; i < sub_map_count_; ++i) {
        if (OB_HASH_NOT_EXIST != (hash_ret = sub_maps_[i]->get_refactored(key, value))) {
          break;
        }
      }
    }

    return hash_ret;
  }

  /**
   * @retval value get the corresponding value of key
   * @retval NULL key does not exist
   */
  const V *get(const K &key) const
  {
    const V *ret = NULL;

    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      COMMON_LOG_RET(WARN, common::OB_NOT_INIT, "not initialize pointer hash map", K(*this), K(lbt()));
    } else {
      for (int64_t i = 0; i < sub_map_count_; ++i) {
        if (NULL != (ret = sub_maps_[i]->get(key))) {
          break;
        }
      }
    }

    return ret;
  }

  V *get(const K &key)
  {
    V *ret = NULL;

    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      COMMON_LOG_RET(WARN, common::OB_NOT_INIT, "not initialize pointer hash map", K(*this));
    } else {
      for (int64_t i = 0; i < sub_map_count_; ++i) {
        if (NULL != (ret = sub_maps_[i]->get(key))) {
          break;
        } else {
          continue;
        }
      }
    }

    return ret;
  }

  void dump_keys(void) const
  {
    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      COMMON_LOG_RET(WARN, common::OB_NOT_INIT, "not initialize pointer hash map", K(*this));
    } else {
      for (int64_t i = 0; i < sub_map_count_; ++i) {
        sub_maps_[i]->dump_keys();
      }
    }
  }

  // should never exceed 1
  // @retval OB_SUCCESS success
  // @retval OB_HASH_NOT_EXIST key not found
  // @retval other errors
  int erase_refactored(const K &key, V &erased_value)
  {
    int ret = OB_HASH_NOT_EXIST;

    if (OB_UNLIKELY(NULL == sub_maps_[0])) {
      COMMON_LOG(WARN, "not initialize pointer hash map", K(*this));
      ret = OB_NOT_INIT;
    } else {
      // Check each map successively. If one succeeds, we're done!
      for (int64_t i = 0; i < sub_map_count_; ++i) {
        if (OB_HASH_NOT_EXIST != (ret = sub_maps_[i]->erase_refactored(key, erased_value))) {
          break;
        } else {
          continue;
        }
      }
    }

    return ret;
  }

  // @retval OB_SUCCESS success
  // @retval OB_HASH_NOT_EXIST key not found
  // @retval other errors
  int erase_refactored(const K &key)
  {
    V erased_value = (V(0));
    return erase_refactored(key, erased_value);
  }

  int erase(iterator &it, const int64_t sub_map_id)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(sub_map_id >= 0) && OB_UNLIKELY(sub_map_id < sub_map_count_)) {
      ret = sub_maps_[sub_map_id]->erase(it);
    } else {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid input value", K(ret), K(sub_map_id), K(*this));
    }
    return ret;
  }

  void clear()
  {
    if (NULL != sub_maps_[0] && sub_map_count_ > 0) {
      sub_maps_[0]->clear();
      for (int64_t i = 1; i < sub_map_count_; ++i) {
        if (NULL != sub_maps_[i]) {
          allocator_.free(sub_maps_[i]);
          sub_maps_[i] = NULL;
        }
      }
      sub_map_count_ = 1;
    }
  }

  int64_t count() const
  {
    int64_t total_count = 0;
    for (int64_t i = 0; i < sub_map_count_; ++i) {
      if (NULL != sub_maps_[i]) {
        total_count += sub_maps_[i]->count();
      }
    }
    return total_count;
  }

  int64_t item_count() const
  {
    int64_t total_item_count = 0;
    for (int64_t i = 0; i < sub_map_count_; ++i) {
      if (NULL != sub_maps_[i]) {
        total_item_count += sub_maps_[i]->item_count();
      }
    }
    return total_item_count;
  }

  iterator begin(const int64_t sub_map_id)
  {
    iterator iter;
    if (OB_UNLIKELY(sub_map_id >= 0) && OB_UNLIKELY(sub_map_id < sub_map_count_)) {
      iter = sub_maps_[sub_map_id]->begin();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid sub map id", K(*this), K(sub_map_id));
    }
    return iter;
  }

  iterator end(const int64_t sub_map_id)
  {
    iterator iter;
    if (OB_UNLIKELY(sub_map_id >= 0) && OB_UNLIKELY(sub_map_id < sub_map_count_)) {
      iter = sub_maps_[sub_map_id]->end();
    } else {
      COMMON_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid sub map id", K(*this), K(sub_map_id));
    }
    return iter;
  }

  int64_t get_sub_map_count() const { return sub_map_count_; }
  int64_t get_sub_map_mem_size() const { return sub_map_mem_size_; }
  void set_sub_map_mem_size(int64_t sub_map_mem_size) { sub_map_mem_size_ = sub_map_mem_size; }
  TO_STRING_KV(KP(this), K_(sub_map_count), K_(sub_map_mem_size), K(allocator_.get_label()));
private:
  ObPointerHashMap(const ObPointerHashMap &other, const int64_t resize_to)
      : sub_map_count_(0), sub_map_mem_size_(resize_to)
  {
    memset(sub_maps_, 0, sizeof(sub_maps_));
    copy_from(other, resize_to);
  }

  SubMap *create_sub_map(const SubMap *sub_map_in = NULL)
  {
    SubMap *sub_map = NULL;
    void *sub_map_mem = NULL;

    if (NULL == (sub_map_mem = allocator_.alloc(sub_map_mem_size_))) {
      COMMON_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "failed to allocate memory for sub map", K(*this));
    } else if (NULL != sub_map_in) {
      sub_map = new(sub_map_mem) SubMap(*sub_map_in);
    } else {
      sub_map = new(sub_map_mem) SubMap(sub_map_mem_size_);
    }
    return sub_map;
  }

  int find_set_pos(const K &key, const V &value, int64_t &sub_map_idx,
                   int64_t &pos, int overwrite)
  {
    int ret = OB_SUCCESS;
    int64_t set_pos = -1;
    int64_t erased_pos = -1;
    sub_map_idx = -1;
    pos = -1;

    for (int64_t i = 0; i < sub_map_count_; ++i) {
      bool exist = false;
      ret = sub_maps_[i]->find_set_pos(key, value, set_pos, erased_pos, exist, overwrite);
      if (OB_SUCC(ret)) {
        if (!exist) {
          if (-1 == sub_map_idx) {
            sub_map_idx = i;
            pos = set_pos;
          }
          break;
        } else {
          sub_map_idx = i;
          pos = set_pos;
          break;
        }
      } else if (OB_HASH_FULL == ret) {
        // first find an erased pos
        if (-1 == pos && -1 != erased_pos) {
          sub_map_idx = i;
          pos = erased_pos;
        }
        // the last sub map is full, but we find an erased pos in previous sub map
        if ((i == (sub_map_count_ - 1)) && -1 != pos) {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        //HASH_EXIST or HASH_NOT_EXIST
        break;
      }
    }
    return ret;
  }

  SubMap *resize()
  {
    SubMap *ret = NULL;
    int64_t resize_to = 0;
    bool add_sub_map = false;
    bool is_need_extend = (count() <= (int64_t)(1.1 * (double)item_count()));
    int64_t extend_size = (is_need_extend ? (sub_map_mem_size_ * 2) : sub_map_mem_size_);
    /**
     * resize sequence: 8K, 16K, 32K, 64K, 128K, 192K, 256K, 512K, 1M, 2M, 4M, 6M, 8M 16M, 32M
     * resize mode:
     *  1. 0 < map_mem_size <= 32K, resize_to = map_mem_size * 2
     *  2. 64K <= map_mem_size < 256K, resize_to = map_mem_size + 64K
     *  3. 256K <= map_mem_size < 2M, resize_to = map_mem_size * 2
     *  4. 2M <= map_mem_size < 8M, resize_to = map_mem_size + 2M
     *  5. map_mem_size >= 8M, resize_to = map_mem_size * 2
     */
    if (1 == sub_map_count_) {
      int64_t needed_size = extend_size;
      if ((OB_MALLOC_NORMAL_BLOCK_SIZE == sub_map_mem_size_
           || OB_MALLOC_BIG_BLOCK_SIZE == sub_map_mem_size_)
          && sub_map_count_ < MAX_SUB_MAP_COUNT) {
        // need add a new sub map
        add_sub_map = true;
      } else if (needed_size <= OB_MALLOC_NORMAL_BLOCK_SIZE) {
        resize_to = needed_size;
      } else if (sub_map_mem_size_ < OB_MALLOC_NORMAL_BLOCK_SIZE) {
        resize_to = OB_MALLOC_NORMAL_BLOCK_SIZE;
      } else if (needed_size <= OB_MALLOC_BIG_BLOCK_SIZE) {
        resize_to = needed_size;
      } else if (sub_map_mem_size_ < OB_MALLOC_BIG_BLOCK_SIZE) {
        resize_to = OB_MALLOC_BIG_BLOCK_SIZE;
        // large block size, more than 2M
      } else {
        resize_to = extend_size;
      }
    } else if (sub_map_count_ > 1 && sub_map_count_ < MAX_SUB_MAP_COUNT) {
      // need add a new sub map
      add_sub_map = true;
    } else if (MAX_SUB_MAP_COUNT == sub_map_count_) {
      if (OB_MALLOC_NORMAL_BLOCK_SIZE == sub_map_mem_size_) {
        resize_to = sub_map_count_ * extend_size;
        if (resize_to > OB_MALLOC_BIG_BLOCK_SIZE) {
          COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected error", K(*this), K(resize_to));
        }
      } else if (OB_MALLOC_BIG_BLOCK_SIZE == sub_map_mem_size_) {
        resize_to = sub_map_count_ * extend_size;
      }
    } else {
      // do nothing
    }

    COMMON_LOG(INFO, "extend hash map", K(*this), K(add_sub_map), K(resize_to),
        "count", count(), "item_count", item_count());

    if (add_sub_map) {
      if (sub_map_count_ < MAX_SUB_MAP_COUNT) {
        if (NULL != (ret = create_sub_map())) {
          sub_maps_[sub_map_count_++] = ret;
        }
      } else {
        // overfollow, return NULL
        COMMON_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "can't add more sub map", K(*this));
      }
    } else {
      ObPointerHashMap tmp(*this, resize_to);
      if (0 == tmp.get_sub_map_count()) {
        COMMON_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "copy hash map failed", K(*this), K(resize_to));
      } else {
        this->swap(tmp);
        ret = sub_maps_[0];
      }
      COMMON_LOG(INFO, "swap hash map", K(*this), K(add_sub_map), K(resize_to),
          "count", count(), "item_count", item_count());
    }

    return ret;
  }

  void swap(ObPointerHashMap &other)
  {
    for (int64_t i = 0; i < std::max(sub_map_count_, other.sub_map_count_); ++i) {
      std::swap(sub_maps_[i], other.sub_maps_[i]);
    }
    std::swap(sub_map_count_, other.sub_map_count_);
    std::swap(sub_map_mem_size_, other.sub_map_mem_size_);
    std::swap(allocator_, other.allocator_);
  }

  void copy_from(const ObPointerHashMap &other, const int64_t resize_to)
  {
    destroy();
    sub_map_mem_size_ = resize_to;
    allocator_ = other.allocator_;

    SubMap *sub_map = create_sub_map();
    if (NULL != sub_map) {
      sub_maps_[sub_map_count_++] = sub_map;
      for (int64_t i = 0; i < other.sub_map_count_; ++i) {
        // We use a const iterator to get non-deleted bcks from ht
        // We could use set() here, but since we know there are
        // no duplicates and no deleted items, we can be more efficient
        for (iterator it = const_cast<ObPointerHashMap &>(other).sub_maps_[i]->begin();
             it != const_cast<ObPointerHashMap &>(other).sub_maps_[i]->end(); ++it) {
          sub_map->set_refactored(other.sub_maps_[i]->get_key(it), *it);
        }
      }
    }
  }

private:
  static const int64_t MAX_SUB_MAP_COUNT = 4;
private:
  SubMap *sub_maps_[MAX_SUB_MAP_COUNT];
  int64_t sub_map_count_;
  int64_t sub_map_mem_size_;
  Allocator allocator_;
};
} // namespace hash
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_HASH_POINTER_HASHMAP_
