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

#ifndef LIB_OBJECTPOOL_OB_TC_FACTORY_
#define LIB_OBJECTPOOL_OB_TC_FACTORY_
#include "lib/list/ob_dlist.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/objectpool/ob_global_factory.h"
namespace oceanbase
{
namespace common
{
// T should be a derived class of common::DLink
template<typename T>
class ObTCFreeList
{
public:
  ObTCFreeList()
    : magic_(0xFBEE1127),
      max_length_(1),
      low_water_(0),
      overlimit_times_(0),
      list_()
  {
  }
  virtual ~ObTCFreeList() {}

  void push(T *ptr)
  {
    if (OB_UNLIKELY(false == list_.add_last(ptr))) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "fail to add ptr to list_'s tail");
    }
  }
  T *pop()
  {
    T *ptr_ret = static_cast<T *>(list_.remove_last());
    if (list_.get_size() < low_water_) {
      low_water_ = list_.get_size();
    }
    return ptr_ret;
  }

  void push_range(common::ObDList<T> &range) { list_.push_range(range); }
  void pop_range(int32_t num, common::ObDList<T> &range)
  {
    list_.pop_range(num, range);
    if (list_.get_size() < low_water_) {
      low_water_ = list_.get_size();
    } else {}
  }

  bool is_empty() const { return list_.is_empty(); }
  int32_t get_length() const { return list_.get_size(); }

  int32_t get_max_length() const { return max_length_; }
  void inc_max_length() { ++max_length_; }
  void set_max_length(int32_t max_length) { max_length_ = max_length; }

  int32_t get_low_water() const { return low_water_; }
  void reset_low_water() { low_water_ = list_.get_size(); }

  int32_t get_overlimit_times() const { return overlimit_times_; }
  void set_overlimit_times(int32_t times) { overlimit_times_ = times; }
  void inc_overlimit_times() { ++overlimit_times_; }
private:
  // data members
  int32_t magic_;
  int32_t max_length_;
  int32_t low_water_;
  int32_t overlimit_times_;
  common::ObDList<T> list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTCFreeList);
};

template <typename T>
void ob_tc_factory_callback_on_put(T *obj, BoolType<true>)
{
  if (OB_ISNULL(obj)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj is NULL");
  } else {
    obj->reset();
  }
}

template <typename T>
void ob_tc_factory_callback_on_put(T *obj, BoolType<false>)
{
  UNUSED(obj);
}

// T should be a derived class of common::DLink
template < typename T,
           int64_t MAX_CLASS_NUM,
           const char *LABEL,
           int64_t MEM_LIMIT = 1 << 24/*16MB*/,
           int32_t MAX_FREE_LIST_LENGTH = 1024 >
class ObTCFactory
{
public:
  static ObTCFactory *get_instance();

  T *get(int32_t type_id, const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void put(T *ptr, const uint64_t tenant_id = OB_SERVER_TENANT_ID);

  void stat();
  void set_mem_limit(const int64_t mem_limit) { mem_limit_ = mem_limit; }
public:
  // function members
  ObTCFactory();
  virtual ~ObTCFactory() {}
private:
  // types and constants
  typedef ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH> self_t;
  typedef ObGlobalFactory<T, MAX_CLASS_NUM, LABEL> global_factory_t;
  typedef ObTCFreeList<T> freelist_t;
  static const int32_t MAX_OVERLIMIT_TIMES = 3;
private:
  T *get_from_global_factory(int32_t type_id, int32_t obj_size);
  void list_too_long(freelist_t &list, int32_t type_id, int32_t obj_size);
  void garbage_collect();
private:
  static volatile self_t *TC_FACTORY_LIST_HEAD;
private:
  //data members
  freelist_t freelists_[MAX_CLASS_NUM];
  int64_t mem_size_;
  int64_t get_count_[MAX_CLASS_NUM];
  int64_t put_count_[MAX_CLASS_NUM];
  volatile self_t *next_;
  int64_t mem_limit_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTCFactory);
};

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
         int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
volatile ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>
*ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::TC_FACTORY_LIST_HEAD = NULL;

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::ObTCFactory()
  : freelists_(),
    mem_size_(0),
    get_count_(),
    put_count_(),
    next_(NULL),
    mem_limit_(MEM_LIMIT)
{
  memset(get_count_, 0, sizeof(get_count_));
  memset(put_count_, 0, sizeof(put_count_));
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
         int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>
*ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get_instance()
{
  auto *instance = GET_TSI(self_t);
  if (OB_ISNULL(instance)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "failed to init tc_factory");
  } else {
    // link all the tc_factory of all threads
    volatile self_t *old_v = NULL;
    while (true) {
      old_v = TC_FACTORY_LIST_HEAD;
      instance->next_ = old_v;
      if (__sync_bool_compare_and_swap(&TC_FACTORY_LIST_HEAD, old_v, instance)) {
        break;
      } else {}
    }
  }
  return instance;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
T *ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get(
    int32_t type_id, const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  T *ptr_ret = NULL;
  if (OB_UNLIKELY(0 > type_id) || OB_UNLIKELY(type_id >= MAX_CLASS_NUM)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid class type id", K(type_id));
  } else {
    global_factory_t *gfactory = global_factory_t::get_instance();
    if (OB_ISNULL(gfactory)) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "gfactory is NULL");
    } else {
      const int32_t obj_size = gfactory->get_obj_size(type_id);
      //OB_PHY_OP_INC(type_id);
      if (freelists_[type_id].is_empty()) {
        // no item in the local free list
        ptr_ret = get_from_global_factory(type_id, obj_size);
      } else {
        ptr_ret = freelists_[type_id].pop();
        mem_size_ -= obj_size;
      }
      if (OB_LIKELY(NULL != ptr_ret)) {
        ++get_count_[type_id];
      } else {}
    }
  }
  return ptr_ret;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
         int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
         T *ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::get_from_global_factory(
             int32_t type_id, int32_t obj_size)
{
  T *ptr_ret = NULL;
  freelist_t &list = freelists_[type_id];
  if (OB_UNLIKELY(false == list.is_empty())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "list is not empty");
  } else {
    global_factory_t *gfactory = global_factory_t::get_instance();
    if (OB_ISNULL(gfactory)) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "gfactory is NULL");
    } else {
      const int32_t batch_count = gfactory->get_batch_count(type_id);
      const int32_t move_num = std::min(batch_count, list.get_max_length());
      common::ObDList<T> range;
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(batch_count <= 0)) {
        LIB_LOG(ERROR, "batch_count must not be 0!", K(gfactory));
        ptr_ret = NULL;
      } else if (OB_FAIL(gfactory->get_objs(type_id, move_num, range))) {
        LIB_LOG(WARN, "global factory get_objs failed", K(ret), K(type_id), K(move_num));
        ptr_ret = NULL;
      } else {
        ptr_ret = static_cast<T *>(range.remove_last());
        if (range.get_size() > 0) {
          mem_size_ += range.get_size() * obj_size;
          list.push_range(range);
        } else {}
        // Increase max length slowly up to batch_count.
        if (list.get_max_length() < batch_count) {
          list.inc_max_length();
        } else {
          int32_t new_length = std::min(list.get_max_length() + batch_count, MAX_FREE_LIST_LENGTH);
          new_length -= new_length % batch_count;
          if (OB_UNLIKELY(0 != (new_length % batch_count))) {
            LIB_LOG(ERROR, "new_length mod batch_count != 0", K(new_length), K(batch_count));
          } else {
            list.set_max_length(new_length);
          }
        }
      }
    }
  }
  return ptr_ret;
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
void ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::put(
    T *ptr, const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  if (OB_LIKELY(NULL != ptr)) {
    global_factory_t *gfactory = global_factory_t::get_instance();
    if (OB_ISNULL(gfactory)) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "gfactory is NULL");
    } else {
      const int32_t type_id = static_cast<const int32_t>(ptr->get_type());
      const int32_t obj_size = gfactory->get_obj_size(type_id);
      //OB_PHY_OP_DEC(type_id);
      freelist_t &list = freelists_[type_id];
      ob_tc_factory_callback_on_put(ptr, BoolType<HAS_MEMBER(T, reset)>());
      list.push(ptr);
      mem_size_ += obj_size;
      if (list.get_length() > list.get_max_length()) {
        //_OB_LOG(ERROR, "list too long, length=%d max_length=%d", list.get_length(), list.get_max_length());
        list_too_long(list, type_id, obj_size);
      } else {}
      if (mem_size_ > mem_limit_) {
        //_OB_LOG(ERROR, "cache too large, mem_size_=%ld", mem_size_);
        garbage_collect();
      } else {}
      ++put_count_[type_id];
    }
  }
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
         int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
void ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::list_too_long(
    freelist_t &list, int32_t type_id, int32_t obj_size)
{
  global_factory_t *gfactory = global_factory_t::get_instance();
  if (OB_ISNULL(gfactory)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "gfactory is NULL");
  } else {
    const int32_t batch_count = gfactory->get_batch_count(type_id);
    // release batch_count objs to the global factory
    common::ObDList<T> range;
    list.pop_range(batch_count, range);
    mem_size_ -= obj_size * range.get_size();
    gfactory->put_objs(type_id, range);

    if (list.get_max_length() < batch_count) {
      list.inc_max_length();
    } else if (list.get_max_length() > batch_count) {
      // shrink the list if we consistently go over max_length
      list.inc_overlimit_times();
      if (list.get_overlimit_times() >= MAX_OVERLIMIT_TIMES) {
        list.set_max_length(list.get_max_length() - batch_count);
        list.set_overlimit_times(0);
      } else {}
    }
  }
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
void ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::garbage_collect()
{
  int ret = OB_SUCCESS;
  global_factory_t *gfactory = global_factory_t::get_instance();
  if (OB_ISNULL(gfactory)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "gfactory is NULL", K(ret));
  } else {
    common::ObDList<T> range;
    for (int32_t type_id = 0; OB_SUCC(ret) && type_id < MAX_CLASS_NUM; ++type_id) {
      const int32_t obj_size = gfactory->get_obj_size(type_id);
      if (0 >= obj_size) {
        // invalid class type, skip
        continue;
      } else {}
      freelist_t &list = freelists_[type_id];
      const int32_t low_water = list.get_low_water();
      if (0 < low_water) {
        const int32_t drop_num = (low_water > 1) ? (low_water / 2) : 1;

        // release drop_num objs to the global factory
        const int32_t batch_count = gfactory->get_batch_count(type_id);
        range.reset();
        list.pop_range(drop_num, range);
        mem_size_ -= obj_size * range.get_size();
        gfactory->put_objs(type_id, range);

        // shrink the list
        if (list.get_max_length() > batch_count) {
          list.set_max_length(list.get_max_length() - batch_count);
        } else {}
      } else {}
      list.reset_low_water();
    } // end for
  }
}

template <typename T, int64_t MAX_CLASS_NUM, const char *LABEL,
int64_t MEM_LIMIT, int32_t MAX_FREE_LIST_LENGTH>
void ObTCFactory<T, MAX_CLASS_NUM, LABEL, MEM_LIMIT, MAX_FREE_LIST_LENGTH>::stat()
{
  int ret = OB_SUCCESS;
  self_t *tc_factory = const_cast<self_t *>(TC_FACTORY_LIST_HEAD);
  int32_t i = 0;
  int64_t total_mem_size = 0;
  int64_t total_length = 0;
  int64_t total_max_length = 0;
  int32_t length = 0;
  int32_t max_length = 0;
  int32_t low_water = 0;
  int32_t overlimit_times = 0;
  int64_t class_total_length[MAX_CLASS_NUM];
  int64_t class_total_max_length[MAX_CLASS_NUM];
  int64_t allocated_count[MAX_CLASS_NUM];
  memset(class_total_length, 0, sizeof(class_total_length));
  memset(class_total_max_length, 0, sizeof(class_total_max_length));
  memset(allocated_count, 0, sizeof(allocated_count));
  while (NULL != tc_factory && OB_SUCC(ret)) {
    LIB_LOG(INFO, "[TCFACTORY_STAT] type_id=ALL", "thread", i, "mem_size", tc_factory->mem_size_);
    total_mem_size += tc_factory->mem_size_;
    for (int32_t type_id = 0; OB_SUCC(ret) && type_id < MAX_CLASS_NUM; ++type_id) {
      length = tc_factory->freelists_[type_id].get_length();
      max_length = tc_factory->freelists_[type_id].get_max_length();
      low_water = tc_factory->freelists_[type_id].get_low_water();
      overlimit_times = tc_factory->freelists_[type_id].get_overlimit_times();

      total_length += length;
      total_max_length += max_length;

      class_total_length[type_id] += length;
      class_total_max_length[type_id] += max_length;

      allocated_count[type_id] += tc_factory->get_count_[type_id] - tc_factory->put_count_[type_id];
      LIB_LOG(INFO, "[TCFACTORY_STAT]", "thread", i,
              K(type_id), K(length), K(max_length), K(low_water), K(overlimit_times),
              "get_count", tc_factory->get_count_[type_id],
              "put_count", tc_factory->put_count_[type_id]);
    }
    tc_factory = const_cast<self_t *>(tc_factory->next_);
    ++i;
  } // end while
  for (int32_t type_id = 0; OB_SUCC(ret) && type_id < MAX_CLASS_NUM; ++type_id) {
    LIB_LOG(INFO, "[TCFACTORY_STAT] thread=ALL",
            K(type_id), "length", class_total_length[type_id],
            "max_length", class_total_max_length[type_id],
            "allocated", allocated_count[type_id]);
  }
  LIB_LOG(INFO, "[TCFACTORY_STAT] thread=ALL type_id=ALL",
          "mem_size", total_mem_size, "length", total_length, "max_length", total_max_length);
}
} // end namespace common
} // end namespace oceanbase
#endif /* LIB_OBJECTPOOL_OB_TC_FACTORY_ */
