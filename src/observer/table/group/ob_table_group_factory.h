/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_GROUP_FACTORY_H_
#define OCEANBASE_OBSERVER_OB_TABLE_GROUP_FACTORY_H_
#include "ob_i_table_struct.h"
#include "ob_table_group_register.h"
namespace oceanbase
{

namespace table
{
// @note thread-safe
template <typename T>
class ObTableGroupFactory final
{
public:
  ObTableGroupFactory(common::ObIAllocator &alloc)
      : alloc_(alloc)
  {}
  virtual ~ObTableGroupFactory() { free_all(); }
  TO_STRING_KV(K(used_list_.get_size()),
               K(free_list_.get_size()));
public:
  T *alloc();
  void free(T *obj);
  void free_and_reuse();
  int64_t get_free_count() const { return free_list_.get_size(); }
  int64_t get_used_count() const { return used_list_.get_size(); }
  int64_t get_used_mem() const { return alloc_.used(); }
  int64_t get_total_mem() const { return alloc_.total(); }
  void free_all();
private:
  common::ObIAllocator &alloc_;
  common::ObSpinLock lock_;
  common::ObDList<T> used_list_;
  common::ObDList<T> free_list_;
};

template <typename T>
T *ObTableGroupFactory<T>::alloc()
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);

  T *obj = free_list_.remove_first();
  if (NULL == obj) {
    void *ptr = alloc_.alloc(sizeof(T));
    if (NULL == ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to alloc memory", K(ret), K(sizeof(T)));
    } else {
      obj = new(ptr) T();
      used_list_.add_last(obj);
    }
  } else {
    used_list_.add_last(obj);
  }

  return obj;
}

template <typename T>
void ObTableGroupFactory<T>::free(T *obj)
{
  if (NULL != obj) {
    ObLockGuard<ObSpinLock> guard(lock_);
    obj->reuse();
    used_list_.remove(obj);
    free_list_.add_last(obj);
  }
}

template <typename T>
void ObTableGroupFactory<T>::free_and_reuse()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  while (!used_list_.is_empty()) {
    this->free(used_list_.get_first());
  }
}

template <typename T>
void ObTableGroupFactory<T>::free_all()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  T *obj = NULL;
  while (NULL != (obj = used_list_.remove_first())) {
    obj->~T();
    alloc_.free(obj);
  }
  while (NULL != (obj = free_list_.remove_first())) {
    obj->~T();
    alloc_.free(obj);
  }
}

class ObTableGroupOpFactory final
{
public:
  ObTableGroupOpFactory(common::ObIAllocator &allocator)
      : allocator_(allocator)
  {}
  virtual ~ObTableGroupOpFactory() { free_all(); }
  TO_STRING_KV(K(used_list_),
               K(free_list_));
public:
  int alloc(ObTableGroupType op_type, ObITableOp *&op);
  void free(ObITableOp *obj);
  void free_and_reuse(ObTableGroupType type);
  int64_t get_free_count(ObTableGroupType type) const { return free_list_[type].get_size(); }
  int64_t get_used_count(ObTableGroupType type) const { return used_list_[type].get_size(); }
  int64_t get_used_mem() const { return allocator_.used(); }
  int64_t get_total_mem() const { return allocator_.total(); }
  void free_all();
private:
  common::ObIAllocator &allocator_;
  common::ObSpinLock locks_[ObTableGroupType::TYPE_MAX];
  common::ObDList<ObITableOp> used_list_[ObTableGroupType::TYPE_MAX];
  common::ObDList<ObITableOp> free_list_[ObTableGroupType::TYPE_MAX];
};
} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_FACTORY_H_ */
