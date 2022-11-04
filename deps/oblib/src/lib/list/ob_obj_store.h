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

#ifndef OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_
#define OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_
#include "lib/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/allocator/page_arena.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace common
{
template <typename T>
class ObObjNode : public ObDLinkBase<ObObjNode<T> >
{
public:
  ObObjNode() : obj_() {}
  explicit ObObjNode(const T &obj) : obj_(obj) {}
  virtual ~ObObjNode() {}

  inline void set_obj(const T &obj) { obj_ = obj; }
  inline const T &get_obj() const { return obj_; }
  inline T &get_obj() { return obj_; }
private:
  T obj_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObObjNode);
};

template <typename T, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false>
class ObObjStore
{
public:
  class Iterator
  {
    friend class ObObjStore;
  public:
    Iterator()
      : cur_node_(nullptr),
        header_node_(nullptr)
    { }
    Iterator(ObObjNode<T> *cur_node, ObObjNode<T> *header_node)
      : cur_node_(cur_node),
        header_node_(header_node)
    { }
    ~Iterator() { }
    inline bool is_end() const { return cur_node_ == header_node_ || cur_node_ == nullptr; }
    inline Iterator& operator++() { cur_node_ = cur_node_->get_next(); return *this; }
    inline T &get_item() { return cur_node_->get_obj(); }
    OB_INLINE const T &get_item() const { return cur_node_->get_obj(); }
    OB_INLINE T &operator *() { return cur_node_->get_obj(); }
    OB_INLINE const T &operator *() const { return cur_node_->get_obj(); }
    OB_INLINE T *operator ->() { return &(cur_node_->get_obj()); }
    OB_INLINE const T *operator ->() const { return &(cur_node_->get_obj()); }
    inline void reset() { cur_node_ = nullptr; header_node_ = nullptr; }
  private:
    ObObjNode<T> *cur_node_;
    ObObjNode<T> *header_node_;
  };
public:
  explicit ObObjStore(const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_OBJ_STORE));
  ~ObObjStore() { destroy(); }

  inline int store_obj(const T &obj);
  inline const ObDList<ObObjNode<T> > &get_obj_list() const { return obj_list_; }
  inline ObDList<ObObjNode<T> > &get_obj_list() { return obj_list_; }
  //@NOTICE: the difference between destroy and clear is
  //that destroy will release the memory held by ObObjStore,
  //while clear does not release the memory, but only resets the obj list
  inline void destroy();
  inline void clear() { obj_list_.clear(); }
  inline void set_allocator(const BlockAllocatorT &alloc) { allocator_ = alloc; }
  inline Iterator begin() { return Iterator(obj_list_.get_first(), obj_list_.get_header()); }
  inline bool empty() const { return obj_list_.get_size() == 0; }
  inline int32_t get_size() const { return obj_list_.get_size(); }

  inline ObObjNode<T> *get_header_node() { return obj_list_.get_header(); }
  inline ObObjNode<T> *get_last_node() { return obj_list_.get_last(); }

private:
  ObDList<ObObjNode<T> > obj_list_;
  BlockAllocatorT allocator_;
};

template <typename T, typename BlockAllocatorT, bool auto_free>
ObObjStore<T, BlockAllocatorT, auto_free>::ObObjStore(const BlockAllocatorT &alloc)
  : allocator_(alloc)
{
  if (lib::this_worker().has_req_flag()) {
    if (auto_free) {
      set_allocator(BlockAllocatorT(
                        static_cast<ObIAllocator&>(
                            lib::this_worker().get_sql_arena_allocator())));
    }
  }
}

template <typename T, typename BlockAllocatorT, bool auto_free>
inline int ObObjStore<T, BlockAllocatorT, auto_free>::store_obj(const T &obj)
{
  int ret = OB_SUCCESS;
  ObObjNode<T> *obj_node = NULL;
  void *ptr = allocator_.alloc(sizeof(ObObjNode<T>));
  if (OB_UNLIKELY(NULL == ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "no more memory to create obj node");
  } else {
    obj_node = new(ptr) ObObjNode<T>(obj);
    if (!obj_list_.add_last(obj_node)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "add obj node to list failed");
    }
  }
  if (ret != OB_SUCCESS && obj_node != NULL) {
    allocator_.free(obj_node);
    obj_node = NULL;
    ptr = NULL;
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free>
inline void ObObjStore<T, BlockAllocatorT, auto_free>::destroy()
{
  DLIST_REMOVE_ALL_NORET(node, obj_list_) {
    if (node != NULL) {
      node->~ObObjNode<T>();
      allocator_.free(node);
      node = NULL;
    }
  }
}
}  // namespace common
}  // namespace oceanbase

#endif /* OCEANBASE_SRC_LIB_LIST_OB_OBJ_STORE_H_ */
