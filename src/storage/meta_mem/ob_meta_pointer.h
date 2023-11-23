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

#ifndef OCEANBASE_STORAGE_OB_META_POINTER_H_
#define OCEANBASE_STORAGE_OB_META_POINTER_H_

#include "lib/allocator/ob_allocator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"

namespace oceanbase
{
namespace storage
{

template <typename T>
class ObMetaPointer
{
public:
  ObMetaPointer();
  explicit ObMetaPointer(const ObMetaDiskAddr &addr);
  ObMetaPointer(const ObMetaDiskAddr &addr, ObMetaObjGuard<T> &guard);
  ObMetaPointer(const ObMetaPointer<T> &other);
  virtual ~ObMetaPointer();

  int get_in_memory_obj(ObMetaObjGuard<T> &guard);
  void get_obj(ObMetaObjGuard<T> &guard);

  void set_obj_pool(ObITenantMetaObjPool &obj_pool);
  void set_obj(const ObMetaObjGuard<T> &guard);
  void set_addr_without_reset_obj(const ObMetaDiskAddr &addr);
  void set_addr_with_reset_obj(const ObMetaDiskAddr &addr);
  OB_INLINE const ObMetaDiskAddr &get_addr() const { return phy_addr_; }

  virtual int set_attr_for_obj(T *t);

  virtual int deep_copy(char *buf, const int64_t buf_len, ObMetaPointer *&value) const;
  virtual int64_t get_deep_copy_size() const;
  bool is_in_memory() const;

  ObMetaPointer<T> &operator = (const ObMetaPointer<T> &other);
  void reset_obj();

  VIRTUAL_TO_STRING_KV(K_(phy_addr), K_(obj));

  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int64_t get_serialize_size() const;

  // load and dump interface
  virtual int acquire_obj(T *&t);
  int read_from_disk(const bool is_full_load,
      common::ObArenaAllocator &allocator, char *&r_buf, int64_t &r_len, ObMetaDiskAddr &addr);
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t buf_len, T *t);
  int deserialize(
      const char *buf,
      const int64_t buf_len, T *t);
  int hook_obj(T *&t, ObMetaObjGuard<T> &guard);
  virtual int release_obj(T *&t);
  virtual int dump_meta_obj(ObMetaObjGuard<T> &guard, void *&free_obj) { return OB_NOT_IMPLEMENT; }

protected:
  virtual void reset();

protected:
  ObMetaDiskAddr phy_addr_;
  ObMetaObj<T> obj_;
};

template <typename T>
ObMetaPointer<T>::ObMetaPointer()
  : phy_addr_(),
    obj_()
{
}

template <typename T>
ObMetaPointer<T>::ObMetaPointer(const ObMetaDiskAddr &addr)
  : phy_addr_(addr),
    obj_()
{
}

template <typename T>
ObMetaPointer<T>::ObMetaPointer(const ObMetaDiskAddr &addr, ObMetaObjGuard<T> &guard)
  : phy_addr_(addr),
    obj_()
{
  guard.get_obj(obj_);
  if (nullptr != obj_.ptr_) {
    if (nullptr == obj_.pool_ && nullptr == obj_.allocator_) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K_(obj));
      ob_abort();
    } else {
      obj_.ptr_->inc_ref();
    }
  }
}

template <typename T>
ObMetaPointer<T>::ObMetaPointer(const ObMetaPointer<T> &other)
  : phy_addr_(),
    obj_()
{
  *this = other;
}

template <typename T>
ObMetaPointer<T>::~ObMetaPointer()
{
  reset();
}

template <typename T>
int ObMetaPointer<T>::acquire_obj(T *&t)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_.pool_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "object pool is nullptr", K(ret), K(obj_));
  } else if (OB_FAIL(static_cast<ObTenantMetaObjPool<T> *>(obj_.pool_)->acquire(t))) {
    STORAGE_LOG(WARN, "fail to acquire object", K(ret), K(phy_addr_));
  } else if (OB_ISNULL(t)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "acquired object is nullptr", K(ret), KP(t));
  }
  return ret;
}

template <typename T>
int ObMetaPointer<T>::read_from_disk(const bool is_full_load,
    common::ObArenaAllocator &allocator, char *&r_buf, int64_t &r_len, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = phy_addr_.size();
  const ObMemAttr mem_attr(MTL_ID(), "MetaPointer");
  ObTenantCheckpointSlogHandler *ckpt_slog_hanlder = MTL(ObTenantCheckpointSlogHandler*);

  if (OB_ISNULL(ckpt_slog_hanlder)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "slog handler is nullptr", K(ret), KP(ckpt_slog_hanlder));
  } else {
    ObMetaDiskAddr real_load_addr = phy_addr_;
    if (!is_full_load && addr.is_raw_block()) {
      if (phy_addr_.size() > ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE) {
        real_load_addr.set_size(ObTabletCommon::MAX_TABLET_FIRST_LEVEL_META_SIZE);
      }
    }
    if (OB_FAIL(ckpt_slog_hanlder->read_from_disk(real_load_addr, allocator, r_buf, r_len))) {
      if (OB_SEARCH_NOT_FOUND != ret) {
        STORAGE_LOG(WARN, "fail to read from addr", K(ret), K(phy_addr_));
      }
    } else {
      addr = phy_addr_;
    }
  }
  return ret;
}

template <typename T>
int ObMetaPointer<T>::hook_obj(T *&t,  ObMetaObjGuard<T> &guard)
{
  int ret = OB_SUCCESS;
  guard.reset();

  if (OB_ISNULL(t)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "load null obj from disk", K(ret), K(phy_addr_));
  } else if (OB_NOT_NULL(obj_.ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "obj already hooked", K(ret), K(phy_addr_), KP(t), KP(obj_.ptr_));
  } else if (OB_UNLIKELY(0 != t->get_ref())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "obj ref cnt not 0", K(ret), K(phy_addr_), K(t->get_ref()));
  } else {
    t->inc_ref();
    t->set_tablet_addr(phy_addr_);
    obj_.ptr_ = t;
    guard.set_obj(obj_);
    ObMetaObjBufferHelper::set_in_map(reinterpret_cast<char *>(t), true/*in_map*/);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(t)) {
    obj_.pool_->free_obj(t);
    obj_.ptr_ = nullptr;
    t = nullptr;
  }

  return ret;
}

template <typename T>
int ObMetaPointer<T>::release_obj(T *&t)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(t)) {
    // do nothing
  } else if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "object pool or allocator is nullptr", K(ret), K(obj_));
  } else if (nullptr != obj_.pool_) {
    obj_.pool_->free_obj(t);
    t = nullptr;
  } else {
    t->~T();
    obj_.allocator_->free(t);
    t = nullptr;
  }
  return ret;
}

template <typename T>
int ObMetaPointer<T>::get_in_memory_obj(ObMetaObjGuard<T> &guard)
{
  int ret = OB_SUCCESS;
  guard.reset();

  if (OB_UNLIKELY(phy_addr_.is_none())) {
    ret = OB_ITEM_NOT_SETTED;
    STORAGE_LOG(DEBUG, "meta disk addr is none, no object to be got", K(ret), K(phy_addr_));
  } else if (OB_UNLIKELY(!is_in_memory())) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(ERROR, "object isn't in memory, not support", K(ret), K(phy_addr_));
  } else {
    guard.set_obj(obj_);
  }
  return ret;
}

template <typename T>
void ObMetaPointer<T>::get_obj(ObMetaObjGuard<T> &guard)
{
  guard.set_obj(obj_);
}

template <typename T>
int ObMetaPointer<T>::deep_copy(char *buf, const int64_t buf_len, ObMetaPointer *&value) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || buf_len < deep_copy_size) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObMetaPointer *pvalue = new (buf) ObMetaPointer(phy_addr_);
    pvalue->obj_.pool_ = obj_.pool_;
    pvalue->obj_.allocator_ = obj_.allocator_;
    pvalue->obj_.ptr_ = obj_.ptr_;
    if (nullptr != obj_.ptr_) {
      if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "object pool is nullptr", K(ret), K_(obj));
        ob_abort();
      } else {
        obj_.ptr_->inc_ref();
      }
    }

    if (OB_SUCC(ret)) {
      value = pvalue;
    } else {
      pvalue->~ObMetaPointer();
    }
  }
  return ret;
}

template <typename T>
int64_t ObMetaPointer<T>::get_deep_copy_size() const
{
  return sizeof(*this);
}

template <typename T>
bool ObMetaPointer<T>::is_in_memory() const
{
  return nullptr != obj_.ptr_;
}

template <typename T>
void ObMetaPointer<T>::set_obj_pool(ObITenantMetaObjPool &obj_pool)
{
  obj_.pool_ = &obj_pool;
}

template <typename T>
void ObMetaPointer<T>::set_addr_without_reset_obj(const ObMetaDiskAddr &addr)
{
  phy_addr_ = addr;
}

template <typename T>
void ObMetaPointer<T>::set_addr_with_reset_obj(const ObMetaDiskAddr &addr)
{
  reset_obj();
  phy_addr_ = addr;
}

template <typename T>
void ObMetaPointer<T>::set_obj(const ObMetaObjGuard<T> &guard)
{
  reset_obj();
  guard.get_obj(obj_);
  set_attr_for_obj(obj_.ptr_);
  if (nullptr != obj_.ptr_) {
    if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K_(obj));
      ob_abort();
    } else {
      obj_.ptr_->inc_ref();
    }
  }
}

template <typename T>
int ObMetaPointer<T>::set_attr_for_obj(T *t)
{
  // do nothing.
  UNUSED(t);
  return common::OB_SUCCESS;
}

template<typename T>
int ObMetaPointer<T>::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || pos >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(phy_addr_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to serialize physical address", K(ret), K(phy_addr_));
  }
  return ret;
}

template <typename T>
int ObMetaPointer<T>::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len <= 0 || pos < 0 || pos >= buf_len) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(phy_addr_.deserialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to de-serialize physical address", K(ret), K(phy_addr_));
  }
  return ret;
}

template <typename T>
int64_t ObMetaPointer<T>::get_serialize_size() const
{
  return phy_addr_.get_serialize_size();
}

template <typename T>
int ObMetaPointer<T>::deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t buf_len,
    T *t)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf) || OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), KP(t));
  } else if (OB_FAIL(set_attr_for_obj(t))) {
    STORAGE_LOG(WARN, "fail to set attr for obj", K(ret));
  } else if (OB_FAIL(t->load_deserialize(allocator, buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to de-serialize T", K(ret), KP(buf), K(buf_len), KP(t));
  }
  return ret;
}

template <typename T>
int ObMetaPointer<T>::deserialize(
    const char *buf,
    const int64_t buf_len,
    T *t)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(buf) || OB_ISNULL(t)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), KP(t));
  } else if (OB_FAIL(set_attr_for_obj(t))) {
    STORAGE_LOG(WARN, "fail to set attr for obj", K(ret));
  } else if (OB_FAIL(t->deserialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "fail to de-serialize T", K(ret), KP(buf), K(buf_len), KP(t));
  }
  return ret;
}

template <typename T>
void ObMetaPointer<T>::reset_obj()
{
  if (nullptr != obj_.ptr_) {
    if (OB_UNLIKELY(nullptr == obj_.pool_ && nullptr == obj_.allocator_)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K_(obj));
      ob_abort();
    } else {
      const int64_t ref_cnt = obj_.ptr_->dec_ref();
      if (0 == ref_cnt) {
        if (nullptr != obj_.pool_) {
          obj_.pool_->free_obj(obj_.ptr_);
        } else {
          obj_.ptr_->~T();
          obj_.allocator_->free(obj_.ptr_);
        }
      } else if (OB_UNLIKELY(ref_cnt < 0)) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj ref cnt may be leaked", K(ref_cnt), KPC(this));
      }
      // The pool ptr on tablet pointer cann't be reset nullptr here. Otherwise, you will
      // encounter the following bug when the tablet is deleted from the map.
      //
      // Bug timeline:
      //  - Thread 1 load tablet from meta.
      //  - Thread 2 remove tablet from map.
      //  - Thread 1 fail to hook loaded tablet into pointer.
      //  - Thread 1 rolls back and releases the tablet and encounters an error.
      obj_.ptr_ = nullptr;
      obj_.allocator_ = nullptr;
    }
  }
}

template <typename T>
void ObMetaPointer<T>::reset()
{
  reset_obj();
  phy_addr_.reset();
}

template <typename T>
ObMetaPointer<T> &ObMetaPointer<T>::operator = (const ObMetaPointer<T> &other)
{
  if (this != &other) {
    reset();
    phy_addr_  = other.phy_addr_;
    obj_.pool_ = other.obj_.pool_;
    if (nullptr != other.obj_.ptr_) {
      if (nullptr == other.obj_.pool_) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K(other));
        ob_abort();
      } else {
        obj_.ptr_ = other.obj_.ptr_;
        other.obj_.ptr_->inc_ref();
      }
    }
  }

  return *this;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_META_POINTER_H_ */
