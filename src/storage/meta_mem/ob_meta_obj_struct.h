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

#ifndef OCEANBASE_STORAGE_OB_META_OBJ_STRUCT_H_
#define OCEANBASE_STORAGE_OB_META_OBJ_STRUCT_H_

#include "common/log/ob_log_constants.h"
#include "share/ob_define.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace storage
{

class ObTenantMetaMemMgr;

class ObMetaDiskAddr final
{
public:
  enum DiskType : uint8_t {
    NONE = 0,
    FILE = 1,
    BLOCK = 2,
    MEM = 3,
    MAX = 4,
  };
public:
  static const int64_t ROOT_BLOCK_SIZE_LIMIT = 16 << 10; // 16KB
public:
  ObMetaDiskAddr();
  ~ObMetaDiskAddr() = default;
  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool operator ==(const ObMetaDiskAddr &other) const;
  bool operator !=(const ObMetaDiskAddr &other) const;

  OB_INLINE bool is_block() const { return BLOCK == type_; }
  OB_INLINE bool is_disked() const { return BLOCK == type_ || FILE == type_; }
  OB_INLINE bool is_file() const { return FILE == type_; }
  OB_INLINE bool is_memory() const { return MEM == type_; }
  OB_INLINE bool is_none() const { return NONE == type_; }
  OB_INLINE void set_none_addr() { type_ = NONE; }
  OB_INLINE int64_t file_id() const { return file_id_; }
  OB_INLINE uint64_t size() const { return size_; }
  OB_INLINE uint64_t offset() const { return offset_; }
  OB_INLINE DiskType type() const { return static_cast<DiskType>(type_); }

  int get_block_addr(
      blocksstable::MacroBlockId &macro_id,
      int64_t &offset,
      int64_t &size) const;
  int set_block_addr(
      const blocksstable::MacroBlockId &macro_id,
      const int64_t offset,
      const int64_t size);
  int get_file_addr(
      int64_t &file_id,
      int64_t &offset,
      int64_t &size) const;
  int set_file_addr(
      const int64_t file_id,
      const int64_t offset,
      const int64_t size);
  int get_mem_addr(
      int64_t &offset,
      int64_t &size) const;
  int set_mem_addr(
      const int64_t offset,
      const int64_t size);

  OB_UNIS_VERSION(1);
private:
  static const uint64_t MD_FID_BIT_OFFSET = 30;
  static const uint64_t MD_FID_BIT_SIZE = 30;
  static const uint64_t MD_FID_BIT_TYPE = 4;
  static const uint64_t MAX_OFFSET = (0x1UL << MD_FID_BIT_OFFSET) - 1;
  static const uint64_t MAX_SIZE = (0x1UL << MD_FID_BIT_SIZE) - 1;
  static const uint64_t MAX_TYPE = (0x1UL << MD_FID_BIT_TYPE) - 1;
private:
  union {
    int64_t first_id_;
  };
  union {
    int64_t second_id_;
    int64_t file_id_;
  };
  union {
    int64_t third_id_;
  };
  union {
    int64_t fourth_id_;
    struct {
      uint64_t offset_ : MD_FID_BIT_OFFSET;
      uint64_t size_   : MD_FID_BIT_SIZE;
      uint64_t type_   : MD_FID_BIT_TYPE;
    };
  };
};

template <typename T>
class ObMetaObj
{
public:
  ObMetaObj();
  virtual ~ObMetaObj() = default;

  TO_STRING_KV(KP_(pool), KP_(ptr));

public:
  ObTenantMetaObjPool<T> *pool_;
  T *ptr_;
};

template <typename T>
class ObMetaObjGuard
{
public:
  ObMetaObjGuard();
  ObMetaObjGuard(const ObMetaObjGuard<T> &other);
  virtual ~ObMetaObjGuard();

  virtual void reset();

  virtual void set_obj(ObMetaObj<T> &obj);
  virtual void set_obj(T *obj, common::ObIAllocator *allocator);

  OB_INLINE virtual T *get_obj();
  OB_INLINE virtual T *get_obj() const;
  OB_INLINE virtual void get_obj(ObMetaObj<T> &obj) const;

  virtual bool is_valid() const;
  virtual bool need_hold_time_check() const;

  ObMetaObjGuard<T> &operator = (const ObMetaObjGuard<T> &other);

  VIRTUAL_TO_STRING_KV(KP_(obj), KP_(obj_pool), KP_(allocator));

protected:
  static const int64_t HOLD_OBJ_MAX_TIME = 2 * 60 * 60 * 1000 * 1000L; // 2h
  virtual void reset_obj();

protected:
  T *obj_;
  ObTenantMetaObjPool<T> *obj_pool_;
  common::ObIAllocator *allocator_;
  int64_t hold_start_time_;
};

template <typename T>
ObMetaObj<T>::ObMetaObj()
  : pool_(nullptr),
    ptr_(nullptr)
{
}

template <typename T>
ObMetaObjGuard<T>::ObMetaObjGuard()
  : obj_(nullptr),
    obj_pool_(nullptr),
    allocator_(nullptr),
    hold_start_time_(INT64_MAX)
{
}

template <typename T>
ObMetaObjGuard<T>::ObMetaObjGuard(const ObMetaObjGuard<T> &other)
  : obj_(nullptr),
    obj_pool_(nullptr),
    allocator_(nullptr),
    hold_start_time_(INT64_MAX)
{
  *this = other;
}

template <typename T>
ObMetaObjGuard<T>::~ObMetaObjGuard()
{
  reset();
}

template <typename T>
void ObMetaObjGuard<T>::set_obj(ObMetaObj<T> &obj)
{
  reset();
  obj_pool_ = obj.pool_;
  if (nullptr != obj.ptr_) {
    if (nullptr == obj.pool_) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K(obj));
      ob_abort();
    } else {
      obj_ = obj.ptr_;
      obj_->inc_ref();
      hold_start_time_ = ObTimeUtility::current_time();
    }
  }
}

template <typename T>
void ObMetaObjGuard<T>::set_obj(T *obj, common::ObIAllocator *allocator)
{
  reset();
  allocator_ = allocator;
  if (nullptr != obj) {
   if (nullptr == allocator) {
     STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "allocator is nullptr", KP(obj), KP(allocator));
     ob_abort();
   } else {
     obj_ = obj;
     obj_->inc_ref();
     hold_start_time_ = ObTimeUtility::current_time();
   }
  }
}

template <typename T>
void ObMetaObjGuard<T>::reset()
{
  reset_obj();
  obj_pool_ = nullptr;
  allocator_ = nullptr;
}

template <typename T>
OB_INLINE bool ObMetaObjGuard<T>::is_valid() const
{
  return nullptr != obj_
      && ((nullptr != obj_pool_ && nullptr == allocator_) || (nullptr == obj_pool_ && nullptr != allocator_));
}

template <typename T>
OB_INLINE bool ObMetaObjGuard<T>::need_hold_time_check() const
{
  return false;
}

template <typename T>
ObMetaObjGuard<T> &ObMetaObjGuard<T>::operator = (const ObMetaObjGuard<T> &other)
{
  if (this != &other) {
    reset();
    obj_pool_ = other.obj_pool_;
    allocator_ = other.allocator_;
    if (nullptr != other.obj_) {
      if (OB_UNLIKELY(!other.is_valid())) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool and allocator is nullptr", K(other), KPC(this));
        ob_abort();
      } else {
        obj_ = other.obj_;
        hold_start_time_ = ObTimeUtility::current_time();
        other.obj_->inc_ref();
        if (OB_UNLIKELY(other.obj_->get_ref() < 2)) {
          STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj guard may be accessed by multiple threads or ref cnt leak", KP(obj_), KP(obj_pool_));
        }
      }
    }
  }
  return *this;
}

template <typename T>
OB_INLINE T *ObMetaObjGuard<T>::get_obj()
{
  return obj_;
}

template <typename T>
OB_INLINE T *ObMetaObjGuard<T>::get_obj() const
{
  return obj_;
}

template <typename T>
OB_INLINE void ObMetaObjGuard<T>::get_obj(ObMetaObj<T> &obj) const
{
  obj.pool_ = obj_pool_;
  obj.ptr_ = obj_;
}

template <typename T>
void ObMetaObjGuard<T>::reset_obj()
{
  if (nullptr != obj_) {
    if (OB_UNLIKELY(!is_valid())) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool and allocator is nullptr", K_(obj), K_(obj_pool), K_(allocator));
      ob_abort();
    } else {
      const int64_t ref_cnt = obj_->dec_ref();
      const int64_t hold_time = ObTimeUtility::current_time() - hold_start_time_;
      if (OB_UNLIKELY(hold_time > HOLD_OBJ_MAX_TIME && need_hold_time_check())) {
        STORAGE_LOG_RET(ERROR, OB_ERR_TOO_MUCH_TIME, "The meta obj reference count was held for more "
            "than two hours ", K(ref_cnt), KP(this), K(hold_time), K(hold_start_time_), KPC(this));
      }
      if (0 == ref_cnt) {
        if (nullptr != obj_pool_) {
          obj_pool_->release(obj_);
        } else {
          STORAGE_LOG(DEBUG, "release obj from allocator", KP(obj_), KP(allocator_));
          obj_->reset();
          obj_->~T();
          allocator_->free(obj_);
        }
      } else if (OB_UNLIKELY(ref_cnt < 0)) {
        STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "obj ref cnt may be leaked", K(ref_cnt), KPC(this));
      }
      obj_ = nullptr;
    }
  }
  hold_start_time_ = INT64_MAX;
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_META_OBJ_STRUCT_H_ */
