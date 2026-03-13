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
#include "common/ob_clock_generator.h"
#include "share/ob_define.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"
#include "storage/blocksstable/ob_macro_block_handle.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace storage
{

class ObMetaDiskAddr final
{
public:
  enum DiskType : uint8_t {
    NONE = 0,
    FILE = 1,
    BLOCK = 2,
    MEM = 3,
    RAW_BLOCK = 4, // refer the shared block of storage meta which has no header
    SSLOG_TABLET_META = 5, // FARM COMPAT WHITELIST
    MAX = 6,
  };
public:
  ObMetaDiskAddr();
  ~ObMetaDiskAddr() = default;
  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool operator ==(const ObMetaDiskAddr &other) const;
  bool operator !=(const ObMetaDiskAddr &other) const;
  bool operator<(const ObMetaDiskAddr &r) const;
  bool is_equal_for_persistence(const ObMetaDiskAddr &other) const;
  OB_INLINE bool is_block() const { return BLOCK == type_ || RAW_BLOCK == type_; }
  OB_INLINE bool is_raw_block() const { return RAW_BLOCK == type_; }
  OB_INLINE bool is_disked() const { return BLOCK == type_ || FILE == type_ || RAW_BLOCK == type_ || is_sslog(); }
  OB_INLINE bool is_file() const { return FILE == type_; }
  OB_INLINE bool is_memory() const { return MEM == type_; }
  OB_INLINE bool is_none() const { return NONE == type_; }
  OB_INLINE bool is_sslog() const {  return SSLOG_TABLET_META == type_; }
  OB_INLINE void set_none_addr() { type_ = NONE; }
  OB_INLINE void set_seq(const uint64_t seq) { seq_ = seq; }
  OB_INLINE void set_size(const uint64_t size) { size_ = size; }
  OB_INLINE void set_type(const DiskType type) { type_ = type; }
  OB_INLINE int64_t file_id() const { return file_id_; }
  OB_INLINE uint64_t size() const { return size_; }
  OB_INLINE uint64_t offset() const { return offset_; }
  OB_INLINE uint64_t seq() const { return seq_; }
  OB_INLINE DiskType type() const { return static_cast<DiskType>(type_); }
  OB_INLINE void inc_seq() { seq_++; }
  int get_macro_block_id(blocksstable::MacroBlockId &block_id) const;
  int get_block_addr(
      blocksstable::MacroBlockId &macro_id,
      int64_t &offset,
      int64_t &size) const;
  int set_block_addr(
      const blocksstable::MacroBlockId &macro_id,
      const int64_t offset,
      const int64_t size,
      const DiskType block_type);
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
  int get_size_for_tablet_space_usage(
      /*out*/ int64_t &size) const;
#ifdef OB_BUILD_SHARED_STORAGE
  int set_sslog_tablet_meta_addr(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorg_scn,
      const share::SCN &row_scn,
      const int64_t offset,
      const int64_t tablet_serialize_size);
  int get_sslog_tablet_meta_addr(
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id,
      share::SCN &reorg_scn,
      share::SCN &row_scn) const;
  int get_sslog_tablet_row_scn(
      share::SCN &row_scn) const;
#endif

  // just for compatibility, the old version ObMetaDiskAddr is serialized directly by memcpy in some scenarios
  int memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  void set_epoch(const int64_t epoch) { epoch_ = epoch; }
  int64_t get_epoch() const { return epoch_; }

  OB_UNIS_VERSION(1);
private:
  static const uint64_t FOURTH_ID_BIT_OFFSET = 30;
  static const uint64_t FOURTH_ID_BIT_SIZE = 30;
  static const uint64_t FOURTH_ID_BIT_TYPE = 4;
  static const uint64_t MAX_OFFSET = (0x1UL << FOURTH_ID_BIT_OFFSET) - 1;
  static const uint64_t MAX_SIZE = (0x1UL << FOURTH_ID_BIT_SIZE) - 1;
  static const uint64_t MAX_TYPE = (0x1UL << FOURTH_ID_BIT_TYPE) - 1;
  static const uint64_t SIX_ID_BIT_SEQ = 32;
  static const uint64_t SIX_ID_BIT_EPOCH = 32;
  int get_block_addr_from_block_(
      blocksstable::MacroBlockId &macro_id,
      int64_t &offset,
      int64_t &size) const;
#ifdef OB_BUILD_SHARED_STORAGE
  int get_block_addr_from_sslog_tablet_meta_(
      blocksstable::MacroBlockId &macro_id,
      int64_t &offset,
      int64_t &size) const;
#endif
private:
  union {
    int64_t first_id_;
    int64_t ls_id_;
  };
  union {
    int64_t second_id_;
    int64_t file_id_;
    uint64_t tablet_id_;
  };
  union {
    int64_t third_id_;
    int64_t reorg_scn_;
  };
  union {
    int64_t fourth_id_;
    struct {
      uint64_t offset_ : FOURTH_ID_BIT_OFFSET;
      uint64_t size_   : FOURTH_ID_BIT_SIZE;
      uint64_t type_   : FOURTH_ID_BIT_TYPE;
    };
  };
  union {
    int64_t fifth_id_;  // for the fourth_id_ of MacroBlockId
    int64_t row_scn_;
  };
  union { // doesn't serialize
    int64_t sixth_id_;
    struct {
      uint64_t seq_ : SIX_ID_BIT_SEQ;
      uint64_t epoch_ : SIX_ID_BIT_EPOCH;
    };
  };
};

template <typename T>
class ObMetaObj
{
public:
  ObMetaObj();
  virtual ~ObMetaObj() { reset(); };
  virtual void reset();

  TO_STRING_KV(KP_(pool), KP_(allocator), KP_(ptr), KP_(t3m));

public:
  ObITenantMetaObjPool *pool_;
  common::ObIAllocator *allocator_;
  T *ptr_;
  ObTenantMetaMemMgr *t3m_;
};

template <typename T>
class ObMetaObjGuard
{
public:
  ObMetaObjGuard();
  ObMetaObjGuard(const ObMetaObjGuard<T> &other);
  virtual ~ObMetaObjGuard();

  void reset();
  OB_INLINE T *get_obj() const;
  OB_INLINE void get_obj(ObMetaObj<T> &obj) const;

  bool is_valid() const;
  bool need_hold_time_check() const;

  void set_obj(ObMetaObj<T> &obj);
  void set_obj(T *obj, common::ObIAllocator *allocator, ObTenantMetaMemMgr *t3m);
  ObMetaObjGuard<T> &operator = (const ObMetaObjGuard<T> &other);

  TO_STRING_KV(KP_(obj), KP_(obj_pool), KP_(allocator), KP_(t3m));

protected:
  static const int64_t HOLD_OBJ_MAX_TIME = 2 * 60 * 60 * 1000 * 1000L; // 2h
  void reset_obj();

protected:
  T *obj_;
  ObITenantMetaObjPool *obj_pool_;
  common::ObIAllocator *allocator_;
  int64_t hold_start_time_;
  ObTenantMetaMemMgr *t3m_;
};

class ObIStorageMetaObj
{
public:
  ObIStorageMetaObj() = default;
  virtual ~ObIStorageMetaObj() = default;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const = 0;
  virtual int64_t get_deep_copy_size() const = 0;
};

template <typename T>
ObMetaObj<T>::ObMetaObj()
  : pool_(nullptr),
    allocator_(nullptr),
    ptr_(nullptr),
    t3m_(MTL(ObTenantMetaMemMgr*))
{
}

template <typename T>
void ObMetaObj<T>::reset()
{
  pool_ = nullptr;
  allocator_ = nullptr;
  ptr_ = nullptr;
  t3m_ = nullptr;
}

template <typename T>
ObMetaObjGuard<T>::ObMetaObjGuard()
  : obj_(nullptr),
    obj_pool_(nullptr),
    allocator_(nullptr),
    hold_start_time_(INT64_MAX),
    t3m_(nullptr)
{
}

template <typename T>
ObMetaObjGuard<T>::ObMetaObjGuard(const ObMetaObjGuard<T> &other)
  : obj_(nullptr),
    obj_pool_(nullptr),
    allocator_(nullptr),
    hold_start_time_(INT64_MAX),
    t3m_(nullptr)
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
  if (nullptr != obj.ptr_) {
    if (OB_UNLIKELY(nullptr == obj.pool_ && nullptr == obj.allocator_)) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool is nullptr", K(obj));
      ob_abort();
    } else {
      obj_pool_ = obj.pool_;
      allocator_ = obj.allocator_;
      t3m_ = obj.t3m_; // t3m maybe nullptr
    }
    obj_ = obj.ptr_;
    obj_->inc_ref();
    hold_start_time_ = ObClockGenerator::getClock();
  }
}

template <typename T>
void ObMetaObjGuard<T>::set_obj(T *obj, common::ObIAllocator *allocator, ObTenantMetaMemMgr *t3m)
{
  reset();
  allocator_ = allocator;
  t3m_ = t3m;
  if (nullptr == obj && nullptr == allocator && nullptr == t3m) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "invalid args to set", KP(obj), KP(allocator), KP(t3m));
    ob_abort();
  } else if (nullptr != obj) {
    if (nullptr == allocator) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "allocator is nullptr", KP(obj), KP(allocator), KP(t3m));
      ob_abort();
    } else {
      obj_ = obj;
      obj_->inc_ref();
      hold_start_time_ = ObClockGenerator::getClock();
    }
  }
}

template <typename T>
void ObMetaObjGuard<T>::reset()
{
  reset_obj();
  obj_pool_ = nullptr;
  allocator_ = nullptr;
  hold_start_time_ = INT64_MAX;
  t3m_ = nullptr;
}

template <typename T>
OB_INLINE bool ObMetaObjGuard<T>::is_valid() const
{
  return nullptr != obj_
      && nullptr != t3m_
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
    t3m_ = other.t3m_;
    if (nullptr != other.obj_) {
      if (OB_UNLIKELY(!other.is_valid())) {
        STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool and allocator is nullptr", K(other), KPC(this));
        ob_abort();
      } else {
        obj_ = other.obj_;
        hold_start_time_ = ObClockGenerator::getClock();
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
OB_INLINE T *ObMetaObjGuard<T>::get_obj() const
{
  return obj_;
}

template <typename T>
OB_INLINE void ObMetaObjGuard<T>::get_obj(ObMetaObj<T> &obj) const
{
  obj.pool_ = obj_pool_;
  obj.allocator_ = allocator_;
  obj.ptr_ = obj_;
  obj.t3m_ = t3m_;
}

template <typename T>
void ObMetaObjGuard<T>::reset_obj()
{
  if (nullptr != obj_) {
    if (OB_UNLIKELY(!is_valid())) {
      STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object pool and allocator is nullptr", K_(obj), K_(obj_pool), K_(allocator), K_(t3m));
      ob_abort();
    } else {
      const int64_t ref_cnt = obj_->dec_ref();
      const int64_t hold_time = ObClockGenerator::getClock() - hold_start_time_;
      if (OB_UNLIKELY(hold_time > HOLD_OBJ_MAX_TIME && need_hold_time_check())) {
        int ret = OB_ERR_TOO_MUCH_TIME;
        STORAGE_LOG(WARN, "The meta obj reference count was held for more "
            "than two hours ", K(ref_cnt), KP(this), K(hold_time), K(hold_start_time_), KPC(this), K(common::lbt()));
      }
      if (0 == ref_cnt) {
        if (nullptr != obj_pool_) {
          obj_pool_->free_obj(obj_);
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
      t3m_ = nullptr;
    }
  }
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_META_OBJ_STRUCT_H_ */
