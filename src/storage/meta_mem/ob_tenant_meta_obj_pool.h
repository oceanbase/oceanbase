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

#ifndef OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_
#define OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_

#include "lib/objectpool/ob_resource_pool.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace storage
{

class ObTenantMetaMemMgr;
class ObTablet;

class RPMetaObjLabel
{
public:
  static constexpr const char LABEL[] = "MetaObj";
};

class ObMetaObjBufferHeader final
{
public:
  static const uint16_t MAGIC_NUM = 0xa12f;
public:
  ObMetaObjBufferHeader()
    : buf_len_(0),
      magic_num_(MAGIC_NUM),
      has_new_(false),
      in_map_(false),
      reserved_(0)
  {}
  explicit ObMetaObjBufferHeader(const uint64_t header)
    : header_(header)
  {}
  ~ObMetaObjBufferHeader() = default;
  TO_STRING_KV(K(header_), K(buf_len_), K(magic_num_), K(has_new_), K(in_map_), K(reserved_));
public:
  union {
    uint64_t header_;
    struct {
      uint64_t buf_len_   : 32;
      uint64_t magic_num_ : 16;
      uint64_t has_new_   : 1;
      uint64_t in_map_    : 1;
      uint64_t reserved_  : 14;
    };
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObMetaObjBufferHeader);
};

typedef common::ObDLinkNode<ObMetaObjBufferHeader> ObMetaObjBufferNode;

template <typename T, int64_t BUFFER_LENGTH>
class ObMetaObjBuffer final
{
public:
  ObMetaObjBuffer()
    : header_()
  {
    header_.get_data().buf_len_ = BUFFER_LENGTH;
    memset(buf_, 0x0, BUFFER_LENGTH);
  }
  ~ObMetaObjBuffer() { reset(); }
  char *buf() { return buf_; }
  void reset()
  {
    if (header_.get_data().has_new_) {
      reinterpret_cast<T *>(buf_)->~T();
      header_.get_data().has_new_ = false;
    }
    header_.get_data().in_map_ = false;
    memset(buf_, 0x0, BUFFER_LENGTH);
  }
public:
  ObMetaObjBufferNode header_;
  char buf_[BUFFER_LENGTH];
private:
  DISALLOW_COPY_AND_ASSIGN(ObMetaObjBuffer);
};

class ObMetaObjBufferHelper final
{
public:
  static ObMetaObjBufferNode &get_linked_node(char *obj);
  static ObMetaObjBufferHeader &get_buffer_header(char *obj);
  static char *get_obj_buffer(ObMetaObjBufferNode *node);
  static void *get_meta_obj_buffer_ptr(char *obj);
  static void set_in_map(char *obj, const bool in_map)
  {
    uint64_t old = ATOMIC_LOAD(&(get_buffer_header(obj).header_));
    ObMetaObjBufferHeader new_header(old);
    new_header.in_map_ = in_map;
    while (old != (new_header.header_ = ATOMIC_CAS(&(get_buffer_header(obj).header_), old, new_header.header_))) {
      old = new_header.header_;
      new_header.in_map_ = in_map;
    }
  }
  static bool is_in_map(char *obj)
  {
    return ObMetaObjBufferHeader(ATOMIC_LOAD(&(get_buffer_header(obj).header_))).in_map_;
  }
  template <typename T>
  static void new_meta_obj(void *buf, T *&obj)
  {
    char *obj_buf = static_cast<char *>(buf) + sizeof(ObMetaObjBufferNode);
    ObMetaObjBufferHeader &header = get_buffer_header(obj_buf);
    abort_unless(false == header.has_new_);
    obj = new (obj_buf) T();
    header.has_new_ = true;
    header.in_map_ = false;
  }
  template <typename T>
  static void del_meta_obj(T *obj)
  {
    ObMetaObjBufferHeader &header = get_buffer_header(reinterpret_cast<char *>(obj));
    obj->~T();
    header.has_new_ = false;
    header.in_map_ = false;
  }
};

class TryWashTabletFunc final
{
public:
  explicit TryWashTabletFunc(ObTenantMetaMemMgr &t3m);
  ~TryWashTabletFunc();

  int operator()(const std::type_info &type_info, void *&free_obj);

private:
  ObTenantMetaMemMgr &t3m_;
  DISALLOW_COPY_AND_ASSIGN(TryWashTabletFunc);
};

class ObITenantMetaObjPool
{
public:
  ObITenantMetaObjPool() = default;
  virtual ~ObITenantMetaObjPool() = default;

  virtual void free_obj(void *obj) = 0;
  virtual int alloc_obj(void *&obj) = 0;
};

template <typename T>
class ObTenantMetaObjPool
    : public common::ObBaseResourcePool<T, RPMetaObjLabel>,
      public ObITenantMetaObjPool
{
private:
  typedef common::ObBaseResourcePool<T, RPMetaObjLabel> BasePool;
public:
  ObTenantMetaObjPool(
      const uint64_t tenant_id,
      const int64_t max_free_list_num,
      const lib::ObLabel &label,
      const uint64_t ctx_id,
      TryWashTabletFunc *wash_func = nullptr,
      const bool allow_over_max_free_num = true);
  virtual ~ObTenantMetaObjPool();

  int64_t used() const { return allocator_.used(); }
  int64_t total() const { return allocator_.total(); }
  int64_t get_used_obj_cnt() const { return ATOMIC_LOAD(&used_obj_cnt_); }
  int64_t get_free_obj_cnt() const { return this->get_free_num(); }
  int64_t get_obj_size() const { return sizeof(T); }
  virtual void free_obj(void *obj) override;
  virtual int alloc_obj(void *&obj) override;
  virtual void free_node_(typename BasePool::Node *ptr) override;

  int acquire(T *&t);
  void release(T *t);

  TO_STRING_KV(K(typeid(T).name()), K(sizeof(T)), K_(used_obj_cnt), "free_obj_hold_cnt",
      this->get_free_num(), "allocator used", allocator_.used(), "allocator total",
      allocator_.total());
private:
  TryWashTabletFunc *wash_func_;
  common::ObFIFOAllocator allocator_;
  int64_t used_obj_cnt_;
  bool allow_over_max_free_num_;
};

template <class T>
void ObTenantMetaObjPool<T>::free_obj(void *obj)
{
  release(static_cast<T *>(obj));
}

template <class T>
int ObTenantMetaObjPool<T>::alloc_obj(void *&obj)
{
  int ret = OB_SUCCESS;
  T *t = nullptr;
  if (OB_FAIL(acquire(t))) {
    STORAGE_LOG(WARN, "fail to acquire object", K(ret));
  } else {
    obj = static_cast<void *>(t);
  }
  return ret;
}

template <class T>
int ObTenantMetaObjPool<T>::acquire(T *&t)
{
  int ret = OB_SUCCESS;
  t = nullptr;
  const bool allow_alloc = allow_over_max_free_num_ || (BasePool::max_free_list_num_ > ATOMIC_LOAD(&used_obj_cnt_));
  if (allow_alloc && OB_NOT_NULL(t = BasePool::alloc())) {
    (void)ATOMIC_AAF(&used_obj_cnt_, 1);
  } else {
    const int64_t max_wait_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 3L; // 3s
    while (OB_SUCC(ret)
           && OB_ISNULL(t)
           && OB_NOT_NULL(wash_func_)
           && max_wait_ts - ObTimeUtility::fast_current_time() >= 0) {
      ob_usleep(1);
      void *free_obj = nullptr;
      if (OB_FAIL((*wash_func_)(typeid(T), free_obj))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "wash function fail", K(ret));
        }
      } else {
        t = static_cast<T *>(free_obj);
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (OB_ISNULL(t)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(DEBUG, "no object could be acquired", K(ret));
      }
    }
  }
  return ret;
}

template <class T>
void ObTenantMetaObjPool<T>::release(T *t)
{
  BasePool::free(t);
  (void)ATOMIC_AAF(&used_obj_cnt_, -1);
}

template <class T>
void ObTenantMetaObjPool<T>::free_node_(typename BasePool::Node *ptr)
{
  if (NULL != ptr) {
    ptr->data.reset();
    ptr->next = NULL;
    if (BasePool::ALLOC_BY_INNER_ALLOCATOR == ptr->flag) {
      if (common::OB_SUCCESS != BasePool::free_list_.push(ptr)) {
        _COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "free node to list fail, size=%ld ptr=%p", BasePool::free_list_.get_total(), ptr);
      }
      (void)ATOMIC_AAF(&(BasePool::inner_used_num_), -1);
    } else if (BasePool::ALLOC_BY_OBMALLOC == ptr->flag) {
      ptr->~Node();
      common::ob_free(ptr);
    } else {
      _COMMON_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid flag=%lu ptr=%p", ptr->flag, ptr);
    }
  }
}

template <class T>
ObTenantMetaObjPool<T>::ObTenantMetaObjPool(
    const uint64_t tenant_id,
    const int64_t max_free_list_num,
    const lib::ObLabel &label,
    const uint64_t ctx_id,
    TryWashTabletFunc *wash_func,
    const bool allow_over_max_free_num)
  : ObBaseResourcePool<T, RPMetaObjLabel>(max_free_list_num, &allocator_,
      lib::ObMemAttr(tenant_id, label, ctx_id)),
      wash_func_(wash_func),
      used_obj_cnt_(0),
      allow_over_max_free_num_(allow_over_max_free_num)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  const int64_t mem_limit = tenant_config.is_valid()
      ? tenant_config->_storage_meta_memory_limit_percentage : OB_DEFAULT_META_OBJ_PERCENTAGE_LIMIT;
  if (ObCtxIds::META_OBJ_CTX_ID == ctx_id && OB_FAIL(lib::set_meta_obj_limit(tenant_id, mem_limit))) {
    STORAGE_LOG(WARN, "fail to set meta object memory limit", K(ret), K(tenant_id), K(mem_limit));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), common::OB_MALLOC_MIDDLE_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, label, ctx_id)))) {
    STORAGE_LOG(WARN, "fail to initialize pool FIFO allocator", K(ret));
  }
  abort_unless(OB_SUCCESS == ret);
}

template <class T>
ObTenantMetaObjPool<T>::~ObTenantMetaObjPool()
{
  ObBaseResourcePool<T, RPMetaObjLabel>::destroy();
}

} // end namespace storage
} // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_TENANT_META_OBJ_MGR_H_ */
