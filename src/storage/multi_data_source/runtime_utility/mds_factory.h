/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_MDS_FACTORY_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_UTILITY_MDS_FACTORY_H
#include "lib/allocator/ob_malloc.h"
#include <type_traits>
#include <typeinfo>
#include "lib/atomic/ob_atomic.h"
#include "ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "src/share/ob_errno.h"
#include "common/meta_programming/ob_type_traits.h"
#include "mds_tenant_service.h"

namespace oceanbase
{
namespace transaction
{
enum class ObTxDataSourceType : int64_t;
class ObTransID;
}
namespace storage
{
namespace mds
{

struct DefaultAllocator : public ObIAllocator
{
  void *alloc(const int64_t size)  { return ob_malloc(size, "MDS"); }
  void *alloc(const int64_t size, const ObMemAttr &attr) { return ob_malloc(size, attr); }
  void free(void *ptr) { ob_free(ptr); }
  void set_label(const lib::ObLabel &) {}
  static DefaultAllocator &get_instance() { static DefaultAllocator alloc; return alloc; }
  static int64_t get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
  static int64_t get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }
private:
  DefaultAllocator() : alloc_times_(0), free_times_(0) {}
  int64_t alloc_times_;
  int64_t free_times_;
};

class BufferCtx;
struct MdsAllocator : public ObIAllocator
{
  void *alloc(const int64_t size);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  void free(void *ptr);
  void set_label(const lib::ObLabel &);
  static MdsAllocator &get_instance();
  static int64_t get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
  static int64_t get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }
private:
  MdsAllocator() : alloc_times_(0), free_times_(0) {}
  int64_t alloc_times_;
  int64_t free_times_;
};

struct MdsFactory
{
  // 如果类型T有init函数，那么先用默认构造函数构造，然后再调用其init函数
  template <typename T, typename ...Args, typename std::enable_if<!std::is_base_of<BufferCtx, T>::value, bool>::type = true>
  static int create(T *&p_obj, Args &&...args)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(create_(p_obj, std::forward<Args>(args)...))) {
      MDS_LOG(WARN, "fail to create object", KR(ret), K(typeid(T).name()), KP(p_obj), K(lbt()));
    }
    return ret;
  }
  template <typename T>
  static void destroy(T *obj)
  {
    MDS_LOG(DEBUG, "destroy object", K(typeid(T).name()), KP(obj), K(lbt()));
    if (OB_NOT_NULL(obj)) {
      obj->~T();
      MdsAllocator::get_instance().free(obj);
    }
  }

  // 根据构造对象时编码好的信息进行运行时反射
  static int deep_copy_buffer_ctx(const transaction::ObTransID &trans_id,
                                  const BufferCtx &old_ctx,
                                  BufferCtx *&new_ctx,
                                  const char *alloc_file = __builtin_FILE(),
                                  const char *alloc_func = __builtin_FUNCTION(),
                                  const int64_t line = __builtin_LINE());
  static int create_buffer_ctx(const transaction::ObTxDataSourceType &data_source_type,
                               const transaction::ObTransID &trans_id,
                               BufferCtx *&buffer_ctx,
                               const char *alloc_file = __builtin_FILE(),
                               const char *alloc_func = __builtin_FUNCTION(),
                               const int64_t line = __builtin_LINE());
private:
  // 如果类型T有init函数，那么先用默认构造函数构造，然后再调用其init函数
  template <typename T, typename ...Args, ENABLE_IF_HAS(T, init, int(Args...))>
  static int create_(T *&p_obj, Args &&...args)
  {
    int ret = common::OB_SUCCESS;
    T *temp_obj = (T *)MdsAllocator::get_instance().alloc(sizeof(T));
    if (OB_ISNULL(temp_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "alloc memory failed", KR(ret));
    } else {
      new (temp_obj)T();
      if (OB_FAIL(temp_obj->init(std::forward<Args>(args)...))) {
        MDS_LOG(WARN, "init obj failed", KR(ret), K(typeid(T).name()));
        temp_obj->~T();
        MdsAllocator::get_instance().free(temp_obj);
      } else {
        p_obj = temp_obj;
      }
    }
    MDS_LOG(DEBUG, "create object with init", K(typeid(T).name()), KP(p_obj), K(lbt()));
    return ret;
  }
  // 如果类型T没有init函数，则通过构造函数构造
  template <typename T, typename ...Args, ENABLE_IF_NOT_HAS(T, init, int(Args...))>
  static int create_(T *&p_obj, Args &&...args)
  {
    int ret = common::OB_SUCCESS;
    T *temp_obj = (T *)MdsAllocator::get_instance().alloc(sizeof(T));
    if (OB_ISNULL(temp_obj)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "alloc memory failed", KR(ret));
    } else {
      new (temp_obj)T(std::forward<Args>(args)...);
      p_obj = temp_obj;
    }
    MDS_LOG(DEBUG, "create object with construction", K(typeid(T).name()), KP(p_obj), K(lbt()));
    return ret;
  }
};

}
}
}
#endif