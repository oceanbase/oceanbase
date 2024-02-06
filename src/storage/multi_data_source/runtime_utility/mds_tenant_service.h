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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TENANT_SERVICE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TENANT_SERVICE_H

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string_holder.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "share/ob_ls_id.h"
#include "share/ob_occam_timer.h"
// #include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "lib/hash/ob_linear_hash_map.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
class ObLS;
class ObLSHandle;
class ObTablet;
class ObTabletHandle;
namespace mds
{
class MdsWriter;
class MdsTableHandle;
class ObTenantMdsService;
/********************FOR MEMORY LEAK DEBUG***************************/
static constexpr const int64_t TAG_SIZE = 64;
extern thread_local char __thread_mds_tag__[TAG_SIZE];
extern TLOCAL(const char *, __thread_mds_alloc_type__);
extern TLOCAL(const char *, __thread_mds_alloc_file__);
extern TLOCAL(const char *, __thread_mds_alloc_func__);
extern TLOCAL(uint32_t, __thread_mds_alloc_line__);

extern void set_mds_mem_check_thread_local_info(const MdsWriter &writer,
                                                const char *alloc_ctx_type,
                                                const char *alloc_file = __builtin_FILE(),
                                                const char *alloc_func = __builtin_FUNCTION(),
                                                const uint32_t line = __builtin_LINE());
extern void set_mds_mem_check_thread_local_info(const share::ObLSID &ls_id,
                                                const ObTabletID &tablet_id,
                                                const char *data_type,
                                                const char *alloc_file = __builtin_FILE(),
                                                const char *alloc_func = __builtin_FUNCTION(),
                                                const uint32_t line = __builtin_LINE());
extern void reset_mds_mem_check_thread_local_info();
/********************************************************************/

struct ObMdsMemoryLeakDebugInfo
{
  ObMdsMemoryLeakDebugInfo()
  : data_type_(nullptr), alloc_file_(nullptr), alloc_func_(nullptr), alloc_line_(0), alloc_ts_(0), tid_(0) {}
  ObMdsMemoryLeakDebugInfo(const char *tag,
                           const int64_t tag_size,
                           const char *type,
                           const char *alloc_file,
                           const char *alloc_func,
                           int64_t line)
  : data_type_(type),
  alloc_file_(alloc_file),
  alloc_func_(alloc_func),
  alloc_line_(line),
  alloc_ts_(ObTimeUtility::fast_current_time()),
  tid_(GETTID()) {
    memcpy(tag_str_, tag, std::min(TAG_SIZE, tag_size));
  }
  ObMdsMemoryLeakDebugInfo(const ObMdsMemoryLeakDebugInfo &rhs) = default;// value sematic copy construction
  ObMdsMemoryLeakDebugInfo &operator=(const ObMdsMemoryLeakDebugInfo &rhs) = default;// value sematic copy assign
  TO_STRING_KV(K_(tag_str), K_(data_type), K_(alloc_file), K_(alloc_func), K_(alloc_line), KTIME_(alloc_ts), K_(tid));
  const char *data_type_;
  const char *alloc_file_;
  const char *alloc_func_;
  int64_t alloc_line_;
  char tag_str_[TAG_SIZE] = {0};
  int64_t alloc_ts_;
  int64_t tid_;
};

class ObTenantMdsAllocator : public ObIAllocator
{
  friend class ObTenantMdsService;
private:
  static const int64_t MDS_ALLOC_CONCURRENCY = 32;
public:
  ObTenantMdsAllocator() = default;
  int init();
  void destroy() {}
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const ObMemAttr &attr) override;
  int64_t hold() { return allocator_.hold(); }
  TO_STRING_KV(KP(this));
private:
  common::ObBlockAllocMgr block_alloc_;
  common::ObVSliceAlloc allocator_;
};

struct ObTenantBufferCtxAllocator : public ObIAllocator// for now, it is just a wrapper of mtl_malloc
{
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void set_attr(const ObMemAttr &) override {}
};

struct ObTenantMdsTimer
{
  ObTenantMdsTimer() = default;
  int init_and_start();
  void stop();
  void wait();
  void try_recycle_mds_table_task();
  void dump_special_mds_table_status_task();
  TO_STRING_KV(KP(this), K_(recycle_task_handle))
  common::ObOccamTimerTaskRAIIHandle recycle_task_handle_;
  common::ObOccamTimerTaskRAIIHandle dump_special_mds_table_status_task_handle_;
  common::ObOccamTimer timer_;
private:
  int process_with_tablet_(ObTablet &tablet);
  int get_tablet_oldest_scn_(ObTablet &tablet, share::SCN &oldest_scn);
  int try_recycle_mds_table_(ObTablet &tablet, const share::SCN &recycle_scn);
  int try_gc_mds_table_(ObTablet &tablet);
};

class ObTenantMdsService
{
public:
  ObTenantMdsService() : is_inited_(false) {}
  ~ObTenantMdsService() {
    if (memory_leak_debug_map_.count() != 0) {
      MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "there are holding items not released when mds allocator released");
      dump_map_holding_item(0);
    }
    MDS_LOG_RET(INFO, OB_SUCCESS, "ObTenantMdsAllocator destructed");
  }
  static int mtl_init(ObTenantMdsService* &);
  static int mtl_start(ObTenantMdsService* &);
  static void mtl_stop(ObTenantMdsService* &);
  static void mtl_wait(ObTenantMdsService* &);
  void destroy() { this->~ObTenantMdsService(); }
  static int for_each_ls_in_tenant(const ObFunction<int(ObLS &)> &op);
  static int for_each_tablet_in_ls(ObLS &ls, const ObFunction<int(ObTablet &)> &op);
  static int for_each_mds_table_in_ls(ObLS &ls, const ObFunction<int(ObTablet &)> &op);
  ObTenantMdsAllocator &get_allocator() { return mds_allocator_; }
  ObTenantBufferCtxAllocator &get_buffer_ctx_allocator() { return buffer_ctx_allocator_; }
  TO_STRING_KV(KP(this), K_(is_inited), K_(mds_allocator), K_(mds_timer))
public:
  /*******************debug for memoy leak************************/
  template <typename OP>
  void update_mem_leak_debug_info(void *obj, OP &&op) {
#ifdef ENABLE_DEBUG_MDS_MEM_LEAK
    int ret = OB_SUCCESS;
    if (OB_FAIL(memory_leak_debug_map_.operate(ObIntWarp((int64_t)obj), op))) {
      MDS_LOG(WARN, "fail to update mem check debug info", KR(ret), KP(obj));
    }
#else
    UNUSED(obj);
    UNUSED(op);
#endif
  }
  void record_alloc_backtrace(void *obj,
                              const char *tag,
                              const char *data_type,
                              const char *alloc_file,
                              const char *alloc_func,
                              const int64_t line);
  void erase_alloc_backtrace(void *obj);
  void dump_map_holding_item(int64_t check_alive_time_threshold);
  /***************************************************************/
private:
  bool is_inited_;
  ObTenantMdsAllocator mds_allocator_;
  ObTenantBufferCtxAllocator buffer_ctx_allocator_;
  ObTenantMdsTimer mds_timer_;
  /*******************debug for memoy leak************************/
  ObLinearHashMap<ObIntWarp, ObMdsMemoryLeakDebugInfo> memory_leak_debug_map_;
  /***************************************************************/
};

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase


#endif