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

#ifndef OCEANBASE_ALLOCATOR_OB_TENANT_VECTOR_ALLOCATOR_H_
#define OCEANBASE_ALLOCATOR_OB_TENANT_VECTOR_ALLOCATOR_H_
#include "lib/vector/ob_vector_util.h"
#include "share/throttle/ob_share_throttle_define.h"
#include "observer/omt/ob_tenant_config_mgr.h"
namespace oceanbase {
namespace share {

class ObVectorMemContext {
public:
    ObVectorMemContext()
    : check_cnt_(0),
      memory_context_(nullptr),
      throttle_tool_(nullptr) {};
  int init(lib::MemoryContext &mem_context, share::TxShareThrottleTool *throttle_tool);

  void *alloc(int64_t size);
  void free(void *ptr);
  static const uint32_t CHECK_USAGE_INTERVAL = 20;
  static const int64_t CHECK_RESOURCE_UNIT_SIZE = 2LL * 1024LL * 1024LL; /* 2MB */

private:
  // when check_cnt_ exceed CHECK_USAGE_INTERVAL, check memory usage
  uint32_t check_cnt_;
  // 由父节点memory_context创建
  lib::MemoryContext memory_context_;
 // 引用内存统计工具
  share::TxShareThrottleTool *throttle_tool_;
};

class ObTenantVectorAllocator : public ObVectorMemContext,
                                public ObIAllocator {
public:
  ObTenantVectorAllocator() : is_inited_(false), all_used_mem_(0), throttle_tool_(nullptr), memory_context_(nullptr) {}
  ~ObTenantVectorAllocator() { destroy(); }
  DEFINE_CUSTOM_FUNC_FOR_THROTTLE(Vector);

  int init();
  void destroy();
  virtual void *alloc(const int64_t size) override { return ObVectorMemContext::alloc(size); }
  void *alloc(const int64_t size, const int64_t expire_ts);
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override { ObVectorMemContext::free(ptr); }
  int64_t hold();
  int64_t used();
  inline lib::MemoryContext get_mem_context() { return memory_context_;}
  inline uint64_t* get_used_mem_ptr() { return &all_used_mem_; }
  int64_t get_rb_mem_used();
  static void get_vector_mem_config(int64_t &resource_limit, int64_t &max_duration);
  static int64_t get_vector_mem_limit_percentage(omt::ObTenantConfigGuard &tenant_config, uint64_t tenant_id = MTL_ID());
  TO_STRING_KV(K(is_inited_), KP(throttle_tool_), KP(memory_context_.ref_context()));

private:
  bool is_inited_;
  uint64_t all_used_mem_;
  share::TxShareThrottleTool *throttle_tool_;
  lib::MemoryContext memory_context_;
};

class ObVsagMemContext : public vsag::Allocator,
                         public ObVectorMemContext
{
public:
  ObVsagMemContext(uint64_t *all_vsag_use_mem)
    : all_vsag_use_mem_(all_vsag_use_mem),
      mem_context_(nullptr) {};
  ~ObVsagMemContext() {
    if (mem_context_ != nullptr) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
  int init(lib::MemoryContext &parent_mem_context, uint64_t *all_vsag_use_mem, uint64_t tenant_id);
  bool is_inited() { return OB_NOT_NULL(mem_context_); }

  std::string Name() override {
    return "ObVsagAlloc";
  }
  void* Allocate(size_t size) override;

  void Deallocate(void* p) override;

  void* Reallocate(void* p, size_t size) override;

  int64_t hold() {
    int64_t res = 0;
    if (mem_context_.ref_context() != NULL) {
      res = mem_context_->hold();
    }
    return res;
  }

  int64_t used() {
    int64_t res = 0;
    if (mem_context_.ref_context() != NULL) {
      res = mem_context_->used();
    }
    return res;
  }
  inline lib::MemoryContext &mem_ctx() { return mem_context_; }

private:
  uint64_t *all_vsag_use_mem_;
  lib::MemoryContext mem_context_;
  constexpr static int64_t MEM_PTR_HEAD_SIZE = sizeof(int64_t);
};

class ObIvfMemContext : public ObVectorMemContext
{
public:
  ObIvfMemContext(uint64_t *all_vsag_use_mem)
    : all_vsag_use_mem_(all_vsag_use_mem),
      mem_context_(nullptr) {};
  ~ObIvfMemContext() {
    if (mem_context_ != nullptr) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
  int init(lib::MemoryContext &parent_mem_context, uint64_t *all_vsag_use_mem, uint64_t tenant_id, const char *label = IVF_CACHE_LABEL);
  bool is_inited() { return OB_NOT_NULL(mem_context_); }
  void* Allocate(size_t size);
  void Deallocate(void* p);
  int64_t hold() {
    int64_t res = 0;
    if (mem_context_.ref_context() != NULL) {
      res = mem_context_->hold();
    }
    return res;
  }

  int64_t used() {
    int64_t res = 0;
    if (mem_context_.ref_context() != NULL) {
      res = mem_context_->used();
    }
    return res;
  }
  inline uint64_t * get_all_vsag_use_mem() { return all_vsag_use_mem_; }
  inline uint64_t get_all_vsag_use_mem_byte() { return ATOMIC_LOAD(all_vsag_use_mem_); }
  inline lib::MemoryContext &get_mem_context() { return mem_context_;}
public:
  static const char* IVF_CACHE_LABEL;
  static const char* IVF_BUILD_LABEL;
private:
  uint64_t *all_vsag_use_mem_;
  lib::MemoryContext mem_context_;
  constexpr static int64_t MEM_PTR_HEAD_SIZE = sizeof(int64_t);
};

}  // namespace share
}  // namespace oceanbase

#endif
