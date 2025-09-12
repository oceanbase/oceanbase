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
 *
 *  TransCtx: Context for Distributed Transactions
 */

#ifndef OCEANBASE_LIBOBCDC_MEM_MGR_H_
#define OCEANBASE_LIBOBCDC_MEM_MGR_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace libobcdc
{

#define OBCDC_ALLOC_MEM_CHECK_NULL(label, ret_obj, allocator_ref, args...) \
  ({ \
    ret_obj = nullptr; \
    bool hang_on_alloc_fail = true; \
    while (OB_ISNULL(ret_obj = allocator_ref.alloc(args)) && hang_on_alloc_fail) { \
    ObCDCMemController::get_instance().handle_when_alloc_fail(label, hang_on_alloc_fail); \
    } \
    ret_obj; \
  })

#define OBCDC_ALLOC_MEM_CHECK_NULL_WITH_CAST(label, obj_type, ret_obj, allocator_ref, args...) \
  ({ \
    ret_obj = nullptr; \
    bool hang_on_alloc_fail = true; \
    while (OB_ISNULL(ret_obj =  static_cast<obj_type*>(allocator_ref.alloc(args))) && hang_on_alloc_fail) { \
      ObCDCMemController::get_instance().handle_when_alloc_fail(label, hang_on_alloc_fail); \
    } \
    ret_obj; \
  })

#define OBCDC_ALLOC_RETRY_ON_FAIL(label, allocator_ref, args...) \
  ({ \
    int tmp_alloc_ret = OB_ALLOCATE_MEMORY_FAILED; \
    bool hang_on_alloc_fail = true; \
    while (OB_ALLOCATE_MEMORY_FAILED == tmp_alloc_ret && hang_on_alloc_fail) { \
      tmp_alloc_ret = allocator_ref.alloc(args); \
      if (OB_ALLOCATE_MEMORY_FAILED == tmp_alloc_ret) { \
        ObCDCMemController::get_instance().handle_when_alloc_fail(label, hang_on_alloc_fail); \
      } \
    } \
    tmp_alloc_ret; \
  })

class ObLogConfig;

// params refresh from config by default
class ObCDCMemController
{
public:
  ObCDCMemController();
  ~ObCDCMemController() { reset(); }
  void reset();
  static ObCDCMemController &get_instance();
public:
  int init(const ObLogConfig &config);
  void configure(const ObLogConfig &config);

  // memory is limited by CHUNK_MGR
  OB_INLINE bool touch_hard_mem_limit() const
  {
    return enable_hard_mem_limit_ && hard_mem_limit_ <= lib::get_memory_hold();
  }

  void handle_when_alloc_fail(const char* label, bool &hang_on_alloc_fail);

  void print_mem_stat();
  void dump_malloc_sample();

  TO_STRING_KV(K_(enable_hard_mem_limit), K_(hard_mem_limit));
private:
  void refresh_hard_mem_limit_(const ObLogConfig &config);
private:
  bool enable_hard_mem_limit_;
  int64_t hard_mem_limit_threshold_;
  int64_t hard_mem_limit_;
  DISALLOW_COPY_AND_ASSIGN(ObCDCMemController);
};

} // end of namespace libobcdc
} // end of namespace oceanbase
#endif
