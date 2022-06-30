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

#ifndef OB_PX_SQC_HANDLER_H_
#define OB_PX_SQC_HANDLER_H_

#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_sub_coord.h"
#include "lib/allocator/ob_safe_arena.h"

namespace oceanbase {

namespace sql {

#define OB_SQC_HANDLER_TRAN_STARTED (1ULL)
#define OB_SQC_HANDLER_QC_SQC_LINKED (1ULL << 1)

class ObPxWorkNotifier {
public:
  ObPxWorkNotifier() : start_worker_count_(0), finish_worker_count_(0), expect_worker_count_(0), cond_()
  {}
  ~ObPxWorkNotifier() = default;

  int wait_all_worker_start();

  void worker_start(int64_t tid);
  inline void worker_end(bool& all_worker_finish);

  int set_expect_worker_count(int64_t worker_count);

  const common::ObIArray<int64_t>& worker_thread_info()
  {
    return tid_array_;
  }

  TO_STRING_KV(K_(start_worker_count), K_(finish_worker_count), K_(expect_worker_count), K_(tid_array))

private:
  volatile int64_t start_worker_count_;
  volatile int64_t finish_worker_count_;
  int64_t expect_worker_count_;
  common::SimpleCond cond_;
  common::ObArray<int64_t> tid_array_;
};

class ObPxSqcHandler : public common::ObDLinkBase<ObPxSqcHandler> {
public:
  typedef uint64_t ObPxSQCHandlerId;

public:
  ObPxSqcHandler()
      : mem_context_(NULL),
        tenant_id_(UINT64_MAX),
        reserved_px_thread_count_(0),
        process_flags_(0),
        end_ret_(OB_SUCCESS),
        reference_count_(1),
        notifier_(nullptr),
        exec_ctx_(nullptr),
        des_phy_plan_(nullptr),
        sqc_init_args_(nullptr),
        sub_coord_(nullptr)
  {}
  ~ObPxSqcHandler() = default;

  static ObPxSqcHandler* get_sqc_handler();
  static void release_handler(ObPxSqcHandler* sqc_handler);
  virtual void reset() override;
  void release(bool& all_released)
  {
    int64_t reference_count = ATOMIC_AAF(&reference_count_, -1);
    all_released = reference_count == 0;
  }
  void inc_ref_count()
  {
    ATOMIC_AAF(&reference_count_, 1);
  }
  void dec_ref_count()
  {
    ATOMIC_AAF(&reference_count_, -1);
  }

  int init();
  bool valid()
  {
    return ((nullptr != notifier_) && (nullptr != exec_ctx_) && (nullptr != des_phy_plan_) &&
            (nullptr != sqc_init_args_) && (nullptr != sub_coord_) && (nullptr != mem_context_));
  }

  int32_t get_type()
  {
    return 0;
  }

  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  sql::ObDesExecContext& get_exec_ctx()
  {
    return *exec_ctx_;
  }
  ObPhysicalPlan& get_phy_plan()
  {
    return *des_phy_plan_;
  }
  ObPxRpcInitSqcArgs& get_sqc_init_arg()
  {
    return *sqc_init_args_;
  }
  common::ObIAllocator* get_des_allocator()
  {
    return &mem_context_->get_safe_arena_allocator();
  }
  int64_t get_reserved_px_thread_count() const
  {
    return reserved_px_thread_count_;
  }
  ObPxSubCoord& get_sub_coord()
  {
    return *sub_coord_;
  }
  ObPxSQCProxy& get_sqc_proxy()
  {
    return sub_coord_->get_sqc_proxy();
  }
  ObSqcCtx& get_sqc_ctx()
  {
    return sub_coord_->get_sqc_ctx();
  }
  ObPxWorkNotifier& get_notifier()
  {
    return *notifier_;
  }

  int worker_end_hook();
  int copy_sqc_init_arg(int64_t& pos, const char* data_buf, int64_t data_len);
  int pre_acquire_px_worker(int64_t& reserved_thread_count);
  int init_env();
  int link_qc_sqc_channel();
  int get_partitions_info(ObIArray<ObPxPartitionInfo>& partitions_info);
  void check_interrupt();
  void reset_reference_count()
  {
    reference_count_ = 1;
  }
  common::ObIAllocator& get_safe_allocator()
  {
    return mem_context_->get_safe_arena_allocator();
  }
  int64_t get_reference_count() const
  {
    return reference_count_;
  }
  bool has_flag(uint64_t flag)
  {
    return !!(process_flags_ & flag);
  };
  void set_end_ret(int ret)
  {
    end_ret_ = ret;
  }
  int get_end_ret()
  {
    return end_ret_;
  }
  bool need_rollback();
  TO_STRING_KV(K_(tenant_id), K_(reserved_px_thread_count), KP_(notifier), K_(exec_ctx), K_(des_phy_plan),
      K_(sqc_init_args), KP_(sub_coord));

private:
  int destroy_sqc();
  void add_flag(uint64_t flag)
  {
    process_flags_ |= flag;
  };

private:
  lib::MemoryContext mem_context_;
  uint64_t tenant_id_;
  int64_t reserved_px_thread_count_;
  uint64_t process_flags_;
  int end_ret_;
  volatile int64_t reference_count_;
  ObPxWorkNotifier* notifier_;
  sql::ObDesExecContext* exec_ctx_;
  ObPhysicalPlan* des_phy_plan_;
  ObPxRpcInitSqcArgs* sqc_init_args_;
  ObPxSubCoord* sub_coord_;
};

typedef common::ObGlobalFactory<ObPxSqcHandler, 1, common::ObModIds::OB_SQL_SQC_HANDLER> ObPxSqcHandlerFactory;
typedef common::ObTCFactory<ObPxSqcHandler, 1, common::ObModIds::OB_SQL_SQC_HANDLER> ObPxSqcHandlerTCFactory;

}  // namespace sql
}  // namespace oceanbase

#endif
