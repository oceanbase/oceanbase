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

#ifndef _OB_SQ_OB_PX_MERGE_SORT_RECEIVE_H_
#define _OB_SQ_OB_PX_MERGE_SORT_RECEIVE_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/executor/ob_receive.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/basic/ob_ra_row_store.h"

namespace oceanbase {
namespace sql {

class ObPxMergeSortReceiveInput : public ObPxReceiveInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxMergeSortReceiveInput() : ObPxReceiveInput()
  {}
  virtual ~ObPxMergeSortReceiveInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_MERGE_SORT_RECEIVE;
  }  // XXX
};

class ObPxMergeSortReceive : public ObPxReceive, public ObSortableTrait {
public:
  OB_UNIS_VERSION_V(1);

public:
  class ObPxMergeSortReceiveCtx;
  class MergeSortInput {
  public:
    explicit MergeSortInput(
        ObRARowStore* get_row_store, ObRARowStore* add_row_ptr, int64_t pos, bool finish, bool can_free)
        : get_row_store_(get_row_store), add_row_store_(add_row_ptr), pos_(pos), finish_(finish), can_free_(can_free)
    {}
    virtual ~MergeSortInput() = default;

    virtual int get_row(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObNewRow*& row) = 0;
    virtual int add_row(ObPxMergeSortReceiveCtx& recv_ctx, const common::ObNewRow& row) = 0;

    virtual void set_finish(bool finish)
    {
      finish_ = finish;
    }
    virtual int64_t max_pos() = 0;
    virtual void destroy() = 0;
    virtual bool is_finish() const
    {
      return finish_;
    }
    virtual void clean_row_store(ObPxMergeSortReceiveCtx& recv_ctx) = 0;

    TO_STRING_KV(K_(pos));

  public:
    ObRARowStore* get_row_store_;
    ObRARowStore* add_row_store_;

    int64_t pos_;
    bool finish_;
    bool can_free_;  // the first local group data can free RaRowStore, 0 == pos_
  };

  class GlobalOrderInput : public MergeSortInput {
  public:
    explicit GlobalOrderInput(uint64_t tenant_id) : MergeSortInput(nullptr, nullptr, 0, false, true)
    {
      tenant_id_ = tenant_id;
    }
    virtual ~GlobalOrderInput()
    {
      destroy();
    }

    virtual int get_row(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObNewRow*& row);
    virtual int add_row(ObPxMergeSortReceiveCtx& recv_ctx, const common::ObNewRow& row);

    virtual int64_t max_pos();
    virtual void destroy();
    virtual void clean_row_store(ObPxMergeSortReceiveCtx& recv_ctx);
    virtual bool is_empty();

  private:
    virtual int reset_add_row_store(bool& reset);
    virtual int switch_get_row_store();
    int get_one_row_from_channels(
        ObPxMergeSortReceiveCtx& recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx) const;

  private:
    static const int64_t MAX_ROWS_PER_STORE = 50L;
    uint64_t tenant_id_;
    int64_t add_saved_pos_;
    int64_t get_pos_;
  };

  class LocalOrderInput : public MergeSortInput {
  public:
    explicit LocalOrderInput(ObRARowStore* get_row_store, int64_t pos, int64_t end_count)
        : MergeSortInput(get_row_store, get_row_store, pos, false, 0 == pos),
          org_start_pos_(pos),
          end_count_(end_count),
          reader_(*get_row_store)
    {}

    virtual ~LocalOrderInput()
    {
      destroy();
    }
    virtual int get_row(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObNewRow*& row);
    virtual int add_row(ObPxMergeSortReceiveCtx& recv_ctx, const common::ObNewRow& row);
    virtual int64_t max_pos();
    virtual void destroy();
    virtual void clean_row_store(ObPxMergeSortReceiveCtx& recv_ctx);

  private:
    int64_t org_start_pos_;  // save origin start position, it can't be changed
    int64_t end_count_;
    ObRARowStore::Reader reader_;
  };

  class ObPxMergeSortReceiveCtx : public ObPxReceiveCtx {
  public:
    friend class ObPxMergeSortReceive;

  public:
    explicit ObPxMergeSortReceiveCtx(ObExecContext& ctx)
        : ObPxReceive::ObPxReceiveCtx(ctx),
          ptr_px_row_(&px_row_),
          ptr_row_msg_loop_(&msg_loop_),
          interrupt_proc_(),
          row_heap_(),
          finish_(false)
    {}
    virtual ~ObPxMergeSortReceiveCtx() = default;
    virtual void destroy()
    {
      row_stores_.reset();
      merge_inputs_.reset();
      row_heap_.reset();
      // no need to reset interrupt_proc_
      ObPxReceiveCtx::destroy();
    }
    int create_ra_row_store(uint64_t tenant_id, ObRARowStore*& row_store);
    OB_INLINE virtual ObPxNewRow* get_px_new_row()
    {
      return ptr_px_row_;
    }
    OB_INLINE virtual dtl::ObDtlChannelLoop* get_dtl_channel_loop()
    {
      return ptr_row_msg_loop_;
    }
    OB_INLINE virtual int64_t get_channel_count()
    {
      return task_channels_.count();
    }

  private:
    ObPxNewRow* ptr_px_row_;
    dtl::ObDtlChannelLoop* ptr_row_msg_loop_;
    ObPxInterruptP interrupt_proc_;
    ObRowHeap<> row_heap_;

    // every merge sort inputs, the number of merge sort inputs may be different from channels
    common::ObArray<MergeSortInput*> merge_inputs_;

    // save data from every channel(1:1) in local order
    common::ObArray<ObRARowStore*> row_stores_;
    bool finish_;
  };

public:
  explicit ObPxMergeSortReceive(common::ObIAllocator& alloc, bool local_order = false)
      : ObPxReceive(alloc), ObSortableTrait(alloc), local_order_(local_order)
  {}
  virtual ~ObPxMergeSortReceive() = default;
  virtual void set_local_order(bool local_order)
  {
    local_order_ = local_order;
  }

protected:
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_merge_sort_input(ObExecContext& ctx, ObPxMergeSortReceiveCtx* recv_ctx, int64_t n_channel) const;
  virtual int release_merge_inputs(ObExecContext& ctx, ObPxMergeSortReceiveCtx* recv_ctx) const;

private:
  int get_one_row_from_channels(ObPxMergeSortReceiveCtx* recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx,
      int64_t channel_idx,  // row heap require data from the channel_idx channel
      const common::ObNewRow*& in_row) const;
  int get_all_rows_from_channels(ObPxMergeSortReceiveCtx& recv_ctx, ObPhysicalPlanCtx* phy_plan_ctx) const;
  int try_link_channel(ObExecContext& ctx) const override;

private:
  static const int64_t MAX_INPUT_NUMBER = 10000L;
  bool local_order_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQ_OB_PX_MERGE_SORT_RECEIVE_H_ */
//// end of header file
