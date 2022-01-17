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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_MS_RECEIVE_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_MS_RECEIVE_OP_H_

#include "sql/engine/px/exchange/ob_receive_op.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/exchange/ob_row_heap.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"

namespace oceanbase {
namespace sql {

class ObPxMSReceiveOpInput : public ObPxReceiveOpInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxMSReceiveOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxMSReceiveOpInput()
  {}
};

class ObPxMSReceiveSpec : public ObPxReceiveSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMSReceiveSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  // [sort_exprs, output_exprs]The first is the sorting column, and the second is the receive output column
  ExprFixedArray all_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
  bool local_order_;
};

class ObPxMSReceiveOp : public ObPxReceiveOp {
public:
  ObPxMSReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxMSReceiveOp()
  {}

private:
  class MergeSortInput {
  public:
    MergeSortInput(ObChunkDatumStore* get_row_store, ObChunkDatumStore* add_row_ptr, bool finish)
        : get_row_store_(get_row_store), add_row_store_(add_row_ptr), finish_(finish), reader_()
    {}
    virtual ~MergeSortInput() = default;

    virtual int get_row(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx,
        const ObChunkDatumStore::StoredRow*& store_row) = 0;
    virtual int add_row(ObExecContext& ctx, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx) = 0;

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
    virtual void clean_row_store(ObExecContext& ctx) = 0;

    TO_STRING_KV(K_(finish));

  public:
    ObChunkDatumStore* get_row_store_;
    ObChunkDatumStore* add_row_store_;
    bool finish_;
    ObChunkDatumStore::Iterator reader_;
  };

  // Globally ordered, which means that the incoming data of each channel of merge sort receive is in order.
  // Just merge and sort all the roads
  // Each channel corresponds to a GlobalOrderInput,
  // and each channel will cache data to solve the stuck phenomenon caused by current limiting.
  // At the same time, in order to reduce the cached data,
  // add and get data back and forth through the two row stores to reduce the amount of buffer data
  // That is, get_row_store_ to spit out data, and add_row_store_ to get channel data,
  // as long as get_row_store_ is all spit out, clear get_row_store_ data after reaching a certain threshold,
  // and switch add_row_store_ to get_row_store_ , get_row_store_ to add_row_store_
  // Switch the add and get of the data back and forth like this
  class GlobalOrderInput : public MergeSortInput {
  public:
    GlobalOrderInput(uint64_t tenant_id)
        : MergeSortInput(nullptr, nullptr, false), get_reader_(), add_row_reader_(nullptr), get_row_reader_(nullptr)
    {
      tenant_id_ = tenant_id;
    }
    virtual ~GlobalOrderInput()
    {
      destroy();
    }

    virtual int get_row(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, const ObChunkDatumStore::StoredRow*& store_row);
    virtual int add_row(ObExecContext& ctx, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx);

    virtual int64_t max_pos();
    virtual void destroy();
    virtual void clean_row_store(ObExecContext& ctx);
    virtual bool is_empty();

  private:
    int create_chunk_datum_store(ObExecContext& ctx, uint64_t tenant_id, ObChunkDatumStore*& row_store);
    virtual int reset_add_row_store(bool& reset);
    virtual int switch_get_row_store();
    int get_one_row_from_channels(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx);

  private:
    static const int64_t MAX_ROWS_PER_STORE = 50L;
    uint64_t tenant_id_;
    // Due to the need for two datum stores to switch back and forth,
    // in order to avoid emptying the data every time it cuts, start inserting again,
    // So you need two iterators to save the current reading position
    // eg:
    // reader1        reader2     step
    //  1               1          reader1(1) ->reader2(1) // read row 1 of reader1 first, then read row 2 of reader2
    //  2               2          reader1(2) ->reader2(2)
    //  4               3          reader2(3) ->reader1(4)
    // The reader of the default parent class here is add_reader
    ObChunkDatumStore::Iterator get_reader_;
    ObChunkDatumStore::Iterator* add_row_reader_;
    ObChunkDatumStore::Iterator* get_row_reader_;
  };

  // Partially ordered, which means that the input data of each channel of merge sort receive is locally ordered, that
  // is, segmented orderly The local order can be divided into more ways of order by segmentation, and then the merge
  // sort can be carried out. The main optimization is to optimize the sorting from all the previous data into more ways
  // of merge sorting. In this way, each channel may correspond to multiple LocalOrderInput, which is divided into
  // multiple ordered data segments, and each ordered segment uses LocalOrderInput to get and add At the same time, each
  // channel will have a row_store to cache all data, and LocalOrderInput will specify the range of its own order
  // segment [start_pos, end_pos). Then continue to pop the data according to the range
  class LocalOrderInput : public MergeSortInput {
  public:
    explicit LocalOrderInput() : MergeSortInput(nullptr, nullptr, false), datum_store_()
    {
      get_row_store_ = &datum_store_;
      add_row_store_ = &datum_store_;
    }

    virtual ~LocalOrderInput()
    {
      destroy();
    }
    virtual int get_row(ObPxMSReceiveOp* ms_receive_op, ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx,
        const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx, const ObChunkDatumStore::StoredRow*& store_row);
    virtual int add_row(ObExecContext& ctx, const common::ObIArray<ObExpr*>& exprs, ObEvalCtx& eval_ctx);
    virtual int64_t max_pos();
    virtual void destroy();
    virtual void clean_row_store(ObExecContext& ctx);
    int open();

  public:
    ObChunkDatumStore datum_store_;
  };

  class Compare {
  public:
    Compare();
    int init(const ObIArray<ObSortFieldCollation>* sort_collations, const ObIArray<ObSortCmpFunc>* sort_cmp_funs);

    bool operator()(const ObChunkDatumStore::StoredRow* l, const common::ObIArray<ObExpr*>* r, ObEvalCtx& eval_ctx);

    bool is_inited() const
    {
      return NULL != sort_collations_;
    }
    // interface required by ObBinaryHeap
    int get_error_code()
    {
      return ret_;
    }

    void reset()
    {
      this->~Compare();
      new (this) Compare();
    }

  public:
    int ret_;
    const ObIArray<ObSortFieldCollation>* sort_collations_;
    const ObIArray<ObSortCmpFunc>* sort_cmp_funs_;
    const common::ObIArray<const ObChunkDatumStore::StoredRow*>* rows_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Compare);
  };

  virtual int inner_open() override;
  virtual void destroy() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row();

  OB_INLINE virtual int64_t get_channel_count()
  {
    return task_channels_.count();
  }

private:
  int new_local_order_input(MergeSortInput*& out_msi);
  int get_all_rows_from_channels(ObPhysicalPlanCtx* phy_plan_ctx);
  int try_link_channel() override;
  int init_merge_sort_input(int64_t n_channel);
  int release_merge_inputs();
  int get_one_row_from_channels(ObPhysicalPlanCtx* phy_plan_ctx, int64_t channel_idx, const ObIArray<ObExpr*>& exprs,
      ObEvalCtx& eval_ctx, const ObChunkDatumStore::StoredRow*& store_row);

private:
  static const int64_t MAX_INPUT_NUMBER = 10000L;
  ObPxNewRow* ptr_px_row_;
  dtl::ObDtlChannelLoop* ptr_row_msg_loop_;
  ObPxInterruptP interrupt_proc_;
  ObRowHeap<ObDatumRowCompare, ObChunkDatumStore::StoredRow> row_heap_;

  // every merge sort inputs, the number of merge sort inputs may be different from channels
  common::ObArray<MergeSortInput*> merge_inputs_;
  bool finish_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_RECEIVE_OP_H_
