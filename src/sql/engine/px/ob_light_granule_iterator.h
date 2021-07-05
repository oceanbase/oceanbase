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

#ifndef OBDEV_SRC_SQL_ENGINE_PX_OB_LIGHT_GRANULE_ITERATOR_H_
#define OBDEV_SRC_SQL_ENGINE_PX_OB_LIGHT_GRANULE_ITERATOR_H_
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/rewrite/ob_query_range.h"

namespace oceanbase {
namespace sql {
class ObLightGranuleIterator;
class ObLGIInput : public ObIPhyOperatorInput {
  struct ObArrayParamInfo {
    OB_UNIS_VERSION(1);

  public:
    ObArrayParamInfo() : param_store_idx_(common::OB_INVALID_INDEX), array_param_()
    {}
    TO_STRING_KV(K_(param_store_idx), K_(array_param));
    int64_t param_store_idx_;
    common::ObSEArray<ObObj, 16> array_param_;
  };

  friend class ObLightGranuleIterator;
  friend class ObLGICtx;
  OB_UNIS_VERSION_V(1);

public:
  ObLGIInput() : location_idx_list_(), part_stmt_ids_(), deserialize_allocator_(nullptr)
  {}
  virtual ~ObLGIInput()
  {}
  virtual void reset() override
  {
    location_idx_list_.reset();
    param_array_list_.reset();
    part_stmt_ids_.reset();
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) override;
  virtual ObPhyOperatorType get_phy_op_type() const override;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator) override;
  int assign_pkeys(const common::ObIArray<common::ObPartitionKey>& pkeys);
  TO_STRING_KV(K_(location_idx_list), K_(param_array_list), K_(part_stmt_ids));

private:
  int init_param_array_list(ObPhysicalPlanCtx& plan_ctx, ObPhyTableLocation& table_loc, ObTaskInfo& task_info,
      const ObLightGranuleIterator& op);

private:
  ObFixedArray<uint64_t, common::ObIAllocator> location_idx_list_;
  // nested table param for every partition.
  common::ObSEArray<ObArrayParamInfo, 32> param_array_list_;
  // stmt_id for every partition.
  common::ObFixedArray<ObSEArray<int64_t, 32>, common::ObIAllocator> part_stmt_ids_;

private:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObLGIInput);
};
class ObLightGranuleIterator : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

private:
  enum ObGranuleIteratorState {
    LGI_UNINITIALIZED,
    LGI_PREPARED,
    LGI_TABLE_SCAN,
    LGI_GET_NEXT_GRANULE_TASK,
    LGI_END,
  };

public:
  struct ObLGIScanInfo {
    OB_UNIS_VERSION(1);

  public:
    ObLGIScanInfo()
        : ref_table_id_(common::OB_INVALID_ID),
          table_location_key_(common::OB_INVALID_ID),
          tsc_op_id_(common::OB_INVALID_ID)
    {}
    TO_STRING_KV(K_(ref_table_id), K_(table_location_key), K_(tsc_op_id));
    uint64_t ref_table_id_;
    uint64_t table_location_key_;
    uint64_t tsc_op_id_;
  };
  class ObLGICtx : public ObPhyOperatorCtx {
    friend class ObLightGranuleIterator;

  public:
    ObLGICtx(ObExecContext& exec_ctx)
        : ObPhyOperatorCtx(exec_ctx),
          cur_granule_pos_(0),
          cur_part_id_(-1),
          cur_param_idx_(0),
          state_(LGI_UNINITIALIZED)
    {}
    virtual ~ObLGICtx()
    {
      destroy();
    }
    virtual void destroy();
    bool is_not_init()
    {
      return state_ == LGI_UNINITIALIZED;
    }
    TO_STRING_KV(K_(cur_granule_pos), K_(cur_part_id), K_(cur_param_idx), K_(state));

  private:
    int64_t cur_granule_pos_;
    int64_t cur_part_id_;
    int64_t cur_param_idx_;
    ObGranuleIteratorState state_;
  };

public:
  explicit ObLightGranuleIterator(common::ObIAllocator& alloc);
  virtual ~ObLightGranuleIterator();
  virtual void reset() override;
  virtual void reuse() override;
  // basic infomation
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const override;
  virtual int rescan(ObExecContext& ctx) const override;
  virtual int init_op_ctx(ObExecContext& ctx) const override;
  virtual int inner_open(ObExecContext& ctx) const override;
  virtual int after_open(ObExecContext& ctx) const override;
  virtual int inner_close(ObExecContext& ctx) const override;
  virtual int inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const override;
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const override
  {
    UNUSED(ctx);
    return OPEN_SELF_FIRST;
  }
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLightGranuleIterator);

public:
  void set_dml_location_key(uint64_t table_location_key)
  {
    dml_location_key_ = table_location_key;
  }
  void set_dml_op_id(uint64_t dml_op_id)
  {
    dml_op_id_ = dml_op_id;
  }
  bool is_partition_wise_join() const
  {
    return is_pwj_;
  }
  void set_is_pwj(bool is_pwj)
  {
    is_pwj_ = is_pwj;
  }
  int init_lgi_scan_infos(int64_t scan_count)
  {
    return lgi_scan_infos_.init(scan_count);
  }
  int add_lgi_scan_info(const ObLGIScanInfo& lgi_scan_info)
  {
    return lgi_scan_infos_.push_back(lgi_scan_info);
  }
  int append_array_param_idxs(common::ObIArray<int64_t>& param_indexs)
  {
    return array_param_idxs_.assign(param_indexs);
  }
  inline const common::ObIArray<int64_t>& get_array_param_idxs() const
  {
    return array_param_idxs_;
  }

private:
  int try_fetch_task(ObExecContext& ctx, ObGranuleTaskInfo& info) const;
  int try_fetch_task_with_pwj(ObExecContext& ctx, common::ObIArray<ObGranuleTaskInfo>& lgi_tasks) const;
  int try_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int do_get_next_granule_task(ObLGICtx& lgi_ctx, bool is_prepare, ObExecContext& ctx) const;
  int prepare_table_scan(ObExecContext& ctx) const;
  int handle_batch_stmt_implicit_cursor(ObExecContext& ctx) const;
  static bool is_task_end(ObLGICtx& lgi_ctx, ObLGIInput& lgi_input);

private:
  uint64_t dml_location_key_;
  uint64_t dml_op_id_;
  bool is_pwj_;
  common::ObFixedArray<ObLGIScanInfo, common::ObIAllocator> lgi_scan_infos_;
  common::ObFixedArray<int64_t, common::ObIAllocator> array_param_idxs_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_PX_OB_LIGHT_GRANULE_ITERATOR_H_ */
