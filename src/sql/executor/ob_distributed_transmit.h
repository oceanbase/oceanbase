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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TRANSMIT_
#define OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TRANSMIT_

#include "share/schema/ob_table_schema.h"
#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_task_event.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObDistributedTransmitInput : public ObTransmitInput {
  OB_UNIS_VERSION_V(1);

public:
  ObDistributedTransmitInput()
      : ObTransmitInput(), expire_time_(0), ob_task_id_(), force_save_interm_result_(false), slice_events_(NULL)
  {}
  virtual ~ObDistributedTransmitInput()
  {}
  virtual void reset() override
  {
    ObTransmitInput::reset();
    expire_time_ = 0;
    ob_task_id_.reset();
    force_save_interm_result_ = false;
    slice_events_ = NULL;
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  inline virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DISTRIBUTED_TRANSMIT;
  }

  inline void set_ob_task_id(const ObTaskID& id)
  {
    ob_task_id_ = id;
  }
  inline ObTaskID& get_ob_task_id()
  {
    return ob_task_id_;
  }
  inline bool is_force_save_interm_result() const
  {
    return force_save_interm_result_;
  }
  inline void set_slice_events(common::ObIArray<ObSliceEvent>* slice_events)
  {
    slice_events_ = slice_events;
  }
  inline common::ObIArray<ObSliceEvent>* get_slice_events_for_update() const
  {
    return slice_events_;
  }

  inline int64_t get_expire_time()
  {
    return expire_time_;
  }

private:
  int64_t expire_time_;
  ObTaskID ob_task_id_;
  bool force_save_interm_result_;
  common::ObIArray<ObSliceEvent>* slice_events_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedTransmitInput);
};

class ObDistributedTransmit : public ObTransmit {
  OB_UNIS_VERSION_V(1);

private:
  class ObSliceInfo {
  public:
    ObSliceInfo()
        : part_offset_(OB_INVALID_INDEX_INT64),
          subpart_offset_(OB_INVALID_INDEX_INT64),
          part_idx_(OB_INVALID_INDEX_INT64),
          subpart_idx_(OB_INVALID_INDEX_INT64),
          slice_idx_(OB_INVALID_INDEX_INT64)
    {}
    virtual ~ObSliceInfo() = default;
    TO_STRING_KV(K(part_offset_), K(subpart_offset_), K(part_idx_), K(subpart_idx_), K(slice_idx_))
    int64_t part_offset_;
    int64_t subpart_offset_;
    int64_t part_idx_;
    int64_t subpart_idx_;
    int64_t slice_idx_;
  };

protected:
  class ObDistributedTransmitCtx : public ObTransmitCtx {
    friend class ObDistributedTransmit;

  public:
    explicit ObDistributedTransmitCtx(ObExecContext& ctx) : ObTransmitCtx(ctx)
    {}
    virtual ~ObDistributedTransmitCtx()
    {}
    virtual void destroy()
    {
      ObTransmitCtx::destroy();
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObDistributedTransmitCtx);
  };

public:
  explicit ObDistributedTransmit(common::ObIAllocator& alloc);
  virtual ~ObDistributedTransmit();

  virtual int create_operator_input(ObExecContext& ctx) const override;
  inline void set_shuffle_func(ObSqlExpression* shuffle_func);
  int get_part_shuffle_key(
      const share::schema::ObTableSchema* table_schema, int64_t part_idx, ObShuffleKey& part_shuffle_key) const;
  int get_subpart_shuffle_key(const share::schema::ObTableSchema* table_schema, int64_t part_idx, int64_t subpart_idx,
      ObShuffleKey& subpart_shuffle_key) const;
  int get_shuffle_part_key(const share::schema::ObTableSchema* table_schema, int64_t part_idx, int64_t subpart_idx,
      common::ObPartitionKey& shuffle_part_key) const;

private:
  int init_slice_infos(
      const share::schema::ObTableSchema& table_schema, common::ObIArray<ObSliceInfo>& slices_info) const;
  int get_slice_idx(ObExecContext& exec_ctx, const share::schema::ObTableSchema* table_schema,
      const common::ObNewRow* row, const ObSqlExpression& part_partition_func,
      const ObSqlExpression& subpart_partition_func, const ObIArray<ObTransmitRepartColumn>& repart_columns,
      const ObIArray<ObTransmitRepartColumn>& repart_sub_columns, int64_t slices_count, int64_t& slice_idx,
      bool& no_match_partiton) const;

protected:
  virtual int inner_open(ObExecContext& exec_ctx) const override;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const override;
  bool skip_empty_slice() const;
  int prepare_interm_result(ObIntermResultManager& interm_result_mgr, ObIntermResult*& interm_result) const;
  int get_next_row(ObExecContext& ctx, const ObNewRow*& row) const override;
  int inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const override;

private:
  const static int64_t NO_MATCH_PARTITION = -2;
  ObSqlExpression* shuffle_func_;

  DISALLOW_COPY_AND_ASSIGN(ObDistributedTransmit);
};

inline void ObDistributedTransmit::set_shuffle_func(ObSqlExpression* shuffle_func)
{
  shuffle_func_ = shuffle_func;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_DISTRIBUTED_TRANSMIT_ */
