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

#ifndef OCEANBASE_SQL_EXECUTOR_TRANSMIT_OPERATOR_
#define OCEANBASE_SQL_EXECUTOR_TRANSMIT_OPERATOR_

#include "sql/executor/ob_job_conf.h"
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/px/ob_px_basic_info.h"

namespace oceanbase {
namespace sql {

class ObJob;
class ObTransmitInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTransmitInput() : ObIPhyOperatorInput(), job_(NULL)
  {}
  virtual ~ObTransmitInput()
  {}
  virtual void reset() override
  {}
  inline void set_job(ObJob* job)
  {
    job_ = job;
  }
  inline ObJob* get_job()
  {
    return job_;
  }

private:
  ObJob* job_;
};

typedef common::ObColumnInfo ObTransmitRepartColumn;

struct ObHashColumn : public common::ObColumnInfo {
  OB_UNIS_VERSION_V(1);

public:
  ObHashColumn() : expr_idx_(OB_INVALID_INDEX), cmp_type_(common::ObNullType)
  {}

  INHERIT_TO_STRING_KV("col_info", ObColumnInfo, K_(expr_idx), K_(cmp_type));

  int64_t expr_idx_;
  common::ObObjType cmp_type_;
};

class ObTransmit : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObTransmitCtx : public ObPhyOperatorCtx {
    friend class ObTransmit;

  public:
    explicit ObTransmitCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx)
    {}
    virtual ~ObTransmitCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
  };

public:
  explicit ObTransmit(common::ObIAllocator& alloc);
  virtual ~ObTransmit();
  // virtual int close(ObExecContext &ctx) const;
  void reset();
  void reuse();
  inline void set_interm_result_manager(ObIntermResultManager* result_mgr);
  inline void set_split_task_count(int64_t count);
  inline int64_t get_split_task_count() const;
  inline void set_parallel_server_count(int64_t count);
  inline int64_t get_parallel_server_count() const;
  inline void set_server_parallel_thread_count(int64_t count);
  inline int64_t get_server_parallel_thread_count() const;
  inline int set_repart_func(ObSqlExpression& expr, ObSqlExpression& sub_expr)
  {
    int ret = OB_SUCCESS;
    ret = repart_func_.assign(expr);
    if (OB_SUCCESS == ret)
      ret = repart_sub_func_.assign(sub_expr);
    return ret;
  }
  // for R/W job config
  ObJobConf& get_job_conf()
  {
    return job_conf_;
  }
  int add_compute(ObColumnExpression* expr);
  int add_filter(ObSqlExpression* expr);

  int init_repart_columns(int64_t repart_count, int64_t repart_sub_count);
  int add_repart_column(const int64_t column_index, ObCollationType cs_type);
  int add_repart_sub_column(const int64_t column_index, ObCollationType cs_type);
  inline void set_repartition_type(ObRepartitionType type)
  {
    repartition_type_ = type;
  }
  inline void set_repartition_table_id(int64_t table_id)
  {
    repartition_table_id_ = table_id;
  }
  int64_t get_repartition_table_id() const
  {
    return repartition_table_id_;
  }
  void need_repart(bool& with_part, bool& with_subpart) const;

  int init_hash_dist_columns(const int64_t count);
  int add_hash_dist_column(const bool is_expr, const int64_t idx, const common::ObObjMeta& cmp_type);

  int set_dist_exprs(ObIArray<ObSqlExpression*>& exprs);

  void set_dist_method(const ObPQDistributeMethod::Type method)
  {
    dist_method_ = method;
  }
  ObPQDistributeMethod::Type get_dist_method() const
  {
    return dist_method_;
  }

  void set_unmatch_row_dist_method(const ObPQDistributeMethod::Type method)
  {
    unmatch_row_dist_method_ = method;
  }
  ObPQDistributeMethod::Type get_unmatch_row_dist_method() const
  {
    return unmatch_row_dist_method_;
  }

  void set_px_dop(const int64_t dop)
  {
    px_dop_ = dop;
  }
  void set_px_single(const bool single)
  {
    px_single_ = single;
  }

  int64_t get_px_dop() const
  {
    return px_dop_;
  }
  bool is_px_single() const
  {
    return px_single_;
  }
  inline void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  inline void set_px_id(int64_t px_id)
  {
    px_id_ = px_id;
  }
  inline int64_t get_dfo_id() const
  {
    return dfo_id_;
  }
  inline int64_t get_px_id() const
  {
    return px_id_;
  }
  inline void set_has_lgi(bool has_lgi)
  {
    has_lgi_ = has_lgi;
  }
  inline bool has_lgi() const
  {
    return has_lgi_;
  }

  static int get_slice_idx_by_partition_ids(
      int64_t part_idx, int64_t subpart_idx, const share::schema::ObTableSchema& table_schema, int64_t& slice_idx);

  void set_slave_mapping_type(SlaveMappingType slave_mapping_type)
  {
    slave_mapping_type_ = slave_mapping_type;
  }
  SlaveMappingType get_slave_mapping_type() const
  {
    return slave_mapping_type_;
  }
  bool is_slave_mapping() const
  {
    return SlaveMappingType::SM_NONE != slave_mapping_type_;
  }

protected:
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const override;

protected:
  inline bool is_repart_exchange() const
  {
    return OB_REPARTITION_NO_REPARTITION != repartition_type_;
  }
  inline bool is_no_repart_exchange() const
  {
    return OB_REPARTITION_NO_REPARTITION == repartition_type_;
  }
  // more goes here
protected:
  int64_t split_task_count_;
  int64_t parallel_server_count_;
  int64_t server_parallel_thread_count_;

  // repart infos
  ObRepartitionType repartition_type_;
  int64_t repartition_table_id_;
  common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator> repart_columns_;
  common::ObFixedArray<ObTransmitRepartColumn, common::ObIAllocator> repart_sub_columns_;
  ObSqlExpression repart_func_;
  ObSqlExpression repart_sub_func_;

  int64_t px_dop_;

  bool px_single_;
  int64_t dfo_id_;
  int64_t px_id_;

  ObPQDistributeMethod::Type dist_method_;
  ObPQDistributeMethod::Type unmatch_row_dist_method_;
  common::ObFixedArray<ObHashColumn, common::ObIAllocator> hash_dist_columns_;
  common::ObFixedArray<ObSqlExpression*, common::ObIAllocator> dist_exprs_;
  SlaveMappingType slave_mapping_type_;
  ObJobConf job_conf_; /* job conf is shared btw threads */
  bool has_lgi_;
  int partition_id_idx_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTransmit);
};

inline void ObTransmit::set_split_task_count(int64_t count)
{
  if (OB_UNLIKELY(count <= 0)) {
    split_task_count_ = 1;
  } else {
    split_task_count_ = count;
  }
}

inline int64_t ObTransmit::get_split_task_count() const
{
  return split_task_count_;
}

inline void ObTransmit::set_parallel_server_count(int64_t count)
{
  if (OB_UNLIKELY(count <= 0)) {
    parallel_server_count_ = 1;
  } else {
    parallel_server_count_ = count;
  }
}

inline int64_t ObTransmit::get_parallel_server_count() const
{
  return parallel_server_count_;
}

inline void ObTransmit::set_server_parallel_thread_count(int64_t count)
{
  if (OB_UNLIKELY(count <= 0)) {
    server_parallel_thread_count_ = 1;
  } else {
    server_parallel_thread_count_ = count;
  }
}

inline int64_t ObTransmit::get_server_parallel_thread_count() const
{
  return server_parallel_thread_count_;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_TRANSMIT_OPERATOR_ */
//// end of header file
