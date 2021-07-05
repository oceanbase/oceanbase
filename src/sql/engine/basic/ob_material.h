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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"

namespace oceanbase {
namespace sql {

/* only for adapt deserialize between 22x and 31x
 * from 22x to 31x: only deserialize MaterialInput
 * from 31x to 22x: no 2+dfo schedule in 22x, just affect performance
 */
class ObMaterialInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObMaterialInput() : bypass_(false)
  {}
  virtual ~ObMaterialInput() = default;
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void reset() override
  {
    bypass_ = false;
  }
  void set_bypass(bool bypass)
  {
    bypass_ = bypass;
  }
  int64_t is_bypass() const
  {
    return bypass_;
  }

protected:
  bool bypass_;
};

class ObExecContext;
class ObMaterial : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObMaterial(common::ObIAllocator& alloc) : ObSingleChildPhyOperator(alloc)
  {}
  virtual ~ObMaterial()
  {}
  void reset();
  void reuse();
  int rescan(ObExecContext& exec_ctx) const;

  int get_material_row_count(ObExecContext& exec_ctx, int64_t& row_count) const;

private:
  class ObMaterialCtx : public ObPhyOperatorCtx {
    friend class ObMaterial;

  public:
    explicit ObMaterialCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx),
          mem_context_(nullptr),
          row_id_(0),
          profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
          sql_mem_processor_(profile_),
          is_first_(false)
    {}
    virtual ~ObMaterialCtx()
    {}
    virtual void destroy()
    {
      row_store_.reset();
      destroy_mem_entiry();
      ObPhyOperatorCtx::destroy_base();
    }

    void destroy_mem_entiry()
    {
      if (nullptr != mem_context_) {
        DESTROY_CONTEXT(mem_context_);
        mem_context_ = nullptr;
      }
    }

    bool need_dump()
    {
      return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound();
    }

  private:
    lib::MemoryContext* mem_context_;
    ObChunkRowStore row_store_;
    ObChunkRowStore::Iterator row_store_it_;
    int64_t row_id_;
    friend class ObValues;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
    bool is_first_;
  };

private:
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& exec_ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& exec_ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;

  int get_all_row_from_child(ObMaterialCtx& mat_ctx, ObSQLSessionInfo& session) const;
  int process_dump(ObMaterialCtx& mat_ctx) const;

private:
  // no data member
private:
  DISALLOW_COPY_AND_ASSIGN(ObMaterial);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_H_ */
