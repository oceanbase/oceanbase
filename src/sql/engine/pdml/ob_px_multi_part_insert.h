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

#ifndef OB_PX_MULTI_PART_INSERT_H_
#define OB_PX_MULTI_PART_INSERT_H_

#include "lib/allocator/ob_allocator.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/pdml/ob_batch_row_cache.h"
#include "sql/engine/pdml/ob_pdml_data_driver.h"
#include "sql/engine/pdml/ob_px_multi_part_modify.h"

namespace oceanbase {
namespace sql {

class ObPxMultiPartInsertInput : public ObPxModifyInput {
  OB_UNIS_VERSION_V(1);

public:
  virtual inline ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_MULTI_PART_INSERT;
  }
};

class ObTableLocation;
class ObPxMultiPartInsert : public ObDMLDataReader, public ObDMLDataWriter, public ObTableModify {
  OB_UNIS_VERSION(1);

private:
  class ObPxMultiPartInsertCtx;
  class ObPDMLRowIteratorWrapper : public common::ObNewRowIterator {
  public:
    ObPDMLRowIteratorWrapper(ObPxMultiPartInsertCtx& op_ctx)
        : op_ctx_(op_ctx), iter_(nullptr), insert_projector_(nullptr), insert_projector_size_(0), insert_row_()
    {}
    virtual ~ObPDMLRowIteratorWrapper() = default;

    void set_insert_projector(int32_t* projector, int64_t projector_size)
    {
      insert_projector_ = projector;
      insert_projector_size_ = projector_size;
    }
    int alloc_insert_row();
    void set_iterator(ObPDMLRowIterator& iter)
    {
      iter_ = &iter;
    }
    int get_next_row(common::ObNewRow*& row) override;
    void reset() override
    {}

  private:
    int project_row(const ObNewRow& input_row, ObNewRow& output_row) const;

  private:
    ObPxMultiPartInsertCtx& op_ctx_;
    ObPDMLRowIterator* iter_;
    int32_t* insert_projector_;
    int64_t insert_projector_size_;
    common::ObNewRow insert_row_;
  };
  class ObPxMultiPartInsertCtx : public ObTableModifyCtx {
  public:
    explicit ObPxMultiPartInsertCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx), data_driver_(op_monitor_info_), row_iter_wrapper_(*this)
    {}
    ~ObPxMultiPartInsertCtx() = default;
    virtual void destroy() override
    {
      ObTableModifyCtx::destroy();
    }
    ObPDMLDataDriver data_driver_;
    ObPDMLRowIteratorWrapper row_iter_wrapper_;
  };

public:
  explicit ObPxMultiPartInsert(common::ObIAllocator& alloc);
  virtual ~ObPxMultiPartInsert();

  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int create_operator_input(ObExecContext& ctx) const override;
  virtual bool is_pdml_operator() const
  {
    return true;
  }
  int read_row(ObExecContext& ctx, const common::ObNewRow*& row, int64_t& part_id) const override;
  // impl. ObDMLDataWriter
  int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLRowIterator& iterator) const override;

  ObDMLRowDesc& get_dml_row_desc()
  {
    return row_desc_;
  }
  ObDMLTableDesc& get_dml_table_desc()
  {
    return table_desc_;
  }

  void set_insert_projector(int32_t* projector, int64_t projector_size)
  {
    insert_projector_ = projector;
    insert_projector_size_ = projector_size;
  }
  void set_with_barrier(bool w)
  {
    with_barrier_ = w;
  }

private:
  int fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session, const ObPhysicalPlan& my_phy_plan,
      const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const;
  int process_row(ObExecContext& ctx, ObPxMultiPartInsertCtx* insert_ctx, const ObNewRow*& insert_row) const;

private:
  ObDMLRowDesc row_desc_;
  ObDMLTableDesc table_desc_;
  int32_t* insert_projector_;
  int64_t insert_projector_size_;
  bool with_barrier_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartInsert);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OB_PX_MULTI_PART_INSERT_H_ */
