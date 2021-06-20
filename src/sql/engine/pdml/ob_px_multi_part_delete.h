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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/pdml/ob_batch_row_cache.h"
#include "sql/engine/pdml/ob_pdml_data_driver.h"
#include "sql/engine/pdml/ob_px_multi_part_modify.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
}
namespace storage {
class ObDMLBaseParam;
}
namespace sql {

class ObPxMultiPartDeleteInput : public ObPxModifyInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxMultiPartDeleteInput() = default;
  virtual ~ObPxMultiPartDeleteInput() = default;
  virtual inline ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_MULTI_PART_DELETE;
  }
};

class ObPxMultiPartDelete : public ObDMLDataReader, public ObDMLDataWriter, public ObTableModify {
  OB_UNIS_VERSION(1);

private:
  class ObPxMultiPartDeleteCtx;

  /**ObPDMLRowIteratorWrapper**/
  class ObPDMLRowIteratorWrapper : public common::ObNewRowIterator {
  public:
    ObPDMLRowIteratorWrapper(ObPxMultiPartDeleteCtx& op_ctx)
        : op_ctx_(op_ctx), iter_(nullptr), delete_projector_(nullptr), delete_projector_size_(0), delete_row_()
    {}
    virtual ~ObPDMLRowIteratorWrapper() = default;

    void set_delete_projector(int32_t* projector, int64_t projector_size)
    {
      delete_projector_ = projector;
      delete_projector_size_ = projector_size;
    }

    int alloc_delete_row();

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
    ObPxMultiPartDeleteCtx& op_ctx_;
    ObPDMLRowIterator* iter_;
    int32_t* delete_projector_;
    int64_t delete_projector_size_;
    common::ObNewRow delete_row_;
  };
  /**ObPxMultiPartDeleteCtx**/
  class ObPxMultiPartDeleteCtx : public ObTableModifyCtx {
  public:
    ObPxMultiPartDeleteCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx), data_driver_(op_monitor_info_), row_iter_wrapper_(*this)
    {}
    ~ObPxMultiPartDeleteCtx() = default;
    ObPDMLDataDriver data_driver_;
    // Used to calc the child output row and return the row required by the DML operation
    ObPDMLRowIteratorWrapper row_iter_wrapper_;
  };

public:
  explicit ObPxMultiPartDelete(common::ObIAllocator& alloc);
  ~ObPxMultiPartDelete();

public:
  virtual int create_operator_input(ObExecContext& ctx) const override;
  virtual bool has_foreign_key() const override
  {
    return false;
  }
  virtual bool is_pdml_operator() const
  {
    return true;
  }
  // impl. ObDMLDataReader
  // Read a row of data buffer from child op to ObPxMultiPartDelete operator
  // at the same time, it is also responsible for calculating the partition_id
  // corresponding to this row
  int read_row(ObExecContext& ctx, const common::ObNewRow*& row, int64_t& part_id) const override;
  // impl. ObDMLDataWriter
  int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLRowIterator& iterator) const override;

  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;

  ObDMLRowDesc& get_dml_row_desc();
  ObDMLTableDesc& get_dml_table_desc();
  void set_pdml_delete_projector(int* projector, int projector_size)
  {
    delete_projector_ = projector;
    delete_projector_size_ = projector_size;
  }
  void set_with_barrier(bool w)
  {
    with_barrier_ = w;
  }
  int register_to_datahub(ObExecContext& ctx) const override;

private:
  int fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session, const ObPhysicalPlan& my_phy_plan,
      const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const;

private:
  ObDMLRowDesc row_desc_;
  ObDMLTableDesc table_desc_;
  int32_t* delete_projector_;
  int64_t delete_projector_size_;
  // Due to the existence of update row movement, the update plan will expand into
  //  INSERT
  //    DELETE
  //
  // insert needs to wait for all delete operators to be executed before they can output data
  bool with_barrier_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartDelete);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_H_ */
//// end of header file
