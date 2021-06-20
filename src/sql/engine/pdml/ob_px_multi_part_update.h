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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_H_

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

class ObPxMultiPartUpdateInput : public ObPxModifyInput {
  OB_UNIS_VERSION_V(1);

public:
  virtual inline ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_MULTI_PART_UPDATE;
  }
};

class ObPxMultiPartUpdate : public ObDMLDataReader,
                            public ObDMLDataWriter,
                            public ObTableModify,
                            public ObDMLRowChecker {
  OB_UNIS_VERSION(1);

public:
private:
  class ObPxMultiPartUpdateCtx;
  class ObPDMLRowIteratorWrapper : public common::ObNewRowIterator {
  public:
    ObPDMLRowIteratorWrapper(ObPxMultiPartUpdateCtx& op_ctx)
        : op_ctx_(op_ctx),
          iter_(nullptr),
          old_projector_(nullptr),
          old_projector_size_(0),
          updated_projector_(nullptr),
          updated_projector_size_(0),
          has_got_old_row_(false),
          old_row_(),
          new_row_()
    {}
    virtual ~ObPDMLRowIteratorWrapper() = default;

    void set_updated_projector(int32_t* projector, int64_t projector_size)
    {
      updated_projector_ = projector;
      updated_projector_size_ = projector_size;
    }
    void set_old_projector(int32_t* projector, int64_t projector_size)
    {
      old_projector_ = projector;
      old_projector_size_ = projector_size;
    }
    void set_dml_row_checker(const ObDMLRowChecker& checker)
    {
      row_checker_ = &checker;
    }
    int init(ObPDMLRowIterator& iter);
    int get_next_row(common::ObNewRow*& row) override;
    void reset() override
    {}

  private:
    int project_old_and_new_row(const ObNewRow& full_row, ObNewRow& old_row, ObNewRow& new_row) const;

  private:
    ObPxMultiPartUpdateCtx& op_ctx_;
    ObPDMLRowIterator* iter_;
    int32_t* old_projector_;
    int64_t old_projector_size_;
    int32_t* updated_projector_;
    int64_t updated_projector_size_;

    // Update spit out the old line first, and then the new line.
    // Generate old rows and new rows at one time during implementation, and then
    // Cache the information of the new line and return to the next iteration
    bool has_got_old_row_;
    common::ObNewRow old_row_;
    common::ObNewRow new_row_;
    const ObDMLRowChecker* row_checker_;
  };

  class ObPxMultiPartUpdateCtx : public ObTableModifyCtx {
  public:
    ObPxMultiPartUpdateCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx), data_driver_(op_monitor_info_), row_iter_wrapper_(*this)
    {}
    ~ObPxMultiPartUpdateCtx() = default;
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
    }
    // ObBatchRowCache cache_;
    ObPDMLDataDriver data_driver_;
    ObPDMLRowIteratorWrapper row_iter_wrapper_;
  };

public:
  explicit ObPxMultiPartUpdate(common::ObIAllocator& alloc);
  ~ObPxMultiPartUpdate();

public:
  virtual int create_operator_input(ObExecContext& ctx) const override;
  virtual bool is_pdml_operator() const
  {
    return true;
  }
  // impl. ObDMLDataReader
  int read_row(ObExecContext& ctx, const common::ObNewRow*& row, int64_t& part_id) const override;
  int write_rows(ObExecContext& ctx, common::ObPartitionKey& pkey, ObPDMLRowIterator& iterator) const override;

  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;

  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count);
  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp);
  void set_updated_projector(int32_t* projector, int64_t projector_size);
  void set_old_projector(int32_t* projector, int64_t projector_size);
  inline const int32_t* get_old_projector() const
  {
    return old_projector_;
  }
  inline int64_t get_old_projector_size() const
  {
    return old_projector_size_;
  }
  // misc
  ObDMLRowDesc& get_dml_row_desc()
  {
    return row_desc_;
  }
  ObDMLTableDesc& get_dml_table_desc()
  {
    return table_desc_;
  }

private:
  int fill_dml_base_param(uint64_t index_tid, ObSQLSessionInfo& my_session, const ObPhysicalPlan& my_phy_plan,
      const ObPhysicalPlanCtx& my_plan_ctx, storage::ObDMLBaseParam& dml_param) const;
  int on_process_new_row(ObExecContext& ctx, const common::ObNewRow& new_row) const;

private:
  /* functions */

  /* variables */
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  ObDMLRowDesc row_desc_;
  ObDMLTableDesc table_desc_;
  int32_t* old_projector_;
  int64_t old_projector_size_;
  int32_t* updated_projector_;
  int64_t updated_projector_size_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdate);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_H_ */
//// end of header file
