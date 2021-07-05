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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_LOCK_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_LOCK_H_
#include "sql/engine/dml/ob_table_modify.h"
namespace oceanbase {
namespace sql {

class ObTableLockInput : public ObTableModifyInput {
  friend class ObTableLock;

public:
  ObTableLockInput() : ObTableModifyInput()
  {}
  virtual ~ObTableLockInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_LOCK;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLockInput);
};

class ObTableLock : public ObTableModify {
public:
  class ObTableLockCtx : public ObTableModifyCtx {
    friend class ObTableLock;

  public:
    explicit ObTableLockCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx), lock_row_(), dml_param_(), part_key_(), part_infos_(), for_update_wait_timeout_(-1)
    {}
    ~ObTableLockCtx()
    {}
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }
    friend class ObTableLock;

  protected:
    common::ObNewRow lock_row_;
    storage::ObDMLBaseParam dml_param_;
    common::ObPartitionKey part_key_;
    common::ObSEArray<DMLPartInfo, 4> part_infos_;
    int64_t for_update_wait_timeout_;
  };

  OB_UNIS_VERSION(1);

public:
  explicit ObTableLock(common::ObIAllocator& alloc);
  virtual ~ObTableLock();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const override;
  void set_rowkey_projector(int32_t* rowkey_projector, int64_t rowkey_projector_size)
  {
    rowkey_projector_ = rowkey_projector;
    rowkey_projector_size_ = rowkey_projector_size;
  }
  void set_wait_time(int64_t for_update_wait_us)
  {
    for_update_wait_us_ = for_update_wait_us;
  }
  void set_skip_locked(bool skip)
  {
    skip_locked_ = skip;
  }
  virtual bool is_dml_without_output() const
  {
    return false;
  }

protected:
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

  int do_table_lock(ObExecContext& ctx) const;
  int lock_single_part(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int lock_multi_part(ObExecContext& ctx) const;

  int build_lock_row(ObNewRow& lock_row, const ObNewRow& row) const;

  bool is_skip_locked() const
  {
    return skip_locked_;
  }

  bool is_nowait() const
  {
    return for_update_wait_us_ == 0;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLock);

protected:
  // projector for build the lock row
  int32_t* rowkey_projector_;
  int64_t rowkey_projector_size_;
  int64_t for_update_wait_us_;
  bool skip_locked_;  // UNUSED for the current
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_DML_OB_TABLE_LOCK_H_
