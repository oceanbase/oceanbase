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

#ifndef _OB_MV_TABLE_SCAN_H
#define _OB_MV_TABLE_SCAN_H 1

#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_table_scan_op.h"

namespace oceanbase {
namespace sql {

class ObMVTableScanInput : public ObTableScanInput {
public:
  virtual ObPhyOperatorType get_phy_op_type() const;
};

class ObMVTableScan : public ObTableScan {
  OB_UNIS_VERSION_V(1);

protected:
  class ObMVTableScanCtx : public ObTableScanCtx {
    friend class ObMVTableScan;

  public:
    ObMVTableScanCtx(ObExecContext& ctx) : ObTableScanCtx(ctx)
    {}
    virtual void destroy()
    {
      right_scan_param_.~ObTableScanParam();
      ObTableScanCtx::destroy();
    }

  protected:
    storage::ObTableScanParam right_scan_param_;
  };

public:
  explicit ObMVTableScan(common::ObIAllocator& allocator);
  virtual ~ObMVTableScan()
  {}

  virtual void reset();
  virtual void reuse();

  const share::schema::ObTableParam& get_right_table_param() const
  {
    return right_table_param_;
  }
  share::schema::ObTableParam& get_right_table_param()
  {
    return right_table_param_;
  }
  void set_right_table_location_key(uint64_t right_table_location_key)
  {
    right_table_location_key_ = right_table_location_key;
  }
  uint64_t get_right_table_location_key() const
  {
    return right_table_location_key_;
  }

  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int prepare_scan_param(ObExecContext& ctx) const;

protected:
  int do_table_scan(ObExecContext& ctx, bool is_rescan) const;

private:
  share::schema::ObTableParam right_table_param_;
  uint64_t right_table_location_key_;
};

// TODO : not implemented right row, Added here to adapt the template.
class ObMVTableScanSpec : public ObTableScanSpec {
public:
  uint64_t get_right_table_location_key() const
  {
    return 0;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_MV_TABLE_SCAN_H */
