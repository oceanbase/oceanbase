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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_PLAN_CACHE_PLAN_EXPLAIN_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_PLAN_CACHE_PLAN_EXPLAIN_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace observer
{
class ObExpVisitor
{
public:
  ObExpVisitor(common::ObIArray<uint64_t> &output_column_ids,
                common::ObNewRow &cur_row,
                common::ObScanner &scanner)
  : output_column_ids_(output_column_ids),
    cur_row_(cur_row),
    scanner_(scanner),
    tenant_id_(common::OB_INVALID_ID),
    plan_id_(common::OB_INVALID_ID),
    allocator_() {}

  template<class Op>
  int add_row(const Op &cur_op);

  template<class Op>
  int get_table_name(const Op &cur_op, common::ObString &tbl_name);

  template<class Op>
  int get_property(const Op &cur_op, common::ObString &property);

  void set_effective_tenant_id(int64_t effective_tenant_id)
  {
    allocator_.set_attr(lib::ObMemAttr(effective_tenant_id, lib::ObLabel("RowMemAlloc")));
  }

  int init(const uint64_t tenant_id,
           const uint64_t plan_id)
  {
    tenant_id_ = tenant_id;
    plan_id_ = plan_id;
    return OB_SUCCESS;
  }
private:
  int get_table_access_desc(bool is_idx_access, const ObQueryFlag &scan_flag, ObString &tab_name, 
                            const ObString &index_name, ObString &ret_name);
protected:
  common::ObIArray<uint64_t> &output_column_ids_;
  common::ObNewRow &cur_row_;
  common::ObScanner &scanner_;
  int64_t tenant_id_;
  int64_t plan_id_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObExpVisitor);
};

class ObOpSpecExpVisitor: public sql::ObOpSpecVisitor, public ObExpVisitor
{
public:
  ObOpSpecExpVisitor(common::ObIArray<uint64_t> &output_column_ids,
                     common::ObNewRow &cur_row,
                     common::ObScanner &scanner)
    : ObExpVisitor(output_column_ids, cur_row, scanner) {}
  virtual ~ObOpSpecExpVisitor() {}
  int init(const uint64_t tenant_id,
           const uint64_t plan_id)
  {
    return ObExpVisitor::init(tenant_id, plan_id);
  }
  int add_row(const sql::ObOpSpec &spec);
  virtual int pre_visit(const sql::ObOpSpec &spec) override
  {
    return add_row(spec);
  }
  virtual int post_visit(const sql::ObOpSpec &spec) override
  {
    UNUSED(spec);
    return common::OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObOpSpecExpVisitor);
};

class ObCacheObjIterator
{
  public:
    ObCacheObjIterator(common::ObSEArray<uint64_t, 16> &tenant_id_array):
    tenant_id_array_(tenant_id_array),
    tenant_id_array_idx_(-1),
    plan_id_array_()
    {}

    int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry);

    int next(int64_t &tenant_id, ObCacheObjGuard &guard);

    common::ObSEArray<uint64_t, 16> &tenant_id_array_;
    int64_t tenant_id_array_idx_;
    common::ObSEArray<uint64_t, 16> plan_id_array_;
};

class ObPlanCachePlanExplain : public common::ObVirtualTableScannerIterator
{
public:
  enum {
    TENANT_ID_COL = 16,
    IP_COL,
    PORT_COL,
    PLAN_ID_COL,
    OP_NAME_COL,
    TBL_NAME_COL,
    ROWS_COL,
    COST_COL,
    PROPERTY_COL,
    PLAN_DEPTH_COL,
    PLAN_LINE_ID_COL,
  };
  ObPlanCachePlanExplain()
    : tenant_id_(common::OB_INVALID_INDEX),
      plan_id_(common::OB_INVALID_INDEX),
      scan_all_plan_(true),
      tenant_id_array_(),
      tenant_id_array_idx_(0),
      cache_obj_iterator_(tenant_id_array_),
      iter_end_(false),
      static_engine_exp_visitor_(output_column_ids_,
                                 cur_row_,
                                 scanner_)
      {}
  virtual ~ObPlanCachePlanExplain();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int set_tenant_plan_id(const common::ObIArray<common::ObNewRange> &ranges);
  int64_t tenant_id_;
  int64_t plan_id_;
  bool scan_all_plan_;
  common::ObSEArray<uint64_t, 16> tenant_id_array_;
  int64_t tenant_id_array_idx_;
  ObCacheObjIterator cache_obj_iterator_;
  bool iter_end_;
  ObOpSpecExpVisitor static_engine_exp_visitor_;
  DISALLOW_COPY_AND_ASSIGN(ObPlanCachePlanExplain);
};
}
}

#endif