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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOCAL_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOCAL_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_lookup_iter.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASLocalLookupIterParam : public ObDASLookupIterParam
{
public:
  ObDASLocalLookupIterParam()
    : ObDASLookupIterParam(false /*local lookup*/),
      trans_desc_(nullptr),
      snapshot_(nullptr)
  {}
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  virtual bool is_valid() const override
  {
    return true;
  }
};

class ObDASScanCtDef;
class ObDASScanRtDef;
class ObDASLocalLookupIter : public ObDASLookupIter
{
public:
  ObDASLocalLookupIter()
    : ObDASLookupIter(ObDASIterType::DAS_ITER_LOCAL_LOOKUP),
      trans_info_array_(),
      lookup_param_(),
      lookup_tablet_id_(),
      lookup_ls_id_(),
      trans_desc_(nullptr),
      snapshot_(nullptr),
      is_first_lookup_(true)
  {}
  virtual ~ObDASLocalLookupIter() {}

  storage::ObTableScanParam &get_lookup_param() { return lookup_param_; }
  void set_tablet_id(const ObTabletID &tablet_id) { lookup_tablet_id_ = tablet_id; }
  void set_ls_id(const share::ObLSID &ls_id) { lookup_ls_id_ = ls_id; }
  int init_scan_param(storage::ObTableScanParam &param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef);

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void reset_lookup_state();

  virtual int add_rowkey() override;
  virtual int add_rowkeys(int64_t count) override;
  virtual int do_index_lookup() override;
  virtual int check_index_lookup() override;
private:
  int init_rowkey_exprs_for_compat();

private:
  ObSEArray<ObDatum *, 4> trans_info_array_;
   // Local lookup das task could rescan multiple times during execution, lookup_tablet_id_ and
   // lookup_ls_id_ store the lookup parameter for this time.
  storage::ObTableScanParam lookup_param_;
  ObTabletID lookup_tablet_id_;
  share::ObLSID lookup_ls_id_;
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool is_first_lookup_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_ */
