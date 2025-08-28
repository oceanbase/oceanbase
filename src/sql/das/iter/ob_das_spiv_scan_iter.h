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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_SCAN_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_SCAN_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/engine/expr/ob_expr_vector.h"
#include "ob_das_vec_scan_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObDASSPIVScanIterParam : public ObDASIterParam
{
public:
  ObDASSPIVScanIterParam()
    : ObDASIterParam(ObDASIterType::DAS_ITER_SPIV_SCAN),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      scan_iter_(nullptr),
      scan_ctdef_(nullptr),
      scan_rtdef_(nullptr), 
      dim_(UINT_MAX32),
      dim_docid_value_tablet_id_(ObTabletID::INVALID_TABLET_ID) {}

  virtual bool is_valid() const override
  {
    return ls_id_.is_valid() &&
           nullptr != tx_desc_ &&
           nullptr != snapshot_ && 
           nullptr != scan_iter_ && 
           dim_docid_value_tablet_id_.is_valid();    
  }

  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *scan_iter_;
  const ObDASScanCtDef *scan_ctdef_;
  ObDASScanRtDef *scan_rtdef_;
  uint32_t dim_;
  ObTabletID dim_docid_value_tablet_id_;
};

class ObDASSPIVScanIter : public ObDASIter
{
public:
    ObDASSPIVScanIter()
    : ObDASIter(ObDASIterType::DAS_ITER_SPIV_SCAN),
      mem_context_(nullptr),
      ls_id_(),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      scan_iter_(nullptr),
      scan_ctdef_(nullptr),
      scan_rtdef_(nullptr),
      scan_iter_param_(),
      dim_docid_value_tablet_id_(ObTabletID::INVALID_TABLET_ID),
      is_first_scan_(true),
      dim_(UINT32_MAX),
      is_inited_(false)
{
}
  
  virtual ~ObDASSPIVScanIter() {}
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  int build_range(ObNewRange &range, uint64_t table_id);

  void set_ls_tablet_id(const share::ObLSID &ls_id, const ObTabletID &related_tablet_id) 
  {
    ls_id_ = ls_id;
    dim_docid_value_tablet_id_ = related_tablet_id;
  }

protected: 
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  lib::MemoryContext mem_context_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;

  ObDASScanIter *scan_iter_;

  const ObDASScanCtDef *scan_ctdef_;
  ObDASScanRtDef *scan_rtdef_;

  ObTableScanParam scan_iter_param_;

  ObTabletID dim_docid_value_tablet_id_;

  bool is_first_scan_;
  uint32_t dim_;
  bool is_inited_;

};


}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_SPIV_SCAN_ITER_H_ */
