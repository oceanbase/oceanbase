/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OBDEV_SRC_SQL_DAS_OB_DOMAIN_INDEX_LOOKUP_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_DOMAIN_INDEX_LOOKUP_OP_H_
#include "sql/das/ob_das_scan_op.h"
#include "storage/ob_store_row_comparer.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_access_service.h"
namespace oceanbase
{
namespace sql
{

class ObDomainRowkeyComp {
public:
  ObDomainRowkeyComp(int &sort_ret) : result_code_(sort_ret) {}

  bool operator()(const ObRowkey *left, const ObRowkey *right)
  {
    bool bool_ret = false;
    if (OB_UNLIKELY(common::OB_SUCCESS != result_code_)) {
      //do nothing
    } else if (OB_UNLIKELY(NULL == left)
              || OB_UNLIKELY(NULL == right)) {
      result_code_ = common::OB_INVALID_ARGUMENT;
      LOG_WARN_RET(result_code_, "Invaid argument, ", KP(left), KP(right), K_(result_code));
    } else {
      bool_ret = (*left) < (*right);
    }
    return bool_ret;
  }

  int &result_code_;
};

class ObDomainIndexLookupOp : public ObLocalIndexLookupOp
{
public:
  ObDomainIndexLookupOp(ObIAllocator &allocator) :
      ObLocalIndexLookupOp(),
      allocator_(&allocator),
      doc_id_scan_param_(),
      doc_id_lookup_ctdef_(nullptr),
      doc_id_lookup_rtdef_(nullptr),
      doc_id_idx_tablet_id_(),
      doc_id_expr_(nullptr),
      doc_id_key_obj_(),
      need_scan_aux_(false) {}

  virtual ~ObDomainIndexLookupOp()
  {
  }

  int init(const ObDASScanCtDef *lookup_ctdef,
           ObDASScanRtDef *lookup_rtdef,
           const ObDASScanCtDef *index_ctdef,
           ObDASScanRtDef *index_rtdef,
           const ObDASScanCtDef *doc_id_lookup_ctdef,
           ObDASScanRtDef *doc_id_lookup_rtdef,
           transaction::ObTxDesc *tx_desc,
           transaction::ObTxReadSnapshot *snapshot,
           storage::ObTableScanParam &scan_param);

  virtual int get_next_row() ;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  void set_doc_id_idx_tablet_id(const ObTabletID &doc_id_idx_tablet_id)
  { doc_id_idx_tablet_id_ = doc_id_idx_tablet_id; }

  virtual int revert_iter() override;
  virtual int reuse_scan_iter();
  ObITabletScan& get_tsc_service() { return *(MTL(ObAccessService *)); }
protected:
  virtual int init_scan_param() override { return ObLocalIndexLookupOp::init_scan_param(); }
protected:
  virtual int reset_lookup_state() override;
  virtual int next_state();
  virtual int init_sort() { return OB_SUCCESS; }
  // get index table rowkey, add rowkey as scan parameter of maintable / auxiliary lookup on demand
  virtual int fetch_index_table_rowkey() { return OB_NOT_IMPLEMENT; }
  virtual int fetch_index_table_rowkeys(int64_t &count, const int64_t capacity) { return OB_NOT_IMPLEMENT; }
  // get maintable rowkey for index lookup by from auxiliary index table on demand;
  virtual int get_aux_table_rowkey() { return OB_NOT_IMPLEMENT; }
  virtual int get_aux_table_rowkeys(const int64_t lookup_row_cnt) { return OB_NOT_IMPLEMENT; }

  virtual int do_aux_table_lookup() { return OB_SUCCESS; }
  virtual void do_clear_evaluated_flag();
  virtual int set_lookup_doc_id_key(ObExpr *doc_id_expr, ObEvalCtx *eval_ctx_);
  int set_doc_id_idx_lookup_param(
      const ObDASScanCtDef *aux_lookup_ctdef,
      ObDASScanRtDef *aux_lookup_rtdef,
      storage::ObTableScanParam& aux_scan_param,
      common::ObTabletID tablet_id_,
      share::ObLSID ls_id_);

protected:
  ObIAllocator *allocator_;
  storage::ObTableScanParam doc_id_scan_param_;
  const ObDASScanCtDef *doc_id_lookup_ctdef_;
  ObDASScanRtDef *doc_id_lookup_rtdef_;
  ObTabletID doc_id_idx_tablet_id_;
  ObExpr *doc_id_expr_;
  ObObj doc_id_key_obj_;

  bool need_scan_aux_;

  static const int64_t SORT_MEMORY_LIMIT = 48L * 1024L * 1024L;
  static const int64_t MAX_NUM_PER_BATCH = 1000;
};

class ObMulValueIndexLookupOp : public ObDomainIndexLookupOp
{
public:
  explicit ObMulValueIndexLookupOp(ObIAllocator &allocator)
    : ObDomainIndexLookupOp(allocator),
      cmp_ret_(0),
      aux_cmp_ret_(0),
      aux_key_count_(0),
      index_rowkey_cnt_(0),
      comparer_(cmp_ret_),
      aux_comparer_(aux_cmp_ret_),
      sorter_(allocator),
      aux_sorter_(allocator),
      aux_lookup_iter_(nullptr),
      last_rowkey_(),
      aux_last_rowkey_(),
      is_inited_(false) {}

  virtual ~ObMulValueIndexLookupOp()
  {
    sorter_.clean_up();
    sorter_.~ObExternalSort();

    aux_sorter_.clean_up();
    aux_sorter_.~ObExternalSort();
  }
  virtual void do_clear_evaluated_flag() override;
  int init(const ObDASBaseCtDef *table_lookup_ctdef,
           ObDASBaseRtDef *table_lookup_rtdef,
           transaction::ObTxDesc *tx_desc,
           transaction::ObTxReadSnapshot *snapshot,
           storage::ObTableScanParam &scan_param);
  int reuse_scan_iter(bool need_switch_param);
protected:
  virtual int init_scan_param() override;
protected:
  virtual int fetch_index_table_rowkey();
  virtual int revert_iter() override;
  int init_sort();
  int save_aux_rowkeys();
  int save_rowkeys();
  int save_doc_id_and_rowkey();
  int fetch_rowkey_from_aux();
  virtual int get_next_row() override;
  void reset_sorter();
  virtual int reset_lookup_state() override;
  virtual int get_aux_table_rowkey() override;
  ObNewRowIterator*& get_aux_lookup_iter() { return aux_lookup_iter_; }
private:
  int cmp_ret_;
  int aux_cmp_ret_;
  uint32_t aux_key_count_;
  int index_rowkey_cnt_;
  ObDomainRowkeyComp comparer_;
  ObDomainRowkeyComp aux_comparer_;
  ObExternalSort<ObRowkey, ObDomainRowkeyComp> sorter_; // use ObRowKeyCompare to compare rowkey
  ObExternalSort<ObRowkey, ObDomainRowkeyComp> aux_sorter_;
  common::ObNewRowIterator *aux_lookup_iter_;
  ObRowkey last_rowkey_;
  ObRowkey aux_last_rowkey_;
  bool is_inited_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DOMAIN_INDEX_LOOKUP_OP_H_ */
