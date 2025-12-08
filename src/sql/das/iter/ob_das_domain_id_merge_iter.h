/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DAS_DOMAIN_ID_MERGE_ITER_H_
#define OB_DAS_DOMAIN_ID_MERGE_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/access/ob_dml_param.h"
#include "share/domain_id/ob_domain_id.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace sql
{

class ObDASDomainIdMergeCtDef;
class ObDASDomainIdMergeRtDef;
class ObDASScanCtDef;
class ObDASScanRtDef;

class ObDASDomainIdMergeIterParam final : public ObDASIterParam
{
public:
  ObDASDomainIdMergeIterParam();
  ~ObDASDomainIdMergeIterParam();

  virtual bool is_valid() const override;
  INHERIT_TO_STRING_KV("ObDASIterParam", ObDASIterParam,
                     K(rowkey_domain_ls_id_),
                     K(rowkey_domain_tablet_ids_),
                     KP(data_table_iter_),
                     KP(data_table_ctdef_),
                     KP(data_table_rtdef_),
                     K(rowkey_domain_table_iters_),
                     K(rowkey_domain_ctdefs_),
                     K(rowkey_domain_rtdefs_),
                     KPC(trans_desc_),
                     KPC(snapshot_));
public:
  share::ObLSID rowkey_domain_ls_id_; // all domain index should be in one ls (local index)
  common::ObArray<common::ObTabletID> rowkey_domain_tablet_ids_;
  ObDASScanIter* data_table_iter_;
  ObDASScanCtDef* data_table_ctdef_;
  ObDASScanRtDef *data_table_rtdef_;
  common::ObArray<ObDASScanIter*> rowkey_domain_table_iters_;
  common::ObArray<ObDASScanCtDef*> rowkey_domain_ctdefs_;
  common::ObArray<ObDASScanRtDef*> rowkey_domain_rtdefs_;
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
};

/**
 * DAS Iter Tree of DAS Scan with Doc Id:
 *
 * CASE 1: Partition Scan Tree                           CASE 2: Index LoopUp Tree
 *
 *                DOC_ID_MERGE_ITER                              DAS_INDEX_LOOKUP_ITER
 *                 /              \                               /                \
 *               /                  \                            /                  \
 * DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDomain)  DAS_SCAN_ITER(IndexTable) DOC_ID_MERGE_ITER
 *                                                                                /          \
 *                                                                              /             \
 *                                                             DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDomain)
 **/
class ObDASDomainIdMergeIter final : public ObDASIter
{
public:
  ObDASDomainIdMergeIter();
  ~ObDASDomainIdMergeIter();

  storage::ObTableScanParam &get_rowkey_domain_scan_param(int64_t idx) { return *rowkey_domain_scan_params_.at(idx); }
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  virtual int set_scan_rowkey(ObEvalCtx *eval_ctx,
                              const ObIArray<ObExpr *> &rowkey_exprs,
                              const ObDASScanCtDef *lookup_ctdef,
                              ObIAllocator *alloc,
                              int64_t group_id) override;
  ObDASScanIter *get_data_table_iter() { return data_table_iter_; }
  int set_domain_id_merge_related_ids(const ObDASRelatedTabletID &tablet_ids, const share::ObLSID &ls_id);

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter,
                     K(rowkey_domain_scan_params_),
                     K(rowkey_domain_iters_),
                     KPC(data_table_iter_));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  common::ObArenaAllocator &get_arena_allocator() { return merge_memctx_->get_arena_allocator(); }
  int init_rowkey_domain_scan_param(
      const common::ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      transaction::ObTxDesc *trans_desc,
      transaction::ObTxReadSnapshot *snapshot,
      storage::ObTableScanParam &scan_param);
  int build_rowkey_domain_range();
  int check_is_need_multi_get();
  int concat_row();
  int concat_rows(int64_t &count, int64_t capacity);
  int sorted_merge_join_row();
  int sorted_merge_join_rows(int64_t &count, int64_t capacity);
  int get_rowkey(
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObRowkey &rowkey);
  int get_rowkeys(
      const int64_t size,
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIArray<common::ObRowkey> &rowkeys);
  int get_domain_id(
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIAllocator &allocator,
      share::ObDomainIdUtils::DomainIds &domain_id);
  int get_and_fill_domain_id_in_data_table(
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIAllocator &allocator);
  int get_domain_ids(
      const int64_t size,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIAllocator &allocator,
      common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids);
  int fill_domain_ids_in_data_table(
      uint64_t domain_tid,
      const common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids);
  int get_rowkeys_and_domain_ids(
      const int64_t size,
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIArray<common::ObRowkey> &rowkeys,
      common::ObIArray<share::ObDomainIdUtils::DomainIds> &domain_ids);
  int get_domain_id_count(const ObDASScanCtDef *ctdef, int64_t &domain_id_count);
  int multi_get_row();
  int multi_get_rows(int64_t &count, int64_t capacity);
  int check_is_emb_vec_domain(int64_t iter_idx, bool &is_emb_vec);
  int check_is_emb_vec_domain_by_table_id(int64_t table_id, bool &is_emb_vec);
  int check_use_rowkey_vid_tbl_by_table_id(int64_t table_id, bool &use_rowkey_vid_tbl);
  int check_table_need_add_part_key(int64_t table_id, int64_t &part_key_num, const ObDASScanCtDef *ctdef);
  int reset_rowkey_domain_iter_scan_range(int64_t iter_idx, const common::ObRowkey &data_table_rowkey);
  int fill_null_domain_id_in_data_table(const ObDASScanCtDef *ctdef,
                                       ObDASScanRtDef *rtdef,
                                       common::ObIAllocator &allocator);
  int get_sync_interval_type(int64_t table_id, ObVectorIndexSyncIntervalType &sync_interval_type);
private:
  bool need_filter_rowkey_domain_;
  bool is_no_sample_;
  ObArray<storage::ObTableScanParam*> rowkey_domain_scan_params_;
  ObArray<ObDASScanIter*> rowkey_domain_iters_;
  ObDASScanIter *data_table_iter_;
  ObArray<ObDASScanCtDef *> rowkey_domain_ctdefs_;
  const ObDASScanCtDef *data_table_ctdef_;
  ObArray<ObDASScanRtDef *> rowkey_domain_rtdefs_;
  ObDASScanRtDef *data_table_rtdef_;
  ObArray<ObTabletID> rowkey_domain_tablet_ids_;
  share::ObLSID rowkey_domain_ls_id_;
  lib::MemoryContext merge_memctx_;
  bool is_need_multi_get_;
};

} // end namespace sql
} // end namespace oceanbase
#endif // OB_DAS_DOMAIN_ID_MERGE_ITER_H_
