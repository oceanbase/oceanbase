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

#ifndef OB_DAS_DOC_ID_MERGE_ITER_H_
#define OB_DAS_DOC_ID_MERGE_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace sql
{

class ObDASDocIdMergeCtDef;
class ObDASDocIdMergeRtDef;

class ObDASDocIdMergeIterParam final : public ObDASIterParam
{
public:
  ObDASDocIdMergeIterParam();
  ~ObDASDocIdMergeIterParam();

  virtual bool is_valid() const override
  {
    return rowkey_doc_tablet_id_.is_valid()
        && rowkey_doc_ls_id_.is_valid()
        && nullptr != rowkey_doc_iter_
        && nullptr != data_table_iter_
        && nullptr != rowkey_doc_ctdef_
        && nullptr != data_table_ctdef_
        && nullptr != rowkey_doc_rtdef_
        && nullptr != data_table_rtdef_
        && nullptr != snapshot_;
  }
  INHERIT_TO_STRING_KV("ObDASIterParam", ObDASIterParam,
                     K(rowkey_doc_tablet_id_),
                     K(rowkey_doc_ls_id_),
                     KP(rowkey_doc_iter_),
                     KP(data_table_iter_),
                     KP(rowkey_doc_ctdef_),
                     KP(data_table_ctdef_),
                     KP(rowkey_doc_rtdef_),
                     KP(data_table_rtdef_),
                     KPC(trans_desc_),
                     KPC(snapshot_));
public:
  common::ObTabletID rowkey_doc_tablet_id_;
  share::ObLSID rowkey_doc_ls_id_;
  ObDASScanIter *rowkey_doc_iter_;
  ObDASScanIter *data_table_iter_;
  ObDASScanCtDef *rowkey_doc_ctdef_;
  ObDASScanCtDef *data_table_ctdef_;
  ObDASScanRtDef *rowkey_doc_rtdef_;
  ObDASScanRtDef *data_table_rtdef_;
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
};

/**
 * DAS Iter Tree of DAS Scan with Doc Id:
 *
 * CASE 1: Partition Scan Tree                        CASE 2: Index LoopUp Tree
 *
 *                DOC_ID_MERGE_ITER                              DAS_INDEX_LOOKUP_ITER
 *                 /              \                               /                \
 *               /                  \                            /                  \
 * DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDoc)  DAS_SCAN_ITER(IndexTable) DOC_ID_MERGE_ITER
 *                                                                                /          \
 *                                                                              /             \
 *                                                             DAS_SCAN_ITER(DataTable) DAS_SCAN_ITER(RowkeyDoc)
 **/
class ObDASDocIdMergeIter final : public ObDASIter
{
public:
  ObDASDocIdMergeIter();
  ~ObDASDocIdMergeIter();

  storage::ObTableScanParam &get_rowkey_doc_scan_param() { return rowkey_doc_scan_param_; }
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;

  ObDASScanIter *get_data_table_iter() { return data_table_iter_; }
  int set_doc_id_merge_related_ids(const ObDASRelatedTabletID &tablet_ids, const share::ObLSID &ls_id);

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter,
                     K(rowkey_doc_scan_param_),
                     KPC(rowkey_doc_iter_),
                     KPC(data_table_iter_));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  common::ObArenaAllocator &get_arena_allocator() { return merge_memctx_->get_arena_allocator(); }
  int init_rowkey_doc_scan_param(
      const common::ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      transaction::ObTxDesc *trans_desc,
      transaction::ObTxReadSnapshot *snapshot);
  int build_rowkey_doc_range();
  int concat_row();
  int concat_rows(int64_t &count, int64_t capacity);
  int sorted_merge_join_row();
  int sorted_merge_join_rows(int64_t &count, int64_t capacity);
  int get_rowkey(
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObRowkey &rowkey);
  int get_doc_id(
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObDocId &doc_id);
  int get_rowkeys(
      const int64_t size,
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIArray<common::ObRowkey> &rowkeys);
  int get_doc_ids(
      const int64_t size,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIArray<common::ObDocId> &doc_ids);
  int get_rowkeys_and_doc_ids(
      const int64_t size,
      common::ObIAllocator &allocator,
      const ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      common::ObIArray<common::ObRowkey> &rowkeys,
      common::ObIArray<common::ObDocId> &doc_ids);
  int fill_doc_id_in_data_table(const common::ObDocId &doc_id);
  int fill_doc_ids_in_data_table(const common::ObIArray<common::ObDocId> &doc_ids);
private:
  bool need_filter_rowkey_doc_;
  storage::ObTableScanParam rowkey_doc_scan_param_;
  ObDASScanIter *rowkey_doc_iter_;
  ObDASScanIter *data_table_iter_;
  const ObDASScanCtDef *rowkey_doc_ctdef_;
  const ObDASScanCtDef *data_table_ctdef_;
  ObDASScanRtDef *rowkey_doc_rtdef_;
  ObDASScanRtDef *data_table_rtdef_;
  ObTabletID rowkey_doc_tablet_id_;
  share::ObLSID rowkey_doc_ls_id_;
  lib::MemoryContext merge_memctx_;
};

} // end namespace sql
} // end namespace oceanbase
#endif // OB_DAS_DOC_MERGE_ITER_H_
