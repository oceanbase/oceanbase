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

#ifndef OCEANBASE_STORAGE_FTS_DOC_WORD_ITERATOR_H
#define OCEANBASE_STORAGE_FTS_DOC_WORD_ITERATOR_H

#include "lib/allocator/page_arena.h"
#include "share/schema/ob_table_param.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace storage
{

class ObFTDocWordScanIterator final
{
public:
  ObFTDocWordScanIterator();
  ~ObFTDocWordScanIterator();

  int init(
      const uint64_t table_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const transaction::ObTxReadSnapshot *snapshot,
      const int64_t schema_version);
  int do_scan(
      const uint64_t table_id,
      const common::ObDocId &doc_id);
  int get_next_row(blocksstable::ObDatumRow *&datum_row);

  void reset();

  TO_STRING_KV(KP(doc_word_iter_), K(scan_param_), K(table_param_));
private:
  int init_scan_param(
      const uint64_t table_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const transaction::ObTxReadSnapshot *snapshot,
      const int64_t schema_version);
  int build_table_param(
      const uint64_t table_id,
      share::schema::ObTableParam &table_param,
      common::ObIArray<uint64_t> &column_ids);
  int build_key_range(
      const uint64_t table_id,
      const common::ObDocId &doc_id,
      common::ObIArray<ObNewRange> &rowkey_range);
  int do_table_scan();
  int do_table_rescan();
  int reuse();

private:
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator scan_allocator_;
  share::schema::ObTableParam table_param_;
  storage::ObTableScanParam scan_param_;
  common::ObNewRowIterator *doc_word_iter_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObFTDocWordScanIterator);
};
} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_FTS_DOC_WORD_ITERATOR_H
