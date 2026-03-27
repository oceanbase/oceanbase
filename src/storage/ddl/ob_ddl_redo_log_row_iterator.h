/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObDDLRedoLogRowIterator
{
public:
  ObDDLRedoLogRowIterator(common::ObIAllocator &allocator, const uint64_t tenant_id);
  ~ObDDLRedoLogRowIterator();
  int init(const ObString &data_buffer);
  void reset();
  int get_next_row(const blocksstable::ObDatumRow *&row,
                   const ObStoreRowkey *&rowkey,
                   transaction::ObTxSEQ &seq_no,
                   blocksstable::ObDmlRowFlag &row_flag);
  int get_next_lob_meta_row(const ObLobMetaInfo *&row,
                            const ObStoreRowkey *&rowkey,
                            transaction::ObTxSEQ &seq_no,
                            blocksstable::ObDmlRowFlag &row_flag);
private:
  common::ObIAllocator &allocator_;
  blocksstable::ObMacroBlockRowBareIterator iter_;
  common::ObStoreRowkey rowkey_;
  common::ObObj *rowkey_obobj_;
  int64_t schema_rowkey_column_count_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
