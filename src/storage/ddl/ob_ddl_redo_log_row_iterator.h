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
