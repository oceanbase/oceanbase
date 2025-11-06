/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_

#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
struct ObMacroDataSeq;
}

namespace storage
{
class ObStorageSchema;
class ObDDLIndependentDag;
struct ObDDLTabletContext;
class ObMacroMetaStoreMananger;

class ObCgMacroBlockWriter
{
public:
  ObCgMacroBlockWriter();
  ~ObCgMacroBlockWriter();
  int init(
      const ObWriteMacroParam &param,
      const ObITable::TableKey &table_key,
      const ObMacroDataSeq &start_sequence,
      const int64_t row_offset,
      const int64_t lob_start_seq = 0 /* ss模式用于lob的宏块meta排序. 非lob统一填0, lob填start_seq*/);
  void reset();
  int append_row(const blocksstable::ObDatumRow &cg_row);
  int append_batch(const blocksstable::ObBatchDatumRows &cg_rows);
  int close();
  bool is_inited() const { return is_inited_; }
  int64_t get_last_macro_seq() const { return macro_block_writer_.get_last_macro_seq(); }
  TO_STRING_KV(K(is_inited_), K(macro_block_writer_), KP(ddl_redo_callback_), K(index_builder_), K(data_desc_));

private:
  DISABLE_COPY_ASSIGN(ObCgMacroBlockWriter);

private:
  bool is_inited_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObSSTableIndexBuilder index_builder_;
  ObIMacroBlockFlushCallback *ddl_redo_callback_;
  blocksstable::ObMacroBlockWriter macro_block_writer_;
};

} // end namespace storage
} // end namespace oceanbase

#endif //OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_
