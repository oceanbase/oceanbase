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

#ifndef _OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_
#define _OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_

#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_tablet_autoincrement_param.h" // for ObTabletCacheInterval
#include "storage/ddl/ob_ddl_seq_generator.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/ddl/ob_ddl_tablet_context.h"

namespace oceanbase
{

namespace blocksstable
{
class ObStorageDatum;
}

namespace storage
{
class ObDDLRedoLogWriterCallback;
class ObColumnSchemaItem;
class ObDDLIndependentDag;
class ObCgMacroBlockWriter;

class ObLobMacroBlockWriter
{
public:
  ObLobMacroBlockWriter();
  ~ObLobMacroBlockWriter();
  int init(const ObWriteMacroParam &param,
           const ObTabletID &data_tablet_id,
           const blocksstable::ObMacroDataSeq &start_sequence);
  int write(const ObColumnSchemaItem &column_schema, ObIAllocator &row_allocator, blocksstable::ObStorageDatum &datum);
  int close();
  TO_STRING_KV(K(is_inited_), K(lob_meta_tablet_id_), K(slice_idx_), K(macro_seq_),
      K(lob_id_cache_), K(lob_column_count_), K(lob_id_generator_), K(lob_meta_row_), KP(macro_block_writer_));

private:
  int transform_lob_meta_row(ObLobMetaWriteResult &lob_meta_write_result);
  int prepare_macro_block_writer();
  int switch_lob_id_cache();
  int close_macro_block_writer();

private:
  bool is_inited_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;

  ObTabletID lob_meta_tablet_id_;
  int64_t slice_idx_;
  int64_t lob_inrow_threshold_;
  blocksstable::ObMacroDataSeq macro_seq_;

  share::ObTabletCacheInterval lob_id_cache_;
  int64_t lob_column_count_;
  ObDDLSeqGenerator lob_id_generator_;

  ObArenaAllocator lob_arena_;
  ObLobMetaWriteIter meta_write_iter_;
  blocksstable::ObDatumRow lob_meta_row_;

  ObCgMacroBlockWriter *macro_block_writer_;
  ObWriteMacroParam param_;
  int64_t total_lob_cell_count_;
  int64_t inrow_lob_cell_count_;
};

} // end namespace storage
} // end namespace oceanbase

#endif//_OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_
