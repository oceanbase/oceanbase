/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTabletContext;
class ObLobMacroBlockWriter;

class ObDirectLoadDagLobBuilder
{
public:
  ObDirectLoadDagLobBuilder();
  ~ObDirectLoadDagLobBuilder();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx);
  int switch_slice(const int64_t slice_idx);
  int append_lob(blocksstable::ObDatumRow &datum_row);
  int append_lob(blocksstable::ObBatchDatumRows &datum_rows);
  int close();

private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  int64_t slice_idx_;
  ObWriteMacroParam write_param_;
  ObLobMacroBlockWriter *lob_writer_;
  ObArenaAllocator lob_allocator_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
