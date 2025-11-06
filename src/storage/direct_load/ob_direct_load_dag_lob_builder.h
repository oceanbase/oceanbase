/**
 * Copyright (c) 2025 OceanBase
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
