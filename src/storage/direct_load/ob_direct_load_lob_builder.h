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

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadLobBuilder
{
public:
  ObDirectLoadLobBuilder();
  ~ObDirectLoadLobBuilder();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx);
  int append_lob(common::ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row);
  int close();
private:
  int init_sstable_slice_ctx();
  int switch_sstable_slice();
private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  int64_t lob_column_count_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  int64_t current_lob_slice_id_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadLobBuilder);
};

} // namespace storage
} // namespace oceanbase
