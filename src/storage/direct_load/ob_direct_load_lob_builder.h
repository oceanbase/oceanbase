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

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace table
{
} // namespace table
namespace common
{
} // namespace common
namespace storage
{

struct ObDirectLoadLobBuildParam
{
public:
  ObDirectLoadLobBuildParam();
  ~ObDirectLoadLobBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  int64_t lob_column_cnt_;
};

class ObDirectLoadLobBuilder
{
public:
  ObDirectLoadLobBuilder();
  ~ObDirectLoadLobBuilder();
  int init(const ObDirectLoadLobBuildParam &param);
  int append_lob(common::ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row);
 int close();
private:
  int init_sstable_slice_ctx();
  int switch_sstable_slice();
private:
  ObDirectLoadLobBuildParam param_;
  common::ObTabletID lob_tablet_id_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  int64_t current_lob_slice_id_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadLobBuilder);
};

} // namespace storage
} // namespace oceanbase
