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

#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertLobTabletContext;

class ObDirectLoadLobBuilder
{
public:
  ObDirectLoadLobBuilder();
  ~ObDirectLoadLobBuilder();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           common::ObIAllocator *lob_allocator = nullptr);

  // 包含多版本列的完整行
  int append_lob(blocksstable::ObDatumRow &datum_row);
  int append_lob(blocksstable::ObBatchDatumRows &datum_rows);
  // 中间过程数据
  int append_lob(ObDirectLoadDatumRow &datum_row,
                 const ObDirectLoadRowFlag &row_flag);
  int append_lob(const IVectorPtrs &vectors,
                 const int64_t row_idx,
                 const ObDirectLoadRowFlag &row_flag);
  int close();

private:
  int init_sstable_slice_ctx();
  int switch_sstable_slice();

  inline int check_can_skip(char *ptr, uint32_t len, bool &can_skip);
  int check_can_skip(const blocksstable::ObDatumRow &datum_row, bool &can_skip);
  int check_can_skip(const blocksstable::ObBatchDatumRows &datum_rows, bool &can_skip);

  int append_row(blocksstable::ObDatumRow &datum_row);
  int append_batch(blocksstable::ObBatchDatumRows &datum_rows);

  int fill_into_datum_row(ObDirectLoadDatumRow &datum_row,
                          const ObDirectLoadRowFlag &row_flag);
  int fetch_from_datum_row(ObDirectLoadDatumRow &datum_row,
                           const ObDirectLoadRowFlag &row_flag);

  int fill_into_datum_row(const IVectorPtrs &vectors,
                          const int64_t row_idx,
                          const ObDirectLoadRowFlag &row_flag);
  int fetch_from_datum_row(const IVectorPtrs &vectors,
                           const int64_t row_idx,
                           const ObDirectLoadRowFlag &row_flag);

private:
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadInsertLobTabletContext *insert_lob_tablet_ctx_;
  common::ObIAllocator *lob_allocator_;
  common::ObArenaAllocator inner_lob_allocator_;
  // 不包含多版本列, lob可能是主键列
  const ObIArray<int64_t> *lob_column_idxs_;
  int64_t lob_column_cnt_;
  int64_t extra_rowkey_cnt_;
  int64_t lob_inrow_threshold_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  int64_t current_lob_slice_id_;
  blocksstable::ObDatumRow datum_row_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadLobBuilder);
};

} // namespace storage
} // namespace oceanbase
