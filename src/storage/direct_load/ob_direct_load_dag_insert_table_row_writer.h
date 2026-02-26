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

#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/direct_load/ob_direct_load_dag_insert_table_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadDMLRowHandler;
class ObDirectLoadDatumRow;

class ObDirectLoadDagInsertTableBatchRowDirectWriter
{
public:
  ObDirectLoadDagInsertTableBatchRowDirectWriter();
  ~ObDirectLoadDagInsertTableBatchRowDirectWriter();
  int init(ObDirectLoadInsertTabletContext *insert_tablet_ctx,
           ObDirectLoadDMLRowHandler *dml_row_handler);
  int append_batch(const ObDirectLoadBatchRows &batch_rows);
  int append_selective(const ObDirectLoadBatchRows &batch_rows, const uint16_t *selector,
                       const int64_t size);
  int append_row(const ObDirectLoadDatumRow &datum_row);
  int close();

  TO_STRING_KV(K_(tablet_id), KP_(insert_tablet_ctx), KP_(dml_row_handler), K_(write_param),
               KP_(slice_writer), K_(write_ctx), K_(insert_table_result));

private:
  int init_batch_rows();
  int switch_slice(const bool is_final = false);
  int flush_buffer();
  int flush_batch(blocksstable::ObBatchDatumRows &datum_rows);

private:
  ObTabletID tablet_id_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadDMLRowHandler *dml_row_handler_;
  ObDirectLoadDagInsertTableRowHandler row_handler_;
  ObDirectLoadBatchRows batch_rows_;
  blocksstable::ObBatchDatumRows datum_rows_;
  blocksstable::ObBatchDatumRows direct_datum_rows_;
  ObWriteMacroParam write_param_;
  ObArenaAllocator allocator_;
  ObITabletSliceWriter *slice_writer_;
  ObDirectLoadInsertTabletWriteCtx write_ctx_;
  ObDirectLoadInsertTableResult insert_table_result_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
