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

#include "common/ob_tablet_id.h"
#include "observer/table_load/plan/ob_table_load_table_builder.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableOp;
class ObTableLoadRowProjector;

class ObTableLoadDataChannel
{
public:
  ObTableLoadDataChannel() = default;
  virtual ~ObTableLoadDataChannel() = default;
  virtual int open() = 0;
  virtual int close() = 0;
  virtual int handle_insert_row(const ObTabletID &tablet_id,
                                const storage::ObDirectLoadDatumRow &datum_row) = 0;
  virtual int handle_insert_row(const ObTabletID &tablet_id,
                                const blocksstable::ObDatumRow &datum_row) = 0;
  virtual int handle_insert_batch(const ObTabletID &tablet_id,
                                  const blocksstable::ObBatchDatumRows &datum_rows) = 0;
  virtual int handle_delete_row(const ObTabletID &tablet_id,
                                const storage::ObDirectLoadDatumRow &datum_row) = 0;
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                const storage::ObDirectLoadDatumRow &datum_row) = 0;
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                ObIArray<const storage::ObDirectLoadExternalRow *> &rows,
                                const storage::ObDirectLoadExternalRow *result_row) = 0;
  virtual int handle_update_row(ObArray<const storage::ObDirectLoadMultipleDatumRow *> &rows,
                                const storage::ObDirectLoadMultipleDatumRow *result_row) = 0;
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                const storage::ObDirectLoadDatumRow &old_row,
                                const storage::ObDirectLoadDatumRow &new_row,
                                const storage::ObDirectLoadDatumRow *result_row) = 0;
  virtual int handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                            const storage::ObDirectLoadDatumRow &datum_row) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObTableLoadTableChannel : public ObTableLoadDataChannel
{
public:
  ObTableLoadTableChannel(ObTableLoadTableOp *up_table_op, ObTableLoadTableOp *down_table_op);
  virtual ~ObTableLoadTableChannel();
  int open() override;
  int close() override;
  int64_t simple_to_string(char *buf, int64_t buf_len) const;
  VIRTUAL_TO_STRING_KV(KP_(up_table_op), KP_(down_table_op), KP_(row_projector), K_(is_closed),
                       K_(is_inited));

protected:
  virtual int create_row_projector() = 0;
  virtual ObDirectLoadTableType::Type get_table_type() = 0;

protected:
  ObTableLoadTableOp *up_table_op_;
  ObTableLoadTableOp *down_table_op_;
  ObArenaAllocator allocator_;
  ObTableLoadRowProjector *row_projector_;
  ObTableLoadTableBuilderMgr table_builder_mgr_;
  bool is_closed_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
