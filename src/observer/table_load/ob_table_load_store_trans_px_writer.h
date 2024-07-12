/**
 * Copyright (c) 2024 OceanBase
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

namespace oceanbase
{
namespace common
{
class ObNewRow;
} // namespace common
namespace observer
{
class ObTableLoadStoreCtx;
class ObTableLoadStoreTrans;
class ObTableLoadTransStoreWriter;

class ObTableLoadStoreTransPXWriter
{
public:
  ObTableLoadStoreTransPXWriter();
  ~ObTableLoadStoreTransPXWriter();

  void reset();
  int init(ObTableLoadStoreCtx *store_ctx,
           ObTableLoadStoreTrans *trans,
           ObTableLoadTransStoreWriter *writer);
  int prepare_write(const common::ObTabletID &tablet_id,
                    const common::ObIArray<uint64_t> &column_ids);
  int write(const common::ObNewRow &row);

  TO_STRING_KV(KP_(store_ctx),
               KP_(trans),
               KP_(writer),
               K_(tablet_id),
               K_(column_count),
               K_(row_count),
               K_(is_heap_table),
               K_(can_write),
               K_(is_inited));

private:
  const static int64_t CHECK_STATUS_CYCLE = 10000;
  int check_tablet(const common::ObTabletID &tablet_id);
  int check_columns(const common::ObIArray<uint64_t> &column_ids);
  int check_status();

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadStoreTrans *trans_;
  ObTableLoadTransStoreWriter *writer_;
  ObTabletID tablet_id_;
  int64_t column_count_;
  int64_t row_count_;
  bool is_heap_table_;
  bool can_write_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
