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

#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadStoreLobTableCtx;

struct ObTableLoadLobTableBuildParam
{
public:
  ObTableLoadLobTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(KP_(lob_table_ctx), KPC_(lob_column_idxs), K_(table_data_desc), KP_(file_mgr));

public:
  ObTableLoadStoreLobTableCtx *lob_table_ctx_;
  const common::ObArray<int64_t> *lob_column_idxs_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  storage::ObDirectLoadTmpFileManager *file_mgr_;
};

class ObTableLoadLobTableBuilder
{
public:
  ObTableLoadLobTableBuilder();
  ~ObTableLoadLobTableBuilder();
  int init(const ObTableLoadLobTableBuildParam &param);
  int append_delete_row(const ObTabletID &tablet_id,
                        const ObDirectLoadDatumRow &row);
  int close();
  int64_t get_row_count() const;
  int get_tables(storage::ObDirectLoadTableHandleArray &table_array,
                 storage::ObDirectLoadTableManager *table_mgr);

private:
  ObTableLoadStoreLobTableCtx *lob_table_ctx_;
  const common::ObArray<int64_t> *lob_column_idxs_;
  ObDirectLoadDatumRow datum_row_;
  ObDirectLoadExternalMultiPartitionTableBuilder table_builder_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
