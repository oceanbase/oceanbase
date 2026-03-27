/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRow;
class ObBatchDatumRows;
} // namespace blocksstable
namespace observer
{
class ObTableLoadRowProjector;

struct ObTableLoadIndexTableBuildParam
{
public:
  ObTableLoadIndexTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(rowkey_column_count), KP_(datum_utils), K_(table_data_desc), KP_(project),
               KP_(file_mgr));

public:
  int64_t rowkey_column_count_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  ObTableLoadRowProjector *project_;
  storage::ObDirectLoadTmpFileManager *file_mgr_;
};

class ObTableLoadIndexTableBuilder
{
public:
  ObTableLoadIndexTableBuilder();
  ~ObTableLoadIndexTableBuilder();
  int init(const ObTableLoadIndexTableBuildParam &param);
  int append_insert_row(const common::ObTabletID &tablet_id,
                        const ObDirectLoadDatumRow &datum_row);
  int append_insert_row(const common::ObTabletID &tablet_id,
                        const blocksstable::ObDatumRow &datum_row);
  int append_insert_batch(const common::ObTabletID &tablet_id,
                          const blocksstable::ObBatchDatumRows &datum_rows);
  int append_replace_row(const common::ObTabletID &tablet_id,
                         const ObDirectLoadDatumRow &old_row,
                         const ObDirectLoadDatumRow &new_row);
  int append_delete_row(const common::ObTabletID &tablet_id,
                        const ObDirectLoadDatumRow &datum_row);
  int close();
  int64_t get_row_count() const;
  int get_tables(storage::ObDirectLoadTableHandleArray &table_array,
                 storage::ObDirectLoadTableManager *table_mgr);

private:
  int64_t rowkey_column_count_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObTableLoadRowProjector *project_;
  ObDirectLoadDatumRow insert_row_;
  ObDirectLoadDatumRow delete_row_;
  ObDirectLoadExternalMultiPartitionTableBuilder table_builder_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
