// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

#include "common/ob_tablet_id.h"
#include "storage/direct_load/ob_direct_load_fast_heap_table_ctx.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadResultInfo;
} // namespace table
namespace common
{
class ObOptColumnStat;
} // namespace common
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObSSTableInsertSliceWriter;

struct ObDirectLoadFastHeapTableBuildParam
{
public:
  ObDirectLoadFastHeapTableBuildParam();
  ~ObDirectLoadFastHeapTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(snapshot_version), K_(table_data_desc), KP_(insert_table_ctx),
               KP_(fast_heap_table_ctx), KP_(result_info), K_(online_opt_stat_gather));
public:
  common::ObTabletID tablet_id_;
  int64_t snapshot_version_;
  ObDirectLoadTableDataDesc table_data_desc_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  ObDirectLoadFastHeapTableContext *fast_heap_table_ctx_;
  table::ObTableLoadResultInfo *result_info_;
  bool online_opt_stat_gather_;
};

class ObDirectLoadFastHeapTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  static const int64_t HIDDEN_ROWKEY_COLUMN_NUM = 1;
public:
  ObDirectLoadFastHeapTableBuilder();
  virtual ~ObDirectLoadFastHeapTableBuilder();
  int init(const ObDirectLoadFastHeapTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const blocksstable::ObDatumRow &datum_row) override;
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  int init_sql_statistics();
  int collect_obj(const blocksstable::ObDatumRow &datum_row);
  int init_sstable_slice_ctx();
  int switch_sstable_slice();
private:
  ObDirectLoadFastHeapTableBuildParam param_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadFastHeapTableTabletContext *fast_heap_table_tablet_ctx_;
  ObSSTableInsertSliceWriter *slice_writer_;
  ObDirectLoadFastHeapTableTabletWriteCtx write_ctx_;
  blocksstable::ObDatumRow datum_row_;
  common::ObArray<ObOptColumnStat*> column_stat_array_;
  int64_t row_count_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadFastHeapTableBuilder);
};

} // namespace storage
} // namespace oceanbase
