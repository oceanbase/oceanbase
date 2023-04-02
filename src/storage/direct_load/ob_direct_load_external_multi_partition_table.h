// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/allocator/page_arena.h"
#include "storage/direct_load/ob_direct_load_external_block_writer.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadExternalMultiPartitionTableBuildParam
{
public:
  ObDirectLoadExternalMultiPartitionTableBuildParam();
  ~ObDirectLoadExternalMultiPartitionTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(datum_utils), KP_(file_mgr), KP_(extra_buf),
               K_(extra_buf_size));
public:
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadTmpFileManager *file_mgr_;
  char *extra_buf_;
  int64_t extra_buf_size_;
};

class ObDirectLoadExternalMultiPartitionTableBuilder : public ObIDirectLoadPartitionTableBuilder
{
  typedef ObDirectLoadExternalMultiPartitionRow RowType;
  typedef ObDirectLoadExternalBlockWriter<RowType> ExternalWriter;
public:
  ObDirectLoadExternalMultiPartitionTableBuilder();
  virtual ~ObDirectLoadExternalMultiPartitionTableBuilder();
  int init(const ObDirectLoadExternalMultiPartitionTableBuildParam &param);
  int append_row(const common::ObTabletID &tablet_id,
                 const blocksstable::ObDatumRow &datum_row) override;
  int close() override;
  int64_t get_row_count() const override { return row_count_; }
  int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                 common::ObIAllocator &allocator) override;
private:
  ObDirectLoadExternalMultiPartitionTableBuildParam param_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadTmpFileHandle file_handle_;
  ExternalWriter external_writer_;
  RowType row_;
  int64_t row_count_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadExternalMultiPartitionTableBuilder);
};

} // namespace storage
} // namespace oceanbase
