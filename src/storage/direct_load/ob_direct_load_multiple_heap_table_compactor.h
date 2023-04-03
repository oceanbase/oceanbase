// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yiren.ly<>

#pragma once

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_multiple_heap_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObIDirectLoadMultipleHeapTableIndexScanner;

struct ObDirectLoadMultipleHeapTableCompactParam
{
public:
  ObDirectLoadMultipleHeapTableCompactParam();
  ~ObDirectLoadMultipleHeapTableCompactParam();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(file_mgr), K_(index_dir_id));
public:
  ObDirectLoadTableDataDesc table_data_desc_;
  ObDirectLoadTmpFileManager *file_mgr_;
  int64_t index_dir_id_;
};

class ObDirectLoadMultipleHeapTableCompactor : public ObIDirectLoadTabletTableCompactor
{
public:
  ObDirectLoadMultipleHeapTableCompactor();
  virtual ~ObDirectLoadMultipleHeapTableCompactor();
  void reset();
  void reuse();
  int init(const ObDirectLoadMultipleHeapTableCompactParam &param);
  int add_table(ObIDirectLoadPartitionTable *table) override;
  int compact() override;
  int get_table(ObIDirectLoadPartitionTable *&table, common::ObIAllocator &allocator) override;
  void stop() override;
private:
  int check_table_compactable(ObDirectLoadMultipleHeapTable *heap_table);
  int construct_index_scanner(ObDirectLoadMultipleHeapTable *heap_table);
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMultipleHeapTableCompactParam param_;
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t index_file_size_;
  int64_t data_file_size_;
  int64_t index_entry_count_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  common::ObArray<ObIDirectLoadMultipleHeapTableIndexScanner *> index_scanners_;
  common::ObArray<int64_t> base_data_fragment_idxs_;
  common::ObArray<ObDirectLoadMultipleHeapTableDataFragment> data_fragments_;
  ObDirectLoadTmpFileHandle compacted_index_file_handle_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
