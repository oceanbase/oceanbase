// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadExternalTable;

struct ObDirectLoadExternalTableCompactParam
{
public:
  ObDirectLoadExternalTableCompactParam();
  ~ObDirectLoadExternalTableCompactParam();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(table_data_desc));
public:
  common::ObTabletID tablet_id_;
  ObDirectLoadTableDataDesc table_data_desc_;
};

class ObDirectLoadExternalTableCompactor : public ObIDirectLoadTabletTableCompactor
{
public:
  ObDirectLoadExternalTableCompactor();
  virtual ~ObDirectLoadExternalTableCompactor();
  int init(const ObDirectLoadExternalTableCompactParam &param);
  int add_table(ObIDirectLoadPartitionTable *table) override;
  int compact() override;
  int get_table(ObIDirectLoadPartitionTable *&table, common::ObIAllocator &allocator) override;
  void stop() override;
private:
  int check_table_compactable(ObDirectLoadExternalTable *external_table);
private:
  ObDirectLoadExternalTableCompactParam param_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  ObDirectLoadExternalFragmentArray fragments_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
