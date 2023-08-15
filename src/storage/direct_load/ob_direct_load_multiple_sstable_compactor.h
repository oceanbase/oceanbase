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

#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadMultipleSSTableCompactParam
{
public:
  ObDirectLoadMultipleSSTableCompactParam();
  ~ObDirectLoadMultipleSSTableCompactParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(datum_utils));
public:
  ObDirectLoadTableDataDesc table_data_desc_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
};

class ObDirectLoadMultipleSSTableCompactor : public ObIDirectLoadTabletTableCompactor
{
public:
  ObDirectLoadMultipleSSTableCompactor();
  virtual ~ObDirectLoadMultipleSSTableCompactor();
  int init(const ObDirectLoadMultipleSSTableCompactParam &param);
  int add_table(ObIDirectLoadPartitionTable *table) override;
  int compact() override;
  int get_table(ObIDirectLoadPartitionTable *&table, common::ObIAllocator &allocator) override;
  void stop() override;
private:
  int check_table_compactable(ObDirectLoadMultipleSSTable *sstable);
private:
  ObDirectLoadMultipleSSTableCompactParam param_;
  int64_t index_block_count_;
  int64_t data_block_count_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  common::ObArenaAllocator start_key_allocator_;
  common::ObArenaAllocator end_key_allocator_;
  ObDirectLoadMultipleDatumRowkey start_key_;
  ObDirectLoadMultipleDatumRowkey end_key_;
  common::ObArray<ObDirectLoadMultipleSSTableFragment> fragments_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
