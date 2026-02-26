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

#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_multi_partition_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableStore;
}
namespace observer
{
struct ObTableLoadTableBuildParam
{
public:
  ObTableLoadTableBuildParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_data_desc), KP_(file_mgr));

public:
  storage::ObDirectLoadTableDataDesc table_data_desc_;
  storage::ObDirectLoadTmpFileManager *file_mgr_;
};
class ObTableLoadTableBuilder
{
public:
  ObTableLoadTableBuilder();
  virtual ~ObTableLoadTableBuilder();
  int init(const ObTableLoadTableBuildParam &param);
  int append_row(const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row);
  int close();
  int get_tables(storage::ObDirectLoadTableHandleArray &table_array,
                 storage::ObDirectLoadTableManager *table_mgr);
  ObDirectLoadDatumRow &get_insert_datum_row() { return insert_datum_row_; }
  ObDirectLoadDatumRow &get_delete_datum_row() { return delete_datum_row_; }

private:
  ObDirectLoadExternalMultiPartitionTableBuilder table_builder_;
  ObDirectLoadDatumRow insert_datum_row_;
  ObDirectLoadDatumRow delete_datum_row_;
  bool is_inited_;
};

class ObTableLoadTableBuilderMgr
{
public:
  ObTableLoadTableBuilderMgr()
    : allocator_("TLD_TB"),
      safe_allocator_(allocator_),
      table_store_(nullptr),
      file_mgr_(nullptr),
      table_mgr_(nullptr),
      is_inited_(false)
  {
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadTableBuilderMgr() { reset(); }

  int init(ObDirectLoadTableStore *table_store, storage::ObDirectLoadTmpFileManager *file_mgr,
           ObDirectLoadTableManager *table_mgr);

  int get_table_builder(ObTableLoadTableBuilder *&table_builder);
  int close();
  void reset();

private:
  int acquire_table_builder(ObTableLoadTableBuilder *&table_builder);

private:
  common::ObArenaAllocator allocator_;
  common::ObSafeArenaAllocator safe_allocator_;
  common::hash::ObHashMap<int64_t, ObTableLoadTableBuilder *> table_builder_map_;
  ObDirectLoadTableStore *table_store_;
  storage::ObDirectLoadTmpFileManager *file_mgr_;
  storage::ObDirectLoadTableManager *table_mgr_;
  bool is_inited_;
};
} // namespace observer
} // namespace oceanbase