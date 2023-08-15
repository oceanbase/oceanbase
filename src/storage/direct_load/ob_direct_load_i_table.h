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

#include "storage/blocksstable/ob_datum_row.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace storage
{

class ObIDirectLoadPartitionTable
{
public:
  ObIDirectLoadPartitionTable() = default;
  virtual ~ObIDirectLoadPartitionTable() = default;
  virtual const common::ObTabletID &get_tablet_id() const = 0;
  virtual int64_t get_row_count() const = 0;
  virtual bool is_valid() const = 0;
  TO_STRING_EMPTY();
};

class ObIDirectLoadPartitionTableBuilder
{
public:
  ObIDirectLoadPartitionTableBuilder() = default;
  virtual ~ObIDirectLoadPartitionTableBuilder() = default;
  virtual int append_row(const common::ObTabletID &tablet_id,
                         const table::ObTableLoadSequenceNo &seq_no,
                         const blocksstable::ObDatumRow &datum_row) = 0;
  virtual int close() = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int get_tables(common::ObIArray<ObIDirectLoadPartitionTable *> &table_array,
                         common::ObIAllocator &allocator) = 0;
  TO_STRING_EMPTY();
};

class ObIDirectLoadTabletTableCompactor
{
public:
  ObIDirectLoadTabletTableCompactor() = default;
  virtual ~ObIDirectLoadTabletTableCompactor() = default;
  virtual int add_table(ObIDirectLoadPartitionTable *table) = 0;
  virtual int compact() = 0;
  virtual int get_table(ObIDirectLoadPartitionTable *&table, common::ObIAllocator &allocator) = 0;
  virtual void stop() = 0;
  TO_STRING_EMPTY();
};

}  // namespace storage
}  // namespace oceanbase
