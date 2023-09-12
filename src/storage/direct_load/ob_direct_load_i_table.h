// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/blocksstable/ob_datum_row.h"

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
