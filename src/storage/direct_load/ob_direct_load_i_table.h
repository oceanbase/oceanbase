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

#include "lib/allocator/ob_small_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
} // namespace common
namespace storage
{
class ObDirectLoadTableManager;
class ObDirectLoadDatumRow;

struct ObDirectLoadTableType
{
#define OB_DIRECT_LOAD_TABLE_TYPE_DEF(DEF) \
  DEF(INVALID_TABLE_TYPE, = 0)             \
  DEF(EXTERNAL_TABLE, = 1)                 \
  DEF(MULTIPLE_HEAP_TABLE, = 2)            \
  DEF(MULTIPLE_SSTABLE, = 3)               \
  DEF(SSTABLE, = 4)                        \
  DEF(MAX_TABLE_TYPE, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_TABLE_TYPE_DEF, static);

  static bool is_type_valid(const Type type)
  {
    return type > ObDirectLoadTableType::INVALID_TABLE_TYPE &&
           type < ObDirectLoadTableType::MAX_TABLE_TYPE;
  }
  static bool is_external_table(const Type type) { return EXTERNAL_TABLE == type; }
  static bool is_multiple_heap_table(const Type type) { return MULTIPLE_HEAP_TABLE == type; }
  static bool is_multiple_sstable(const Type type) { return MULTIPLE_SSTABLE == type; }
  static bool is_sstable(const Type type) { return SSTABLE == type; }
};

// type, classType, name, shortName
#define OB_DIRECT_LOAD_TABLE_DEF(DEF)                                                                         \
  DEF(ObDirectLoadTableType::EXTERNAL_TABLE, ObDirectLoadExternalTable, external_table, "ExtT")               \
  DEF(ObDirectLoadTableType::MULTIPLE_HEAP_TABLE, ObDirectLoadMultipleHeapTable, multiple_heap_table, "MHT")  \
  DEF(ObDirectLoadTableType::MULTIPLE_SSTABLE, ObDirectLoadMultipleSSTable, multiple_sstable, "MSST")         \
  DEF(ObDirectLoadTableType::SSTABLE, ObDirectLoadSSTable, sstable, "SST")

class ObDirectLoadITable
{
public:
  ObDirectLoadITable() : table_type_(ObDirectLoadTableType::INVALID_TABLE_TYPE), ref_cnt_(0) {}
  virtual ~ObDirectLoadITable() = default;
  virtual const common::ObTabletID &get_tablet_id() const = 0;
  virtual int64_t get_row_count() const = 0;
  virtual bool is_valid() const = 0;
  virtual void inc_ref() { int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1); }
  virtual int64_t dec_ref()
  {
    int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1);
    return cnt;
  }
  virtual int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  ObDirectLoadTableType::Type get_table_type() const { return table_type_; }

#define OB_DIRECT_LOAD_TABLE_TYPE_CHECK(type, classType, name, shortName) \
  OB_INLINE bool is_##name() const { return ObDirectLoadTableType::is_##name(table_type_); }

  OB_DIRECT_LOAD_TABLE_DEF(OB_DIRECT_LOAD_TABLE_TYPE_CHECK);

#undef OB_DIRECT_LOAD_TABLE_TYPE_CHECK

  VIRTUAL_TO_STRING_KV(K_(table_type), K_(ref_cnt));

protected:
  ObDirectLoadTableType::Type table_type_;

private:
  int64_t ref_cnt_;
  DISABLE_COPY_ASSIGN(ObDirectLoadITable);
};

class ObDirectLoadTableHandle final
{
public:
  ObDirectLoadTableHandle();
  ~ObDirectLoadTableHandle();
  ObDirectLoadTableHandle(const ObDirectLoadTableHandle &other);
  ObDirectLoadTableHandle &operator=(const ObDirectLoadTableHandle &other);
  void reset();
  bool is_valid() const { return nullptr != table_ && nullptr != table_mgr_; }
  int set_table(ObDirectLoadITable *table, ObDirectLoadTableManager *table_mgr);
  ObDirectLoadITable *get_table() const { return table_; }
  ObDirectLoadTableManager *get_table_mgr() const { return table_mgr_; }
  TO_STRING_KV(KPC_(table), KP_(table_mgr))
private:
  ObDirectLoadITable *table_;
  ObDirectLoadTableManager *table_mgr_;
};

class ObDirectLoadTableHandleArray
{
public:
  typedef common::ObArray<ObDirectLoadTableHandle>::iterator iterator;
public:
  ObDirectLoadTableHandleArray();
  ~ObDirectLoadTableHandleArray();
  void reset();
  int assign(const ObDirectLoadTableHandleArray &other);
  int add(const ObDirectLoadTableHandle &table_handle);
  int add(const ObDirectLoadTableHandleArray &tables_handle);
  int64_t count() const { return tables_.count(); }
  bool empty() const { return tables_.empty(); }
  int get_table(int64_t idx, ObDirectLoadTableHandle &table_handle) const;
  ObDirectLoadTableHandle &at(int64_t i) { return tables_.at(i); }
  const ObDirectLoadTableHandle &at(int64_t i) const { return tables_.at(i); }
  iterator begin() { return tables_.begin(); }
  iterator end() { return tables_.end(); }
  TO_STRING_KV(K_(tables));
private:
  common::ObArray<ObDirectLoadTableHandle> tables_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTableHandleArray);
};

class ObDirectLoadTableManager
{
public:
  ObDirectLoadTableManager();
  ~ObDirectLoadTableManager();
  int init();

#define DEFINE_OBJ_ALLOC_INTERFACE(type, classType, name, shortName) \
  int alloc_##name(ObDirectLoadTableHandle &table_handle);

  OB_DIRECT_LOAD_TABLE_DEF(DEFINE_OBJ_ALLOC_INTERFACE);

#undef DEFINE_OBJ_ALLOC_INTERFACE

  void release_table(ObDirectLoadITable *table);

private:
  common::ObSmallAllocator allocators_[ObDirectLoadTableType::MAX_TABLE_TYPE];
  bool is_inited_;
  DISABLE_COPY_ASSIGN(ObDirectLoadTableManager);
};

class ObIDirectLoadPartitionTableBuilder
{
public:
  ObIDirectLoadPartitionTableBuilder() = default;
  virtual ~ObIDirectLoadPartitionTableBuilder() = default;
  virtual int append_row(const common::ObTabletID &tablet_id,
                         const ObDirectLoadDatumRow &datum_row) = 0;
  virtual int close() = 0;
  virtual int64_t get_row_count() const = 0;
  virtual int get_tables(ObDirectLoadTableHandleArray &table_array,
                         ObDirectLoadTableManager *table_manager) = 0;
  TO_STRING_EMPTY();
};

class ObIDirectLoadTabletTableCompactor
{
public:
  ObIDirectLoadTabletTableCompactor() = default;
  virtual ~ObIDirectLoadTabletTableCompactor() = default;
  virtual int add_table(const ObDirectLoadTableHandle &table_handle) = 0;
  virtual int compact() = 0;
  virtual int get_table(ObDirectLoadTableHandle &table_handle,
                        ObDirectLoadTableManager *table_manager) = 0;
  virtual void stop() = 0;
  TO_STRING_EMPTY();
};

} // namespace storage
} // namespace oceanbase
