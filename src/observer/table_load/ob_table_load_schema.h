// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#pragma once

#include "common/object/ob_obj_type.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_schema.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{

class ObTableLoadSchema
{
public:
  static int get_schema_guard(uint64_t tenant_id, share::schema::ObSchemaGetterGuard &schema_guard);
  static int get_table_schema(uint64_t tenant_id, uint64_t database_id,
                              const common::ObString &table_name,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const share::schema::ObTableSchema *&table_schema);
  static int get_table_schema(uint64_t tenant_id, uint64_t table_id,
                              share::schema::ObSchemaGetterGuard &schema_guard,
                              const share::schema::ObTableSchema *&table_schema);
  static int get_column_names(const share::schema::ObTableSchema *table_schema,
                              common::ObIAllocator &allocator,
                              table::ObTableLoadArray<common::ObString> &column_names);
public:
  ObTableLoadSchema();
  ~ObTableLoadSchema();
  void reset();
  int init(uint64_t tenant_id, uint64_t table_id);
  bool is_valid() const { return is_inited_; }
  TO_STRING_KV(K_(table_name), K_(is_partitioned_table), K_(is_heap_table), K_(has_autoinc_column),
               K_(has_identity_column), K_(rowkey_column_count), K_(store_column_count),
               K_(collation_type), K_(column_descs), K_(is_inited));
private:
  int init_table_schema(const share::schema::ObTableSchema *table_schema);
public:
  common::ObArenaAllocator allocator_;
  common::ObString table_name_;
  bool is_partitioned_table_;
  bool is_heap_table_;
  bool has_autoinc_column_;
  bool has_identity_column_;
  int64_t rowkey_column_count_;
  // column count in store, does not contain virtual generated columns
  int64_t store_column_count_;
  common::ObCollationType collation_type_;
  int64_t schema_version_;
  // if it is a heap table, it contains hidden primary key column
  // does not contain virtual generated columns
  common::ObArray<share::schema::ObColDesc> column_descs_;
  common::ObArray<share::schema::ObColDesc> multi_version_column_descs_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  table::ObTableLoadArray<table::ObTableLoadPartitionId> partition_ids_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
