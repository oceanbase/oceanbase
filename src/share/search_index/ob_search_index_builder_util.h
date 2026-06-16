/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_GIN_INDEX_BUILDER_UTIL_H_
#define OCEANBASE_SHARE_GIN_INDEX_BUILDER_UTIL_H_

#include "object/ob_object.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace share { namespace schema { class ObSchemaGuardWrapper; }}

namespace rootserver
{
class ObDDLService;
} // namespace rootserver

namespace share
{
class ObSearchIndexBuilderUtil
{
public:
    static int generate_search_index_name(
        obrpc::ObCreateIndexArg &arg,
        ObIAllocator *allocator);
    static int append_search_index_arg(
        const obrpc::ObCreateIndexArg &index_arg,
        ObIAllocator *allocator,
        ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
    static int set_search_index_table_columns(
        const obrpc::ObCreateIndexArg &arg,
        const ObTableSchema &data_schema,
        ObTableSchema &index_schema);
    static int check_single_layer_array_for_search_index(
        const share::schema::ObColumnSchemaV2 &column_schema,
        ObIAllocator *allocator,
        bool &is_supported);
    static int get_dropping_search_data_index_invisiable_index_schema(
        const uint64_t tenant_id,
        const ObTableSchema &index_table_schema,
        share::schema::ObSchemaGuardWrapper &schema_guard_wrapper,        common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas);
    static int get_search_index_column_name(
        const ObTableSchema &data_table_schema,
        const ObTableSchema &index_table_schema,
        ObIArray<ObString> &col_names);
private:
  static int add_search_index_column(
      const ObTableSchema &data_schema,
      ObTableSchema &index_schema,
      const int64_t tenant_id,
      const char *name,
      ObObjType type,
      const int64_t length,
      const bool is_binary,
      const bool is_rowkey,
      int64_t &rowkey_pos);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_GIN_INDEX_BUILDER_UTIL_H_
