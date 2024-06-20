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

#ifndef OCEANBASE_SHARE_INDEX_BUILDER_UTIL_H_
#define OCEANBASE_SHARE_INDEX_BUILDER_UTIL_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObRowDesc;
};
namespace obrpc
{
struct ObColumnSortItem;
struct ObCreateIndexArg;
};

namespace sql
{
class ObRawExpr;
}  // namespace sql

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
};
class ObIndexBuilderUtil
{
public:
  static int adjust_expr_index_args(
      obrpc::ObCreateIndexArg &arg,
      share::schema::ObTableSchema &data_schema,
      common::ObIAllocator &allocator,
      common::ObIArray<share::schema::ObColumnSchemaV2*> &gen_columns);
  static int generate_ordinary_generated_column(
      sql::ObRawExpr &expr,
      const ObSQLMode sql_mode,
      share::schema::ObTableSchema &data_schema,
      share::schema::ObColumnSchemaV2 *&gen_col,
      share::schema::ObSchemaGetterGuard *schema_guard,
      const uint64_t index_id = OB_INVALID_ID);
  static int set_index_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema,
      bool check_data_schema = true);
  static void del_column_flags_and_default_value(share::schema::ObColumnSchemaV2 &column);
  static int add_column(
      const share::schema::ObColumnSchemaV2 *data_column,
      const bool is_index,
      const bool is_rowkey,
      const common::ObOrderType order_type,
      common::ObRowDesc &row_desc,
      share::schema::ObTableSchema &table_schema,
      const bool is_hidden,
      const bool is_specified_storing_col);
  static int set_shadow_column_info(
      const ObString &src_column_name,
      const uint64_t src_column_id,
      ObColumnSchemaV2 &shadow_column_schema);
private:
  static const int SPATIAL_MBR_COLUMN_MAX_LENGTH = 32;
  static int generate_prefix_column(
      const obrpc::ObColumnSortItem &sort_item,
      const ObSQLMode sql_mode,
      share::schema::ObTableSchema &data_schema,
      share::schema::ObColumnSchemaV2 *&prefix_col);
  static int adjust_ordinary_index_column_args(
      obrpc::ObCreateIndexArg &arg,
      share::schema::ObTableSchema &data_schema,
      common::ObIAllocator &allocator,
      common::ObIArray<share::schema::ObColumnSchemaV2*> &gen_columns);
  static int add_shadow_pks(
      const share::schema::ObTableSchema &data_schema,
      common::ObRowDesc &row_desc,
      share::schema::ObTableSchema &schema,
      bool check_data_schema = true);
  static int add_shadow_partition_keys(
      const share::schema::ObTableSchema &data_schema,
      common::ObRowDesc &row_desc,
      share::schema::ObTableSchema &schema);
  static int adjust_spatial_args(
      obrpc::ObCreateIndexArg &arg,
      share::schema::ObTableSchema &data_schema,
      common::ObIAllocator &allocator,
      common::ObIArray<share::schema::ObColumnSchemaV2*> &spatial_cols);
  static int generate_spatial_columns(
      const common::ObString &col_name,
      share::schema::ObTableSchema &data_schema,
      common::ObIArray<share::schema::ObColumnSchemaV2*> &spatial_cols);
  static int generate_spatial_cellid_column(
      share::schema::ObColumnSchemaV2 &col_schema,
      share::schema::ObTableSchema &data_schema,
      share::schema::ObColumnSchemaV2 *&cellid_col);
  static int generate_spatial_mbr_column(
    share::schema::ObColumnSchemaV2 &col_schema,
    share::schema::ObTableSchema &data_schema,
    share::schema::ObColumnSchemaV2 *&mbr_col);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_INDEX_BUILDER_UTIL_H_
