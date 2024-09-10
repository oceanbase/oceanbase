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


#ifndef OCEANBASE_SHARE_VECTOR_INDEX_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/schema/ob_table_schema.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace share
{

class ObVectorIndexUtil final
{
public:
  static int parser_params_from_string(
      const ObString &origin_string,
      ObVectorIndexHNSWParam &param);
  static int insert_index_param_str(
      const ObString &new_add_param,
      ObIAllocator &allocator,
      ObString &current_index_param);
  static int get_index_name_prefix(
      const schema::ObTableSchema &index_schema,
      ObString &prefix);
  static int check_column_has_vector_index(
      const ObTableSchema &data_table_schema,
      ObSchemaGetterGuard &schema_guard,
      const int64_t col_id,
      bool &is_column_has_vector_index);
  static int get_vector_index_column_name(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      ObIArray<ObString> &col_names);
  static int get_vector_index_column_id(
      const ObTableSchema &data_table_schema,
      const ObTableSchema &index_table_schema,
      ObIArray<uint64_t> &col_ids);
  static int get_vector_index_column_dim(
      const ObTableSchema &index_table_schema,
      int64_t &dim);
  static int get_vector_index_tid(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const ObIndexType index_type,
      const int64_t col_id,
      uint64_t &tid);
  static int get_vector_dim_from_extend_type_info(
      const ObIArray<ObString> &extend_type_info,
      int64_t &dim);
  static int generate_new_index_name(
      ObIAllocator &allocator,
      ObString &new_index_name);
  static int generate_switch_index_names(
      const ObString &old_domain_index_name,
      const ObString &new_domain_index_name,
      ObIAllocator &allocator,
      ObIArray<ObString> &old_table_names,
      ObIArray<ObString> &new_table_names);
  static int update_index_tables_status(
      const int64_t tenant_id,
      const int64_t database_id,
      const ObIArray<ObString> &old_table_names,
      const ObIArray<ObString> &new_table_names,
      rootserver::ObDDLOperator &ddl_operator,
      ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      ObIArray<ObTableSchema> &table_schemas);
  static int update_index_tables_attributes(
      const int64_t tenant_id,
      const int64_t database_id,
      const int64_t data_table_id,
      const int64_t expected_update_table_cnt,
      const ObIArray<ObString> &old_table_names,
      const ObIArray<ObString> &new_table_names,
      rootserver::ObDDLOperator &ddl_operator,
      ObSchemaGetterGuard &schema_guard,
      common::ObMySQLTransaction &trans,
      ObIArray<ObTableSchema> &table_schemas);
  static int generate_index_schema_from_exist_table(
      const int64_t tenant_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      rootserver::ObDDLService &ddl_service,
      const obrpc::ObCreateIndexArg &create_index_arg,
      const ObTableSchema &data_table_schema,
      ObTableSchema &new_index_schema);
  static bool has_multi_index_on_same_column(
      ObIArray<uint64_t> &vec_index_cols,
      const uint64_t col_id);
};

// For vector index snapshot write data
class ObVecIdxSnapshotDataWriteCtx final
{
public:
  ObVecIdxSnapshotDataWriteCtx()
    : ls_id_(), data_tablet_id_(), lob_meta_tablet_id_(), lob_piece_tablet_id_(),
      vals_()
  {}
  ~ObVecIdxSnapshotDataWriteCtx() {}
  ObLSID& get_ls_id() { return ls_id_; }
  const ObLSID& get_ls_id() const { return ls_id_; }
  ObTabletID& get_data_tablet_id() { return data_tablet_id_; }
  const ObTabletID& get_data_tablet_id() const { return data_tablet_id_; }
  ObTabletID& get_lob_meta_tablet_id() { return lob_meta_tablet_id_; }
  const ObTabletID& get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  ObTabletID& get_lob_piece_tablet_id() { return lob_piece_tablet_id_; }
  const ObTabletID& get_lob_piece_tablet_id() const { return lob_piece_tablet_id_; }
  ObIArray<ObString>& get_vals() { return vals_; }
  void reset();
  TO_STRING_KV(K(ls_id_), K(data_tablet_id_), K(lob_meta_tablet_id_), K(lob_piece_tablet_id_), K(vals_));
public:
  ObLSID ls_id_;
  ObTabletID data_tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;
  ObArray<ObString> vals_;
};

}
}

#endif