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

#ifndef OCEANBASE_SHARE_VEC_INDEX_BUILDER_UTIL_H_
#define OCEANBASE_SHARE_VEC_INDEX_BUILDER_UTIL_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace share
{

class ObVecIndexBuilderUtil
{
public:
  static const int64_t OB_VEC_DELTA_BUFFER_TABLE_INDEX_COL_CNT = 2;         // 辅助表的主键列数
  static const int64_t OB_VEC_INDEX_ID_TABLE_INDEX_COL_CNT = 3;             // 辅助表的主键列数
  static const int64_t OB_VEC_INDEX_SNAPSHOT_DATA_TABLE_INDEX_COL_CNT = 1;  // 辅助表的主键列数
  static const char * ROWKEY_VID_TABLE_NAME;
  static const char * VID_ROWKEY_TABLE_NAME;
  static const char * DELTA_BUFFER_TABLE_NAME_SUFFIX;
  static const char * INDEX_ID_TABLE_NAME_SUFFIX;
  static const char * SNAPSHOT_DATA_TABLE_NAME_SUFFIX;

public:
  static int append_vec_rowkey_vid_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_vec_vid_rowkey_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_vec_delta_buffer_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      const sql::ObSQLSessionInfo *session_info,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_vec_index_id_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_vec_index_snapshot_data_arg(
      const obrpc::ObCreateIndexArg &arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);

  static int check_vec_index_allowed(
      ObTableSchema &data_schema);

  static int adjust_vec_args(
      obrpc::ObCreateIndexArg &index_arg,
      ObTableSchema &data_schema,
      ObIAllocator &allocator,
      ObIArray<ObColumnSchemaV2 *> &gen_columns);
  static int set_vec_rowkey_vid_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_vec_vid_rowkey_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_vec_delta_buffer_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_vec_index_id_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_vec_index_snapshot_data_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int generate_vec_index_name(
      common::ObIAllocator *allocator,
      const share::schema::ObIndexType type,
      const ObString &index_name,
      ObString &new_index_name);
  static int get_vec_table_schema_by_name(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const int64_t tenant_id,
      const int64_t database_id,
      const ObString &index_name, /* domain index name */
      const share::schema::ObIndexType index_type,
      ObIAllocator *allocator,
      const ObTableSchema *&index_schema);
  static int get_vector_index_prefix(
      const ObTableSchema &index_schema,
      ObString &prefix);
private:
  static int check_vec_cols(
    const obrpc::ObCreateIndexArg *index_arg,
    ObTableSchema &data_schema);
  static int get_vec_vid_col(
    const ObTableSchema &data_schema,
    const ObColumnSchemaV2 *&vid_col);
  static int get_vec_type_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&type_col);
  static int get_vec_vector_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&vector_col);
  static int get_vec_scn_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&scn_col);
  static int get_vec_key_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&key_col);
  static int get_vec_data_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&data_col);
   static int check_index_match(
    const schema::ObColumnSchemaV2 &column,
    const schema::ColumnReferenceSet &index_column_ids,
    bool &is_match);
  static int push_back_gen_col(
    ObIArray<const ObColumnSchemaV2 *> &cols,
    const ObColumnSchemaV2 *existing_col,
    ObColumnSchemaV2 *generated_col);
  static int generate_vid_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&vid_col);
  static int generate_type_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&type_col);
  static int generate_vector_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&vector_col);
  static int generate_scn_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&scn_col);
  static int generate_key_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&key_col);
  static int generate_data_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&data_col);
  static int construct_vid_col_name(
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int construct_type_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int construct_vector_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int construct_scn_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int construct_key_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int construct_data_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos);
  static int adjust_vec_arg(
    obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    ObIAllocator &allocator,
    const ObIArray<const ObColumnSchemaV2 *> &vec_cols);
  static int inner_adjust_vec_arg(
    obrpc::ObCreateIndexArg *vec_arg,
    const ObIArray<const ObColumnSchemaV2 *> &vec_cols,
    const int index_column_cnt,   // 辅助表的主键列数
    ObIAllocator *allocator);
  static int check_vec_gen_col(
    const ObTableSchema &data_schema,
    const uint64_t col_id,
    const char *col_name_buf,
    const int64_t name_pos,
    bool &col_exists);
  static int get_index_column_ids(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg &arg,
    schema::ColumnReferenceSet &index_column_ids);
  static bool is_part_key_column_exist(
    const ObTableSchema &index_schema,
    const ObColumnSchemaV2 &part_key_col);
  static int set_part_key_columns(
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema);
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_VEC_INDEX_BUILDER_UTIL_H_
