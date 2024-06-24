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

#ifndef OCEANBASE_SHARE_FTS_INDEX_BUILDER_UTIL_H_
#define OCEANBASE_SHARE_FTS_INDEX_BUILDER_UTIL_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_schema_checker.h"

namespace oceanbase
{
namespace share
{
class ObMulValueIndexBuilderUtil;

class ObFtsIndexBuilderUtil
{
  friend class ObMulValueIndexBuilderUtil;
public:
  static const int64_t OB_FTS_INDEX_TABLE_INDEX_COL_CNT = 2;
  static const int64_t OB_FTS_DOC_WORD_TABLE_INDEX_COL_CNT = 2;
public:
  static int append_fts_rowkey_doc_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_fts_doc_rowkey_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_fts_index_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int append_fts_doc_word_arg(
      const obrpc::ObCreateIndexArg &index_arg,
      ObIAllocator *allocator,
      ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
  static int fts_doc_word_schema_exist(
      uint64_t tenant_id,
      uint64_t database_id,
      ObSchemaGetterGuard &schema_guard,
      const ObString &index_name,
      bool &is_exist);
  static int generate_fts_aux_index_name(
      obrpc::ObCreateIndexArg &arg,
      ObIAllocator *allocator);
  static int adjust_fts_args(
      obrpc::ObCreateIndexArg &index_arg,
      ObTableSchema &data_schema, // not const since will add column to data schema
      ObIAllocator &allocator,
      ObIArray<ObColumnSchemaV2 *> &gen_columns);
  static int set_fts_rowkey_doc_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_fts_doc_rowkey_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int set_fts_index_table_columns(
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &index_schema);
  static int get_doc_id_col(
      const ObTableSchema &data_schema,
      const ObColumnSchemaV2 *&doc_id_col);
  static int check_fts_or_multivalue_index_allowed(
      ObTableSchema &data_schema);
private:
  static int check_ft_cols(
      const obrpc::ObCreateIndexArg *index_arg,
      ObTableSchema &data_schema); // not const since will add cascade flag
  static int adjust_fts_arg(
      obrpc::ObCreateIndexArg *index_arg, // not const since index_columns_ will be modified
      const ObTableSchema &data_schema,
      ObIAllocator &allocator,
      const ObIArray<const ObColumnSchemaV2 *> &fts_cols);
  static int inner_adjust_fts_arg(
      obrpc::ObCreateIndexArg *fts_arg,
      const ObIArray<const ObColumnSchemaV2 *> &fts_cols,
      const int index_column_cnt,
      ObIAllocator &allocator);
  static int generate_doc_id_column(
      const obrpc::ObCreateIndexArg *index_arg,
      const uint64_t col_id,
      ObTableSchema &data_schema, // not const since will add column to data schema
      ObColumnSchemaV2 *&doc_id_col);
  static int generate_word_segment_column(
      const obrpc::ObCreateIndexArg *index_arg,
      const uint64_t col_id,
      ObTableSchema &data_schema, // not const since will add column to data schema
      ObColumnSchemaV2 *&word_segment_col);
  static int generate_word_count_column(
      const obrpc::ObCreateIndexArg *index_arg,
      const uint64_t col_id,
      ObTableSchema &data_schema, // not const since will add column to data schema
      ObColumnSchemaV2 *&word_count_col);
  static int generate_doc_length_column(
      const obrpc::ObCreateIndexArg *index_arg,
      const uint64_t col_id,
      ObTableSchema &data_schema, // not const since will add column to data schema
      ObColumnSchemaV2 *&doc_length_col);
  static int construct_doc_id_col_name(
      char *col_name_buf,
      const int64_t buf_len,
      int64_t &name_pos);
  static int construct_word_segment_col_name(
      const obrpc::ObCreateIndexArg *index_arg,
      const ObTableSchema &data_schema,
      char *col_name_buf,
      const int64_t buf_len,
      int64_t &name_pos);
  static int construct_word_count_col_name(
      const obrpc::ObCreateIndexArg *index_arg,
      const ObTableSchema &data_schema,
      char *col_name_buf,
      const int64_t buf_len,
      int64_t &name_pos);
  static int construct_doc_length_col_name(
      const obrpc::ObCreateIndexArg *index_arg,
      const ObTableSchema &data_schema,
      char *col_name_buf,
      const int64_t buf_len,
      int64_t &name_pos);
  static int check_fts_gen_col(
      const ObTableSchema &data_schema,
      const uint64_t col_id,
      const char *col_name_buf,
      const int64_t name_pos,
      bool &col_exists);
  static int get_word_segment_col(
      const ObTableSchema &data_schema,
      const obrpc::ObCreateIndexArg *index_arg,
      const ObColumnSchemaV2 *&word_segment_col);
  static int get_word_cnt_col(
      const ObTableSchema &data_schema,
      const obrpc::ObCreateIndexArg *index_arg,
      const ObColumnSchemaV2 *&word_cnt_col);
  static int get_doc_length_col(
      const ObTableSchema &data_schema,
      const obrpc::ObCreateIndexArg *index_arg,
      const ObColumnSchemaV2 *&doc_len_col);
  static int push_back_gen_col(
      ObIArray<const ObColumnSchemaV2 *> &cols,
      const ObColumnSchemaV2 *existing_col,
      ObColumnSchemaV2 *generated_col);
  static int generate_fts_parser_name(
      obrpc::ObCreateIndexArg &arg,
      ObIAllocator *allocator);
  static int get_index_column_ids(
      const ObTableSchema &data_schema,
      const obrpc::ObCreateIndexArg &arg,
      schema::ColumnReferenceSet &index_column_ids);
  static int check_index_match(
      const schema::ObColumnSchemaV2 &column,
      const schema::ColumnReferenceSet &index_column_ids,
      bool &is_match);
};

class ObMulValueIndexBuilderUtil
{
public:
 static int generate_mulvalue_index_name(
   obrpc::ObCreateIndexArg &arg,
   ObIAllocator *allocator);
 static int construct_mulvalue_col_name(
   const obrpc::ObCreateIndexArg *index_arg,
   const ObTableSchema &data_schema,
   bool is_budy_column,
   char *col_name_buf,
   const int64_t buf_len,
   int64_t &name_pos);
 static int append_mulvalue_arg(
   const obrpc::ObCreateIndexArg &index_arg,
   ObIAllocator *allocator,
   ObIArray<obrpc::ObCreateIndexArg> &index_arg_list);
 static int is_multivalue_index_type(
   const ObString& column_string,
   bool& is_multi_value_index);
 static int adjust_index_type(
   const ObString& column_string,
   bool& is_multi_value_index,
   int* index_keyname);
 static int get_mulvalue_col(
   const ObTableSchema &data_schema,
   const obrpc::ObCreateIndexArg *index_arg,
   const ObColumnSchemaV2 *&mulvalue_col,
   const ObColumnSchemaV2 *&budy_col);
 static int adjust_mulvalue_index_args(
   obrpc::ObCreateIndexArg &index_arg,
   ObTableSchema &data_schema, // not const since will add column to data schema
   ObIArray<ObColumnSchemaV2 *> &gen_columns);
 static int build_and_generate_multivalue_column_raw(
   obrpc::ObCreateIndexArg &arg,
   ObTableSchema &data_schema,
   ObColumnSchemaV2 *&mulvalue_col,
   ObColumnSchemaV2 *&budy_col);
 static int build_and_generate_multivalue_column(
   obrpc::ObColumnSortItem& sort_item,
   sql::ObRawExprFactory &expr_factory,
   const sql::ObSQLSessionInfo &session_info,
   ObTableSchema &table_schema,
   sql::ObSchemaChecker *schema_checker,
   bool force_rebuild,
   ObColumnSchemaV2 *&gen_col,
   ObColumnSchemaV2 *&budy_col);
 static int generate_multivalue_column(
    sql::ObRawExpr &expr,
    ObTableSchema &data_schema,
    ObSchemaGetterGuard *schema_guard,
    bool force_rebuild,
    ObColumnSchemaV2 *&gen_col,
    ObColumnSchemaV2 *&gen_budy_col);
 static int inner_adjust_multivalue_arg(
   obrpc::ObCreateIndexArg &index_arg,
   const ObTableSchema &data_schema,
   ObColumnSchemaV2 *doc_id_col);
 static int set_multivalue_index_table_columns(
   const obrpc::ObCreateIndexArg &arg,
   const ObTableSchema &data_schema,
   ObTableSchema &index_schema);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_FTS_INDEX_BUILDER_UTIL_H_
