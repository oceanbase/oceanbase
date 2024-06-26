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

#define USING_LOG_PREFIX STORAGE_FTS
#include <regex>
#include "ob_fts_index_builder_util.h"
#include "ob_index_builder_util.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_plugin_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;

namespace share
{

int ObFtsIndexBuilderUtil::append_fts_rowkey_doc_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg fts_rowkey_doc_arg;
  if (OB_ISNULL(allocator) ||
      !(is_fts_index(index_arg.index_type_) || is_multivalue_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(fts_rowkey_doc_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to fts rowkey doc arg", K(ret));
  } else if (FALSE_IT(fts_rowkey_doc_arg.index_option_.parser_name_.reset())) {
  } else if (FALSE_IT(fts_rowkey_doc_arg.index_type_ =
                        INDEX_TYPE_ROWKEY_DOC_ID_LOCAL)) {
  } else if (OB_FAIL(generate_fts_aux_index_name(fts_rowkey_doc_arg, allocator))) {
    LOG_WARN("failed to generate fts aux index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(fts_rowkey_doc_arg))) {
    LOG_WARN("failed to push back fts rowkey doc arg", K(ret));
  }
  return ret;
}

int ObFtsIndexBuilderUtil::append_fts_doc_rowkey_arg(
    const obrpc::ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg fts_doc_rowkey_arg;
  // NOTE index_arg.index_type_ is fts doc rowkey
  if (OB_ISNULL(allocator) ||
      !(is_fts_index(index_arg.index_type_) ||
        is_multivalue_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(fts_doc_rowkey_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to fts rowkey doc arg", K(ret));
  } else {
    fts_doc_rowkey_arg.index_option_.parser_name_.reset();
    if (is_local_fts_index(index_arg.index_type_) ||
        is_local_multivalue_index(index_arg.index_type_)) {
      fts_doc_rowkey_arg.index_type_ = INDEX_TYPE_DOC_ID_ROWKEY_LOCAL;
    } else if (is_global_fts_index(index_arg.index_type_)) {
      fts_doc_rowkey_arg.index_type_ = INDEX_TYPE_DOC_ID_ROWKEY_GLOBAL;
    } else if (is_global_local_fts_index(index_arg.index_type_)) {
      fts_doc_rowkey_arg.index_type_ = INDEX_TYPE_DOC_ID_ROWKEY_GLOBAL_LOCAL_STORAGE;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_fts_aux_index_name(fts_doc_rowkey_arg, allocator))) {
    LOG_WARN("failed to generate fts aux index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(fts_doc_rowkey_arg))) {
    LOG_WARN("failed to push back fts doc rowkey arg", K(ret));
  }
  return ret;
}

int ObFtsIndexBuilderUtil::append_fts_index_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg fts_index_arg;
  if (OB_ISNULL(allocator) ||
      !share::schema::is_fts_index(index_arg.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg));
  } else if (OB_FAIL(fts_index_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to fts index arg", K(ret));
  } else {
    if (is_local_fts_index(index_arg.index_type_)) {
      fts_index_arg.index_type_ = INDEX_TYPE_FTS_INDEX_LOCAL;
    } else if (is_global_fts_index(index_arg.index_type_)) {
      fts_index_arg.index_type_ = INDEX_TYPE_FTS_INDEX_GLOBAL;
    } else if (is_global_local_fts_index(index_arg.index_type_)) {
      fts_index_arg.index_type_ = INDEX_TYPE_FTS_INDEX_GLOBAL_LOCAL_STORAGE;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_fts_parser_name(fts_index_arg, allocator))) {
    LOG_WARN("fail to generate fts parser name", K(ret));
  } else if (OB_FAIL(generate_fts_aux_index_name(fts_index_arg, allocator))) {
    LOG_WARN("failed to generate fts aux index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(fts_index_arg))) {
    LOG_WARN("failed to push back fts index arg", K(ret));
  }
  return ret;
}

int ObFtsIndexBuilderUtil::append_fts_doc_word_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg fts_doc_word_arg;
  if (OB_ISNULL(allocator) ||
      !share::schema::is_fts_index(index_arg.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg));
  } else if (OB_FAIL(fts_doc_word_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to fts doc word arg", K(ret));
  } else {
    if (is_local_fts_index(index_arg.index_type_)) {
      fts_doc_word_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_LOCAL;
    } else if (is_global_fts_index(index_arg.index_type_)) {
      fts_doc_word_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_GLOBAL;
    } else if (is_global_local_fts_index(index_arg.index_type_)) {
      fts_doc_word_arg.index_type_ = INDEX_TYPE_FTS_DOC_WORD_GLOBAL_LOCAL_STORAGE;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_fts_parser_name(fts_doc_word_arg, allocator))) {
    LOG_WARN("fail to generate fts parser name", K(ret));
  } else if (OB_FAIL(generate_fts_aux_index_name(fts_doc_word_arg, allocator))) {
    LOG_WARN("failed to generate fts aux index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(fts_doc_word_arg))) {
    LOG_WARN("failed to push back fts doc word arg", K(ret));
  }
  return ret;
}

int ObFtsIndexBuilderUtil::fts_doc_word_schema_exist(
    uint64_t tenant_id,
    uint64_t database_id,
    ObSchemaGetterGuard &schema_guard,
    const ObString &index_name,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  const int64_t buf_size = OB_MAX_TABLE_NAME_BUF_LENGTH;
  char buf[buf_size] = {0};
  int64_t pos = 0;
  ObString doc_word_index_name;
  const ObTableSchema *fts_doc_word_schema = nullptr;
  if (OB_FAIL(databuff_printf(buf,
                              buf_size,
                              pos,
                              "%.*s_fts_doc_word",
                              index_name.length(),
                              index_name.ptr()))) {
    LOG_WARN("fail to printf fts doc word name str", K(ret), K(index_name));
  } else if (OB_FALSE_IT(doc_word_index_name.assign_ptr(buf, pos))) {
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   database_id,
                                                   doc_word_index_name,
                                                   true/*is_index*/,
                                                   fts_doc_word_schema,
                                                   false/*with_hidden_flag*/,
                                                   true/*is_built_in_index*/))) {
    LOG_WARN("failed to get index schema", K(ret), K(tenant_id));
  } else if (OB_NOT_NULL(fts_doc_word_schema)) {
    is_exist = true;
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_fts_aux_index_name(
    obrpc::ObCreateIndexArg &arg,
    ObIAllocator *allocator)
{
  // TODO: @zhenhan.gzh remove index name postfix, and only take one name in index namespace for fulltext index
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  share::schema::ObIndexType type = arg.index_type_;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (!share::schema::is_fts_index(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    int64_t pos = 0;
    if (share::schema::is_rowkey_doc_aux(type)) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(name_buf,
                                         OB_MAX_TABLE_NAME_LENGTH,
                                         pos,
                                         "fts_rowkey_doc"))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (share::schema::is_doc_rowkey_aux(type)) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(name_buf,
                                         OB_MAX_TABLE_NAME_LENGTH,
                                         pos,
                                         "fts_doc_rowkey"))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (share::schema::is_fts_index_aux(type)) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(name_buf,
                                         OB_MAX_TABLE_NAME_LENGTH,
                                         pos,
                                         "%.*s",
                                         arg.index_name_.length(),
                                         arg.index_name_.ptr()))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else if (share::schema::is_fts_doc_word_aux(type)) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(name_buf,
                                         OB_MAX_TABLE_NAME_LENGTH,
                                         pos,
                                         "%.*s_fts_doc_word",
                                         arg.index_name_.length(),
                                         arg.index_name_.ptr()))) {
        LOG_WARN("failed to print", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, unknown fts index type", K(ret), K(type));
    }
    if (OB_SUCC(ret)) {
      arg.index_name_.assign_ptr(name_buf, static_cast<int32_t>(pos));
    } else {
      LOG_WARN("failed to generate fts aux index name", K(ret));
    }
  }
  return ret;
}

/*
 * this func will also:
 * 1. add cascade flag to corresponding column of data_schema
 * 2. add doc_id, word, word_count column to data_schema
*/
int ObFtsIndexBuilderUtil::adjust_fts_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;
  uint64_t doc_id_col_id = OB_INVALID_ID;
  uint64_t word_col_id = OB_INVALID_ID;
  uint64_t word_count_col_id = OB_INVALID_ID;
  uint64_t doc_len_col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *existing_doc_id_col = nullptr;
  const ObColumnSchemaV2 *existing_word_col = nullptr;
  const ObColumnSchemaV2 *existing_word_count_col = nullptr;
  const ObColumnSchemaV2 *existing_doc_length_col = nullptr;
  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;
  bool is_rowkey_doc = false;
  bool is_doc_rowkey = false;
  bool is_fts_index = false;
  bool is_doc_word = false;
  if (!data_schema.is_valid() || !share::schema::is_fts_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (FALSE_IT(is_rowkey_doc = share::schema::is_rowkey_doc_aux(index_type))) {
  } else if (FALSE_IT(is_doc_rowkey = share::schema::is_doc_rowkey_aux(index_type))) {
  } else if (FALSE_IT(is_fts_index = share::schema::is_fts_index_aux(index_type))) {
  } else if (FALSE_IT(is_doc_word = share::schema::is_fts_doc_word_aux(index_type))) {
  } else if (OB_FAIL(check_ft_cols(&index_arg, data_schema))) {
    LOG_WARN("ft cols check failed", K(ret));
  } else if (OB_FAIL(get_doc_id_col(data_schema, existing_doc_id_col))) {
    LOG_WARN("failed to get doc id col", K(ret));
  } else if (OB_FAIL(get_word_segment_col(data_schema, &index_arg, existing_word_col))) {
    LOG_WARN("failed to get word segment col", K(ret));
  } else if (OB_FAIL(get_doc_length_col(data_schema, &index_arg, existing_doc_length_col))) {
    LOG_WARN("fail to get document length column", K(ret));
  } else if (OB_FAIL(get_word_cnt_col(data_schema, &index_arg, existing_word_count_col))) {
    LOG_WARN("failed to get word cnt col", K(ret));
  } else {
    ObColumnSchemaV2 *generated_doc_id_col = nullptr;
    ObColumnSchemaV2 *generated_word_col = nullptr;
    ObColumnSchemaV2 *generated_doc_len_col = nullptr;
    ObColumnSchemaV2 *generated_word_count_col = nullptr;
    if (OB_ISNULL(existing_doc_id_col)) { // need to generate doc id col
      doc_id_col_id = available_col_id++;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_doc_id_column(&index_arg,
                                                doc_id_col_id,
                                                data_schema,
                                                generated_doc_id_col))) {
        LOG_WARN("failed to generate doc id column", K(ret));
      } else if (OB_FAIL(gen_columns.push_back(generated_doc_id_col))) {
        LOG_WARN("failed to push back doc id col", K(ret));
      }
    }
    if (is_rowkey_doc || is_doc_rowkey) {
    } else if (is_fts_index || is_doc_word) {
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_word_col)) {
        word_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_word_segment_column(&index_arg,
                                                        word_col_id,
                                                        data_schema,
                                                        generated_word_col))) {
          LOG_WARN("failed to generate word segment column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_word_col))) {
          LOG_WARN("failed to push back word column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_word_count_col)) {
        word_count_col_id = available_col_id++;
        if (OB_FAIL(generate_word_count_column(&index_arg,
                                               word_count_col_id,
                                               data_schema,
                                               generated_word_count_col))) {
          LOG_WARN("failed to generate word count column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_word_count_col))) {
          LOG_WARN("failed to push back word count column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_doc_length_col)) {
        doc_len_col_id = available_col_id++;
        if (OB_FAIL(generate_doc_length_column(&index_arg,
                                               doc_len_col_id,
                                               data_schema,
                                               generated_doc_len_col))) {
          LOG_WARN("fail to generate document length column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_doc_len_col))) {
          LOG_WARN("fail to push back generated document length", K(ret));
        }
      }
    }
    if (is_rowkey_doc || is_doc_rowkey) {
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                           existing_doc_id_col,
                                           generated_doc_id_col))) {
        LOG_WARN("failed to push back doc id col", K(ret));
      } else if (OB_FAIL(adjust_fts_arg(&index_arg,
                                        data_schema,
                                        allocator,
                                        tmp_cols))) {
        LOG_WARN("failed to append fts_index arg", K(ret));
      }
    } else if (is_fts_index || is_doc_word) {
      if (OB_FAIL(ret)) {
      } else if (is_fts_index) {
        if (OB_FAIL(push_back_gen_col(tmp_cols,
                                      existing_word_col,
                                      generated_word_col))) {
          LOG_WARN("failed to push back word col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_doc_id_col,
                                             generated_doc_id_col))) {
          LOG_WARN("failed to push back doc id col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_word_count_col,
                                             generated_word_count_col))) {
          LOG_WARN("failed to push back word count col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_doc_length_col,
                                             generated_doc_len_col))) {
          LOG_WARN("fail to push back document length column", K(ret));
        } else if (OB_FAIL(adjust_fts_arg(&index_arg,
                                          data_schema,
                                          allocator,
                                          tmp_cols))) {
          LOG_WARN("failed to append fts_index arg", K(ret));
        }
      } else if (is_doc_word) {
        if (OB_FAIL(push_back_gen_col(tmp_cols,
                                      existing_doc_id_col,
                                      generated_doc_id_col))) {
          LOG_WARN("failed to push back doc id col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_word_col,
                                             generated_word_col))) {
          LOG_WARN("failed to push back word col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_word_count_col,
                                             generated_word_count_col))) {
          LOG_WARN("failed to push back word count col", K(ret));
        } else if (OB_FAIL(push_back_gen_col(tmp_cols,
                                             existing_doc_length_col,
                                             generated_doc_len_col))) {
          LOG_WARN("fail to push back document length column", K(ret));
        } else if (OB_FAIL(adjust_fts_arg(&index_arg,
                                          data_schema,
                                          allocator,
                                          tmp_cols))) {
          LOG_WARN("failed to append fts_index arg", K(ret));
        }
      }
    }
  }
  FLOG_INFO("adjust fts arg finished", K(index_arg));
  return ret;
}

int ObFtsIndexBuilderUtil::set_fts_rowkey_doc_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      arg.store_columns_.count() != 1 ||
      !share::schema::is_rowkey_doc_aux(arg.index_type_)) {
    // expect only doc id column in store columns
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema),
        K(arg.store_columns_.count()), K(arg.index_type_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add rowkey columns as index column of fts rowkey doc table
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      const ObColumnSortItem &rowkey_col_item = arg.index_columns_.at(i);
      const ObString &rowkey_col_name = rowkey_col_item.column_name_;
      if (rowkey_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(rowkey_col_name));
      } else if (OB_ISNULL(rowkey_column =
                      data_schema.get_column_schema(rowkey_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       rowkey_col_name.length(),
                       rowkey_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", rowkey_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(
                                               rowkey_column,
                                               true/*is_index_column*/,
                                               true/*is_rowkey*/,
                                               arg.index_columns_.at(i).order_type_,
                                               row_desc,
                                               index_schema,
                                               false/*is_hidden*/,
                                               false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "rowkey_column", *rowkey_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());

      // 2. add doc id column to fts rowkey doc table
      const ObColumnSchemaV2 *doc_id_column = nullptr;
      const ObString &doc_id_col_name = arg.store_columns_.at(0);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (doc_id_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(doc_id_col_name));
      } else if (OB_ISNULL(doc_id_column = data_schema.get_column_schema(doc_id_col_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", doc_id_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(doc_id_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", "doc_id_column", *doc_id_column,
            K(row_desc), K(ret));
      } else if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort column", K(ret));
      } else {
        LOG_INFO("succeed to set fts_rowkey_doc table columns", K(index_schema));
      }
    }
  }
  STORAGE_FTS_LOG(DEBUG, "set rowkey doc table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObFtsIndexBuilderUtil::set_fts_doc_rowkey_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      arg.index_columns_.count() != 1 ||
      !share::schema::is_doc_rowkey_aux(arg.index_type_)) {
    // expect only doc id column in index columns
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema),
        K(arg.index_columns_.count()), K(arg.index_type_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add doc id column as index columns of fts doc rowkey table
    const ObColumnSchemaV2 *doc_id_column = nullptr;
    const ObColumnSortItem &doc_id_col_item = arg.index_columns_.at(0);
    const ObString &doc_id_col_name = doc_id_col_item.column_name_;
    if (OB_FAIL(ret)) {
    } else if (doc_id_col_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty", K(ret), K(doc_id_col_name));
    } else if (OB_ISNULL(doc_id_column =
          data_schema.get_column_schema(doc_id_col_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                     doc_id_col_name.length(), doc_id_col_name.ptr());
      LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
               "database_id", data_schema.get_database_id(),
               "table_name", data_schema.get_table_name(),
               "column name", doc_id_col_name, K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::add_column(doc_id_column,
                                                      true/*is_index_column*/,
                                                      true/*is_rowkey*/,
                                                      doc_id_col_item.order_type_,
                                                      row_desc,
                                                      index_schema,
                                                      false/*is_hidden*/,
                                                      false/*is_specified_storing_col*/))) {
      LOG_WARN("add column failed ", "doc_id_column", *doc_id_column,
          "rowkey_order_type", doc_id_col_item.order_type_, K(row_desc), K(ret));
    } else {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());

      // 2. add rowkey column to fts doc rowkey table
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        uint64_t column_id = OB_INVALID_ID;
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          LOG_WARN("get_column_id failed", "index", i, K(ret));
        } else if (OB_ISNULL(rowkey_column = data_schema.get_column_schema(column_id))) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("get_column_schema failed", "table_id", data_schema.get_table_id(),
              K(column_id), K(ret));
        } else if (ob_is_text_tc(rowkey_column->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_WARN("Lob column should not appear in rowkey position",
                   "rowkey_column", *rowkey_column, "order_in_rowkey",
                    rowkey_column->get_order_in_rowkey(), K(row_desc), K(ret));
        } else if (ob_is_extend(rowkey_column->get_data_type()) ||
                   ob_is_user_defined_sql_type(rowkey_column->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_WARN("udt column should not appear in rowkey position",
                   "rowkey_column", *rowkey_column, "order_in_rowkey",
                   rowkey_column->get_order_in_rowkey(), K(row_desc), K(ret));
        } else if (ob_is_json_tc(rowkey_column->get_data_type())) {
          ret = OB_ERR_JSON_USED_AS_KEY;
          LOG_WARN("JSON column cannot be used in key specification.",
                   "rowkey_column", *rowkey_column, "order_in_rowkey",
                   rowkey_column->get_order_in_rowkey(), K(row_desc), K(ret));
        } else if (OB_FAIL(ObIndexBuilderUtil::add_column(rowkey_column,
                                                          false/*is_index_column*/,
                                                          false/*is_rowkey*/,
                                                          rowkey_column->get_order_in_rowkey(),
                                                          row_desc,
                                                          index_schema,
                                                          false/*is_hidden*/,
                                                          false/*is_specified_storing_col*/))) {
          LOG_WARN("add column failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort column", K(ret));
      } else {
        LOG_INFO("succeed to set fts_doc_rowkey table columns", K(index_schema));
      }
    }
  }
  STORAGE_FTS_LOG(DEBUG, "set fts doc rowkey table columns", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObFtsIndexBuilderUtil::set_fts_index_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      (!share::schema::is_fts_index_aux(arg.index_type_) &&
      !share::schema::is_fts_doc_word_aux(arg.index_type_)) ||
      arg.index_columns_.count() != 2 ||
      arg.store_columns_.count() != 2) {
    // expect word col, doc id col in index_columns,
    // expect worc count, doc length col in store_columns.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(arg.index_type_),
        K(arg.index_columns_.count()), K(arg.store_columns_.count()),
        K(arg.index_columns_), K(arg.store_columns_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add word col, doc id col to fts index table
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *fts_column = nullptr;
      const ObColumnSortItem &fts_col_item = arg.index_columns_.at(i);
      const ObString &fts_col_name = fts_col_item.column_name_;
      if (fts_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(fts_col_name));
      } else if (OB_ISNULL(fts_column = data_schema.get_column_schema(fts_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       fts_col_name.length(), fts_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", fts_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(fts_column,
                                                        true/*is_index_column*/,
                                                        true/*is_rowkey*/,
                                                        arg.index_columns_.at(i).order_type_,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "fts_column", *fts_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
    }
    // 2. add word count, doc length col to fts index table
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.store_columns_.count(); ++i) {
      const ObColumnSchemaV2 *store_column = nullptr;
      const ObString &store_column_name = arg.store_columns_.at(i);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (OB_UNLIKELY(store_column_name.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(store_column_name));
      } else if (OB_ISNULL(store_column = data_schema.get_column_schema(store_column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", store_column_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(store_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", K(store_column), K(row_desc), K(ret));
      }
    }
    if (FAILEDx(index_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret));
    } else {
      LOG_INFO("succeed to set fts index table columns", K(index_schema));
    }
  }
  STORAGE_FTS_LOG(DEBUG, "set fts index table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObFtsIndexBuilderUtil::check_ft_cols(
    const ObCreateIndexArg *index_arg,
    ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *col_schema = NULL;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema.is_valid()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
    const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
    if (column_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty", K(ret), K(column_name));
    } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                     column_name.ptr());
    } else if (!col_schema->is_string_type() ||
               col_schema->get_meta_type().is_blob()) {
      ret = OB_ERR_BAD_FT_COLUMN;
      LOG_USER_ERROR(OB_ERR_BAD_FT_COLUMN, column_name.length(), column_name.ptr());
    } else {
      col_schema->add_column_flag(GENERATED_DEPS_CASCADE_FLAG);
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::adjust_fts_arg(
    ObCreateIndexArg *index_arg, // not const since index_columns_ will be modified
    const ObTableSchema &data_schema,
    ObIAllocator &allocator,
    const ObIArray<const ObColumnSchemaV2 *> &fts_cols)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema));
  } else {
    const ObIndexType &index_type = index_arg->index_type_;
    const bool is_rowkey_doc = share::schema::is_rowkey_doc_aux(index_arg->index_type_);
    const bool is_doc_rowkey = share::schema::is_doc_rowkey_aux(index_arg->index_type_);
    const bool is_fts_index = share::schema::is_fts_index_aux(index_arg->index_type_);
    const bool is_doc_word = share::schema::is_fts_doc_word_aux(index_arg->index_type_);
    if ((is_rowkey_doc && fts_cols.count() != 1) ||
        (is_doc_rowkey && fts_cols.count() != 1) ||
        (is_fts_index && fts_cols.count() != 4) ||
        (is_doc_word && fts_cols.count() != 4) ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fts cols count not expected", K(ret), K(index_type), K(fts_cols));
    } else {
      index_arg->index_columns_.reuse();
      index_arg->store_columns_.reuse();

      if (is_rowkey_doc) {
        // 1. add rowkey column to arg->index_columns
        const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
          ObColumnSortItem rowkey_column;
          const ObColumnSchemaV2 *rowkey_col = NULL;
          uint64_t column_id = OB_INVALID_ID;
          if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
            LOG_WARN("get_column_id failed", "index", i, K(ret));
          } else if (NULL == (rowkey_col = data_schema.get_column_schema(column_id))) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            LOG_WARN("get_column_schema failed", "table_id",
                data_schema.get_table_id(), K(column_id), K(ret));
          } else if (OB_FAIL(ob_write_string(allocator,
                                             rowkey_col->get_column_name_str(),
                                             rowkey_column.column_name_))) {
            //to keep the memory lifetime of column_name consistent with index_arg
            LOG_WARN("deep copy column name failed", K(ret));
          } else if (OB_FAIL(index_arg->index_columns_.push_back(rowkey_column))) {
            LOG_WARN("failed to push back rowkey column", K(ret));
          }
        }
        // 2. add doc id column to arg->store_columns
        const ObColumnSchemaV2 *doc_id_col = fts_cols.at(0);
        ObString doc_id_col_name;
        if (FAILEDx(ob_write_string(allocator, doc_id_col->get_column_name_str(), doc_id_col_name))) {
          LOG_WARN("fail to deep copy doc id column name", K(ret));
        } else if (OB_FAIL(index_arg->store_columns_.push_back( doc_id_col_name))) {
          LOG_WARN("failed to push back doc id column", K(ret));
        }
      } else if (is_doc_rowkey) {
        // add doc id column to arg->index_columns
        ObColumnSortItem doc_id_column;
        const ObColumnSchemaV2 *doc_id_col = fts_cols.at(0);
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(doc_id_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fts_col is null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator,
                                           doc_id_col->get_column_name_str(),
                                           doc_id_column.column_name_))) {
          //to keep the memory lifetime of column_name consistent with index_arg
          LOG_WARN("deep copy column name failed", K(ret));
        } else if (OB_FAIL(index_arg->index_columns_.push_back(doc_id_column))) {
          LOG_WARN("failed to push back doc id column", K(ret));
        }

      } else if (is_fts_index) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inner_adjust_fts_arg(index_arg,
                                                fts_cols,
                                                OB_FTS_INDEX_TABLE_INDEX_COL_CNT,
                                                allocator))) {
          LOG_WARN("failed to inner_adjust_fts_arg", K(ret));
        }
      } else if (is_doc_word) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inner_adjust_fts_arg(index_arg,
                                                fts_cols,
                                                OB_FTS_DOC_WORD_TABLE_INDEX_COL_CNT,
                                                allocator))) {
          LOG_WARN("failed to inner_adjust_fts_arg", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::inner_adjust_fts_arg(
    obrpc::ObCreateIndexArg *fts_arg,
    const ObIArray<const ObColumnSchemaV2 *> &fts_cols,
    const int index_column_cnt,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fts_arg) ||
      (!share::schema::is_fts_index_aux(fts_arg->index_type_) &&
       !share::schema::is_fts_doc_word_aux(fts_arg->index_type_)) ||
      fts_cols.count() != index_column_cnt + 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KPC(fts_arg), K(fts_cols.count()),
        K(index_column_cnt));
  } else {
    // 1. add doc id column, word column to arg->index_columns
    for (int64_t i = 0; OB_SUCC(ret) && i < index_column_cnt; ++i) {
      ObColumnSortItem fts_column;
      const ObColumnSchemaV2 *fts_col = fts_cols.at(i);
      if (OB_ISNULL(fts_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fts_col is null", K(ret), K(i));
      } else if (OB_FAIL(ob_write_string(allocator,
              fts_col->get_column_name_str(),
              fts_column.column_name_))) {
        //to keep the memory lifetime of column_name consistent with index_arg
        LOG_WARN("deep copy column name failed", K(ret));
      } else if (OB_FAIL(fts_arg->index_columns_.push_back(fts_column))) {
        LOG_WARN("failed to push back index column", K(ret));
      }
    }
    // 2. add word count column to arg->store_columns
    const ObColumnSchemaV2 *word_count_col = fts_cols.at(index_column_cnt);
    ObString word_count_col_name;
    if (FAILEDx(ob_write_string(allocator, word_count_col->get_column_name_str(), word_count_col_name))) {
      LOG_WARN("fail to deep copy word count column name", K(ret));
    } else if (OB_FAIL(fts_arg->store_columns_.push_back(word_count_col_name))) {
      LOG_WARN("failed to push back word count column", K(ret));
    }
    // 3. add document length column to arg->store_columns
    const ObColumnSchemaV2 *doc_length_col = fts_cols.at(index_column_cnt + 1);
    ObString doc_length_col_name;
    if (FAILEDx(ob_write_string(allocator, doc_length_col->get_column_name_str(), doc_length_col_name))) {
      LOG_WARN("fail to deep copy doc length column", K(ret));
    } else if (OB_FAIL(fts_arg->store_columns_.push_back(doc_length_col_name))) {
      LOG_WARN("fail to push document length column", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_doc_id_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&doc_id_col)
{
  int ret = OB_SUCCESS;
  doc_id_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_doc_id_col_name(col_name_buf,
                                               OB_MAX_COLUMN_NAME_LENGTH,
                                               name_pos))) {
    LOG_WARN("failed to construct doc id col name", K(ret));
  } else if (OB_FAIL(check_fts_gen_col(data_schema,
                                       col_id,
                                       col_name_buf,
                                       name_pos,
                                       col_exists))) {
    LOG_WARN("check doc id col failed", K(ret));
  } else if (!col_exists) {
    const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
    const ObColumnSchemaV2 *col_schema = nullptr;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], ft_expr_def) {
      MEMSET(ft_expr_def, 0, sizeof(ft_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(databuff_printf(ft_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "DOC_ID()"))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      } else {
        ObColumnSchemaV2 column_schema;
        ObObj default_value;
        default_value.set_varchar(ft_expr_def, static_cast<int32_t>(def_pos));
        column_schema.set_rowkey_position(0); //非主键列
        column_schema.set_index_position(0); //非索引列
        column_schema.set_tbl_part_key_pos(0); //非partition key
        column_schema.set_tenant_id(data_schema.get_tenant_id());
        column_schema.set_table_id(data_schema.get_table_id());
        column_schema.set_column_id(col_id);
        column_schema.add_column_flag(GENERATED_DOC_ID_COLUMN_FLAG);
        column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
        column_schema.set_is_hidden(true);
        column_schema.set_nullable(false);
        column_schema.set_data_type(ObVarcharType);
        column_schema.set_data_length(OB_DOC_ID_COLUMN_BYTE_LENGTH);
        column_schema.set_collation_type(CS_TYPE_BINARY);
        column_schema.set_prev_column_id(UINT64_MAX);
        column_schema.set_next_column_id(UINT64_MAX);
        if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
          LOG_WARN("set column name failed", K(ret));
        } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
          LOG_WARN("set orig default value failed", K(ret));
        } else if (OB_FAIL(column_schema.set_cur_default_value(default_value))) {
          LOG_WARN("set current default value failed", K(ret));
        } else if (OB_FAIL(data_schema.add_column(column_schema))) {
          LOG_WARN("add column schema to data table failed", K(ret));
        } else {
          doc_id_col = data_schema.get_column_schema(column_schema.get_column_id());
          if (OB_ISNULL(doc_id_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("generate doc id col failed", K(ret), KP(doc_id_col));
          } else {
            LOG_INFO("succeed to generate doc id column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_word_segment_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&word_segment_col)
{
  int ret = OB_SUCCESS;
  word_segment_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_word_segment_col_name(index_arg,
                                                     data_schema,
                                                     col_name_buf,
                                                     OB_MAX_COLUMN_NAME_LENGTH,
                                                     name_pos))) {
    LOG_WARN("failed to construct word segment col name", K(ret));
  } else if (OB_FAIL(check_fts_gen_col(data_schema,
                                       col_id,
                                       col_name_buf,
                                       name_pos,
                                       col_exists))) {
    LOG_WARN("check word segment col failed", K(ret));
  } else if (!col_exists) {
    int32_t max_data_length = 0;
    ObCollationType collation_type = CS_TYPE_INVALID;
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], ft_expr_def) {
      MEMSET(ft_expr_def, 0, sizeof(ft_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                         OB_MAX_DEFAULT_VALUE_LENGTH,
                                         def_pos,
                                         "WORD_SEGMENT("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                           OB_MAX_DEFAULT_VALUE_LENGTH,
                                           def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else {
          if (max_data_length < col_schema->get_data_length()) {
            max_data_length = col_schema->get_data_length();
          }
          if (CS_TYPE_INVALID == collation_type) {
            collation_type = col_schema->get_collation_type();
          } else if (collation_type != col_schema->get_collation_type()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "create fulltext index on columns with different collation");
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(ft_expr_def,
                                    OB_MAX_DEFAULT_VALUE_LENGTH,
                                    def_pos,
                                    ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(ft_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObVarcharType);
          column_schema.set_data_length(max_data_length); //生成列的长度和被分词列的最大长度保持一致
          column_schema.set_collation_type(collation_type); //生成列的collation和被分词列的collation保持一致
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            word_segment_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(word_segment_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate word segment col failed", K(ret), KP(word_segment_col));
            } else {
              LOG_INFO("succeed to generate word segment column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_word_count_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&word_count_col)
{
  int ret = OB_SUCCESS;
  word_count_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index_aux(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_word_count_col_name(index_arg,
                                                   data_schema,
                                                   col_name_buf,
                                                   OB_MAX_COLUMN_NAME_LENGTH,
                                                   name_pos))) {
    LOG_WARN("failed to construct word count col name", K(ret));
  } else if (OB_FAIL(check_fts_gen_col(data_schema,
                                       col_id,
                                       col_name_buf,
                                       name_pos,
          col_exists))) {
    LOG_WARN("check word count col failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], ft_expr_def) {
      MEMSET(ft_expr_def, 0, sizeof(ft_expr_def));
      ObCollationType collation_type = CS_TYPE_INVALID;
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                         OB_MAX_DEFAULT_VALUE_LENGTH,
                                         def_pos,
                                         "WORD_COUNT("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                           OB_MAX_DEFAULT_VALUE_LENGTH,
                                           def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else if (CS_TYPE_INVALID == collation_type) {
          collation_type = col_schema->get_collation_type();
        } else if (collation_type != col_schema->get_collation_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create fulltext index on columns with different collation");
        }
      }
      if (OB_SUCC(ret)) {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(ft_expr_def,
                                    OB_MAX_DEFAULT_VALUE_LENGTH,
                                    def_pos,
                                    ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(ft_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_FTS_WORD_COUNT_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObUInt64Type);
          column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add word_count column schema to data table failed", K(ret));
          } else {
            word_count_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(word_count_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate word count col failed", K(ret), KP(word_count_col));
            } else {
              LOG_INFO("succeed to generate word count column", K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_doc_length_column(
    const obrpc::ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&doc_length_col)
{
  int ret = OB_SUCCESS;
  doc_length_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg)
      || OB_UNLIKELY(!share::schema::is_fts_index_aux(index_arg->index_type_))
      || OB_UNLIKELY(!data_schema.is_valid())
      || OB_UNLIKELY(col_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_doc_length_col_name(index_arg,
                                                   data_schema,
                                                   col_name_buf,
                                                   OB_MAX_COLUMN_NAME_LENGTH,
                                                   name_pos))) {
    LOG_WARN("fail to construct document length column name", K(ret));
  } else if (OB_FAIL(check_fts_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("fail to check document count", K(ret), K(col_id));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], ft_expr_def) {
      MEMSET(ft_expr_def, 0, sizeof(ft_expr_def));
      ObCollationType collation_type = CS_TYPE_INVALID;
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                         OB_MAX_DEFAULT_VALUE_LENGTH,
                                         def_pos,
                                         "DOC_LENGTH("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(ft_expr_def,
                                           OB_MAX_DEFAULT_VALUE_LENGTH,
                                           def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else if (CS_TYPE_INVALID == collation_type) {
          collation_type = col_schema->get_collation_type();
        } else if (collation_type != col_schema->get_collation_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create fulltext index on columns with different collation");
        }
      }
      if (OB_SUCC(ret)) {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(ft_expr_def,
                                    OB_MAX_DEFAULT_VALUE_LENGTH,
                                    def_pos,
                                    ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(ft_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObUInt64Type);
          column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add word_count column schema to data table failed", K(ret));
          } else {
            doc_length_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(doc_length_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate word count col failed", K(ret), KP(doc_length_col));
            } else {
              LOG_INFO("succeed to generate document length column", K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::construct_doc_id_col_name(
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf,
                                buf_len,
                                name_pos,
                                OB_DOC_ID_COLUMN_NAME))) {
      LOG_WARN("print generate column name failed", K(ret));
    } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::construct_word_segment_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf,
                                buf_len,
                                name_pos,
                                OB_WORD_SEGMENT_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf,
                                         buf_len,
                                         name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::construct_word_count_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf,
                                buf_len,
                                name_pos,
                                OB_WORD_COUNT_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf,
                                         buf_len,
                                         name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::construct_doc_length_col_name(
    const obrpc::ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg)
      || OB_UNLIKELY(!share::schema::is_fts_index(index_arg->index_type_))
      || OB_UNLIKELY(!data_schema.is_valid())
      || OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(index_arg), K(data_schema), K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf,
                                buf_len,
                                name_pos,
                                OB_DOC_LENGTH_COLUMN_NAME_PREFIX))) {
      LOG_WARN("fail to printf document length column", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf,
                                         buf_len,
                                         name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("fail to printf document length column", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::check_fts_gen_col(
    const ObTableSchema &data_schema,
    const uint64_t col_id,
    const char *col_name_buf,
    const int64_t name_pos,
    bool &col_exists)
{
  int ret = OB_SUCCESS;
  col_exists = false;
  if (!data_schema.is_valid() ||
      OB_INVALID_ID == col_id ||
      OB_ISNULL(col_name_buf) ||
      name_pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(col_id),
        KP(col_name_buf), K(name_pos));
  } else {
    // another fulltext index could have created the generated column
    const ObColumnSchemaV2 *ft_col = data_schema.get_column_schema(col_name_buf);
    if (OB_NOT_NULL(ft_col) && ft_col->get_column_id() != col_id) {
      // check the specified column id is consistent with the existed column schema
      ret = OB_ERR_INVALID_COLUMN_ID;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, static_cast<int>(name_pos),
          col_name_buf);
      LOG_WARN("Column id specified by create fulltext index mismatch "
               "with column schema id", K(ret), K(col_id), K(*ft_col));
    } else if (OB_ISNULL(ft_col) && OB_NOT_NULL(data_schema.get_column_schema(col_id))) {
      // check the specified column id is not used by others
      ret = OB_ERR_INVALID_COLUMN_ID;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, static_cast<int>(name_pos),
          col_name_buf);
      LOG_WARN("Column id specified by create fulltext index has been used",
          K(ret), K(col_id));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_NOT_NULL(ft_col)) {
      // the generated colum is created
      col_exists = true;
      if (OB_UNLIKELY(!ft_col->has_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG))) {
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, static_cast<int>(name_pos),
            col_name_buf);
        LOG_WARN("Generate column name has been used", K(ret), K(*ft_col));
      }
    } else {
      col_exists = false;
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::get_doc_id_col(
    const ObTableSchema &data_schema,
    const ObColumnSchemaV2 *&doc_id_col)
{
  int ret = OB_SUCCESS;
  doc_id_col = nullptr;
  if (!data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(doc_id_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_doc_id_column()) {
        doc_id_col = column_schema;
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::check_fts_or_multivalue_index_allowed(
    ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema));
  } else if (data_schema.is_partitioned_table() && data_schema.is_heap_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create full-text or multi-value index on partition table without primary key");
  }
  return ret;
}

int ObFtsIndexBuilderUtil::get_word_segment_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&word_segment_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  word_segment_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(word_segment_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_word_segment_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          word_segment_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::get_word_cnt_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&word_cnt_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  word_cnt_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_fts_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(word_cnt_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_word_count_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          word_cnt_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::get_doc_length_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&doc_len_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  doc_len_col = nullptr;
  if (OB_UNLIKELY(!data_schema.is_valid())
      || OB_ISNULL(index_arg)
      || OB_UNLIKELY(!share::schema::is_fts_index(index_arg->index_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(doc_len_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_doc_length_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          doc_len_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::push_back_gen_col(
    ObIArray<const ObColumnSchemaV2 *> &cols,
    const ObColumnSchemaV2 *existing_col,
    ObColumnSchemaV2 *generated_col)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(existing_col)) {
    if (OB_FAIL(cols.push_back(existing_col))) {
      LOG_WARN("failed to push back existing col", K(ret));
    }
  } else {
    if (OB_ISNULL(generated_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated col is nullptr", K(ret));
    } else if (OB_FAIL(cols.push_back(generated_col))) {
      LOG_WARN("failed to push back generated col", K(ret));
    }
  }
  return ret;
}

int ObFtsIndexBuilderUtil::generate_fts_parser_name(
    obrpc::ObCreateIndexArg &arg,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  share::schema::ObIndexType type = arg.index_type_;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), KP(allocator));
  } else if (OB_UNLIKELY(!share::schema::is_fts_index_aux(type)
                      && !share::schema::is_fts_doc_word_aux(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_PLUGIN_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc name buffer", K(ret));
  } else {
    share::ObPluginName parser_name;
    storage::ObFTParser parser;
    const char *name_str = nullptr;
    if (arg.index_option_.parser_name_.empty()) {
      name_str = common::OB_DEFAULT_FULLTEXT_PARSER_NAME;
    } else {
      name_str = arg.index_option_.parser_name_.ptr();
    }
    if (OB_FAIL(parser_name.set_name(name_str))) {
      LOG_WARN("fail to set plugin name", K(ret), KCSTRING(name_str));
    } else if (OB_FAIL(OB_FT_PLUGIN_MGR.get_ft_parser(parser_name, parser))) {
      LOG_WARN("fail to get fulltext parser", K(ret), K(parser_name));
    } else if (OB_FAIL(parser.serialize_to_str(name_buf, OB_PLUGIN_NAME_LENGTH))) {
      LOG_WARN("fail to serialize to cstring", K(ret), K(parser));
    } else {
      arg.index_option_.parser_name_ = common::ObString::make_string(name_buf);
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(name_buf)) {
    allocator->free(name_buf);
  }
  return ret;
}

int ObFtsIndexBuilderUtil::get_index_column_ids(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg &arg,
    schema::ColumnReferenceSet &index_column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!share::schema::is_fts_index(arg.index_type_) || !data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg), K(data_schema));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObString &column_name = arg.index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      } else if (OB_FAIL(index_column_ids.add_member(col_schema->get_column_id()))) {
        LOG_WARN("fail to add index column id", K(ret), K(col_schema->get_column_id()));
      }
    }
  }
  return ret;
}
int ObFtsIndexBuilderUtil::check_index_match(
    const schema::ObColumnSchemaV2 &column,
    const schema::ColumnReferenceSet &index_column_ids,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> cascaded_col_ids;
  is_match = false;
  if (OB_UNLIKELY(!column.is_valid() || index_column_ids.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column), K(index_column_ids));
  } else if (OB_FAIL(column.get_cascaded_column_ids(cascaded_col_ids))) {
    LOG_WARN("fail to get cascaded column ids", K(ret), K(column));
  } else if (cascaded_col_ids.count() == index_column_ids.num_members()) {
    bool mismatch = false;
    for (int64_t i = 0; !mismatch && i < cascaded_col_ids.count(); ++i) {
      if (!index_column_ids.has_member(cascaded_col_ids.at(i))) {
        mismatch = true;
      }
    }
    is_match = !mismatch;
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::generate_mulvalue_index_name(
    obrpc::ObCreateIndexArg &arg,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  share::schema::ObIndexType type = arg.index_type_;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (!is_multivalue_index_aux(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    MEMSET(name_buf, 0, OB_MAX_TABLE_NAME_LENGTH);
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(name_buf,
                                OB_MAX_TABLE_NAME_LENGTH,
                                pos,
                                "%.*s",
                                arg.index_name_.length(),
                                arg.index_name_.ptr()))) {
      LOG_WARN("failed to print", K(ret));
    }
    if (OB_SUCC(ret)) {
      arg.index_name_.assign_ptr(name_buf, static_cast<int32_t>(pos));
    } else {
      LOG_WARN("failed to generate multivalue aux index name", K(ret), K(type));
    }
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::construct_mulvalue_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    bool is_budy_column,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !is_multivalue_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);

    const ObColumnSchemaV2 *col_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      bool is_define_mv_expr = false;
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      ObString define_string;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      } else if ((!is_budy_column && col_schema->is_multivalue_generated_column()) ||
        (is_budy_column && col_schema->is_multivalue_generated_array_column())) {
        ObString column_name = col_schema->get_column_name_str();
        if (OB_FAIL(databuff_printf(col_name_buf, OB_MAX_COLUMN_NAME_LENGTH, name_pos,
                                    "%s", column_name.ptr()))) {
          LOG_WARN("column name write failed", K(ret), K(column_name));
        }
      }
    }
  }

  return ret;
}

int ObMulValueIndexBuilderUtil::append_mulvalue_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg multivlaue_arg;
  if (OB_ISNULL(allocator) ||
      !is_multivalue_index(index_arg.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(multivlaue_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to multivalue arg", K(ret));
  } else if (OB_FAIL(generate_mulvalue_index_name(multivlaue_arg, allocator))) {
    LOG_WARN("failed to generate multivalue aux index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(multivlaue_arg))) {
    LOG_WARN("failed to push back multivalue arg", K(ret));
  }
  return ret;
}


int ObMulValueIndexBuilderUtil::is_multivalue_index_type(
  const ObString& column_string,
  bool& is_multi_value_index)
{
  INIT_SUCC(ret);

  char* buf = nullptr;
  if (column_string.length() == 0 || column_string.length() > OB_MAX_COLUMN_NAMES_LENGTH) {
  } else {
    SMART_VAR(char[OB_MAX_COLUMN_NAMES_LENGTH * 2], buf) {
      MEMCPY(buf, column_string.ptr(), column_string.length());
      buf[column_string.length()] = 0;

      std::regex pattern(R"(CAST\s*\(\s*.*\s*as\s*.*\s*array\s*\))", std::regex_constants::icase);
      if (std::regex_match(buf, pattern)) {
        is_multi_value_index = true;
      } else {
        is_multi_value_index = false;
        std::regex pattern1(R"(JSON_QUERY\s*\(\s*.*\s*ASIS\s*.*\s*MULTIVALUE\s*\))", std::regex_constants::icase);
        if (std::regex_match(buf, pattern1)) {
          is_multi_value_index = true;
        }
      }
    }
  }

  return ret;
}

int ObMulValueIndexBuilderUtil::adjust_index_type(const ObString& column_string,
                                          bool& is_multi_value_index,
                                          int* index_keyname)
{
  INIT_SUCC(ret);

  char* buf = nullptr;
  if (OB_ISNULL(index_keyname)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null param input", K(ret));
  } else if (OB_FAIL(is_multivalue_index_type(column_string, is_multi_value_index))) {
    LOG_WARN("failed to resolve index type", K(ret), K(column_string));
  } else if (!is_multi_value_index) {
  } else if (*index_keyname == static_cast<int>(sql::ObDDLResolver::NORMAL_KEY)) {
    *index_keyname = static_cast<int>(sql::ObDDLResolver::MULTI_KEY);
  } else if (*index_keyname == static_cast<int>(sql::ObDDLResolver::UNIQUE_KEY)) {
    *index_keyname = static_cast<int>(sql::ObDDLResolver::MULTI_UNIQUE_KEY);
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::get_mulvalue_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&mulvalue_col,
    const ObColumnSchemaV2 *&budy_mulvalue_col)
{
  int ret = OB_SUCCESS;
  mulvalue_col = nullptr;
  budy_mulvalue_col = nullptr;

  int64_t name_pos = 0;
  int64_t budy_name_pos = 0;

  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  char budy_col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};

  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !is_multivalue_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KP(index_arg));
  } else if (OB_FAIL(construct_mulvalue_col_name(index_arg,
                                                 data_schema,
                                                 false, // not budy column, scalar column
                                                 col_name_buf,
                                                 OB_MAX_COLUMN_NAME_LENGTH,
                                                 name_pos))) {
    if (ret != OB_ERR_KEY_COLUMN_DOES_NOT_EXITS) {
      LOG_WARN("failed to construct multivalue column name", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(construct_mulvalue_col_name(index_arg,
                                                 data_schema,
                                                 true, // is budy column, array column
                                                 budy_col_name_buf,
                                                 OB_MAX_COLUMN_NAME_LENGTH,
                                                 budy_name_pos))) {
    LOG_WARN("failed to construct budy multivalue column name", K(ret));
  } else {
    ObString mulvalue_col_name(name_pos, col_name_buf);
    mulvalue_col = data_schema.get_column_schema(mulvalue_col_name);

    ObString budy_mulvalue_col_name(budy_name_pos, budy_col_name_buf);
    budy_mulvalue_col = data_schema.get_column_schema(budy_mulvalue_col_name);
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::adjust_mulvalue_index_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;
  const ObColumnSchemaV2 *existing_doc_id_col = nullptr;
  const ObColumnSchemaV2 *existing_mulvalue_col = nullptr;
  const ObColumnSchemaV2 *existing_budy_mulvalue_col = nullptr;
  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;

  if (!data_schema.is_valid() || !is_multivalue_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (OB_FAIL(ObFtsIndexBuilderUtil::get_doc_id_col(data_schema, existing_doc_id_col))) {
    LOG_WARN("failed to get doc id col", K(ret));
  } else if (OB_FAIL(get_mulvalue_col(data_schema, &index_arg, existing_mulvalue_col, existing_budy_mulvalue_col))) {
    LOG_WARN("failed to get multivalue col", K(ret));
  } else {
    ObColumnSchemaV2 *generated_doc_id_col = nullptr;
    ObColumnSchemaV2 *generated_mulvalue_col = nullptr;
    ObColumnSchemaV2 *generated_budy_mulvalue_col = nullptr;
    if (OB_ISNULL(existing_doc_id_col)) {
      uint64_t doc_id_col_id = available_col_id++;
      if (OB_FAIL(ObFtsIndexBuilderUtil::generate_doc_id_column(&index_arg,
                                         doc_id_col_id,
                                         data_schema,
                                         generated_doc_id_col))) {
        LOG_WARN("failed to generate doc id column", K(ret));
      } else if (OB_FAIL(gen_columns.push_back(generated_doc_id_col))) {
        LOG_WARN("failed to push back doc id col", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(existing_mulvalue_col)) {
      if (OB_FAIL(build_and_generate_multivalue_column_raw(index_arg, data_schema,
        generated_mulvalue_col, generated_budy_mulvalue_col))) {
        LOG_WARN("failed to build and generate multi value generated column", K(ret));
      } else if (OB_FAIL(gen_columns.push_back(generated_mulvalue_col))) {
        LOG_WARN("failed to push back multi value col", K(ret));
      } else if (OB_FAIL(gen_columns.push_back(generated_budy_mulvalue_col))) {
        LOG_WARN("failed to push back multi value col", K(ret));
      }
    }
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::build_and_generate_multivalue_column_raw(
    ObCreateIndexArg &arg,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&mulvalue_col,
    ObColumnSchemaV2 *&budy_mulvalue_col)
{
  int ret = OB_SUCCESS;
  mulvalue_col = nullptr;
  budy_mulvalue_col = nullptr;

  ObIArray<ObColumnSortItem> &sort_items = arg.index_columns_;
  ObString expr_def_string;

  bool is_oracle_mode = false;
  bool is_add_column = false;
  if (OB_FAIL(data_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check_if_oracle_compat_mode failed", K(ret));
  } else if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("oracle mode create index not supported yet.", K(ret));
  }

  int64_t expr_idx = 0;
  for (; OB_SUCC(ret) && expr_idx < sort_items.count(); ++expr_idx) {
    ObColumnSortItem& sort_item = sort_items.at(expr_idx);
    bool is_multi_value_index = false;
    if (sort_item.prefix_len_ > 0) {
    } else if (!sort_item.is_func_index_) {
    } else if (OB_FAIL(is_multivalue_index_type(sort_item.column_name_, is_multi_value_index))) {
      LOG_WARN("failed to calc index type", K(ret), K(sort_item.column_name_));
    } else if (is_multi_value_index) {
      is_add_column = true;
      expr_def_string = sort_item.column_name_;
      // found multivalue index define, break
      break;
    }
  }

  if (OB_SUCC(ret) && expr_def_string.length() > 0) {
    ObColumnSortItem sort_item = sort_items.at(expr_idx);
    const ObString &index_expr_def = expr_def_string;
    ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
    ObRawExprFactory expr_factory(allocator);

    SMART_VARS_2((sql::ObSQLSessionInfo, session),
                 (sql::ObExecContext, exec_ctx, allocator)) {
      uint64_t tenant_id = data_schema.get_tenant_id();
      const ObTenantSchema *tenant_schema = nullptr;
      ObSchemaGetterGuard guard;
      ObSchemaChecker schema_checker;

      ObRawExpr *expr = nullptr;
      ObColumnSchemaV2 *gen_col = nullptr;
      budy_mulvalue_col = nullptr;
      bool force_rebuild = false;

      if (OB_FAIL(session.init(0 /*default session id*/,
                                0 /*default proxy id*/,
                                &allocator))) {
        LOG_WARN("init session failed", K(ret));
      } else if (OB_FAIL(session.set_default_database(arg.database_name_))) {
        LOG_WARN("failed to set default session default database name", K(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(schema_checker.init(guard))) {
        LOG_WARN("failed to init schema checker", K(ret));
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("get tenant_schema failed", K(ret));
      } else if (OB_FAIL(session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
        LOG_WARN("init tenant failed", K(ret));
      } else if (OB_FAIL(session.load_all_sys_vars(guard))) {
        LOG_WARN("session load system variable failed", K(ret));
      } else if (OB_FAIL(session.load_default_configs_in_pc())) {
        LOG_WARN("session load default configs failed", K(ret));
      } else if (OB_FAIL(build_and_generate_multivalue_column(sort_item,
                                                              expr_factory,
                                                              session,
                                                              data_schema,
                                                              &schema_checker,
                                                              force_rebuild,
                                                              gen_col,
                                                              budy_mulvalue_col))) {
        LOG_WARN("session load default configs failed", K(ret));
      } else {
        ObColumnSortItem& ref_item = arg.index_columns_.at(expr_idx);
        ref_item = sort_item;
        mulvalue_col = gen_col;

        // need add multivalue budy column
        if (OB_ISNULL(budy_mulvalue_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build generate multivalue column failed, budy_mulvalue_col is null", K(ret), KP(budy_mulvalue_col));
        } else if (is_add_column) {
          ObColumnSortItem budy_item;
          budy_item.is_func_index_ = true;
          budy_item.column_name_ = budy_mulvalue_col->get_column_name_str();
          if (OB_FAIL(arg.index_columns_.push_back(budy_item))) {
            LOG_WARN("failed to push back column item.", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObMulValueIndexBuilderUtil::build_and_generate_multivalue_column(
    ObColumnSortItem& sort_item,
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session_info,
    ObTableSchema &table_schema,
    sql::ObSchemaChecker *schema_checker,
    bool force_rebuild,
    ObColumnSchemaV2 *&gen_col,
    ObColumnSchemaV2 *&budy_col)
{
  INIT_SUCC(ret);
  ObRawExpr *expr = nullptr;
  if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(nullptr,
                                                   sort_item.column_name_,
                                                   expr_factory,
                                                   session_info,
                                                   table_schema,
                                                   expr,
                                                   schema_checker,
                                                   ObResolverUtils::CHECK_FOR_FUNCTION_INDEX))) {
    LOG_WARN("build generated column expr failed", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build generated column expr is null", K(ret));
  } else if (!expr->is_sys_func_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multivalue generated expr should be function, not column ref.", K(ret));
  } else {
    //real index expr, so generate hidden generated column in data table schema
    if (OB_FAIL(generate_multivalue_column(*expr, table_schema, schema_checker->get_schema_guard(), force_rebuild, gen_col, budy_col))) {
      LOG_WARN("generate ordinary generated column failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::check_generated_column_expr_str(
        gen_col->get_cur_default_value().get_string(), session_info, table_schema))) {
      LOG_WARN("fail to check printed generated column expr", K(ret));
    } else {
      sort_item.column_name_ = gen_col->get_column_name_str();
      sort_item.is_func_index_ = true;
    }
  }

  return ret;
}

int ObMulValueIndexBuilderUtil::generate_multivalue_column(
    sql::ObRawExpr &expr,
    ObTableSchema &data_schema,
    ObSchemaGetterGuard *schema_guard,
    bool force_rebuild,
    ObColumnSchemaV2 *&gen_col,
    ObColumnSchemaV2 *&gen_budy_col)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 multival_col;
  SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_def_buf) {
    MEMSET(expr_def_buf, 0, sizeof(expr_def_buf));
    int64_t pos = 0;
    ObRawExprPrinter expr_printer(expr_def_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, schema_guard);
    if (OB_FAIL(expr_printer.do_print(&expr, T_NONE_SCOPE, true))) {
      LOG_WARN("print expr definition failed", K(ret));
    } else {
      // add check
      ObString expr_def(pos, expr_def_buf);
      ObColumnSchemaV2 *old_gen_col = NULL;

      size_t expr_str_len = strlen(expr_def_buf);
      expr_def.assign_ptr(expr_def_buf, expr_str_len);

      if (!force_rebuild && OB_FAIL(data_schema.get_generated_column_by_define(expr_def,
                                                             true/*only hidden column*/,
                                                             old_gen_col))) {
        LOG_WARN("get generated column by define failed", K(ret), K(expr_def));
      } else if (old_gen_col != NULL) {
        //got it
        gen_col = old_gen_col;
        gen_budy_col = data_schema.get_column_schema(gen_col->get_column_id());
      } else {
        //need to add new generated column
        ObObj default_value;
        char col_name_buf[OB_MAX_COLUMN_NAMES_LENGTH] = {'\0'};
        pos = 0;
        default_value.set_varchar(expr_def);
        multival_col.set_rowkey_position(0); //非主键列
        multival_col.set_index_position(0); //非索引列
        multival_col.set_tbl_part_key_pos(0); //非partition key
        multival_col.set_tenant_id(data_schema.get_tenant_id());
        multival_col.set_table_id(data_schema.get_table_id());
        multival_col.set_column_id(data_schema.get_max_used_column_id() + 1);
        multival_col.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
        multival_col.add_column_flag(MULTIVALUE_INDEX_GENERATED_COLUMN_FLAG);
        multival_col.set_is_hidden(true);
        if (expr.get_result_type().is_null()) {
          const ObAccuracy varchar_accuracy(0);
          multival_col.set_data_type(ObVarcharType);
          multival_col.set_collation_type(data_schema.get_collation_type());
          multival_col.set_accuracy(varchar_accuracy);
        } else {
          multival_col.set_data_type(expr.get_data_type());
          multival_col.set_collation_type(expr.get_collation_type());
          multival_col.set_accuracy(expr.get_accuracy());
        }
        multival_col.set_prev_column_id(UINT64_MAX);
        multival_col.set_next_column_id(UINT64_MAX);
        ObSEArray<ObRawExpr*, 4> dep_columns;
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(&expr, dep_columns))) {
          LOG_WARN("extract column exprs failed", K(ret), K(expr));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < dep_columns.count(); ++i) {
          const ObRawExpr *dep_column = dep_columns.at(i);
          if (OB_ISNULL(dep_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("deps_column is null");
          } else if (!dep_column->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dep column is invalid", K(ret), KPC(dep_column));
          } else if (OB_FAIL(multival_col.add_cascaded_column_id(
              static_cast<const ObColumnRefRawExpr*>(dep_column)->get_column_id()))) {
            LOG_WARN("add cascaded column id failed", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          //do nothing
        } else if (OB_FAIL(databuff_printf(col_name_buf, OB_MAX_COLUMN_NAMES_LENGTH, pos,
                                           "SYS_NC_mvi_%ld", /*naming rules are compatible with oracle*/
                                           multival_col.get_column_id()))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        } else if (OB_FAIL(multival_col.set_column_name(col_name_buf))) {
          LOG_WARN("set column name failed", K(ret));
        } else if (OB_FAIL(multival_col.set_orig_default_value(default_value))) {
          LOG_WARN("set orig default value failed", K(ret));
        } else if (OB_FAIL(multival_col.set_cur_default_value(default_value))) {
          LOG_WARN("set current default value failed", K(ret));
        } else if (OB_FAIL(data_schema.add_column(multival_col))) {
          LOG_WARN("add column schema to data table failed", K(ret));
        } else {
          gen_col = data_schema.get_column_schema(multival_col.get_column_id());
        }

        ObColumnSchemaV2 multival_arr_col;
        if (FAILEDx(multival_arr_col.assign(multival_col))) {
          LOG_WARN("fail to assign multival arr col", K(ret), K(multival_col));
        } else {
          multival_arr_col.set_column_id(data_schema.get_max_used_column_id() + 1);
          multival_arr_col.del_column_flag(MULTIVALUE_INDEX_GENERATED_COLUMN_FLAG);
          multival_arr_col.add_column_flag(MULTIVALUE_INDEX_GENERATED_ARRAY_COLUMN_FLAG);

          pos = 0;
          ObObj default_value;
          char col_name_buf[OB_MAX_COLUMN_NAMES_LENGTH] = {'\0'};
          snprintf(expr_def_buf + expr_str_len - 1, OB_MAX_DEFAULT_VALUE_LENGTH - (expr_str_len - 1),
                 "%s", " multivalue)");

          expr_str_len = strlen(expr_def_buf);
          expr_def.assign_ptr(expr_def_buf, expr_str_len);
          default_value.set_varchar(expr_def);

          multival_arr_col.set_data_type(ObJsonType);
          multival_arr_col.set_collation_type(CS_TYPE_UTF8MB4_BIN);
          multival_arr_col.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]);

          if (OB_FAIL(databuff_printf(col_name_buf, OB_MAX_COLUMN_NAMES_LENGTH, pos,
                                            "SYS_NC_mvi_arr_%ld", /*naming rules are compatible with oracle*/
                                            multival_arr_col.get_column_id()))) {
            LOG_WARN("print generate column prefix name failed", K(ret));
          } else if (OB_FAIL(multival_arr_col.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(multival_arr_col.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(multival_arr_col.set_cur_default_value(default_value))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(multival_arr_col))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            gen_budy_col = data_schema.get_column_schema(multival_arr_col.get_column_id());
          }
        }
      }
    }
  }
  return ret;
}

int ObMulValueIndexBuilderUtil::inner_adjust_multivalue_arg(
    ObCreateIndexArg &index_arg,
    const ObTableSchema &data_schema,
    ObColumnSchemaV2 *doc_id_col)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSortItem> sort_items;
  ObIAllocator *allocator = index_arg.index_schema_.get_allocator();

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (OB_FAIL(sort_items.assign(index_arg.index_columns_))) {
    LOG_WARN("failed to assign old index columns", K(ret));
  } else {
    index_arg.index_columns_.reuse();
    index_arg.store_columns_.reuse();
  }

  for (int i = 0; OB_SUCC(ret) && i < sort_items.count(); ++i) {
    ObColumnSortItem new_sort_item;
    ObColumnSortItem &sort_item = sort_items.at(i);
    const ObString column_name = sort_item.column_name_;
    const ObColumnSchemaV2 *col_schema = nullptr;

    if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_WARN("failed to get col schema", K(ret), K(column_name));
    } else if (OB_FAIL(ob_write_string(*allocator,
                                       col_schema->get_column_name_str(),
                                       new_sort_item.column_name_))) {
      //to keep the memory lifetime of column_name consistent with index_arg
      LOG_WARN("deep copy column name failed", K(ret));
    } else if (OB_FAIL(index_arg.index_columns_.push_back(new_sort_item))) {
      LOG_WARN("failed to push back index column", K(ret));
    } else if (col_schema->is_multivalue_generated_column()) {
      const ObColumnSchemaV2 *budy_col_schema = nullptr;
      ObColumnSortItem budy_sort_item;
      if (OB_ISNULL(budy_col_schema = data_schema.get_column_schema(col_schema->get_column_id() + 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get budy column", K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator,
                                       budy_col_schema->get_column_name_str(),
                                       budy_sort_item.column_name_))) {
        //to keep the memory lifetime of column_name consistent with index_arg
        LOG_WARN("deep copy column name failed", K(ret));
      } else if (OB_FAIL(index_arg.index_columns_.push_back(budy_sort_item))) {
        LOG_WARN("failed to push back index column", K(ret));
      }
    }
  }

  const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    ObColumnSortItem rowkey_column;
    const ObColumnSchemaV2 *rowkey_col = NULL;
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("get_column_id failed", "index", i, K(ret));
    } else if (NULL == (rowkey_col = data_schema.get_column_schema(column_id))) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("get_column_schema failed", "table_id",
          data_schema.get_table_id(), K(column_id), K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator,
                                        rowkey_col->get_column_name_str(),
                                        rowkey_column.column_name_))) {
      //to keep the memory lifetime of column_name consistent with index_arg
      LOG_WARN("deep copy column name failed", K(ret));
    } else if (OB_FAIL(index_arg.index_columns_.push_back(rowkey_column))) {
      LOG_WARN("failed to push back rowkey column", K(ret));
    }
  }


  ObColumnSortItem tmp_sort_item;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_write_string(*allocator,
                                     doc_id_col->get_column_name_str(),
                                     tmp_sort_item.column_name_))) {
    //to keep the memory lifetime of column_name consistent with index_arg
    LOG_WARN("deep copy column name failed", K(ret));
  } else if (OB_FAIL(index_arg.index_columns_.push_back(tmp_sort_item))) {
    LOG_WARN("failed to push back index column", K(ret));
  }

  return ret;
}

int ObMulValueIndexBuilderUtil::set_multivalue_index_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  if (!data_schema.is_valid()) {
    // expect word col, doc id col in index_columns,
    // expect worc count col in store_columns.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema),
        K(arg.index_columns_.count()), K(arg.store_columns_.count()),
        K(arg.index_columns_), K(arg.store_columns_));
  }

  HEAP_VAR(ObRowDesc, row_desc) {
    common::ObOrderType order_type;
    const ObColumnSchemaV2 *mvi_array_column = nullptr;
    int32_t multi_column_cnt = 0;
    // 2 means : multivalue column, multivalue array column
    bool is_complex_index = arg.index_columns_.count() > 2;
    bool is_real_unique = index_schema.is_unique_index() && !is_complex_index;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *mvi_column = nullptr;
      const ObColumnSortItem &mvi_col_item = arg.index_columns_.at(i);
      order_type = mvi_col_item.order_type_;
      if (OB_ISNULL(mvi_column = data_schema.get_column_schema(mvi_col_item.column_name_))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
            mvi_col_item.column_name_.length(), mvi_col_item.column_name_.ptr());
        LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
            "database_id", data_schema.get_database_id(),
            "table_name", data_schema.get_table_name(),
            "column name", mvi_col_item.column_name_, K(ret));
      } else if (mvi_column->is_rowkey_column()) {
      } else if (!mvi_column->is_multivalue_generated_array_column()) {
        if (OB_FAIL(ObIndexBuilderUtil::add_column(mvi_column,
                                                   true/*is_index_column*/,
                                                   true/*is_rowkey*/,
                                                   order_type,
                                                   row_desc,
                                                   index_schema,
                                                   false/*is_hidden*/,
                                                   false/*is_specified_storing_col*/))) {
          LOG_WARN("add column failed", "mvi_column", *mvi_column, "rowkey_order_type",
              mvi_col_item.order_type_, K(row_desc), K(ret));
        } else if (mvi_column->is_multivalue_generated_column()) {
          multi_column_cnt++;
          if (multi_column_cnt > 1) {
            ret = OB_NOT_MULTIVALUE_SUPPORT;
            LOG_USER_ERROR(OB_NOT_MULTIVALUE_SUPPORT, "more than one multi-valued key part per index");
          }
        }
      } else if (mvi_column->is_multivalue_generated_array_column()) {
        mvi_array_column = mvi_column;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(mvi_array_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get multivalue array column", K(ret));
    } else if (is_real_unique) {
      // json-array column is not index coumn, not rowkey column
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
    }

    bool is_rowkey = !is_real_unique;
    bool is_index_column = is_rowkey;

    const ObColumnSchemaV2 *rowkey_column = nullptr;
    const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      uint64_t column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("get_column_id failed", "index", i, K(ret));
      } else if (OB_ISNULL(rowkey_column = data_schema.get_column_schema(column_id))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", "table_id", data_schema.get_table_id(),
            K(column_id), K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(rowkey_column,
                                                        is_index_column/*is_index_column*/,
                                                        is_rowkey /*is_rowkey*/,
                                                        rowkey_column->get_order_in_rowkey(),
                                                        row_desc,
                                                        index_schema,
                                                        false /*is_hidden*/,
                                                        true /*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", K(ret));
      }
    }

    const ObColumnSchemaV2 *doc_id_col = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::get_doc_id_col(data_schema, doc_id_col))) {
      LOG_WARN("failed to get doc id col", K(ret));
    } else if (OB_ISNULL(doc_id_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get doc id col is null", K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::add_column(doc_id_col,
                                                      is_index_column /*is_index_column*/,
                                                      is_rowkey /*is_rowkey*/,
                                                      order_type,
                                                      row_desc, index_schema,
                                                      false/*is_hidden*/,
                                                      true /*is_specified_storing_col*/))) {
      LOG_WARN("add column failed", "docid column", *doc_id_col, "rowkey_order_type",
               order_type, K(row_desc), K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::add_column(mvi_array_column,
                                                      false/*is_index_column*/,
                                                      false/*is_rowkey*/,
                                                      order_type,
                                                      row_desc,
                                                      index_schema,
                                                      false/*is_hidden*/,
                                                      false/*is_specified_storing_col*/))) {
      LOG_WARN("add column failed", "mvi_array_column", *mvi_array_column, K(row_desc), K(ret));
    }

    if (OB_SUCC(ret) && !is_real_unique) {
      // json-array column is not index coumn, not rowkey column
      index_schema.set_rowkey_column_num(row_desc.get_column_num() - 1);
      index_schema.set_index_column_num(row_desc.get_column_num() - 1);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret));
    } else {
      LOG_INFO("succeed to set multivalue index table columns", K(index_schema));
    }
  }
  return ret;
}


}//end namespace rootserver
}//end namespace oceanbase
