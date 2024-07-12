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

#define USING_LOG_PREFIX SQL_RESV
#include "lib/compress/ob_compressor_pool.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/ob_store_format.h"
#include "lib/charset/ob_charset.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/object/ob_obj_cast.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "ob_sequence_stmt.h"
#include "sql/resolver/ddl/ob_column_sequence_resolver.h"
#include "share/ob_get_compat_mode.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "pl/ob_pl_stmt.h"
#include "share/table/ob_ttl_util.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{
#define LOG_USER_WARN_ONCE(ret_code, args...) \
  do { \
    const ObWarningBuffer *warnings_buf = common::ob_get_tsi_warning_buffer(); \
    if (OB_NOT_NULL(warnings_buf) && warnings_buf->get_total_warning_count() <= 0) { \
      LOG_USER_WARN(ret_code, ##args); \
    } \
  } while(0) \


ObDDLResolver::ObDDLResolver(ObResolverParams &params)
  : ObStmtResolver(params),
    block_size_(OB_DEFAULT_SSTABLE_BLOCK_SIZE),
    consistency_level_(INVALID_CONSISTENCY),
    index_scope_(NOT_SPECIFIED),
    replica_num_(0),
    tablet_size_(-1),
    pctfree_(OB_DEFAULT_PCTFREE),
    tablegroup_id_(OB_INVALID_ID),
    index_attributes_set_(OB_DEFAULT_INDEX_ATTRIBUTES_SET),
    charset_type_(CHARSET_INVALID),
    collation_type_(CS_TYPE_INVALID),
    use_bloom_filter_(false),
    expire_info_(),
    compress_method_(),
    comment_(),
    tablegroup_name_(),
    primary_zone_(),
    row_store_type_(MAX_ROW_STORE),
    store_format_(OB_STORE_FORMAT_INVALID),
    progressive_merge_num_(OB_DEFAULT_PROGRESSIVE_MERGE_NUM),
    storage_format_version_(OB_STORAGE_FORMAT_VERSION_INVALID),
    table_id_(OB_INVALID_ID),
    data_table_id_(OB_INVALID_ID),
    index_table_id_(OB_INVALID_ID),
    virtual_column_id_(OB_INVALID_ID),
    read_only_(false),
    with_rowid_(false),
    table_name_(),
    database_name_(),
    partition_func_type_(PARTITION_FUNC_TYPE_HASH),
    auto_increment_(1),
    index_name_(),
    index_keyname_(NORMAL_KEY),
    global_(true),
    store_column_names_(),
    hidden_store_column_names_(),
    zone_list_(),
    sort_column_array_(),
    storing_column_set_(),
    has_index_using_type_(false),
    index_using_type_(share::schema::USING_BTREE),
    locality_(),
    is_random_primary_zone_(false),
    duplicate_scope_(share::ObDuplicateScope::DUPLICATE_SCOPE_NONE),
    enable_row_movement_(false),
    encryption_(),
    tablespace_id_(OB_INVALID_ID),
    table_dop_(DEFAULT_TABLE_DOP),
    hash_subpart_num_(-1),
    is_external_table_(false),
    ttl_definition_(),
    kv_attributes_(),
    name_generated_type_(GENERATED_TYPE_UNKNOWN),
    have_generate_fts_arg_(false),
    is_set_lob_inrow_threshold_(false),
    lob_inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    auto_increment_cache_size_(0),
    external_table_format_type_(ObExternalFileFormat::INVALID_FORMAT),
    mocked_external_table_column_ids_()
{
  table_mode_.reset();
}

ObDDLResolver::~ObDDLResolver()
{
}

int ObDDLResolver::append_fts_args(
    const ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg *index_arg,
    bool &fts_common_aux_table_exist,
    ObIArray<ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg *> &index_arg_list,
    ObIAllocator *arg_allocator)
{
  int ret = OB_SUCCESS;
  ObSArray<obrpc::ObCreateIndexArg> fts_args;
  if (OB_ISNULL(arg_allocator) || OB_ISNULL(index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arg_allocator), KP(index_arg));
  } else if (OB_FAIL(append_fts_args(resolve_result,
                                     *index_arg,
                                     fts_common_aux_table_exist,
                                     resolve_results,
                                     fts_args,
                                     arg_allocator))) {
    LOG_WARN("failed to append fts args", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < fts_args.count(); ++i) {
    ObCreateIndexArg *index_arg = NULL;
    void *tmp_ptr = NULL;
    if (NULL == (tmp_ptr = (ObCreateIndexArg *)arg_allocator->alloc(
            sizeof(obrpc::ObCreateIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (FALSE_IT(index_arg = new (tmp_ptr) ObCreateIndexArg())) {
    } else if (OB_FAIL(index_arg->assign(fts_args.at(i)))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(index_arg_list.push_back(index_arg))) {
      index_arg->~ObCreateIndexArg();
      arg_allocator->free(index_arg);
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::append_fts_args(
    const ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    bool &fts_common_aux_table_exist,
    ObIArray<ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (!fts_common_aux_table_exist) {
    const int64_t num_fts_args = 4;
    // append fts index aux arg first, keep same logic as build fts index on existing table
    if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_index_arg(index_arg,
                                                            allocator,
                                                            index_arg_list))) {
      LOG_WARN("failed to append fts_index arg", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_rowkey_doc_arg(index_arg,
                                                                        allocator,
                                                                        index_arg_list))) {
      LOG_WARN("failed to append fts_rowkey_doc arg", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_doc_rowkey_arg(index_arg,
                                                                        allocator,
                                                                        index_arg_list))) {
      LOG_WARN("failed to append fts_doc_rowkey arg", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_doc_word_arg(index_arg,
                                                                      allocator,
                                                                      index_arg_list))) {
      LOG_WARN("failed to append fts_doc_word arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_fts_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
    if (OB_SUCC(ret)) {
      fts_common_aux_table_exist = true;
    }
  } else {
    const int64_t num_fts_args = 2;
    if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_index_arg(index_arg,
                                                            allocator,
                                                            index_arg_list))) {
      LOG_WARN("failed to append fts_index arg", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_doc_word_arg(index_arg,
                                                                      allocator,
                                                                      index_arg_list))) {
      LOG_WARN("failed to append fts_doc_word arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_fts_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
  }
  return ret;
}

int ObDDLResolver::append_multivalue_args(
    const ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg *index_arg,
    bool &common_aux_table_exist,
    ObIArray<ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg *> &index_arg_list,
    ObIAllocator *arg_allocator)
{
  int ret = OB_SUCCESS;
  ObSArray<obrpc::ObCreateIndexArg> multivalue_args;
  if (OB_ISNULL(arg_allocator) || OB_ISNULL(index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arg_allocator), KP(index_arg));
  } else if (OB_FAIL(append_multivalue_args(resolve_result,
                                            *index_arg,
                                            common_aux_table_exist,
                                            resolve_results,
                                            multivalue_args,
                                            arg_allocator))) {
    LOG_WARN("failed to append multivalue args", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < multivalue_args.count(); ++i) {
    ObCreateIndexArg *index_arg = NULL;
    void *tmp_ptr = NULL;
    if (NULL == (tmp_ptr = (ObCreateIndexArg *)arg_allocator->alloc(
            sizeof(obrpc::ObCreateIndexArg)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (FALSE_IT(index_arg = new (tmp_ptr) ObCreateIndexArg())) {
    } else if (OB_FAIL(index_arg->assign(multivalue_args.at(i)))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(index_arg_list.push_back(index_arg))) {
      index_arg->~ObCreateIndexArg();
      arg_allocator->free(index_arg);
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::append_multivalue_args(
    const ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    bool &common_aux_table_exist,
    ObIArray<ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  int64_t num_mulvalue_args = 3;

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (!common_aux_table_exist) {
    ObCreateIndexArg tmp_index_arg;
    if (OB_FAIL(tmp_index_arg.assign(index_arg))) {
      LOG_WARN("failed to assign arg", K(ret));
    } else if (FALSE_IT(tmp_index_arg.index_columns_.reuse())) {
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_rowkey_doc_arg(tmp_index_arg,
                                                                 allocator,
                                                                 index_arg_list))) {
      LOG_WARN("failed to append fts_rowkey_doc arg", K(ret));
    } else if (OB_FAIL(ObFtsIndexBuilderUtil::append_fts_doc_rowkey_arg(tmp_index_arg,
                                                                        allocator,
                                                                        index_arg_list))) {
      LOG_WARN("failed to append fts_doc_rowkey arg", K(ret));
    } else if (OB_FAIL(ObMulValueIndexBuilderUtil::append_mulvalue_arg(index_arg,
                                                                       allocator,
                                                                       index_arg_list))) {
      LOG_WARN("failed to append fts_index arg", K(ret));
    }
  } else {
    num_mulvalue_args = 1;
    if (OB_FAIL(ObMulValueIndexBuilderUtil::append_mulvalue_arg(index_arg,
                                                                allocator,
                                                                index_arg_list))) {
      LOG_WARN("failed to append mulvalue index arg", K(ret));
    }
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < num_mulvalue_args; ++i) {
    if (OB_FAIL(resolve_results.push_back(resolve_result))) {
      LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
    }
  }
  if (OB_SUCC(ret)) {
    common_aux_table_exist = true;
  }
  return ret;
}

int ObDDLResolver::append_domain_index_args(
    const ObTableSchema &table_schema,
    const ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg *index_arg,
    bool &common_aux_table_exist,
    ObIArray<ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg *> &index_arg_list,
    ObIAllocator *arg_allocator)
{
  int ret = OB_SUCCESS;

  const ObColumnSchemaV2 *doc_id_col = nullptr;
  if (OB_FAIL(ObFtsIndexBuilderUtil::get_doc_id_col(table_schema,
                                                    doc_id_col))) {
    LOG_WARN("failed to get doc id col", K(ret));
  } else if (OB_NOT_NULL(doc_id_col)) {
    common_aux_table_exist = true;
  }

  if (OB_FAIL(ret)) {
  } else if (is_multivalue_index_aux(index_arg->index_type_)) {
    if (OB_FAIL(ObDDLResolver::append_multivalue_args(resolve_result,
                                                      index_arg,
                                                      common_aux_table_exist,
                                                      resolve_results,
                                                      index_arg_list,
                                                      arg_allocator))) {
      LOG_WARN("failed to append multivalue args", K(ret), K(index_arg->index_type_));
    }
  } else if (is_fts_index(index_arg->index_type_)) {
    if (OB_FAIL(ObDDLResolver::append_fts_args(resolve_result,
                                               index_arg,
                                               common_aux_table_exist,
                                               resolve_results,
                                               index_arg_list,
                                               arg_allocator))) {
      LOG_WARN("failed to append fts args", K(ret));
    }
  }

  return ret;
}

int ObDDLResolver::get_part_str_with_type(
    const bool is_oracle_mode,
    ObPartitionFuncType part_func_type,
    ObString &func_str,
    ObSqlString &part_str)
{
  int ret = OB_SUCCESS;
  ObString type_str;
  if (OB_FAIL(get_part_type_str(is_oracle_mode, part_func_type, type_str))) {
    LOG_WARN("Failed to get part type str", K(ret));
  } else if (OB_FAIL(part_str.append_fmt("%.*s (%.*s)",
                                         type_str.length(),
                                         type_str.ptr(),
                                         func_str.length(),
                                         func_str.ptr()))) {
    LOG_WARN("Failed to append part str", K(ret));
  } else { }//do nothing
  return ret;
}

int ObDDLResolver::get_mv_container_table(
    uint64_t tenant_id,
    const uint64_t mv_container_table_id,
    const share::schema::ObTableSchema *&mv_container_table_schema,
    common::ObString &mv_container_table_name)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_version = 0;
  mv_container_table_schema = nullptr;
  mv_container_table_name.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mv_container_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mv_container_table_id));
  } else if (OB_UNLIKELY(nullptr == schema_checker_ || nullptr == allocator_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("schema checker or allocator can not be NULL", KR(ret), KP(schema_checker_), KP(allocator_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else if (tenant_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview before 4.3 is");
    LOG_WARN("mview not supported before 4.3", KR(ret), K(tenant_id), K(tenant_version));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, mv_container_table_id, mv_container_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(mv_container_table_id));
  } else if (OB_ISNULL(mv_container_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is NULL", KR(ret), K(tenant_id), K(mv_container_table_id));
  } else if (OB_FAIL(ob_write_string(*allocator_, mv_container_table_schema->get_table_name(), mv_container_table_name))) {
    LOG_WARN("fail to deep copy table name", KR(ret));
  }
  return ret;
}

// check whether the column is allowed to be selected as part of primary key.
int ObDDLResolver::check_add_column_as_pk_allowed(const ObColumnSchemaV2 &column_schema) {
  int ret = OB_SUCCESS;
  if (ob_is_text_tc(column_schema.get_data_type())) {
    ret = OB_ERR_WRONG_KEY_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
    SQL_RESV_LOG(WARN, "BLOB, TEXT column can't be primary key", K(ret), K(column_schema));
  } else if (ob_is_roaringbitmap(column_schema.get_data_type())) {
    ret = OB_ERR_WRONG_KEY_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
    SQL_RESV_LOG(WARN, "roaringbitmap column can't be primary key", K(ret), K(column_schema));
  } else if (ob_is_extend(column_schema.get_data_type()) || ob_is_user_defined_sql_type(column_schema.get_data_type())) {
    ret = OB_ERR_WRONG_KEY_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
    SQL_RESV_LOG(WARN, "udt column can't be primary key", K(ret), K(column_schema));
  } else if (ob_is_json_tc(column_schema.get_data_type())) {
    ret = OB_ERR_JSON_USED_AS_KEY;
    LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
    SQL_RESV_LOG(WARN, "JSON column can't be primary key", K(ret), K(column_schema));
  } else if (ob_is_geometry(column_schema.get_data_type())) {
    ret = OB_ERR_SPATIAL_UNIQUE_INDEX;
    LOG_USER_ERROR(OB_ERR_SPATIAL_UNIQUE_INDEX);
    SQL_RESV_LOG(WARN, "GEO column can't be primary key", K(ret), K(column_schema));
  } else if (ObTimestampTZType == column_schema.get_data_type()) {
    ret = OB_ERR_WRONG_KEY_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
    SQL_RESV_LOG(WARN, "TIMESTAMP WITH TIME ZONE column can't be primary key", K(ret), K(column_schema));
  } else if (column_schema.is_generated_column()) {
    ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
    LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
                  "Defining a generated column as primary key");
    SQL_RESV_LOG(WARN, "virtual and stored generated column can't be primary key", K(ret), K(column_schema));
  }
  return ret;
}

/**
 * set default value for primary key
 */
int ObDDLResolver::get_primary_key_default_value(const ObObjType type, ObObj &default_value) {
  int ret = OB_SUCCESS;
  switch (type) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType:
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type:
      default_value.set_int(type, 0);
      break;
    case ObFloatType:
    case ObUFloatType:
      default_value.set_float(0);
      break;
    case ObDoubleType:
    case ObUDoubleType:
      default_value.set_double(0);
      break;
    case ObNumberType: // set as string
    case ObUNumberType:
    case ObNumberFloatType:
    case ObDecimalIntType:
      default_value.set_varchar("0");
      default_value.set_type(ObVarcharType);
      break;
    case ObYearType:
      default_value.set_year(ObTimeConverter::ZERO_YEAR);
      break;
    case ObDateType:
      default_value.set_date(ObTimeConverter::ZERO_DATE);
      break;
    case ObTimeType:
      default_value.set_time(ObTimeConverter::ZERO_TIME);
      break;
    case ObDateTimeType:
      default_value.set_datetime(ObTimeConverter::ZERO_DATETIME);
      break;
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
      default_value.set_otimestamp_value(type, ObOTimestampData());
      break;
    case ObTimestampType:
      default_value.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
      break;
    case ObCharType:
    case ObVarcharType:
    case ObNVarchar2Type:
    case ObNCharType:
      default_value.set_string(type, ObString("").ptr(), 0);
      break;
    case ObRawType:
      default_value.set_raw(ObString("").ptr(), 0);
      break;
    case ObIntervalYMType:
      default_value.set_interval_ym(ObIntervalYMValue());
      break;
    case ObIntervalDSType:
      default_value.set_interval_ds(ObIntervalDSValue());
      break;
    default:
      ret = OB_ERR_ILLEGAL_TYPE;
      SQL_RESV_LOG(WARN, "invalid  type of default value", K(type), K(ret));
      break;
  }
  return ret;
}

int update_datetime_default_value(ObObjParam &default_value, ParseNode &def_val,
    const ObObjType datetime_type, const int64_t action_flag)
{
  int ret = OB_SUCCESS;
  int16_t scale = 0;
  default_value.set_ext(action_flag);
  default_value.set_param_meta();
  if (def_val.value_ < 0) {
    scale = ObAccuracy::DDL_DEFAULT_ACCURACY[datetime_type].get_scale();
    default_value.set_scale(scale);
  } else if (OB_ISNULL(def_val.children_) || OB_UNLIKELY(1 != def_val.num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(def_val.children_), K(def_val.num_child_));
  } else {
    if (NULL != def_val.children_[0]) {
      scale = static_cast<int16_t>(def_val.children_[0]->value_);
    } else if (lib::is_oracle_mode()) {
      scale = ObAccuracy::DDL_DEFAULT_ACCURACY[datetime_type].get_scale();
    } else {
      scale = 0;
    }
    default_value.set_scale(scale);
  }
  return ret;
}

int ObDDLResolver::resolve_default_value(ParseNode *def_node,
                                        //  const ObObjType column_data_type,
                                         ObObjParam &default_value)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  ObString str;
  ObObj val;
  int16_t scale = -1;
  ObIAllocator *name_pool = NULL;
  ParseNode *def_val = NULL;
  if (NULL != def_node) {
    def_val = def_node;
    if (def_node->type_ == T_CONSTR_DEFAULT
      || (def_node->type_ == T_CONSTR_ORIG_DEFAULT)) {
      def_val = def_node->children_[0];
    }
    if (OB_ISNULL(allocator_) || OB_ISNULL(def_val)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "allocator_ or def_val is null", K(allocator_),  K(ret));
    } else {
      name_pool = static_cast<ObIAllocator *>(allocator_);
      if (OB_ISNULL(name_pool)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "name pool is null", K(name_pool),  K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing;
    } else {
      switch (def_val->type_) {
      case T_INT:
        default_value.set_int(def_val->value_);
        default_value.set_param_meta();
        break;
      case T_UINT64:
        default_value.set_uint64(static_cast<uint64_t>(def_val->value_));
        default_value.set_param_meta();
        break;
      case T_NCHAR: //mysql mode
      case T_CHAR:
      case T_VARCHAR:
      case T_LOB:
        tmp_str.assign_ptr(const_cast<char *>(def_val->str_value_),
                           static_cast<int32_t>(def_val->str_len_));
        if ((OB_FAIL(ob_write_string(*name_pool, tmp_str, str)))) {
          SQL_RESV_LOG(WARN, "Can not malloc space for default value", K(ret));
        } else {
          ObCollationType coll = session_info_->get_local_collation_connection();
          if (def_val->type_ == T_NCHAR) {
            ObString charset(strlen("utf8mb4"), "utf8mb4");
            ObCharsetType charset_type = ObCharset::charset_type(charset.trim());
            coll = ObCharset::get_default_collation(charset_type);
          }
          default_value.set_varchar(str);
          default_value.set_collation_type(coll);
          default_value.set_param_meta();
        }
        break;
      case T_RAW:
        tmp_str.assign_ptr(const_cast<char *>(def_val->str_value_),
                           static_cast<int32_t>(def_val->str_len_));
        if ((OB_FAIL(ob_write_string(*name_pool, tmp_str, str)))) {
          SQL_RESV_LOG(WARN, "Can not malloc space for default value", K(ret));
          break;
        }
        default_value.set_raw(str);
        default_value.set_param_meta();
        break;
      case T_HEX_STRING:
        tmp_str.assign_ptr(const_cast<char *>(def_val->str_value_),
                           static_cast<int32_t>(def_val->str_len_));
        if ((OB_FAIL(ob_write_string(*name_pool, tmp_str, str)))) {
          SQL_RESV_LOG(WARN, "Can not malloc space for default value", K(ret));
          break;
        }
        default_value.set_hex_string(str);
        default_value.set_scale(0);
        default_value.set_param_meta();
        break;
      case T_YEAR:
        default_value.set_year(static_cast<uint8_t>(def_val->value_));
        default_value.set_scale(0);
        default_value.set_param_meta();
        break;
      /*
       * mysql5.6 支持这样的语法 c1 date|time|timestamp default date|time|timestamp'xxx'
       */
      case T_DATE: {
        ObString time_str(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
        int32_t time_val = 0;
        ObDateSqlMode date_sql_mode;
        if (OB_ISNULL(session_info_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "session_info_ is null", K(ret));
        } else if (FALSE_IT(date_sql_mode.init(session_info_->get_sql_mode()))) {
        } else if (OB_FAIL(ObTimeConverter::str_to_date(time_str, time_val, date_sql_mode))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATE", to_cstring(time_str));
        } else {
          default_value.set_date(time_val);
          default_value.set_scale(0);
          default_value.set_param_meta();
        }
        break;
      }
      case T_TIME: {
        ObString time_str(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
        int64_t time_val = 0;
        if (OB_FAIL(ObTimeConverter::str_to_time(time_str, time_val, &scale))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIME", to_cstring(time_str));
        } else {
          default_value.set_time(time_val);
          default_value.set_scale(scale);
          default_value.set_param_meta();
        }
        break;
      }
      case T_TIMESTAMP: {
        ObString time_str(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
        int64_t time_val = 0;
        ObDateSqlMode date_sql_mode;
        ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_info_), false);
        if (OB_ISNULL(session_info_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "session_info_ is null", K(ret));
        } else if (FALSE_IT(date_sql_mode.init(session_info_->get_sql_mode()))) {
        } else if (FALSE_IT(date_sql_mode.allow_invalid_dates_ = false)) {
        } else if (OB_FAIL(ObTimeConverter::str_to_datetime(time_str, cvrt_ctx, time_val, &scale, date_sql_mode))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIMESTAMP", to_cstring(time_str));
        } else {
          default_value.set_datetime(time_val);
          default_value.set_scale(scale);
          default_value.set_param_meta();
        }
        break;
      }
      case T_TIMESTAMP_TZ: {
        ObObjType value_type = ObMaxType;
        ObOTimestampData tz_value;
        ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_info_), false);
        ObString time_str(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
        //if (OB_FAIL(ObTimeConverter::str_to_otimestamp(time_str, cvrt_ctx, tmp_type, ot_data))) {
        if (OB_FAIL(ObTimeConverter::literal_timestamp_validate_oracle(time_str, cvrt_ctx, value_type, tz_value))) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_USER_ERROR(OB_INVALID_DATE_VALUE, 9, "TIMESTAMP", to_cstring(time_str));
        } else {
          /* use max scale bug:#18093350 */
          default_value.set_otimestamp_value(value_type, tz_value);
          default_value.set_scale(OB_MAX_TIMESTAMP_TZ_PRECISION);
          default_value.set_param_meta();
        }
        break;
      }
      case T_DATETIME: {
        ObString time_str(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
        int64_t time_val = 0;
        ObTimeConvertCtx cvrt_ctx(TZ_INFO(session_info_), false);
        if (OB_FAIL(ObTimeConverter::literal_date_validate_oracle(time_str, cvrt_ctx, time_val))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATE", to_cstring(time_str));
        } else {
          default_value.set_datetime(time_val);
          default_value.set_scale(OB_MAX_DATE_PRECISION);
          default_value.set_param_meta();
        }
        break;
      }
      case T_FLOAT: {
          int err = 0;
          double value = 0;
          char *endptr = NULL;
          value = ObCharset::strntod(def_val->str_value_,
                                     static_cast<int32_t>(def_val->str_len_),
                                     &endptr,
                                     &err);
          if (EOVERFLOW == err) {
            ret = OB_DATA_OUT_OF_RANGE;
          } else {
            default_value.set_float(static_cast<float>(value));
            default_value.set_param_meta();
          }
          break;
        }
      case T_DOUBLE:
        {
          int err = 0;
          double value = 0;
          char *endptr = NULL;
          value = ObCharset::strntod(def_val->str_value_,
                                     static_cast<int32_t>(def_val->str_len_),
                                     &endptr,
                                     &err);
          if (EOVERFLOW == err) {
            ret = OB_DATA_OUT_OF_RANGE;
          } else {
            default_value.set_double(value);
            default_value.set_param_meta();
          }
          break;
        }
      case T_NUMBER:
      case T_NUMBER_FLOAT:
        { // set as string
          ObString number(static_cast<int32_t>(def_val->str_len_), def_val->str_value_);
          if (OB_FAIL(ob_write_string(*name_pool, number, str))) {
            SQL_RESV_LOG(WARN, "Can not malloc space for default value", K(ret));
            break;
          }
          default_value.set_varchar(str);
          default_value.set_type(ObVarcharType);
          default_value.set_collation_type(ObCharset::get_system_collation());
          default_value.set_param_meta();
          break;
        }
      case T_BOOL:
        default_value.set_bool(def_val->value_ == 1 ? true : false);
        default_value.set_param_meta();
        break;
      case T_NULL:
        default_value.set_type(ObNullType);
        default_value.set_param_meta();
        break;
      case T_FUN_SYS_CUR_TIMESTAMP: {
        ret = update_datetime_default_value(default_value, *def_val, ObTimestampType, ObActionFlag::OP_DEFAULT_NOW_FLAG);
        break;
      }
      case T_INTERVAL_YM:
      case T_INTERVAL_DS:
      case T_NVARCHAR2:
      case T_UROWID: {
        //oracle 模式default值直接把用户输入当做string存储，因此不会走到这个路径
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify default value for interval_ym/interval_ds/urowid");
        break;
      }
      case T_OP_POS:
      case T_OP_NEG: {
        if (OB_FAIL(resolve_sign_in_default_value(
                    name_pool, def_val, default_value, T_OP_NEG == def_val->type_))) {
          SQL_RESV_LOG(WARN, "resolve_sign_in_default_value failed", K(ret), K(def_val->type_));
        }
        break;
      }
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        SQL_RESV_LOG(WARN, "Illegal type of default value",K(ret), K(def_val->type_));
        break;
    }
   }
  } else {
    default_value.set_null();
    default_value.set_param_meta();
  }
  if (OB_SUCC(ret)) {
    _OB_LOG(DEBUG, "resolve default value: %s", to_cstring(default_value));
  }
  return ret;
}

int ObDDLResolver::resolve_sign_in_default_value(
    ObIAllocator *name_pool, ParseNode *def_val, ObObjParam &default_value, const bool is_neg)
{
  int ret = OB_SUCCESS;
  if (is_neg) {
    ObObjParam old_obj;
    ret = resolve_default_value(def_val->children_[0], old_obj);
    if (OB_FAIL(ret)) {
      SQL_RESV_LOG(WARN, "Resolve default const value failed", K(ret));
    } else if (ObIntType == old_obj.get_type()) {
      int64_t value = 0;
      old_obj.get_int(value);
      default_value.set_int(-value);
      default_value.set_param_meta();
    } else if (ObFloatType == old_obj.get_type()) {
      float value = 0.0f;
      old_obj.get_float(value);
      default_value.set_float(-value);
      default_value.set_param_meta();
    } else if (ObDoubleType == old_obj.get_type()) {
      double value = 0.0;
      if (OB_FAIL(old_obj.get_double(value))) {
        SQL_RESV_LOG(WARN, "failed to get double value", K(ret));
      } else {
        default_value.set_double(-value);
        default_value.set_param_meta();
      }
    } else if (T_NUMBER == def_val->children_[0]->type_) {
      ObString str;
      char buffer[number::ObNumber::MAX_PRINTABLE_SIZE];
      if (OB_FAIL(old_obj.get_varchar(str))) {
        SQL_RESV_LOG(WARN, "failed to get varchar value", K(ret));
      } else if (FALSE_IT(snprintf(buffer, sizeof(buffer), "-%.*s", str.length(), str.ptr()))) {
      } else if ((OB_FAIL(ob_write_string(*name_pool,
                                          ObString::make_string(buffer),
                                          str)))) {
        SQL_RESV_LOG(WARN, "Can not malloc space for default value",K(ret));
      } else {
        default_value.set_varchar(str);
        default_value.set_collation_type(ObCharset::get_system_collation());
        default_value.set_param_meta();
      }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "Invalid flag '-' in default value",K(ret));
    }
  } else { // is_pos
    if (OB_FAIL(resolve_default_value(def_val->children_[0], default_value))) {
      SQL_RESV_LOG(WARN, "resolve_default_value failed",K(ret), K(def_val->children_[0]->type_));
    }
  }
  return ret;
}

int ObDDLResolver::deep_copy_str(const ObString &src, ObString &dest)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "fail to get an allocator", K(ret));
  } else if (src.length() > 0) {
    int64_t len = src.length() + 1;
    if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Fail to allocate memory, ", K(ret), K(len));
    } else {
      MEMCPY(buf, src.ptr(), len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len-1));
    }
  } else {
    dest.reset();
  }

  return ret;
}

int ObDDLResolver::set_table_name(const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (allocator_) {
    if (OB_FAIL(ob_write_string(*allocator_, table_name, table_name_))) {
      SQL_RESV_LOG(WARN, "deep copy table name failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "allocator is null", K(ret));
  }
  return ret;
}

int ObDDLResolver::set_database_name(const ObString &database_name)
{
  int ret = OB_SUCCESS;
  if (allocator_) {
    if (OB_FAIL(ob_write_string(*allocator_, database_name, database_name_))) {
      SQL_RESV_LOG(WARN, "deep copy table name failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "allocator is null", K(ret));
  }
  return ret;
}

int ObDDLResolver::set_encryption_name(const ObString &encryption)
{
  int ret = OB_SUCCESS;
  if (allocator_) {
    if (OB_FAIL(ob_write_string(*allocator_, encryption, encryption_))) {
      SQL_RESV_LOG(WARN, "deep copy table name failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "allocator is null", K(ret));
  }
  return ret;
}

int ObDDLResolver::resolve_table_id_pre(ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    ParseNode *option_node = NULL;
    int32_t num = 0;
    if(T_TABLE_OPTION_LIST != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse node", K(ret));
    } else if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "session_info_ is null", K(session_info_), K(ret));
    } else {
      num = node->num_child_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (OB_ISNULL(option_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node is null", K(ret));
      } else if (option_node->type_ == T_TABLE_ID) {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          table_id_ = static_cast<uint64_t>(option_node->children_[0]->value_);
        }
      }
    }
  }
  return ret;
}
int ObDDLResolver::resolve_table_options(ParseNode *node, bool is_index_option)
{
  int ret = OB_SUCCESS;
  if (NULL != node) {
    ParseNode *option_node = NULL;
    int32_t num = 0;
    if(T_TABLE_OPTION_LIST != node->type_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse node", KR(ret), K(node->type_), K(node->num_child_));
    } else if ( OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "session_info_ is null", K(session_info_), K(ret));
    } else {
      num = node->num_child_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      if (OB_ISNULL(option_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node is null", K(ret));
      } else if (OB_FAIL(resolve_table_option(option_node, is_index_option))) {
        SQL_RESV_LOG(WARN, "resolve table option failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (CHARSET_INVALID == charset_type_
        &&  CS_TYPE_INVALID == collation_type_ ) {
      // The database character set and collation affect these aspects of server operation:
      //
      // For CREATE TABLE statements, the database character set and collation are used as default
      // values for table definitions if the table character set and collation are not specified.
      // To override this, provide explicit CHARACTER SET and COLLATE table options.
      const uint64_t tenant_id = session_info_->get_effective_tenant_id();
      ObString database_name;
      uint64_t database_id = OB_INVALID_ID;
      const ObDatabaseSchema *database_schema = NULL;
      if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name_, database_id)))  {
        SQL_RESV_LOG(WARN, "fail to get database_id.", K(ret), K(database_name_), K(tenant_id));
      } else if (OB_FAIL(schema_checker_->get_database_schema(tenant_id, database_id, database_schema))) {
        LOG_WARN("failed to get db schema", K(ret), K(database_id));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. db schema is null", K(ret), K(database_schema));
      } else {
        charset_type_ = database_schema->get_charset_type();
        collation_type_ = database_schema->get_collation_type();
      }
    } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type_, collation_type_))) {
      SQL_RESV_LOG(WARN, "fail to fill collation info", K(ret));
    }
  }
  return ret;
}

/**
 * @param check_column_exist is used for 'alter table add index'/'create index' to ignore schema check
 * check_column_exist default is true
 */
int ObDDLResolver::add_storing_column(const ObString &column_name,
                                      bool check_column_exist,
                                      bool is_hidden,
                                      bool *has_invalid_types)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"schema checker or stmt cat not be null", K(ret));
  }
  if (OB_SUCCESS == ret && check_column_exist) {
    ObColumnSchemaV2 *column_schema = NULL;

    if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
      //create table中的schema manager不含有本张表的schema
      ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      ObTableSchema &tbl_schema = create_table_stmt->get_create_table_arg().schema_;
      if (NULL == (column_schema = tbl_schema.get_column_schema(column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), table_name_.length(), table_name_.ptr());
      } else {
        if (ob_is_text_tc(column_schema->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else if (ob_is_roaringbitmap_tc(column_schema->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else if (ob_is_extend(column_schema->get_data_type())
                  || ob_is_user_defined_sql_type(column_schema->get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        } else if (ob_is_json_tc(column_schema->get_data_type())) {
          ret = OB_ERR_JSON_USED_AS_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column_name.length(), column_name.ptr());
        } else if (ObTimestampTZType == column_schema->get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column_name.length(), column_name.ptr());
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(has_invalid_types)) {
          *has_invalid_types = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "column schema is null", K(ret));
      } else if (column_schema->get_rowkey_position() > 0) {
      //rowkey can't be used as storing column, so ignore it
    } else {
      //do nothing;
    }
   }
  }
  if (OB_SUCC(ret)) {
    ObColumnNameHashWrapper column_name_key(column_name);
    ObColumnNameWrapper column_key(column_name, 0);  // prefix length of storing column is 0
    bool check_prefix_len = true;
    if (is_column_exists(sort_column_array_, column_key, check_prefix_len)) {
      //column exists in sort columns, so ignore it
    } else if (OB_HASH_EXIST == storing_column_set_.exist_refactored(column_name_key)) {
      if (is_hidden) {
        // try add storing column by observer, and column is duplicate, just ignore
      } else {
        ret = OB_ERR_COLUMN_DUPLICATE;
        LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
      }
    } else if (OB_FAIL(storing_column_set_.set_refactored(column_name_key))) {
      SQL_RESV_LOG(WARN, "set column name to storing column set failed", K(ret));
    } else if (!is_hidden && OB_FAIL(store_column_names_.push_back(column_name))) {
      SQL_RESV_LOG(WARN, "add column name failed", K(column_name), K(ret));
    } else if (is_hidden && OB_FAIL(hidden_store_column_names_.push_back(column_name))) {
      SQL_RESV_LOG(WARN, "add column name failed", K(column_name), K(ret));
    }
  }
  return ret;
}


int ObDDLResolver::resolve_file_prefix(ObString &url, ObSqlString &prefix_str, common::ObStorageType &device_type) {
  int ret = OB_SUCCESS;
  ObString tmp_url;
  ObArenaAllocator allocator;
  OZ (ob_write_string(allocator, url, tmp_url));
  ObCharset::caseup(CS_TYPE_UTF8MB4_GENERAL_CI, tmp_url);
  device_type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  ObString tmp_prefix = tmp_url.split_on(':');
  OZ (ob_write_string(allocator, tmp_prefix, tmp_prefix, true));
  if (!tmp_prefix.empty()) {
    OZ (get_storage_type_from_name(tmp_prefix.ptr(), device_type));
  }
  if (device_type == common::ObStorageType::OB_STORAGE_MAX_TYPE) {
    device_type = common::ObStorageType::OB_STORAGE_FILE;
    if (url.empty()) {
      ret = OB_DIR_NOT_EXIST;
      LOG_USER_ERROR(OB_DIR_NOT_EXIST);
    }
  } else {
    const char *ts = get_storage_type_str(device_type);
    url += (strlen(ts) + 3);
  }
  if (OB_SUCC(ret)) {
    ObString prefix;
    const char *ts = get_storage_type_str(device_type);
    CK (params_.allocator_ != NULL);
    OZ (ob_write_string(*params_.allocator_, ObString(ts), prefix));
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, prefix);
    OZ (prefix_str.append(prefix));
    OZ (prefix_str.append("://"));
  }
  return ret;
}
int ObDDLResolver::resolve_table_option(const ParseNode *option_node, const bool is_index_option)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_info_->get_effective_tenant_id();
  ObString database_name;
  uint64_t database_id = OB_INVALID_ID;
  if (OB_ISNULL(stmt_) || OB_ISNULL(allocator_) || OB_ISNULL(schema_checker_)){
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"allocator_ or stmt_ or schema_checker_ cat not be null",K(ret));
  } else {
    database_name = database_name_;
  }
  CHECK_COMPATIBILITY_MODE(session_info_);
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name, database_id)))  {
    SQL_RESV_LOG(WARN, "fail to get database_id.", K(ret), K(database_name), K(tenant_id));
  }
  if (OB_SUCCESS == ret && NULL != option_node) {
    switch (option_node->type_) {
    case T_EXPIRE_INFO: {
//        //not supported in version(1.0)
//        ret = OB_NOT_SUPPORTED;
//        LOG_USER_ERROR(ret, "expire info in version(1.0)");
//        if (stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
//          ret = OB_ERR_PARSE_SQL;
//          SQL_RESV_LOG(WARN, "Expire info can not be specified in index option", K(ret));
//        }
//        const bool is_index_table = false;
//        if (OB_ISNULL(option_node->children_)) {
//          ret = OB_ERR_UNEXPECTED;
//          SQL_RESV_LOG(WARN, "(the children of option_node is null", K(option_node->children_), K(ret));
//        } else if (NULL != option_node->children_[0] && T_NULL == option_node->children_[0]->type_) {
//          //drop expire info
//          expire_info_.assign_ptr(NULL, 0);
//        } else {
//          ObRawExpr *expr = NULL;
//          ObString expire_info;
//          expire_info.assign_ptr(const_cast<char *>(option_node->str_value_),
//                                 static_cast<int32_t>(option_node->str_len_));
//          if (OB_ISNULL(option_node->children_[0])) {
//            ret = OB_ERR_UNEXPECTED;
//            SQL_RESV_LOG(ERROR,"children can't be null", K(ret));
//          } else if (OB_FAIL(ob_write_string(*allocator_, expire_info, expire_info_))) {
//            SQL_RESV_LOG(WARN, "write string failed", K(ret));
//          } else {
//            if (stmt::T_ALTER_TABLE != stmt_->get_stmt_type()) {
//              if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, database_id, table_name_,
//                                                            is_index_table, &tab_schema))) {
//                SQL_RESV_LOG(WARN, "table is not exist", K(tenant_id), K(database_id), K_(table_name), K(ret));
//              } else if (OB_ISNULL(tab_schema)) {
//                ret = OB_ERR_UNEXPECTED;
//                SQL_RESV_LOG(WARN, "tab schema is null", K(tab_schema), K(ret));
//              } else if ((tab_schema->get_progressive_merge_num() > 1 || progressive_merge_num_ > 1)
//                         && tab_schema->get_index_tid_count() > 0) {
//                ret = OB_OP_NOT_ALLOW;
//                SQL_RESV_LOG(WARN, "this progressive merge num > 1 and contain index table", K(ret));
//              } else if (OB_FAIL(resolve_sql_expr(*(option_node->children_[0]), expr, T_EXPIRE_SCOPE))
//                  || OB_FAIL(stmt_->get_condition_exprs().push_back(expr))) {
//                if (OB_ERR_BAD_FIELD_ERROR != ret) {
//                  SQL_RESV_LOG(WARN, "Resolve expire info failed", K(ret));
//                }
//              }
//            } else {
//              //mark alter table option
//              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::EXPIRE_INFO))) {
//                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
//              }
//            }
//          }
//        }
      break;
    }
      case T_BLOCK_SIZE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          const int64_t block_size = option_node->children_[0]->value_;
          if (block_size < MIN_BLOCK_SIZE || block_size > MAX_BLOCK_SIZE) {
            ret = OB_ERR_INVALID_BLOCK_SIZE;
            SQL_RESV_LOG(WARN, "block size should between 1024 and 1048576", K(block_size),
                K(ret));
          } else {
            block_size_ = block_size;
          }
        }
        if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::BLOCK_SIZE))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_REPLICA_NUM: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            replica_num_ = static_cast<int32_t>(option_node->children_[0]->value_);
            if (replica_num_ <= 0 || MAX_REPLICA_NUM < replica_num_) {
              ret = OB_NOT_SUPPORTED;
              SQL_RESV_LOG(WARN, "Invalid replica_num", K_(replica_num), K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "replica number less than 0 or greater than 6");
            }
          }
          if (ret == OB_SUCCESS && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::REPLICA_NUM))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index option should not specify replica num", K(ret));
        }
        break;
      }
      case T_TABLET_SIZE: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            tablet_size_ = option_node->children_[0]->value_;
            if (tablet_size_ < 0 || tablet_size_ & ((1 << 21) - 1)) {
              ret = OB_INVALID_CONFIG;
              SQL_RESV_LOG(WARN, "tablet_size must be a multiple of 2M", K_(tablet_size), K(ret));
            }
          }
          if (ret == OB_SUCCESS && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLET_SIZE))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index option should not specify tablet size", K(ret));
        }
        break;
      }
      case T_PCTFREE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(ret), K(option_node->children_[0]));
        } else {
          pctfree_ = static_cast<int32_t>(option_node->children_[0]->value_);
          if (pctfree_ < 0 || pctfree_ >= OB_MAX_PCTFREE) {
            if (lib::is_oracle_mode()) {
              ret = OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE;
            } else {
              ret = OB_INVALID_CONFIG;
            }
            SQL_RESV_LOG(WARN, "invalid pctfree value", K(ret), K_(pctfree));
          }
        }
        if (ret == OB_SUCCESS && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::PCTFREE))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_PCTUSED: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(ret), K(option_node->children_[0]));
        } else {
          int64_t pctused = static_cast<int32_t>(option_node->children_[0]->value_);
          if (pctused <= 0 || pctused > OB_MAX_PCTUSED) {
            ret = OB_ERR_INVALID_PCTFREE_OR_PCTUSED_VALUE;
            SQL_RESV_LOG(WARN, "invalid pctused value", K(ret), K(pctused));
          }
        }
        break;
      }
      case T_INITRANS: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(ret), K(option_node->children_[0]));
        } else {
          int64_t initrans = static_cast<int32_t>(option_node->children_[0]->value_);
          if (initrans <= 0 || initrans > OB_MAX_TRANS) {
            ret = OB_ERR_INVALID_INITRANS_VALUE;
            SQL_RESV_LOG(WARN, "invalid initrans value", K(ret), K(initrans));
          }
        }
        break;
      }
      case T_MAXTRANS: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(ret), K(option_node->children_[0]));
        } else {
          int64_t maxtrans = static_cast<int32_t>(option_node->children_[0]->value_);
          if (maxtrans < 0 || maxtrans > OB_MAX_TRANS) {
            ret = OB_ERR_INVALID_MAXTRANS_VALUE;
            SQL_RESV_LOG(WARN, "invalid initrans value", K(ret), K(maxtrans));
          }
        }
        break;
      }
      case T_STORAGE_OPTIONS: {
        if (0 == option_node->num_child_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node's num child can not be 0", K(ret), K(option_node->num_child_));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < option_node->num_child_; ++i) {
          if (OB_ISNULL(option_node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null!", K(ret), K(i));
          } else {
            // TODO: 暂时只支持 storage 语法，不支持功能
          }
        }
        break;
      }
      case T_COMPRESSION: {
        if (!is_index_option) {
          ObString tmp_str;
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            tmp_str.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                               static_cast<int32_t>(option_node->children_[0]->str_len_));
            compress_method_ = tmp_str.trim();
          }
          bool is_find = false;
          const char *find_compress_name = NULL;
          for (int i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(common::compress_funcs) && !is_find; i++) {
            if (0 == ObString::make_string(compress_funcs[i]).case_compare(compress_method_)) {
              is_find = true;
              find_compress_name = compress_funcs[i];
            }
          }
          if (OB_FAIL(ret)) {
            //do nothing
          } else if (OB_ISNULL(find_compress_name)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, to_cstring(compress_method_));
          } else if (OB_FAIL(ob_write_string(*allocator_, find_compress_name, compress_method_))) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "write string failed", K(ret));
          } else {}
          if (ret == OB_SUCCESS && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::COMPRESS_METHOD))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify compress method", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify compress method in index option");
        }
        break;
      }
      case T_STORE_FORMAT: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            store_format_ = static_cast<ObStoreFormatType>(option_node->children_[0]->value_);
            if (!ObStoreFormat::is_store_format_valid(store_format_, lib::is_oracle_mode())) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "Unexpected invalid store format value", K_(store_format), K(ret));
            } else if (OB_FAIL(ObDDLResolver::get_row_store_type(tenant_id, store_format_, row_store_type_))) {
              SQL_RESV_LOG(WARN, "fail to get_row_store_type", K(tenant_id), K_(store_format), K(ret));
            }
            if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::STORE_FORMAT))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              } else if (lib::is_oracle_mode()) {
                const char* compress_name = ObStoreFormat::get_store_format_compress_name(store_format_);
                if (OB_ISNULL(compress_name)) {
                  ret = OB_ERR_UNEXPECTED;
                  SQL_RESV_LOG(WARN, "Unexpected null compress_name", K_(store_format), K(ret));
                } else if (OB_FAIL(ob_write_string(*allocator_,
                        ObString::make_string(compress_name), compress_method_))) {
                  ret = OB_ERR_UNEXPECTED;
                  SQL_RESV_LOG(WARN, "write string failed", K(ret));
                } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::COMPRESS_METHOD))) {
                  SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
                }
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify store format", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify store format in index option");
        }
        break;
      }
      case T_PROGRESSIVE_MERGE_NUM: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            progressive_merge_num_ = option_node->children_[0]->value_;
          }
          if (OB_FAIL(ret)) {
            //do nothing
          } else if (progressive_merge_num_ < 0 ||
                     progressive_merge_num_ > MAX_PROGRESSIVE_MERGE_NUM) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "progressive_merge_num");
          } else {
            if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::PROGRESSIVE_MERGE_NUM))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              }
            } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
              ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
              ObTableSchema &tbl_schema = create_table_stmt->get_create_table_arg().schema_;
              if (!tbl_schema.is_user_table()) {
                ret = OB_OP_NOT_ALLOW;
                SQL_RESV_LOG(WARN, "only allow to set progressive merge num for data table", K(ret));
              }
            } else {
              ret = OB_ERR_PARSE_SQL;
              SQL_RESV_LOG(WARN, "PROGRESSIVE_MERGE_NUM can not be specified in creating index", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify progressive merge num", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify progressive merge num in index option");
        }
        break;
      }
      case T_STORAGE_FORMAT_VERSION: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            storage_format_version_ = option_node->children_[0]->value_;
          }
          if (OB_FAIL(ret)) {
            //do nothing
          } else if (storage_format_version_ < OB_STORAGE_FORMAT_VERSION_V1 ||
                     storage_format_version_ >= OB_STORAGE_FORMAT_VERSION_MAX) {
            ret = OB_INVALID_ARGUMENT;
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid storage format version");
          } else {
            if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::STORAGE_FORMAT_VERSION))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              }
            } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
              ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
              ObTableSchema &tbl_schema = create_table_stmt->get_create_table_arg().schema_;
              if (!tbl_schema.is_user_table()) {
                ret = OB_OP_NOT_ALLOW;
                SQL_RESV_LOG(WARN, "only allow to set storage format version for data table", K(ret));
              }
            } else {
              ret = OB_ERR_PARSE_SQL;
              SQL_RESV_LOG(WARN, "STORAGE_FORMAT_VERSION can not be specified in creating index", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should be be specified storage_format_version", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify storage format version in index option");
        }
        break;
      }
      case T_USE_BLOOM_FILTER: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            use_bloom_filter_ = option_node->children_[0]->value_ ? true : false;
          }
          if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::USE_BLOOM_FILTER))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify use bloom filter", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify use bloom filter in index option");
        }
        break;
      }
      case T_INDEX_SCOPE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          index_scope_ = option_node->children_[0]->value_ == 0 ? LOCAL_INDEX : GLOBAL_INDEX;
        }
        if (OB_SUCCESS == ret && GLOBAL_INDEX == index_scope_) {
          if (OB_ISNULL(option_node->children_[1])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[1]), K(ret));
          } else if (0 != STRNCMP("GLOBAL",
               static_cast<const char*>(option_node->children_[1]->str_value_),
               option_node->children_[1]->str_len_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR, "invalid index type!", K(ret));
          }
        }
        if (OB_SUCCESS == ret && LOCAL_INDEX == index_scope_) {
          if (OB_ISNULL(option_node->children_[1])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[1]), K(ret));
          } else if (0 != STRNCMP("LOCAL",
               static_cast<const char*>(option_node->children_[1]->str_value_),
               option_node->children_[1]->str_len_)){
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR, "invalid index type!", K(ret));
          }
        }
        break;
      }
      case T_REVERSE: {
        // 仅支持语法
        break;
      }
      case T_STORING_COLUMN_LIST: {
        ParseNode *cur_node = NULL;
        ObString column_name;
        if (OB_ISNULL(option_node->children_[0]) ||
            T_STORING_COLUMN_LIST != option_node->type_ ||
            option_node->num_child_ <1) {
         ret = OB_ERR_UNEXPECTED;
         SQL_RESV_LOG(ERROR, "invalid node type and node child!", K(ret));
        }
        for (int64_t i = 0; OB_SUCCESS == ret  && i < option_node->num_child_; i++) {
          if(OB_ISNULL(option_node->children_[i])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid node type and node child!", K(ret));
          } else {
            cur_node = option_node->children_[i];
            if (OB_ISNULL(cur_node)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(ERROR, "invalid node type and node child!", K(cur_node), K(ret));
            } else {
              int32_t len = static_cast<int32_t>(cur_node->str_len_);
              column_name = ObString(len, len, cur_node->str_value_);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type() ||
              stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
            bool check_column_exist = false;
            if (OB_FAIL(add_storing_column(column_name, check_column_exist))) {
              SQL_RESV_LOG(WARN, "Add storing column failed", K(ret), K(column_name));
            }
          } else {
            if (OB_FAIL(add_storing_column(column_name))) {
              SQL_RESV_LOG(WARN, "Add storing column failed", K(ret), K(column_name));
            }
            if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE ==stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLEGROUP_NAME))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              }
            }
          }
        }
        break;
      }
      case T_PARSER_NAME: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("option_node child is null");
        } else {
          int32_t str_len = static_cast<int32_t>(option_node->children_[0]->str_len_);
          parser_name_.assign_ptr(option_node->children_[0]->str_value_, str_len);
        }
        break;
      }
      case T_WITH_ROWID: {
        with_rowid_ = true;
        break;
      }
      case T_COMMENT: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          comment_ = ObString(option_node->children_[0]->str_len_, option_node->children_[0]->str_value_);
          if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                        *allocator_, session_info_->get_dtc_params(), comment_))) {
            LOG_WARN("fail to convert comment to utf8", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if(comment_.length() > MAX_TABLE_COMMENT_LENGTH) {
            comment_ = "";
            if(is_index_option) {
              ret = OB_ERR_TOO_LONG_INDEX_COMMENT;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_INDEX_COMMENT, MAX_INDEX_COMMENT_LENGTH);
            } else {
              ret = OB_ERR_TOO_LONG_TABLE_COMMENT;
              LOG_USER_ERROR(OB_ERR_TOO_LONG_TABLE_COMMENT, MAX_TABLE_COMMENT_LENGTH);
            }
          }
        }
        if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type() && !is_index_option) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::COMMENT))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_TABLEGROUP: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            tablegroup_name_.assign_ptr((char *)(option_node->children_[0]->str_value_),
                                        static_cast<int32_t>(option_node->children_[0]->str_len_));
            tablegroup_id_ = OB_INVALID_ID;
            if (is_external_table_ && !tablegroup_name_.trim().empty()) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "Specifying tablegroup on external table");
            }
          }
          if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE ==stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLEGROUP_NAME))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify tablegroup", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify tablegroup in index option");
        }
        break;
      }
      case T_TABLE_MODE: {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          bool is_sync_ddl_user = false;
          ObString table_mode_str(static_cast<int32_t>(option_node->children_[0]->str_len_),
                                  (char *)(option_node->children_[0]->str_value_));
          if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
            LOG_WARN("Failed to check sync_ddl_user", K(ret));
          } else if (is_sync_ddl_user) { // in backup mode
            if (OB_FAIL(ObBackUpTableModeOp::get_table_mode(table_mode_str, table_mode_, tenant_data_version))) {
              LOG_WARN("Failed to get table mode from string", K(ret), K(table_mode_str));
            }
          } else if (0 == table_mode_str.case_compare("normal")) {
            table_mode_.mode_flag_ = TABLE_MODE_NORMAL;
          } else if (0 == table_mode_str.case_compare("queuing")) {
            table_mode_.mode_flag_ = TABLE_MODE_QUEUING;
          } else if (0 == table_mode_str.case_compare("moderate")) {
            table_mode_.mode_flag_ = TABLE_MODE_QUEUING_MODERATE;
          } else if (0 == table_mode_str.case_compare("super")) {
            table_mode_.mode_flag_ = TABLE_MODE_QUEUING_SUPER;
          } else if (0 == table_mode_str.case_compare("extreme")) {
            table_mode_.mode_flag_ = TABLE_MODE_QUEUING_EXTREME;
          } else if (0 == table_mode_str.case_compare("heap_organized_table")) {
            table_mode_.organization_mode_ = TOM_HEAP_ORGANIZED;
            table_mode_.pk_mode_ = TPKM_TABLET_SEQ_PK;
          } else if (0 == table_mode_str.case_compare("index_organized_table")) {
            table_mode_.organization_mode_ = TOM_INDEX_ORGANIZED;
          } else {
            ret = OB_NOT_SUPPORTED;
            int tmp_ret = OB_SUCCESS;
            ObSqlString err_msg;
            if (OB_TMP_FAIL(err_msg.append_fmt("Table mode %s is", table_mode_str.ptr()))) {
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "this table mode is");
              LOG_WARN("failed to append err msg", K(tmp_ret), K(err_msg), K(table_mode_str));
            } else {
              LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
            }
          }
          if (OB_FAIL(ret)) {
            SQL_RESV_LOG(WARN, "failed to resolve table mode str!", K(ret));
          } else if (not_compat_for_queuing_mode(tenant_data_version)
                 && is_new_queuing_mode(static_cast<ObTableModeFlag>(table_mode_.mode_flag_))) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN(QUEUING_MODE_NOT_COMPAT_WARN_STR, K(ret), K(table_mode_str), K(tenant_data_version));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, QUEUING_MODE_NOT_COMPAT_USER_ERROR_STR);
          }
        }
        if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE ==stmt_->get_stmt_type()) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLE_MODE))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          } else {
            HEAP_VAR(ObTableSchema, tmp_table_schema) {
              if (OB_FAIL(get_table_schema_for_check(tmp_table_schema))) {
                LOG_WARN("get table schema failed", K(ret));
              } else if ((tmp_table_schema.is_primary_aux_vp_table() || tmp_table_schema.is_aux_vp_table())
                  && is_queuing_table_mode(static_cast<ObTableModeFlag>(table_mode_.mode_flag_))) {
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "set vertical partition table as queuing table mode");
                SQL_RESV_LOG(WARN, "Vertical partition table cannot set queuing table mode", K(ret));
              } else { // 暂不支持用户在alter table时变更PK_MODE
                // 设置Table当前的PK_MODE，组装最终态TableMode
                table_mode_.pk_mode_ = tmp_table_schema.get_table_mode_struct().pk_mode_;
                table_mode_.organization_mode_ = tmp_table_schema.get_table_mode_struct().organization_mode_;
              }
            }
          }
        }
        break;
      }
      case T_CHARSET: {
        if (!is_index_option) {
          if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type() && lib::is_oracle_mode()) {
            ret = OB_NOT_SUPPORTED;
            SQL_RESV_LOG(WARN, "Not support to alter table charset", K(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table charset");
          } else if (CHARSET_INVALID == charset_type_) {
            if (OB_ISNULL(option_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
            } else {
              ObString node_value(option_node->children_[0]->str_len_,
                                  option_node->children_[0]->str_value_);
              ObString charset = node_value.trim();
              ObCharsetType charset_type = ObCharset::charset_type(charset);
              if (CHARSET_INVALID == charset_type) {
                ret = OB_ERR_UNKNOWN_CHARSET;
                LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
              } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(charset_type,
                                                                                session_info_->get_effective_tenant_id()))) {
                SQL_RESV_LOG(WARN, "failed to check charset data version valid", K(ret));
              } else {
                charset_type_ = charset_type;
                if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
                  if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::CHARSET_TYPE))) {
                    SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
                  }
                }
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify charset");
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify charset in index option");
        }
        break;
      }
      case T_COLLATION: {
        if (!is_index_option) {
          if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()
              && lib::is_oracle_mode()) {
            ret = OB_ERR_PARSE_SQL;
            SQL_RESV_LOG(WARN, "Not support to alter collation", K(ret));
          } else if (CS_TYPE_INVALID == collation_type_) {
            ObString node_value(option_node->str_len_, option_node->str_value_);
            ObString collation = node_value.trim();
            ObCollationType collation_type = ObCharset::collation_type(collation);
            if (CS_TYPE_INVALID == collation_type) {
              ret = OB_ERR_UNKNOWN_COLLATION;
              LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
            } else if (OB_FAIL(sql::ObSQLUtils::is_charset_data_version_valid(common::ObCharset::charset_type_by_coll(collation_type),
                                                                              session_info_->get_effective_tenant_id()))) {
              SQL_RESV_LOG(WARN, "failed to check charset data version valid", K(ret));
            } else if (OB_FAIL(sql::ObSQLUtils::is_collation_data_version_valid(collation_type,
                                                                                session_info_->get_effective_tenant_id()))) {
              SQL_RESV_LOG(WARN, "failed to check collation data version valid", K(ret));
            } else {
              collation_type_ = collation_type;
              if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
                if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::COLLATION_TYPE))) {
                  SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
                }
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify collation", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify collation in index option");
        }
        break;
      }
      case T_TABLE_ID: {
        if (!is_index_option) {
          if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            ret = OB_ERR_PARSE_SQL;
            SQL_RESV_LOG(WARN, "Not support to alter table id", K(ret));
          } else if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            table_id_ = static_cast<uint64_t>(option_node->children_[0]->value_);
            if (is_cte_table(table_id_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("This table cann't be a cte table", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify table_id", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify table id in index option");
        }
        break;
      }
      case T_DATA_TABLE_ID: {
        if (is_index_option && stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            uint64_t table_id = static_cast<uint64_t>(option_node->children_[0]->value_);
            // bool is_sync_ddl_user = false;
            // if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
            //  LOG_WARN("Failed to check check_sync_ddl_user", K(ret));
            // } else if (!is_sync_ddl_user) {
            //   ret = OB_ERR_PARSE_SQL;
            //  LOG_WARN("Only support for sync ddl user to specify data_table_id", K(ret), K(session_info_->get_user_name()));
            // } else {
              data_table_id_ = table_id;
            // }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only create index can specify data_table_id for restore purpose", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify data table id not for restore purpose in create index");
        }
        break;
      }
      case T_INDEX_TABLE_ID: {
        if (is_index_option && stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else {
            uint64_t table_id = static_cast<uint64_t>(option_node->children_[0]->value_);
            // bool is_sync_ddl_user = false;
            // if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
            //   LOG_WARN("Failed to check check_sync_ddl_user", K(ret));
            // } else if (!is_sync_ddl_user) {
            //   ret = OB_ERR_PARSE_SQL;
            //   LOG_WARN("Only support for sync ddl user to specify index_table_id", K(ret), K(session_info_->get_user_name()));
            // } else {
              index_table_id_ = table_id;
            // }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only create index can specify index_table_id for restore purpose", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify index table id not for restore purpose in create index");
        }
        break;
      }
      case T_VIRTUAL_COLUMN_ID: {
        if (is_index_option && stmt::T_CREATE_INDEX == stmt_->get_stmt_type()) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option node child is null", K(option_node->children_[0]), K(ret));
          } else {
            bool is_sync_ddl_user = false;
            if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
              LOG_WARN("Failed to check check_sync_ddl_user", K(ret));
            } else if (!is_sync_ddl_user) {
              ret = OB_ERR_PARSE_SQL;
              LOG_WARN("Only support for sync ddl user to specify index_table_id", K(ret), K(session_info_->get_user_name()));
            } else {
              virtual_column_id_ = static_cast<uint64_t>(option_node->children_[0]->value_);
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only create index can specify virtual column id for restore purpose", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify virtual column id not for restore purpose in create index");
        }
        break;
      }
      case T_MAX_USED_PART_ID: {
        // max_used_part_id is deprecated in 4.0, we just ignore and show warnings
        LOG_USER_WARN(OB_NOT_SUPPORTED, "specify max_used_part_id");
        break;
      }
      case T_PRIMARY_ZONE: {
        if (!is_index_option) {
          ObString tmp_str;
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
          } else if (T_DEFAULT == option_node->children_[0]->type_) {
            // do nothing
          } else if (T_RANDOM == option_node->children_[0]->type_) {
            primary_zone_.assign_ptr(common::OB_RANDOM_PRIMARY_ZONE,
                                       static_cast<int32_t>(strlen(common::OB_RANDOM_PRIMARY_ZONE)));
          } else {
            tmp_str.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                               static_cast<int32_t>(option_node->children_[0]->str_len_));
            primary_zone_ = tmp_str.trim();
            if (primary_zone_.empty()) {
              ret = OB_OP_NOT_ALLOW;
              SQL_RESV_LOG(WARN, "set primary_zone empty is not allowed now", K(ret));
              LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set primary_zone empty");
            }
          }
          if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            bool is_sync_ddl_user = false;
            if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
              LOG_WARN("Failed to check check_sync_ddl_user", K(ret));
            } else if (is_sync_ddl_user) {
              ret = OB_IGNORE_SQL_IN_RESTORE;
              LOG_WARN("Cannot support for sync ddl user to alter primary zone", K(ret), K(session_info_->get_user_name()));
            } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::PRIMARY_ZONE))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify primary zone", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify primary zone in index option");
        }
        break;
      }
      case T_READ_ONLY: {
        if (!is_index_option) {
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR, "read only options can not be null", K(ret));
          } else if (T_ON == option_node->children_[0]->type_) {
            read_only_ = true;
          } else if (T_OFF == option_node->children_[0]->type_) {
            read_only_ = false;
          } else {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "unknown read only options", K(ret));
          }
          if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member( obrpc::ObAlterTableArg::READ_ONLY))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify read only", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify read only in index option");
        }
        break;
      }
      case T_AUTO_INCREMENT: {
        if (!is_index_option) {
          // parser filters negative value
          errno = 0;
          int err = 0;
          uint64_t auto_increment = 0;
          if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR, "read only options can not be null", K(ret));
          } else {
            auto_increment = ObCharset::strntoull(
                             option_node->children_[0]->str_value_,
                             static_cast<int32_t>(option_node->children_[0]->str_len_),10, &err);
          }
          /* If the value read is out of the range of representable values by a long long int,
           * the function returns LLONG_MAX or LLONG_MIN, and errno is set to ERANGE.
           */
          if (OB_FAIL(ret)) {
            //do nothing
          } else if (ERANGE == err && (UINT_MAX == auto_increment || 0 == auto_increment)) {
            ret = OB_DATA_OUT_OF_RANGE;
          } else if (EDOM == err) {
            ObString node_str(static_cast<int32_t>(option_node->children_[0]->str_len_),
                               option_node->children_[0]->str_value_);
            ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
            SQL_RESV_LOG(WARN, "failed, invalid value", K(auto_increment),
                         K(node_str), K(err), K(ret));
          } else {
            auto_increment_ = auto_increment;
          }
          if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::AUTO_INCREMENT))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify autoincrement id", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify autoincrement id in index option");
        }
        break;
      }
      case T_AUTO_INCREMENT_MODE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          bool is_sync_ddl_user = false;
          ObString mode_str(static_cast<int32_t>(option_node->children_[0]->str_len_),
                              (char *)(option_node->children_[0]->str_value_));
          if (0 == mode_str.case_compare("order")) {
            table_mode_.auto_increment_mode_ = ObTableAutoIncrementMode::ORDER;
          } else if (0 == mode_str.case_compare("noorder")) {
            table_mode_.auto_increment_mode_ = ObTableAutoIncrementMode::NOORDER;
          } else {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_RESV_LOG(WARN, "failed to resolve table mode str!", K(ret));
          }
        }
        if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE ==stmt_->get_stmt_type()) {
          HEAP_VAR(ObTableSchema, tmp_table_schema) {
            if (OB_FAIL(get_table_schema_for_check(tmp_table_schema))) {
              LOG_WARN("get table schema failed", K(ret));
            } else if (table_mode_.auto_increment_mode_ ==
                         tmp_table_schema.get_table_auto_increment_mode()) {
              // same as the original auto_increment_mode, do nothing
            } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::INCREMENT_MODE))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_AUTO_INCREMENT_CACHE_SIZE: {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < MOCK_DATA_VERSION_4_2_3_0 ||
            (tenant_data_version >= DATA_VERSION_4_3_0_0 && tenant_data_version < DATA_VERSION_4_3_2_0)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("alter table auto_increment_cache_size is not supported in data version less than 4.2.3",
                   K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table auto_increment_cache_size is not supported in data version less than 4.2.3");
        } else if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          const static int64_t MAX_AUTO_INCREMENT_CACHE_SIZE = 100000000;
          const int64_t cache_size = option_node->children_[0]->value_;
          if (cache_size < 0 || cache_size > MAX_AUTO_INCREMENT_CACHE_SIZE) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "Specify table auto increment cache size should be [0, 100000000]",
                        K(ret), K(cache_size));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "table auto_increment_cache_size");
          } else {
            auto_increment_cache_size_ = cache_size;
          }
        }
        if (OB_SUCCESS == ret && stmt::T_ALTER_TABLE ==stmt_->get_stmt_type()) {
          HEAP_VAR(ObTableSchema, tmp_table_schema) {
            if (OB_FAIL(get_table_schema_for_check(tmp_table_schema))) {
              LOG_WARN("get table schema failed", K(ret));
            } else if (auto_increment_cache_size_ ==
                         tmp_table_schema.get_auto_increment_cache_size()) {
              // same as the original auto_increment_mode, do nothing
            } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::INCREMENT_CACHE_SIZE))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_ENABLE_EXTENDED_ROWID: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          const bool enable_extended_rowid = static_cast<bool>(option_node->children_[0]->value_);
          table_mode_.rowid_mode_ = enable_extended_rowid ? ObTableRowidMode::ROWID_EXTENDED
                                                          : ObTableRowidMode::ROWID_NORMAL;
          if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (enable_extended_rowid) {
              HEAP_VAR(ObTableSchema, tmp_table_schema) {
                if (OB_FAIL(get_table_schema_for_check(tmp_table_schema))) {
                  LOG_WARN("get table schema failed", K(ret));
                } else if (table_mode_.rowid_mode_ == tmp_table_schema.get_table_rowid_mode()) {
                  // same as the original rowid_mode, do nothing
                } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::ENABLE_EXTENDED_ROWID))) {
                  SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
                }
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              SQL_RESV_LOG(WARN, "alter table cannot disable extended rowid", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "disable extended rowid in alter table");
            }
          }
        }
        break;
      }
      case T_TABLE_RENAME: {
        if (!is_index_option) {
          if (stmt::T_ALTER_TABLE != stmt_->get_stmt_type()) {
            ret = OB_ERR_RESOLVE_SQL;
            SQL_RESV_LOG(WARN, "Can't alter table name when creating table", K(ret));
          } else {
            ParseNode *relation_node = NULL;
            ObString new_database_name;
            if (OB_ISNULL(option_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(ERROR, "read only options can not be null", K(ret));
            } else {
              relation_node = option_node->children_[0];
            }

            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(relation_node)) {
              ret = OB_ERR_PARSE_SQL;
              SQL_RESV_LOG(WARN, "table relation node should not be null!", K(ret));
            } else if (OB_FAIL(resolve_table_relation_node(relation_node,
                                                           table_name_,
                                                           new_database_name))) {
            } else if (table_name_.empty()) {
              ret = OB_WRONG_TABLE_NAME;
              LOG_USER_ERROR(OB_WRONG_TABLE_NAME, table_name_.length(), table_name_.ptr());
            } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLE_NAME))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            } else if (lib::is_oracle_mode()) {
              ParseNode *new_db_node = relation_node->children_[0];
              if (OB_ISNULL(new_db_node)) {
                database_name_ = static_cast<ObAlterTableStmt *>(stmt_)->get_org_database_name();
              } else {
                ret = OB_ERR_ALTER_TABLE_RENAME_WITH_OPTION;
                SQL_RESV_LOG(WARN, "new_db_node should be null in oracle mode", K(ret));
              }
            } else {
              database_name_ = new_database_name;
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not rename table", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify table rename in index option");
        }
        break;
      }
      case T_USING_BTREE: {
        has_index_using_type_ = true;
        index_using_type_ = USING_BTREE;
        break;
      }
      case T_USING_HASH: {
        has_index_using_type_ = true;
        index_using_type_ = USING_HASH;
        break;
      }
      case T_ENGINE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(ERROR, "engine node can not be null", K(ret));
        } else {
          ObString engine_name(static_cast<int32_t>(option_node->children_[0]->str_len_),
                             option_node->children_[0]->str_value_);
          LOG_USER_WARN(OB_ERR_UNKNOWN_STORAGE_ENGINE, engine_name.length(), engine_name.ptr());
          SQL_RESV_LOG(WARN, "unknown engine", K(engine_name), K(ret));
          ret = OB_SUCCESS;
        }
        break;
      }
      case T_INVISIBLE: {
        index_attributes_set_ |= (uint64_t)1 << ObTableSchema::INDEX_VISIBILITY;
        break;
      }
      case T_VISIBLE: {
        /*do nothing default is visible*/
        index_attributes_set_ &= ~((uint64_t)1 << ObTableSchema::INDEX_VISIBILITY);
        break;
      }
      case T_DUPLICATE_SCOPE: {
        ObString duplicate_scope_str;
        share::ObDuplicateScope my_duplicate_scope = share::ObDuplicateScope::DUPLICATE_SCOPE_MAX;
        if (nullptr == option_node->children_ || 1 != option_node->num_child_) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid replicate scope arg", K(ret), "num_child", option_node->num_child_);
        } else if (nullptr == option_node->children_[0]) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option node child is null", K(ret));
        } else {
          duplicate_scope_str.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                                         static_cast<int32_t>(option_node->children_[0]->str_len_));
          duplicate_scope_str = duplicate_scope_str.trim();
          if (OB_FAIL(ObDuplicateScopeChecker::convert_duplicate_scope_string(
                duplicate_scope_str, my_duplicate_scope))) {
            SQL_RESV_LOG(WARN, "fail to convert replicate scope string", K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate_scope");
          } else {
            duplicate_scope_ = my_duplicate_scope;
          }
          if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("alter table duplicate scope not supported", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table duplicate scope");
          }
        }
        break;
      }
      case T_LOCALITY: {
        if (NULL == option_node->children_ || option_node->num_child_ != 2) {
          ret = common::OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid locality argument", K(ret), "num_child", option_node->num_child_);
        } else if (T_DEFAULT == option_node->children_[0]->type_) {
          // do nothing
        } else {
          int64_t locality_length = option_node->children_[0]->str_len_;
          const char *locality_str = option_node->children_[0]->str_value_;
          common::ObString locality(locality_length, locality_str);
          if (OB_UNLIKELY(locality_length > common::MAX_LOCALITY_LENGTH)) {
            ret = common::OB_ERR_TOO_LONG_IDENT;
            SQL_RESV_LOG(WARN, "locality length is beyond limit", K(ret), K(locality));
            LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, locality.length(), locality.ptr());
          } else if (0 == locality_length) {
            ret = OB_OP_NOT_ALLOW;
            SQL_RESV_LOG(WARN, "set locality empty is not allowed now", K(ret));
            LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set locality empty");
          } else {
            locality_.assign_ptr(locality_str, static_cast<int32_t>(locality_length));
          }
        }
        if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          bool is_sync_ddl_user= false;
          if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
            LOG_WARN("Failed to check sync_dll_user", K(ret));
          } else if (is_sync_ddl_user) {
            ret = OB_IGNORE_SQL_IN_RESTORE;
            LOG_WARN("Cannot support for sync ddl user to alter locality",
                     K(ret), K(session_info_->get_user_name()));
          } else if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::LOCALITY))) {
            SQL_RESV_LOG(WARN, "fail to add member to bitset!", K(ret));
          } else if (nullptr == option_node->children_[1]) {
            // not force alter locality
          } else if (option_node->children_[1]->type_ != T_FORCE) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR, "invalid node", K(ret));
          } else if (OB_FAIL(alter_table_bitset_.add_member(
                  obrpc::ObAlterTableArg::FORCE_LOCALITY))) {
            SQL_RESV_LOG(WARN, "fail to add force locality member to bitset", K(ret));
          }
        }
        break;
      }
      case T_ENABLE_ROW_MOVEMENT: {
        if (NULL == option_node->children_ || 1 != option_node->num_child_ || T_BOOL != option_node->children_[0]->type_) {
            ret = common::OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid row movement argument", K(ret), "num_child", option_node->num_child_);
        } else {
          enable_row_movement_ = static_cast<bool>(option_node->children_[0]->value_);
          if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
            if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::ENABLE_ROW_MOVEMENT))) {
              SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
            }
          }
        }
        break;
      }
      case T_ENCRYPTION: {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "alter/create encryption table is not supported", K(option_node->children_[0]), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify encryption in table/index option");
        break;
      }
      case T_TABLESPACE: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else if (lib::is_mysql_mode() && is_index_option) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("set tablespace for index is not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify tablespace in index option");
        } else if (is_external_table_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Specifying tablegroup on external table");
        } else {
          ParseNode *tablespace_node = option_node->children_[0];
          const ObTablespaceSchema *tablespace_schema = NULL;
          ObString tablespace_name(tablespace_node->str_len_, tablespace_node->str_value_);
          if (OB_ISNULL(schema_checker_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema checker ptr is null", K(ret));
          } else if (OB_FAIL(schema_checker_->get_tablespace_schema(tenant_id, tablespace_name, tablespace_schema))) {
            LOG_WARN("fail to get tablespace schema", K(ret), K(tablespace_name));
          } else {
            tablespace_id_ = tablespace_schema->get_tablespace_id();
            if (OB_FAIL(set_encryption_name(tablespace_schema->get_encryption_name()))) {
              LOG_WARN("fail to set encryption name from tablespace schema in ob ddl resolver", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLESPACE_ID))) {
            LOG_WARN("failed to add encryption member to bitset", K(ret));
          }
        }
        break;
      }
      case T_PARALLEL: {
        if (OB_ISNULL(option_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "option_node child is null", K(option_node->children_[0]), K(ret));
        } else {
          const int64_t table_dop = option_node->children_[0]->value_;
          if (table_dop <= 0) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid table dop", K(table_dop),
                K(ret));
          } else {
            table_dop_ = table_dop;
          }
        }
        if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TABLE_DOP))) {
            SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
          }
        }
        break;
      }
      case T_TABLE_CHECKSUM:
      case T_DELAY_KEY_WRITE:
      case T_AVG_ROW_LENGTH: {
        break;
      }
      case T_EXTERNAL_FILE_LOCATION: {
        ParseNode *string_node = NULL;
        if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid file format option", K(ret));
        } else {
          ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
          ObCreateTableArg &arg = create_table_stmt->get_create_table_arg();
          if (!arg.schema_.is_external_table()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "location option");
          } else if (option_node->num_child_ != 1 || OB_ISNULL(string_node = option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child num", K(option_node->num_child_));
          } else {
            ObString url = ObString(string_node->str_len_, string_node->str_value_).trim_space_only();
            ObSqlString tmp_location;
            ObSqlString prefix;
            ObBackupStorageInfo storage_info;
            char storage_info_buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
            OZ (resolve_file_prefix(url, prefix, storage_info.device_type_));
            OZ (tmp_location.append(prefix.string()));
            url = url.trim_space_only();

            if (OB_STORAGE_FILE != storage_info.device_type_) {
              OZ (ObSQLUtils::split_remote_object_storage_url(url, storage_info));
            }
            OZ (tmp_location.append(url));
            OZ (storage_info.get_storage_info_str(storage_info_buf, sizeof(storage_info_buf)));
            OZ (arg.schema_.set_external_file_location(tmp_location.string()));
            OZ (arg.schema_.set_external_file_location_access_info(storage_info_buf));
          }

          if (OB_SUCC(ret)) {
            if (OB_ISNULL(params_.session_info_)) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              ObString cur_sql = params_.session_info_->get_current_query_string();
              ObString masked_sql;
              if (OB_FAIL(ObDCLResolver::mask_password_for_passwd_node(
                            params_.allocator_, cur_sql, string_node, masked_sql, true))) {
                LOG_WARN("fail to gen masked sql", K(ret));
              } else {
                create_table_stmt->set_masked_sql(masked_sql);
              }
            }
          }
        }
        break;
      }
      case T_EXTERNAL_FILE_FORMAT: {
        if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid file format option", K(ret));
        } else {
          ObCreateTableArg &arg = static_cast<ObCreateTableStmt*>(stmt_)->get_create_table_arg();
          if (!arg.schema_.is_external_table()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "format option");
          } else {
            bool has_file_format = false;
            ObExternalFileFormat format;
            format.csv_format_.init_format(ObDataInFileStruct(), 10, CS_TYPE_UTF8MB4_BIN);
            // 1. resolve file type and encoding type
            for (int i = 0; OB_SUCC(ret) && i < option_node->num_child_; ++i) {
              if (OB_ISNULL(option_node->children_[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed. get unexpected NULL ptr", K(ret), K(option_node->num_child_));
              } else if (T_EXTERNAL_FILE_FORMAT_TYPE == option_node->children_[i]->type_
                         || T_CHARSET == option_node->children_[i]->type_) {
                if (OB_FAIL(resolve_file_format(option_node->children_[i], format))) {
                  LOG_WARN("fail to resolve file format", K(ret));
                }
                has_file_format |= (T_EXTERNAL_FILE_FORMAT_TYPE == option_node->children_[i]->type_);
              }
            }
            if (OB_SUCC(ret) && !has_file_format) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "format");
            }
            // 2. resolve other format value
            for (int i = 0; OB_SUCC(ret) && i < option_node->num_child_; ++i) {
              if (OB_ISNULL(option_node->children_[i])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed. get unexpected NULL ptr", K(ret), K(option_node->num_child_));
              } else if (T_EXTERNAL_FILE_FORMAT_TYPE == option_node->children_[i]->type_ ||
                         T_CHARSET == option_node->children_[i]->type_) {
              } else if (OB_FAIL(resolve_file_format(option_node->children_[i], format))) {
                LOG_WARN("fail to resolve file format", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              bool is_valid = true;
              if (OB_FAIL(check_format_valid(format, is_valid))) {
                LOG_WARN("check format valid failed", K(ret));
              } else if (!is_valid) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("file format is not valid", K(ret));
              } else {
                char *buf = NULL;
                int64_t buf_len = DEFAULT_BUF_LENGTH / 2;
                int64_t pos = 0;
                do {
                  buf_len *= 2;
                  if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(buf_len)))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
                  } else {
                    pos = format.to_string(buf, buf_len);
                  }
                } while (OB_SUCC(ret) && pos >= buf_len);
                if (OB_SUCC(ret)) {
                  arg.schema_.set_external_file_format(ObString(pos, buf));
                  LOG_DEBUG("debug external file format",
                            K(arg.schema_.get_external_file_format()));
                }
              }
            }
          }
        }
        break;
      }
      case T_EXTERNAL_FILE_PATTERN: {
        if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_ERR_UNEXPECTED; //TODO-EXTERNAL-TABLE add new error code
          LOG_WARN("invalid file format option", K(ret));
        } else {
          ObCreateTableArg &arg = static_cast<ObCreateTableStmt*>(stmt_)->get_create_table_arg();
          if (!arg.schema_.is_external_table()) {
            ret = OB_NOT_SUPPORTED;
            ObSqlString err_msg;
            err_msg.append_fmt("Using PATTERN as a CREATE TABLE option");
            LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
            LOG_WARN("using PATTERN as a table option is support in external table only", K(ret));
          } else if (option_node->num_child_ != 1 || OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child num", K(option_node->num_child_));
          } else if (0 == option_node->children_[0]->str_len_) {
            ObSqlString err_msg;
            err_msg.append_fmt("empty regular expression");
            ret = OB_ERR_REGEXP_ERROR;
            LOG_USER_ERROR(OB_ERR_REGEXP_ERROR, err_msg.ptr());
            LOG_WARN("empty regular expression", K(ret));
          } else {
            ObString pattern(option_node->children_[0]->str_len_,
                             option_node->children_[0]->str_value_);
            if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                                                    session_info_->get_dtc_params(),
                                                                    pattern))) {
              LOG_WARN("failed to convert pattern to utf8", K(ret));
            } else if (OB_FAIL(arg.schema_.set_external_file_pattern(pattern))) {
              LOG_WARN("failed to set external file pattern", K(ret), K(pattern));
            }
          }
        }
        break;
      }
      case T_TTL_DEFINITION: {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ttl definition is not supported in data version less than 4.2.1", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ttl definition is not supported in data version less than 4.2.1");
        } else if (!is_index_option) {
          if (OB_ISNULL(option_node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "(the children of option_node is null", K(option_node->children_), K(ret));
          } else {
            ObRawExpr *expr = NULL;
            ObString tmp_str;
            tmp_str.assign_ptr(const_cast<char *>(option_node->str_value_),
                                  static_cast<int32_t>(option_node->str_len_));
            if (OB_ISNULL(option_node->children_[0])) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(ERROR,"children can't be null", K(ret));
            } else if (OB_FAIL(ob_write_string(*allocator_, tmp_str, ttl_definition_))) {
              SQL_RESV_LOG(WARN, "write string failed", K(ret));
            } else if (OB_FAIL(check_ttl_definition(option_node))) {
              LOG_WARN("fail to check ttl definition", K(ret));
            } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::TTL_DEFINITION))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify ttl", K(ret));
        }
        break;
      }
      case T_KV_ATTRIBUTES: {
        uint64_t tenant_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
          LOG_WARN("get tenant data version failed", K(ret));
        } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("kv attributes is not supported in data version less than 4.2.1", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes is not supported in data version less than 4.2.1");
        } else if (!is_index_option) {
          if (OB_ISNULL(option_node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "(the children of option_node is null", K(option_node->children_), K(ret));
          } else if (OB_ISNULL(option_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(ERROR,"children can't be null", K(ret));
          } else {
            ObRawExpr *expr = NULL;
            ObString tmp_str;
            int32_t max_versions = 0;
            int32_t time_to_live = 0;
            tmp_str.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                                  static_cast<int32_t>(option_node->children_[0]->str_len_));
            LOG_INFO("resolve kv attributes", K(tmp_str));
            if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                          *allocator_, session_info_->get_dtc_params(), tmp_str))) {
              LOG_WARN("fail to convert comment to utf8", K(ret));
            } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(tmp_str, max_versions, time_to_live))) {
              LOG_WARN("fail to parse kv attributes", K(ret));
            } else if (OB_FAIL(ob_write_string(*allocator_, tmp_str, kv_attributes_))) {
              SQL_RESV_LOG(WARN, "write string failed", K(ret));
            } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::KV_ATTRIBUTES))) {
                SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
              }
            }
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("index option should not specify kv attributes", K(ret));
        }
        break;
      }
      case T_LOB_INROW_THRESHOLD: {
        ret = resolve_lob_inrow_threshold(option_node, is_index_option);
        break;
      }
      case T_LOB_STORAGE_CLAUSE : {
        ret = resolve_lob_storage_parameters(option_node);
        break;
      }
      case T_EXTERNAL_USER_SPECIFIED_PARTITION: {
        if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_ERR_UNEXPECTED; //TODO-EXTERNAL-TABLE add new error code
          LOG_WARN("invalid file format option", K(ret));
        } else {
          ObCreateTableArg &arg = static_cast<ObCreateTableStmt*>(stmt_)->get_create_table_arg();
          if (!arg.schema_.is_external_table()) {
            ret = OB_NOT_SUPPORTED;
            ObSqlString err_msg;
            err_msg.append_fmt("Using PARTITION_TYPE as a CREATE TABLE option");
            LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
            LOG_WARN("using PARTITION_TYPE as a table option is support in external table only", K(ret));
          } else {
            arg.schema_.set_user_specified_partition_for_external_table();
            if (arg.schema_.get_external_table_auto_refresh() != 0) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("user specified partition without auto refresh off not supported", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "user specified partition without auto refresh off");
            }
          }
        }
        break;
      }
      case T_EXTERNAL_TABLE_AUTO_REFRESH: {
         if (stmt_->get_stmt_type() == stmt::T_CREATE_TABLE) {
           ObCreateTableArg &arg = static_cast<ObCreateTableStmt*>(stmt_)->get_create_table_arg();
           if (!arg.schema_.is_external_table()) {
             ret = OB_NOT_SUPPORTED;
             ObSqlString err_msg;
             err_msg.append_fmt("Using CREATE ON REFRESH as a CREATE TABLE option");
             LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
             LOG_WARN("using CREATE ON REFRESH as a table option is support in external table only", K(ret));
           } else if (option_node->num_child_ != 1 || OB_ISNULL(option_node->children_[0])) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WARN("unexpected child num", K(option_node->num_child_));
           } else {
             arg.schema_.set_external_table_auto_refresh(option_node->children_[0]->value_);
             if (arg.schema_.get_external_table_auto_refresh() != 0
                && arg.schema_.is_user_specified_partition_for_external_table()) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("user specified partition without auto refresh off not supported", K(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "user specified partition without auto refresh off");
             }
           }
         } else if (stmt_->get_stmt_type() == stmt::T_ALTER_TABLE) {
           ObAlterTableArg &arg = static_cast<ObAlterTableStmt*>(stmt_)->get_alter_table_arg();
           if (!arg.alter_table_schema_.is_external_table()) {
             ret = OB_NOT_SUPPORTED;
             ObSqlString err_msg;
             err_msg.append_fmt("Using ALTER AUTO REFRESH as a ALTER TABLE option");
             LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
             LOG_WARN("using ALTER AUTO REFRESH as a table option is support in external table only", K(ret));
           } else if (option_node->num_child_ != 1 || OB_ISNULL(option_node->children_[0])) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WARN("unexpected child num", K(option_node->num_child_));
           } else {
             arg.alter_table_schema_.set_external_table_auto_refresh(option_node->children_[0]->value_);
             if (arg.alter_table_schema_.get_external_table_auto_refresh() != 0
                && arg.alter_table_schema_.is_user_specified_partition_for_external_table()) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("user specified partition without auto refresh off not supported", K(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "user specified partition without auto refresh off");
             }
           }
         } else {
           ret = OB_ERR_UNEXPECTED;
         }
         break;
       }
      default: {
        /* won't be here */
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(ERROR, "should not reach here", K(option_node->type_), K(ret));
        break;
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_file_format(const ParseNode *node, ObExternalFileFormat &format)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || node->num_child_ != 1 || OB_ISNULL(node->children_[0]) ||
      OB_ISNULL(params_.session_info_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node", K(ret));
  } else {
    switch (node->type_) {
      case T_EXTERNAL_FILE_FORMAT_TYPE: {
        ObString string_v = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_).trim_space_only();
        for (int i = 0; i < ObExternalFileFormat::MAX_FORMAT; i++) {
          if (0 == string_v.case_compare(ObExternalFileFormat::FORMAT_TYPE_STR[i])) {
            format.format_type_ = static_cast<ObExternalFileFormat::FormatType>(i);
            break;
          }
        }
        if (ObExternalFileFormat::INVALID_FORMAT == format.format_type_) {
          ObSqlString err_msg;
          err_msg.append_fmt("format '%.*s'", string_v.length(), string_v.ptr());
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg.ptr());
          LOG_WARN("failed. external file format type is not supported yet", K(ret),
                   KPHEX(string_v.ptr(), string_v.length()));
        }
        break;
      }
      case T_FIELD_TERMINATED_STR: {
        if (OB_FAIL(ObResolverUtils::resolve_file_format_string_value(node->children_[0],
                                                            format.csv_format_.cs_type_,
                                                            params_,
                                                            format.csv_format_.field_term_str_))) {
          LOG_WARN("failed to resolve file format field terminated str", K(ret));
        } else {
          format.origin_file_format_str_.origin_field_term_str_.assign_ptr(node->str_value_, node->str_len_);
        }
        break;
      }
      case T_LINE_TERMINATED_STR: {
        if (OB_FAIL(ObResolverUtils::resolve_file_format_string_value(node->children_[0],
                                                              format.csv_format_.cs_type_,
                                                              params_,
                                                              format.csv_format_.line_term_str_))) {
          LOG_WARN("failed to resolve file format line terminated str", K(ret));
        } else {
          format.origin_file_format_str_.origin_line_term_str_.assign_ptr(node->str_value_, node->str_len_);
        }
        break;
      }
      case T_ESCAPED_STR: {
        ObString string_v;
        if (OB_FAIL(ObResolverUtils::resolve_file_format_string_value(node->children_[0],
                                                                      format.csv_format_.cs_type_,
                                                                      params_,
                                                                      string_v))) {
          LOG_WARN("failed to resolve file format escape str", K(ret));
        } else if (string_v.length() > 1) {
          ret = OB_ERR_INVALID_ESCAPE_CHAR_LENGTH;
          LOG_USER_ERROR(OB_ERR_INVALID_ESCAPE_CHAR_LENGTH);
          LOG_WARN("failed. ESCAPE CHAR length is wrong", K(ret), KPHEX(string_v.ptr(),
                                                                        string_v.length()));
        } else if (string_v.length() == 1) {
          format.csv_format_.field_escaped_char_ = string_v.ptr()[0];
        } else {
          format.csv_format_.field_escaped_char_ = INT64_MAX; // default value
        }
        if (OB_SUCC(ret)) {
          format.origin_file_format_str_.origin_field_escaped_str_.assign_ptr(node->str_value_, node->str_len_);
        }
        break;
      }
      case T_CLOSED_STR: {
        ObString string_v;
        if (OB_FAIL(ObResolverUtils::resolve_file_format_string_value(node->children_[0],
                                                                      format.csv_format_.cs_type_,
                                                                      params_,
                                                                      string_v))) {
          LOG_WARN("failed to resolve file format close str", K(ret));
        } else if (string_v.length() > 1) {
          ret = OB_WRONG_FIELD_TERMINATORS;
          LOG_USER_ERROR(OB_WRONG_FIELD_TERMINATORS);
          LOG_WARN("failed. ENCLOSED CHAR length is wrong", K(ret), KPHEX(string_v.ptr(),
                                                                          string_v.length()));
        } else if (string_v.length() == 1) {
          format.csv_format_.field_enclosed_char_ = string_v.ptr()[0];
        } else {
          format.csv_format_.field_enclosed_char_ = INT64_MAX; // default value
        }
        if (OB_SUCC(ret)) {
          format.origin_file_format_str_.origin_field_enclosed_str_.assign_ptr(node->str_value_,
                                                                   node->str_len_);
        }
        break;
      }
      case T_CHARSET: {
        ObString string_v = ObString(node->children_[0]->str_len_, node->children_[0]->str_value_).trim_space_only();
        ObCharsetType cs_type = CHARSET_INVALID;
        if (CHARSET_INVALID == (cs_type = ObCharset::charset_type(string_v))) {
          ret = OB_ERR_UNSUPPORTED_CHARACTER_SET;
          LOG_USER_ERROR(OB_ERR_UNSUPPORTED_CHARACTER_SET);
          LOG_WARN("failed. Encoding type is unsupported", K(ret), KPHEX(string_v.ptr(),
                                                                         string_v.length()));
        } else {
          format.csv_format_.cs_type_ = cs_type;
        }
        break;
      }
      case T_SKIP_HEADER: {
        format.csv_format_.skip_header_lines_ = node->children_[0]->value_;
        break;
      }
      case T_SKIP_BLANK_LINE: {
        format.csv_format_.skip_blank_lines_ = node->children_[0]->value_;
        break;
      }
      case T_TRIM_SPACE: {
        format.csv_format_.trim_space_ = node->children_[0]->value_;
        break;
      }
      case T_NULL_IF_EXETERNAL: {
        if (OB_FAIL(format.csv_format_.null_if_.allocate_array(*allocator_,
                                                               node->children_[0]->num_child_))) {
          LOG_WARN("allocate array failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < node->children_[0]->num_child_; i++) {
          if (OB_FAIL(ObResolverUtils::resolve_file_format_string_value(
                                                              node->children_[0]->children_[i],
                                                              format.csv_format_.cs_type_,
                                                              params_,
                                                              format.csv_format_.null_if_.at(i)))) {
            LOG_WARN("failed to resolve file format line terminated str", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          format.origin_file_format_str_.origin_null_if_str_.assign_ptr(node->str_value_, node->str_len_);
        }
        break;
      }
      case T_EMPTY_FIELD_AS_NULL: {
        format.csv_format_.empty_field_as_null_ = node->children_[0]->value_;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid file format option", K(ret), K(node->type_));
      }
    }
  }
  return ret;
}


int ObDDLResolver::resolve_column_definition_ref(ObColumnSchemaV2 &column,
                                                  ParseNode *node /* column_definition_def */,
                                                  bool is_resolve_for_alter_table)
{
  int ret = OB_SUCCESS;
  ObString name;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parser tree!", K(ret), K(node));
  } else if (T_COLUMN_REF != node->type_ || 3 != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parser tree!", K(ret), K(node->type_), K(node->num_child_));
  } else {
    ParseNode *db_name_node = node->children_[0];
    ParseNode *table_name_node = node->children_[1];
    if (is_oracle_mode()) {
      if (OB_NOT_NULL(table_name_node) || OB_NOT_NULL(db_name_node)) {
        ret = OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED;
        LOG_WARN("only simple column names allowed here", K(ret));
      }
    } else {
      if (NULL != db_name_node) {
        ObString dbname(db_name_node->str_len_, db_name_node->str_value_);
        if (0 != dbname.compare(database_name_)) {
          ret = OB_WRONG_DB_NAME;
          LOG_WARN("invalid database name", K(ret));
          LOG_USER_ERROR(OB_WRONG_DB_NAME, dbname.length(), dbname.ptr());
        }
      }
      if (OB_FAIL(ret)) {
      } else if (NULL != table_name_node) {
        ObString table_name(table_name_node->str_len_, table_name_node->str_value_);
        if (0 != table_name.compare(table_name_)) {
          ret = OB_WRONG_TABLE_NAME;
          LOG_WARN("invalid table name", K(ret));
          LOG_USER_ERROR(OB_WRONG_TABLE_NAME, table_name.length(), table_name.ptr());
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(resolve_column_name(name, node->children_[2]))) {
    LOG_WARN("resolve column name failed", K(ret));
  } else if (is_resolve_for_alter_table) {
    AlterColumnSchema &alter_column_schema = static_cast<AlterColumnSchema &>(column);
    if (OB_FAIL(alter_column_schema.set_origin_column_name(name))) {
      SQL_RESV_LOG(WARN, "fail to set origin column name", K(name), K(ret));
    }
  } else if (OB_FAIL(column.set_column_name(name))) {
    SQL_RESV_LOG(WARN, "fail to set column name", K(name), K(ret));
  }
  return ret;
}

int ObDDLResolver::check_format_valid(const ObExternalFileFormat &format, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (!format.csv_format_.line_term_str_.empty() && !format.csv_format_.field_term_str_.empty()) {
    if (0 == MEMCMP(format.csv_format_.field_term_str_.ptr(),
                    format.csv_format_.line_term_str_.ptr(),
                    std::min(format.csv_format_.field_term_str_.length(),
                             format.csv_format_.line_term_str_.length()))) {
      is_valid = false;
      LOG_USER_WARN(OB_NOT_SUPPORTED,
          "LINE_DELIMITER or FIELD_DELIMITER cannot be a substring of the delimiter for the other");
      LOG_WARN("LINE_DELIMITER or FIELD_DELIMITER cann't be a substring of the other's", K(ret),
               K(format.csv_format_.line_term_str_), K(format.csv_format_.field_term_str_));
    }
  }
  if (OB_SUCC(ret)) {
     if (!format.csv_format_.line_term_str_.empty()
         && (format.csv_format_.line_term_str_[0] == format.csv_format_.field_escaped_char_
             || format.csv_format_.line_term_str_[0] == format.csv_format_.field_enclosed_char_)) {
       ret = OB_WRONG_FIELD_TERMINATORS;
       LOG_WARN("invalid line terminator", K(ret));
     } else if (!format.csv_format_.field_term_str_.empty()
                && (format.csv_format_.field_term_str_[0] == format.csv_format_.field_escaped_char_
                    || format.csv_format_.field_term_str_[0] == format.csv_format_.field_enclosed_char_)) {
       ret = OB_WRONG_FIELD_TERMINATORS;
       LOG_WARN("invalid field terminator", K(ret));
     }
  }
  return ret;
}

int ObDDLResolver::resolve_column_name(common::ObString &col_name, ParseNode *node/* column_name */)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || T_IDENT != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parser tree", K(ret));
  } else {
    col_name.assign_ptr(node->str_value_, node->str_len_);
    int32_t name_length = col_name.length();
    const char *name_ptr = col_name.ptr();
    if ((lib::is_oracle_mode() && name_length > OB_MAX_COLUMN_NAME_LENGTH)
        || (lib::is_mysql_mode() && name_length > OB_MAX_COLUMN_NAME_LENGTH * OB_MAX_CHAR_LEN)) {
      ret = OB_ERR_TOO_LONG_IDENT;
      _SQL_RESV_LOG(WARN, "identifier name '%.*s' is too long, ret=%d",
                    static_cast<int32_t>(name_length),
                    name_ptr, ret);
      LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, static_cast<int32_t>(name_length), name_ptr);
    } else if (0 == name_length) {
      ret = OB_WRONG_COLUMN_NAME;
      _SQL_RESV_LOG(WARN, "identifier name '%.*s' is empty, ret=%d",
                    static_cast<int32_t>(name_length),
                    name_ptr, ret);
      LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int32_t>(name_length), name_ptr);
    } else {
      ObCollationType cs_type = CS_TYPE_INVALID;
      if (lib::is_oracle_mode() &&
          0 == col_name.case_compare(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, name_length, name_ptr,
                                              table_name_.length(), table_name_.ptr());
        LOG_WARN("invalid rowid column", K(ret));
      } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
        LOG_WARN("fail to get collation_connection", K(ret));
      } else if (OB_FAIL(ObSQLUtils::check_column_name(cs_type, col_name))) {
        SQL_RESV_LOG(WARN, "fail to check column name", K(col_name), K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::get_identity_column_count(const ObTableSchema &table_schema, int64_t &identity_column_count)
{
  int ret = OB_SUCCESS;
  identity_column_count = 0;
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
       OB_SUCC(ret) && iter != table_schema.column_end(); ++iter) {
    ObColumnSchemaV2 &column_schema = (**iter);
    if (column_schema.is_identity_column()) {
      identity_column_count++;
    }
  }
  return ret;
}

int ObDDLResolver::resolve_identity_column_definition(ObColumnSchemaV2 &column,
                                                      ParseNode *node /* column_definition */)
{
  int ret = OB_SUCCESS;
  ParseNode *generated_option = NULL;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt cat not be null.", K(ret));
  } else if (OB_ISNULL(generated_option = node->children_[6])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generated_option is null", K(ret));
  } else if (generated_option->type_ != T_GENERATED_COLUMN ||
             generated_option->num_child_ != IDEN_OPTION_DEFINITION_NUM_CHILD ||
             OB_ISNULL(generated_option = generated_option->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid option node type or child num", K(ret),
             K(generated_option->type_), K(generated_option->num_child_));
  } else {
    column.erase_identity_column_flags();
    if (generated_option->type_ == T_CONSTR_ALWAYS) {
      column.add_column_flag(ALWAYS_IDENTITY_COLUMN_FLAG);
    } else if (generated_option->type_ == T_CONSTR_DEFAULT) {
      ParseNode *null_option = NULL;
      if (generated_option->num_child_ != IDEN_OPTION_DEFINITION_NUM_CHILD ||
          OB_ISNULL(null_option = generated_option->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid option node type or child num",
                 K(generated_option->type_), K(generated_option->num_child_), K(ret));
      } else {
        if (null_option->type_ == T_CONSTR_NOT_NULL) {
          column.add_column_flag(DEFAULT_IDENTITY_COLUMN_FLAG);
        } else {
          column.add_column_flag(DEFAULT_ON_NULL_IDENTITY_COLUMN_FLAG);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObColumnSequenceStmt *seq_stmt = NULL;
    ObColumnSequenceResolver seq_resolver(params_);
    if (OB_FAIL(seq_resolver.resolve_sequence_without_name(seq_stmt, node->children_[3]))) {
      LOG_WARN("failed to resolve sequence options.", K(ret));
    } else if (OB_ISNULL(seq_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt cat not be null.", K(ret));
    } else {
      if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
        create_table_stmt->set_sequence_ddl_arg(seq_stmt->get_arg());
      } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
        ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
        alter_table_stmt->set_sequence_ddl_arg(seq_stmt->get_arg());
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected stem type.", K(ret), K(stmt_->get_stmt_type()));
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_column_name(ObColumnSchemaV2 &column,
                                       ParseNode *node /* column_definition */)
{
  int ret = OB_SUCCESS;
  ParseNode *column_definition_ref_node = NULL;
  CK (OB_NOT_NULL(node));
  CK (OB_NOT_NULL(node->children_[0]));
  CK (node->type_ == T_COLUMN_DEFINITION);
  if (OB_SUCC(ret)) {
    column_definition_ref_node = node->children_[0];
    OZ (resolve_column_definition_ref(column, column_definition_ref_node, false));
  }
  return ret;
}

int ObDDLResolver::resolve_column_definition(ObColumnSchemaV2 &column,
                                             ParseNode *node, /* column_definition */
                                             ObColumnResolveStat &resolve_stat,
                                             bool &is_modify_column_visibility,
                                             common::ObString &pk_name,
                                             const ObTableSchema &table_schema,
                                             const bool is_oracle_temp_table,
                                             const bool is_create_table_as,
                                             const bool allow_has_default)
{
  int ret = OB_SUCCESS;
  bool is_external_table = table_schema.is_external_table();
  bool is_modify_column = stmt::T_ALTER_TABLE == stmt_->get_stmt_type()
                  && OB_DDL_MODIFY_COLUMN == (static_cast<AlterColumnSchema &>(column)).alter_type_;
  ParseNode *column_definition_ref_node = NULL;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (T_COLUMN_DEFINITION != node->type_ || node->num_child_ < COLUMN_DEFINITION_NUM_CHILD ||
     OB_ISNULL(allocator_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0]) ||
     T_COLUMN_REF != node->children_[0]->type_ ||
     COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_
     || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse node",
                 K(ret), K(node->type_), K(node->num_child_), K_(allocator), K_(session_info));
  } else {
    resolve_stat.reset();
    column_definition_ref_node = node->children_[0];
    ParseNode *table_node = column_definition_ref_node->children_[1];
    if (is_oracle_mode() && OB_NOT_NULL(table_node)) {
      ret = OB_ERR_ONLY_SIMPLE_COLUMN_NAME_ALLOWED;
      LOG_WARN("only simple column names allowed here", K(ret));
    } else if (OB_SUCC(resolve_column_definition_ref(column, column_definition_ref_node, false))) {
      //empty
    } else {
      SQL_RESV_LOG(WARN, "fail to resolve column_definition_ref", K(ret));
    }
  }
  ParseNode *type_node = NULL;
  // 由于modify column时type可为空,在这种场景下不再去resolve type
  if (OB_SUCC(ret)) {
    type_node = node->children_[1];
    if (OB_ISNULL(type_node)) {
      if (lib::is_oracle_mode()
          && (is_modify_column
              || (GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_ && !is_external_table) //external table need an explicit defination
              || is_create_table_as)) {
        //在Oracle模式下，以下情况允许data_type node为空
        //  1. 在alter table column中
        //  2. 在生成列的定义中，但是外表的生成列不允许为空
        //  3. 在create table as中
        if (is_create_table_as) {
          // create table as 的 data_type 为空，但是为了能够add_column，先mock一个date type，
          // 后面resolve subquery后会覆盖
          ObDataType data_type;
          column.set_data_type(ObUInt64Type);
          column.set_charset_type(CHARSET_BINARY);
          column.set_collation_type(CS_TYPE_BINARY);
        }
      } else {
        ret = OB_ERR_INVALID_DATATYPE;
        LOG_WARN("type_node is invalid", K(ret));
      }
    } else if (OB_UNLIKELY(!ob_is_valid_obj_type(static_cast<ObObjType>(type_node->type_)))) {
      ret = OB_ERR_INVALID_DATATYPE;
      SQL_RESV_LOG(WARN, "type_node or stmt_ or datatype is invalid", K(ret));
    }
  }
  if (OB_SUCC(ret) && type_node != NULL) {
    ObDataType data_type;
    // session_info_ NPE check is done in up layer caller
    omt::ObTenantConfigGuard tcg(
        TENANT_CONF(session_info_->get_effective_tenant_id()));
    bool convert_real_to_decimal =
        (tcg.is_valid() && tcg->_enable_convert_real_to_decimal);
    bool enable_decimalint_type = false;
    if (OB_FAIL(ObSQLUtils::check_enable_decimalint(session_info_, enable_decimalint_type))) {
      LOG_WARN("fail to check enable decimal int", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_data_type(*type_node,
                                                   column.get_column_name_str(),
                                                   data_type,
                                                   (OB_NOT_NULL(session_info_) && is_oracle_mode()),
                                                   false,
                                                   session_info_->get_session_nls_params(),
                                                   session_info_->get_effective_tenant_id(),
                                                   enable_decimalint_type,
                                                   convert_real_to_decimal))) {
      LOG_WARN("resolve data type failed", K(ret), K(column.get_column_name_str()));
    } else if (ObExtendType == data_type.get_obj_type()) {
      const ParseNode *name_node = type_node->children_[0];
      CK (OB_NOT_NULL(session_info_) && OB_NOT_NULL(schema_checker_));
      CK (OB_NOT_NULL(name_node));
      CK (T_SP_TYPE == name_node->type_);
      if (OB_SUCC(ret)) {
        uint64_t udt_id = OB_INVALID_ID;
        uint64_t db_id = session_info_->get_database_id();
        uint64_t tenant_id = session_info_->get_effective_tenant_id();
        ObString udt_name = ObString(name_node->children_[1]->str_len_, name_node->children_[1]->str_value_);
        if (NULL != name_node->children_[0]) {
          OZ (schema_checker_->get_database_id(tenant_id,
                                               ObString(name_node->children_[0]->str_len_,
                                                        name_node->children_[0]->str_value_),
                                               db_id));
        }
        OZ (schema_checker_->get_udt_id(tenant_id, db_id, OB_INVALID_ID, udt_name, udt_id));
        if (OB_SUCC(ret) && udt_id == OB_INVALID_ID) {
          // not found in current tenant, try get from tenant schema
          if (tenant_data_version < DATA_VERSION_4_2_0_0) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("tenant version is less than 4.2, udt type not supported", K(ret), K(tenant_data_version));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.2, udt type");
          } else if (OB_FAIL(schema_checker_->get_sys_udt_id(udt_name, udt_id))) {
            LOG_WARN("failed to get sys udt id", K(ret));
          } else if (udt_id == OB_INVALID_ID) {
            ret = OB_ERR_INVALID_DATATYPE;
            SQL_RESV_LOG(WARN, "type_node or stmt_ or datatype is invalid", K(ret));
          } else {
            tenant_id = OB_SYS_TENANT_ID;
          }
        }

        if (OB_SUCC(ret)) {
          data_type.set_udt_id(udt_id);
          column.set_sub_data_type(udt_id);
          if (udt_id == T_OBJ_XML) {
            data_type.set_obj_type(ObUserDefinedSQLType);
            data_type.set_collation_type(CS_TYPE_BINARY);
            // udt column is varbinary used for null bitmap
            ObDDLArg *ddl_arg = NULL;
            if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
              ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
              ddl_arg = &create_table_stmt->get_ddl_arg();
            } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
              ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
              ddl_arg = &alter_table_stmt->get_ddl_arg();
            } else {
              // do nothing.
            }
            const ObUDTTypeInfo *udt_info = NULL;
            if (OB_ISNULL(ddl_arg)) {
            } else if (OB_FAIL(schema_checker_->get_udt_info(tenant_id, udt_id, udt_info))) {
              LOG_WARN("failed to get udt info", K(ret));
            } else if (OB_FAIL(ob_udt_check_and_add_ddl_dependency(udt_id,
                                                                   UDT_SCHEMA,
                                                                   udt_info->get_schema_version(),
                                                                   udt_info->get_tenant_id(),
                                                                   *ddl_arg))) {
              LOG_WARN("failed to add udt type ddl dependency", K(ret), K(udt_info));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      column.set_meta_type(data_type.get_meta_type());
      column.set_accuracy(data_type.get_accuracy());
      column.set_charset_type(data_type.get_charset_type());
      column.set_collation_type(data_type.get_collation_type());
      if (data_type.is_binary_collation()) {
        column.set_binary_collation(true);
        column.set_collation_type(CS_TYPE_INVALID);
      }
      if (data_type.get_meta_type().is_integer_type() || data_type.get_meta_type().is_numeric_type()
          || ob_is_decimal_int(data_type.get_meta_type().get_type())) {
        column.set_zero_fill(data_type.is_zero_fill());
      }
      if (ob_is_nstring_type(column.get_meta_type().get_type())) {
        CK (OB_NOT_NULL(session_info_));
        if (OB_SUCC(ret)) {
          ObCollationType coll_type = session_info_->get_nls_collation_nation();
          column.set_collation_type(coll_type);
          column.set_charset_type(ObCharset::charset_type_by_coll(coll_type));
        }
      }
      if (OB_SUCC(ret) && tenant_data_version < DATA_VERSION_4_1_0_0) {
        if (column.is_geometry() || data_type.get_meta_type().is_geometry()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant version is less than 4.1, geometry type not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, geometry type");
        } else if (is_oracle_mode() && (column.is_json() || data_type.get_meta_type().is_json())) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant version is less than 4.1, json type not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, json type");
        }

      }
      if (OB_SUCC(ret) && tenant_data_version < DATA_VERSION_4_3_2_0) {
        if (column.is_roaringbitmap() || data_type.get_meta_type().is_roaringbitmap()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("tenant version is less than 4.3.2, roaringbitmap type not supported", K(ret), K(tenant_data_version));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.3.2, roaringbitmap type");
        }
      }
      if (OB_SUCC(ret) && (column.is_string_type() || column.is_json() || column.is_geometry())) {
        ObCharsetType charset_type = charset_type_;
        ObCollationType collation_type = collation_type_;
        if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          ObTableSchema &table_schema = static_cast<ObAlterTableStmt *>(stmt_)->get_alter_table_arg().alter_table_schema_;
          if (CHARSET_INVALID == charset_type) {
            charset_type = table_schema.get_charset_type();
          }
          if (CS_TYPE_INVALID == collation_type) {
            collation_type = table_schema.get_collation_type();
          }
        }
        if (OB_FAIL(check_and_fill_column_charset_info(column, charset_type, collation_type))) {
          SQL_RESV_LOG(WARN, "fail to check and fill column charset info", K(ret));
        } else if (data_type.get_meta_type().is_lob() || data_type.get_meta_type().is_json()
                   || data_type.get_meta_type().is_geometry() || data_type.get_meta_type().is_roaringbitmap()) {
          if (OB_FAIL(check_text_column_length_and_promote(column, table_id_))) {
            SQL_RESV_LOG(WARN, "fail to check text or blob column length", K(ret), K(column));
          }
        } else if (OB_FAIL(check_string_column_length(column, lib::is_oracle_mode(), params_.is_prepare_stage_))) {
          SQL_RESV_LOG(WARN, "fail to check string column length", K(ret), K(column));
        }
      }
      if (OB_SUCC(ret) && ObRawType == column.get_data_type() && stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
          SQL_RESV_LOG(WARN, "failed to check raw column length", K(ret), K(column));
        }
      }
      if (OB_SUCC(ret) && ObURowIDType == column.get_data_type() &&
          stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        if (OB_FAIL(ObDDLResolver::check_urowid_column_length(column))) {
          SQL_RESV_LOG(WARN, "failed to check rowid column length", K(ret));
        }
      }
      if (OB_SUCC(ret) && column.is_enum_or_set()) {
        if (OB_FAIL(resolve_enum_or_set_column(type_node, column))) {
          LOG_WARN("fail to resolve set column", K(ret), K(column));
        }
      }
      if (OB_SUCC(ret) && column.is_geometry() && OB_FAIL(column.set_geo_type(type_node->int32_values_[1]))) {
        SQL_RESV_LOG(WARN, "fail to set geometry sub type", K(ret), K(column));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_modify_column && column.is_identity_column() && COLUMN_DEFINITION_NUM_CHILD == node->num_child_) {
      // modify column from identity to normal, do nothing
      if (!column.get_meta_type().is_numeric_type()) {
        ret = OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE;
        LOG_USER_ERROR(OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE);
      }
    } else if (GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_) {
      //处理identity column的定义
      if (OB_NOT_NULL(node->children_[4]) && node->children_[4]->type_ == T_IDENTITY_COLUMN) {
        if (ob_is_real_type(column.get_meta_type().get_type())
              || !column.get_meta_type().is_numeric_type()) {
          ret = OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE;
          LOG_USER_ERROR(OB_ERR_IDENTITY_COLUMN_MUST_BE_NUMERIC_TYPE);
        } else if (OB_FAIL(resolve_identity_column_definition(column, node))) {
          LOG_WARN("resolve identity column failed", K(ret));
        }
      } else {
        //处理生成列的定义
        if (lib::is_oracle_mode()
            && (column.get_meta_type().is_blob() || column.get_meta_type().is_clob() || column.is_xmltype())
            && !is_external_table) {
          ret = OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE;
          LOG_WARN("invalid use of blob/clob type with generate defnition",
                   K(ret), K(column.get_meta_type()));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "generate column with blob type");
        } else if (lib::is_oracle_mode() && is_create_table_as) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("generate column in create table as not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify generate column in create table as");
        } else {
          ParseNode *expr_node = NULL;
          if (node->children_[6] != NULL && node->children_[6]->num_child_ == IDEN_OPTION_DEFINITION_NUM_CHILD &&
              node->children_[6]->children_ != NULL && node->children_[6]->children_[0]->type_ != T_CONSTR_ALWAYS) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("generate column can only start with generated always.", K(ret));
          } else if (OB_ISNULL(expr_node = node->children_[3])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr_node is null");
          } else {
            ObString expr_str(expr_node->str_len_, expr_node->str_value_);
            ObObj default_value;
            /* bugfix:
             * in NO_BACKSLAH_ESCAPES sql_mode, mysql will convert '\\' to '\\\\';
             */
            bool is_no_backslash_escapes = false;
            IS_NO_BACKSLASH_ESCAPES(session_info_->get_sql_mode(), is_no_backslash_escapes);
            if (is_no_backslash_escapes &&
                OB_FAIL(ObSQLUtils::convert_escape_char(*allocator_, expr_str, expr_str))) {
              LOG_WARN("convert escape char fail", K(ret));
            } else if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_,
                                                        session_info_->get_dtc_params(), expr_str))) {
              LOG_WARN("fail to convert sql text", K(ret), K(expr_str));
            } else {
              default_value.set_varchar(expr_str);
              default_value.set_collation_type(ObCharset::get_system_collation());
              if (OB_FAIL(column.set_cur_default_value(default_value))) {
                LOG_WARN("set current default value failed", K(ret));
              } else if ((node->children_[4] != NULL && node->children_[4]->type_ == T_STORED_COLUMN)
                         || (node->children_[4] == NULL && is_external_table)) {
                column.add_column_flag(STORED_GENERATED_COLUMN_FLAG);
              } else {
                if (is_external_table) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_USER_ERROR(OB_NOT_SUPPORTED, "virtual column");
                }
                column.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
              }
            }
            if (OB_SUCC(ret) && is_pad_char_to_full_length(session_info_->get_sql_mode())) {
              column.add_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG);
            }
          }
        }
      }
    } else if (is_external_table) {
      //mock generated column
      ObExternalFileFormat format;
      format.format_type_ = external_table_format_type_;
      ObString mock_gen_column_str;
      if (OB_FAIL(format.mock_gen_column_def(column, *allocator_, mock_gen_column_str))) {
        LOG_WARN("fail to mock gen column def", K(ret));
      } else {
        ObObj default_value;
        default_value.set_varchar(mock_gen_column_str);
        default_value.set_collation_type(ObCharset::get_system_collation());
        if (OB_FAIL(column.set_cur_default_value(default_value))) {
          LOG_WARN("set current default value failed", K(ret));
        } else {
          column.add_column_flag(STORED_GENERATED_COLUMN_FLAG);
          if (is_pad_char_to_full_length(session_info_->get_sql_mode())) {
            column.add_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(mocked_external_table_column_ids_.add_member(column.get_column_id()))) {
          LOG_WARN("fail to add bitset", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ParseNode *attrs_node = node->children_[2];
    if (attrs_node != NULL) {
      if (column.is_generated_column()) {
        if (OB_FAIL(resolve_generated_column_attribute(column, attrs_node, resolve_stat, is_external_table))) {
          LOG_WARN("resolve generated column attribute failed", K(ret));
        }
      } else if (column.is_identity_column()) {
        if (OB_FAIL(resolve_identity_column_attribute(column, attrs_node, resolve_stat, pk_name))) {
          LOG_WARN("resolve identity column attribute failed", K(ret));
        }
      } else if (OB_FAIL(resolve_normal_column_attribute(column,
                                                         attrs_node,
                                                         resolve_stat,
                                                         pk_name,
                                                         allow_has_default))) {
        LOG_WARN("resolve normal column attribute failed", K(ret));
      }
    }
    // identity column默认为not null，不可更改
    if (OB_SUCC(ret) && column.is_identity_column()) {
      if (!column.has_not_null_constraint()) {
        if (OB_FAIL(add_default_not_null_constraint(column, table_name_, *allocator_, stmt_))) {
          LOG_WARN("add default not null constraint for identity column failed", K(ret));
        }
      }
    }
    LOG_DEBUG("resolve column definition mid", K(column));
    // 指定 column 的位置，目前仅在 mysql 模式下 add column 语法支持
    if (OB_SUCC(ret) && lib::is_mysql_mode()) {
      ParseNode *pos_node = NULL;
      if (OB_UNLIKELY(GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_)) {
        // generated column with pos_column
        pos_node = node->children_[5];
      } else {
        // normal column with pos_column
        pos_node = node->children_[3];
      }
    }
    // 解析 visibility_option，仅在 oracle 模式支持
    if (OB_SUCC(ret) && lib::is_oracle_mode()) {
      ParseNode *visiblity_node = NULL;
      if (OB_UNLIKELY(GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_)) {
        visiblity_node = node->children_[5];
      } else {
        visiblity_node = node->children_[3];
      }
      if (NULL != visiblity_node) {
        if (is_modify_column) {
          // Column visibility modifications can not be combined with any other modified column DDL option.
          if (OB_UNLIKELY(GEN_COLUMN_DEFINITION_NUM_CHILD == node->num_child_)) {
            visiblity_node = node->children_[5];
            if (OB_NOT_NULL(node->children_[1]) || OB_NOT_NULL(node->children_[2]) || OB_NOT_NULL(node->children_[3]) || OB_NOT_NULL(node->children_[4])) {
              ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
              SQL_RESV_LOG(WARN, "Column visibility modifications can not be combined with any other modified column DDL option.", K(ret));
            } else {
              is_modify_column_visibility = true;
            }
          } else {
            if (OB_NOT_NULL(node->children_[1]) || OB_NOT_NULL(node->children_[2])) {
              ret = OB_ERR_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION;
              SQL_RESV_LOG(WARN, "Column visibility modifications can not be combined with any other modified column DDL option.", K(ret));
            } else {
              is_modify_column_visibility = true;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (T_INVISIBLE == visiblity_node->type_) {
            const ObTableSchema *table_schema = NULL;
            if (is_oracle_temp_table) {
              ret = OB_ERR_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE;
              LOG_WARN("Invisible column is not supported on temp table.", K(ret));
            } else if (OB_ISNULL(schema_checker_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("schema checker ptr is null", K(ret));
            } else if (OB_ISNULL(session_info_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("session_info_ is null", K(ret));
            } else if (is_modify_column && OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), column.get_table_id(), table_schema))) {
              LOG_WARN("get_table_schema failed", K(ret), K(column.get_table_id()));
            } else if (is_modify_column && is_sys_database_id(table_schema->get_database_id())) {
              // The visibility of a column from a table owned by a SYS user cannot be modified to invisible, but can be modified to visible
              ret = OB_ERR_MODIFY_COL_VISIBILITY_BY_SYS_USER;
              SQL_RESV_LOG(WARN, "The visibility of a column from a table owned by a SYS user cannot be changed.", K(ret));
            } else {
              column.add_column_flag(INVISIBLE_COLUMN_FLAG);
            }
          } else {
            // T_VISIBLE == visiblity_node->type_
            column.del_column_flag(INVISIBLE_COLUMN_FLAG);
          }
        }
      }
    }
  }
  return ret;
}

// only use in oracle mode
int ObDDLResolver::resolve_uk_name_from_column_attribute(
    ParseNode *attrs_node,
    common::ObString &uk_name)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(attrs_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("attrs_node is invalid", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < attrs_node->num_child_; ++i) {
    ParseNode *attr_node = attrs_node->children_[i];
    if (T_CONSTR_UNIQUE_KEY != attr_node->type_) {
      continue;
    } else {
      ParseNode *uk_name_node = attr_node->children_[0];
      if (OB_ISNULL(uk_name_node)) {
        // do nothing
      } else {
        uk_name.assign_ptr(uk_name_node->str_value_,static_cast<int32_t>(uk_name_node->str_len_));
      }
      break;
    }
  }

  return ret;
}

int ObDDLResolver::resolve_normal_column_attribute_constr_not_null(ObColumnSchemaV2 &column,
                                                   ParseNode *attr_node,
                                                   ObColumnResolveStat &resolve_stat)
{
  INIT_SUCC(ret);
  if (is_oracle_mode()) {
    if (resolve_stat.is_set_not_null_ || resolve_stat.is_set_null_) {
      ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
      LOG_WARN("duplicate NOT NULL specifications", K(ret));
    } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()
              && column.has_not_null_constraint()) {
      ret = OB_COLUMN_CANT_CHANGE_TO_NOT_NULL;
      LOG_WARN("column to be modified to NOT NULL is already NOT NULL", K(ret));
    } else if (OB_FAIL(resolve_not_null_constraint_node(column, attr_node, false))) {
      SQL_RESV_LOG(WARN, "resolve not null constraint failed", K(ret));
    }
  } else {
    column.set_nullable(false);
  }
  resolve_stat.is_set_not_null_ = true;
  return ret;
}

int ObDDLResolver::resolve_normal_column_attribute_constr_null(ObColumnSchemaV2 &column,
                                                   ObColumnResolveStat &resolve_stat)
{
  INIT_SUCC(ret);
  if (is_oracle_mode()) {
    if (resolve_stat.is_set_not_null_ || resolve_stat.is_set_null_) {
      ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
      LOG_WARN("duplicate NULL specifications", K(ret));
    } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()
              && OB_DDL_ADD_COLUMN != (static_cast<AlterColumnSchema &>(column)).alter_type_) {
      if (!column.has_not_null_constraint()) {
        ret = OB_COLUMN_CANT_CHANGE_TO_NULLALE;
        LOG_WARN("column to be modified to NULL cannot be modified to NULL", K(ret));
      } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()
              && OB_FAIL(drop_not_null_constraint(column))) {
        LOG_WARN("drop not null constraint failed", K(ret));
      } else {
        column.drop_not_null_cst();
      }
    }
  } else {
    //set non_primary_key_column to nullable
    if (!resolve_stat.is_primary_key_) {
      column.set_nullable(true);
    }
  }
  resolve_stat.is_set_null_ = true;
  return ret;
}

int ObDDLResolver::resolve_normal_column_attribute_constr_default(ObColumnSchemaV2 &column,
                                                                  ParseNode *attr_node,
                                                                  ObColumnResolveStat &resolve_stat,
                                                                  ObObjParam& default_value,
                                                                  bool& is_set_cur_default,
                                                                  bool& is_set_orig_default)
{
  INIT_SUCC(ret);
  if (lib::is_oracle_mode()) {
    resolve_stat.is_set_default_value_ = true;
    ObString expr_str(attr_node->str_len_, attr_node->str_value_);
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                          *allocator_, session_info_->get_dtc_params(), expr_str))) {
      LOG_WARN("fail to copy and convert string charset", K(ret));
    } else {
      default_value.set_varchar(expr_str);
      default_value.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      default_value.set_param_meta();
      if (T_CONSTR_ORIG_DEFAULT == attr_node->type_) {
        ret = OB_NOT_SUPPORTED;
        //TODO:@yanhua do it next
        LOG_WARN("not support set orig default now", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify orig default value for column");
      } else if (OB_FAIL(column.set_cur_default_value(default_value))) {
        LOG_WARN("set current default value failed", K(ret));
      } else {
        column.add_column_flag(DEFAULT_EXPR_V2_COLUMN_FLAG);
      }
    }
  } else {
    if (1 != attr_node->num_child_ || NULL == attr_node->children_[0]) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "resolve default value failed", K(ret));
    } else if (OB_FAIL(resolve_default_value(attr_node, default_value))) {
      SQL_RESV_LOG(WARN, "resolve default value failed", K(ret));
    } else if (IS_DEFAULT_NOW_OBJ(default_value)) {
      if ((ObDateTimeTC != column.get_data_type_class() && ObOTimestampTC != column.get_data_type_class())
          || default_value.get_scale() != column.get_accuracy().get_scale()) {
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(),
                      column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "Invalid default value", K(column), K(default_value), K(ret));
      } else {
        default_value.set_scale(column.get_accuracy().get_scale());
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (!default_value.is_null() && ob_is_text_tc(column.get_data_type())) {
      ret = OB_INVALID_DEFAULT;
      LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      SQL_RESV_LOG(WARN, "BLOB, TEXT column can't have a default value", K(column), K(default_value), K(ret));
    } else if (!default_value.is_null()
               && (ob_is_json_tc(column.get_data_type()) || ob_is_geometry_tc(column.get_data_type()))) {
      ret = OB_ERR_BLOB_CANT_HAVE_DEFAULT;
      LOG_USER_ERROR(OB_ERR_BLOB_CANT_HAVE_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      SQL_RESV_LOG(WARN, "JSON or GEOM column can't have a default value", K(column),
                  K(default_value), K(ret));
    } else {
      if (T_CONSTR_DEFAULT == attr_node->type_) {
        resolve_stat.is_set_default_value_ = true;
        if (is_set_cur_default) {
          ret = OB_ERR_PARSER_SYNTAX;
          SQL_RESV_LOG(WARN, "cannot set current default value twice", K(ret));
        } else {
          is_set_cur_default = true;
          column.set_cur_default_value(default_value);
        }
      } else {
        // T_CONSTR_ORIG_DEFAULT == column.get_data_type_class()
        resolve_stat.is_set_orig_default_value_ = true;
        bool is_sync_ddl_user = false;
        if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_dll_user", K(ret));
        } else if (!is_sync_ddl_user || stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_ERR_PARSER_SYNTAX;
          SQL_RESV_LOG(WARN, "Only support for sync ddl user to specify the orig_default_value", K(ret));
        } else if (is_set_orig_default) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_RESV_LOG(WARN, "cannot set orig default value twice", K(ret));
        } else {
          is_set_orig_default = true;
          column.set_orig_default_value(default_value);
        }
      }
    }
  }
  return ret;
}



int ObDDLResolver::resolve_normal_column_attribute(ObColumnSchemaV2 &column,
                                                   ParseNode *attrs_node,
                                                   ObColumnResolveStat &resolve_stat,
                                                   ObString &pk_name,
                                                   const bool allow_has_default)
{
  int ret = OB_SUCCESS;
  ObObjParam default_value;
  default_value.set_null();
  bool is_set_cur_default = false;
  bool is_set_orig_default = false;

  SMART_VAR(ObSArray<ObConstraint>, alter_csts) {
    ObCreateTableStmt *create_table_stmt = nullptr;
    ObAlterTableStmt *alter_table_stmt = nullptr;
    if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
      alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
    } else {
      create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    }

    if (OB_ISNULL(attrs_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("attrs_node is invalid", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < attrs_node->num_child_; ++i) {
      ParseNode *attr_node = attrs_node->children_[i];
      switch (attr_node->type_) {
      case T_CONSTR_NOT_NULL: {
        if (OB_FAIL(resolve_normal_column_attribute_constr_not_null(column, attr_node, resolve_stat))) {
          LOG_WARN("resovle not null constraint failed", K(ret));
        }
        break;
      }
      case T_CONSTR_NULL:
        if (OB_FAIL(resolve_normal_column_attribute_constr_null(column, resolve_stat))) {
          LOG_WARN("resovle null constraint failed", K(ret));
        }
        break;
      case T_CONSTR_PRIMARY_KEY: {
        resolve_stat.is_primary_key_ = true;
        // primary key should not be nullable
        column.set_nullable(false);
        if (ob_is_text_tc(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "BLOB, TEXT column can't be primary key", K(column), K(ret));
        } else if (ob_is_roaringbitmap_tc(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "roaringbitmap column can't be primary key", K(column), K(ret));
        } else if (ob_is_extend(column.get_data_type()) || ob_is_user_defined_sql_type(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "udt column can't be primary key", K(column), K(ret));
        } else if (ob_is_json_tc(column.get_data_type())) {
          ret = OB_ERR_JSON_USED_AS_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "JSON column can't be primary key", K(column), K(ret));
        } else if (ob_is_geometry(column.get_data_type())) {
          ret = OB_ERR_SPATIAL_UNIQUE_INDEX;
          LOG_USER_ERROR(OB_ERR_SPATIAL_UNIQUE_INDEX);
          SQL_RESV_LOG(WARN, "geometry column can't be primary key", K(column), K(ret));
        } else if (ObTimestampTZType == column.get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "TIMESTAMP WITH TIME ZONE column can't be primary key", K(column), K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (lib::is_oracle_mode()) {
          ParseNode *pk_name_node = attr_node->children_[0];
          if (OB_ISNULL(pk_name_node)) {
            // do nothing
          } else {
            pk_name.assign_ptr(pk_name_node->str_value_,static_cast<int32_t>(pk_name_node->str_len_));
          }
        }
        break;
      }
      case T_CONSTR_UNIQUE_KEY: {
        resolve_stat.is_unique_key_ = true;
        if (ob_is_text_tc(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "BLOB, TEXT column can't be unique key", K(column), K(ret));
        } else if (ob_is_roaringbitmap_tc(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "roaringbitmap column can't be unique key", K(column), K(ret));
        } else if (ob_is_extend(column.get_data_type()) || ob_is_user_defined_sql_type(column.get_data_type())) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "UDT column can't be unique key", K(column), K(ret));
        } else if (ob_is_json_tc(column.get_data_type())) {
          ret = OB_ERR_JSON_USED_AS_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "JSON column can't be unique key", K(column), K(ret));
        } else if (ob_is_geometry(column.get_data_type())) {
          ret = OB_ERR_SPATIAL_UNIQUE_INDEX;
          LOG_USER_ERROR(OB_ERR_SPATIAL_UNIQUE_INDEX);
          SQL_RESV_LOG(WARN, "geometry column can't be unique key", K(column), K(ret));
        } else if (ObTimestampTZType == column.get_data_type()) {
          ret = OB_ERR_WRONG_KEY_COLUMN;
          LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "TIMESTAMP WITH TIME ZONE column can't be unique key", K(column), K(ret));
        }
        break;
      }
      case T_CONSTR_ORIG_DEFAULT:
      case T_CONSTR_DEFAULT: {
        if (!allow_has_default) {
          ret = OB_ERR_DEFAULT_NOT_ALLOWED;
          LOG_WARN("Virtual column cannot have a default value", K(ret), K(column));
        } else if (OB_FAIL(resolve_normal_column_attribute_constr_default(column, attr_node, resolve_stat,
                                                                   default_value, is_set_cur_default,
                                                                   is_set_orig_default))) {
          LOG_WARN("resolve default value failed", K(ret));
        }
        LOG_DEBUG("finish resolve default value", K(ret), K(default_value), K(column), K(attr_node->num_child_), K(attr_node->param_num_));
        break;
      }
      case T_CONSTR_AUTO_INCREMENT:
        if (ob_is_text_tc(column.get_data_type()) || ob_is_json_tc(column.get_data_type())
            || ob_is_geometry_tc(column.get_data_type()) || ob_is_roaringbitmap_tc(column.get_data_type())) {
          ret = OB_ERR_COLUMN_SPEC;
          LOG_USER_ERROR(OB_ERR_COLUMN_SPEC, column.get_column_name_str().length(), column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "BLOB, TEXT column can't set autoincrement", K(column), K(default_value), K(ret));
        } else {
          column.set_autoincrement(true);
          resolve_stat.is_autoincrement_ = true;
        }
        break;
      case T_COLUMN_ID: {
        bool is_sync_ddl_user = false;
        if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
          LOG_WARN("Failed to check sync_dll_user", K(ret));
        } else if (!is_sync_ddl_user && !GCONF.enable_sys_table_ddl) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("Only support for sync ddl user or inner_table add column to specify column id",
                    K(ret), K(session_info_->get_user_name()));
        } else if (attr_node->num_child_ != 1
            || OB_ISNULL(attr_node->children_[0])
            || T_INT != attr_node->children_[0]->type_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid node", K(attr_node->children_[0]), K(ret));
        } else {
          const uint64_t column_id = static_cast<uint64_t>(attr_node->children_[0]->value_);
          // 1. 用户指定列从16开始递增 2. 唯一索引隐藏列（shadow列）从32768(32767+1)开始递增
          // 普通用户指定column id：[16, 32767]
          if (column_id < OB_APP_MIN_COLUMN_ID || column_id > OB_MIN_SHADOW_COLUMN_ID) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "invalid column id", K(column_id), K(ret));
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "column id");
          } else {
            column.set_column_id(column_id);
          }
        }
        break;
      }
      case T_COMMENT:{
        if (attr_node->num_child_ != 1
            || OB_ISNULL(attr_node->children_[0])
            || T_VARCHAR != attr_node->children_[0]->type_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid node!", K(ret));
        } else {
          ObString comment(attr_node->children_[0]->str_len_, attr_node->children_[0]->str_value_);
          if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_, session_info_->get_dtc_params(), comment))) {
            LOG_WARN("fail to convert comment to utf8", K(ret), K(comment));
          } else {
            int64_t comment_length = comment.length();
            char *comment_ptr = const_cast<char *>(comment.ptr());
            if(OB_FAIL(ObResolverUtils::check_comment_length(session_info_,
                                                            comment_ptr,
                                                            &comment_length,
                                                            MAX_COLUMN_COMMENT_CHAR_LENGTH))){
              LOG_WARN("fail to check_comment_length", K(ret));
            } else {
              column.set_comment(ObString(comment_length, comment_ptr));
            }
          }
        }
        break;
      }
      case T_ON_UPDATE:
        if (ObDateTimeType == column.get_data_type() || ObTimestampType == column.get_data_type()) {
          if (T_FUN_SYS_CUR_TIMESTAMP != attr_node->children_[0]->type_) {
            ret = OB_ERR_PARSER_SYNTAX;
            SQL_RESV_LOG(WARN, "on_update attribute can only be timestamp or synonyms type",
                        "node_type", attr_node->children_[0]->type_, K(column), K(ret));
          } else {
            int16_t scale = 0;
            if (OB_UNLIKELY(NULL == attr_node->children_[0] || 1 != attr_node->children_[0]->num_child_)) {
              ret = OB_INVALID_ON_UPDATE;
              LOG_WARN("invalid argument", K(ret), K(attr_node->children_[0]));
            } else {
              if (NULL != attr_node->children_[0]->children_[0]) {
                scale = static_cast<int16_t>(attr_node->children_[0]->children_[0]->value_);
              } else {
                //defaule value
              }
              if (column.get_accuracy().get_scale() != scale) {
                ret = OB_INVALID_ON_UPDATE;
                LOG_USER_ERROR(OB_INVALID_ON_UPDATE, column.get_column_name());
                SQL_RESV_LOG(WARN, "Invalid ON UPDATE clause for ",
                          K(column), K(ret));
              } else {
                column.set_on_update_current_timestamp(true);
              }
            }
          }
        } else {
          ret = OB_INVALID_ON_UPDATE;
          LOG_USER_ERROR(OB_INVALID_ON_UPDATE, column.get_column_name());
          SQL_RESV_LOG(WARN, "only ObDateTimeType and ObTimeStampType column can have attribute"
                      " of on_update", K(column), K(ret));
        }
        break;
      case T_CHECK_CONSTRAINT: {
        if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
          // Adding a column-level check constraint is only supported in mysql mode
          if (is_oracle_mode()) {
            ret = OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL;
            LOG_WARN("Adding a column-level check constraint is not supported in oracle mode", K(ret), K(stmt_->get_stmt_type()));
          } else if (OB_FAIL(resolve_check_constraint_node(*attr_node, alter_csts, &column))) {
            SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
          }
        } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(resolve_normal_column_attribute_check_cons(column, attr_node, create_table_stmt))){
            LOG_WARN("failed to resovle normal column attr check constriants", K(ret), K(column), K(attr_node), K(create_table_stmt));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmt_type is unexpected", K(ret), K(stmt_->get_stmt_type()));
        }
        break;
      }
      case T_FOREIGN_KEY: {
        if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
          ret = OB_NOT_SUPPORTED;
          SQL_RESV_LOG(WARN, "Adding a column-level fk while altering table is not supported",
            K(ret), K(stmt_->stmt_type_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Adding column-level foreign key constraints while altering table");
        } else if (OB_FAIL(resolve_normal_column_attribute_foreign_key(column, attr_node, create_table_stmt))) {
          LOG_WARN("failed to resovle normal column attr foreign key",
                            K(ret), K(column), K(attr_node), K(create_table_stmt));
        }
        break;
      }
      case T_EMPTY: {
          // compatible with mysql 5.7 check (expr), do nothing
          // alter table t modify c1 json check(xyz);
          break;
      }
      case T_CONSTR_SRID: {
        if (OB_FAIL(resolve_srid_node(column, *attr_node))) {
          SQL_RESV_LOG(WARN, "fail to resolve srid node", K(ret));
        }
        break;
      }
      case T_COLLATION: {
        if (lib::is_oracle_mode()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "set collate in oracle mode");
          LOG_WARN("set collate in oracle mode is not supported now", K(ret));
        } else if (column.is_string_type() || column.is_enum_or_set()) {
          //To compat with mysql, only check here.
          ObString collation;
          ObCollationType collation_type;
          ObCharsetType charset_type = column.get_charset_type();
          collation.assign_ptr(attr_node->str_value_,
                              static_cast<int32_t>(attr_node->str_len_));
          if (CS_TYPE_INVALID == (collation_type = ObCharset::collation_type(collation))) {
            ret = OB_ERR_UNKNOWN_COLLATION;
            LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
          } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))) {
            SQL_RESV_LOG(WARN, "fail to fill charset and collation info", K(charset_type), K(collation_type), K(ret));
          }
        }
        break;
      }
      case T_COL_SKIP_INDEX: {
        if (OB_FAIL(resolve_column_skip_index(*attr_node, column))) {
          SQL_RESV_LOG(WARN, "fail to resolve column skip index", K(ret));
        }
        break;
      }
      case T_LOB_CHUNK_SIZE:
      case T_CONSTR_LOB_CHUNK_SIZE: {
        if (OB_FAIL(resolve_lob_chunk_size(column, *attr_node))) {
          SQL_RESV_LOG(WARN, "fail to resolve lob chunk size", K(ret), K(attr_node->type_));
        }
        break;
      }
      default:  // won't be here
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "Wrong column attribute", K(ret), K(attr_node->type_));
        break;
      }
    }
    if (OB_SUCC(ret) && stmt::T_ALTER_TABLE == stmt_->get_stmt_type() && alter_csts.count() > 0) {
      AlterTableSchema* alter_table_schema = &alter_table_stmt->get_alter_table_arg().alter_table_schema_;
      for (int64_t i = 0;OB_SUCC(ret) && i < alter_csts.count(); ++i) {
        if (OB_FAIL(alter_table_schema->add_constraint(alter_csts.at(i)))) {
          SQL_RESV_LOG(WARN, "add constraint failed", K(ret));
        }
      }
      alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::ADD_CONSTRAINT;
    }
    if (OB_SUCC(ret) && column.get_cur_default_value().is_null()
        && resolve_stat.is_set_default_value_
        && (!column.is_nullable() || resolve_stat.is_primary_key_)) {
      ret = OB_INVALID_DEFAULT;
      LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      SQL_RESV_LOG(WARN, "Invalid default value", K(column), K(ret));
    }
    //不容许同时设定null 和not null属性；
    //mysql表现为不报错，但是他承认这是个BUG；
    //http://bugs.mysql.com/bug.php?id=79645
    if (OB_SUCC(ret) && resolve_stat.is_set_null_
        && resolve_stat.is_set_not_null_) {
      ret = OB_ERR_COLUMN_DEFINITION_AMBIGUOUS;
      LOG_USER_ERROR(OB_ERR_COLUMN_DEFINITION_AMBIGUOUS, column.get_column_name_str().length(), column.get_column_name_str().ptr());
    }
    if (OB_SUCC(ret) && column.is_autoincrement()) {
      if (column.get_cur_default_value().get_type() != ObNullType) {
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "can not set default value for auto_increment column", K(ret));
      }

      if (OB_SUCC(ret)) {
        // for show create table
        // set auto-increment column default null => still not null
        column.set_nullable(false);

        if (column.get_data_type() < ObTinyIntType || column.get_data_type() > ObUDoubleType) {
          ret = OB_ERR_COLUMN_SPEC;
          LOG_USER_ERROR(OB_ERR_COLUMN_SPEC, column.get_column_name_str().length(),
                        column.get_column_name_str().ptr());
          SQL_RESV_LOG(WARN, "wrong column type for auto_increment", K(ret));
        }
      }
    }
  }
  LOG_DEBUG("resolve normal column attribute end", K(column));
  return ret;
}

int ObDDLResolver::resolve_normal_column_attribute_check_cons(ObColumnSchemaV2 &column,
                                                              ParseNode *attrs_node,
                                                              ObCreateTableStmt *create_table_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObConstraint, 4> &csts = create_table_stmt->get_create_table_arg().constraint_list_;
  if (OB_FAIL(resolve_check_constraint_node(*attrs_node, csts, &column))) {
    SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObDDLResolver::resolve_normal_column_attribute_foreign_key(ObColumnSchemaV2 &column,
                                                               ParseNode *attrs_node,
                                                               ObCreateTableStmt *create_table_stmt)
{
  int ret = OB_SUCCESS;
  ObCreateForeignKeyArg foreign_key_arg;
  if (OB_FAIL(resolve_foreign_key_node(attrs_node, foreign_key_arg, false, &column))) {
    SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret));
  } else if (OB_FAIL(create_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
    SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObDDLResolver::resolve_generated_column_attribute(ObColumnSchemaV2 &column,
                                                      ParseNode *attrs_node,
                                                      ObColumnResolveStat &resolve_stat,
                                                      const bool is_external_table)
{
  int ret = OB_SUCCESS;
  bool is_add_column = false;
  if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    AlterColumnSchema &alter_col_schema = static_cast<AlterColumnSchema &>(column);
    is_add_column = OB_DDL_ADD_COLUMN == alter_col_schema.alter_type_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && attrs_node && i < attrs_node->num_child_; ++i) {
    ParseNode *attr_node = attrs_node->children_[i];
    LOG_DEBUG("resolve generated column attr", K(attr_node->type_), K(resolve_stat));
    switch (attr_node->type_) {
    case T_CONSTR_NOT_NULL:
      if (is_external_table) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Null constraint on external table columns");
      } else if (lib::is_oracle_mode()) {
        if (resolve_stat.is_set_not_null_ || resolve_stat.is_set_null_) {
          ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
          LOG_WARN("duplicate NULL specifications", K(ret));
        } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
          if (OB_FAIL(resolve_not_null_constraint_node(column, attr_node, false))) {
            LOG_WARN("resolve not null constraint failed", K(ret));
          }
        } else if (is_add_column) {
          // there is always a problem with alter table add not null generated column
          // we have to check data validity if table is not empty.
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Alter table add not null generated column");
          LOG_WARN("Alter table add not null generated column not supported", K(ret));
        } else {
          ret  = OB_ERR_COLUMN_EXPRESSION_MODIFICATION_WITH_OTHER_DDL;
          LOG_WARN("cannot modify generated column not null", K(ret));
        }
      } else {
        column.set_nullable(false);
      }
      resolve_stat.is_set_not_null_ = true;
      break;
    case T_CONSTR_NULL:
      if (is_oracle_mode()) {
        if (resolve_stat.is_set_not_null_ || resolve_stat.is_set_null_) {
          ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
          LOG_WARN("duplicate NULL specifications", K(ret));
        } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() || is_add_column) {
        } else {
          if (!column.has_not_null_constraint()) {
            ret = OB_COLUMN_CANT_CHANGE_TO_NULLALE;
            LOG_WARN("column to be modified to NULL cannot be modified to NULL", K(ret));
          } else {
            ret = OB_ERR_VIRTUAL_COL_WITH_CONSTRAINT_CANT_BE_CHANGED;
            LOG_WARN("virtual column with constraint can't be changed", K(ret));
          }
        }
      } else {
        if (!resolve_stat.is_primary_key_) {
          column.set_nullable(true);
        }
      }
      resolve_stat.is_set_null_ = true;
      break;
    case T_CONSTR_PRIMARY_KEY: {
      if (is_external_table) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Primary key constraint on external table columns");
      } else {
//        if (column.is_stored_generated_column()) {
//          resolve_stat.is_primary_key_ = true;
        // primary key should not be nullable
//          column.set_nullable(false);
//        } else {
        ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
        LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
                       "Defining a generated column as primary key");
//        }
      }
      break;
    }
    case T_CONSTR_UNIQUE_KEY: {
      if (is_external_table) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Unique constraint on external table columns");
      } else {
        resolve_stat.is_unique_key_ = true;
      }
      break;
    }
    case T_COMMENT:{
      if (attr_node->num_child_ != 1
          || OB_ISNULL(attr_node->children_[0])
          || (T_CHAR != attr_node->children_[0]->type_
              && T_VARCHAR != attr_node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid node!", K(ret));
      } else {
        ObString comment(attr_node->children_[0]->str_len_, attr_node->children_[0]->str_value_);
        if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(*allocator_, session_info_->get_dtc_params(), comment))) {
          LOG_WARN("fail to convert comment to utf8", K(ret), K(comment));
        } else {
          int64_t comment_length = comment.length();
          char *comment_ptr = const_cast<char *>(comment.ptr());
          if(OB_FAIL(ObResolverUtils::check_comment_length(session_info_,
                                                          comment_ptr,
                                                          &comment_length,
                                                          MAX_COLUMN_COMMENT_CHAR_LENGTH))){
            LOG_WARN("fail to check_comment_length", K(ret));
          } else {
            column.set_comment(ObString(comment_length, comment_ptr));
          }
        }
      }
      break;
    }
    case T_COLUMN_ID: {
      bool is_sync_ddl_user = false;
      if (attr_node->num_child_ != 1
          || OB_ISNULL(attr_node->children_[0])
          || T_INT != attr_node->children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid node", K(attr_node->children_[0]), K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
        LOG_WARN("Failed to check sync_dll_user", K(ret));
      } else if (!is_sync_ddl_user) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("Only support for sync ddl user to specify column id", K(ret), K(session_info_->get_user_name()));
      } else {
        const uint64_t column_id = static_cast<uint64_t>(attr_node->children_[0]->value_);
        // 1. 用户指定列从16开始递增 2. 唯一索引隐藏列（shadow列）从32768(32767+1)开始递增
        // 普通用户指定column id：[16, 32767]
        if (column_id < OB_APP_MIN_COLUMN_ID || column_id > OB_MIN_SHADOW_COLUMN_ID) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid column id", K(column_id), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "column id");
        } else {
          column.set_column_id(column_id);
        }
      }
      break;
    }
    case T_CHECK_CONSTRAINT: {
      if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN,
            "Adding column-level check cst while altering table not supported",
            K(ret), K(stmt_->stmt_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add column-level check constraint while altering table");
      } else if (is_external_table) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Check constraint on external table columns");
      } else {
        ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
        if (OB_FAIL(resolve_normal_column_attribute_check_cons(column,
                                                               attr_node,
                                                               create_table_stmt))){
          LOG_WARN("failed to resovle normal column attr check constriants",
                   K(ret), K(column), K(attr_node), K(create_table_stmt));
        }
      }
      break;
    }
    case T_EMPTY: {
        // compatible with mysql 5.7 check (expr), do nothing
        // alter table t modify c1 json check(xyz);
        break;
    }
    case T_CONSTR_SRID: {
      if (OB_FAIL(resolve_srid_node(column, *attr_node))) {
        SQL_RESV_LOG(WARN, "fail to resolve srid node", K(ret));
      }
      break;
    }
    case T_CONSTR_DEFAULT: {
      if (is_external_table) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "DEFAULT constraint on external table columns");
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        SQL_RESV_LOG(WARN, "Wrong column attribute", K(ret), K(attr_node->type_));
      }
      break;
    }
    default:  // won't be here
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "Wrong column attribute", K(ret), K(attr_node->type_));
      break;
    }
  }
  //不容许同时设定null 和not null属性；
  //mysql表现为不报错，但是他承认这是个BUG；
  //http://bugs.mysql.com/bug.php?id=79645
  if (OB_SUCCESS == ret && resolve_stat.is_set_null_
      && resolve_stat.is_set_not_null_) {
    ret = OB_ERR_COLUMN_DEFINITION_AMBIGUOUS;
    LOG_USER_ERROR(OB_ERR_COLUMN_DEFINITION_AMBIGUOUS, column.get_column_name_str().length(), column.get_column_name_str().ptr());
  }
  return ret;
}

int ObDDLResolver::resolve_srid_node(share::schema::ObColumnSchemaV2 &column,
                                     const ParseNode &srid_node)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  uint64_t tenant_data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, srid attribute");
  } else if (T_CONSTR_SRID != srid_node.type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(srid_node.type_));
  } else {
    if (is_oracle_mode() && tenant_data_version < DATA_VERSION_4_3_2_0) {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "srid column attribute is not supported in oracle mode",
          K(ret), K(srid_node.type_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "srid column attribute is not supported in oracle mode");
    } else {
      if (srid_node.num_child_ != 1
          || OB_ISNULL(srid_node.children_[0])
          || T_INT != srid_node.children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid node", K(ret));
      } else if (!ob_is_geometry_tc(column.get_data_type())) {
        ret = OB_ERR_SRID_WRONG_USAGE;
        SQL_RESV_LOG(WARN, "srid column attribute only support in geometry column",
            K(ret), K(srid_node.type_));
        LOG_USER_ERROR(OB_ERR_SRID_WRONG_USAGE);
      } else {
        int64_t srid = srid_node.children_[0]->value_;
        if (OB_FAIL(ObSqlGeoUtils::check_srid_by_srs(session_info_->get_effective_tenant_id(), srid))) {
          SQL_RESV_LOG(WARN, "invalid srid", K(ret), K(srid));
        } else {
          column.set_srid(srid);
        }
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_lob_storage_parameters(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  ObColumnSchemaV2 *column_schema = nullptr;
  ObString column_name;
  ObObjType type = ObObjType::ObNullType;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt_) ||
      OB_ISNULL(session_info_) || OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpected null value", K(ret), K(schema_checker_),
                 K(stmt_), K(session_info_), K(node));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    SQL_RESV_LOG(WARN, "get tenant data version failed", KR(ret));
  } else if (! ((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "chunk size attribute not support in current version", KR(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "chunk size attribute not support in current version");
  } else if (is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "lob chunk size column attribute is not supported in oracle mode", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "chunk size column attribute in oracle mode");
  } else if (T_LOB_STORAGE_CLAUSE != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", KR(ret), K(node->type_));
  } else if (node->num_child_ != 3) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "num_child_ not correct", K(ret), K(node->num_child_));
  } else if (OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "type node is null", K(ret), K(node));
  } else if (OB_FALSE_IT(type = static_cast<ObObjType>(node->children_[0]->value_))) {
  } else if (! ob_is_json(type)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "type is not support", K(ret), K(type), K(node));
  } else if (OB_ISNULL(node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "column name node is null", K(ret), K(node));
  } else if (OB_FALSE_IT(column_name.assign_ptr(node->children_[1]->str_value_, node->children_[1]->str_len_))) {
  } else if (OB_ISNULL(node->children_[2])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "param node is null", K(ret), K(node));
  } else if (node->children_[2]->num_child_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "param node is empty", K(ret), K(node), K(node->children_[2]->num_child_));
  } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    column_schema = const_cast<ObColumnSchemaV2*>(create_table_stmt->get_column_schema(column_name));
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
    ObTableSchema &tbl_schema = alter_table_stmt->get_alter_table_schema();
    for (ObTableSchema::const_column_iterator iter = tbl_schema.column_begin();
          iter != tbl_schema.column_end() && nullptr == column_schema; ++iter) {
      ObColumnSchemaV2 &column = (**iter);
      if (column.get_column_name_str().case_compare(column_name) == 0) {
        const AlterColumnSchema &alter_col_schema = static_cast<const AlterColumnSchema &>(column);
        if (alter_col_schema.alter_type_ != OB_DDL_ADD_COLUMN) {
          ret = OB_NOT_SUPPORTED;
          SQL_RESV_LOG(WARN, "lob chunk size column attribute is not supported modify",
              KR(ret), K(alter_col_schema));
        } else {
          column_schema = &column;
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "not supported statement for lob storage parameter", K(ret), K(stmt_->get_stmt_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(column_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "column not found", K(ret), K(type), K(column_name));
  } else if (column_schema->get_data_type() != type) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "column type node match", K(ret), K(type), K(*column_schema));
  } else {
    for (int64_t i = 0; i < node->children_[2]->num_child_ && OB_SUCC(ret); i ++) {
      if (OB_FAIL(resolve_lob_storage_parameter(*column_schema, *node->children_[2]->children_[i]))) {
        SQL_RESV_LOG(WARN, "resolve_lob_storage_parameter fail", K(ret), K(type), K(*column_schema));
      }
    }
  }
  return ret;
}
int ObDDLResolver::resolve_lob_storage_parameter(share::schema::ObColumnSchemaV2 &column, const ParseNode &param_node)
{
  int ret = OB_SUCCESS;
  switch (param_node.type_) {
    case T_LOB_CHUNK_SIZE:
    case T_CONSTR_LOB_CHUNK_SIZE: {
      if (OB_FAIL(resolve_lob_chunk_size(column, param_node))) {
        SQL_RESV_LOG(WARN, "fail to resolve lob meta size", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "UnKnown type", K(ret), K(param_node.type_));
      break;
    }
  }
  return ret;
}
int ObDDLResolver::resolve_lob_chunk_size(const ParseNode &size_node, int64_t &lob_chunk_size)
{
  int ret = OB_SUCCESS;
  const char *str = nullptr;
  int64_t len = 0;
  int64_t value = 0;
  bool valid = false;
  ObString unit;
  if (T_CONSTR_LOB_CHUNK_SIZE != size_node.type_ && T_LOB_CHUNK_SIZE != size_node.type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", KR(ret), K(size_node.type_));
  } else if (size_node.num_child_ != 1
      || OB_ISNULL(size_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid node", KR(ret));
  } else if (T_LOB_CHUNK_SIZE == size_node.type_) {
    lob_chunk_size = size_node.children_[0]->value_ * 1024;
  } else if (OB_ISNULL(str = size_node.children_[0]->str_value_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "size node value is null", KR(ret), K(size_node.type_));
  } else {
    int64_t i = 0;
    len = size_node.children_[0]->str_len_;
    // calc integer part
    for (; i < len; ++i) {
      char c = str[i];
      if (isdigit(c)) {
        value = value * 10 + (c - '0');
      } else {
        break;
      }
    }
    // cacl unit part, only support kb
    unit.assign_ptr(str + i, len - i);
    if (i >= len) {
      // if no unit, use kb as unit
      valid = true;
      value <<= 10;
    } else if (0 == unit.case_compare("kb")
        || 0 == unit.case_compare("k")) {
      value <<= 10;
      valid = true;
    }
    if (valid) {
      lob_chunk_size = value;
    } else {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "size input is invalid", KR(ret), K(ObString(len, str)));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (lob_chunk_size < OB_MIN_LOB_CHUNK_SIZE || lob_chunk_size > OB_MAX_LOB_CHUNK_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "lob meta size invalid", KR(ret), K(lob_chunk_size));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid CHUNK LOB storage option value");
  }
  return ret;
}
int ObDDLResolver::resolve_lob_chunk_size(
    share::schema::ObColumnSchemaV2 &column,
    const ParseNode &lob_chunk_size_node)
{
  int ret = OB_SUCCESS;
  int64_t lob_chunk_size = 0;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    SQL_RESV_LOG(WARN, "get tenant data version failed", KR(ret));
  } else if (! ((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "lob chunk size column attribute is not supported in oracle mode",
        KR(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "chunk size attribute not support in current version");
  } else if (is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "lob chunk size column attribute is not supported in oracle mode",
        KR(ret), K(lob_chunk_size_node.type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "chunk size column attribute in oracle mode");
  } else if (! column.is_json()) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "lob chunk size column attribute is only supported for json",
        KR(ret), K(lob_chunk_size_node.type_), K(column));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "chunk size column attribute only supported for json, others");
  } else if (OB_FAIL(resolve_lob_chunk_size(lob_chunk_size_node, lob_chunk_size))) {
    SQL_RESV_LOG(WARN, "resolve size node fail", KR(ret));
  } else {
    column.set_lob_chunk_size(lob_chunk_size);
  }
  return ret;
}

int ObDDLResolver::resolve_lob_inrow_threshold(const ParseNode *option_node, const bool is_index_option)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session_info_ is null", K(ret));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("lob inrow threshold is not supported in data version less than 4.2.1.2", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "lob inrow threshold is not supported in data version less than 4.2.1.2");
  } else if (is_index_option) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index option should not specify lob inrow threshold", K(ret));
  } else if (OB_ISNULL(option_node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "option_node is null", K(ret));
  } else if (OB_ISNULL(option_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the children of option_node is null", K(option_node->children_), K(ret));
  } else if (OB_ISNULL(option_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"children can't be null", K(ret));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"stmt_ is null", K(ret));
  } else {
    lob_inrow_threshold_ = option_node->children_[0]->value_;
    is_set_lob_inrow_threshold_ = true;
    if (lob_inrow_threshold_ < OB_MIN_LOB_INROW_THRESHOLD || lob_inrow_threshold_ > OB_MAX_LOB_INROW_THRESHOLD) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(ERROR, "invalid inrow threshold", K(ret), K(lob_inrow_threshold_));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "lob inrow threshold, should be [0, 786432]");
    } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
      if (OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::LOB_INROW_THRESHOLD))) {
        SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
      }
    }
  }
  return ret;
}

/*
int ObDDLResolver::resolve_generated_column_definition(ObColumnSchemaV2 &column,
    ParseNode *node, ObColumnResolveStat &resolve_stat)
{
  int ret = OB_SUCCESS;
  ParseNode *name_node = NULL;
  if (T_COLUMN_DEFINITION != node->type_ || node->num_child_ != 6 ||
     OB_ISNULL(allocator_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0]) ||
     T_COLUMN_REF != node->children_[0]->type_ ||
     COLUMN_DEF_NUM_CHILD != node->children_[0]->num_child_){
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "invalid parse node",K(ret));
  } else {
    resolve_stat.reset();
    name_node = node->children_[0]->children_[2];
    if (OB_ISNULL(name_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "name node can not be null", K(ret));
    } else if (OB_FAIL(resolve_column_definition_ref(column, name_node))) {
      SQL_RESV_LOG(WARN, "fail to resolve column_definition_ref", K(ret));
    }
  }
  ParseNode *type_node = NULL;
  if (OB_SUCC(ret)) {
    type_node = node->children_[1];
  }
  if (OB_SUCC(ret) && type_node != NULL) {
    ObDataType data_type;
    const ObLengthSemantics default_length_semantics = LS_BYTE;
    if (OB_FAIL(ObResolverUtils::resolve_data_type(*type_node,
                                                   column.get_column_name_str(),
                                                   data_type,
                                                   (session_info_ != NULL && is_oracle_mode()),
                                                   default_length_semantics))) {
      LOG_WARN("resolve data type failed", K(ret), K(column.get_column_name_str()));
    } else if (ObExtendType == data_type.get_obj_type()) {
      const ParseNode *name_node = type_node->children_[0];
      CK (OB_NOT_NULL(session_info_) && OB_NOT_NULL(schema_checker_));
      CK (OB_NOT_NULL(name_node));
      CK (T_SP_TYPE == name_node->type_);
      if (OB_SUCC(ret)) {
        uint64_t udt_id = OB_INVALID_ID;
        uint64_t db_id = session_info_->get_database_id();
        if (NULL != name_node->children_[0]) {
          OZ (schema_checker_->get_database_id(session_info_->get_effective_tenant_id(),
                                            ObString(name_node->children_[0]->str_len_, name_node->children_[0]->str_value_),
                                            db_id));
        }
        OZ (schema_checker_->get_udt_id(session_info_->get_effective_tenant_id(), db_id, OB_INVALID_ID,
                       ObString(name_node->children_[1]->str_len_, name_node->children_[1]->str_value_), udt_id));
        if (OB_SUCC(ret)) {
          data_type.set_udt_id(udt_id);
        }
      }
    } else { }

    if (OB_SUCC(ret)) {
      column.set_meta_type(data_type.get_meta_type());
      column.set_accuracy(data_type.get_accuracy());
      column.set_charset_type(data_type.get_charset_type());
      if (data_type.is_binary_collation()) {
        column.set_binary_collation(true);
        column.set_collation_type(CS_TYPE_INVALID);
      }
      if (OB_SUCC(ret) && column.is_string_type() && stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        if (OB_FAIL(check_and_fill_column_charset_info(column, charset_type_, collation_type_))) {
          SQL_RESV_LOG(WARN, "fail to check and fill column charset info", K(ret));
        } else if (data_type.get_meta_type().is_lob()) {
          if (OB_FAIL(check_text_column_length_and_promote(column))) {
            SQL_RESV_LOG(WARN, "fail to check text or blob column length", K(ret), K(column));
          }
        } else if (OB_FAIL(check_string_column_length(column, lib::is_oracle_mode()))) {
          SQL_RESV_LOG(WARN, "fail to check string column length", K(ret), K(column));
        }
      }
      if (OB_SUCC(ret) && ObRawType == column.get_data_type() && stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
        if (OB_FAIL(ObDDLResolver::check_raw_column_length(column))) {
          SQL_RESV_LOG(WARN, "failed to check raw column length", K(ret), K(column));
        }
      }
    }
  }
  return ret;
}
*/

int ObDDLResolver::resolve_identity_column_attribute(ObColumnSchemaV2 &column,
                                                     ParseNode *attrs_node,
                                                     ObColumnResolveStat &resolve_stat,
                                                     ObString &pk_name)
{
  int ret = OB_SUCCESS;
  ObObjParam default_value;
  default_value.set_null();
  ObCreateTableStmt *create_table_stmt = NULL;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_ISNULL(attrs_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("attrs_node is invalid", K(ret));
  } else {
    create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < attrs_node->num_child_; ++i) {
    ParseNode *attr_node = attrs_node->children_[i];
    switch (attr_node->type_) {
    case T_CONSTR_NOT_NULL:
      if (resolve_stat.is_set_not_null_) {
        ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
        LOG_WARN("duplicate NOT NULL specifications", K(ret));
      } else if (OB_FAIL(resolve_not_null_constraint_node(column, attr_node, true))) {
        SQL_RESV_LOG(WARN, "resolve not null constraint failed", K(ret));
      }
      resolve_stat.is_set_not_null_ = true;
      break;
    case T_CONSTR_NULL:
      if (resolve_stat.is_set_not_null_) {
        ret = OB_ERR_DUPLICATE_NULL_SPECIFICATION;
        LOG_WARN("duplicate NOT NULL specifications", K(ret));
      } else if (column.is_default_on_null_identity_column()) {
        ret = OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_IDENTITY_COLUMN;
        LOG_WARN("nullable identity column not allowed", K(ret));
      } else {
        ret = OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
        LOG_WARN("nullable identity column not allowed", K(ret));
      }
      break;
    case T_CONSTR_DEFAULT:
      ret = OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE;
      LOG_USER_ERROR(OB_ERR_IDENTITY_COLUMN_CANNOT_HAVE_DEFAULT_VALUE);
      break;
    case T_CONSTR_PRIMARY_KEY: {
      resolve_stat.is_primary_key_ = true;
      // primary key should not be nullable
      column.set_nullable(false);
      if (ob_is_text_tc(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "BLOB, TEXT column can't be primary key", K(column), K(ret));
      } else if (ob_is_roaringbitmap_tc(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "roaringbitmap column can't be primary key", K(column), K(ret));
      } else if (ob_is_extend(column.get_data_type()) || ob_is_user_defined_sql_type(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "UDT column can't be primary key", K(column), K(ret));
      } else if (ob_is_json_tc(column.get_data_type())) {
        ret = OB_ERR_JSON_USED_AS_KEY;
        LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "JSON column can't be primary key", K(column), K(ret));
      } else if (ObTimestampTZType == column.get_data_type()) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "TIMESTAMP WITH TIME ZONE column can't be primary key", K(column), K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (lib::is_oracle_mode()) {
        ParseNode *pk_name_node = attr_node->children_[0];
        if (OB_ISNULL(pk_name_node)) {
          // do nothing
        } else {
          pk_name.assign_ptr(pk_name_node->str_value_,static_cast<int32_t>(pk_name_node->str_len_));
        }
      }
      break;
    }
    case T_CONSTR_UNIQUE_KEY: {
      resolve_stat.is_unique_key_ = true;
      if (ob_is_text_tc(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "BLOB, TEXT column can't be unique key", K(column), K(ret));
      } else if (ob_is_text_tc(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "roaringbitmap column can't be unique key", K(column), K(ret));
      } else if (ob_is_extend(column.get_data_type()) || ob_is_user_defined_sql_type(column.get_data_type())) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "UDT column can't be unique key", K(column), K(ret));
      } else if (ob_is_json_tc(column.get_data_type())) {
        ret = OB_ERR_JSON_USED_AS_KEY;
        LOG_USER_ERROR(OB_ERR_JSON_USED_AS_KEY, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "JSON column can't be unique key", K(column), K(ret));
      } else if (ObTimestampTZType == column.get_data_type()) {
        ret = OB_ERR_WRONG_KEY_COLUMN;
        LOG_USER_ERROR(OB_ERR_WRONG_KEY_COLUMN, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        SQL_RESV_LOG(WARN, "TIMESTAMP WITH TIME ZONE column can't be unique key", K(column), K(ret));
      }
      break;
    }
    case T_COLUMN_ID: {
      bool is_sync_ddl_user = false;
      if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info_, is_sync_ddl_user))) {
        LOG_WARN("Failed to check sync_dll_user", K(ret));
      } else if (!is_sync_ddl_user && !GCONF.enable_sys_table_ddl) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("Only support for sync ddl user or inner_table add column to specify column id",
                  K(ret), K(session_info_->get_user_name()));
      } else if (attr_node->num_child_ != 1
          || OB_ISNULL(attr_node->children_[0])
          || T_INT != attr_node->children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid node", K(attr_node->children_[0]), K(ret));
      } else {
        const uint64_t column_id = static_cast<uint64_t>(attr_node->children_[0]->value_);
        // 1. 用户指定列从16开始递增 2. 唯一索引隐藏列（shadow列）从32768(32767+1)开始递增
        // 普通用户指定column id：[16, 32767]
        if (column_id < OB_APP_MIN_COLUMN_ID || column_id > OB_MIN_SHADOW_COLUMN_ID) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid column id", K(column_id), K(ret));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "column id");
        } else {
          column.set_column_id(column_id);
        }
      }
      break;
    }
    case T_COMMENT:{
      if (attr_node->num_child_ != 1
          || OB_ISNULL(attr_node->children_[0])
          || T_VARCHAR != attr_node->children_[0]->type_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid node!", K(ret));
      } else {
        int64_t comment_length = attr_node->children_[0]->str_len_;
        char *comment_ptr = const_cast<char *>(attr_node->children_[0]->str_value_);
        if(OB_FAIL(ObResolverUtils::check_comment_length(session_info_,
                                                        comment_ptr,
                                                        &comment_length,
                                                        MAX_COLUMN_COMMENT_CHAR_LENGTH))){
          LOG_WARN("fail to check_comment_length", K(ret));
        } else {
          column.set_comment(ObString(comment_length, comment_ptr));
        }
      }
      break;
    }
    case T_CHECK_CONSTRAINT: {
      if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN,
            "Adding column-level check cst while altering table not supported",
            K(ret), K(stmt_->stmt_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add column-level check constraint while altering table");
      } else {
        ObSEArray<ObConstraint, 4> &csts = create_table_stmt->get_create_table_arg().constraint_list_;
        if (OB_FAIL(resolve_check_constraint_node(*attr_node, csts, &column))) {
          SQL_RESV_LOG(WARN, "resolve constraint failed", K(ret));
        }
      }
      break;
    }
    case T_FOREIGN_KEY: {
      if (stmt::T_CREATE_TABLE != stmt_->get_stmt_type()) {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "Adding a column-level fk while altering table is not supported",
          K(ret), K(stmt_->stmt_type_));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Adding column-level foreign key constraint while altering table");
      } else {
        ObCreateForeignKeyArg foreign_key_arg;
        if (OB_FAIL(resolve_foreign_key_node(attr_node, foreign_key_arg, false, &column))) {
          SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret));
        } else if (OB_FAIL(create_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
          SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
        }
      }
      break;
    }
    default:
      ret = OB_ERR_PARSER_SYNTAX;
      SQL_RESV_LOG(WARN, "Wrong column attribute", K(ret), K(attr_node->type_));
      break;
    }
  }

  return ret;
}

int ObDDLResolver::resolve_tablespace_node(const ParseNode *node, int64_t &tablespace_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_[0]) || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tablespace node or session_info_ is invalid", K(ret));
  } else {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    const ObTablespaceSchema *tablespace_schema = NULL;
    const ParseNode *tablespace_node = node->children_[0];
    const ObString tablespace_name(tablespace_node->str_len_, tablespace_node->str_value_);
    if (OB_ISNULL(schema_checker_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema checker ptr is null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_tablespace_schema(tenant_id, tablespace_name, tablespace_schema))) {
      LOG_WARN("fail to get tablespace schema", K(ret), K(tablespace_name));
    } else if (OB_ISNULL(tablespace_schema)) {
      ret = OB_TABLESPACE_NOT_EXIST;
      LOG_WARN("tablespace schema is not exist", K(ret), K(tenant_id), K(tablespace_name));
    } else {
      tablespace_id = tablespace_schema->get_tablespace_id();
    }
  }
  return ret;
}

int ObDDLResolver::cast_default_value(ObObj &default_value,
                                      const ObTimeZoneInfo *tz_info,
                                      const common::ObString *nls_formats,
                                      ObIAllocator &allocator,
                                      ObColumnSchemaV2 &column_schema,
                                      const ObSQLMode sql_mode)
{
  //cast default value
  int ret = OB_SUCCESS;
  const int64_t CUR_TIME = 0;
  bool need_cast = false;

  if (default_value.is_null()) {
    need_cast = false;
  } else if (column_schema.get_data_type() != default_value.get_type()) {
    need_cast = true;
  } else if (column_schema.is_string_type() && default_value.is_string_type()
             && column_schema.get_collation_type() != default_value.get_collation_type()) {
    need_cast = true;
  }

  if (need_cast) {
    ObAccuracy res_accuracy;
    const ObDataTypeCastParams dtc_params(tz_info, nls_formats, CS_TYPE_INVALID, CS_TYPE_INVALID, CS_TYPE_UTF8MB4_GENERAL_CI);
    ObCastMode cast_mode = lib::is_oracle_mode() ? CM_ORACLE_MODE : CM_COLUMN_CONVERT;
    if (is_allow_invalid_dates(sql_mode)) {
      cast_mode |= CM_ALLOW_INVALID_DATES;
    }
    if (is_no_zero_date(sql_mode)) {
      cast_mode |= CM_NO_ZERO_DATE;
    }
    if (column_schema.get_meta_type().is_decimal_int()) {
      res_accuracy = column_schema.get_accuracy();
    }
    ObCastCtx cast_ctx(&allocator, &dtc_params, CUR_TIME,
                       cast_mode,
                       column_schema.get_collation_type(), NULL, &res_accuracy);
    if (ob_is_enumset_tc(column_schema.get_data_type())) {
      if (OB_FAIL(cast_enum_or_set_default_value(column_schema, cast_ctx, default_value))) {
        LOG_WARN("fail to cast enum or set default value", K(default_value), K(column_schema), K(ret));
      }
    } else if (IS_DEFAULT_NOW_OBJ(default_value)) {
      if (ObDateTimeTC == column_schema.get_data_type_class()) {
        if (OB_FAIL(column_schema.set_cur_default_value(default_value))) {
          SQL_RESV_LOG(WARN, "set current default value failed", K(ret));
        }
      } else {
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column_schema.get_column_name_str().length(),
                       column_schema.get_column_name_str().ptr());
      }
    } else {
      ObObjType obj_type = column_schema.get_data_type();
      ObObjTypeClass obj_type_class = column_schema.get_data_type_class();
      // for decimal int default value, we cast it to number first, then cast to decimal int
      if (ObDecimalIntType == obj_type) {
        obj_type = ObNumberType;
        obj_type_class = ObNumberTC;
      }
      //so cool. cast in place.
      if(OB_FAIL(ObObjCaster::to_type(obj_type, cast_ctx, default_value, default_value))) {
        SQL_RESV_LOG(WARN, "cast default value failed", K(ret),
                     "src_type", default_value.get_type(), "dst_type",
                     column_schema.get_data_type(), K(ret));
      } else if (ObNumberTC == obj_type_class) {
        number::ObNumber nmb;
        nmb = default_value.get_number();
        if (lib::is_oracle_mode()) {
          const ObObj *res_obj = &default_value;
          const common::ObAccuracy &accuracy = column_schema.get_accuracy();
          if (OB_FAIL(common::obj_accuracy_check(cast_ctx, accuracy,
                             column_schema.get_collation_type(),
                             *res_obj, default_value, res_obj))) {
            SQL_RESV_LOG(WARN, "check and round number failed on oracle mode", K(ret), K(default_value), K(*res_obj));
          }
        } else {
          if (OB_FAIL(nmb.check_and_round(column_schema.get_data_precision(),
                                          column_schema.get_data_scale()))) {
            SQL_RESV_LOG(WARN, "check and round number failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (ObNumberTC == column_schema.get_data_type_class()) {
            default_value.set_number(column_schema.get_data_type(), nmb);
          } else {
            ObDecimalInt *decint = nullptr;
            int32_t int_bytes = 0;
            const ObScale scale = column_schema.get_data_scale();
            if (OB_ISNULL(cast_ctx.allocator_v2_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("allocator is null", K(ret));
            } else if (OB_FAIL(wide::from_number(nmb, *cast_ctx.allocator_v2_, scale, decint, int_bytes))) {
              LOG_WARN("fail to cast number to deciaml int", K(ret));
            } else {
              default_value.set_decimal_int(int_bytes, scale, decint);
            }
          }
        }
      } else if (lib::is_mysql_mode() &&
                   (ObFloatTC == column_schema.get_data_type_class() ||
                      ObDoubleTC == column_schema.get_data_type_class()) &&
                   (column_schema.get_data_precision() != PRECISION_UNKNOWN_YET &&
                    column_schema.get_data_scale() != SCALE_UNKNOWN_YET)) {
        const ObObj *res_obj = &default_value;
        const common::ObAccuracy &accuracy = column_schema.get_accuracy();
        if (OB_FAIL(common::obj_accuracy_check(cast_ctx, accuracy,
                            column_schema.get_collation_type(),
                            *res_obj, default_value, res_obj))) {
          SQL_RESV_LOG(WARN, "check default value failed on mysql mode", K(ret), K(default_value), K(*res_obj));
        }
      } else if (ObRoaringBitmapTC == column_schema.get_data_type_class()) {
        // remove lob header
        ObString real_str;
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, default_value, real_str))) {
          LOG_WARN("fail to get real data.", K(ret), K(real_str));
        } else {
          default_value.set_string(ObRoaringBitmapType, real_str);
        }
      }
      if (OB_FAIL(ret)) {
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column_schema.get_column_name_str().length(),
                       column_schema.get_column_name_str().ptr());
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (default_value.get_type() == column_schema.get_data_type()
        && (ObTimeTC == column_schema.get_data_type_class() ||
            ObDateTimeTC == column_schema.get_data_type_class())) {
      int64_t value = default_value.get_time();
      ObTimeConverter::round_datetime(column_schema.get_data_scale(), value);
      default_value.set_time_value(value);
    }
  }
  return ret;
}

int ObDDLResolver::trim_space_for_default_value(
    const bool is_mysql_mode,
    const bool is_char_type,
    const ObCollationType &collation_type,
    ObObj &default_value,
    ObString &out_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(default_value.get_varchar(out_str))) {
    LOG_WARN("invalid default data", K(ret), K(default_value));
  } else if (is_mysql_mode && is_char_type) {
    const char *str = out_str.ptr();
    int32_t len = out_str.length();
    ObString space_pattern = ObCharsetUtils::get_const_str(collation_type, ' ');
    for (; len >= space_pattern.length(); len -= space_pattern.length()) {
      if (0 != MEMCMP(str + len - space_pattern.length(), space_pattern.ptr(), space_pattern.length())) {
        break;
      }
    }
    default_value.set_varchar(str, len);
    out_str.assign_ptr(str, len);
  }
  return ret;
}

int ObDDLResolver::check_default_value_length(const bool is_mysql_mode,
                                              const ObColumnSchemaV2 &column,
                                              ObObj &default_value)
{
  int ret = OB_SUCCESS;
  int64_t strlen = -1;
  if (ObStringTC == column.get_data_type_class()) {
     if (CS_TYPE_INVALID == column.get_collation_type()) {
       ret = OB_ERR_UNEXPECTED;
       SQL_RESV_LOG(ERROR, "invaild default data type", K(ret));
     } else {
      // get characters of default value under specified charset
      ObString str;
      const bool is_byte_length = is_oracle_byte_length(!is_mysql_mode, column.get_length_semantics());
      if (default_value.is_null()) {
        strlen = 0;
      } else if (OB_FAIL(trim_space_for_default_value(is_mysql_mode, column.get_meta_type().is_char(), column.get_collation_type(), default_value, str))) {
        SQL_RESV_LOG(WARN, "trim space for default value failed", K(ret));
      } else if (is_mysql_mode && str.empty()) {
        strlen = 0;
        default_value.set_varchar("");
      } else {
        strlen = is_byte_length ? str.length() : ObCharset::strlen_char(column.get_collation_type(), str.ptr(), str.length());
      }
      if (OB_SUCC(ret) && strlen > column.get_data_length()) {
        ret = OB_INVALID_DEFAULT;
        SQL_RESV_LOG(WARN, "Invalid default value: length is larger than data length",
                     "default_value", str, "length", strlen, "data_length", column.get_data_length(),
                     K(is_byte_length));
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      }
    }
  } else if (ObBitTC == column.get_data_type_class()) {
    int32_t bit_len = 0;
    if (default_value.is_null()) {
      //do nothing
    } else if (OB_FAIL(get_bit_len(default_value.get_bit(), bit_len))) {
      SQL_RESV_LOG(WARN, "fail to  get bit length", K(ret), K(default_value), K(bit_len));
    } else if (bit_len > column.get_data_precision()) {
      ret = OB_INVALID_DEFAULT;
      SQL_RESV_LOG(WARN, "default value length is larger than column length", K(ret),
                   K(default_value), K(bit_len), K(column.get_data_precision()));
      LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObDDLResolver::build_partition_key_info(ObTableSchema &table_schema,
                                            common::ObSEArray<ObString, 4> &partition_keys,
                                            const ObPartitionFuncType &part_func_type)
{
  int ret = OB_SUCCESS;
  ObRawExpr *partition_key_expr = NULL;
  ObArray<ObQualifiedName> qualified_names;
  if (OB_FAIL(ObResolverUtils::build_partition_key_expr(
          params_, table_schema, partition_key_expr, &qualified_names))) {
    LOG_WARN("failed to build partition key expr!", K(ret));
  } else if (OB_UNLIKELY(qualified_names.count() <= 0)) {
    //no primary key, error now
    ret = OB_ERR_FIELD_NOT_FOUND_PART;
    LOG_WARN("Field in list of fields for partition function not found in table", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < qualified_names.count(); ++i) {
      ObQualifiedName &q_name = qualified_names.at(i);
      if (OB_FAIL(partition_keys.push_back(q_name.col_name_))) {
        LOG_WARN("push back failed", K(ret), K(q_name.col_name_));
      }
    }
  }
  return ret;
}

int ObDDLResolver::set_partition_keys(
    ObTableSchema &table_schema,
    ObIArray<ObString> &partition_keys,
    const bool is_subpart)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_keys.count(); ++i) {
    if (is_subpart) {
      if (OB_FAIL(table_schema.add_subpartition_key(partition_keys.at(i)))) {
        LOG_WARN("Failed to add subpartition key", K(ret), "key_name", partition_keys.at(i));
      }
    } else if (OB_FAIL(table_schema.add_partition_key(partition_keys.at(i)))) {
      LOG_WARN("Failed to add partition key", K(ret), "key_name", partition_keys.at(i));
    } else { }//do nothing
  }
  return ret;
}

void ObDDLResolver::reset() {
  block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
  consistency_level_ = INVALID_CONSISTENCY;
  index_scope_ = NOT_SPECIFIED;
  replica_num_ = 0;
  tablet_size_ = -1;
  charset_type_ = CHARSET_INVALID;
  collation_type_ = CS_TYPE_INVALID;
  use_bloom_filter_ = false;
  expire_info_.reset();
  compress_method_.reset();
  parser_name_.reset();
  comment_.reset();
  tablegroup_name_.reset();
  primary_zone_.reset();
  row_store_type_ = MAX_ROW_STORE;
  store_format_ = OB_STORE_FORMAT_INVALID;
  progressive_merge_num_= OB_DEFAULT_PROGRESSIVE_MERGE_NUM;
  storage_format_version_ = OB_STORAGE_FORMAT_VERSION_INVALID;
  table_id_ = OB_INVALID_ID;
  data_table_id_ = OB_INVALID_ID;
  index_table_id_ = OB_INVALID_ID;
  partition_func_type_ = PARTITION_FUNC_TYPE_HASH;
  auto_increment_ = 1;
  index_name_.reset();
  index_keyname_ = NORMAL_KEY;
  global_ = true;
  store_column_names_.reset();
  hidden_store_column_names_.reset();
  sort_column_array_.reset();
  storing_column_set_.reset();
  locality_.reset();
  is_random_primary_zone_ = false;
  enable_row_movement_ = false;
  table_mode_.reset();
  encryption_.reset();
  tablespace_id_ = OB_INVALID_ID;
  table_dop_ = DEFAULT_TABLE_DOP;
  hash_subpart_num_ = -1;
  ttl_definition_.reset();
  kv_attributes_.reset();
  is_set_lob_inrow_threshold_ = false;
  lob_inrow_threshold_ = OB_DEFAULT_LOB_INROW_THRESHOLD;
  auto_increment_cache_size_ = 0;
}

bool ObDDLResolver::is_valid_prefix_key_type(const ObObjTypeClass column_type_class)
{
  return column_type_class == ObStringTC || column_type_class == ObTextTC;
}

int ObDDLResolver::check_prefix_key(const int32_t prefix_len,
                                    const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  if (prefix_len > 0) {
    if (!is_valid_prefix_key_type(column_schema.get_data_type_class())) {
      ret = OB_WRONG_SUB_KEY;

      SQL_RESV_LOG(WARN, "The used key part isn't a string", "data_type_class",
                   column_schema.get_data_type_class(), K(ret));
    } else if (column_schema.get_data_length() < prefix_len) {
      ret = OB_WRONG_SUB_KEY;
      LOG_USER_ERROR(OB_WRONG_SUB_KEY);
      SQL_RESV_LOG(WARN, "The used length is longer than the key part", K(prefix_len), K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::check_string_column_length(const ObColumnSchemaV2 &column, const bool is_oracle_mode, const bool is_prepare_stage)
{
  int ret = OB_SUCCESS;
  if(ObStringTC != column.get_data_type_class()
     || CHARSET_INVALID == column.get_charset_type()
     || CS_TYPE_INVALID == column.get_collation_type()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"column infomation is error", K(column), K(ret));
  } else if (ObCharType == column.get_data_type()
             || ObNCharType == column.get_data_type()) {
  /* 兼容mysql
   * mysql中char和binary最长255字符，varchar和varbinary最长65536字节
   * varchar(N)&varbinary(N): N表示字符个数, N的上限依赖于具体的字符集
   * char(N)&binary(N): N表示字符个数, 上限为255字符
   * oralce
   * char(N)&raw(N): N表示byte个数, 上限为2000byte
   */
    const int64_t max_char_length = is_oracle_mode ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE : OB_MAX_CHAR_LENGTH;
    const int64_t data_len = column.get_data_length();
    if (data_len < 0 || data_len > max_char_length) {
      ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
      LOG_WARN("column data length is invalid", K(ret), K(max_char_length), "real_data_length", column.get_data_length());
      LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(), static_cast<int>(max_char_length));
    } else if (is_oracle_mode && 0 == data_len) {
      ret = OB_ERR_ZERO_LEN_COL;
      LOG_WARN("column data length cannot be zero on oracle mode", K(ret), K(data_len));
    }
  } else if (ObVarcharType == column.get_data_type()
             || ObNVarchar2Type == column.get_data_type()) {
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(column.get_collation_type(), mbmaxlen))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "fail to get mbmaxlen", K(ret), K(column.get_collation_type()));
    } else {
      const int64_t data_len = column.get_data_length();
      if (0 == mbmaxlen){
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(ERROR, "mbmaxlen can not be 0", K(ret));
      } else if ((!is_prepare_stage && data_len < 0) ||
          (is_oracle_mode
            ? data_len > OB_MAX_ORACLE_VARCHAR_LENGTH
            : data_len * mbmaxlen > OB_MAX_VARCHAR_LENGTH)) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        const uint64_t real_data_length = static_cast<uint64_t>(data_len);
        LOG_WARN("column data length is invalid", K(ret), K(data_len), K(real_data_length), K(mbmaxlen));
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(),
            static_cast<int>(is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH : OB_MAX_VARCHAR_LENGTH/mbmaxlen));
      } else if (is_oracle_mode && 0 == data_len) {
        ret = OB_ERR_ZERO_LEN_COL;
        LOG_WARN("column data length cannot be zero on oracle mode", K(ret), K(data_len));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check_string_column_length failed", K(ret), K(column));
  }
  return ret;
}

// raw(N): N 表示 byte 个数, 上限为 2000 byte
int ObDDLResolver::check_raw_column_length(const share::schema::ObColumnSchemaV2 &column) {
  int ret = OB_SUCCESS;

  if(ObRawType != column.get_data_type()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"column infomation is error", K(column), K(ret));
  } else {
    const int64_t data_len = column.get_data_length();
    if (data_len < 0 || data_len > OB_MAX_ORACLE_RAW_SQL_COL_LENGTH) {
      ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
      LOG_WARN("column data length is invalid",
               K(ret), K(OB_MAX_ORACLE_RAW_SQL_COL_LENGTH),
               "real_data_length", column.get_data_length());
      LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH,
                     column.get_column_name(),
                     static_cast<int>(OB_MAX_ORACLE_RAW_SQL_COL_LENGTH));
    } else if (0 == data_len) {
      ret = OB_ERR_ZERO_LEN_COL;
      LOG_WARN("column data length cannot be zero on oracle mode", K(ret), K(data_len));
    }
  }

  return ret;
}

int ObDDLResolver::check_urowid_column_length(const share::schema::ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  if (ObURowIDType != column.get_data_type()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "column information is error", K(ret), K(column));
  } else {
    const int64_t data_len = column.get_data_length();
    if (data_len < 0 || data_len > OB_MAX_USER_ROW_KEY_LENGTH) {
      // create table t (a urwoid(int_num));
      // int_num是urowij解码之d的最大长度，也就是主键的最大长度
      ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
      LOG_WARN("column data length is invalid", K(ret), K(OB_MAX_USER_ROW_KEY_LENGTH),
               "real_data_length", column.get_data_length());
      LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, column.get_column_name(),
                     static_cast<int>(OB_MAX_USER_ROW_KEY_LENGTH));
    } else if (0 == data_len) {
      ret = OB_ERR_ZERO_LEN_COL;
      LOG_WARN("column data length cannot be zero on oracle mode", K(ret), K(data_len));
    }
  }
  return ret;
}

int ObDDLResolver::check_text_length(ObCharsetType cs_type,
                                     ObCollationType co_type,
                                     const char *name,
                                     ObObjType &type,
                                     int32_t &length,
                                     bool need_rewrite_length,
                                     const bool is_byte_length/* = false */)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 0;
  int32_t default_length = ObAccuracy::DDL_DEFAULT_ACCURACY[type].get_length();
  if(!(ob_is_text_tc(type) || ob_is_json_tc(type) || ob_is_geometry_tc(type) || ob_is_roaringbitmap_tc(type))
     || CHARSET_INVALID == cs_type || CS_TYPE_INVALID == co_type) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"column infomation is error", K(cs_type), K(co_type), K(ret));
  } else if (!is_byte_length &&
             OB_FAIL(ObCharset::get_mbmaxlen_by_coll(co_type, mbmaxlen))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "fail to get mbmaxlen", K(ret), K(co_type));
  } else if (is_byte_length && OB_FALSE_IT(mbmaxlen = 1)) {
  } else if (0 == mbmaxlen) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR, "mbmaxlen can not be 0", K(ret), K(co_type), K(mbmaxlen));
  } else if (lib::is_oracle_mode() || 0 == length) {
    length = default_length;
  } else if (0 > length) {
    ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
    LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, name,
                   static_cast<int>(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType].get_length() / mbmaxlen));
    SQL_RESV_LOG(WARN, "fail to check column data length",
                 K(ret), K(length), K(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType].get_length()), K(mbmaxlen));
  } else {
    // eg. text(128) will be tinytext in mysql, and text(65537) will be mediumtext
    if (ObTextType == type) {
      if (length * mbmaxlen > ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType].get_length()) {
        ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
        LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, name, static_cast<int>(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType].get_length() / mbmaxlen));
      } else {
        for (int64_t i = ObTinyTextType; i <= ObLongTextType; ++i) {
          default_length = ObAccuracy::DDL_DEFAULT_ACCURACY[i].get_length();
          if (length * mbmaxlen <= default_length) {
            type = static_cast<ObObjType>(i);
            length = default_length;
            break;
          }
        }
      }//int -> tinytext
    } else if (lib::is_mysql_mode()
      && CS_TYPE_BINARY == co_type && length * mbmaxlen > default_length) {
      ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, name, static_cast<int>(default_length / mbmaxlen));
      SQL_RESV_LOG(WARN, "fail to check column data length",
                   K(ret), K(default_length), K(length), K(mbmaxlen));
    } else {
      length = default_length;
    }
  }

  if (OB_SUCC(ret) && lib::is_mysql_mode() && need_rewrite_length) {
    if (OB_FAIL(rewrite_text_length_mysql(type, length))) {
      LOG_WARN("check_text_length_mysql fails", K(ret), K(type), K(length));
    }
  }
  return ret;
}

// old version ObTinyTextType, ObTextType, ObMediumTextType, ObLongTextType max_length is incorrect
// correct max_legth is ObTinyTextType:255 etc.
// so when create new user table, must rewrite max column length
int ObDDLResolver::rewrite_text_length_mysql(ObObjType &type, int32_t &length)
{
  int ret = OB_SUCCESS;
  int32_t max_length = ObAccuracy::MAX_ACCURACY[type].get_length();
  if (length < 0 || length > max_length) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length can not be less than 0 or larger than max_length",
        K(ret), K(type), K(length), K(max_length));
  } else if (ob_is_text_tc(type) && max_length == length) {
    length = length - 1;
  }
  return ret;
}

// TODO@hanhui texttc should care about the the defined length not the actual length
int ObDDLResolver::check_text_column_length_and_promote(ObColumnSchemaV2 &column,
                                                        int64_t table_id,
                                                        const bool is_byte_length/* = false */)
{
  int ret = OB_SUCCESS;
  bool need_check_length = true;
  ObObjType type = column.get_data_type();
  int32_t length = column.get_data_length();
  if (OB_INVALID_ID != table_id && is_inner_table(table_id)) {
    // inner table don't need to rewrite
    // if table_id == OB_INVALID_ID, this is not inner_table
    need_check_length = false;
  }
  if (OB_FAIL(check_text_length(column.get_charset_type(),
                                column.get_collation_type(),
                                column.get_column_name(),
                                type,
                                length,
                                need_check_length,
                                is_byte_length))) {
    LOG_WARN("failed to check text length", K(ret), K(column));
  } else {
    column.set_data_type(type);
    column.set_data_length(length);
  }
  return ret;
}

int ObDDLResolver::check_and_fill_column_charset_info(ObColumnSchemaV2 &column,
                                                      const ObCharsetType table_charset_type,
                                                      const ObCollationType table_collation_type) {
  int ret = OB_SUCCESS;
  ObCharsetType charset_type = CHARSET_INVALID;
  ObCollationType collation_type = CS_TYPE_INVALID;;
  if (CHARSET_INVALID == table_charset_type || CS_TYPE_INVALID == table_collation_type) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid column charset info!", K(ret));
  } else {
    charset_type = column.get_charset_type();
    collation_type = column.get_collation_type();
  }
  if (OB_FAIL(ret)) {
    //empty
  } else if (column.is_binary_collation() && CS_TYPE_INVALID == collation_type) {
    if (CHARSET_INVALID == charset_type) {
      column.set_charset_type(table_charset_type);
      column.set_collation_type(ObCharset::get_bin_collation(table_charset_type));
    } else {
      column.set_charset_type(charset_type);
      column.set_collation_type(ObCharset::get_bin_collation(charset_type));
    }
  } else if (CHARSET_INVALID == charset_type && CS_TYPE_INVALID == collation_type) {
    column.set_charset_type(table_charset_type);
    column.set_collation_type(table_collation_type);
  } else if (OB_FAIL(ObCharset::check_and_fill_info(charset_type, collation_type))) {
    SQL_RESV_LOG(WARN, "fail to fill charset and collation info", K(collation_type), K(ret));
  } else {
    column.set_charset_type(charset_type);
    column.set_collation_type(collation_type);
  }
  return ret;
}

int ObDDLResolver::is_gen_col_with_udf(const ObTableSchema &table_schema,
                                       const ObRawExpr *col_expr,
                                       bool &res)
{
  int ret = OB_SUCCESS;
  res = false;
  uint64_t col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *col_schema = NULL;
  const ObColumnRefRawExpr *column_expr = NULL;
  if (OB_ISNULL(col_expr)) {
    // do nothing
  } else if (!col_expr->is_column_ref_expr()) {
    // mult col(partition by hash(c1, c2)) may be T_FUN_SYS_PART_HASH
    for (int64_t i = 0; !res && OB_SUCC(ret) && i < col_expr->get_param_count(); ++i) {
      OZ (SMART_CALL(is_gen_col_with_udf(table_schema, col_expr->get_param_expr(i), res)), i);
    }
  } else if (FALSE_IT(col_id = static_cast<const ObColumnRefRawExpr *>(col_expr)->get_column_id())) {
  } else if (OB_INVALID_ID == col_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column ref expr", KPC(static_cast<const ObColumnRefRawExpr *>(col_expr)));
  } else if(OB_ISNULL(col_schema = table_schema.get_column_schema(col_id))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got null column schema", KPC(static_cast<const ObColumnRefRawExpr *>(col_expr)));
  } else {
    res = col_schema->is_generated_column_using_udf();
  }
  return ret;
}

int ObDDLResolver::resolve_part_func(ObResolverParams &params,
                                     const ParseNode *node,
                                     const ObPartitionFuncType partition_func_type,
                                     const ObTableSchema &table_schema,
                                     ObIArray<ObRawExpr *> &part_func_exprs,
                                     ObIArray<ObString> &partition_keys)
{
  int ret = OB_SUCCESS;
  part_func_exprs.reset();
  partition_keys.reset();
  bool is_gen_col_udf = false;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret));
  } else if (T_EXPR_LIST == node->type_) {
    if (node->num_child_ < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Partition fun node should not less than 1", K(ret));
    } else if (node->num_child_ > OB_MAX_PART_COLUMNS) {
      ret = OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS;
      LOG_WARN("Too may partition func fields", K(ret));
    } else {
      ObRawExpr *func_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
        func_expr = NULL;
        if (OB_ISNULL(node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is null", K(ret));
        } else if (OB_UNLIKELY(T_EXPR_LIST == node->children_[i]->type_)) {
          ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;
          LOG_WARN("row expr as partition function is not allowed for hash/range/list partition");
        } else if (OB_FAIL(ObResolverUtils::resolve_partition_expr(params,
                                                                   *(node->children_[i]),
                                                                   table_schema,
                                                                   partition_func_type,
                                                                   func_expr,
                                                                   &partition_keys))) {
          LOG_WARN("resolve partition expr failed", K(ret));
        } else if (OB_FAIL(is_gen_col_with_udf(table_schema, func_expr, is_gen_col_udf))) {
          LOG_WARN("failed to check gen column is udf", K(ret));
        } else if (is_gen_col_udf) {
          ret = OB_ERR_USE_UDF_IN_PART;
          LOG_USER_ERROR(OB_ERR_USE_UDF_IN_PART);
        } else if (OB_FAIL(part_func_exprs.push_back(func_expr))) {
          LOG_WARN("array push back fail", K(ret));
        } else {
          LOG_DEBUG("succ to resolve_part_func", KPC(func_expr));

        }//do nothing
      } // end of for
    }
  } else {
    ObRawExpr *func_expr = NULL;
    if (OB_FAIL(ObResolverUtils::resolve_partition_expr(params,
                                                        *node,
                                                        table_schema,
                                                        partition_func_type,
                                                        func_expr,
                                                        &partition_keys))) {
      LOG_WARN("resolve partition expr failed", K(ret));
    } else if (OB_FAIL(is_gen_col_with_udf(table_schema, func_expr, is_gen_col_udf))) {
      LOG_WARN("failed to check gen column is udf", K(ret));
    } else if (is_gen_col_udf) {
      ret = OB_ERR_USE_UDF_IN_PART;
      LOG_USER_ERROR(OB_ERR_USE_UDF_IN_PART);
    } else if (OB_FAIL(part_func_exprs.push_back(func_expr))) {
      LOG_WARN("array push back fail", K(ret));
    } else if (partition_keys.count() > OB_MAX_PART_COLUMNS) {
      ret = OB_ERR_TOO_MANY_PARTITION_FUNC_FIELDS;
      LOG_WARN("too may partition func fields", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    //check duplicate of PARTITION_FUNC_TYPE_RANGE_COLUMNS
    /* because key range has checked as sys(c1,c1) before, so here key is no need check */
    if (OB_SUCC(ret)) {
      bool need_check_dup_col = false;
      if (lib::is_oracle_mode()) {
        need_check_dup_col = true;
      } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == partition_func_type
                 || PARTITION_FUNC_TYPE_LIST_COLUMNS == partition_func_type) {
        need_check_dup_col = true;
      }
      if (need_check_dup_col) {
        for (int64_t idx = 0; OB_SUCC(ret) && idx < partition_keys.count(); ++idx) {
          const ObString &key_name = partition_keys.at(idx);
          for (int64_t b_idx = 0; OB_SUCC(ret) && b_idx < idx; ++b_idx) {
            if (ObCharset::case_insensitive_equal(key_name, partition_keys.at(b_idx))) {
              if (!lib::is_oracle_mode()) {
                ret = OB_ERR_SAME_NAME_PARTITION_FIELD;
                LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION_FIELD, key_name.length(), key_name.ptr());
              } else {
                ret = OB_ERR_FIELD_SPECIFIED_TWICE;
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_list_partition_elements(const ObDDLStmt *stmt,
                                                   ParseNode *node,
                                                   const bool is_subpartition,
                                                   const ObPartitionFuncType part_type,
                                                   const ObIArray<ObRawExpr *> &part_func_exprs,
                                                   ObDDLStmt::array_t &list_value_exprs,
                                                   ObIArray<ObPartition> &partitions,
                                                   ObIArray<ObSubPartition> &subpartitions,
                                                   const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt_));
  } else {
    int64_t partition_num = node->num_child_;
    ParseNode *partition_expr_list = node;
    ObPartition partition;
    ObSubPartition subpartition;
    bool has_empty_name = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      subpartition.reset();
      partition.reset();
      ParseNode *element_node = partition_expr_list->children_[i];
      if (OB_ISNULL(element_node)
          || OB_ISNULL(element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else {
        ObString partition_name;
        ObBasePartition *target_partition = is_subpartition ?
            static_cast<ObBasePartition*>(&subpartition) : static_cast<ObBasePartition*>(&partition);
        if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                           partition_name,
                                           *target_partition))) {
          LOG_WARN("failed to resolve partition name", K(ret));
        } else if (target_partition->is_empty_partition_name()) {
          has_empty_name = true;
        }
        ParseNode *expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE];
        if (T_EXPR_LIST != expr_list_node->type_ && T_DEFAULT != expr_list_node->type_) { //也有可能等于default
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr_list_node->type_ is not T_EXPR_LIST or T_DEFAULT", K(ret),
                   "expr_list_node type", expr_list_node->type_);
        } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
          // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
          LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
        }
        //检查tablespace
        int64_t tablespace_id = OB_INVALID_ID;
        if (OB_SUCC(ret)) {
          if (element_node->num_child_ > 3 && OB_NOT_NULL(element_node->children_[3])) {
            ParseNode *physical_node = element_node->children_[3];
            if (physical_node->num_child_ == 1) {
              if (T_TABLESPACE == physical_node->type_ &&
                  OB_FAIL(resolve_tablespace_node(physical_node, tablespace_id))) {
                LOG_WARN("fail to resolve tablespace node", K(ret));
              }
            } else {
              for (int ph_i = 0; OB_SUCC(ret) && ph_i < physical_node->num_child_; ++ph_i) {
                if (OB_ISNULL(physical_node->children_[ph_i])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("node is null", K(ret));
                } else if (T_TABLESPACE == physical_node->children_[ph_i]->type_) {
                  if (OB_FAIL(resolve_tablespace_node(physical_node->children_[ph_i], tablespace_id))) {
                    LOG_WARN("fail to resolve tablespace node", K(ret));
                  }
                }
              }
            }
          }
        }
        //add list partition elements to table schema
        if (OB_SUCC(ret)) {
          if (is_subpartition) {
            subpartition.set_tablespace_id(tablespace_id);
            if (OB_FAIL(subpartitions.push_back(subpartition))) {
              LOG_WARN("fail to push back subpartition", KR(ret), K(subpartition));
            }
          } else {
            partition.set_tablespace_id(tablespace_id);
            if (OB_FAIL(partitions.push_back(partition))) {
              LOG_WARN("Failed to push back partition", KR(ret), K(partition));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_list_partition_value_node(*expr_list_node, partition_name,
                                                        part_type, part_func_exprs,
                                                        list_value_exprs, in_tablegroup))) {
            LOG_WARN("failed to resolve list partition value node", K(ret));
          }
        }
      }
    }
    if (OB_UNLIKELY(has_empty_name) &&
        (stmt::T_CREATE_TABLE == stmt->get_stmt_type() ||
         stmt::T_CREATE_TABLEGROUP == stmt->get_stmt_type() ||
         stmt::T_CREATE_INDEX == stmt->get_stmt_type())) {
      if (OB_FAIL(create_name_for_empty_partition(is_subpartition, partitions, subpartitions))) {
        LOG_WARN("failed to create name for empty [sub]partitions", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_range_partition_elements(const ObDDLStmt *stmt,
                                                    ParseNode *node,
                                                    const bool is_subpartition,
                                                    const ObPartitionFuncType part_type,
                                                    const ObIArray<ObRawExpr *> &part_func_exprs,
                                                    ObIArray<ObRawExpr *> &range_value_exprs,
                                                    ObIArray<ObPartition> &partitions,
                                                    ObIArray<ObSubPartition> &subpartitions,
                                                    const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt));
  } else {
    int64_t partition_num = node->num_child_;
    ParseNode *partition_expr_list = node;
    ObPartition partition;
    ObSubPartition subpartition;
    bool has_empty_name = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      subpartition.reset();
      partition.reset();
      ParseNode *element_node = partition_expr_list->children_[i];
      if (OB_ISNULL(element_node)
          || OB_ISNULL(element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else {
        ObString partition_name;
        ObBasePartition *target_partition = is_subpartition ?
            static_cast<ObBasePartition*>(&subpartition) : static_cast<ObBasePartition*>(&partition);
        if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                           partition_name,
                                           *target_partition))) {
          LOG_WARN("failed to resolve partition name", K(ret));
        } else if (target_partition->is_empty_partition_name()) {
          has_empty_name = true;
        }
        ParseNode *expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE];
        if (T_EXPR_LIST != expr_list_node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr_list_node->type_ is not T_EXPR_LIST", K(ret));
        } else if (part_func_exprs.count() != expr_list_node->num_child_) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
        } else {
            //检查tablespace
          int64_t tablespace_id = OB_INVALID_ID;
          if (OB_SUCC(ret)) {
            if (element_node->num_child_ > 3 && OB_NOT_NULL(element_node->children_[3])) {
              ParseNode *physical_node = element_node->children_[3];
              if (physical_node->num_child_ == 1) {
                if (T_TABLESPACE == physical_node->type_ &&
                    OB_FAIL(resolve_tablespace_node(physical_node, tablespace_id))) {
                  LOG_WARN("fail to resolve tablespace node", K(ret));
                }
              } else {
                for (int ph_i = 0; OB_SUCC(ret) && ph_i < physical_node->num_child_; ++ph_i) {
                  if (OB_ISNULL(physical_node->children_[ph_i])) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("node is null", K(ret));
                  } else if (T_TABLESPACE == physical_node->children_[ph_i]->type_) {
                    if (OB_FAIL(resolve_tablespace_node(physical_node->children_[ph_i], tablespace_id))) {
                      LOG_WARN("fail to resolve tablespace node", K(ret));
                    }
                  }
                }
              }
            }
          }
          //add range partition elements to table schema
          if (OB_SUCC(ret)) {
            if (is_subpartition) {
              subpartition.set_tablespace_id(tablespace_id);
              if (OB_FAIL(subpartitions.push_back(subpartition))) {
                LOG_WARN("fail to push back subpartition", KR(ret), K(subpartition));
              }
            } else {
              partition.set_tablespace_id(tablespace_id);
              if (OB_FAIL(partitions.push_back(partition))) {
                LOG_WARN("fail to push back partition", KR(ret), K(partition));
              } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
                // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
                LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
              }
            }
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < expr_list_node->num_child_; j++) {
            if (OB_ISNULL(expr_list_node->children_[j])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node is null", K(ret));
            } else if (T_MAXVALUE == expr_list_node->children_[j]->type_) {
              ObRawExpr *maxvalue_expr = NULL;
              ObConstRawExpr *c_expr = NULL;
              c_expr = (ObConstRawExpr *)allocator_->alloc(sizeof(ObConstRawExpr));
              if (NULL != c_expr) {
                c_expr = new (c_expr) ObConstRawExpr();
                maxvalue_expr = c_expr;
                maxvalue_expr->set_data_type(common::ObMaxType);
                if (OB_FAIL(range_value_exprs.push_back(maxvalue_expr))) {
                  LOG_WARN("array push back fail", K(ret));
                }
              } else {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              }
            } else if (T_NULL == expr_list_node->children_[j]->type_) {
              ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
              LOG_WARN("null value is not allowed in less than", K(ret));
            } else if (T_EXPR_LIST != expr_list_node->children_[j]->type_) {
              ObRawExpr *part_value_expr = NULL;
              ObRawExpr *part_func_expr = NULL;
              if (OB_FAIL(part_func_exprs.at(j, part_func_expr))) {
                LOG_WARN("get part expr failed", K(j), "size", part_func_exprs.count(), K(ret));
              } else if (OB_ISNULL(part_func_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("part_func_expr is invalid", K(ret));
              } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params_,
                                                                                     *(expr_list_node->children_[j]),
                                                                                     partition_name,
                                                                                     part_type,
                                                                                     *part_func_expr,
                                                                                     part_value_expr,
                                                                                     in_tablegroup))) {
                LOG_WARN("resolve partition expr failed", K(ret));
              } else if (OB_FAIL(range_value_exprs.push_back(part_value_expr))) {
                LOG_WARN("array push back fail", K(ret));
              }
            } else {
              ret = OB_ERR_PARSER_SYNTAX;
              LOG_WARN("syntax error, expect single expr while expr list got", K(ret));
            }
          }
        }
      }
    }
    if (OB_UNLIKELY(has_empty_name) &&
        (stmt::T_CREATE_TABLE == stmt->get_stmt_type() ||
         stmt::T_CREATE_TABLEGROUP == stmt->get_stmt_type() ||
         stmt::T_CREATE_INDEX == stmt->get_stmt_type())) {
      if (OB_FAIL(create_name_for_empty_partition(is_subpartition, partitions, subpartitions))) {
        LOG_WARN("failed to create name for empty [sub]partitions", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_partition_name_duplicate(ParseNode *node, bool is_oracle_modle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else {
    hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE> *partition_name_set = nullptr;
    void *buf = nullptr;
    int64_t partition_num = node->num_child_;
    ParseNode *partition_expr_list = node;
    ParseNode *element_node = NULL;
    if (OB_ISNULL(buf = allocator_->alloc(sizeof(
              hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret));
    } else {
      partition_name_set = new(buf)hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(partition_name_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition name hash set is null", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      element_node = partition_expr_list->children_[i];
      if (OB_ISNULL(element_node)
          || OB_ISNULL(element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else if ((OB_ISNULL(element_node->children_[PARTITION_NAME_NODE])
            || OB_ISNULL(element_node->children_[PARTITION_NAME_NODE]->children_[NAMENODE]))
          && !is_oracle_modle) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else {
        ObString partition_name;
        if (OB_NOT_NULL(element_node->children_[PARTITION_NAME_NODE])) {
          ParseNode *partition_name_node = element_node->children_[PARTITION_NAME_NODE]->children_[NAMENODE];
          partition_name.assign_ptr(partition_name_node->str_value_,
              static_cast<int32_t>(partition_name_node->str_len_));
          ObPartitionNameHashWrapper partition_name_key(partition_name);
          if (OB_HASH_EXIST == partition_name_set->exist_refactored(partition_name_key)) {
            ret = OB_ERR_SAME_NAME_PARTITION;
            LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, partition_name.length(), partition_name.ptr());
          } else {
            if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
              LOG_WARN("add partition name to map failed", K(ret), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::fill_extended_type_info(const ParseNode &str_list_node, ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> type_info_array;
  CK(NULL != session_info_);
  CK(NULL != allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObResolverUtils::resolve_extended_type_info(str_list_node, type_info_array))) {
    LOG_WARN("failed to resolve extended type info", K(ret));
  } else {
    // The resolved extended type info's collation is session connection collation,
    // need convert to column collation finally. The converting may be done in RS,
    // but the session connection collation info is lost in RS, so we convert to a
    // intermediate collation (system collation) here. Convert from the intermediate
    // collation to column collation finally in ObResolverUtils::check_extended_type_info().
    ObCollationType src = session_info_->get_local_collation_connection();
    ObCollationType dst = ObCharset::get_system_collation();
    FOREACH_CNT_X(str, type_info_array, OB_SUCC(ret)) {
      OZ(ObCharset::charset_convert(*allocator_, *str, src, dst, *str));
    }
  }
  OZ(column.set_extended_type_info(type_info_array));
  return ret;
}

int ObDDLResolver::resolve_enum_or_set_column(const ParseNode *type_node, ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type_node)
      || OB_UNLIKELY(type_node->num_child_ != 4)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type node is NULL", K(ret), K(type_node), K(session_info_));
  } else if (OB_ISNULL(type_node->children_[3])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is NULL", K(ret));
  } else if (OB_FAIL(fill_extended_type_info(*(type_node->children_[3]), column))) {
    LOG_WARN("fail to fill type info", K(ret), K(column));
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    //下面的操作可能需要依赖于table的charset，因此alter table时需要在RS中完成。
  } else if (OB_FAIL(check_and_fill_column_charset_info(column, charset_type_, collation_type_))) {
    LOG_WARN("fail to check and fill column charset info", K(ret), K(column), K(charset_type_), K(collation_type_));
  } else if (OB_FAIL(check_extended_type_info(column, session_info_->get_sql_mode()))) {
    LOG_WARN("fail to fill extended type info", K(ret), K(column), K(session_info_->get_sql_mode()));
  } else if (OB_FAIL(calc_enum_or_set_data_length(column))) {
    LOG_WARN("fail to calc data length", K(ret), K(column));
  }
  return ret;
}

int ObDDLResolver::calc_enum_or_set_data_length(const ObIArray<common::ObString> &type_info,
                                                const ObCollationType &collation_type,
                                                const ObObjType &type,
                                                int32_t &length)
{
  int ret = OB_SUCCESS;
  int32_t cur_len = 0;
  if (OB_UNLIKELY(ObEnumType != type && ObSetType != type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column type", K(ret));
  } else if (ObEnumType == type) {
    for (int64_t i = 0; OB_SUCC(ret) && i < type_info.count(); ++i) {
      const ObString &type_str = type_info.at(i);
      cur_len = static_cast<int32_t>(ObCharset::strlen_char(collation_type, type_str.ptr(), type_str.length()));
      length = length < cur_len ? cur_len : length;
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < type_info.count(); ++i) {
      const ObString &type_str = type_info.at(i);
      cur_len = static_cast<int32_t>(ObCharset::strlen_char(collation_type, type_str.ptr(), type_str.length()));
      length += cur_len;
    }
    length += (static_cast<int32_t>(type_info.count()) - 1)
        * ObCharsetUtils::get_const_str(collation_type, ',').length();
  }
  return ret;
}

int ObDDLResolver::calc_enum_or_set_data_length(ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  int32_t length = 0;
  if (OB_FAIL(calc_enum_or_set_data_length(column.get_extended_type_info(),
                                           column.get_collation_type(),
                                           column.get_data_type(),
                                           length))) {
    LOG_WARN("failed to calc enum or set data length", K(ret));
  } else {
    column.set_data_length(length);
  }
  return ret;
}

int ObDDLResolver::check_extended_type_info(ObColumnSchemaV2 &column, ObSQLMode sql_mode)
{
  return ObResolverUtils::check_extended_type_info(*allocator_,
                                                   column.get_extended_type_info(),
                                                   ObCharset::get_system_collation(),
                                                   column.get_column_name_str(),
                                                   column.get_data_type(),
                                                   column.get_collation_type(),
                                                   sql_mode);
}

int ObDDLResolver::check_duplicates_in_type_infos(const ObColumnSchemaV2 &col, ObSQLMode sql_mode, int32_t &dup_cnt)
{
  return ObResolverUtils::check_duplicates_in_type_infos(col.get_extended_type_info(),
                                                         col.get_column_name_str(),
                                                         col.get_data_type(),
                                                         col.get_collation_type(),
                                                         sql_mode,
                                                         dup_cnt);
}

int ObDDLResolver::check_type_info_incremental_change(const ObColumnSchemaV2 &ori_schema,
                                                      const ObColumnSchemaV2 &new_schema,
                                                      bool &is_incremental)
{
  int ret = OB_SUCCESS;
  is_incremental = true;
  const ObIArray<common::ObString> &ori_type_info = ori_schema.get_extended_type_info();
  const ObIArray<common::ObString> &new_type_info = new_schema.get_extended_type_info();
  if (OB_UNLIKELY(ori_schema.get_charset_type() != new_schema.get_charset_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret), K(ori_schema), K(new_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change column charset");
  } else if (OB_UNLIKELY(ori_schema.get_collation_type() != new_schema.get_collation_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret), K(ori_schema), K(new_schema));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change column collation");
  } else if (new_type_info.count() < ori_type_info.count()) {
    is_incremental = false;
  } else {
    ObCollationType coll_type = ori_schema.get_collation_type();
    for (int64_t i = 0; OB_SUCC(ret) && i < ori_type_info.count(); ++i) {
      const ObString &ori_str = ori_type_info.at(i);
      const ObString &new_str = new_type_info.at(i);
      if (0 != ObCharset::strcmp(coll_type, ori_str.ptr(), ori_str.length(), new_str.ptr(), new_str.length())) {
        is_incremental = false;
      }
    }
  }
  return ret;
}

int ObDDLResolver::cast_enum_or_set_default_value(const ObColumnSchemaV2 &column, ObCastCtx &cast_ctx, ObObj &def_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!def_val.is_string_type())) {
    ret = OB_INVALID_DEFAULT;
    LOG_WARN("invalid default value type", K(def_val), K(ret));
    LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(),
                   column.get_column_name_str().ptr());
  } else if (OB_UNLIKELY(!ob_is_enumset_tc(column.get_data_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column type", K(column), K(ret));
  } else {
    ObExpectType expect_type;
    expect_type.set_type(column.get_data_type());
    expect_type.set_collation_type(column.get_collation_type());
    expect_type.set_type_infos(&column.get_extended_type_info());
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, def_val, def_val))) {
      ret = OB_INVALID_DEFAULT;
      LOG_WARN("fail to cast to enum or set", K(def_val), K(expect_type), K(ret));
      LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(),
                     column.get_column_name_str().ptr());
    }
  }

  return ret;
}

int ObDDLResolver::print_expr_to_default_value(ObRawExpr &expr,
    share::schema::ObColumnSchemaV2 &column, ObSchemaChecker *schema_checker, const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  }
  HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
    MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
    int64_t pos = 0;
    ObString expr_def;
    ObObj default_value;
    ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos,
                                  schema_checker->get_schema_guard(), tz_info);
    if (OB_FAIL(expr_printer.do_print(&expr, T_NONE_SCOPE, true))) {
      LOG_WARN("print expr definition failed", K(expr), K(ret));
    } else if (FALSE_IT(expr_def.assign_ptr(expr_str_buf, static_cast<int32_t>(pos)))) {
    } else if (FALSE_IT(default_value.set_varchar(expr_def))) {
    } else if (FALSE_IT(default_value.set_collation_type(ObCharset::get_system_collation()))) {
    } else if (OB_FAIL(column.set_cur_default_value(default_value))) {
      LOG_WARN("set orig default value failed", K(ret));
    } else {
      LOG_DEBUG("succ to print_expr_to_default_value", K(expr), K(column), K(default_value), K(expr_def), K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::check_dup_gen_col(const ObString &expr,
                                     ObIArray<ObString> &gen_col_expr_arr)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && i < gen_col_expr_arr.count(); i++) {
    if (expr == gen_col_expr_arr.at(i)) {
      ret = OB_ERR_DUPLICATE_COLUMN_EXPRESSION_WAS_SPECIFIED;
      LOG_WARN("check dup generated columns faild", K(ret));
    }
  }
  return ret;
}

// this function is called in ddl service, which can not access user session_info, so need to
// construct an empty session
int ObDDLResolver::init_empty_session(const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                      const common::ObString *nls_formats,
                                      const share::schema::ObLocalSessionVar *local_session_var,
                                      ObIAllocator &allocator,
                                      ObTableSchema &table_schema,
                                      const ObSQLMode sql_mode,
                                      ObSchemaChecker *schema_checker,
                                      ObSQLSessionInfo &empty_session)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObTenantSchema *tenant_schema = NULL;
  bool is_oracle_compat_mode = true;
  const ObDatabaseSchema *db_schema = NULL;
  if (OB_ISNULL(schema_checker)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null schema checker", K(ret));
  } else if (OB_FAIL(empty_session.test_init(0, 0, 0, &allocator))) {
    LOG_WARN("init empty session failed", K(ret));
  } else if (OB_FAIL(schema_checker->get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("get tenant_schema failed", K(ret));
  } else if (OB_FAIL(empty_session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
    LOG_WARN("init tenant failed", K(ret));
  } else if (OB_FAIL(empty_session.load_all_sys_vars(*(schema_checker->get_schema_guard())))) {
    LOG_WARN("session load system variable failed", K(ret));
  } else if (OB_FAIL(empty_session.load_default_configs_in_pc())) {
    LOG_WARN("session load default configs failed", K(ret));
  } else if (OB_FAIL(empty_session.set_tz_info_wrap(tz_info_wrap))) {
    LOG_WARN("fail to set set_tz_info_wrap", K(ret));
  } else if (OB_FAIL(schema_checker->get_database_schema(tenant_id, table_schema.get_database_id(), db_schema))) {
    LOG_WARN("get database schema failed", K(ret));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database info is null", K(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_compat_mode))) {
    LOG_WARN("failed to get table compatibility mode", K(ret));
  } else {
    ObSessionDDLInfo ddl_info;
    ddl_info.set_ddl_check_default_value(true);
    empty_session.set_ddl_info(ddl_info);
    empty_session.set_nls_formats(nls_formats);
    empty_session.set_compatibility_mode(
      is_oracle_compat_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    empty_session.set_sql_mode(sql_mode);
    empty_session.set_default_database(db_schema->get_database_name_str());
    empty_session.set_database_id(table_schema.get_database_id());
  }
  if (OB_SUCC(ret) && NULL != local_session_var) {
    if (OB_FAIL(local_session_var->update_session_vars_with_local(empty_session))) {
      LOG_WARN("fail to update session vars", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::reformat_generated_column_expr(ObObj &default_value,
                                                  const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                                  const common::ObString *nls_formats,
                                                  const share::schema::ObLocalSessionVar &local_session_var,
                                                  ObIAllocator &allocator,
                                                  ObTableSchema &table_schema,
                                                  ObColumnSchemaV2 &column,
                                                  const ObSQLMode sql_mode,
                                                  ObSchemaChecker *schema_checker)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaV2 *> dummy_array;
  ObString expr_str;
  ObRawExpr *expr = NULL;
  ObRawExprFactory expr_factory(allocator);
  SMART_VAR(ObSQLSessionInfo, empty_session) {
    if (OB_FAIL(init_empty_session(tz_info_wrap,
                                   nls_formats,
                                   &local_session_var,
                                   allocator,
                                   table_schema,
                                   sql_mode,
                                   schema_checker,
                                   empty_session))) {
      LOG_WARN("failed to init empty session", K(ret));
    } else if (OB_FAIL(default_value.get_string(expr_str))) {
      LOG_WARN("failed to get expr str", K(ret));
    } else if (OB_FAIL(resolve_generated_column_expr(expr_str, allocator, table_schema, dummy_array, column,
                                          &empty_session, schema_checker, expr, expr_factory))) {
      LOG_WARN("check default value failed", K(ret));
    }
  }
  return ret;
}


int ObDDLResolver::resolve_generated_column_expr(ObString &expr_str,
                                       ObIAllocator &allocator,
                                       ObTableSchema &table_schema,
                                       ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                       ObColumnSchemaV2 &column,
                                       ObSQLSessionInfo *session_info,
                                       ObSchemaChecker *schema_checker,
                                       ObRawExpr *&expr,
                                       ObRawExprFactory &expr_factory,
                                       bool coltype_not_defined)
{
  int ret = OB_SUCCESS;
  ObResolverParams params;
  params.expr_factory_ = &expr_factory;
  params.allocator_ = &allocator;
  params.session_info_ = session_info;
  params.schema_checker_ = schema_checker;
  if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(allocator,
                                          session_info->get_dtc_params(), expr_str))) {
    LOG_WARN("fail to convert for resolve", K(ret));
  } else if (OB_FAIL(ObResolverUtils::resolve_generated_column_expr(params,
                                                                    expr_str,
                                                                    table_schema,
                                                                    resolved_cols,
                                                                    column,
                                                                    expr,
                                                ObResolverUtils::CHECK_FOR_GENERATED_COLUMN,
                                                                    coltype_not_defined))) {
    LOG_WARN("resolve generated column expr failed", K(ret));
  }
  return ret;
}

// this function is called in ddl service, which can not access user session_info, so need to
// construct an empty session
int ObDDLResolver::check_default_value(ObObj &default_value,
                                       const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                       const common::ObString *nls_formats,
                                       const ObLocalSessionVar *local_session_var,
                                       ObIAllocator &allocator,
                                       ObTableSchema &table_schema,
                                       ObColumnSchemaV2 &column,
                                       ObIArray<ObString> &gen_col_expr_arr,
                                       const ObSQLMode sql_mode,
                                       bool allow_sequence,
                                       ObSchemaChecker *schema_checker,
                                       share::schema::ObColumnSchemaV2 *hidden_col)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaV2 *> dummy_array;
  SMART_VAR(ObSQLSessionInfo, empty_session) {
    if (OB_FAIL(init_empty_session(tz_info_wrap,
                                   nls_formats,
                                   local_session_var,
                                   allocator,
                                   table_schema,
                                   sql_mode,
                                   schema_checker,
                                   empty_session))) {
      LOG_WARN("failed to init empty session", K(ret));
    } else if (FALSE_IT(empty_session.set_stmt_type(stmt::T_CREATE_TABLE))) { // set a fake ddl stmt type to specifiy ddl stmt type
    } else if (OB_FAIL(check_default_value(default_value, tz_info_wrap, nls_formats, allocator,
                                          table_schema, dummy_array,column, gen_col_expr_arr, sql_mode,
                                          &empty_session, allow_sequence, schema_checker))) {
      LOG_WARN("check default value failed", K(ret));
    }
  }
  return ret;
}

// 解析生成列表达式时，首先在table_schema中的column_schema中寻找依赖的列，如果找不到，再在 resolved_cols中找
int ObDDLResolver::check_default_value(ObObj &default_value,
                                       const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                       const common::ObString *nls_formats,
                                       ObIAllocator &allocator,
                                       ObTableSchema &table_schema,
                                       ObIArray<ObColumnSchemaV2 *> &resolved_cols,
                                       ObColumnSchemaV2 &column,
                                       ObIArray<ObString> &gen_col_expr_arr,
                                       const ObSQLMode sql_mode,
                                       ObSQLSessionInfo *session_info,
                                       bool allow_sequence,
                                       ObSchemaChecker *schema_checker,
                                       bool coltype_not_defined)
{
  int ret = OB_SUCCESS;
  const ObObj input_default_value = default_value;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (column.is_identity_column()) {
    // identity column's default value is sequence_id, so it can't change
  } else if (column.is_generated_column()) {
    ObString expr_str;
    ObRawExpr *expr = NULL;
    ObRawExprFactory expr_factory(allocator);
    // sequence in definition of generated column not allowed.
    if (OB_FAIL(input_default_value.get_string(expr_str))) {
      LOG_WARN("get expr string from default value failed", K(ret), K(input_default_value));
    } else if (OB_FAIL(resolve_generated_column_expr(expr_str, allocator, table_schema, resolved_cols,
            column, session_info, schema_checker, expr, expr_factory, coltype_not_defined))) {
      LOG_WARN("resolve generated column expr failed", K(ret));
    } else {
      if (true == lib::is_oracle_mode()) {
        if (!table_schema.is_external_table()) {
          OZ (check_dup_gen_col(default_value.get_string(), gen_col_expr_arr));
          OZ (gen_col_expr_arr.push_back(default_value.get_string()));
        }
        if (OB_NOT_NULL(expr) && expr->is_udf_expr()) {
          OX (column.add_column_flag(GENERATED_COLUMN_UDF_EXPR));
        }
      }
      if (OB_SUCC(ret) && column.get_meta_type().is_null()) {
        //column type not defined, use the deduced type from generated column expr
        if (expr->get_result_type().is_null()) {
          ObAccuracy varchar_accuracy(0);
          column.set_data_type(ObVarcharType);
          column.set_collation_type(session_info->get_local_collation_connection());
          varchar_accuracy.length_semantics_ = session_info->get_actual_nls_length_semantics();
          column.set_accuracy(varchar_accuracy);
        } else {
          column.set_data_type(expr->get_data_type());
          column.set_collation_type(expr->get_collation_type());
          column.set_accuracy(expr->get_accuracy());
        }
        OZ (adjust_string_column_length_within_max(column, lib::is_oracle_mode()));
      }
    }
  } else if (column.is_default_expr_v2_column()) {
    ObString expr_str;
    ObResolverParams params;
    ObRawExpr *expr = NULL;
    ObRawExprFactory expr_factory(allocator);
    ParamStore empty_param_list( (ObWrapperAllocator(allocator)) );
    params.expr_factory_ = &expr_factory;
    params.allocator_ = &allocator;
    params.session_info_ = session_info;
    params.param_list_ = &empty_param_list;
    params.schema_checker_ = schema_checker;
    common::ObObj tmp_default_value;
    common::ObObj tmp_dest_obj;
    const ObObj *tmp_res_obj = NULL;
    common::ObObj tmp_dest_obj_null;
    const bool is_strict = true;//oracle mode

    ObObjType data_type = column.get_data_type();
    const ObAccuracy &accuracy = column.get_accuracy();
    ObCollationType collation_type = column.get_collation_type();
    if (lib::is_oracle_mode() && column.is_xmltype()) {
      // use hidden column type, treat as clob, wil transform to blob in _makexmlbinary
      data_type = ObLongTextType;
      collation_type = CS_TYPE_UTF8MB4_BIN;
    }
    const ObDataTypeCastParams dtc_params = session_info->get_dtc_params();
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, collation_type);
    ObAccuracy res_acc = accuracy;
    cast_ctx.res_accuracy_ = &res_acc;
    if (OB_FAIL(input_default_value.get_string(expr_str))) {
      LOG_WARN("get expr string from default value failed", K(ret), K(input_default_value));
    } else if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(allocator,
                                            dtc_params, expr_str))) {
      LOG_WARN("fail to convert for resolve", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_default_expr_v2_column_expr(params, expr_str, column, expr, allow_sequence))) {
      LOG_WARN("resolve expr_default expr failed", K(expr_str), K(column), K(ret));
    } else if (OB_FAIL(ObSQLUtils::calc_simple_expr_without_row(
        params.session_info_, expr, tmp_default_value, params.param_list_, allocator))) {
      LOG_WARN("Failed to get simple expr value", K(ret));
    } else if (column.is_xmltype()) {
      if (expr->get_result_type().is_string_type()) {
        data_type = expr->get_result_type().get_type();
        collation_type = CS_TYPE_UTF8MB4_BIN;
      } else {
        data_type = ObUserDefinedSQLType;
        tmp_dest_obj.set_type(data_type);
        tmp_dest_obj.set_subschema_id(ObXMLSqlType);
      }
    } else if (lib::is_oracle_mode() && column.is_xmltype() &&
               expr->get_result_type().is_xml_sql_type()) {
      data_type = ObUserDefinedSQLType;
    }

    if (OB_FAIL(ret)) {
    } else if (column.is_xmltype() && (ob_is_numeric_type(tmp_default_value.get_type()) || is_lob(tmp_default_value.get_type()))) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_WARN("incorrect cmp type with xml arguments",K(tmp_default_value.get_type()), K(ret));
    } else if (lib::is_oracle_mode() && column.get_meta_type().is_blob() && ob_is_numeric_type(tmp_default_value.get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", "expected", data_type, "got", tmp_default_value.get_type(), K(ret));
    } else if(OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, tmp_default_value, tmp_dest_obj, tmp_res_obj))) {
      LOG_WARN("cast obj failed, ", "src type", tmp_default_value.get_type(), "dest type", data_type, K(tmp_default_value), K(ret));
    } else if (OB_ISNULL(tmp_res_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast obj failed, ", "src type", tmp_default_value.get_type(), "dest type", data_type, K(tmp_default_value), K(ret));
    } else if (OB_FAIL(obj_collation_check(is_strict, collation_type,  *const_cast<ObObj*>(tmp_res_obj)))) {
      LOG_WARN("failed to check collation", K(ret), K(collation_type), K(tmp_dest_obj));
    } else if (OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, collation_type, *tmp_res_obj, tmp_dest_obj, tmp_res_obj))) {
      LOG_WARN("failed to check accuracy", K(ret), K(accuracy), K(collation_type), KPC(tmp_res_obj));
    } else if (0 == input_default_value.get_string().compare("''")) {
      //if default is '', we should store '' instead of NULL.
      //FIXME::when observer differentiate '' and null, we can delete this code @yanhua
      tmp_dest_obj_null.set_varchar(input_default_value.get_string());
      tmp_dest_obj_null.set_collation_type(ObCharset::get_system_collation());
      if (OB_FAIL(column.set_cur_default_value(tmp_dest_obj_null))) {
        LOG_WARN("set orig default value failed", K(ret));
      }
    } else if (OB_FAIL(print_expr_to_default_value(*expr, column, schema_checker, tz_info_wrap.get_time_zone_info()))) {
      LOG_WARN("fail to print_expr_to_default_value", KPC(expr), K(column), K(ret));
    }
    LOG_DEBUG("finish check default value", K(input_default_value), K(expr_str), K(tmp_default_value), K(tmp_dest_obj), K(tmp_dest_obj_null), KPC(expr), K(ret));
  } else {
    bool is_oracle_mode = false;
    if (OB_FAIL(cast_default_value(default_value, tz_info_wrap.get_time_zone_info(),
                                   nls_formats, allocator, column, sql_mode))) {
      LOG_WARN("fail to cast default value!", K(ret), K(default_value), KPC(tz_info_wrap.get_time_zone_info()), K(column), K(sql_mode));
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(table_schema.get_tenant_id(), is_oracle_mode))) {
      LOG_WARN("check if_oracle_compat_mode failed", KR(ret), K(table_schema.get_tenant_id()));
    } else if (OB_FAIL(check_default_value_length(!is_oracle_mode, column, default_value))) {
      LOG_WARN("fail to check default value length", K(ret), K(default_value), K(column));
    } else {
      default_value.set_collation_type(column.get_collation_type());
      LOG_DEBUG("succ to set default value", K(input_default_value), K(default_value), K(column), K(ret), K(lbt()));
    }
  }
  return ret;
}

int ObDDLResolver::check_default_value(ObObj &default_value,
                                       const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                       const common::ObString *nls_formats,
                                       const ObLocalSessionVar *local_session_var,
                                       ObIAllocator &allocator,
                                       ObTableSchema &table_schema,
                                       ObIArray<ObColumnSchemaV2> &resolved_cols,
                                       ObColumnSchemaV2 &column,
                                       ObIArray<ObString> &gen_col_expr_arr,
                                       const ObSQLMode sql_mode,
                                       ObSQLSessionInfo *session_info,
                                       bool allow_sequence,
                                       ObSchemaChecker *schema_checker,
                                       bool coltype_not_defined)
{
  int ret = OB_SUCCESS;
  ObArray<ObColumnSchemaV2 *> resolved_col_ptrs;
  for (int64_t i = 0; OB_SUCC(ret) && i < resolved_cols.count(); ++i) {
    OZ (resolved_col_ptrs.push_back(&resolved_cols.at(i)));
  }
  OZ (check_default_value(default_value, tz_info_wrap, nls_formats, allocator, table_schema,
                          resolved_col_ptrs, column, gen_col_expr_arr, sql_mode,
                          session_info, allow_sequence, schema_checker, coltype_not_defined));
  return ret;
}

// called on rs, will not used to calc udt defaults
int ObDDLResolver::calc_default_value(share::schema::ObColumnSchemaV2 &column,
                                      common::ObObj &default_value,
                                      const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                      const common::ObString *nls_formats,
                                      common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_DEFAULT_NOW_OBJ(default_value)) {
    int64_t cur_time = ObTimeUtility::current_time();
    ObTimeConverter::round_datetime(column.get_data_scale(), cur_time);
    switch (column.get_data_type()) {
      case ObDateTimeType: {
        int64_t dt_value = 0;
        if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(cur_time, tz_info_wrap.get_time_zone_info(), dt_value))) {
          LOG_WARN("failed to convert timestamp to datetime", K(ret));
        } else {
          ObTimeConverter::round_datetime(column.get_data_scale(), dt_value);
          default_value.set_datetime(dt_value);
        }
        break;
      }
      case ObTimestampType: {
        ObTimeConverter::round_datetime(column.get_data_scale(), cur_time);
        default_value.set_timestamp(cur_time);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("UnKnown type!", "default value type", column.get_data_type(), K(ret));
        break;
      }
    }
  } else if (column.is_default_expr_v2_column()) {
    ObString expr_str;
    ObResolverParams params;
    ObRawExpr *expr = NULL;
    ObRawExprFactory expr_factory(allocator);
    SMART_VAR(sql::ObSQLSessionInfo, empty_session) {
      uint64_t tenant_id = column.get_tenant_id();
      const ObTenantSchema *tenant_schema = NULL;
      ObSchemaGetterGuard guard;
      ObSessionDDLInfo ddl_info;
      ddl_info.set_ddl_check_default_value(true);
      ParamStore empty_param_list( (ObWrapperAllocator(allocator)) );
      params.expr_factory_ = &expr_factory;
      params.allocator_ = &allocator;
      params.session_info_ = &empty_session;
      params.param_list_ = &empty_param_list;
      if (OB_FAIL(empty_session.test_init(0, 0, 0, &allocator))) {
        LOG_WARN("init empty session failed", K(ret));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, guard))) {
        LOG_WARN("get schema guard failed", K(ret));
      } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("get tenant_schema failed", K(ret));
      } else if (OB_FAIL(empty_session.init_tenant(tenant_schema->get_tenant_name_str(), tenant_id))) {
        LOG_WARN("init tenant failed", K(ret));
      } else if (OB_FAIL(empty_session.load_all_sys_vars(guard))) {
        LOG_WARN("session load system variable failed", K(ret));
      } else if (OB_FAIL(empty_session.load_default_configs_in_pc())) {
        LOG_WARN("session load default configs failed", K(ret));
      } else if (OB_FAIL(empty_session.set_tz_info_wrap(tz_info_wrap))) {
        LOG_WARN("fail to set set_tz_info_wrap", K(ret));
      } else if (FALSE_IT(empty_session.set_nls_formats(nls_formats))) {
      } else if (FALSE_IT(empty_session.set_compatibility_mode(
          lib::is_oracle_mode() ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE))) {
      } else if (FALSE_IT(empty_session.set_sql_mode(
          lib::is_oracle_mode() ? DEFAULT_ORACLE_MODE : DEFAULT_MYSQL_MODE))) {
      } else if (FALSE_IT(empty_session.set_ddl_info(ddl_info))) {
        LOG_WARN("fail to set ddl_info", K(ret));
      } else if (OB_FAIL(default_value.get_string(expr_str))) {
        LOG_WARN("get expr string from default value failed", K(ret), K(default_value));
      } else if (OB_FAIL(ObResolverUtils::resolve_default_expr_v2_column_expr(params, expr_str,
                                            column, expr, false/* allow_sequence */))) {
        LOG_WARN("resolve expr_default expr failed", K(ret));
      } else if (OB_FAIL(ObSQLUtils::calc_simple_expr_without_row(
          params.session_info_, expr, default_value, params.param_list_, allocator))) {
        LOG_WARN("Failed to get simple expr value", K(ret));
      } else {
        ObObjType data_type = column.get_data_type();
        ObCollationType collation_type = column.get_collation_type();
        ObObj dest_obj;
        const ObDataTypeCastParams dtc_params(tz_info_wrap.get_time_zone_info(), nls_formats, CS_TYPE_INVALID, CS_TYPE_INVALID, CS_TYPE_UTF8MB4_GENERAL_CI);
        ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, collation_type);
        ObAccuracy res_acc = column.get_accuracy();
        cast_ctx.res_accuracy_ = &res_acc;
        if (OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, default_value, dest_obj))) {
          LOG_WARN("cast obj failed, ", "src type", default_value.get_type(), "dest type", data_type, K(default_value), K(ret));
        } else {
          // remove lob header for lob
          if (dest_obj.has_lob_header()) {
            ObString str;
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator, dest_obj, str))) {
              LOG_WARN("failed to read real data for lob obj", K(ret), K(dest_obj));
            } else {
              dest_obj.set_string(dest_obj.get_type(), str);
              dest_obj.set_inrow();
            }
          }
          if (OB_UNLIKELY(lib::is_oracle_mode()
                          && dest_obj.is_number_float()
                          && PRECISION_UNKNOWN_YET != column.get_data_precision())) {
            const int64_t number_precision =
              static_cast<int64_t>(floor(column.get_data_precision() * OB_PRECISION_BINARY_TO_DECIMAL_FACTOR));
            const number::ObNumber &nmb = dest_obj.get_number();
            number::ObNumber tmp_nmb;
            if (OB_FAIL(tmp_nmb.from(nmb, allocator))) {
              LOG_WARN("copy nmb failed", K(ret), K(nmb));
            } else if (OB_FAIL(tmp_nmb.round_precision(number_precision))) {
              LOG_WARN("round precision failed", K(ret), K(tmp_nmb), K(number_precision));
            } else {
              dest_obj.set_number(column.get_data_type(), tmp_nmb);
            }
          }
          dest_obj.set_scale(column.get_data_scale());
          default_value = dest_obj;
        }
      }
    }
    LOG_DEBUG("finish calc default value", K(column), K(expr_str), K(default_value), KPC(expr), K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it should not arrive here", K(ret), K(default_value), K(column), K(lbt()));
  }
  return ret;

}

// check default value for udt, do not call this function on rs
int ObDDLResolver::check_udt_default_value(ObObj &default_value,
                                           const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                           const common::ObString *nls_formats,
                                           ObIAllocator &allocator,
                                           ObTableSchema &table_schema,
                                           ObColumnSchemaV2 &column,
                                           const ObSQLMode sql_mode,
                                           ObSQLSessionInfo *session_info,
                                           ObSchemaChecker *schema_checker,
                                           ObDDLArg &ddl_arg)
{
  ObObj extend_result;
  return get_udt_column_default_values(default_value, tz_info_wrap, allocator,
                                       column, sql_mode, session_info, schema_checker,
                                       extend_result, ddl_arg);
}

int ObDDLResolver::ob_udt_check_and_add_ddl_dependency(const uint64_t schema_id,
                                                       const ObSchemaType schema_type,
                                                       const int64_t schema_version,
                                                       const uint64_t schema_tenant_id,
                                                       obrpc::ObDDLArg &ddl_arg)
{
  int ret = OB_SUCCESS;
  if (schema_id == OB_INVALID_ID) { // do nothing
  } else if (schema_version == OB_INVALID_VERSION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error default dependency item", K(ret), K(schema_id), K(schema_type), K(schema_version));
  } else {
    // check duplicate
    bool found_same_schema = false;
    for (int64_t i = 0;
        !found_same_schema && OB_SUCC(ret) && (i < ddl_arg.based_schema_object_infos_.count());
        i++) {
      const ObBasedSchemaObjectInfo &info = ddl_arg.based_schema_object_infos_.at(i);
      if (schema_id == info.schema_id_
          && schema_type == info.schema_type_
          && schema_tenant_id == info.schema_tenant_id_) {
        if (schema_version == info.schema_version_) {
          found_same_schema = true;
          // same schema do nothing
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error default dependency item with different schema",
                  K(ret), K(schema_id), K(schema_type), K(schema_tenant_id),
                  K(schema_version), K(info.schema_version_));
        }
      }
    }

    if (OB_SUCC(ret) && !found_same_schema) {
      if (OB_FAIL(ddl_arg.based_schema_object_infos_.push_back(
                  ObBasedSchemaObjectInfo(schema_id, schema_type, schema_version, schema_tenant_id)))) {
        LOG_WARN("fail to add udt default dependency",
                  K(ret), K(schema_id), K(schema_type), K(schema_version), K(schema_tenant_id));
      } else {
        LOG_DEBUG("succ to add udt default dependency",
                  K(schema_id), K(schema_type), K(schema_version), K(schema_tenant_id));
      }
    }
  }
  return ret;
}

int ObDDLResolver::add_udt_default_dependency(ObRawExpr *expr,
                                              ObSchemaChecker *schema_checker,
                                              obrpc::ObDDLArg &ddl_arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
  } else if (OB_ISNULL(schema_checker) || OB_ISNULL(schema_checker->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need schema checker to validate udt dependency", K(ret));
  } else {
    bool need_dependency = false;
    ObSchemaObjVersion obj_version;
    ObArray<ObSchemaObjVersion> obj_versions;

    if (expr->get_expr_type() == T_FUN_PL_OBJECT_CONSTRUCT) {
      ObObjectConstructRawExpr *obj_cons_expr = static_cast<ObObjectConstructRawExpr *>(expr);
      // refer to ObDMLResolver::resolve_external_name
      need_dependency = obj_cons_expr->need_add_dependency();
      if (need_dependency) {
        OZ(obj_cons_expr->get_schema_object_version(obj_version));
        OZ(obj_versions.push_back(obj_version));
      }
    } else if (expr->get_expr_type() == T_FUN_PL_COLLECTION_CONSTRUCT) {
      ObCollectionConstructRawExpr *obj_coll_expr = static_cast<ObCollectionConstructRawExpr *>(expr);
      need_dependency = obj_coll_expr->need_add_dependency();
      if (need_dependency) {
        OZ(obj_coll_expr->get_schema_object_version(obj_version));
        OZ(obj_versions.push_back(obj_version));
      }
    } else if (expr->is_udf_expr()) {
      ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr *>(expr);
      need_dependency = udf_expr->need_add_dependency();
      if (need_dependency) {
        OZ(udf_expr->get_schema_object_version(*schema_checker->get_schema_guard(), obj_versions));
      }
    }
    for (int64_t i = 0; need_dependency && OB_SUCC(ret) && i < obj_versions.count(); ++i) {
      obj_version = obj_versions.at(i);
      uint64_t object_id = obj_version.get_object_id();
      uint64_t tenant_id = pl::get_tenant_id_by_object_id(object_id);
      ObSchemaType schema_type = obj_version.get_schema_type();
      int64_t schema_version = obj_version.get_version();
      int64_t schema_check_version = OB_INVALID_VERSION;
      // local validate
      if (schema_type != UDT_SCHEMA
          && schema_type != PACKAGE_SCHEMA
          && schema_type != ROUTINE_SCHEMA) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error default dependency item", K(ret), K(object_id), K(schema_type), K(schema_version));
      } else if (OB_FAIL(schema_checker->get_schema_version(tenant_id,
                                                            object_id,
                                                            schema_type,
                                                            schema_check_version))) {
        LOG_WARN("failed to get_schema_version", K(ret), K(tenant_id), K(object_id), K(schema_type));
      } else if (OB_INVALID_VERSION == schema_check_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get_schema_version, schema may not exist",
                 K(ret), K(tenant_id), K(object_id), K(schema_type));
      } else if (schema_version != schema_check_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_version validation failed", K(ret),
        K(tenant_id), K(object_id), K(schema_type), K(schema_version), K(schema_check_version));
      } else if (OB_FAIL(ob_udt_check_and_add_ddl_dependency(object_id,
                                                             schema_type,
                                                             schema_version,
                                                             tenant_id,
                                                             ddl_arg))) {
        LOG_WARN("failed to add udt type ddl dependency",
                 K(ret), K(object_id), K(schema_type), K(schema_version), K(tenant_id));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(add_udt_default_dependency(expr->get_param_expr(i), schema_checker, ddl_arg))) {
        LOG_WARN("fail to add udt default dependency", K(ret), K(expr), K(ddl_arg), K(i));
      }
    }
  }

  return ret;
}

// check & calc udt default_value, do not call this function on RS
int ObDDLResolver::get_udt_column_default_values(const ObObj &default_value,
                                                 const common::ObTimeZoneInfoWrap &tz_info_wrap,
                                                 ObIAllocator &allocator,
                                                 ObColumnSchemaV2 &column,
                                                 const ObSQLMode sql_mode,
                                                 ObSQLSessionInfo *session_info,
                                                 ObSchemaChecker *schema_checker,
                                                 ObObj &extend_result,
                                                 obrpc::ObDDLArg &ddl_arg)
{
  int ret = OB_SUCCESS;
  const ObObj input_default_value = default_value;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (!(column.is_extend()) && !(lib::is_oracle_mode() && column.is_geometry())) {
    // do nothing
  } else if (column.is_identity_column() || column.is_generated_column()) {
    ret = OB_ERR_INVALID_VIRTUAL_COLUMN_TYPE;
    LOG_WARN("udt columns cannot be generated column or identity column",
             K(ret), K(column), K(default_value));
  } else if (default_value.is_null()) {
    // do nothing
  } else if (!column.is_default_expr_v2_column()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("udt columns is not default expr v2 column",
             K(ret), K(column), K(default_value));
  } else {
    extend_result.set_null();
    ObString expr_str;
    ObResolverParams params;
    ObRawExpr *expr = NULL;
    ObRawExprFactory expr_factory(allocator);
    ParamStore empty_param_list( (ObWrapperAllocator(allocator)) );
    params.expr_factory_ = &expr_factory;
    params.allocator_ = &allocator;
    params.session_info_ = session_info;
    params.param_list_ = &empty_param_list;
    params.schema_checker_ = schema_checker;
    common::ObObj tmp_default_value;
    common::ObObj tmp_dest_obj;
    const ObObj *tmp_res_obj = NULL;
    common::ObObj tmp_dest_obj_null;
    const bool is_strict = true;//oracle mode

    ObObjType data_type = column.get_data_type();
    const ObAccuracy &accuracy = column.get_accuracy();
    ObCollationType collation_type = column.get_collation_type();
    const ObDataTypeCastParams dtc_params = session_info->get_dtc_params();
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, collation_type);

    if (OB_FAIL(input_default_value.get_string(expr_str))) {
      LOG_WARN("get expr string from default value failed", K(ret), K(input_default_value));
    } else if (OB_FAIL(ObSQLUtils::convert_sql_text_from_schema_for_resolve(allocator,
                                                                            dtc_params,
                                                                            expr_str))) {
      LOG_WARN("fail to convert for resolve", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_default_expr_v2_column_expr(params,
                                                                            expr_str,
                                                                            column,
                                                                            expr,
                                                                            false))) {
      LOG_WARN("resolve expr_default expr failed", K(expr_str), K(column), K(ret));
    } else if (OB_FAIL(ObSQLUtils::calc_simple_expr_without_row(
        params.session_info_, expr, tmp_default_value, params.param_list_, allocator))) {
      LOG_WARN("Failed to get simple expr value", K(ret));
    } else if (OB_FAIL(add_udt_default_dependency(expr, schema_checker, ddl_arg))) {
      LOG_WARN("Failed to add udt default expr dependency", K(ret), K(expr));
    } else if (column.is_xmltype()) {
      if (expr->get_result_type().is_string_type()) {
        data_type = expr->get_result_type().get_type();
        collation_type = CS_TYPE_UTF8MB4_BIN;
      } else {
        data_type = ObUserDefinedSQLType;
        tmp_dest_obj.set_type(data_type);
        tmp_dest_obj.meta_.set_sql_udt(ObXMLSqlType);
      }
    } else { /* do nothing */ }

    if (OB_FAIL(ret)) {
    } else if (column.is_xmltype() && (ob_is_numeric_type(tmp_default_value.get_type()) || is_lob(tmp_default_value.get_type()))) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_WARN("incorrect cmp type with xml arguments",K(tmp_default_value.get_type()), K(ret));
    } else if (lib::is_oracle_mode()
               && ((column.get_meta_type().is_blob() && ob_is_numeric_type(tmp_default_value.get_type()))
                   || (column.get_meta_type().is_geometry() && !ob_is_extend(tmp_default_value.get_type()) &&  !expr->get_result_type().is_null()))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("inconsistent datatypes", "expected", data_type, "got", tmp_default_value.get_type(), K(ret));
    } else if(OB_FAIL(ObObjCaster::to_type(data_type, cast_ctx, tmp_default_value, tmp_dest_obj, tmp_res_obj))) {
      LOG_WARN("cast obj failed, ", "src type", tmp_default_value.get_type(), "dest type", data_type, K(tmp_default_value), K(ret));
    } else if (OB_ISNULL(tmp_res_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cast obj failed, ", "src type", tmp_default_value.get_type(), "dest type", data_type, K(tmp_default_value), K(ret));
    } else if (OB_FAIL(obj_collation_check(is_strict, collation_type,  *const_cast<ObObj*>(tmp_res_obj)))) {
      LOG_WARN("failed to check collation", K(ret), K(collation_type), K(tmp_dest_obj));
    } else if (OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, collation_type, *tmp_res_obj, tmp_dest_obj, tmp_res_obj))) {
      LOG_WARN("failed to check accuracy", K(ret), K(accuracy), K(collation_type), KPC(tmp_res_obj));
    } else if (0 == input_default_value.get_string().compare("''")) {
      tmp_dest_obj_null.set_varchar(input_default_value.get_string());
      tmp_dest_obj_null.set_collation_type(ObCharset::get_system_collation());
      if (OB_FAIL(column.set_cur_default_value(tmp_dest_obj_null))) {
        LOG_WARN("set orig default value failed", K(ret));
      }
    } else if (OB_FAIL(print_expr_to_default_value(*expr, column, schema_checker, tz_info_wrap.get_time_zone_info()))) {
      LOG_WARN("fail to print_expr_to_default_value", KPC(expr), K(column), K(ret));
    }
    if (OB_SUCC(ret)) {
      extend_result = tmp_dest_obj;
    }
    LOG_DEBUG("finish check udt default value", K(input_default_value), K(expr_str),
              K(tmp_default_value), K(tmp_dest_obj), K(tmp_dest_obj_null), KPC(expr), K(ret));
    if (OB_SUCC(ret) && tmp_default_value.is_pl_extend()) {
      OZ (pl::ObUserDefinedType::destruct_obj(tmp_default_value, session_info));
    }
  }
  return ret;
}

// the column length is determined by the expr result length in CTAS and GC, if the result length
// is longer than the max length of string type, need to adjust the column length to max length
int ObDDLResolver::adjust_string_column_length_within_max(share::schema::ObColumnSchemaV2 &column,
                                                          const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (!is_oracle_mode || ObStringTC != column.get_data_type_class()) {
    // do nothing
  } else if (ObCharType == column.get_data_type() || ObNCharType == column.get_data_type()) {
    const int64_t data_len = column.get_data_length();
    const int64_t max_char_length = is_oracle_mode ? OB_MAX_ORACLE_CHAR_LENGTH_BYTE
                                                     : OB_MAX_CHAR_LENGTH;
    if (data_len > max_char_length) {
      column.set_data_length(max_char_length);
    }
  } else if (ObVarcharType == column.get_data_type() || ObNVarchar2Type == column.get_data_type()) {
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(column.get_collation_type(), mbmaxlen))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "fail to get mbmaxlen", K(ret), K(column.get_collation_type()));
    } else if (0 == mbmaxlen) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(ERROR, "mbmaxlen can not be 0", K(ret));
    } else {
      const int64_t data_len = column.get_data_length();
      const int64_t max_varchar_length = is_oracle_mode ? OB_MAX_ORACLE_VARCHAR_LENGTH
                                                          : OB_MAX_VARCHAR_LENGTH / mbmaxlen;
      if (data_len > max_varchar_length) {
        column.set_data_length(max_varchar_length);
      }
    }
  }
  return ret;
}

// number and decimal column accuracy is determined by the expr result length in CTAS and GC, if the result is
// more accurate than the max accuracy of decimal, need to adjust it.
int ObDDLResolver::adjust_number_decimal_column_accuracy_within_max(share::schema::ObColumnSchemaV2 &column,
                                                                    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (is_oracle_mode || !ob_is_number_or_decimal_int_tc(column.get_data_type())) {
    // do nothing
  } else {
    int16_t ori_scale = column.get_data_scale();
    column.set_data_scale(MIN(OB_MAX_DECIMAL_SCALE, ori_scale));
    int16_t data_precision = column.get_data_precision() - (ori_scale - column.get_data_scale());
    column.set_data_precision(MIN(OB_MAX_DECIMAL_PRECISION, data_precision));
  }
  return ret;
}

int ObDDLResolver::resolve_range_partition_elements(ParseNode *node,
                                                    const bool is_subpartition,
                                                    const ObPartitionFuncType part_type,
                                                    const int64_t expr_num,
                                                    ObIArray<ObRawExpr *> &range_value_exprs,
                                                    ObIArray<ObPartition> &partitions,
                                                    ObIArray<ObSubPartition> &subpartitions,
                                                    const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), KP(stmt_));
  } else if (expr_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr num is invalid", K(ret), K(expr_num));
  } else {
    int64_t partition_num = node->num_child_;
    ParseNode *partition_expr_list = node;
    ObPartition partition;
    ObSubPartition subpartition;
    const ObCreateTablegroupStmt *tablegroup_stmt = static_cast<ObCreateTablegroupStmt *>(stmt_);
    bool has_empty_name = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      subpartition.reset();
      partition.reset();
      ParseNode *element_node = partition_expr_list->children_[i];
      if (OB_ISNULL(element_node)
          || OB_ISNULL(element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else {
        ObString partition_name;
        ObBasePartition *target_partition = is_subpartition ?
            static_cast<ObBasePartition*>(&subpartition) : static_cast<ObBasePartition*>(&partition);
        if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                           partition_name,
                                           *target_partition))) {
          LOG_WARN("failed to resolve partition name", K(ret));
        } else if (target_partition->is_empty_partition_name()) {
          has_empty_name = true;
        }
        ParseNode *expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE];
        if (T_EXPR_LIST != expr_list_node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr_list_node->type_ is not T_EXPR_LIST", K(ret));
        } else if (expr_num != expr_list_node->num_child_) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret), K(expr_num), "num_child", expr_list_node->num_child_);
        } else {
          if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
            // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
            LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
          }
          if (is_subpartition) {
            if (OB_FAIL(subpartitions.push_back(subpartition))) {
              LOG_WARN("fail to push back subpartition", KR(ret), K(subpartition));
            }
          } else {
            if (OB_FAIL(partitions.push_back(partition))) {
              LOG_WARN("fail to push back partition", KR(ret), K(partition));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(resolve_range_value_exprs(expr_list_node,
                                                       part_type,
                                                       partition_name,
                                                       range_value_exprs,
                                                       in_tablegroup))) {
            LOG_WARN("fail to resolve range partition element", K(ret));
          }
        }
      }
    }
    if (OB_UNLIKELY(has_empty_name) &&
        (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() ||
         stmt::T_CREATE_TABLEGROUP == stmt_->get_stmt_type() ||
         stmt::T_CREATE_INDEX == stmt_->get_stmt_type())) {
      if (OB_FAIL(create_name_for_empty_partition(is_subpartition, partitions, subpartitions))) {
        LOG_WARN("failed to create name for empty [sub]partitions", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_range_value_exprs(ParseNode *expr_list_node,
                                             const ObPartitionFuncType part_type,
                                             const ObString &partition_name,
                                             ObIArray<ObRawExpr *> &range_value_exprs,
                                             const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_list_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(expr_list_node));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < expr_list_node->num_child_; j++) {
      if (OB_ISNULL(expr_list_node->children_[j])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if (T_MAXVALUE == expr_list_node->children_[j]->type_) {
        ObRawExpr *maxvalue_expr = NULL;
        ObConstRawExpr *c_expr = NULL;
        c_expr = (ObConstRawExpr *)allocator_->alloc(sizeof(ObConstRawExpr));
        if (NULL != c_expr) {
          c_expr = new (c_expr) ObConstRawExpr();
          maxvalue_expr = c_expr;
          maxvalue_expr->set_data_type(common::ObMaxType);
          if (OB_FAIL(range_value_exprs.push_back(maxvalue_expr))) {
            LOG_WARN("array push back fail", K(ret));
          }
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      } else if (T_NULL == expr_list_node->children_[j]->type_) {
        ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
        LOG_WARN("null value is not allowed in less than", K(ret));
      } else if (T_EXPR_LIST != expr_list_node->children_[j]->type_) {
        ObRawExpr *part_value_expr = NULL;
        if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(
                                                       params_,
                                                       *(expr_list_node->children_[j]),
                                                       partition_name,
                                                       part_type,
                                                       part_value_expr,
                                                       in_tablegroup))) {
          LOG_WARN("resolve partition expr failed", K(ret));
        } else if (OB_FAIL(range_value_exprs.push_back(part_value_expr))) {
          LOG_WARN("array push back fail", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr_node type is error", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_spatial_index_constraint(
    const share::schema::ObTableSchema &table_schema,
    const common::ObString &column_name,
    int64_t column_num,
    const int64_t index_keyname_value,
    bool is_explicit_order,
    bool is_func_index,
    ObIArray<share::schema::ObColumnSchemaV2*> *resolved_cols)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  bool is_oracle_mode = false;
  uint64_t tenant_data_version = 0;
  uint64_t tenant_id = 0;

  if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info_), K(allocator_));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
    LOG_WARN("check oracle compat mode failed", K(ret));
  } else if (is_func_index && is_mysql_mode()) {
    ObRawExprFactory expr_factory(*allocator_);
    ObRawExpr *expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(NULL,
                                                            column_name,
                                                            expr_factory,
                                                            *session_info_,
                                                            table_schema,
                                                            expr,
                                                            schema_checker_,
                                                            ObResolverUtils::CHECK_FOR_FUNCTION_INDEX,
                                                            resolved_cols))) {
      LOG_WARN("build generated column expr failed", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to build generated column expr", K(ret));
    } else if (expr->is_column_ref_expr()) {
      ret = OB_ERR_FUNCTIONAL_INDEX_ON_FIELD;
      LOG_WARN("Functional index on a column is not supported.", K(ret), K(column_name));
    } else if (index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::SPATIAL_KEY)) {
      ret = OB_ERR_SPATIAL_FUNCTIONAL_INDEX;
      LOG_WARN("Spatial functional index is not supported.", K(ret), K(column_name));
    } else {
      //do nothing, check result type of expr on rootserver later
    }
  } else {
    // if create idx by alter table, resolved_cols is not null
    // if current col in resolved_cols, means it has been altered, use col schema in alter table schema
    if (OB_NOT_NULL(resolved_cols) && resolved_cols->count() > 0) {
      bool found = false;
      for (int i = 0; i < resolved_cols->count() && !found && OB_SUCC(ret); ++i) {
        ObColumnSchemaV2* tmp_col_schema = resolved_cols->at(i);
        if (OB_ISNULL(tmp_col_schema)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("should not be null.", K(i), K(resolved_cols->count()), K(ret));
        } else {
          ObCompareNameWithTenantID column_name_cmp(table_schema.get_tenant_id());
          if (0 == column_name_cmp.compare(column_name, tmp_col_schema->get_column_name_str())) {
            found = true;
            column_schema = tmp_col_schema;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(column_schema) && OB_FALSE_IT(column_schema = table_schema.get_column_schema(column_name))) {
    } else if (OB_ISNULL(column_schema)) {
      if (index_keyname_value != static_cast<int64_t>(INDEX_KEYNAME::SPATIAL_KEY)) {
        // do nothing
      } else {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      }
    } else if (OB_FAIL(resolve_spatial_index_constraint(*column_schema, column_num,
        index_keyname_value, is_oracle_mode, is_explicit_order))) {
      LOG_WARN("resolve spatial index constraint fail", K(ret), K(column_num), K(index_keyname_value));
    }
  }

  return ret;
}

// 1. A spatial index can only be built on a single spatial column.
//    CREATE TABLE spatial_index_constraint (g1 GEOMETRY NOT NULL, g2 GEOMETRY NOT NULL);
//    CREATE INDEX idx ON spatial_index_constraint (g1, g2); -->illegal
// 2. Spatial index can only be built on spatial columns.
//    CREATE TABLE spatial_index_constraint (i int, g GEOMETRY NOT NULL);
//    CREATE SPATIAL INDEX idx ON spatial_index_constraint (i); -->illegal
// 3. Spatial column can only be indexed spatial index, can't build other index.
//    CREATE TABLE spatial_index_constraint (i int, g GEOMETRY NOT NULL);
//    CREATE UNIQUE INDEX idx ON spatial_index_constraint (g); -->illegal
// 4. Column of a SPATIAL index must be NOT NULL.
//    DROP TABLE IF EXISTS spatial_index_constraint;
//    CREATE TABLE spatial_index_constraint (i int, g GEOMETRY);
//    CREATE SPATIAL INDEX idx ON spatial_index_constraint (g); -->illegal
//    CREATE INDEX idx ON spatial_index_constraint (g); -->illegal
// 5. Column of a SPATIAL index must not be generated column;
// 6. Cannot specify spatial index order.
int ObDDLResolver::resolve_spatial_index_constraint(
    const share::schema::ObColumnSchemaV2 &column_schema,
    int64_t column_num,
    const int64_t index_keyname_value,
    bool is_oracle_mode,
    bool is_explicit_order)
{
  int ret = OB_SUCCESS;
  bool is_spatial_index = index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::SPATIAL_KEY);
  bool is_default_index = index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::NORMAL_KEY);
  uint64_t tenant_id = column_schema.get_tenant_id();
  bool is_geo_column = ob_is_geometry_tc(column_schema.get_data_type());
  uint64_t tenant_data_version = 0;

  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (is_oracle_mode && tenant_data_version < DATA_VERSION_4_3_2_0) {
    if (is_geo_column) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("oracle spatial index not supported", K(ret), K(is_geo_column), K(is_spatial_index));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "oracle spatial index");
    } else {
      // do nothing
    }
  } else if ((is_geo_column || is_spatial_index) && tenant_data_version < DATA_VERSION_4_1_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant version is less than 4.1, spatial index not supported", K(ret), K(is_geo_column), K(is_spatial_index));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant version is less than 4.1, spatial index");
  } else {
    if (is_spatial_index) { // has 'SPATIAL' keyword
      if (is_geo_column) {
        index_keyname_ = SPATIAL_KEY;
      } else {
        ret = OB_ERR_SPATIAL_MUST_HAVE_GEOM_COL;
        LOG_USER_ERROR(OB_ERR_SPATIAL_MUST_HAVE_GEOM_COL);
        LOG_WARN("spatial index can only be built on spatial column", K(ret), K(column_schema));
      }
    } else if (is_default_index && lib::is_mysql_mode()) { // there are no keyword, not allowed in oracle mode
      if (is_geo_column) {
        index_keyname_ = SPATIAL_KEY;
      } else {
        // other index, do nothing
      }
    } else { // other index type (UNIQUE_KEY, FULLTEXT)
      if (is_geo_column) {
        if (lib::is_mysql_mode()) {
          ret = OB_ERR_SPATIAL_UNIQUE_INDEX;
          LOG_USER_ERROR(OB_ERR_SPATIAL_UNIQUE_INDEX);
          LOG_WARN("spatial column can only be indexed spatial index, can't build other index.",
              K(ret), K(column_schema));
        } else {
          ret = OB_ERR_XML_INDEX;
          LOG_USER_ERROR(OB_ERR_XML_INDEX, column_schema.get_column_name_str().length(), column_schema.get_column_name_str().ptr());
          LOG_WARN("cannot create index on expression with datatype ADT.",
              K(ret), K(column_schema));
        }
      } else {
        // do nothing
      }
    }
  }

  if (OB_SUCC(ret) && index_keyname_ == SPATIAL_KEY) {
    if (column_num != 1) { // spatial only can be built in one column
      ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
      LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    } else if (column_schema.is_generated_column()) {
      ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
      LOG_USER_ERROR(OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, column_schema.get_column_name());
    } else if (is_explicit_order) {
      ret = OB_ERR_INDEX_ORDER_WRONG_USAGE;
      LOG_USER_ERROR(OB_ERR_INDEX_ORDER_WRONG_USAGE);
    } else if (lib::is_mysql_mode() && column_schema.is_nullable()) {
      ret = OB_ERR_SPATIAL_CANT_HAVE_NULL;
      LOG_USER_ERROR(OB_ERR_SPATIAL_CANT_HAVE_NULL);
      LOG_WARN("column of a spatial index must be NOT NULL.", K(ret), K(column_schema));
    }
  }

  return ret;
}

int ObDDLResolver::resolve_fts_index_constraint(
    const share::schema::ObTableSchema &table_schema,
    const common::ObString &column_name,
    const int64_t index_keyname_value)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  if (!table_schema.is_valid() || column_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(table_schema), K(column_name));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info_), K(allocator_));
  } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_name))) {
    ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
    LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                   column_name.length(),
                   column_name.ptr());
  } else if (OB_FAIL(resolve_fts_index_constraint(*column_schema,
                                                  index_keyname_value))) {
    LOG_WARN("resolve fts index constraint fail", K(ret), K(index_keyname_value));
  }
  return ret;
}

// Fts index can only be built on text columns.
// CREATE TABLE fts_index_constraint (id int,
//                                    title varchar(100),
//                                    content text,
//                                    FULLTEXT(title, content));
int ObDDLResolver::resolve_fts_index_constraint(
                   const share::schema::ObColumnSchemaV2 &column_schema,
                   const int64_t index_keyname_value)
{
  int ret = OB_SUCCESS;
  if (!column_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(column_schema));
  } else {
    bool is_fts_index =
         (index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::FTS_KEY));
    uint64_t tenant_id = column_schema.get_tenant_id();
    bool is_text_column = ob_is_string_tc(column_schema.get_data_type()) ||
                          ob_is_text_tc(column_schema.get_data_type());
    uint64_t tenant_data_version = 0;
    if (!is_fts_index) {
      // do nothing
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_3_1_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is less than 4.3.1, fulltext index not supported", K(ret), K(tenant_data_version));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3.1, fulltext index");
    } else if (!is_text_column) {
      ret = OB_ERR_FTS_MUST_HAVE_TEXT_COL;
      LOG_USER_ERROR(OB_ERR_FTS_MUST_HAVE_TEXT_COL);
      LOG_WARN("fts index can only be built on text column", K(ret), K(column_schema));
    } else {
      index_keyname_ = FTS_KEY;
    }
  }
  return ret;
}

int ObDDLResolver::resolve_multivalue_index_constraint(
    const share::schema::ObTableSchema &table_schema,
    const common::ObString &column_name,
    const int64_t index_keyname_value)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(table_schema));
  } else if (OB_ISNULL(session_info_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info_), K(allocator_));
  } else if (OB_ISNULL(column_schema = table_schema.get_column_schema(column_name))) {
    ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
    LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
  } else if (OB_FAIL(resolve_multivalue_index_constraint(*column_schema,
          index_keyname_value))) {
    LOG_WARN("resolve multivalue index constraint fail", K(ret), K(index_keyname_value));
  }
  return ret;
}

// multi value index can only be built on json columns.
// CREATE TABLE multivalue_index_constraint (id int,
//                                    title varchar(100),
//                                    content json,
//                                    index mvi ((cast(content as unsigned array))));
int ObDDLResolver::resolve_multivalue_index_constraint(
                   const share::schema::ObColumnSchemaV2 &column_schema,
                   const int64_t index_keyname_value)
{
  int ret = OB_SUCCESS;
  if (!column_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", K(ret), K(column_schema));
  } else {
    bool is_multival_index = (index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::MULTI_KEY)
                         || index_keyname_value == static_cast<int64_t>(INDEX_KEYNAME::MULTI_UNIQUE_KEY));
    uint64_t tenant_id = column_schema.get_tenant_id();
    uint64_t tenant_data_version = 0;
    if (!is_multival_index) {
      // do nothing
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_3_1_0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("tenant data version is less than 4.3.1, multivalue index not supported", K(ret), K(tenant_data_version));
    } else {
      index_keyname_ = static_cast<INDEX_KEYNAME>(index_keyname_value);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_list_partition_elements(ParseNode *node,
                                                   const bool is_subpartition,
                                                   const ObPartitionFuncType part_type,
                                                   int64_t &expr_num,
                                                   ObDDLStmt::array_t &list_value_exprs,
                                                   ObIArray<ObPartition> &partitions,
                                                   ObIArray<ObSubPartition> &subpartitions,
                                                   const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt_));
  } else {
    int64_t partition_num = node->num_child_;
    ParseNode *partition_expr_list = node;
    ObPartition partition;
    ObSubPartition subpartition;
    int64_t first_non_default_value_idx = OB_INVALID_INDEX;
    const sql::ObCreateTablegroupStmt *tablegroup_stmt = static_cast<ObCreateTablegroupStmt*>(stmt_);
    bool has_empty_name = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      subpartition.reset();
      partition.reset();
      ParseNode *element_node = partition_expr_list->children_[i];
      if (OB_ISNULL(element_node)
          || OB_ISNULL(element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition expr list node is null", K(ret), K(element_node));
      } else if (element_node->type_ != T_PARTITION_LIST_ELEMENT) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("not a valid list partition define", K(element_node->type_));
      } else if (is_subpartition
                 && OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        ret = OB_ERR_PARSE_SQL;
        LOG_WARN("subpartition can not specify part id", K(ret), K(i), K(is_subpartition), K(in_tablegroup));
      } else {
        ObString partition_name;
        ObBasePartition *target_partition = is_subpartition ?
            static_cast<ObBasePartition*>(&subpartition) : static_cast<ObBasePartition*>(&partition);
        if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                           partition_name,
                                           *target_partition))) {
          LOG_WARN("failed to resolve partition name", K(ret));
        } else if (target_partition->is_empty_partition_name()) {
          has_empty_name = true;
        }
        ParseNode *expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE];
        if (T_EXPR_LIST != expr_list_node->type_ && T_DEFAULT != expr_list_node->type_) { //也有可能等于default
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr_list_node->type_ is not T_EXPR_LIST or T_DEFAULT", K(ret));
        }
        //add list partition elements to tablegroup schema
        if (OB_SUCC(ret)) {
          if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
            // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
            LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
          }
          if (is_subpartition) {
            if (OB_FAIL(subpartitions.push_back(subpartition))) {
              LOG_WARN("fail to push back subpartition", KR(ret), K(subpartition));
            }
          } else {
            if (OB_FAIL(partitions.push_back(partition))) {
              LOG_WARN("fail to push back partition", KR(ret), K(partition));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObOpRawExpr *row_expr = NULL;
          if (OB_ISNULL(params_.expr_factory_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
            LOG_WARN("failed to create raw expr", K(ret));
          } else if (OB_ISNULL(row_expr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allcoate memory", K(ret));
          } else if (T_DEFAULT == expr_list_node->type_) {
            //这里使用max来代替default值
            ObRawExpr *maxvalue_expr = NULL;
            ObConstRawExpr *c_expr = NULL;
            c_expr = (ObConstRawExpr *) allocator_->alloc(sizeof(ObConstRawExpr));
            if (OB_ISNULL(c_expr)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allcoate memory", K(ret));
            } else {
              c_expr = new(c_expr) ObConstRawExpr();
              maxvalue_expr = c_expr;
              maxvalue_expr->set_data_type(common::ObMaxType);
              if (OB_FAIL(row_expr->add_param_expr(maxvalue_expr))) {
                LOG_WARN("failed add param expr", K(ret));
              } else if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
                LOG_WARN("array push back fail", K(ret));
              }
            }
          } else {
            ObSEArray<ObRawExpr *, 16> part_value_exprs;

            bool is_all_expr_list = false;
            expr_num = OB_INVALID_COUNT;
            if (expr_list_node->num_child_ > 0) {
              is_all_expr_list = (expr_list_node->children_[0]->type_ == T_EXPR_LIST);
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < expr_list_node->num_child_; j++) {
              part_value_exprs.reset();
              if (OB_ISNULL(expr_list_node->children_[j])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("node is null", K(ret));
              } else if ((is_all_expr_list && expr_list_node->children_[j]->type_ != T_EXPR_LIST) ||
                         (!is_all_expr_list && expr_list_node->children_[j]->type_ == T_EXPR_LIST)) {
                ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
                LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
              } else if (OB_FAIL(ObResolverUtils::resolve_partition_list_value_expr(params_,
                                                                                    *(expr_list_node->children_[j]),
                                                                                    partition_name,
                                                                                    part_type,
                                                                                    expr_num,
                                                                                    part_value_exprs,
                                                                                    in_tablegroup))) {
                LOG_WARN("resolve partition expr failed", K(ret));
              }
              for (int64_t k = 0; OB_SUCC(ret) && k < part_value_exprs.count(); k ++) {
                int64_t idx = row_expr->get_param_count() % expr_num; //列向量中第idx列
                ObObjType cur_data_type = part_value_exprs.at(k)->get_data_type();
                ObObjType pre_data_type = cur_data_type;
                if (first_non_default_value_idx >= 0) {
                  if (list_value_exprs.at(first_non_default_value_idx)->get_param_count() <= idx) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("value expr num in row is invalid", K(ret),
                             K(first_non_default_value_idx), K(idx),
                             K(list_value_exprs.at(first_non_default_value_idx)), KPC(part_value_exprs.at(k)));
                  } else {
                    pre_data_type = list_value_exprs.at(first_non_default_value_idx)->get_param_expr(idx)->get_data_type();
                  }
                } else if (row_expr->get_param_count() > idx) {
                  pre_data_type = row_expr->get_param_expr(idx)->get_data_type();
                }
                if (OB_FAIL(ret)) {
                  // do nothing
                } else if (ObMaxType != cur_data_type
                    && ObMaxType != pre_data_type
                    && cur_data_type != pre_data_type) {
                  // 对于tablegroup分区语法而言，由于缺乏column type信息，需要校验同列的value的类型是否一致
                  ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
                  LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
                  LOG_WARN("object type is invalid ", K(ret), K(cur_data_type), K(pre_data_type));
                } else if (OB_FAIL(row_expr->add_param_expr(part_value_exprs.at(k)))) {
                  LOG_WARN("array push back fail", K(ret));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (expr_num > 1 && !is_all_expr_list) {
                if (row_expr->get_param_count() != expr_num) {
                  ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
                  LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret), K(expr_num));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
                LOG_WARN("array push back fail", K(ret));
              } else if (first_non_default_value_idx < 0) {
                first_non_default_value_idx = i;
              }
            }
          }
        }
      }
    }
    if (OB_UNLIKELY(has_empty_name) &&
        (stmt::T_CREATE_TABLE == stmt_->get_stmt_type() ||
         stmt::T_CREATE_TABLEGROUP == stmt_->get_stmt_type() ||
         stmt::T_CREATE_INDEX == stmt_->get_stmt_type())) {
      if (OB_FAIL(create_name_for_empty_partition(is_subpartition, partitions, subpartitions))) {
        LOG_WARN("failed to create name for empty [sub]partitions", K(ret));
      }
    }
  }
  return ret;
}

// 主要用于在 alter table 时检查需要修改的列是否为外键列
// 如果该列属于外键列，不允许修改外键列的类型
int ObDDLResolver::check_column_in_foreign_key(const ObTableSchema &table_schema,
                                               const ObString &column_name,
                                               const bool is_drop_column)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_parent_table() || table_schema.is_child_table()) {
    const ObColumnSchemaV2 *alter_column = table_schema.get_column_schema(column_name);
    if (OB_ISNULL(alter_column)) {
      // do nothing
      // 根据列名查不到列是因为表中不存在该列，后面会在 RS 端再检查一遍表中是否存在该列，并在 RS 端根据操作的不同报不同的错误
    } else {
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
        if (table_schema.get_table_id() == foreign_key_info.parent_table_id_) {
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.parent_column_ids_.count(); j++) {
            if (alter_column->get_column_id() == foreign_key_info.parent_column_ids_.at(j)) {
              ret = OB_ERR_ALTER_COLUMN_FK;
              LOG_USER_ERROR(OB_ERR_ALTER_COLUMN_FK, column_name.length(), column_name.ptr());
            }
          }
        }
        if (table_schema.get_table_id() == foreign_key_info.child_table_id_) {
          if (is_drop_column) {
            // To be compatible with Mysql 5.6 and 8.0, follwing behavior on child table are allowed on OB 4.0:
            // 1. drop foreign key non-related columns and drop any foreign key in one sql;
            // 2. drop the foreign key and its' some/all related columns in one sql.
            // Thus, RS should report OB_ERR_ALTER_COLUMN_FK if drop fk related column without drop this fk.
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
              if (alter_column->get_column_id() == foreign_key_info.child_column_ids_.at(j)) {
                ret = OB_ERR_ALTER_COLUMN_FK;
                LOG_USER_ERROR(OB_ERR_ALTER_COLUMN_FK, column_name.length(), column_name.ptr());
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_column_in_check_constraint(
    const share::schema::ObTableSchema &table_schema,
    const ObString &column_name,
    ObAlterTableStmt *alter_table_stmt)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *alter_column = table_schema.get_column_schema(column_name);
  int64_t cst_cnt = table_schema.get_constraint_count();
  if (OB_ISNULL(alter_column)) {
    // do nothing
    // 根据列名查不到列是因为表中不存在该列，后面会在 RS 端再检查一遍表中是否存在该列，并在 RS 端根据操作的不同报不同的错误
  } else {
    AlterTableSchema& alter_table_schema = alter_table_stmt->get_alter_table_arg().alter_table_schema_;
    for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
         OB_SUCC(ret) && (iter != table_schema.constraint_end());
         ++iter) {
      if (CONSTRAINT_TYPE_CHECK == (*iter)->get_constraint_type()
          || CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
        for (ObConstraint::const_cst_col_iterator cst_col_iter = (*iter)->cst_col_begin();
             OB_SUCC(ret) && (cst_col_iter != (*iter)->cst_col_end());
             ++cst_col_iter) {
          if (*cst_col_iter == alter_column->get_column_id()) {
            if (0 == (*iter)->get_column_cnt()) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "check/not null cst don't have column info", K(ret), K(**iter));
            } else {
              // drop check constraint cascaded
              bool is_dropped = false;
              // check if constraints have been dropped
              ObTableSchema::const_constraint_iterator iter_dropped = alter_table_schema.constraint_begin();
              for (int64_t i = 0; i < cst_cnt && (iter_dropped != alter_table_schema.constraint_end()); ++i) {
                if ((*iter)->get_constraint_id() == (*iter_dropped)->get_constraint_id()) {
                  is_dropped = true;
                  break;
                }
              }
              if (is_dropped) {
                // skip this constraint
              } else if (1 == (*iter)->get_column_cnt()) {
                if (OB_FAIL(alter_table_schema.add_constraint(**iter))) {
                  SQL_RESV_LOG(WARN, "add constraint failed!", K(ret), K(**iter));
                } else {
                  alter_table_stmt->get_alter_table_arg().alter_constraint_type_ = ObAlterTableArg::DROP_CONSTRAINT;
                }
              } else {/* do nothing. */}
            }
          }
        }
      }
    }
  }

  return ret;
}

// 当外键中的外键列仅有一列时，oracle 模式允许通过 alter table 删除子表中的外键列
int ObDDLResolver::check_column_in_foreign_key_for_oracle(
    const ObTableSchema &table_schema,
    const ObString &column_name,
    ObAlterTableStmt *alter_table_stmt)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_parent_table() || table_schema.is_child_table()) {
    const ObColumnSchemaV2 *alter_column = table_schema.get_column_schema(column_name);
    if (OB_ISNULL(alter_column)) {
      // do nothing
      // 根据列名查不到列是因为表中不存在该列，后面会在 RS 端再检查一遍表中是否存在该列，并在 RS 端根据操作的不同报不同的错误
    } else {
      const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
        const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
        // 父表的列不能被删
        if (OB_SUCC(ret)
            && table_schema.get_table_id() == foreign_key_info.parent_table_id_) {
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.parent_column_ids_.count(); j++) {
            if (alter_column->get_column_id() == foreign_key_info.parent_column_ids_.at(j)) {
              ret = OB_ERR_DROP_PARENT_KEY_COLUMN;
            }
          }
        }
        // 当删除的外键列上的外键存在多列外键时，不允许通过 alter table 删除子表中的外键列
        if (OB_SUCC(ret)
            && table_schema.get_table_id() == foreign_key_info.child_table_id_
            && foreign_key_info.child_column_ids_.count() > 1) {
          for (int64_t j = 0; OB_SUCC(ret) && j < foreign_key_info.child_column_ids_.count(); j++) {
            if (alter_column->get_column_id() == foreign_key_info.child_column_ids_.at(j)) {
              ret = OB_ERR_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT;
            }
          }
        }
        // 当删除的外键列上的外键均为单列外键时，oracle 模式允许通过 alter table 删除子表中的外键列
        // 删除外键列的同时会自动删除外键列上的所有单列外键
        if (OB_SUCC(ret)
            && table_schema.get_table_id() == foreign_key_info.child_table_id_
            && foreign_key_info.child_column_ids_.count() == 1) {
          if (alter_column->get_column_id() == foreign_key_info.child_column_ids_.at(0)) {
            ObDropForeignKeyArg *foreign_key_arg = NULL;
            void *tmp_ptr = NULL;
            bool has_same_fk_arg = false;
            if (OB_ISNULL(tmp_ptr = allocator_->alloc(sizeof(ObDropForeignKeyArg)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SQL_RESV_LOG(ERROR, "failed to allocate memory", K(ret));
            } else if (FALSE_IT(foreign_key_arg = new (tmp_ptr)ObDropForeignKeyArg())) {
            } else if (OB_FAIL(deep_copy_str(foreign_key_info.foreign_key_name_,
                                             foreign_key_arg->foreign_key_name_))) {
              LOG_WARN("failed to deep copy foreign_key_name", K(ret), K(foreign_key_info));
            } else if (OB_ISNULL(alter_table_stmt)) {
              ret = OB_ERR_UNEXPECTED;
              SQL_RESV_LOG(WARN, "alter table stmt should not be null", K(ret));
            } else if (OB_FAIL(alter_table_stmt->check_drop_fk_arg_exist(foreign_key_arg, has_same_fk_arg))) {
              SQL_RESV_LOG(WARN, "check_drop_fk_arg_exist failed", K(ret), K(foreign_key_arg));
            } else if (has_same_fk_arg) {
              // do nothing
            } else if (OB_FAIL(alter_table_stmt->add_index_arg(foreign_key_arg))) {
              SQL_RESV_LOG(WARN, "add index to drop_index_list failed!", K(ret));
            } else {
              alter_table_stmt->set_alter_table_index();
            }
          }
        }
      }
    }
  }
  return ret;
}

/* 功能：检查 src_list 是否和 dest_list 是否完全匹配
   参数：
     in: src_list
     in: dest_list
   返回值：
     如果 src_list 和 dest_list 完全匹配，则 is_match 返回 true，否则返回 false
     如果 src_list 和 dest_list 都为空，则返回 true
*/
bool ObDDLResolver::is_ids_match(const ObIArray<uint64_t> &src_list, const ObIArray<uint64_t> &dest_list)
{
  bool is_match = true;
  ObSEArray<uint64_t, 8> tmp_src_list;
  ObSEArray<uint64_t, 8> tmp_dest_list;
  if (src_list.count() != dest_list.count()) {
    is_match = false;
  } else {
    for(int64_t i = 0; i < src_list.count(); ++i) {
      tmp_src_list.push_back(src_list.at(i));
      tmp_dest_list.push_back(dest_list.at(i));
    }
    lib::ob_sort(tmp_src_list.begin(), tmp_src_list.end());
    lib::ob_sort(tmp_dest_list.begin(), tmp_dest_list.end());
    for(int64_t i = 0; is_match && i < tmp_src_list.count(); ++i) {
      if(tmp_src_list.at(i) != tmp_dest_list.at(i)) {
        is_match = false;
      }
    }
  }
  return is_match;
}

/* 功能：检查索引列和表的外键列是否完全匹配
         如果表是外键的子表，则索引列是否和 child_column_ids_ 完全一致
   参数：
     in: table_schema,
     in: index_table_schema
   返回值：
     如果索引列和某个外键列完全匹配的话，则返回 OB_ERR_ALTER_COLUMN_FK
     否则，返回 OB_SUCCESS
*/
int ObDDLResolver::check_index_columns_equal_foreign_key(
    const ObTableSchema &table_schema,
    const ObTableSchema &index_table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_child_table()) {
    ObString index_name;
    ObSEArray<uint64_t, 8> index_column_ids;
    const ObIndexInfo &index_info = index_table_schema.get_index_info();
    if (OB_FAIL(index_info.get_column_ids(index_column_ids))) {
      LOG_WARN("failed to get column ids from ObRowkeyInfo", K(ret));
    } else if (OB_FAIL(index_table_schema.get_index_name(index_name))) {
      LOG_WARN("failed to get index name", K(ret));
    }
    const ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema.get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); ++i) {
      const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
      const int64_t child_column_num = foreign_key_info.child_column_ids_.count();
      const uint64_t data_table_id = index_table_schema.get_data_table_id();
      if (0 == child_column_num) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected foreign key columns num", K(ret), K(child_column_num));
      }
      if (OB_SUCC(ret)) {
        if (data_table_id == foreign_key_info.child_table_id_) {
          if (child_column_num == index_table_schema.get_index_column_number()) {
            if (is_ids_match(index_column_ids, foreign_key_info.child_column_ids_)) {
              ret = OB_ERR_ALTER_COLUMN_FK;
              LOG_USER_ERROR(OB_ERR_ALTER_COLUMN_FK, index_name.length(), index_name.ptr());
            }
          }
        } // child_table_id_
      }
    } // for
  }
  return ret;
}

// for mysql mode
// mysql 模式下认为同一表中的 index_1(c1, c2) 和 index_2(c2, c1) 是同样的索引
int ObDDLResolver::check_indexes_on_same_cols(const ObTableSchema &table_schema,
                                              const share::schema::ObTableSchema &input_index_table_schema,
                                              ObSchemaChecker &schema_checker,
                                              bool &has_other_indexes_on_same_cols)
{
  int ret = OB_SUCCESS;
  uint64_t same_indexes_cnt = 0;
  ObSEArray<ObString, 8> input_index_columns_name;
  has_other_indexes_on_same_cols = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  if (OB_FAIL(ObResolverUtils::get_columns_name_from_index_table_schema(input_index_table_schema, input_index_columns_name))) {
    LOG_WARN("get columns name from input index table schema failed", K(ret));
  } else if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  // 遍历同一张表的每一个索引，比较索引列与输入索引是否完全匹配
  for (int64_t i = 0; OB_SUCC(ret) && !has_other_indexes_on_same_cols && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = NULL;
    bool is_match = false;
    if (OB_FAIL(schema_checker.get_table_schema(table_schema.get_tenant_id(), simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else {
      ObSEArray<ObString, 8> index_columns_name;
      if (OB_FAIL(ObResolverUtils::get_columns_name_from_index_table_schema(*index_table_schema, index_columns_name))) {
        LOG_WARN("get columns name from compared index table schema failed", K(ret));
      } else if (OB_FAIL(ObResolverUtils::check_match_columns(input_index_columns_name, index_columns_name, is_match))) {
        LOG_WARN("Failed to check_match_columns", K(ret));
      } else if (true == is_match) {
        ++same_indexes_cnt;
        if (same_indexes_cnt > 1) {
          has_other_indexes_on_same_cols = true;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !has_other_indexes_on_same_cols && table_schema.is_iot_table()) {
    ObSEArray<uint64_t, 8> pk_column_ids;
    ObSEArray<ObString, 8> pk_columns_names;
    table_schema.get_rowkey_column_ids(pk_column_ids);
    ObString column_name;
    bool is_column_exist = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < pk_column_ids.count(); ++i) {
      table_schema.get_column_name_by_column_id(pk_column_ids.at(i), column_name, is_column_exist);
      if (!is_column_exist) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is not exist", K(ret), K(table_schema), K(pk_column_ids.at(i)));
      } else if (OB_FAIL(pk_columns_names.push_back(column_name))) {
        LOG_WARN("push_back failed", K(ret), K(column_name));
      }
    }
    if (FAILEDx(ObResolverUtils::check_match_columns(input_index_columns_name, pk_columns_names, has_other_indexes_on_same_cols))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    }
  }
  return ret;
}

// for oracle mode
// oracle 模式下认为同一表中的 index 需要各列的顺序一致且各列的 order 顺序一致才算是相同索引
int ObDDLResolver::check_indexes_on_same_cols(const ObTableSchema &table_schema,
                                              const ObCreateIndexArg &create_index_arg,
                                              ObSchemaChecker &schema_checker,
                                              bool &has_other_indexes_on_same_cols)
{
  int ret = OB_SUCCESS;
  has_other_indexes_on_same_cols = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  // 遍历同一张表的每一个索引，比较索引列与输入索引是否完全匹配
  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_other_indexes_on_same_cols && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = NULL;
    if (OB_FAIL(schema_checker.get_table_schema(table_schema.get_tenant_id(), simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (OB_FAIL(ObResolverUtils::check_match_columns_strict_with_order(
                       index_table_schema, create_index_arg, has_other_indexes_on_same_cols))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    }
  }

  return ret;
}

int ObDDLResolver::check_index_name_duplicate(const ObTableSchema &table_schema,
                                              const ObCreateIndexArg &create_index_arg,
                                              ObSchemaChecker &schema_checker,
                                              bool &has_same_index_name)
{
  int ret = OB_SUCCESS;
  has_same_index_name = false;
  ObString index_name;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;

  // 遍历同一张表的每一个索引，比较索引名与输入索引名是否相同
  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("get simple_index_infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_same_index_name && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = NULL;
    if (OB_FAIL(schema_checker.get_table_schema(table_schema.get_tenant_id(),
                                                simple_index_infos.at(i).table_id_,
                                                index_table_schema))) {
      LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema should not be null", K(ret));
    } else if (OB_FAIL(index_table_schema->get_index_name(index_name))) {
      LOG_WARN("Failed to get_index_name", K(ret));
    } else if (0 == index_name.compare(create_index_arg.index_name_)) {
      has_same_index_name = true;
    }
  }

  if (is_oracle_mode() && !has_same_index_name) {
    //in oracle mode, names of index and primary key can't be same
    ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
    for (; !has_same_index_name && iter != table_schema.constraint_end(); ++iter) {
      if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
        if (0 == create_index_arg.index_name_.compare((*iter)->get_constraint_name_str())) {
          has_same_index_name = true;
        }
      }
    }
  }

  return ret;
}

// child 5 of root node, resolve index partition node,
// 1 this index is global, we need to first generate index schema,
//   than resolve global index partition info
// 2 this index is local, an error is raised since we cannot specify
//   partition info for a local index
int ObDDLResolver::resolve_index_partition_node(
    ParseNode *index_partition_node,
    ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == index_partition_node)
      || OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_partition_node), KP(crt_idx_stmt));
  } else {
    ObTableSchema &index_schema = crt_idx_stmt->get_create_index_arg().index_schema_;
    if (NULL == index_partition_node) {
      // 没有指定分区方式
    } else if (!global_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("partitioned local index is not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "local index with partition option");
    } else if (!index_schema.is_global_index_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non global index with partition option not supported", K(ret), "index_type", index_schema.get_index_type());
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non global index with partition option");
    } else if (OB_FAIL(resolve_partition_node(crt_idx_stmt, index_partition_node, index_schema))) {
      LOG_WARN("failed to resolve partition node", K(ret));
    } else if (PARTITION_LEVEL_ZERO == index_schema.get_part_level()
               || PARTITION_LEVEL_ONE == index_schema.get_part_level()) {
      // good
    } else if (PARTITION_LEVEL_TWO == index_schema.get_part_level()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "index with sub-partitions");
      LOG_WARN("index table with two level partitions not support", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_key_cover_partition_column(crt_idx_stmt, index_schema))) {
      LOG_WARN("fail to check key cover partition column", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::check_key_cover_partition_keys(
    const bool is_range_part,
    const common::ObPartitionKeyInfo &part_key_info,
    share::schema::ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_info.get_size(); ++i) {
    ObSEArray<uint64_t, 8> cascaded_columns;
    uint64_t column_id = OB_INVALID_ARGUMENT;
    const ObColumnSchemaV2 *column_schema = NULL;
    if (OB_FAIL(part_key_info.get_column_id(i, column_id))) {
      LOG_WARN("fail to get column id", K(ret));
    } else if (NULL == (column_schema = index_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else if (column_schema->get_index_position() > 0) {
      if (is_range_part) {
        if (column_schema->get_index_position() != i + 1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("partition columns not prefix of index columns not support", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition columns not prefix of index columns");
        }
      }
    } else if (!column_schema->is_generated_column()) {
      ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
      LOG_WARN("global index should cover all partition column of global index", K(ret));
      LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "GLOBAL INDEX");
    } else if (is_range_part) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("range partition on generated column in global index not support", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "range partition on generated column in global index");
    } else if (OB_FAIL(column_schema->get_cascaded_column_ids(cascaded_columns))) {
      LOG_WARN("fail to get cascaded columns ids", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < cascaded_columns.count(); ++j) {
        uint64_t cascaded_column_id = cascaded_columns.at(j);
        const ObColumnSchemaV2 *cascaded_column = NULL;
        if (NULL == (cascaded_column = index_schema.get_column_schema(cascaded_column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret));
        } else if (column_schema->get_index_position() > 0) {
          // good, caccaded column is part the index column
        } else {
          ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
          LOG_WARN("global index should cover all partition column of global index", K(ret));
          LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "GLOBAL INDEX");
        }
      }
    }
  }
  return ret;
}
/* 本函数用于检查分区列是否满足唯一性要求
 * 分区列是生成列，则生成列或基础列需要是索引列的子集
 * 分区列非生成列，则分区列需要是索引列的子集
 */
int ObDDLResolver::check_key_cover_partition_column(
    ObCreateIndexStmt *crt_idx_stmt,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (global_) {
    if (INDEX_TYPE_NORMAL_GLOBAL == crt_idx_stmt->get_create_index_arg().index_type_
        || INDEX_TYPE_UNIQUE_GLOBAL == crt_idx_stmt->get_create_index_arg().index_type_
        || INDEX_TYPE_SPATIAL_GLOBAL == crt_idx_stmt->get_create_index_arg().index_type_) {
      const common::ObPartitionKeyInfo &part_key_info = index_schema.get_partition_key_info();
      const common::ObPartitionKeyInfo &subpart_key_info = index_schema.get_subpartition_key_info();
      if (!index_schema.is_partitioned_table()) {
        // not a partition index, good
      } else if (OB_FAIL(check_key_cover_partition_keys(
              index_schema.is_range_part(), part_key_info, index_schema))) {
        LOG_WARN("fail to check key cover", K(ret));
      } else if (OB_FAIL(check_key_cover_partition_keys(
              index_schema.is_range_subpart(), subpart_key_info, index_schema))) {
        LOG_WARN("fail to check key cover", K(ret));
      } else {} // no more to do
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index type", K(ret),
               "index_type", crt_idx_stmt->get_create_index_arg().index_type_);
    }
  } else {
    // no need to check
  }
  return ret;
}

int ObDDLResolver::generate_global_index_schema(
    ObCreateIndexStmt *crt_idx_stmt)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  ObTableSchema &index_schema = crt_idx_stmt->get_create_index_arg().index_schema_;
  if (OB_UNLIKELY(NULL == crt_idx_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (!global_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not build global schema on a local index", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker ptr is null", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(
          session_info_->get_effective_tenant_id(),
          crt_idx_stmt->get_create_index_arg().database_name_,
          crt_idx_stmt->get_create_index_arg().table_name_,
          false/* is index table*/,
          table_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(crt_idx_stmt->get_create_index_arg().database_name_),
                     to_cstring(crt_idx_stmt->get_create_index_arg().table_name_));
      LOG_WARN("table not exist", K(ret),
               "database_name", crt_idx_stmt->get_create_index_arg().database_name_,
               "table_name", crt_idx_stmt->get_create_index_arg().table_name_);
    } else {
      LOG_WARN("fail to get table schema", K(ret));
    }
  } else if (OB_UNLIKELY(NULL == table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(crt_idx_stmt->get_create_index_arg().database_name_),
                   to_cstring(crt_idx_stmt->get_create_index_arg().table_name_));
    LOG_WARN("table not exist", K(ret),
             "database_name", crt_idx_stmt->get_create_index_arg().database_name_,
             "table_name", crt_idx_stmt->get_create_index_arg().table_name_);
  } else if (!GCONF.enable_sys_table_ddl && !table_schema->is_user_table() && !table_schema->is_tmp_table()) {
    ret = OB_ERR_WRONG_OBJECT;
    LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(crt_idx_stmt->get_create_index_arg().database_name_),
                   to_cstring(crt_idx_stmt->get_create_index_arg().table_name_), "BASE_TABLE");
    ObTableType table_type = table_schema->get_table_type();
    LOG_WARN("Not support to create index on non-normal table", K(ret), K(table_type),
             "arg", crt_idx_stmt->get_create_index_arg());
  } else {
    ObArray<ObColumnSchemaV2 *> gen_columns;
    ObCreateIndexArg &create_index_arg = crt_idx_stmt->get_create_index_arg();
    SMART_VAR(ObCreateIndexArg, my_create_index_arg) {
      index_schema.set_table_type(USER_INDEX);
      index_schema.set_index_type(create_index_arg.index_type_);
      ObTableSchema new_table_schema;
      if (OB_FAIL(new_table_schema.assign(*table_schema))) {
        LOG_WARN("fail to assign schema", K(ret));
      } else if (OB_FAIL(my_create_index_arg.assign(create_index_arg))) {
        LOG_WARN("fail to assign index arg", K(ret));
      } else if (OB_FAIL(share::ObIndexBuilderUtil::adjust_expr_index_args(
              my_create_index_arg, new_table_schema, *allocator_, gen_columns))) {
        LOG_WARN("fail to adjust expr index args", K(ret));
      } else if (OB_FAIL(do_generate_global_index_schema(
              my_create_index_arg, new_table_schema))) {
        LOG_WARN("fail to do generate global index schema", K(ret));
      } else if (OB_FAIL(index_schema.assign(my_create_index_arg.index_schema_))){
        LOG_WARN("fail to assign schema", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLResolver::do_generate_global_index_schema(
    ObCreateIndexArg &create_index_arg,
    share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema &index_schema = create_index_arg.index_schema_;
  if (OB_FAIL(share::ObIndexBuilderUtil::set_index_table_columns(
          create_index_arg, table_schema, index_schema))) {
    LOG_WARN("fail to set index table columns", K(ret));
  } else {} // no more to do
  return ret;
}

int ObDDLResolver::resolve_check_constraint_node(
    const ParseNode &cst_node,
    ObIArray<ObConstraint> &csts,
    const share::schema::ObColumnSchemaV2 *column_schema)
{
  int ret = OB_SUCCESS;
  if (cst_node.num_child_ != 3) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the num_child of constraint_node is wrong.", K(cst_node.num_child_), K(ret));
  } else {
    ObString cst_name;
    ParseNode *cst_name_node = cst_node.children_[0];
    ParseNode *cst_check_expr_node = cst_node.children_[1];
    ParseNode* cst_check_state_node = cst_node.children_[2];
    bool is_sys_generated_cst_name = false;
    if (OB_ISNULL(cst_check_expr_node)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "NULL ptr", K(ret), K(cst_check_expr_node));
    } else if (OB_ISNULL(cst_name_node)) {
      is_sys_generated_cst_name = true;
    } else if (is_mysql_mode() && cst_name_node->num_child_ != 1) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "the num_child of constraint_name_node is wrong", K(ret), K(cst_name_node->num_child_));
    } else if (lib::is_mysql_mode() && OB_ISNULL(cst_name_node->children_[0])) {
      is_sys_generated_cst_name = true;
    } else {
      if (is_oracle_mode()) {
        cst_name.assign_ptr(cst_name_node->str_value_, static_cast<int32_t>(cst_name_node->str_len_));
      } else {
        cst_name.assign_ptr(cst_name_node->children_[0]->str_value_, static_cast<int32_t>(cst_name_node->children_[0]->str_len_));
      }
      if (cst_name.empty()) {
        ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
        SQL_RESV_LOG(WARN, "zero-length constraint name is illegal", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool need_reset_generated_name = is_sys_generated_cst_name;
      do {
        if (need_reset_generated_name) { // generate cst name automatically
          if (OB_FAIL(ObTableSchema::create_cons_name_automatically(cst_name, table_name_, *allocator_, CONSTRAINT_TYPE_CHECK, lib::is_oracle_mode()))) {
            SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
          } else {
            need_reset_generated_name = false;
          }
        }
        // check length of constraint name
        if (OB_FAIL(ret)) {
        } else if (lib::is_oracle_mode() && cst_name.length() > OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE) {
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_WARN("constraint_name length overflow", K(ret), K(cst_name.length()));
        } else if (lib::is_mysql_mode() && cst_name.length() > OB_MAX_CONSTRAINT_NAME_LENGTH_MYSQL) {
          // TODO:@xiaofeng.lby, can we add this restrict for mysql mode ?
          ret = OB_ERR_TOO_LONG_IDENT;
          LOG_WARN("constraint_name length overflow", K(ret), K(cst_name.length()));
        }
        //check if cst name is duplicate
        for (int64_t i = 0; OB_SUCC(ret) && i < csts.count() && !need_reset_generated_name; ++i) {
          if (lib::is_oracle_mode() && cst_name == csts.at(i).get_constraint_name_str()) {
            if (is_sys_generated_cst_name) {
              need_reset_generated_name = true; // sys generated cst name is duplicate
            } else {
              ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
              LOG_WARN("duplicate check constraint name", K(ret), K(cst_name));
            }
          } else if (lib::is_mysql_mode() && 0 == cst_name.case_compare(csts.at(i).get_constraint_name_str())) {
            if (is_sys_generated_cst_name) {
              need_reset_generated_name = true; // sys generated cst name is duplicate
            } else {
              ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
              LOG_USER_ERROR(OB_ERR_CONSTRAINT_NAME_DUPLICATE, cst_name.length(), cst_name.ptr());
              LOG_WARN("duplicate check constraint name", K(ret), K(cst_name));
            }
          }
        }
      } while (OB_SUCC(ret) && need_reset_generated_name);
      if (OB_SUCC(ret)) {
        ObConstraint cst;
        ObTableSchema tmp_table_schema;
        ObRawExpr* check_expr = NULL;
        if (OB_FAIL(get_table_schema_for_check(tmp_table_schema))) {
          LOG_WARN("get table schema failed", K(ret), K(cst_name));
        } else {
          if (OB_FAIL(check_is_json_contraint(tmp_table_schema, csts, cst_check_expr_node))) {
            LOG_WARN("repeate is json check", K(ret));
          } else if (OB_FAIL(cst.set_constraint_name(cst_name))) {
            LOG_WARN("set constraint name failed", K(ret), K(cst_name));
          } else if (OB_FAIL(resolve_check_constraint_expr(params_,
                                                           cst_check_expr_node,
                                                           tmp_table_schema,
                                                           cst,
                                                           check_expr,
                                                           column_schema))) {
            LOG_WARN("resolver constraint expr failed", K(ret));
          } else {
            cst.set_name_generated_type(is_sys_generated_cst_name ? GENERATED_TYPE_SYSTEM : GENERATED_TYPE_USER);
            // resolve constranit_state in oracle mode
            if (lib::is_oracle_mode()) {
              if (OB_FAIL(resolve_check_cst_state_node_oracle(cst_check_state_node, cst))) {
                SQL_RESV_LOG(WARN, "fail to resolve check cst state node", K(ret));
              }
            } else { // mysql mode
              if (OB_FAIL(resolve_check_cst_state_node_mysql(cst_check_state_node, cst))) {
                SQL_RESV_LOG(WARN, "fail to resolve check cst state node", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              cst.set_constraint_type(CONSTRAINT_TYPE_CHECK);
              ret = csts.push_back(cst);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_is_json_contraint(ObTableSchema &t_table_schema, ObIArray<ObConstraint> &csts, ParseNode *cst_check_expr_node)
{
  INIT_SUCC(ret);
  const share::schema::ObColumnSchemaV2 *column_schema = NULL;
  uint64_t col_id = 0;
  if (cst_check_expr_node->type_ == T_FUN_SYS_IS_JSON &&
              OB_NOT_NULL(cst_check_expr_node->children_[0]) && OB_NOT_NULL(cst_check_expr_node->children_[0]->children_[0])) {
    ParseNode *cur_node = cst_check_expr_node->children_[0]->children_[0];
    ObString col_str(cur_node->str_len_, cur_node->str_value_);
    if (OB_ISNULL(column_schema = t_table_schema.get_column_schema(col_str))) {
      // ignore ret
      LOG_WARN("get column schema fail", K(ret));
    } else {
      col_id = column_schema->get_column_id();
      const ParseNode *node = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < csts.count(); ++i) {
        ObConstraint &cst = csts.at(i);
        for (ObConstraint::const_cst_col_iterator cst_col_iter = cst.cst_col_begin();
                OB_SUCC(ret) && (cst_col_iter != cst.cst_col_end()); ++cst_col_iter) {
          if (*cst_col_iter == col_id) {
            if (OB_ISNULL(cst.get_check_expr_str().ptr())) {
            } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                  cst.get_check_expr_str(), *allocator_, node))) {
              LOG_WARN("parse expr node from string failed", K(ret));
            } else {
              if (node->type_ == T_FUN_SYS_IS_JSON) {
                ret = OB_ERR_ADDITIONAL_IS_JSON;
                LOG_WARN("cannot add additional is json check constraint", K(ret));
              }
            }
          }
        }
      }
      for (ObTableSchema::const_constraint_iterator iter = t_table_schema.constraint_begin(); OB_SUCC(ret) &&
                          iter != t_table_schema.constraint_end(); iter ++) {
        ObConstraint *cst = *iter;
        for (ObConstraint::const_cst_col_iterator cst_col_iter = cst->cst_col_begin();
                OB_SUCC(ret) && (cst_col_iter != cst->cst_col_end()); ++cst_col_iter) {
          if (*cst_col_iter == col_id) {
            if (OB_ISNULL(cst->get_check_expr_str().ptr())) {
            } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                  cst->get_check_expr_str(), *allocator_, node))) {
              LOG_WARN("parse expr node from string failed", K(ret));
            } else {
              if (node->type_ == T_FUN_SYS_IS_JSON) {
                ret = OB_ERR_ADDITIONAL_IS_JSON;
                LOG_WARN("cannot add additional is json check constraint", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// only use in mysql mode
int ObDDLResolver::resolve_check_cst_state_node_mysql(
    const ParseNode* cst_check_state_node,
    ObConstraint& cst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cst_check_state_node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "cst_check_state_node is null ptr", K(ret));
  } else if (T_ENFORCED_CONSTRAINT == cst_check_state_node->type_) {
    cst.set_enable_flag(true);
    cst.set_validate_flag(CST_FK_VALIDATED);
  } else if (T_NOENFORCED_CONSTRAINT == cst_check_state_node->type_) {
    cst.set_enable_flag(false);
    cst.set_validate_flag(CST_FK_NO_VALIDATE);
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "constraint type not support");
  }
  return ret;
}

// only use in oracle mode
int ObDDLResolver::resolve_check_cst_state_node_oracle(
    const ParseNode *cst_check_state_node,
    ObConstraint &cst)
{
  LOG_DEBUG("before resolve cst state", K(cst));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cst_check_state_node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "cst_check_state_node is null ptr", K(ret));
  } else if (T_CONSTRAINT_STATE != cst_check_state_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "cst_check_state_node->type_ must be T_CONSTRAINT_STATE", K(ret), K(cst_check_state_node->type_));
  } else if (cst_check_state_node->num_child_ != 4) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the num_child of cst_check_state_node is wrong.", K(ret), K(cst_check_state_node->num_child_));
  } else if (OB_NOT_NULL(cst_check_state_node->children_[1])) {
    // oracle 下 check 约束如果出现 using index 选项，会报 parser 错误；但 using index 是个约束的通用选项，故在此报错
    // https://docs.oracle.com/cd/E11882_01/server.112/e41084/clauses002.htm#SQLRF52180
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_RESV_LOG(WARN, "check constraint can't assign state of using index", K(ret));
  } else {
    if (OB_NOT_NULL(cst_check_state_node->children_[0])) {
      cst.set_rely_flag(T_RELY_CONSTRAINT == cst_check_state_node->children_[0]->type_ ? true : false);
    }
    if (OB_NOT_NULL(cst_check_state_node->children_[2])) {
      cst.set_enable_flag(T_ENABLE_CONSTRAINT == cst_check_state_node->children_[2]->type_ ? true : false);
      // validate_flag 的缺省值和 enable_flag 相关：enable子句中，validate_flag 缺省是 true；disable 子句中，validate_flag 缺省是 false
      cst.set_validate_flag(T_ENABLE_CONSTRAINT == cst_check_state_node->children_[2]->type_ ? CST_FK_VALIDATED : CST_FK_NO_VALIDATE);
    }
    if (OB_NOT_NULL(cst_check_state_node->children_[3])) {
      cst.set_validate_flag(T_VALIDATE_CONSTRAINT == cst_check_state_node->children_[3]->type_ ? CST_FK_VALIDATED : CST_FK_NO_VALIDATE);
    }
  }
  LOG_DEBUG("after resolve cst state", K(cst));
  return ret;
}

// 这个函数只会用在 oracle 模式下面，用于解析 oracle 模式下的主键约束
int ObDDLResolver::resolve_pk_constraint_node(const ParseNode &pk_cst_node,
                                              common::ObString pk_name,
                                              ObSEArray<ObConstraint, 4> &csts)
{
  int ret = OB_SUCCESS;
  ObString cst_name;
  bool is_sys_generated_cst_name = false;
  if ((T_PRIMARY_KEY != pk_cst_node.type_) && (T_COLUMN_DEFINITION != pk_cst_node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node type is wrong.", K(ret), K(pk_cst_node.type_));
  } else if (T_PRIMARY_KEY == pk_cst_node.type_) {
    // 处理 create table t1(c1 int, primary key(c1)); 这种情况
    if (pk_cst_node.num_child_ != 2) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "the num_child of constraint_node is wrong.", K(pk_cst_node.num_child_), K(ret));
    } else {
      ParseNode *cst_name_node = pk_cst_node.children_[1];
      if (OB_ISNULL(cst_name_node)) {
        // 用户没有显式为主键命名时，系统为主键约束命名
        if (OB_FAIL(ObTableSchema::create_cons_name_automatically(cst_name, table_name_, *allocator_, CONSTRAINT_TYPE_PRIMARY_KEY, lib::is_oracle_mode()))) {
          SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
        } else {
          is_sys_generated_cst_name = true;
        }
      } else if (NULL == cst_name_node->str_value_ || 0 == cst_name_node->str_len_) {
        ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
        SQL_RESV_LOG(WARN, "zero-length constraint name is illegal", K(ret));
      } else {
        cst_name.assign_ptr(cst_name_node->str_value_,static_cast<int32_t>(cst_name_node->str_len_));
      }
    }
  } else {
    if (NULL == pk_name.ptr()) {
      if (OB_FAIL(ObTableSchema::create_cons_name_automatically(cst_name, table_name_, *allocator_, CONSTRAINT_TYPE_PRIMARY_KEY, lib::is_oracle_mode()))) {
        SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
      } else {
        is_sys_generated_cst_name = true;
      }
    } else {
      cst_name.assign_ptr(pk_name.ptr(), pk_name.length());
    }
  }
  if (OB_SUCC(ret)) {
    ObConstraint cst;
    if (cst_name.length() > OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE) {
      ret = OB_ERR_TOO_LONG_IDENT;
      LOG_WARN("constraint_name length overflow", K(ret), K(cst_name.length()));
    } else {
      //ObTableSchema tmp_table_schema;
      if (csts.end() != std::find_if(csts.begin(), csts.end(),
                   [&cst_name](const ObConstraint &cst) {
                     return cst_name == cst.get_constraint_name_str();
                   })) {
        ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
        LOG_WARN("duplicate constraint name", K(ret), K(cst_name));
      } else if (OB_FAIL(cst.set_constraint_name(cst_name))) {
      } else {
        cst.set_name_generated_type(is_sys_generated_cst_name ? GENERATED_TYPE_SYSTEM : GENERATED_TYPE_USER);
        cst.set_constraint_type(CONSTRAINT_TYPE_PRIMARY_KEY);
        ret = csts.push_back(cst);
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_check_constraint_expr(
    ObResolverParams &params,
    const ParseNode *node,
    ObTableSchema &table_schema,
    ObConstraint &constraint,
    ObRawExpr *&check_expr,
    const share::schema::ObColumnSchemaV2 *column_schema)
{
  int ret = OB_SUCCESS;
  check_expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("NULL ptr", K(node));
  } else {
    if (OB_FAIL(ObResolverUtils::resolve_check_constraint_expr(params, node,
                                                               table_schema,
                                                               constraint,
                                                               check_expr,
                                                               column_schema))) {
          LOG_WARN("resolve check constraint expr failed", K(ret));
        } else if (OB_ISNULL(check_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check_expr is null", K(ret));
    }
  }
  return ret;
}
int ObDDLResolver::set_index_tablespace(const ObTableSchema &table_schema,
                                        ObCreateIndexArg &index_arg)
{
  int ret = OB_SUCCESS;
  index_arg.index_schema_.set_tablespace_id(table_schema.get_tablespace_id());
  if (OB_FAIL(index_arg.index_schema_.set_encryption_str(table_schema.get_encryption_str()))) {
    LOG_WARN("fail to set encryption str", K(table_schema), K(ret));
  }
  return ret;
}

int ObDDLResolver::resolve_split_partition_range_element(const ParseNode *node,
                                                         const share::schema::ObPartitionFuncType part_type,
                                                         const ObIArray<ObRawExpr *> &part_func_exprs,
                                                         common::ObIArray<ObRawExpr *> &range_value_exprs,
                                                         const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)
      || T_EXPR_LIST != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", K(ret), "type", node->type_);
  } else if (!in_tablegroup) {
    if (node->num_child_ != part_func_exprs.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid_argument", K(ret), "node num", node->num_child_,
               "function num", part_func_exprs.count());
    }
  }
  //这个地方没有什么特殊的含义，只是Oracle中可以允许不定义分区名，
  //但是下面函数的解析需要part_name这个变量，所以随意添加了一个值
  ObString part_name;
  for (int i = 0 ; OB_SUCC(ret) && i < node->num_child_; ++i) {
    if (OB_ISNULL(node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret), K(i), "node", node->children_[i]);
    } else if (T_MAXVALUE == node->children_[i]->type_) {
      ObRawExpr *maxvalue_expr = NULL;
      ObConstRawExpr *c_expr = NULL;
      c_expr = (ObConstRawExpr *) allocator_->alloc(sizeof(ObConstRawExpr));
      if (OB_ISNULL(c_expr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc raw expr", K(ret), K(c_expr));
      } else {
        c_expr = new(c_expr) ObConstRawExpr();
        maxvalue_expr = c_expr;
        maxvalue_expr->set_data_type(common::ObMaxType);
        if (OB_FAIL(range_value_exprs.push_back(maxvalue_expr))) {
          LOG_WARN("array push back fail", K(ret));
        }
      }
    } else if (T_NULL == node->children_[i]->type_) {
      ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
      LOG_WARN("null value is not allowed in less than", K(ret));
    } else if (T_EXPR_LIST != node->children_[i]->type_) {
      ObRawExpr *part_value_expr = NULL;
      ObRawExpr *part_func_expr = NULL;
      if (!in_tablegroup) {
        if (OB_FAIL(part_func_exprs.at(i, part_func_expr))) {
          LOG_WARN("get part expr failed", K(ret), K(i), "size", part_func_exprs.count());
        } else if (OB_ISNULL(part_func_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part func expr is invalid", K(ret), K(i));
        } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params_,
                                                                               *(node->children_[i]),
                                                                               part_name,
                                                                               part_type,
                                                                               *part_func_expr,
                                                                               part_value_expr,
                                                                               in_tablegroup))) {
          LOG_WARN("failed to resolve at values", K(ret));
        }
      } else {
        if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params_,
                                                                        *(node->children_[i]),
                                                                        part_name,
                                                                        part_type,
                                                                        part_value_expr,
                                                                        in_tablegroup))) {
          LOG_WARN("failed to resolve at values", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(range_value_exprs.push_back(part_value_expr))) {
        LOG_WARN("failed to push back raw_expr", K(ret), K(part_value_expr));
      }
    }
  }// end for process t_expr_list
  return ret;
}

int ObDDLResolver::resolve_split_partition_list_value(const ParseNode *node,
                                                      const share::schema::ObPartitionFuncType part_type,
                                                      const ObIArray<ObRawExpr *> &part_func_exprs,
                                                      ObDDLStmt::array_t &list_value_exprs,
                                                      int64_t &expr_num,
                                                      const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObString part_name;
  ObOpRawExpr *row_expr = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(params_.expr_factory_));
  } else if (T_EXPR_LIST != node->type_ && T_DEFAULT != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument node type", K(ret), "node type", node->type_);
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(row_expr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allcoate memory", K(ret));
  } else if (T_DEFAULT == node->type_) {
    //这里使用max来代替default值
    ObRawExpr *maxvalue_expr = NULL;
    ObConstRawExpr *c_expr = NULL;
    c_expr = (ObConstRawExpr *)allocator_->alloc(sizeof(ObConstRawExpr));
    if (OB_ISNULL(c_expr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      c_expr = new (c_expr) ObConstRawExpr();
      maxvalue_expr = c_expr;
      maxvalue_expr->set_data_type(common::ObMaxType);
      ObDDLStmt::array_t part_value_expr_array;
      if (OB_FAIL(row_expr->add_param_expr(maxvalue_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
        LOG_WARN("array push back fail", K(ret));
      }
    }
  } else {
    ObSEArray<ObRawExpr *, 16> part_value_exprs;

    bool is_all_expr_list = false;
    if (node->num_child_ > 0) {
      is_all_expr_list = (node->children_[0]->type_ == T_EXPR_LIST);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < node->num_child_; j++) {
      part_value_exprs.reset();
      if (OB_ISNULL(node->children_[j])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret));
      } else if ((is_all_expr_list && node->children_[j]->type_ != T_EXPR_LIST) ||
                 (!is_all_expr_list && node->children_[j]->type_ == T_EXPR_LIST)) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
      } else if (!in_tablegroup && OB_FAIL(ObResolverUtils::resolve_partition_list_value_expr(params_,
                                                                                              *(node->children_[j]),
                                                                                              part_name,
                                                                                              part_type,
                                                                                              part_func_exprs,
                                                                                              part_value_exprs))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (in_tablegroup && OB_FAIL(ObResolverUtils::resolve_partition_list_value_expr(params_,
                                                                                             *(node->children_[j]),
                                                                                             part_name,
                                                                                             part_type,
                                                                                             expr_num,
                                                                                             part_value_exprs,
                                                                                             in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      }
      for (int64_t k = 0; OB_SUCC(ret) && k < part_value_exprs.count(); k++) {
        if (OB_FAIL(row_expr->add_param_expr(part_value_exprs.at(k)))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      }//end for
    }// end for
    if (OB_FAIL(ret)) {
    } else if (part_func_exprs.count() > 1 && !is_all_expr_list) {
      if (row_expr->get_param_count() != part_func_exprs.count()) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
      LOG_WARN("array push back fail", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::check_split_type_valid(const ParseNode *split_node,
                                          const share::schema::ObPartitionFuncType part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(split_node)
      || T_SPLIT_ACTION != split_node->type_
      || OB_ISNULL(split_node->children_[SPLIT_PARTITION_TYPE_NODE])
      || PARTITION_FUNC_TYPE_MAX == part_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node type error or partition type node is null", K(ret),
             "node_type", split_node->type_,
             "split_partition_type", split_node->children_[SPLIT_PARTITION_TYPE_NODE],
             K(part_type));
  } else if (share::schema::is_list_part(part_type)
             && T_SPLIT_LIST != split_node->children_[SPLIT_PARTITION_TYPE_NODE]->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "VALUES LESS THAN or AT clause cannot be used with List partition");
    LOG_WARN("at clause cannot be used with list partition", K(ret),
             "node_type", split_node->children_[SPLIT_PARTITION_TYPE_NODE]->type_);
  } else if (share::schema::is_range_part(part_type)
             && T_SPLIT_RANGE != split_node->children_[SPLIT_PARTITION_TYPE_NODE]->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "expecting VALUES LESS THAN  or AT clause");
    LOG_WARN("values clause cannot be used with range partitioned", K(ret),
             "node_type", split_node->children_[SPLIT_PARTITION_TYPE_NODE]->type_);
  }
  return ret;
}

int ObDDLResolver::generate_index_name(ObString &index_name,
    IndexNameSet &current_index_name_set,
    const common::ObString &first_col_name)
{
  int ret = OB_SUCCESS;
  //inspect whether first_column_name is exist
  ObIndexNameHashWrapper index_key(first_col_name);
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "allocator is null.", K(ret));
  } else if (OB_HASH_EXIST != current_index_name_set.exist_refactored(index_key)) {
    if (OB_FAIL(ob_write_string(*allocator_, first_col_name, index_name))) {
      SQL_RESV_LOG(WARN, "failed to set index name to first column name", K(ret));
    }
  } else {
    char buffer[number::ObNumber::MAX_PRINTABLE_SIZE];
    ObString str;
    bool b_flag = false;
    ObIndexNameHashWrapper tmp_key;
    for (int32_t i = 2; OB_SUCC(ret) && !b_flag; ++i) {
      if (snprintf(buffer, sizeof(buffer), "%.*s_%d", first_col_name.length(),
                   first_col_name.ptr(), i) < 0) {
        ret = OB_SIZE_OVERFLOW;
        SQL_RESV_LOG(WARN, "failed to generate buffer", K(first_col_name), K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator_,
          ObString::make_string(buffer), str))) {
        SQL_RESV_LOG(WARN, "Can not malloc space for index name");
      } else {
        tmp_key.set_name(str);
        if (OB_HASH_EXIST != current_index_name_set.exist_refactored(tmp_key)) {
          b_flag = true;
        }
      }
    }
    index_name.assign_ptr(str.ptr(), str.length());
  }

  return ret;
}

// description: 解析创建表同时创建外键的相关语法
//
// @param [in] node                create_table 解析树的 table_element_list 子节点
// @param [in] node_position_list  table_element_list 节点中和外键相关的子节点的下标构成的数组
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::resolve_foreign_key(const ParseNode *node,
                                       ObArray<int> &node_position_list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    // do nothing, create table t as select ... will come here
  } else if (T_TABLE_ELEMENT_LIST != node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_));
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_NOT_INIT;
    SQL_RESV_LOG(WARN, "stmt_ is null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->children_));
  } else {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < node_position_list.size(); ++i) {
      int child_pos = node_position_list.at(i);
      ObCreateForeignKeyArg foreign_key_arg;
      if (!(0 <= child_pos && child_pos < node->num_child_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "node pos out of range", K(ret), K(child_pos), K(node->num_child_));
      } else if (OB_FAIL(resolve_foreign_key_node(node->children_[child_pos], foreign_key_arg, false))) {
        SQL_RESV_LOG(WARN, "failed to resolve foreign key node", K(ret), K(child_pos));
      } else if (OB_FAIL(create_table_stmt->get_foreign_key_arg_list().push_back(foreign_key_arg))) {
        SQL_RESV_LOG(WARN, "failed to push back foreign key arg", K(ret));
      }
    }
    if (OB_SUCC(ret)
        && lib::is_oracle_mode()
        && OB_FAIL(ObResolverUtils::check_dup_foreign_keys_exist(
                   create_table_stmt->get_foreign_key_arg_list()))) {
      SQL_RESV_LOG(WARN, "failed to check dup foreign keys exist", K(ret));
    }
    current_foreign_key_name_set_.reset();
  }

  return ret;
}

// int resolve_foreign_key_node(const ParseNode *node, obrpc::ObCreateForeignKeyArg &arg);
// description: 解析 table_element_list 节点中和外键相关的子节点
//
// @param [in] node  table_element_list 节点中和外键相关的子节点
// @param [out] arg  外键相关子节点解析生成的 ObCreateForeignKeyArg
//
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::resolve_foreign_key_node(const ParseNode *node,
                                            obrpc::ObCreateForeignKeyArg &arg,
                                            bool is_alter_table,
                                            const ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node is null", K(ret));
  } else if (!is_oracle_mode()) {
    // mysql mode
    if (T_FOREIGN_KEY != node->type_ || 7 != node->num_child_ || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
    } else {
      ParseNode *child_columns  = node->children_[0];
      ParseNode *parent_table   = node->children_[1];
      ParseNode *parent_columns = node->children_[2];
      ParseNode *reference_options = node->children_[3];
      ParseNode *constraint_name = node->children_[4];
      // foreign_key_name 这个 node 是在 constraint_name 没有被显式声明时，为创建外键时自动创建的索引命名用的
      // 注意：目前版本的外键没有在创建同时建立索引，所以这个 node 暂时没有被用到
      ParseNode *foreign_key_name = node->children_[5];
      UNUSED(foreign_key_name);
      ParseNode *match_options = node->children_[6];
      if (OB_FAIL(resolve_table_relation_node(parent_table, arg.parent_table_, arg.parent_database_))) {
        LOG_WARN("failed to resolve foreign key parent table", K(ret));
      } else if (OB_FAIL(resolve_foreign_key_columns(child_columns, arg.child_columns_))) {
        LOG_WARN("failed to resolve foreign key child columns", K(ret));
      } else if (OB_FAIL(resolve_foreign_key_columns(parent_columns, arg.parent_columns_))) {
        LOG_WARN("failed to resolve foreign key parent columns", K(ret));
      } else if (OB_FAIL(resolve_foreign_key_options(reference_options, arg.update_action_, arg.delete_action_))) {
        LOG_WARN("failed to resolve foreign key options", K(ret));
      } else if (OB_FAIL(resolve_foreign_key_name(constraint_name, arg.foreign_key_name_, arg.name_generated_type_))) {
        LOG_WARN("failed to resolve foreign key name", K(ret));
      } else if (OB_FAIL(check_foreign_key_reference(arg, is_alter_table, NULL))) {
        LOG_WARN("failed to check reference columns", K(ret));
      } else if (OB_FAIL(resolve_match_options(match_options))) {
        LOG_WARN("failed to resolve match options", K(ret));
      }
    }
  } else {
    // oracle mode
    if (NULL == column) { // table level foreign key in oracle mode
      if (T_FOREIGN_KEY != node->type_ || 4 != node->num_child_ || OB_ISNULL(node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
      } else {
        ParseNode *constraint_name = node->children_[0];
        ParseNode *child_columns = node->children_[1];
        ParseNode *references_clause_node = node->children_[2];
        ParseNode *fk_enable_state_node = node->children_[3];
        ParseNode *parent_table = nullptr;
        ParseNode *parent_columns = nullptr;
        ParseNode *reference_options = nullptr;
        if (OB_ISNULL(references_clause_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to resolve references clause, node is null", K(ret));
        } else {
          // resolve references_clause
          if (T_REFERENCES_CLAUSE != references_clause_node->type_
              || 3 != references_clause_node->num_child_
              || OB_ISNULL(references_clause_node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(references_clause_node->type_), K(references_clause_node->num_child_), K(references_clause_node->children_));
          } else {
            parent_table = references_clause_node->children_[0];
            parent_columns = references_clause_node->children_[1];
            reference_options = references_clause_node->children_[2];
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_table_relation_node(parent_table, arg.parent_table_, arg.parent_database_))) {
            LOG_WARN("failed to resolve foreign key parent table", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_columns(child_columns, arg.child_columns_))) {
            LOG_WARN("failed to resolve foreign key child columns", K(ret));
          } else if (OB_FAIL(resolve_fk_referenced_columns_oracle(
                             parent_columns, arg, is_alter_table, arg.parent_columns_))) {
            LOG_WARN("failed to resolve foreign key parent columns in oracle mode", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_option(reference_options, arg.update_action_, arg.delete_action_))) {
            LOG_WARN("failed to resolve foreign key options", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_name(constraint_name, arg.foreign_key_name_, arg.name_generated_type_))) {
            LOG_WARN("failed to resolve foreign key name", K(ret));
          } else if (OB_FAIL(check_foreign_key_reference(arg, is_alter_table, NULL))) {
            LOG_WARN("failed to check reference columns", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_state(fk_enable_state_node, arg))) {
            LOG_WARN("failed to resolve foreign key enable_state", K(ret));
          }
        }
      }
    } else { // column level foreign key in oracle mode
      if (T_FOREIGN_KEY != node->type_ || 3 != node->num_child_ || OB_ISNULL(node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
      } else {
        ParseNode *constraint_name = node->children_[0];
        ParseNode *references_clause_node = node->children_[1];
        ParseNode *fk_enable_state_node = node->children_[2];
        ParseNode *parent_table = nullptr;
        ParseNode *parent_columns = nullptr;
        ParseNode *reference_options = nullptr;
        if (OB_ISNULL(references_clause_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to resolve references clause, node is null", K(ret));
        } else {
          // resolve references_clause
          if (T_REFERENCES_CLAUSE != references_clause_node->type_
              || 3 != references_clause_node->num_child_
              || OB_ISNULL(references_clause_node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(references_clause_node->type_), K(references_clause_node->num_child_), K(references_clause_node->children_));
          } else {
            parent_table = references_clause_node->children_[0];
            parent_columns = references_clause_node->children_[1];
            reference_options = references_clause_node->children_[2];
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(resolve_table_relation_node(parent_table, arg.parent_table_, arg.parent_database_))) {
            LOG_WARN("failed to resolve foreign key parent table", K(ret));
          } else if (OB_FAIL(arg.child_columns_.push_back(ObString(
                             column->get_column_name_str().length(),
                             column->get_column_name_str().ptr())))) {
            LOG_WARN("failed to push back column name", K(ret));
          } else if (OB_FAIL(resolve_fk_referenced_columns_oracle(
                             parent_columns, arg, is_alter_table, arg.parent_columns_))) {
            LOG_WARN("failed to resolve foreign key parent columns", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_option(reference_options, arg.update_action_, arg.delete_action_))) {
            LOG_WARN("failed to resolve foreign key options", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_name(constraint_name, arg.foreign_key_name_, arg.name_generated_type_))) {
            LOG_WARN("failed to resolve foreign key name", K(ret));
          } else if (OB_FAIL(check_foreign_key_reference(arg, is_alter_table, column))) {
            LOG_WARN("failed to check reference columns", K(ret));
          } else if (OB_FAIL(resolve_foreign_key_state(fk_enable_state_node, arg))) {
            LOG_WARN("failed to resolve foreign key enable_state", K(ret));
          }
        }
      }
    }

  } // end oracle mode

  return ret;
}

// description: 解析建立外键关系的列
//
// @param [in] node      与建立有外键关系的列相关的节点
// @param [out] columns  向 arg 中填充的外键列名称
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::resolve_foreign_key_columns(const ParseNode *node,
                                               ObIArray<ObString> &columns)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node is null", K(ret));
  } else if (OB_ISNULL(node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_), K(node->children_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode *column  = node->children_[i];
      if (OB_ISNULL(column)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "child is null", K(ret));
      } else if (OB_ISNULL(column->str_value_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(column->type_), K(column->str_value_));
      } else if (OB_FAIL(columns.push_back(ObString(column->str_len_, column->str_value_)))) {
        LOG_WARN("failed to push back column name", K(ret), K(column->str_value_));
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_fk_referenced_columns_oracle(
    const ParseNode *node,
    const obrpc::ObCreateForeignKeyArg &arg,
    bool is_alter_table,
    ObIArray<ObString> &columns)
{
  int ret = OB_SUCCESS;

  if (lib::is_oracle_mode() && OB_ISNULL(node)) {
    // oracle 模式下可以不指定被引用列，默认引用被引用的父表的主键列
    const ObTableSchema *parent_table_schema = NULL;
    if (0 == arg.parent_table_.case_compare(table_name_)
        && 0 == arg.parent_database_.case_compare(database_name_)
        && !is_alter_table) {
      // create table 时创建自引用外键
      if (OB_ISNULL(static_cast<ObCreateTableStmt*>(stmt_))) {
        ret = OB_NOT_INIT;
        SQL_RESV_LOG(WARN, "stmt_ is null.", K(ret));
      } else {
        parent_table_schema =
            &static_cast<ObCreateTableStmt*>(stmt_)->get_create_table_arg().schema_;
        if (OB_ISNULL(parent_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parent table schema is null", K(ret));
        }
      }
    } else if (OB_FAIL(schema_checker_->get_table_schema(
                       session_info_->get_effective_tenant_id(),
                       arg.parent_database_, arg.parent_table_,
                       false, parent_table_schema))) {
      LOG_WARN("table is not exist", K(ret), K(arg.parent_database_), K(arg.parent_table_));
    } else if (OB_ISNULL(parent_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parent table schema is null", K(ret));
    }
    if (OB_SUCC(ret)) {
      const ObRowkeyInfo &rowkey_info = parent_table_schema->get_rowkey_info();
      // 通过 rowkey_info 把父表的主键列列名拿出来，然后放到 columns 里面
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        uint64_t column_id = 0;
        const ObColumnSchemaV2 *col_schema = NULL;
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get rowkey info", K(ret), K(i), K(rowkey_info));
        } else if (NULL == (col_schema = parent_table_schema->get_column_schema(column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get index column schema failed", K(ret));
        } else if (col_schema->is_hidden() || col_schema->is_shadow_column()) {
          // do nothing
        } else if (OB_FAIL(columns.push_back(col_schema->get_column_name()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push back index column failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && (0 == columns.count())) {
        ret = OB_ERR_REFERENCED_TABLE_HAS_NO_PK;
        LOG_WARN("referenced table does not have a primary key", K(ret), KPC(parent_table_schema));
      }
    }
  } else if (OB_FAIL(resolve_foreign_key_columns(node, columns))) {
    LOG_WARN("failed to resolve foreign key columns", K(ret));
  }

  return ret;
}

// description: 解析外键的 ReferenceAction，包括 update_action 和 delete_action
//
// @param [in] node      与外键的 ReferenceAction 相关的节点
// @param [out] update_action  向 arg 中填充的 ObReferenceAction 信息
// @param [out] delete_action  向 arg 中填充的 ObReferenceAction 信息
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::resolve_foreign_key_options(const ParseNode *node,
                                               ObReferenceAction &update_action,
                                               ObReferenceAction &delete_action)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    // nothing.
  } else if (T_REFERENCE_OPTION_LIST != node->type_ || node->num_child_ > 2) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(node->type_), K(node->num_child_));
  } else if (NULL != node->children_) {
    update_action = ACTION_INVALID;
    delete_action = ACTION_INVALID;
    ParseNode *option_node = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      option_node = node->children_[i];
      ObReferenceAction action = ACTION_INVALID;
      if (OB_ISNULL(option_node)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "option node is null", K(ret), K(i));
      } else {
        switch (option_node->int32_values_[1]) {
        case T_RESTRICT:
          action = ACTION_RESTRICT;
          break;
        case T_CASCADE:
          action = ACTION_CASCADE;
          break;
        case T_SET_NULL:
          {
            uint64_t data_version = 0;
            if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
              LOG_WARN("failed to get data version", K(ret), K(session_info_->get_effective_tenant_id()));
            } else if (data_version >= DATA_VERSION_4_2_1_0) {
              action = ACTION_SET_NULL;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("current tenant data version is less than 4.2.1.0, foreign key set null not supported", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "create foreign key with set null option");
            }
          }
          break;
        case T_NO_ACTION:
          action = ACTION_NO_ACTION;
          break;
        case T_SET_DEFAULT:
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          SQL_RESV_LOG(WARN, "Cannot add foreign key constraint, because the key word of action is ACTION_SET_DEFAULT", K(ret), K(node->int32_values_[1]));
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid reference option", K(ret), K(option_node->int32_values_[1]));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        switch (option_node->int32_values_[0]) {
        case T_UPDATE:
          if (ACTION_INVALID != update_action) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "mutli update option", K(ret));
          } else {
            update_action = action;
          }
          break;
        case T_DELETE:
          if (ACTION_INVALID != delete_action) {
            ret = OB_INVALID_ARGUMENT;
            SQL_RESV_LOG(WARN, "mutli delete option", K(ret));
          } else {
            delete_action = action;
          }
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(WARN, "invalid reference option", K(ret), K(option_node->int32_values_[0]));
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ACTION_INVALID == update_action) {
      update_action = ACTION_RESTRICT;
    }
    if (ACTION_INVALID == delete_action) {
      delete_action = ACTION_RESTRICT;
    }
  }

  return ret;
}

// for oracle mode
int ObDDLResolver::resolve_foreign_key_option(const ParseNode *option_node,
                                              share::schema::ObReferenceAction &update_action,
                                              share::schema::ObReferenceAction &delete_action)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(option_node)) {
    // do nothing.
  } else if (T_REFERENCE_OPTION != option_node->type_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(option_node->type_));
  } else {
    ObReferenceAction action = ACTION_INVALID;
    switch (option_node->int32_values_[1]) {
    case T_CASCADE:
      action = ACTION_CASCADE;
      break;
    case T_SET_NULL:
      {
        uint64_t data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
          LOG_WARN("failed to get data version", K(ret), K(session_info_->get_effective_tenant_id()));
        } else if (data_version >= DATA_VERSION_4_2_1_0) {
          action = ACTION_SET_NULL;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("current tenant data version is less than 4.2.1.0, foreign key set null not supported", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create foreign key with set null option");
        }
      }
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid reference option", K(ret), K(option_node->int32_values_[1]));
      break;
    }
    if (OB_SUCC(ret)) {
      switch (option_node->int32_values_[0]) {
      case T_DELETE:
          delete_action = action;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid reference option", K(ret), K(option_node->int32_values_[0]));
        break;
      }
    }
  }
  // default value is ACTION_RESTRICT
  if (OB_SUCC(ret)) {
    if (ACTION_INVALID == update_action) {
      update_action = ACTION_RESTRICT;
    }
    if (ACTION_INVALID == delete_action) {
      delete_action = ACTION_RESTRICT;
    }
  }

  return ret;
}

// description: 解析创建外键时显示声明的 foreign_key_name
//              在用户没有显式创建外键约束名时，系统自动为用户创建外键约束名
//
// @param [in] constraint_node
// @param [in] foreign_key_node
// @param [out] foreign_key_name
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::resolve_foreign_key_name(const ParseNode *constraint_node,
                                            ObString &foreign_key_name,
                                            ObNameGeneratedType &name_generated_type)
{
  int ret = OB_SUCCESS;
  foreign_key_name.reset();
  name_generated_type = GENERATED_TYPE_USER;

  if (OB_NOT_NULL(constraint_node)) {
    if (!is_oracle_mode()) {
      // mysql mode
      const ParseNode *constraint_name = NULL;
      if (T_CHECK_CONSTRAINT != constraint_node->type_ || 1 != constraint_node->num_child_ || OB_ISNULL(constraint_node->children_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(constraint_node->type_), K(constraint_node->num_child_), KP(constraint_node->children_));
      } else if (OB_NOT_NULL(constraint_name = constraint_node->children_[0])) {
        if (NULL == constraint_name->str_value_ || 0 == constraint_name->str_len_) {
          ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
          SQL_RESV_LOG(WARN, "zero-length constraint name is illegal", K(ret));
        } else {
          foreign_key_name.assign_ptr(constraint_name->str_value_, static_cast<int32_t>(constraint_name->str_len_));
          // 检查一条 create table 语句里连续建多个外键时，外键名是否重复
          ObForeignKeyNameHashWrapper fk_key(foreign_key_name);
          if (OB_HASH_EXIST == (ret = current_foreign_key_name_set_.exist_refactored(fk_key))) {
            SQL_RESV_LOG(WARN, "duplicate fk name", K(ret), K(foreign_key_name));
            ret = OB_ERR_CANNOT_ADD_FOREIGN;
          } else {
            ret = OB_SUCCESS;
            // 向 hash set 插入 fk_name
            if (OB_FAIL(current_foreign_key_name_set_.set_refactored(fk_key))) {
              SQL_RESV_LOG(WARN, "set foreign key name to hash set failed", K(ret), K(foreign_key_name));
            }
          }
        }
      } else if (OB_FAIL(create_fk_cons_name_automatically(foreign_key_name))) {
        SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
      } else {
        name_generated_type = GENERATED_TYPE_SYSTEM;
      }
    } else {
      // oracle mode
      if (NULL == constraint_node->str_value_ || 0 == constraint_node->str_len_) {
        ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
        SQL_RESV_LOG(WARN, "zero-length constraint name is illegal", K(ret));
      } else {
        foreign_key_name.assign_ptr(constraint_node->str_value_, static_cast<int32_t>(constraint_node->str_len_));
        // 检查一条 create table 语句里连续建多个外键时，外键名是否重复
        ObForeignKeyNameHashWrapper fk_key(foreign_key_name);
        if (OB_HASH_EXIST == (ret = current_foreign_key_name_set_.exist_refactored(fk_key))) {
          SQL_RESV_LOG(WARN, "duplicate fk name", K(ret), K(foreign_key_name));
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
        } else {
          ret = OB_SUCCESS;
          // 向 hash set 插入 fk_name
          if (OB_FAIL(current_foreign_key_name_set_.set_refactored(fk_key))) {
            SQL_RESV_LOG(WARN, "set foreign key name to hash set failed", K(ret), K(foreign_key_name));
          }
        }
      }
    }
  } else if (OB_FAIL(create_fk_cons_name_automatically(foreign_key_name))) {
    SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
  } else {
    name_generated_type = GENERATED_TYPE_SYSTEM;
  }

  return ret;
}

int ObDDLResolver::resolve_foreign_key_state(
    const ParseNode *fk_state_node,
    obrpc::ObCreateForeignKeyArg &arg)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(fk_state_node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "cst_check_state_node is null ptr", K(ret));
  } else if (T_CONSTRAINT_STATE != fk_state_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "fk_state_node->type_ must be T_CONSTRAINT_STATE", K(ret), K(fk_state_node->type_));
  } else if (fk_state_node->num_child_ != 4) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "the num_child of cst_check_state_node is wrong.", K(ret), K(fk_state_node->num_child_));
  } else if (OB_NOT_NULL(fk_state_node->children_[1])) {
    // oracle 下 fk 约束如果出现 using index 选项，会报 parser 错误；但 using index 是个通用选项，故在此报错
    // https://docs.oracle.com/cd/E11882_01/server.112/e41084/clauses002.htm#SQLRF52180
    ret = OB_ERR_PARSER_SYNTAX;
    SQL_RESV_LOG(WARN, "foreign key can't assign state of using index", K(ret));
  } else {
    if (OB_NOT_NULL(fk_state_node->children_[0])) {
      arg.rely_flag_ = (T_RELY_CONSTRAINT == fk_state_node->children_[0]->type_ ? true : false);
    }
    if (OB_NOT_NULL(fk_state_node->children_[2])) {
      arg.enable_flag_ = (T_ENABLE_CONSTRAINT == fk_state_node->children_[2]->type_ ? true : false);
      // validate_flag 的缺省值和 enable_flag 相关：enable子句中，validate_flag 缺省是 true；disable 子句中，validate_flag 缺省是 false
      arg.validate_flag_ = (T_ENABLE_CONSTRAINT == fk_state_node->children_[2]->type_ ? CST_FK_VALIDATED : CST_FK_NO_VALIDATE);
    }
    if (OB_NOT_NULL(fk_state_node->children_[3])) {
      arg.validate_flag_ = (T_VALIDATE_CONSTRAINT == fk_state_node->children_[3]->type_ ? CST_FK_VALIDATED : CST_FK_NO_VALIDATE);
    }
  }

  return ret;
}

// description: 检查创建外键时父表和子表的外键列数量和类型是否匹配
//              检查创建外键时父表的外键列是否是主键或者唯一索引
//              检查当外键属于自引用时，外键列是否不为同一列
//              检查外键的casacde行为是否和schema里的定义相冲突 例如 set_null 和 not_null
//
// @param [in] arg  已经存有外键信息的 ObCreateForeignKeyArg
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::check_foreign_key_reference(
    obrpc::ObCreateForeignKeyArg &arg,
    bool is_alter_table,
    const ObColumnSchemaV2 *column)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session_info_ is null.", K(ret));
  } else {
    ObCreateTableStmt *create_table_stmt = NULL;
    ObAlterTableStmt *alter_table_stmt = NULL;
    const ObTableSchema *child_table_schema = NULL;
    const ObTableSchema *parent_table_schema = NULL;
    ObString &database_name = arg.parent_database_;
    ObString &parent_table_name = arg.parent_table_;
    bool is_self_reference = false;
    if (!is_alter_table) {
      create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
      if (OB_ISNULL(create_table_stmt)) {
        ret = OB_NOT_INIT;
        SQL_RESV_LOG(WARN, "stmt_ is null.", K(ret));
      } else {
        child_table_schema = &create_table_stmt->get_create_table_arg().schema_;
      }
    } else { // is alter table
      alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
      if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                                                    alter_table_stmt->get_org_database_name(),
                                                    alter_table_stmt->get_org_table_name(),
                                                    false,
                                                    child_table_schema))) {
        LOG_WARN("table is not exist", K(database_name), K(parent_table_name), K(ret));
      } else if (OB_ISNULL(child_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent table schema is null", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // 判断外键约束是否属于自引用
      if (0 == parent_table_name.case_compare(table_name_) &&
          0 == database_name.case_compare(database_name_)) {
        is_self_reference = true;
        parent_table_schema = child_table_schema;
        // 如果属于自引用，则参考列必须都不同
        if (OB_FAIL(ObResolverUtils::check_self_reference_fk_columns_satisfy(arg))) {
          LOG_WARN("check self reference foreign key columns satisfy failed", K(ret), K(arg));
        }
      } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
                         database_name, parent_table_name, false, parent_table_schema))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          int64_t foreign_key_checks = true;
          session_info_->get_foreign_key_checks(foreign_key_checks);
          foreign_key_checks = is_oracle_mode() || (is_mysql_mode() && foreign_key_checks);
          if (!foreign_key_checks) {
            if (0 != database_name.case_compare(database_name_)) {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "parent table and child table in foreign key belong to different databases");
              LOG_WARN("create mock fk parent table which has child tables in different database not supported", K(ret), K(database_name), K(database_name_));
            } else {
              ret = OB_SUCCESS;
              arg.is_parent_table_mock_ = true;
              arg.need_validate_data_ = false;
              LOG_INFO("parent_table is not exist and foreign_key_checks is off", K(ret),
                       K(session_info_->get_effective_tenant_id()), K(database_name), K(parent_table_name));
            }
          } else {
            LOG_WARN("table is not exist", K(ret), K(database_name), K(parent_table_name));
          }
        }
      } else if (OB_ISNULL(parent_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent table schema is null", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!child_table_schema->is_user_table()) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("foreign key cannot be based on non-user table",
                 K(ret), K(child_table_schema->is_user_table()));
      } else if (arg.is_parent_table_mock_) {
        // skip checking parent table
        uint64_t database_id = session_info_->get_database_id();
        const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
        if (!arg.database_name_.empty()
            && OB_FAIL(schema_checker_->get_database_id(
               session_info_->get_effective_tenant_id(), arg.database_name_, database_id))) {
          LOG_WARN("failed to get_database_id", K(ret), K(session_info_->get_effective_tenant_id()),
                                                K(arg.database_name_), K(database_id));
        } else if (OB_FAIL(schema_checker_->get_mock_fk_parent_table_with_name(
                           session_info_->get_effective_tenant_id(), database_id,
                           arg.foreign_key_name_, mock_fk_parent_table_schema))) {
          LOG_WARN("failed to get_mock_fk_parent_table_schema_with_name", K(ret),
                    K(session_info_->get_effective_tenant_id()), K(database_id),
                    K(arg.foreign_key_name_));
        } else if (OB_NOT_NULL(mock_fk_parent_table_schema)) {
          if (is_alter_table
              && OB_FAIL(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.push_back(
                 ObBasedSchemaObjectInfo(
                     mock_fk_parent_table_schema->get_mock_fk_parent_table_id(),
                     MOCK_FK_PARENT_TABLE_SCHEMA,
                     mock_fk_parent_table_schema->get_schema_version())))) {
            LOG_WARN("failed to add based_schema_object_info to arg",
                         K(ret), K(mock_fk_parent_table_schema->get_mock_fk_parent_table_id()),
                         K(mock_fk_parent_table_schema->get_schema_version()));
          } else if (!is_alter_table
                     && OB_FAIL(create_table_stmt->get_create_table_arg().based_schema_object_infos_.push_back(
                     ObBasedSchemaObjectInfo(
                         mock_fk_parent_table_schema->get_mock_fk_parent_table_id(),
                         MOCK_FK_PARENT_TABLE_SCHEMA,
                         mock_fk_parent_table_schema->get_schema_version())))) {
            LOG_WARN("failed to add based_schema_object_info to arg",
                         K(ret), K(mock_fk_parent_table_schema->get_mock_fk_parent_table_id()),
                         K(mock_fk_parent_table_schema->get_schema_version()));
          }
        }
      } else if (!parent_table_schema->is_user_table()) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("foreign key cannot be based on non-user table",
                 K(ret), K(parent_table_schema->is_user_table()));
      } else {
        ObSEArray<ObString, 8> &child_columns = arg.child_columns_;
        ObSEArray<ObString, 8> &parent_columns = arg.parent_columns_;
        if (OB_FAIL(ObResolverUtils::check_foreign_key_columns_type(
                    lib::is_mysql_mode(),
                    *child_table_schema,
                    *parent_table_schema,
                    child_columns,
                    parent_columns,
                    column))) {
          LOG_WARN("Failed to check_foreign_key_columns_type", K(ret));
        } else {
          bool is_matched = false;
          ObSArray<ObCreateIndexArg> index_arg_list;
          if (!is_alter_table) {
            if (parent_table_schema->get_table_id() == child_table_schema->get_table_id()) {
              // 只有自引用的时候， index_arg_list 里才会有子表的索引信息
              index_arg_list.assign(create_table_stmt->get_index_arg_list());
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObResolverUtils::foreign_key_column_match_uk_pk_column(
              *parent_table_schema, *schema_checker_, parent_columns, index_arg_list, !lib::is_mysql_mode()/*is_oracle_mode*/,
              arg.ref_cst_type_, arg.ref_cst_id_, is_matched))) {
            LOG_WARN("Failed to check reference columns in parent table");
          } else if (!is_matched) {
            if (lib::is_mysql_mode()) {
              ret = OB_ERR_CANNOT_ADD_FOREIGN;
              LOG_WARN("reference columns aren't reference to pk or uk in parent table", K(ret));
            } else { // oracle mode
              ret = OB_ERR_NO_MATCHING_UK_PK_FOR_COL_LIST;
              LOG_WARN("reference columns aren't reference to pk or uk in parent table", K(ret));
            }
          } else { } // do-nothing
        }
        if (OB_SUCC(ret) && !is_self_reference) {
          // add parent table info to based_schema_object_infos_
          if (is_alter_table
              && OB_FAIL(alter_table_stmt->get_alter_table_arg().based_schema_object_infos_.push_back(
                 ObBasedSchemaObjectInfo(parent_table_schema->get_table_id(),
                                         TABLE_SCHEMA,
                                         parent_table_schema->get_schema_version())))) {
            LOG_WARN("push back to based_schema_object_infos_ failed", K(ret));
          } else if (!is_alter_table
                     && OB_FAIL(create_table_stmt->get_create_table_arg().based_schema_object_infos_.push_back(
                     ObBasedSchemaObjectInfo(parent_table_schema->get_table_id(),
                                             TABLE_SCHEMA,
                                             parent_table_schema->get_schema_version())))) {
            LOG_WARN("push back to based_schema_object_infos_ failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObResolverUtils::check_foreign_key_set_null_satisfy(arg, *child_table_schema, lib::is_mysql_mode()))) {
        LOG_WARN("check fk set null satisfy failed", K(ret), K(arg), "is_mysql_mode", lib::is_mysql_mode());
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_match_options(const ParseNode *match_options_node) {
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(match_options_node)) {
    // 现在我们兼容 MYSQL 5.6 版本，仅支持 match 子句的语法，不支持 match 功能。
    LOG_WARN("the function of match options on foreign key is not supported now.");
    // 检查 match 后面的选项是否是合法的的
    if (T_FOREIGN_KEY_MATCH != match_options_node->type_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_RESV_LOG(WARN, "invalid argument", K(ret), K(match_options_node->type_));
    } else {
      switch (match_options_node->int32_values_[0]) {
      case T_SIMPLE:
        break;
      case T_FULL:
        break;
      case T_PARTIAL:
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        SQL_RESV_LOG(WARN, "invalid reference option", K(ret), K(match_options_node->int32_values_[0]));
        break;
      }
    }
  }
  else {
    // 没有 match 子句，什么也不检查
  }

  return ret;
}

// description: 当用户创建外键时，没有显式声明外键约束名，系统会自动为其生成一个外键约束名
//              生成规则：fk_name_sys_auto = tblname_OBFK_timestamp
//              如果 tblname 长度超过 60 字节，则截断前六十个字节当做拼接名中的 tblname
// @param [in] foreign_key_name
// @return oceanbase error code defined in lib/ob_errno.def
int ObDDLResolver::create_fk_cons_name_automatically(ObString &foreign_key_name) {
  int ret = OB_SUCCESS;
  char temp_str_buf[number::ObNumber::MAX_PRINTABLE_SIZE];
  ObString cons_name_str;
  bool is_exist = false;
  ObString tmp_table_name;

  if (table_name_.length() > OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN) {
    if (OB_FAIL(ob_sub_str(*allocator_, table_name_, 0, OB_ORACLE_CONS_OR_IDX_CUTTED_NAME_LEN - 1, tmp_table_name))) {
      SQL_RESV_LOG(WARN, "failed to cut table to 60 byte", K(ret), K(table_name_));
    }
  } else {
    tmp_table_name = table_name_;
  }
  if (OB_SUCC(ret)) {
    // 用时间戳的目的是在保证外键名的唯一性的同时避免用户显式声明的外键约束名和系统自动生成的约束名相同
    if (snprintf(temp_str_buf, sizeof(temp_str_buf), "%.*s_OBFK_%ld", tmp_table_name.length(), tmp_table_name.ptr(),
                 ObTimeUtility::current_time()) < 0) {
      ret = OB_SIZE_OVERFLOW;
      SQL_RESV_LOG(WARN, "failed to generate buffer for temp_str_buf", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString::make_string(temp_str_buf), cons_name_str))) {
      SQL_RESV_LOG(WARN, "Can not malloc space for constraint name", K(ret));
    } else {
      int hash_ret = OB_HASH_NOT_EXIST;
      ObForeignKeyNameHashWrapper fk_key(foreign_key_name);
      foreign_key_name.assign_ptr(cons_name_str.ptr(), cons_name_str.length());
      // 检查自动生成的外键名是否会和用户创建的外键名重复，如果重复就重新自动生成
      if (OB_HASH_EXIST == (hash_ret = current_foreign_key_name_set_.exist_refactored(fk_key))) {
        is_exist = true;
      }
      if (true == is_exist) {
        if (OB_FAIL(create_fk_cons_name_automatically(foreign_key_name))) {
          SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
        }
      } else {
        if (OB_FAIL(current_foreign_key_name_set_.set_refactored(fk_key))) {
          SQL_RESV_LOG(WARN, "set foreign key name to hash set failed", K(ret), K(foreign_key_name));
        }
      }
    }
  }

  return ret;
}

//alter table t modify c1 null;  not null constraint on c1 should be dropped.
int ObDDLResolver::drop_not_null_constraint(const ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info_ is null", K(ret));
    } else if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(), column.get_table_id(), table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(column));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else {
      ObConstraint cst;
      ObTableSchema::const_constraint_iterator iter = table_schema->constraint_begin();
      for (;OB_SUCC(ret) && iter != table_schema->constraint_end(); ++iter) {
        if (CONSTRAINT_TYPE_NOT_NULL == (*iter)->get_constraint_type()) {
          if ((*iter)->cst_col_begin() + 1 != (*iter)->cst_col_end()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("not null constraint should contain only one column", K(ret), KPC(*iter));
          } else if(*((*iter)->cst_col_begin()) == column.get_column_id()) {
            if (OB_FAIL(cst.assign(**iter))) {
              LOG_WARN("Fail to assign constraint", K(ret));
            }
            break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (table_schema->constraint_end() == iter) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not null constraint not found", K(ret), KPC(table_schema), K(column));
      } else {
        ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
        AlterTableSchema &alter_table_schema = alter_table_stmt->
                                            get_alter_table_arg().alter_table_schema_;
        if (OB_FAIL(alter_table_schema.add_constraint(cst))) {
          LOG_WARN("push back cst failed", K(ret));
        } else if (OB_UNLIKELY(
                  alter_table_stmt->get_alter_table_arg().alter_constraint_type_
                    != obrpc::ObAlterTableArg::DROP_CONSTRAINT
                  && alter_table_stmt->get_alter_table_arg().alter_constraint_type_
                    != obrpc::ObAlterTableArg::CONSTRAINT_NO_OPERATION)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "Add/modify not null constraint together with other DDLs");
        } else {
          alter_table_stmt->get_alter_table_arg().alter_constraint_type_ =
                            obrpc::ObAlterTableArg::DROP_CONSTRAINT;
        }
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_not_null_constraint_node(
    ObColumnSchemaV2 &column,
    const ParseNode *cst_node,
    const bool is_identity_column)
{
  int ret = OB_SUCCESS;
  ObString cst_name;
  ObConstraint cst;
  if (NULL != cst_node &&
      (T_CONSTR_NOT_NULL != cst_node->type_ || (is_oracle_mode() && cst_node->num_child_ != 2))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "node type is wrong.", K(ret), K(cst_node->type_), K(cst_node->num_child_));
  } else {
    ParseNode *cst_name_node = is_oracle_mode() && NULL != cst_node ? cst_node->children_[0] : NULL;
    ParseNode *cst_check_state_node = is_oracle_mode() && NULL != cst_node ? cst_node->children_[1] : NULL;
    bool is_sys_generate_name = false;
    if (NULL == cst_name_node) {
      if (OB_FAIL(ObTableSchema::create_cons_name_automatically(cst_name, table_name_, *allocator_,
                                                                CONSTRAINT_TYPE_NOT_NULL,
                                                                lib::is_oracle_mode()))) {
        SQL_RESV_LOG(WARN, "create cons name automatically failed", K(ret));
      } else {
        is_sys_generate_name = true;
      }
    } else if (NULL == cst_name_node->str_value_ || 0 == cst_name_node->str_len_) {
      ret = OB_ERR_ZERO_LENGTH_IDENTIFIER;
      SQL_RESV_LOG(WARN, "zero-length constraint name is illegal", K(ret));
    } else {
      cst_name.assign_ptr(cst_name_node->str_value_, static_cast<int32_t>(cst_name_node->str_len_));
    }
    LOG_DEBUG("resolve not null constraint node mid", K(cst_name), K(cst_name_node));
    if (OB_SUCC(ret)) {
      if (is_oracle_mode() &&
          OB_FAIL(resolve_check_cst_state_node_oracle(cst_check_state_node, cst))) {
        LOG_WARN("resolve check cst state node failed", K(ret));
      } else if (OB_UNLIKELY(is_identity_column
                            && (!cst.get_enable_flag()
                                || cst.is_no_validate()))) {
        ret = column.is_default_on_null_identity_column() ?
                OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_DEFAULT_ON_NULL_IDENTITY_COLUMN :
                OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
        LOG_WARN("invalid not null flag for identity column", K(ret));
      } else if (column.has_not_null_constraint()) {
        if (OB_UNLIKELY(!is_identity_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect resolve not null constraint for not null column", K(ret), K(column));
        } else {
          // do nothing if alter table modify identity_column not null
        }
      } else if (OB_FAIL(add_not_null_constraint(column, cst_name, is_sys_generate_name, cst, *allocator_, stmt_))) {
        LOG_WARN("add not null constraint", K(ret));
      } else {
        LOG_DEBUG("before column set not null", K(column), K(cst));
        column.add_not_null_cst(cst.get_rely_flag(), cst.get_enable_flag(),
                                cst.is_validated());
        LOG_DEBUG("after column set not null", K(column));
      }
    }
  }

  return ret;
}

// identity column is default not null, also for ctas not null column
int ObDDLResolver::add_default_not_null_constraint(ObColumnSchemaV2 &column,
                                                   const ObString &table_name,
                                                   ObIAllocator &allocator,
                                                   ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObString cst_name;
  ObConstraint cst;
  if (OB_FAIL(ObTableSchema::create_cons_name_automatically(cst_name, table_name, allocator,
                                                                CONSTRAINT_TYPE_NOT_NULL,
                                                                lib::is_oracle_mode()))) {
    LOG_WARN("create cons name automatically failed", K(ret));
  } else if (OB_FAIL(add_not_null_constraint(column, cst_name, true, cst, allocator, stmt))) {
    LOG_WARN("add not null constraint", K(ret));
  } else {
    column.add_not_null_cst();
  }
  LOG_WARN("end add default not null cst", K(column));
  return ret;
}

int ObDDLResolver::add_not_null_constraint(ObColumnSchemaV2 &column,
                                           const ObString &cst_name,
                                           bool is_sys_generate_name,
                                           ObConstraint &cst,
                                           ObIAllocator &allocator,
                                           ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(stmt));
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_INVALID_ID == column.get_column_id())) {
    bool is_alter_add_column = false;
    if (stmt::T_ALTER_TABLE == stmt->get_stmt_type()) {
      AlterColumnSchema &alter_col_schema = static_cast<AlterColumnSchema &>(column);
      if (OB_DDL_ADD_COLUMN == alter_col_schema.alter_type_) {
        is_alter_add_column = true;
      }
    }
    // column id is unknown only if executing "alter table add column", column id will be added by RS.
    if (OB_UNLIKELY(! is_alter_add_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column id", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(cst_name.empty() || cst_name.length() > OB_MAX_CONSTRAINT_NAME_LENGTH_ORACLE)) {
    ret = OB_ERR_TOO_LONG_IDENT;
    LOG_WARN("constraint_name length overflow", K(ret), K(cst_name.length()));
  } else if (OB_FAIL(cst.assign_not_null_cst_column_id(column.get_column_id()))) {
    LOG_WARN("assign column ids failed", K(ret));
  } else if (OB_FAIL(cst.set_constraint_name(cst_name))) {
    LOG_WARN("failed to set constraint name", K(ret));
  } else {
    cst.set_tenant_id(column.get_tenant_id());
    cst.set_table_id(column.get_table_id());
    cst.set_name_generated_type(is_sys_generate_name ? GENERATED_TYPE_SYSTEM : GENERATED_TYPE_USER);
    cst.set_constraint_type(CONSTRAINT_TYPE_NOT_NULL);
    cst.set_check_expr(column.get_column_name_str());
    const ObString &column_name = column.get_column_name_str();
    ObString expr_str;
    if (OB_FAIL(ObResolverUtils::create_not_null_expr_str(column_name, allocator, expr_str, is_oracle_mode()))) {
      LOG_WARN("create not null expr string failed", K(ret));
    } else {
      cst.set_check_expr(expr_str);
      LOG_DEBUG("set not null check expr", K(expr_str));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (stmt::T_CREATE_TABLE == stmt->get_stmt_type()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt);
    ObSEArray<ObConstraint, 4> &csts = create_table_stmt->get_create_table_arg().constraint_list_;
    for (uint64_t i = 0; OB_SUCC(ret) && i < csts.count(); ++i) {
      if (csts.at(i).get_constraint_name_str() == cst_name) {
        ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
        LOG_WARN("duplicate check constraint name", K(ret), K(cst_name));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(csts.push_back(cst))) {
      LOG_WARN("push back cst failed", K(ret));
    }
  } else if (stmt::T_ALTER_TABLE == stmt->get_stmt_type()) {
    ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt);
    AlterTableSchema &alter_table_schema = alter_table_stmt->
                                          get_alter_table_arg().alter_table_schema_;
    for (ObTableSchema::const_constraint_iterator iter = alter_table_schema.constraint_begin();
          OB_SUCC(ret) && iter != alter_table_schema.constraint_end(); iter ++) {
      if ((*iter)->get_constraint_name_str() == cst_name) {
        ret = OB_ERR_CONSTRAINT_NAME_DUPLICATE;
        LOG_WARN("duplicate check constraint name", K(ret), K(cst_name));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alter_table_schema.add_constraint(cst))) {
      LOG_WARN("push back cst failed", K(ret));
    } else {
      alter_table_stmt->get_alter_table_arg().alter_constraint_type_ =
                          obrpc::ObAlterTableArg::ADD_CONSTRAINT;
    }
    LOG_DEBUG("alter, add not null constraint", K(cst), K(alter_table_schema));
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "unexpected stmt type", K(ret), K(stmt->get_stmt_type()));
  }
  return ret;
}

int ObDDLResolver::create_name_for_empty_partition(const bool is_subpartition,
                                                   ObIArray<ObPartition> &partitions,
                                                   ObIArray<ObSubPartition> &subpartitions)
{
  int ret = OB_SUCCESS;
  if (is_subpartition) {
    if (OB_FAIL(create_name_for_empty_partition(subpartitions))) {
      LOG_WARN("failed to create name for empty subpartitions", K(ret));
    }
  } else if (OB_FAIL(create_name_for_empty_partition(partitions))) {
    LOG_WARN("failed to create name for empty partitions", K(ret));
  }
  return ret;
}

int ObDDLResolver::store_part_key(const ObTableSchema &table_schema,
                                  obrpc::ObCreateIndexArg &index_arg)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> part_key_ids;
  ObSEArray<uint64_t, 4> index_rowkey_ids;
  const ObColumnSchemaV2 *column = NULL;
  const bool check_column_exists = false;
  const bool is_hidden = true;
  if (OB_UNLIKELY(!table_schema.is_partitioned_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected partition level", K(ret), K(table_schema.get_part_level()));
  } else if (OB_FAIL(table_schema.get_partition_key_info().get_column_ids(part_key_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level() &&
             OB_FAIL(table_schema.get_subpartition_key_info().get_column_ids(part_key_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg.index_columns_.count(); ++i) {
    const ObColumnSortItem &column_item = index_arg.index_columns_.at(i);
    if (OB_FAIL(index_rowkey_ids.push_back(column_item.column_id_))) {
      LOG_WARN("failed to push back column id", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_ids.count(); ++i) {
    uint64_t column_id = part_key_ids.at(i);
    if (ObOptimizerUtil::find_item(index_rowkey_ids, column_id)) {
      // part key already in index rowkey, do nothing
    } else if (OB_ISNULL(column = table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(add_storing_column(column->get_column_name_str(),
                                          check_column_exists,
                                          is_hidden))) {
      LOG_WARN("failed to add storing column", K(ret));
    }
  }
  LOG_TRACE("store part key", K(hidden_store_column_names_));
  return ret;
}

bool ObDDLResolver::is_column_exists(ObIArray<ObColumnNameWrapper> &sort_column_array,
                                     ObColumnNameWrapper &column_key,
                                     bool check_prefix_len)
{
  int bret = false;
  for (int64_t i = 0; !bret && i < sort_column_array.count(); ++i) {
    if (check_prefix_len) {
      if (sort_column_array.at(i).all_equal(column_key)) {
        bret = true;
      }
    } else if (sort_column_array.at(i).name_equal(column_key)) {
      bret = true;
    }
  }
  return bret;
}

int ObDDLResolver::get_enable_split_partition(const int64_t tenant_id, bool &enable_split_partition)
{
  int ret = OB_SUCCESS;
  enable_split_partition = false;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id));
  }
  return ret;
}

int ObDDLResolver::get_row_store_type(const uint64_t tenant_id,
                                      const ObStoreFormatType store_format,
                                      ObRowStoreType &row_store_type)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  row_store_type = ObRowStoreType::MAX_ROW_STORE;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", K(ret), K(tenant_id));
  } else if ((ObStoreFormatType::OB_STORE_FORMAT_COMPRESSED_MYSQL == store_format) &&
      (compat_version < DATA_VERSION_4_3_0_0)) {
    row_store_type = ObRowStoreType::ENCODING_ROW_STORE;
  } else if ((ObStoreFormatType::OB_STORE_FORMAT_ARCHIVE_HIGH_ORACLE == store_format) &&
      (compat_version < DATA_VERSION_4_3_0_0)) {
    // not support before DATA_VERSION_4_3_0_0
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ARCHIVE_HIGH_ORACL not supported before DATA_VERSION_4_3_0_0", K(ret), K(compat_version));
  } else if (OB_UNLIKELY((row_store_type = ObStoreFormat::get_row_store_type(store_format))
      >= ObRowStoreType::MAX_ROW_STORE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected row_store_type", K(ret), K(compat_version), K(store_format));
  }
  return ret;
}

int ObDDLResolver::resolve_partition_name(ParseNode *partition_name_node,
                                          ObString &partition_name,
                                          ObBasePartition &partition)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_name_node) || OB_ISNULL(partition_name_node->children_[NAMENODE])) {
    if (is_oracle_mode()) {
      partition.set_is_empty_partition_name(true);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition expr list node is null", K(ret));
    }
  } else {
    ParseNode *name_node = partition_name_node->children_[NAMENODE];
    partition_name.assign_ptr(name_node->str_value_, static_cast<int32_t>(name_node->str_len_));
    if (OB_FAIL(ObSQLUtils::check_ident_name(CS_TYPE_UTF8MB4_GENERAL_CI,
                                             partition_name,
                                             false, //check_for_path_chars
                                             OB_MAX_PARTITION_NAME_LENGTH))) {
      if (OB_ERR_WRONG_IDENT_NAME == ret) {
        if (lib::is_oracle_mode()) {
          // It allows the last char of partition name is space in oracle mode
          ret = OB_SUCCESS;
        } else {
          ret = OB_WRONG_PARTITION_NAME;
          LOG_WARN("get wrong partition name", K(partition_name), K(ret));
          LOG_USER_ERROR(OB_WRONG_PARTITION_NAME, partition_name.length(), partition_name.ptr());
        }
      } else if (OB_ERR_TOO_LONG_IDENT == ret) {
        LOG_WARN("partition name is too long", K(partition_name.length()), K(ret));
      } else {
        LOG_WARN("fail to check ident name", K(partition_name), K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(partition.set_part_name(partition_name))) {
      LOG_WARN("failed to set part name", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_tablespace_id(ParseNode *node, int64_t &tablespace_id)
{
  int ret = OB_SUCCESS;
  tablespace_id = OB_INVALID_ID;
  if (OB_ISNULL(node)) {
    // use default tablespace id
  } else if (node->num_child_ == 1) {
    // tablespace node
    if (T_TABLESPACE == node->type_ && OB_FAIL(resolve_tablespace_node(node, tablespace_id))) {
      LOG_WARN("fail to resolve tablespace node", K(ret));
    }
  } else {
    // physical attributes options
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
      if (OB_ISNULL(node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_TABLESPACE == node->children_[i]->type_ &&
                 OB_FAIL(resolve_tablespace_node(node->children_[i], tablespace_id))) {
        LOG_WARN("fail to resolve tablespace node", K(ret));
      }
    }
  }
  return ret;
}

// resolve hash/key partition basic infos, include:
// 1. partition function type
// 2. partition function expr name
// 3. partition keys (set in this function)
int ObDDLResolver::resolve_hash_or_key_partition_basic_infos(ParseNode *node,
                                                             bool is_subpartition,
                                                             ObTableSchema &table_schema,
                                                             ObPartitionFuncType &part_func_type,
                                                             ObString &func_expr_name)
{
  int ret = OB_SUCCESS;
  ParseNode *partition_fun_node = NULL;
  part_func_type = PARTITION_FUNC_TYPE_HASH;
  const share::schema::ObTablegroupSchema *tablegroup_schema = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(node->children_) ||
      OB_ISNULL(partition_fun_node = node->children_[HASH_FUN_EXPR_NODE]) ||
      OB_ISNULL(schema_checker_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(partition_fun_node),
                                    K(schema_checker_), K(session_info_));
  } else if (!table_schema.get_tablegroup_name().empty() &&
             OB_FAIL(schema_checker_->get_tablegroup_schema(session_info_->get_effective_tenant_id(),
                                                            table_schema.get_tablegroup_name(),
                                                            tablegroup_schema))) {
    LOG_WARN("fail to get tablegroup schema", K(ret), K(table_schema.get_tablegroup_name()));
  } else {
    part_func_type = T_HASH_PARTITION == node->type_
          ? share::schema::PARTITION_FUNC_TYPE_HASH
          : share::schema::PARTITION_FUNC_TYPE_KEY;
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObString, 4> partition_keys;
    if (NULL == partition_fun_node->children_[1]) { //partition by key()
      part_func_type = share::schema::PARTITION_FUNC_TYPE_KEY_IMPLICIT;
      if (OB_FAIL(build_partition_key_info(table_schema, partition_keys, part_func_type))) {
        LOG_WARN("failed to build partition key info", K(ret), K(table_schema));
      }
    } else {
      ObSEArray<ObRawExpr *, 4> dummy_part_func_exprs;
      if (OB_FAIL(resolve_part_func(params_, partition_fun_node, part_func_type,
                                    table_schema, dummy_part_func_exprs, partition_keys))) {
        LOG_WARN("failed to resolve part func", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
       if (OB_FAIL(set_partition_keys(table_schema, partition_keys, is_subpartition))) {
        LOG_WARN("failed to set partition keys", K(ret), K(table_schema), K(is_subpartition));
      }
    }
  }

  if (OB_SUCC(ret)) {
    func_expr_name.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                  *allocator_, session_info_->get_dtc_params(), func_expr_name))) {
      LOG_WARN("fail to copy and convert string charset", K(ret));
    } else if (!lib::is_oracle_mode()) {
      //TODO(yaoying.yyy:maybe not valid here, if expr include table name or databae name)
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, func_expr_name);
    }

    //TODO now, just for compat inner_table process.
    if (OB_FAIL(ret)) {
    } else if (is_inner_table(table_id_)) {
      // 这里为什么先获取part str再改变part func type?
      ObSqlString part_expr;
      bool is_oracle_mode = lib::is_oracle_mode();
      const uint64_t tenant_id = session_info_->get_effective_tenant_id();
      if (static_cast<int64_t>(table_id_) > 0
          && OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(
                     tenant_id, table_id_, is_oracle_mode))) {
        LOG_WARN("fail to check oralce mode", KR(ret), K(tenant_id), K_(table_id));
      } else if (OB_FAIL(get_part_str_with_type(is_oracle_mode, part_func_type, func_expr_name, part_expr))) {
        SQL_RESV_LOG(WARN, "Failed to get part str with type", K(ret));
      } else if (OB_FAIL(ob_write_string(*allocator_, part_expr.string(), func_expr_name))) {
        LOG_WARN("failed to copy string", K(part_expr.string()));
      } else if (PARTITION_FUNC_TYPE_KEY == part_func_type) {
        part_func_type = PARTITION_FUNC_TYPE_KEY;
      }
    }
  }
  return ret;
}

// resolve range partition basic infos, include:
// 1. partition function type
// 2. partition function expr name
// 3. partition function exprs
// 4. partition keys (set in this function)
int ObDDLResolver::resolve_range_partition_basic_infos(ParseNode *node,
                                                       bool is_subpartition,
                                                       ObTableSchema &table_schema,
                                                       ObPartitionFuncType &part_func_type,
                                                       ObString &func_expr_name,
                                                       ObIArray<ObRawExpr*> &part_func_exprs)
{
  int ret = OB_SUCCESS;
  ParseNode *partition_fun_node = NULL;
  part_func_type = PARTITION_FUNC_TYPE_RANGE;
  ObSEArray<ObString, 4> partition_keys;
  if (OB_ISNULL(node) || OB_ISNULL(partition_fun_node = node->children_[RANGE_FUN_EXPR_NODE])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(partition_fun_node));
  } else if (T_RANGE_COLUMNS_PARTITION == node->type_) {
    part_func_type = PARTITION_FUNC_TYPE_RANGE_COLUMNS;
  }

  if (OB_SUCC(ret)) {
    func_expr_name.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                  *allocator_, session_info_->get_dtc_params(), func_expr_name))) {
      LOG_WARN("fail to copy and convert string charset", K(ret));
    } else if (OB_FAIL(resolve_part_func(params_, partition_fun_node, part_func_type,
                                         table_schema, part_func_exprs, partition_keys))) {
      LOG_WARN("resolve part func failed", K(ret));
    } else if (OB_FAIL(set_partition_keys(table_schema, partition_keys, is_subpartition))) {
      LOG_WARN("Failed to set partition keys", K(ret), K(table_schema), K(is_subpartition));
    }
  }
  return ret;
}

// resolve list partition basic infos, include:
// 1. partition function type
// 2. partition function expr name
// 3. partition function exprs
// 4. partition keys (set in this function)
int ObDDLResolver::resolve_list_partition_basic_infos(ParseNode *node,
                                                      bool is_subpartition,
                                                      ObTableSchema &table_schema,
                                                      ObPartitionFuncType &part_func_type,
                                                      ObString &func_expr_name,
                                                      ObIArray<ObRawExpr*> &part_func_exprs)
{
  int ret = OB_SUCCESS;
  ParseNode *partition_fun_node = NULL;
  part_func_type = PARTITION_FUNC_TYPE_LIST;
  ObSEArray<ObString, 4> partition_keys;
  if (OB_ISNULL(node) || OB_ISNULL(partition_fun_node = node->children_[LIST_FUN_EXPR_NODE])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(partition_fun_node));
  } else if (T_LIST_COLUMNS_PARTITION == node->type_) {
    part_func_type = PARTITION_FUNC_TYPE_LIST_COLUMNS;
  }

  if (OB_SUCC(ret)) {
    func_expr_name.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                  *allocator_, session_info_->get_dtc_params(), func_expr_name))) {
      LOG_WARN("fail to copy and convert string charset", K(ret));
    } else if (OB_FAIL(resolve_part_func(params_, partition_fun_node, part_func_type,
                                         table_schema, part_func_exprs, partition_keys))) {
      LOG_WARN("resolve part func failed", K(ret));
    } else if (OB_FAIL(set_partition_keys(table_schema, partition_keys, is_subpartition))) {
      LOG_WARN("Failed to set partition keys", K(ret), K(table_schema), K(is_subpartition));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_partition_node(ObPartitionedStmt *stmt,
                                          ParseNode *part_node,
                                          ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (is_hash_type_partition(part_node->type_)) {
    ret = resolve_partition_hash_or_key(stmt, part_node, false, table_schema);
  } else if (is_range_type_partition(part_node->type_)) {
    ret = resolve_partition_range(stmt, part_node, false, table_schema);
  } else if (is_list_type_partition(part_node->type_)) {
    ret = resolve_partition_list(stmt, part_node, false, table_schema);
  } else {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "node type is invalid.", K(ret), K(part_node->type_));
  }

  if (OB_SUCC(ret) && !common::is_virtual_table(table_id_)) {
    int64_t partnum = 0;
    if (stmt->use_def_sub_part()) {
      partnum = table_schema.get_partition_num() * table_schema.get_def_sub_part_num();
    } else {
      partnum = table_schema.get_all_part_num();
    }
    if (partnum > (lib::is_oracle_mode()
        ? common::OB_MAX_PARTITION_NUM_ORACLE :
          ObResolverUtils::get_mysql_max_partition_num(table_schema.get_tenant_id()))) {
      ret = common::OB_TOO_MANY_PARTITIONS_ERROR;
    }
  }

  if (OB_SUCC(ret) && !table_schema.is_external_table()) {
    if (OB_FAIL(check_and_set_partition_names(stmt, table_schema))) {
      LOG_WARN("failed to check and set partition names", K(ret));
    }
  }

  return ret;
}

int ObDDLResolver::resolve_subpartition_option(ObPartitionedStmt *stmt,
                                               ParseNode *subpart_node,
                                               ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (is_hash_type_partition(subpart_node->type_)) {
    if (OB_FAIL(resolve_partition_hash_or_key(stmt, subpart_node, true, table_schema))) {
      SQL_RESV_LOG(WARN, "failed to resolve partition hash or key", K(ret));
    }
  } else if (is_range_type_partition(subpart_node->type_)) {
    if (OB_FAIL(resolve_partition_range(stmt, subpart_node, true, table_schema))) {
      SQL_RESV_LOG(WARN, "failed to resolve partition range", K(ret));
    }
  } else if (is_list_type_partition(subpart_node->type_)) {
    if (OB_FAIL(resolve_partition_list(stmt, subpart_node, true, table_schema))) {
      SQL_RESV_LOG(WARN, "failed to resolve partition list", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected subpartition type", K(ret), K(subpart_node->type_));
  }
  return ret;
}

int ObDDLResolver::resolve_individual_subpartition(ObPartitionedStmt *stmt,
                                                   ParseNode *part_node,
                                                   ParseNode *partition_list_node,
                                                   ParseNode *subpart_node,
                                                   share::schema::ObTableSchema &table_schema,
                                                   bool &force_template)
{
  int ret = OB_SUCCESS;
  share::schema::ObPartitionFuncType partition_func_type = share::schema::PARTITION_FUNC_TYPE_HASH;
  ObString func_expr_name;
  common::ObSEArray<ObRawExpr*, 8> part_func_exprs;
  table_schema.set_part_level(share::schema::PARTITION_LEVEL_TWO);
  force_template = false;

  if (OB_ISNULL(stmt) || OB_ISNULL(part_node) || OB_ISNULL(subpart_node)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected null", K(ret), K(stmt), K(part_node), K(subpart_node));
  } else if (is_range_type_partition(subpart_node->type_)) {
    // range subpartition
    if (OB_FAIL(resolve_range_partition_basic_infos(subpart_node, true, table_schema,
                                                    partition_func_type, func_expr_name,
                                                    part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to resolve range partition basic infos", K(ret));
    } else if (OB_FAIL(stmt->get_subpart_fun_exprs().assign(part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
    }
  } else if (is_list_type_partition(subpart_node->type_)) {
    // list subpartition
    if (OB_FAIL(resolve_list_partition_basic_infos(subpart_node, true, table_schema,
                                                   partition_func_type, func_expr_name,
                                                   part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to resolve list partition basic infos", K(ret));
    } else if (OB_FAIL(stmt->get_subpart_fun_exprs().assign(part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
    }
  } else if (is_hash_type_partition(subpart_node->type_)) {
    /**
     * 通过subpartitios number的方式定义的hash分区，本质上是非模板化的语意，但在2.2.70之前的版本都是按照模板化
     * 的语意处理的。备份恢复要求在低版本上的备份，能在高版本上恢复。为了兼容这一点，对于通过subpartitios number
     * 方式定义的hash分区，需要检查一级分区的定义中是否存在定义非模板化二级分区的行为，如果存在则认为是非模板化二
     * 级分区，否则认为是模板化的二级分区。
     * e.g. 如下t1认为是一个模板化二级分区表，t2认为是一个非模板化的二级分区表
     *  create table t1 (c1 int, c2 int) partition by range (c1)
     *  subpartition by hash (c2) subpartitions 3
     *  (
     *    partition p0 values less than (100),
     *    partition p1 values less than (200)
     *  );
     *  create table t2 (c1 int, c2 int) partition by range (c1)
     *  subpartition by hash (c2) subpartitions 3
     *  (
     *    partition p0 values less than (100)
     *    (
     *      subpartition p0_h1,
     *      subpartition p0_h2
     *    ),
     *    partition p1 values less than (200)
     *  );
     */
    if (NULL != partition_list_node) {
      bool has_def_subpart = false;
      for (int64_t i = 0; OB_SUCC(ret) && !has_def_subpart && i < partition_list_node->num_child_; ++i) {
        if (OB_ISNULL(partition_list_node->children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (NULL != partition_list_node->children_[i]->children_[ELEMENT_SUBPARTITION_NODE]) {
          has_def_subpart = true;
        }
      }
      if (OB_SUCC(ret) && !has_def_subpart) {
        force_template = true;
      }
    } else {
      // partition_list_node为空说明一级分区是通过partitions定义的hash分区，直接设置成模板化的二级分区
      force_template = true;
    }

    if (OB_SUCC(ret) && !force_template) {
      if (OB_FAIL(resolve_hash_or_key_partition_basic_infos(subpart_node, true, table_schema,
                                                            partition_func_type, func_expr_name))) {
        SQL_RESV_LOG(WARN, "failed to resolve hash or key parititon basic infos", K(ret));
      } else if (NULL != subpart_node->children_[HASH_PARTITION_NUM_NODE]) {
        hash_subpart_num_ = subpart_node->children_[HASH_PARTITION_NUM_NODE]->value_;
        if (hash_subpart_num_ <= 0) {
          ret = common::OB_NO_PARTS_ERROR;
          LOG_USER_ERROR(OB_NO_PARTS_ERROR);
        }
      } else {
        hash_subpart_num_ = 1;
      }
    }
  }

  if (OB_SUCC(ret) && !force_template) {
    table_schema.get_sub_part_option().set_part_func_type(partition_func_type);
    if (OB_FAIL(table_schema.get_sub_part_option().set_part_expr(func_expr_name))) {
      SQL_RESV_LOG(WARN, "set partition express string failed", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_subpartition_elements(ObPartitionedStmt *stmt,
                                                 ParseNode *node,
                                                 ObTableSchema &table_schema,
                                                 ObPartition *partition,
                                                 bool in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObPartitionFuncType subpart_type = table_schema.get_sub_part_option().get_part_func_type();
  if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(node)) {
    if (is_range_part(subpart_type)) {
      ObDDLStmt::array_t range_value_exprs;
      OZ (generate_default_range_subpart(stmt,
                                         OB_INVALID_ID,
                                         table_schema,
                                         partition,
                                         range_value_exprs));
      OZ (stmt->get_individual_subpart_values_exprs().push_back(range_value_exprs));
    } else if (is_list_part(subpart_type)) {
      ObDDLStmt::array_t list_value_exprs;
      OZ (generate_default_list_subpart(stmt,
                                        OB_INVALID_ID,
                                        table_schema,
                                        partition,
                                        list_value_exprs));
      OZ (stmt->get_individual_subpart_values_exprs().push_back(list_value_exprs));
    } else {
      if (hash_subpart_num_ < 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("default subpart num not specified", K(ret), K(hash_subpart_num_));
      } else if (OB_FAIL(generate_default_hash_subpart(stmt,
                                                       hash_subpart_num_,
                                                       OB_INVALID_ID, // tablespace_id
                                                       table_schema,
                                                       partition))) {
        LOG_WARN("failed to generate default hash subpart", K(ret));
      }
    }
  } else if ((is_hash_like_part(subpart_type) && node->type_ != T_HASH_SUBPARTITION_LIST) ||
             (is_range_part(subpart_type) && node->type_ != T_RANGE_SUBPARTITION_LIST) ||
             (is_list_part(subpart_type) && node->type_ != T_LIST_SUBPARTITION_LIST)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subpartition define is not match declare", K(ret), K(subpart_type), K(node->type_));
  } else if (T_HASH_SUBPARTITION_LIST == node->type_) {
    if (OB_FAIL(resolve_hash_subpartition_elements(stmt, node, table_schema, partition))) {
      LOG_WARN("failed to resolve hash subpartition elements", K(ret));
    }
  } else if (T_RANGE_SUBPARTITION_LIST == node->type_) {
    ObDDLStmt::array_t range_value_exprs;
    if (OB_FAIL(resolve_range_subpartition_elements(stmt, node, table_schema, partition,
                                                    subpart_type, stmt->get_subpart_fun_exprs(),
                                                    range_value_exprs, in_tablegroup))) {
      LOG_WARN("failed to resolve hash subpartition elements", K(ret));
    } else if (OB_FAIL(stmt->get_individual_subpart_values_exprs().push_back(range_value_exprs))) {
      LOG_WARN("failed to push back range value exprs");
    }
  } else if (T_LIST_SUBPARTITION_LIST == node->type_) {
    ObDDLStmt::array_t list_value_exprs;
    if (OB_FAIL(resolve_list_subpartition_elements(stmt, node, table_schema, partition,
                                                   subpart_type, stmt->get_subpart_fun_exprs(),
                                                   list_value_exprs, in_tablegroup))) {
      LOG_WARN("failed to resolve hash subpartition elements", K(ret));
    } else if (OB_FAIL(stmt->get_individual_subpart_values_exprs().push_back(list_value_exprs))) {
      LOG_WARN("failed to push back range value exprs");
    }
  }
  return ret;
}

int ObDDLResolver::resolve_partition_hash_or_key(
    ObPartitionedStmt *stmt,
    ParseNode *node,
    const bool is_subpartition,
    ObTableSchema &table_schema)
{
  int ret = common::OB_SUCCESS;
  ObString partition_expr;
  ObPartitionFuncType partition_func_type = PARTITION_FUNC_TYPE_HASH;
  int64_t partition_num = 0;
  ObPartitionOption *partition_option = NULL;

  if (OB_ISNULL(node) || OB_ISNULL(node->children_) || !is_hash_type_partition(node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected node", K(ret), K(node));
  } else if (is_subpartition && NULL != node->children_[HASH_SUBPARTITIOPPN_NODE]) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpartition cannot has another subpartition", K(ret));
  } else if (is_subpartition) {
    partition_option = &(table_schema.get_sub_part_option());
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_TWO);
  } else {
    partition_option = &(table_schema.get_part_option());
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
  }

  // 判断二级分区是否是模板化的
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[HASH_SUBPARTITIOPPN_NODE]) {
      bool force_template = false;
      if (NULL != node->children_[HASH_SUBPARTITIOPPN_NODE]->children_[HASH_TEMPLATE_MARK]) {
        // 模板化的二级分区定义后面再解析
        stmt->set_use_def_sub_part(true);
      } else if (OB_FAIL(resolve_individual_subpartition(stmt, node,
                                                         node->children_[HASH_PARTITION_LIST_NODE],
                                                         node->children_[HASH_SUBPARTITIOPPN_NODE],
                                                         table_schema, force_template))) {
        SQL_RESV_LOG(WARN, "failed to resolve individual subpartition", K(ret));
      } else if (force_template) {
        stmt->set_use_def_sub_part(true);
      } else {
        stmt->set_use_def_sub_part(false);
      }
    }
  }

  // 1. resolve partition type, partition keys, partition expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_hash_or_key_partition_basic_infos(node, is_subpartition, table_schema,
                                                          partition_func_type, partition_expr))) {
      SQL_RESV_LOG(WARN, "failed to resolve hash or key parititon basic infos", K(ret));
    } else {
      partition_option->set_part_func_type(partition_func_type);
      if (OB_FAIL(partition_option->set_part_expr(partition_expr))) {
        SQL_RESV_LOG(WARN, "set partition express string failed", K(ret));
      }
    }
  }

  // 2. 处理每个分区的定义
  if (OB_SUCC(ret)) {
    /**
     * 1. 只定义了分区数(隐式定义)
     *    自动生成分区名, 按照p0, p1, p2, ...的顺序
     * 2. 只定义了分区名(显式定义)
     *    按照定义的分区名生成对应数量的分区
     * 3. 同时使用了隐式定义和显式定义
     *    mysql:
     *      一级分区: 按照显示定义的分区名生成各个分区, 需要检查显式定义的分区数等于隐式定义的分区数
     *      二级分区: 1. 以模板的形式显式定义二级分区时, 不允许同时定义
     *               2. 以非模板的形式显式定义二级分区时, 允许同时定义, 需要检查显式定义的分区数等于隐式定义的分区数
     *    oracle:
     *      一级分区: 不允许同时定义
     *      二级分区: 1. 以模板的形式显式定义二级分区时, 不允许同时定义
     *               2. 以非模板的形式显式定义二级分区时, 允许同时定义, 以显示定义的分区数为准
     * 4. 即没有显示定义, 也没有隐式定义
     *    认为隐式定义了一个分区, 即`partitions 1`
     */
    if (NULL != node->children_[HASH_PARTITION_NUM_NODE]) {
      // case 1, 3: 分区数先初始化为隐式定义的数量
      partition_num = node->children_[HASH_PARTITION_NUM_NODE]->value_;
    } else if (NULL == node->children_[HASH_PARTITION_LIST_NODE]) {
      // case 4: 分区数为1
      partition_num = 1;
    } else {
      // case 2: 分区数先初始化为1
      partition_num = 1;
    }

    if (NULL != node->children_[HASH_PARTITION_NUM_NODE] &&
        NULL != node->children_[HASH_PARTITION_LIST_NODE]) {
      if (is_oracle_mode()) {
        ret = OB_ERR_SPECIFIY_PARTITION_DESCRIPTION;
      } else if (partition_num != node->children_[HASH_PARTITION_LIST_NODE]->num_child_) {
        ret = OB_ERR_PARSE_PARTITION_RANGE;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (partition_num <= 0) {
      ret = common::OB_NO_PARTS_ERROR;
      LOG_USER_ERROR(OB_NO_PARTS_ERROR);
    } else if (!common::is_virtual_table(table_id_) &&
               partition_num > (lib::is_oracle_mode()
                  ? common::OB_MAX_PARTITION_NUM_ORACLE
                  : sql::ObResolverUtils::get_mysql_max_partition_num(table_schema.get_tenant_id()))) {
      ret = common::OB_TOO_MANY_PARTITIONS_ERROR;
    } else if (is_subpartition) {
      if (NULL != node->children_[HASH_PARTITION_LIST_NODE]) {
        // 解析显式定义的分区
        if (OB_FAIL(resolve_hash_subpartition_elements(stmt,
                                                       node->children_[HASH_PARTITION_LIST_NODE],
                                                       table_schema,
                                                       NULL/* dummy partition*/))) {
          SQL_RESV_LOG(WARN, "failed to resolve hash subpartition elements", K(ret));
        }
      } else {
        // 只存在隐式定义, 自动生成分区名
        int64_t tablespace_id = OB_INVALID_ID;
        if (OB_FAIL(resolve_tablespace_id(node->children_[HASH_TABLESPACE_NODE], tablespace_id))) {
          LOG_WARN("failed to resolve tablespace", K(ret));
        } else if (OB_FAIL(generate_default_hash_subpart(stmt,
                                                         partition_num,
                                                         tablespace_id,
                                                         table_schema,
                                                         NULL))) { // dummy partition
          LOG_WARN("failed to generate default hash part", K(ret));
        }
      }
    } else {
      if (NULL != node->children_[HASH_PARTITION_LIST_NODE]) {
        // 解析显式定义的分区
        if (OB_FAIL(resolve_hash_partition_elements(stmt,
                                                    node->children_[HASH_PARTITION_LIST_NODE],
                                                    table_schema))) {
          LOG_WARN("failed to resolve hash partition elements", K(ret));
        }
      } else {
        // 只存在隐式定义, 自动生成分区名
        int64_t tablespace_id = OB_INVALID_ID;
        if (OB_FAIL(resolve_tablespace_id(node->children_[HASH_TABLESPACE_NODE], tablespace_id))) {
          LOG_WARN("failed to resolve tablespace", K(ret));
        } else if (OB_FAIL(generate_default_hash_part(partition_num,
                                                      tablespace_id,
                                                      table_schema))) {
          LOG_WARN("failed to generate default hash part", K(ret));
        }
        if (!stmt->use_def_sub_part()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_first_part_num(); ++i) {
            if (OB_FAIL(resolve_subpartition_elements(stmt,
                                                      NULL, // dummy node
                                                      table_schema,
                                                      table_schema.get_part_array()[i],
                                                      false))) {
              LOG_WARN("failed to resolve subpartition elements", K(ret));
            }
          }
        }
      }
    }
  }

  // 6. 解析模板化二级分区定义
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[HASH_SUBPARTITIOPPN_NODE] && stmt->use_def_sub_part()) {
      if (OB_FAIL(resolve_subpartition_option(stmt, node->children_[HASH_SUBPARTITIOPPN_NODE], table_schema))) {
        SQL_RESV_LOG(WARN, "failed to resolve subpartition", K(ret));
      }
    }
  }
  return ret;
}

/*
  4.1 检查interval_expr是否是立即数或者，1+1 不算
  4.2 expr的类型是否和col匹配 否则 ORA-14752
*/
int ObDDLResolver::resolve_interval_node(ObResolverParams &params,
                                         ParseNode *interval_node,
                                         common::ColumnType &col_dt,
                                         int64_t precision,
                                         int64_t scale,
                                         ObRawExpr *&interval_value_expr_out)
{
  int ret = OB_SUCCESS;
  ParseNode * expr_node;

  CK (NULL != interval_node);
  expr_node = interval_node->children_[0];;
  CK (NULL != expr_node);

  if (OB_SUCC(ret)) {
    if (expr_node->type_ == T_NULL) {
      ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
    } else {
      ObRawExpr *interval_value_expr = NULL;
      OZ (ObResolverUtils::resolve_partition_range_value_expr(params, *expr_node, "interval_part",
                                             PARTITION_FUNC_TYPE_RANGE_COLUMNS,
                                             interval_value_expr, false, true));
      if (ret == OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED) {
        ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
      } else if (!OB_SUCC(ret)) {
        ret = OB_WRONG_COLUMN_NAME;
        LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int>(interval_node->str_len_),
                       interval_node->str_value_);
      } else {
        common::ObObjType expr_type = interval_value_expr->get_data_type();
        switch (col_dt) {
          case ObIntType:
          case ObFloatType:
          case ObDoubleType:
          case ObNumberFloatType:
          case ObNumberType: {
            if (expr_type != ObIntType && expr_type != ObFloatType
               && expr_type != ObNumberType && expr_type != ObDoubleType) {
              ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
              LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), K(ret));
            } else if (col_dt == ObNumberType) {
              ObAccuracy acc;
              acc.set_precision(precision);
              acc.set_scale(scale);
              interval_value_expr->set_accuracy(acc);
            }
            if (OB_SUCC(ret)) {
              ParamStore dummy_params;
              ObRawExprFactory expr_factory(*(params.allocator_));
              ObObj out_val;
              ObRawExpr *sign_expr = NULL;
              OZ (ObRawExprUtils::build_sign_expr(expr_factory, interval_value_expr, sign_expr));
              OZ (sign_expr->formalize(params.session_info_));
              OZ (ObSQLUtils::calc_simple_expr_without_row(params.session_info_,
                                                          sign_expr, out_val,
                                                          &dummy_params, *(params.allocator_)));

              if (OB_FAIL(ret)) {
                // do nothing
              } else if (out_val.is_negative_number()) {
                ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
                LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), K(ret));
              }
              // if (OB_FAIL(ret)) {
              //   // do nothing
              // } else if (lib::is_oracle_mode()){
              //   if (!out_val.is_number()) {
              //     ret = OB_ERR_UNEXPECTED;
              //     LOG_WARN("expected number type, but actually is not", K(ret));
              //   } else {
              //     number::ObNumber res;
              //     OZ (out_val.get_number(res), out_val);
              //     OX (res.is_negative_number)
              //   }
              // }
            }
            break;
          }
          case ObDateTimeType:
          case ObTimestampNanoType: {
            if (expr_type != ObIntervalYMType && expr_type != ObIntervalDSType) {
              ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
              LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), K(ret));
            }
            break;
          }
          default: {
            ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
            LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        interval_value_expr_out = interval_value_expr;
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_interval_expr_low(ObResolverParams &params,
                                             ParseNode *interval_node,
                                             const share::schema::ObTableSchema &table_schema,
                                             ObRawExpr *transition_expr,
                                             ObRawExpr *&interval_value)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col_schema = NULL;
  common::ColumnType col_dt = ObNullType;
  /* 1. interval 分区只支持一个分区键 否则 ORA-14750*/
  if (OB_SUCC(ret)) {
    if (table_schema.get_partition_key_column_num() > 1) {
      ret = OB_ERR_INTERVAL_CLAUSE_HAS_MORE_THAN_ONE_COLUMN;
      SQL_RESV_LOG(WARN, "interval clause has more then one column", K(ret));
    }
  }

  /* 2. interval 分区列只支持数据类型： number, date, float, timestamp。 否则 ORA-14751 */
  if (OB_SUCC(ret)) {
    uint64_t col_id = OB_INVALID_ID;
    ObItemType item_type;
    const ObPartitionKeyInfo &part_key_info = table_schema.get_partition_key_info();

    OZ (part_key_info.get_column_id(0, col_id));
    CK (OB_NOT_NULL(col_schema = table_schema.get_column_schema(col_id)));
    if (OB_SUCC(ret)) {
      col_dt = col_schema->get_data_type();
      if (ObFloatType == col_dt || ObDoubleType == col_dt) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support float or double as interval partition column", K(ret), K(col_dt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "interval partition with float or double type partition column");
      } else if (false == ObResolverUtils::is_valid_oracle_interval_data_type(col_dt, item_type)) {
        ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
        SQL_RESV_LOG(WARN, "invalid interval column data type", K(ret), K(col_dt));
      }
    }
  }
  /* 3. 最大分区不能是maxvalue。否则 ORA-14761 */
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret) && transition_expr->get_data_type() == ObMaxType) {
      ret = OB_ERR_MAXVALUE_PARTITION_WITH_INTERVAL;
      SQL_RESV_LOG(WARN, "interval with maxvalue ", K(ret), K(table_schema.get_table_name()));
    }
  }
  /* 4. 检查inteval的表达式
    4.1 检查是否是立即数，1+1 不算
    4.2 expr的类型是否和col匹配 否则 ORA-14752
  */
  CK (OB_NOT_NULL(col_schema));
  OZ (resolve_interval_node(params, interval_node, col_dt, col_schema->get_accuracy().get_precision(),
                            col_schema->get_accuracy().get_scale(), interval_value));

  return ret;
}

int ObDDLResolver::resolve_interval_clause(ObPartitionedStmt *stmt,
                                           ParseNode *node,
                                           ObTableSchema &table_schema,
                                           common::ObSEArray<ObRawExpr*, 8> &range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(node));
  } else if (node->num_child_ > RANGE_INTERVAL_NODE) {
    /* interval info record in 6th param */
    ParseNode *interval_node = node->children_[RANGE_INTERVAL_NODE];
    ObRawExpr *transition_expr = range_exprs.at(range_exprs.count() - 1);
    ObRawExpr *interval_value = NULL;
    if (OB_ISNULL(transition_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null transition expr", K(ret));
    } else if (NULL == interval_node) {
      // no nothing
    } else if (OB_FAIL(resolve_interval_expr_low(params_, interval_node, table_schema,
                                                 transition_expr, interval_value))) {
      LOG_WARN("failed to resolve interval expr low", K(ret));
    } else {
      stmt->set_interval_expr(interval_value);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_partition_range(ObPartitionedStmt *stmt,
                                           ParseNode *node,
                                           const bool is_subpartition,
                                           ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObString func_expr_name;
  int64_t partition_num = 0;
  share::schema::ObPartitionFuncType part_func_type = share::schema::PARTITION_FUNC_TYPE_RANGE;
  common::ObSEArray<ObRawExpr*, 8> part_func_exprs;
  common::ObSEArray<ObRawExpr*, 8> range_values_exprs;
  share::schema::ObPartitionOption *partition_option = NULL;

  if (OB_ISNULL(stmt) || OB_ISNULL(node) || OB_ISNULL(node->children_) ||
      OB_ISNULL(node->children_[RANGE_ELEMENTS_NODE])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected null", K(ret), K(stmt), K(node));
  } else if (!is_range_type_partition(node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected partition type", K(ret), K(node->type_));
  } else if (is_subpartition) {
    partition_option = &(table_schema.get_sub_part_option());
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_TWO);
  } else {
    partition_option = &(table_schema.get_part_option());
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
  }

  if (OB_SUCC(ret)) {
    if (NULL != node->children_[RANGE_SUBPARTITIOPPN_NODE]) {
      bool force_template = false;
      if (NULL != node->children_[RANGE_SUBPARTITIOPPN_NODE]->children_[RANGE_TEMPLATE_MARK]) {
        // 模板化的二级分区定义后面再解析
        stmt->set_use_def_sub_part(true);
      } else if (OB_FAIL(resolve_individual_subpartition(stmt, node,
                                                         node->children_[RANGE_ELEMENTS_NODE],
                                                         node->children_[RANGE_SUBPARTITIOPPN_NODE],
                                                         table_schema, force_template))) {
        SQL_RESV_LOG(WARN, "failed to resolve individual subpartition", K(ret));
      } else if (force_template) {
        stmt->set_use_def_sub_part(true);
      } else {
        stmt->set_use_def_sub_part(false);
      }
    }
  }

  // 1. resolve partition type, partition keys, partition expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_range_partition_basic_infos(node, is_subpartition, table_schema,
                                                    part_func_type, func_expr_name, part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to resolve range partition basic infos", K(ret));
    } else if (OB_FAIL(partition_option->set_part_expr(func_expr_name))) {
      SQL_RESV_LOG(WARN, "set partition express string failed", K(ret));
    } else {
      partition_option->set_part_func_type(part_func_type);
    }
  }

  // 2. resolve range partition define
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[RANGE_PARTITION_NUM_NODE]) {
      partition_num = node->children_[RANGE_PARTITION_NUM_NODE]->value_;
      if (partition_num != node->children_[RANGE_ELEMENTS_NODE]->num_child_) {
        ret = OB_ERR_PARSE_PARTITION_RANGE;
      }
    } else {
      partition_num = node->children_[RANGE_ELEMENTS_NODE]->num_child_;
    }
    partition_option->set_part_num(partition_num);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_partition_name_duplicate(node->children_[RANGE_ELEMENTS_NODE],
                                                 lib::is_oracle_mode()))) {
        SQL_RESV_LOG(WARN, "duplicate partition name", K(ret));
      } else if (is_subpartition) {
        if (OB_FAIL(resolve_range_subpartition_elements(stmt,
                                                        node->children_[RANGE_ELEMENTS_NODE],
                                                        table_schema,
                                                        NULL, // dummy partition
                                                        part_func_type,
                                                        part_func_exprs,
                                                        range_values_exprs))) {
          SQL_RESV_LOG(WARN, "resolve reange partition elements fail", K(ret));
        }
      } else {
        if (OB_FAIL(resolve_range_partition_elements(stmt,
                                                     node->children_[RANGE_ELEMENTS_NODE],
                                                     table_schema,
                                                     part_func_type,
                                                     part_func_exprs,
                                                     range_values_exprs))) {
          SQL_RESV_LOG(WARN, "resolve reange partition elements fail", K(ret));
        }
      }
    }
  }
  // 2.5 resolve interval clause
  OZ (resolve_interval_clause(stmt, node, table_schema, range_values_exprs));

  // 解析模板化二级分区定义
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[RANGE_SUBPARTITIOPPN_NODE] && stmt->use_def_sub_part()) {
      if (OB_FAIL(resolve_subpartition_option(stmt, node->children_[RANGE_SUBPARTITIOPPN_NODE], table_schema))) {
        SQL_RESV_LOG(WARN, "failed to resolve subpartition", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_subpartition) {
      if (OB_FAIL(stmt->get_subpart_fun_exprs().assign(part_func_exprs))) {
        SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
      } else if (OB_FAIL(stmt->get_template_subpart_values_exprs().assign(range_values_exprs))) {
        SQL_RESV_LOG(WARN, "failed to assign range values exprs", K(ret));
      }
    } else {
      if (OB_FAIL(stmt->get_part_fun_exprs().assign(part_func_exprs))) {
        SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
      } else if (OB_FAIL(stmt->get_part_values_exprs().assign(range_values_exprs))) {
        SQL_RESV_LOG(WARN, "failed to assign range values exprs", K(ret));
      }
    }
    SQL_RESV_LOG(DEBUG, "succ to resolve_partition_range", KPC(stmt), K(part_func_exprs), K(range_values_exprs));
  }
  return ret;
}

int ObDDLResolver::resolve_partition_list(ObPartitionedStmt *stmt,
                                          ParseNode *node,
                                          const bool is_subpartition,
                                          ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObString func_expr_name;
  ObPartitionOption *partition_option = NULL;
  int64_t partition_num = 0;
  ObPartitionFuncType part_func_type = PARTITION_FUNC_TYPE_LIST;
  common::ObSEArray<ObRawExpr*, 8> part_func_exprs;
  ObDDLStmt::array_t list_values_exprs;

  if (OB_ISNULL(stmt) || OB_ISNULL(node) || OB_ISNULL(node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected null", K(ret), K(stmt), K(node));
  } else if (!table_schema.is_external_table() && OB_ISNULL(node->children_[LIST_ELEMENTS_NODE])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected null", K(ret), K(stmt), K(node));
  } else if (!is_list_type_partition(node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "get unexpected partition type", K(ret), K(node->type_));
  } else if (is_subpartition) {
    if (table_schema.is_external_table()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("external table do not support subpartition now", K(ret));
    } else {
      partition_option = &(table_schema.get_sub_part_option());
      table_schema.set_part_level(share::schema::PARTITION_LEVEL_TWO);
    }
  } else {
    partition_option = &(table_schema.get_part_option());
    table_schema.set_part_level(share::schema::PARTITION_LEVEL_ONE);
  }

  if (OB_SUCC(ret)) {
    if (NULL != node->children_[LIST_SUBPARTITIOPPN_NODE]) {
      bool force_template = false;
      if (NULL != node->children_[LIST_SUBPARTITIOPPN_NODE]->children_[LIST_TEMPLATE_MARK]) {
        // 模板化的二级分区定义后面再解析
        stmt->set_use_def_sub_part(true);
      } else if (OB_FAIL(resolve_individual_subpartition(stmt, node,
                                                         node->children_[LIST_ELEMENTS_NODE],
                                                         node->children_[LIST_SUBPARTITIOPPN_NODE],
                                                         table_schema, force_template))) {
        SQL_RESV_LOG(WARN, "failed to resolve individual subpartition", K(ret));
      } else if (force_template) {
        stmt->set_use_def_sub_part(true);
      } else {
        stmt->set_use_def_sub_part(false);
      }
    }
  }
  // 1. resolve partition type, partition keys, partition expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resolve_list_partition_basic_infos(node, is_subpartition, table_schema,
                                                   part_func_type, func_expr_name, part_func_exprs))) {
      SQL_RESV_LOG(WARN, "failed to resolve range partition basic infos", K(ret));
    } else if (OB_FAIL(partition_option->set_part_expr(func_expr_name))) {
      SQL_RESV_LOG(WARN, "set partition express string failed", K(ret));
    } else {
      partition_option->set_part_func_type(part_func_type);
    }
  }

  if (OB_SUCC(ret)) {
    if (!OB_ISNULL(node->children_[LIST_PARTITION_NUM_NODE])) {
      partition_num = node->children_[LIST_PARTITION_NUM_NODE]->value_;
      if (partition_num != node->children_[LIST_ELEMENTS_NODE]->num_child_) {
        ret = common::OB_ERR_PARSE_PARTITION_LIST;
      }
    } else if (node->children_[LIST_ELEMENTS_NODE] == NULL) {
      partition_num = 0;
    } else {
      partition_num = node->children_[LIST_ELEMENTS_NODE]->num_child_;
    }
    partition_option->set_part_num(partition_num);
    if (OB_SUCC(ret) && !table_schema.is_external_table()) {
      if (OB_FAIL(check_partition_name_duplicate(node->children_[LIST_ELEMENTS_NODE],
                                                 lib::is_oracle_mode()))) {
        SQL_RESV_LOG(WARN, "duplicate partition name", K(ret));
      } else if (is_subpartition) {
        if (OB_FAIL(resolve_list_subpartition_elements(stmt,
                                                       node->children_[LIST_ELEMENTS_NODE],
                                                       table_schema,
                                                       NULL, // dummy partition
                                                       part_func_type,
                                                       part_func_exprs,
                                                       list_values_exprs,
                                                       -1))) {
          SQL_RESV_LOG(WARN, "resolve reange partition elements fail", K(ret));
        }
      } else {
        if (OB_FAIL(resolve_list_partition_elements(stmt,
                                                    node->children_[LIST_ELEMENTS_NODE],
                                                    table_schema,
                                                    part_func_type,
                                                    part_func_exprs,
                                                    list_values_exprs))) {
          SQL_RESV_LOG(WARN, "resolve reange partition elements fail", K(ret));
        }
      }
    }
  }

  // 解析模板化二级分区定义
  if (OB_SUCC(ret)) {
    if (NULL != node->children_[LIST_SUBPARTITIOPPN_NODE] && stmt->use_def_sub_part()) {
      if (OB_FAIL(resolve_subpartition_option(stmt, node->children_[LIST_SUBPARTITIOPPN_NODE], table_schema))) {
        SQL_RESV_LOG(WARN, "failed to resolve subpartition", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
     if (is_subpartition) {
       if (OB_FAIL(stmt->get_subpart_fun_exprs().assign(part_func_exprs))) {
         SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
       } else if (OB_FAIL(stmt->get_template_subpart_values_exprs().assign(list_values_exprs))) {
         SQL_RESV_LOG(WARN, "failed to assign range values exprs", K(ret));
       }
     } else {
       if (OB_FAIL(stmt->get_part_fun_exprs().assign(part_func_exprs))) {
         SQL_RESV_LOG(WARN, "failed to assign range fun exprs", K(ret));
       } else if (OB_FAIL(stmt->get_part_values_exprs().assign(list_values_exprs))) {
         SQL_RESV_LOG(WARN, "failed to assign range values exprs", K(ret));
       }
     }
    SQL_RESV_LOG(DEBUG, "succ to resolve_partition_list", KPC(stmt), K(part_func_exprs), K(list_values_exprs));
  }
  return ret;
}
int ObDDLResolver::resolve_hash_partition_elements(ObPartitionedStmt *stmt,
                                                   ParseNode *node,
                                                   ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(stmt));
  } else {
    int64_t partition_num = node->num_child_;
    ObPartition partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      partition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;
      // 1. check partition name
      if (OB_ISNULL(element_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(element_node));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, partition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                            tablespace_id))) {
        LOG_WARN("failed to resolve tablespace id", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }
      //add hash partition elements to table schema
      if (OB_SUCC(ret)) {
        partition.set_part_idx(i);
        partition.set_tablespace_id(tablespace_id);
        if (OB_FAIL(table_schema.add_partition(partition))) {
          LOG_WARN("failed to add partition", K(ret));
        } else if (stmt->use_def_sub_part() &&
                   OB_NOT_NULL(element_node->children_[ELEMENT_SUBPARTITION_NODE])) {
          ret = OB_INVALID_SUB_PARTITION_TYPE;
          LOG_WARN("individual subpartition with sub part template", K(ret));
        } else if (!stmt->use_def_sub_part()) {
          // resolve non template
          ObPartition *cur_partition = table_schema.get_part_array()[i];
          if (OB_ISNULL(cur_partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(resolve_subpartition_elements(
              stmt,
              element_node->children_[ELEMENT_SUBPARTITION_NODE],
              table_schema,
              cur_partition,
              false))) {
            LOG_WARN("failed to resolve subpartition elements", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_schema.get_part_option().set_part_num(partition_num);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_hash_subpartition_elements(ObPartitionedStmt *stmt,
                                                      ParseNode *node,
                                                      ObTableSchema &table_schema,
                                                      ObPartition *partition)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  bool is_template = false;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(stmt));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else if (!is_template && OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null while not sub part template", K(ret));
  } else {
    int64_t partition_num = node->num_child_;
    ObSubPartition subpartition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      subpartition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;

      // 1. check partition name
      if (OB_ISNULL(element_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, subpartition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                               tablespace_id))) {
        LOG_WARN("failed to resolve tablespace id", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }
      // add hash subpartition elements to table schema or partition
      if (OB_SUCC(ret)) {
        subpartition.set_sub_part_idx(i);
        subpartition.set_tablespace_id(tablespace_id);
        if (is_template) {
          if (OB_FAIL(table_schema.add_def_subpartition(subpartition))) {
            LOG_WARN("failed to add partition", K(ret));
          }
        } else if (OB_FAIL(partition->add_partition(subpartition))) {
          LOG_WARN("failed to add partition", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_template) {
        table_schema.get_sub_part_option().set_part_num(partition_num);
      } else {
        partition->set_sub_part_num(partition_num);
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_range_partition_elements(ObPartitionedStmt *stmt,
                                                    ParseNode *node,
                                                    ObTableSchema &table_schema,
                                                    const ObPartitionFuncType part_type,
                                                    const ObIArray<ObRawExpr *> &part_func_exprs,
                                                    ObIArray<ObRawExpr *> &range_value_exprs,
                                                    const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  ParseNode *expr_list_node = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt));
  } else {
    int64_t partition_num = node->num_child_;
    ObPartition partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      partition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;
      if (OB_ISNULL(element_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node", K(ret), K(element_node));
      } else if (element_node->type_ != T_PARTITION_RANGE_ELEMENT) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("not a valid range partition define", K(element_node->type_));
      } else if (OB_ISNULL(expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE]) ||
                 OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node", K(expr_list_node));
      } else if (part_func_exprs.count() != expr_list_node->num_child_) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, partition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                               tablespace_id))) {
        LOG_WARN("failed to resolve tablespace id", K(ret));
      } else if (OB_FAIL(resolve_range_partition_value_node(*expr_list_node, partition_name,
                                                            part_type, part_func_exprs,
                                                            range_value_exprs, in_tablegroup))) {
        LOG_WARN("failed to resolve range partition value node", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }

      //add range partition elements to table schema
      if (OB_SUCC(ret)) {
        partition.set_tablespace_id(tablespace_id);
        if (OB_FAIL(table_schema.add_partition(partition))) {
          LOG_WARN("failed to add partition", K(ret));
        } else if (stmt->use_def_sub_part() &&
                   OB_NOT_NULL(element_node->children_[ELEMENT_SUBPARTITION_NODE])) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("individual subpartition with sub part template", K(ret));
        } else if (!stmt->use_def_sub_part()) {
          // resolve non template
          ObPartition *cur_partition = table_schema.get_part_array()[i];
          if (OB_ISNULL(cur_partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(resolve_subpartition_elements(
              stmt,
              element_node->children_[ELEMENT_SUBPARTITION_NODE],
              table_schema,
              cur_partition,
              false))) {
            LOG_WARN("failed to resolve subpartition elements", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_schema.get_part_option().set_part_num(partition_num);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_range_subpartition_elements(ObPartitionedStmt *stmt,
                                                       ParseNode *node,
                                                       ObTableSchema &table_schema,
                                                       ObPartition *partition,
                                                       const ObPartitionFuncType part_type,
                                                       const ObIArray<ObRawExpr *> &part_func_exprs,
                                                       ObIArray<ObRawExpr *> &range_value_exprs,
                                                       const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  ParseNode *expr_list_node = NULL;
  bool is_template = false;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else if (!is_template && OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null while not sub part template", K(ret));
  } else {
    int64_t partition_num = node->num_child_;
    ObSubPartition subpartition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      subpartition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;
      bool current_spec = false;
      if (OB_ISNULL(element_node = node->children_[i]) ||
          OB_ISNULL(expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE]) ||
          OB_UNLIKELY(T_EXPR_LIST != expr_list_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node", K(ret), K(element_node), K(expr_list_node));
      } else if (part_func_exprs.count() != expr_list_node->num_child_) {
        ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
        LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, subpartition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                               tablespace_id))) {
        LOG_WARN("failed to resolve tablespace id", K(ret));
      } else if (OB_FAIL(resolve_range_partition_value_node(*expr_list_node, partition_name,
                                                            part_type, part_func_exprs,
                                                            range_value_exprs, in_tablegroup))) {
        LOG_WARN("failed to resolve range partition value node", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }

      //add range partition elements to table schema
      if (OB_SUCC(ret)) {
        subpartition.set_tablespace_id(tablespace_id);
        if (is_template) {
          if (OB_FAIL(table_schema.add_def_subpartition(subpartition))) {
            LOG_WARN("failed to add partition", K(ret));
          }
        } else if (OB_FAIL(partition->add_partition(subpartition))) {
          LOG_WARN("failed to add partition", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_template) {
        table_schema.get_sub_part_option().set_part_num(partition_num);
      } else {
        partition->set_sub_part_num(partition_num);
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_list_partition_elements(ObPartitionedStmt *stmt,
                                                   ParseNode *node,
                                                   ObTableSchema &table_schema,
                                                   const ObPartitionFuncType part_type,
                                                   const ObIArray<ObRawExpr *> &part_func_exprs,
                                                   ObDDLStmt::array_t &list_value_exprs,
                                                   const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  ParseNode *expr_list_node = NULL;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(stmt));
  } else {
    int64_t partition_num = node->num_child_;
    ObPartition partition;
    bool init_specified = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      partition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;
      bool current_spec = false;
      if (OB_ISNULL(element_node = node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect node", K(ret), K(element_node));
      } else if (element_node->type_ != T_PARTITION_LIST_ELEMENT) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("not a valid range partition define", K(element_node->type_));
      } else if (OB_ISNULL(expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect node", K(ret), K(expr_list_node));
      } else if (T_EXPR_LIST != expr_list_node->type_ && T_DEFAULT != expr_list_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node type", K(ret), K(expr_list_node->type_));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, partition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                               tablespace_id))) {
        LOG_WARN("failed to reolve tablespace id", K(ret));
      } else if (OB_FAIL(resolve_list_partition_value_node(*expr_list_node, partition_name,
                                                           part_type, part_func_exprs,
                                                           list_value_exprs, in_tablegroup))) {
        LOG_WARN("failed to resolve list partition value node", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }

      //add list partition elements to table schema
      if (OB_SUCC(ret)) {
        partition.set_tablespace_id(tablespace_id);
        if (OB_FAIL(table_schema.add_partition(partition))) {
          LOG_WARN("failed to add partition", K(ret));
        } else if (stmt->use_def_sub_part() &&
                   OB_NOT_NULL(element_node->children_[ELEMENT_SUBPARTITION_NODE])) {
          ret = OB_ERR_PARSE_SQL;
          LOG_WARN("individual subpartition with sub part template", K(ret));
        } else if (!stmt->use_def_sub_part()) {
          // resolve non template
          ObPartition *cur_partition = table_schema.get_part_array()[i];
          if (OB_ISNULL(cur_partition)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(resolve_subpartition_elements(
              stmt,
              element_node->children_[ELEMENT_SUBPARTITION_NODE],
              table_schema,
              cur_partition,
              false))) {
            LOG_WARN("failed to resolve subpartition elements", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_schema.get_part_option().set_part_num(partition_num);
    }
  }
  return ret;
}

int ObDDLResolver::resolve_list_subpartition_elements(ObPartitionedStmt *stmt,
                                                      ParseNode *node,
                                                      ObTableSchema &table_schema,
                                                      ObPartition *partition,
                                                      const ObPartitionFuncType part_type,
                                                      const ObIArray<ObRawExpr *> &part_func_exprs,
                                                      ObDDLStmt::array_t &list_value_exprs,
                                                      const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ParseNode *element_node = NULL;
  ParseNode *expr_list_node = NULL;
  bool is_template = false;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null or stmt is null", K(ret), K(node), K(stmt_));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else {
    int64_t partition_num = node->num_child_;
    ObSubPartition subpartition;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      subpartition.reset();
      ObString partition_name;
      int64_t tablespace_id = OB_INVALID_ID;
      bool current_spec = false;
      if (OB_ISNULL(element_node = node->children_[i]) ||
          OB_ISNULL(expr_list_node = element_node->children_[PARTITION_ELEMENT_NODE])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpect node", K(ret), K(element_node), K(expr_list_node));
      } else if (T_EXPR_LIST != expr_list_node->type_ && T_DEFAULT != expr_list_node->type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected node type", K(ret), K(expr_list_node->type_));
      } else if (OB_FAIL(resolve_partition_name(element_node->children_[PARTITION_NAME_NODE],
                                                partition_name, subpartition))) {
        LOG_WARN("failed to resolve partition name", K(ret));
      } else if (OB_FAIL(resolve_tablespace_id(element_node->children_[ELEMENT_ATTRIBUTE_NODE],
                                               tablespace_id))) {
        LOG_WARN("failed to reolve tablespace id", K(ret));
      } else if (OB_FAIL(resolve_list_partition_value_node(*expr_list_node, partition_name,
                                                           part_type, part_func_exprs,
                                                           list_value_exprs, in_tablegroup))) {
        LOG_WARN("failed to resolve list partition value node", K(ret));
      } else if (OB_NOT_NULL(element_node->children_[PART_ID_NODE])) {
        // PART_ID is deprecated in 4.0, we just ignore and show warnings here.
        LOG_USER_WARN_ONCE(OB_NOT_SUPPORTED, "specify part_id");
      }

      //add list partition elements to table schema
      if (OB_SUCC(ret)) {
        subpartition.set_tablespace_id(tablespace_id);
        if (is_template) {
          if (OB_FAIL(table_schema.add_def_subpartition(subpartition))) {
            LOG_WARN("failed to add partition", K(ret));
          }
        } else if (OB_FAIL(partition->add_partition(subpartition))) {
          LOG_WARN("failed to add partition", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_template) {
        table_schema.get_sub_part_option().set_part_num(partition_num);
      } else {
        partition->set_sub_part_num(partition_num);
      }
    }
  }
  return ret;
}

int ObDDLResolver::resolve_range_partition_value_node(ParseNode &expr_list_node,
                                                      const ObString &partition_name,
                                                      const ObPartitionFuncType part_type,
                                                      const ObIArray<ObRawExpr *> &part_func_exprs,
                                                      ObIArray<ObRawExpr *> &range_value_exprs,
                                                      const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node.num_child_; ++i) {
    if (OB_ISNULL(expr_list_node.children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null node", K(ret));
    } else if (T_NULL == expr_list_node.children_[i]->type_) {
      ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
      LOG_WARN("null value is not allowed in less than", K(ret));
    } else if (T_MAXVALUE == expr_list_node.children_[i]->type_) {
      ObRawExpr *maxvalue_expr = NULL;
      ObConstRawExpr *c_expr = NULL;
      c_expr = (ObConstRawExpr *)allocator_->alloc(sizeof(ObConstRawExpr));
      if (OB_ISNULL(c_expr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        c_expr = new (c_expr) ObConstRawExpr();
        maxvalue_expr = c_expr;
        maxvalue_expr->set_data_type(common::ObMaxType);
        if (OB_FAIL(range_value_exprs.push_back(maxvalue_expr))) {
          LOG_WARN("array push back fail", K(ret));
        }
      }
    } else if (T_EXPR_LIST != expr_list_node.children_[i]->type_) {
      ObRawExpr *part_value_expr = NULL;
      ObRawExpr *part_func_expr = NULL;
      if (OB_FAIL(part_func_exprs.at(i, part_func_expr))) {
        LOG_WARN("get part expr failed", K(i), "size", part_func_exprs.count(), K(ret));
      } else if (OB_ISNULL(part_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_func_expr is invalid", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params_,
                                                                             *(expr_list_node.children_[i]),
                                                                             partition_name,
                                                                             part_type,
                                                                             *part_func_expr,
                                                                             part_value_expr,
                                                                             in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_FAIL(range_value_exprs.push_back(part_value_expr))) {
        LOG_WARN("array push back fail", K(ret));
      }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("syntax error, expect single expr while expr list got", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::resolve_list_partition_value_node(ParseNode &expr_list_node,
                                                     const ObString &partition_name,
                                                     const ObPartitionFuncType part_type,
                                                     const ObIArray<ObRawExpr *> &part_func_exprs,
                                                     ObDDLStmt::array_t &list_value_exprs,
                                                     const bool &in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *row_expr = NULL;
  if (OB_ISNULL(params_.expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr))) {
    LOG_WARN("failed to create raw expr", K(ret));
  } else if (OB_ISNULL(row_expr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allcoate memory", K(ret));
  } else if (T_DEFAULT == expr_list_node.type_) {
    //这里使用max来代替default值
    ObRawExpr *maxvalue_expr = NULL;
    ObConstRawExpr *c_expr = NULL;
    c_expr = (ObConstRawExpr *) allocator_->alloc(sizeof(ObConstRawExpr));
    if (OB_ISNULL(c_expr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allcoate memory", K(ret));
    } else {
      c_expr = new(c_expr) ObConstRawExpr();
      maxvalue_expr = c_expr;
      maxvalue_expr->set_data_type(common::ObMaxType);
      if (OB_FAIL(row_expr->add_param_expr(maxvalue_expr))) {
        LOG_WARN("failed add param expr", K(ret));
      }
    }
  } else {
    ObSEArray<ObRawExpr *, 16> part_value_exprs;
    bool is_all_expr_list = false;
    if (expr_list_node.num_child_ > 0) {
      is_all_expr_list = (expr_list_node.children_[0]->type_ == T_EXPR_LIST);
    }
    /* multi cols in partition key and only one vector in values
    CREATE TABLE W0CASE (W0_TEST0 NVARCHAR2(100),W0_TEST1 DATE)
    PARTITION BY LIST (W0_TEST1,W0_TEST0)
    (PARTITION P0 VALUES (DATE'2020-02-02',1));
    */
    if (!is_all_expr_list && part_func_exprs.count() > 1) {
      if (OB_FAIL(ObResolverUtils::resolve_partition_list_value_expr(params_,
                                                                  expr_list_node,
                                                                  partition_name,
                                                                  part_type,
                                                                  part_func_exprs,
                                                                  part_value_exprs,
                                                                  in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < part_value_exprs.count(); ++j) {
        if (OB_FAIL(row_expr->add_param_expr(part_value_exprs.at(j)))) {
          LOG_WARN("failed add param expr", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_list_node.num_child_; ++i) {
        part_value_exprs.reset();
        if ((is_all_expr_list && expr_list_node.children_[i]->type_ != T_EXPR_LIST) ||
            (!is_all_expr_list && expr_list_node.children_[i]->type_ == T_EXPR_LIST)) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
        } else if (OB_ISNULL(expr_list_node.children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("node is null", K(ret));
        } else if (OB_FAIL(ObResolverUtils::resolve_partition_list_value_expr(params_,
                                                                              *(expr_list_node.children_[i]),
                                                                              partition_name,
                                                                              part_type,
                                                                              part_func_exprs,
                                                                              part_value_exprs,
                                                                              in_tablegroup))) {
          LOG_WARN("resolve partition expr failed", K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < part_value_exprs.count(); ++j) {
          if (OB_FAIL(row_expr->add_param_expr(part_value_exprs.at(j)))) {
            LOG_WARN("failed add param expr", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (part_func_exprs.count() > 1 && !is_all_expr_list) {
        if (row_expr->get_param_count() != part_func_exprs.count()) {
          ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
          LOG_WARN("Inconsistency in usage of column lists for partitioning near", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::generate_default_hash_part(const int64_t partition_num,
                                              const int64_t tablespace_id,
                                              ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.get_part_option().set_part_num(partition_num);
  char name_buf[common::OB_MAX_PARTITION_NAME_LENGTH];
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
    ObPartition partition;
    partition.set_part_idx(i);
    partition.set_tablespace_id(tablespace_id);
    ObString part_name;
    MEMSET(name_buf, 0, common::OB_MAX_PARTITION_NAME_LENGTH);

    if (OB_FAIL(ObPartitionSchema::gen_hash_part_name(partition.get_part_idx(),
                                                ObHashNameType::FIRST_PART,
                                                is_oracle_mode(),
                                                name_buf,
                                                common::OB_MAX_PARTITION_NAME_LENGTH,
                                                NULL))) {
      LOG_WARN("failed to gen hash part name", K(ret));
    } else if (FALSE_IT(part_name = ObString(strlen(name_buf), name_buf))) {
    } else if (OB_FAIL(partition.set_part_name(part_name))) {
      LOG_WARN("fail to set part name", K(ret));
    } else if (OB_FAIL(table_schema.check_part_name(partition))) {
      LOG_WARN("fail to check part name", K(ret));
    } else if (OB_FAIL(table_schema.add_partition(partition))) {
      LOG_WARN("fail to add partition", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::generate_default_hash_subpart(
    ObPartitionedStmt *stmt,
    const int64_t partition_num,
    const int64_t tablespace_id,
    ObTableSchema &table_schema,
    ObPartition *partition)
{
  int ret = OB_SUCCESS;
  char name_buf[common::OB_MAX_PARTITION_NAME_LENGTH];
  bool is_template = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else if (!is_template && OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not subpart template by partition is null", K(ret));
  } else if (is_template) {
    table_schema.get_sub_part_option().set_part_num(partition_num);
  } else {
    partition->set_sub_part_num(partition_num);
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      ObSubPartition subpartition;
      subpartition.set_sub_part_idx(i);
      subpartition.set_tablespace_id(tablespace_id);
      ObString subpart_name;
      MEMSET(name_buf, 0, common::OB_MAX_PARTITION_NAME_LENGTH);
      if (is_template &&
          OB_FAIL(ObPartitionSchema::gen_hash_part_name(subpartition.get_sub_part_idx(),
                                                  ObHashNameType::TEMPLATE_SUB_PART,
                                                  is_oracle_mode(),
                                                  name_buf,
                                                  common::OB_MAX_PARTITION_NAME_LENGTH,
                                                  NULL))) {
        LOG_WARN("faield to gen hash part name", K(ret));
      } else if (!is_template &&
                 OB_FAIL(ObPartitionSchema::gen_hash_part_name(subpartition.get_sub_part_idx(),
                                                         ObHashNameType::INDIVIDUAL_SUB_PART,
                                                         is_oracle_mode(),
                                                         name_buf,
                                                         common::OB_MAX_PARTITION_NAME_LENGTH,
                                                         NULL,
                                                         partition))) {
        LOG_WARN("faield to gen hash part name", K(ret));
      } else if (FALSE_IT(subpart_name = ObString(strlen(name_buf), name_buf))) {
      } else if (OB_FAIL(subpartition.set_part_name(subpart_name))) {
        LOG_WARN("fail to set part name", K(ret));
      } else if (is_template) {
        if (OB_FAIL(table_schema.add_def_subpartition(subpartition))) {
          LOG_WARN("fail to add partition", K(ret));
        }
      } else if (OB_FAIL(partition->add_partition(subpartition))) {
        LOG_WARN("failed to add partition", K(ret));
      }
    }
  }
  return ret;
}

/* generate default range subpartition.
   subpartnum: 1
   values: maxvalue
   is_template = false only.
   to do when is_template = true */
int ObDDLResolver::generate_default_range_subpart(
    ObPartitionedStmt *stmt,
    const int64_t tablespace_id,
    ObTableSchema &table_schema,
    ObPartition *partition,
    ObIArray<ObRawExpr *> &range_value_exprs)
{
  int ret = OB_SUCCESS;
  const int64_t partition_num = 1;
  char name_buf[common::OB_MAX_PARTITION_NAME_LENGTH];
  bool is_template = false;
  int n_expr_in_subkey = table_schema.get_subpartition_key_column_num();

  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else if (!is_template && OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not subpart template by partition is null", K(ret));
  } else if (is_template) {
    table_schema.get_sub_part_option().set_part_num(partition_num);
  } else {
    partition->set_sub_part_num(partition_num);
  }

  if (OB_SUCC(ret) && !is_template) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      ObSubPartition subpartition;
      for (int j = 0; OB_SUCC(ret) && j < n_expr_in_subkey; ++j) {
        ObRawExpr *maxvalue_expr = NULL;
        ObConstRawExpr *c_expr = NULL;
        c_expr = (ObConstRawExpr *)allocator_->alloc(sizeof(ObConstRawExpr));
        if (OB_ISNULL(c_expr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          c_expr = new (c_expr) ObConstRawExpr();
          maxvalue_expr = c_expr;
          maxvalue_expr->set_data_type(common::ObMaxType);
          OZ (range_value_exprs.push_back(maxvalue_expr));
        }
      }
      if (OB_SUCC(ret)) {
        subpartition.set_sub_part_idx(i);
        subpartition.set_tablespace_id(tablespace_id);
        OX (subpartition.set_is_empty_partition_name(true));
        OZ (partition->add_partition(subpartition));

      }
    }
  }
  return ret;
}

/* generate default list subpartition.
   subpartnum: 1
   values: default
   is_template = false only.
   to do when is_template = true */
int ObDDLResolver::generate_default_list_subpart(
    ObPartitionedStmt *stmt,
    const int64_t tablespace_id,
    ObTableSchema &table_schema,
    ObPartition *partition,
    ObIArray<ObRawExpr *> &list_value_exprs)
{
  int ret = OB_SUCCESS;
  const int64_t partition_num = 1;
  char name_buf[common::OB_MAX_PARTITION_NAME_LENGTH];
  bool is_template = false;
  int n_expr_in_subkey = table_schema.get_subpartition_key_column_num();

  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (FALSE_IT(is_template = stmt->use_def_sub_part())) {
  } else if (!is_template && OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not subpart template by partition is null", K(ret));
  } else if (is_template) {
    table_schema.get_sub_part_option().set_part_num(partition_num);
  } else {
    partition->set_sub_part_num(partition_num);
  }

  if (OB_SUCC(ret) && !is_template) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      ObSubPartition subpartition;
      ObOpRawExpr *row_expr = NULL;

      CK (OB_NOT_NULL(params_.expr_factory_));
      OZ (params_.expr_factory_->create_raw_expr(T_OP_ROW, row_expr));
      CK (OB_NOT_NULL(row_expr));
      if (OB_SUCC(ret)) {
        ObRawExpr *maxvalue_expr = NULL;
        ObConstRawExpr *c_expr = NULL;
        c_expr = (ObConstRawExpr *) allocator_->alloc(sizeof(ObConstRawExpr));
        if (OB_ISNULL(c_expr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allcoate memory", K(ret));
        } else {
          c_expr = new(c_expr) ObConstRawExpr();
          maxvalue_expr = c_expr;
          maxvalue_expr->set_data_type(common::ObMaxType);
          if (OB_FAIL(row_expr->add_param_expr(maxvalue_expr))) {
            LOG_WARN("failed add param expr", K(ret));
          } else if (OB_FAIL(list_value_exprs.push_back(row_expr))) {
            LOG_WARN("array push back fail", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        subpartition.set_sub_part_idx(i);
        subpartition.set_tablespace_id(tablespace_id);
        OX (subpartition.set_is_empty_partition_name(true));
        OZ (partition->add_partition(subpartition));
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_and_set_partition_names(ObPartitionedStmt *stmt,
                                                 ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", KR(ret), KP(stmt));
  } else if (OB_FAIL(check_and_set_partition_names(stmt, table_schema, false))) {
    LOG_WARN("failed to check and set partition names", K(ret));
  } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
    if (stmt->use_def_sub_part()) {
      if (OB_FAIL(check_and_set_partition_names(stmt, table_schema, true))) {
        LOG_WARN("failed to check and set partition names", K(ret));
      }
    } else if (OB_FAIL(check_and_set_individual_subpartition_names(stmt, table_schema))) {
      LOG_WARN("failed to check and set individual subpartition names", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::check_and_set_partition_names(ObPartitionedStmt *stmt,
                                                 ObTableSchema &table_schema,
                                                 bool is_subpart)
{
  int ret = OB_SUCCESS;
  hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE> *partition_name_set = nullptr;
  void *buf = nullptr;
  int64_t partition_num = is_subpart ?
      table_schema.get_def_sub_part_num() : table_schema.get_first_part_num();
  ObBasePartition *partition = NULL;
  ObPartition **partition_array = table_schema.get_part_array();
  ObSubPartition **subpartition_array = table_schema.get_def_subpart_array();
  ObSEArray<int64_t, 128> empty_part_idx;
  if (is_subpart && OB_ISNULL(subpartition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null subpartition array", K(ret));
  } else if (!is_subpart && OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null partition array", K(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(
      hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    partition_name_set = new(buf)hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>();
    if (OB_ISNULL(partition_name_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition name hash set is null", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
    if (is_subpart) {
      partition = subpartition_array[i];
    } else {
      partition = partition_array[i];
    }
    if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (partition->is_empty_partition_name()) {
      if (OB_FAIL(empty_part_idx.push_back(i))) {
        LOG_WARN("failed to push back part idx", K(ret));
      }
    } else {
      const ObString &partition_name = partition->get_part_name();
      ObPartitionNameHashWrapper partition_name_key(partition_name);
      if (OB_HASH_EXIST == partition_name_set->exist_refactored(partition_name_key)) {
        ret = OB_ERR_SAME_NAME_PARTITION;
        LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION, partition_name.length(), partition_name.ptr());
      } else if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
        LOG_WARN("add partition name to map failed", K(ret), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !empty_part_idx.empty() &&
      (stmt::T_CREATE_TABLE == stmt->get_stmt_type() ||
       stmt::T_CREATE_TABLEGROUP == stmt->get_stmt_type() ||
       stmt::T_CREATE_INDEX == stmt->get_stmt_type())) {
    //FIXME: partition_name may still conflict in one table since we can specify partition_name.
    int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
    ObString part_name_str;
    for (int64_t i = 0; OB_SUCC(ret) && i < empty_part_idx.count(); ++i) {
      const int64_t part_idx = empty_part_idx.at(i);
      if (is_subpart) {
        partition = subpartition_array[part_idx];
      } else {
        partition = partition_array[part_idx];
      }
      bool is_valid = false;
      while (OB_SUCC(ret) && !is_valid) {
        char part_name[OB_MAX_PARTITION_NAME_LENGTH];
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", max_part_id))) {
          LOG_WARN("failed to print databuff", K(ret), K(max_part_id));
        } else {
          part_name_str.assign(part_name, static_cast<int32_t>(pos));
          ObPartitionNameHashWrapper partition_name_key(part_name_str);
          if (OB_HASH_EXIST == partition_name_set->exist_refactored(partition_name_key)) {
            // do nothing
          } else if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
            LOG_WARN("add partition name to map failed", K(ret), K(ret));
          } else if (OB_FAIL(partition->set_part_name(part_name_str))) {
            LOG_WARN("failed to set part name", K(ret));
          } else {
            partition->set_is_empty_partition_name(false);
            is_valid = true;
          }
        }
        ++max_part_id;
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_and_set_individual_subpartition_names(ObPartitionedStmt *stmt,
                                                               ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE> *partition_name_set = nullptr;
  void *buf = nullptr;
  int64_t partition_num = table_schema.get_first_part_num();
  ObPartition **partition_array = NULL;
  ObPartition *partition = NULL;
  ObSubPartition **subpartition_array = NULL;
  ObSubPartition *subpartition = NULL;
  ObSEArray<int64_t, 128> empty_part_idx;
  ObSEArray<int64_t, 128> empty_subpart_idx;
  if (OB_ISNULL(stmt) || OB_UNLIKELY(stmt->use_def_sub_part())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected subpartition", K(ret), KP(stmt));
  } else if (OB_ISNULL(partition_array = table_schema.get_part_array())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(buf = allocator_->alloc(sizeof(
      hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    partition_name_set = new(buf)hash::ObPlacementHashSet<ObPartitionNameHashWrapper, OB_MAX_PARTITION_NUM_ORACLE>();
    if (OB_ISNULL(partition_name_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition name hash set is null", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
    if (OB_ISNULL(partition = partition_array[i]) ||
        OB_ISNULL(subpartition_array = partition->get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(partition), K(subpartition_array));
    }
    /* part name and subpart name share one namespace .
       So, add partition name to hash table */
    if (!partition->is_empty_partition_name()) {
      const ObString &partition_name = partition->get_part_name();
      ObPartitionNameHashWrapper partition_name_key(partition_name);
      if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
        LOG_WARN("add partition name to map failed", K(ret));
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < partition->get_sub_part_num(); ++j) {
      if (OB_ISNULL(subpartition = subpartition_array[j])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (subpartition->is_empty_partition_name()) {
        if (OB_FAIL(empty_part_idx.push_back(i))) {
          LOG_WARN("failed to push back part idx", K(ret));
        } else if (OB_FAIL(empty_subpart_idx.push_back(j))) {
          LOG_WARN("failed to push back subpart idx", K(ret));
        }
      } else {
        const ObString &partition_name = subpartition->get_part_name();
        ObPartitionNameHashWrapper partition_name_key(partition_name);
        if (OB_HASH_EXIST == partition_name_set->exist_refactored(partition_name_key)) {
          ret = OB_ERR_SAME_NAME_SUBPARTITION;
          LOG_USER_ERROR(OB_ERR_SAME_NAME_SUBPARTITION, partition_name.length(), partition_name.ptr());
        } else if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
          LOG_WARN("add partition name to map failed", K(ret), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) &&
      !empty_part_idx.empty() &&
      (stmt::T_CREATE_TABLE == stmt->get_stmt_type() ||
       stmt::T_CREATE_TABLEGROUP == stmt->get_stmt_type() ||
       stmt::T_CREATE_INDEX == stmt->get_stmt_type() ||
       stmt::T_ALTER_TABLE == stmt->get_stmt_type())) {
    //FIXME: partition_name may still conflict in one table since we can specify partition_name.
    int64_t max_part_id = OB_MAX_PARTITION_NUM_MYSQL;
    if (stmt::T_ALTER_TABLE == stmt->get_stmt_type()) {
      const ObTableSchema *orig_table_schema = NULL;
      int64_t max_used_part_id = OB_INVALID_ID;
      if (OB_ISNULL(schema_checker_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema checker ptr is null", K(ret));
      } else if (OB_FAIL(schema_checker_->get_table_schema(
        table_schema.get_tenant_id(), table_schema.get_table_id(), orig_table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret), K(table_schema));
      } else if (OB_ISNULL(orig_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table is not exist", KR(ret), K(table_schema.get_table_id()));
      } else if (OB_FAIL(orig_table_schema->get_max_part_idx(max_used_part_id))) {
        LOG_WARN("fail to get max part_id", KR(ret), KPC(orig_table_schema));
      } else {
        max_part_id += max_used_part_id;
      }
    }
    ObString part_name_str;
    for (int64_t i = 0; OB_SUCC(ret) && i < empty_part_idx.count(); ++i) {
      int64_t part_idx = empty_part_idx.at(i);
      int64_t subpart_idx = empty_subpart_idx.at(i);
      subpartition = partition_array[part_idx]->get_subpart_array()[subpart_idx];
      bool is_valid = false;
      while (OB_SUCC(ret) && !is_valid) {
        char part_name[OB_MAX_PARTITION_NAME_LENGTH];
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(part_name, OB_MAX_PARTITION_NAME_LENGTH, pos, "P%ld", max_part_id))) {
          LOG_WARN("failed to print databuff", K(ret), K(max_part_id));
        } else {
          part_name_str.assign(part_name, static_cast<int32_t>(pos));
          ObPartitionNameHashWrapper partition_name_key(part_name_str);
          if (OB_HASH_EXIST == partition_name_set->exist_refactored(partition_name_key)) {
            // do nothing
          } else if (OB_FAIL(partition_name_set->set_refactored(partition_name_key))) {
            LOG_WARN("add partition name to map failed", K(ret), K(ret));
          } else if (OB_FAIL(subpartition->set_part_name(part_name_str))) {
            LOG_WARN("failed to set part name", K(ret));
          } else {
            subpartition->set_is_empty_partition_name(false);
            is_valid = true;
          }
        }
        ++max_part_id;
      }
    }
  }
  return ret;
}

int ObDDLResolver::calc_ddl_parallelism(const uint64_t hint_parallelism, const uint64_t table_dop, uint64_t &parallelism)
{
  int ret = OB_SUCCESS;
  if (hint_parallelism > 1) {
    parallelism = hint_parallelism;
  } else {
    uint64_t force_parallel_ddl_dop = 0;
    bool enable_parallel_ddl = false;
    if (OB_FAIL(session_info_->get_force_parallel_ddl_dop(force_parallel_ddl_dop))) {
      LOG_WARN("get force parallel ddl dop failed", K(ret));
    } else {
      if (force_parallel_ddl_dop > 1) {
        parallelism = force_parallel_ddl_dop;
      } else {
        if (OB_FAIL(session_info_->get_enable_parallel_ddl(enable_parallel_ddl))) {
          LOG_WARN("get enable parallel ddl failed", K(ret));
        } else if (enable_parallel_ddl) {
          parallelism = table_dop;
        } else {
          parallelism = 1;
        }
      }
    }
  }
  LOG_INFO("calc ddl parallelism", K(parallelism));
  return ret;
}

int ObDDLResolver::resolve_hints(const ParseNode *node, ObDDLStmt &stmt, const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t hint_parallel = 1;
  uint64_t parallelism = 1;
  if (OB_UNLIKELY(nullptr == node) || OB_UNLIKELY(OB_ISNULL(session_info_))) {
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode *hint_node = node->children_[i];
      if (nullptr != hint_node) {
        if (T_PARALLEL == hint_node->type_) {
          ParseNode *parallel_node = nullptr;
          if (1 != hint_node->num_child_) {
            /* ignore parallel(auto) and parallel(manual)*/
            LOG_WARN("Unused parallel hint");
          } else if (OB_ISNULL(parallel_node = hint_node->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, child of stmt parallel degree node should not be null", K(ret));
          } else if (parallel_node->value_ < 1) {
            hint_parallel = 1;  // ignore invalid hint
          } else {
            hint_parallel = parallel_node->value_;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t table_dop = stmt::T_CREATE_INDEX == stmt.get_cmd_type() ? static_cast<ObCreateIndexStmt &>(stmt).get_index_dop() : table_schema.get_dop();
    if (OB_FAIL(calc_ddl_parallelism(hint_parallel, table_dop, parallelism))) {
      LOG_WARN("calc ddl parallelism failed", K(ret));
    } else {
      stmt.set_parallelism(parallelism);
    }
  }
  return ret;
}

int ObDDLResolver::deep_copy_string_in_part_expr(ObPartitionedStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt");
  } else if (OB_FAIL(append(exprs, stmt->get_part_fun_exprs()))) {
    LOG_WARN("failed to append part fun exprs", K(ret));
  } else if (OB_FAIL(append(exprs, stmt->get_subpart_fun_exprs()))) {
    LOG_WARN("failed to append subpart fun exprs", K(ret));
  } else if (exprs.count() > 0 && OB_FAIL(deep_copy_column_expr_name(*allocator_, exprs))) {
    LOG_WARN("failed to deep copy column expr name");
  }
  return ret;
}

int ObDDLResolver::deep_copy_column_expr_name(common::ObIAllocator &allocator,
                                              ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(exprs, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      ObColumnRefRawExpr* column_expr = NULL;
      if (OB_ISNULL(column_exprs.at(i)) || !column_exprs.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(i), K(column_exprs));
      } else if (OB_FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i)))) {
      } else if (OB_FAIL(ob_write_string(allocator, column_expr->get_column_name(), column_expr->get_column_name()))) {
        LOG_WARN("failed to write string");
      } else if (OB_FAIL(ob_write_string(allocator, column_expr->get_database_name(), column_expr->get_database_name()))) {
        LOG_WARN("failed to write string");
      }
    }
  }
  return ret;
}

int ObDDLResolver::parse_column_group(const ParseNode *column_group_node,
                                      const ObTableSchema &table_schema,
                                      ObTableSchema &dst_table_schema)
{
  int ret = OB_SUCCESS;
  bool sql_exist_all_column_group = false;
  bool sql_exist_single_column_group = false;
  ObColumnGroupSchema column_group_schema;
  if (OB_ISNULL(column_group_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column gorup node should not be null", K(ret));
  } else if (!ObSchemaUtils::can_add_column_group(table_schema)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported table type to add column group", K(ret));
  } else {
    dst_table_schema.set_max_used_column_group_id(table_schema.get_max_used_column_group_id());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_group_node->num_child_; ++i) {
    if (OB_ISNULL(column_group_node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "column group node children is null", K(ret), K(i));
    } else if (column_group_node->children_[i]->type_ == T_ALL_COLUMN_GROUP) {
      if (sql_exist_all_column_group) {
        ret = OB_ERR_COLUMN_GROUP_DUPLICATE;
        SQL_RESV_LOG(WARN, "all column group already exist in sql",
                     K(ret), K(column_group_node->children_[i]->type_));
        const ObString error_msg = "all column group";
        LOG_USER_ERROR(OB_ERR_COLUMN_GROUP_DUPLICATE, error_msg.length(), error_msg.ptr());
      } else {
        sql_exist_all_column_group = true;
      }
    } else if (column_group_node->children_[i]-> type_ == T_SINGLE_COLUMN_GROUP) {
      if (sql_exist_single_column_group) {
        ret = OB_ERR_COLUMN_GROUP_DUPLICATE;
        SQL_RESV_LOG(WARN, "single column group already exist in sql",
                     K(ret), K(column_group_node->children_[i]->type_));
        const ObString error_msg = "single column group";
        LOG_USER_ERROR(OB_ERR_COLUMN_GROUP_DUPLICATE, error_msg.length(), error_msg.ptr());
      } else {
        sql_exist_single_column_group = true;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      SQL_RESV_LOG(WARN, "Resovle unsupported column group type",
                   K(ret), K(column_group_node->children_[i]->type_));
    }
  }

  /* all column group */
  /* column group in resolver do not use real column group id*/
  /* ddl service use column group name to distingush them*/
  if (OB_SUCC(ret) && sql_exist_all_column_group) {
    column_group_schema.reset();
    if (OB_FAIL(ObSchemaUtils::build_all_column_group(table_schema, session_info_->get_effective_tenant_id(),
                                                      dst_table_schema.get_max_used_column_group_id() + 1,
                                                      column_group_schema))) {
      SQL_RESV_LOG(WARN, "build all column group failed", K(ret));
    } else if (OB_FAIL(dst_table_schema.add_column_group(column_group_schema))) {
      SQL_RESV_LOG(WARN, "fail to add column group schema", K(ret));
    }
  }

  /* single column group*/
  if (OB_SUCC(ret) && sql_exist_single_column_group) {
    if (OB_FAIL(ObSchemaUtils::build_add_each_column_group(table_schema, dst_table_schema))) {
      LOG_WARN("fail to build each column group", K(ret));
    }
  }
  return ret;
}

int ObDDLResolver::parse_cg_node(const ParseNode &cg_node, obrpc::ObCreateIndexArg &create_index_arg) const
{
  int ret = OB_SUCCESS;
  bool is_all_cg_exist = false;
  bool is_each_cg_exist = false;

  if (OB_UNLIKELY(T_COLUMN_GROUP != cg_node.type_ || cg_node.num_child_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument", KR(ret), K(cg_node.type_), K(cg_node.num_child_));
  } else {
    const int64_t num_child = cg_node.num_child_;
    // handle all_type column_group & single_type column_group
    for (int64_t i = 0; OB_SUCC(ret) && (i < num_child); ++i) {
      ParseNode *node = cg_node.children_[i];
      if (OB_ISNULL(node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("children of column_group_list should not be null", KR(ret));
      } else if (T_ALL_COLUMN_GROUP == node->type_) {
        if (is_all_cg_exist) {
          ret = OB_ERR_COLUMN_GROUP_DUPLICATE;
          SQL_RESV_LOG(WARN, "all column group already exist in sql",
                       K(ret), K(node->children_[i]->type_));
          const ObString error_msg = "all columns";
          LOG_USER_ERROR(OB_ERR_COLUMN_GROUP_DUPLICATE, error_msg.length(), error_msg.ptr());
        } else {
          is_all_cg_exist = true;
        }
      } else if (T_SINGLE_COLUMN_GROUP == node->type_) {
        if (is_each_cg_exist) {
          ret = OB_ERR_COLUMN_GROUP_DUPLICATE;
          SQL_RESV_LOG(WARN, "single column group already exist in sql",
                       K(ret), K(node->type_));
          const ObString error_msg = "each column";
          LOG_USER_ERROR(OB_ERR_COLUMN_GROUP_DUPLICATE, error_msg.length(), error_msg.ptr());
        } else {
          is_each_cg_exist = true;
        }
      } else if (T_NORMAL_COLUMN_GROUP == node->type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("column store table with customized column group are not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "column store tables with customized column group are");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_each_cg_exist) {
      obrpc::ObCreateIndexArg::ObIndexColumnGroupItem each_cg_item;
      each_cg_item.is_each_cg_ = true;
      each_cg_item.cg_type_ = ObColumnGroupType::SINGLE_COLUMN_GROUP;
      if (OB_FAIL(create_index_arg.index_cgs_.push_back(each_cg_item))) {
        LOG_WARN("failed to push back value", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_all_cg_exist) {
      obrpc::ObCreateIndexArg::ObIndexColumnGroupItem all_cg_item;
      all_cg_item.is_each_cg_ = false;
      all_cg_item.cg_type_ = ObColumnGroupType::ALL_COLUMN_GROUP;
      if (OB_FAIL(create_index_arg.index_cgs_.push_back(all_cg_item))) {
        LOG_WARN("failed to push back value", K(ret));
      } else {
        create_index_arg.exist_all_column_group_ = true; /* for compat*/
      }
    }
  }
  return ret;
}

int ObDDLResolver::check_ttl_definition(const ParseNode *node)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObTableSchema *tbl_schema = NULL;
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(stmt_) ||
      OB_ISNULL(session_info_) || OB_ISNULL(node) || node->type_ != T_TTL_DEFINITION) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"unexpected null value", K(ret), K_(schema_checker),
                 K_(stmt), K_(session_info), K(node));
  } else if (node->type_ != T_TTL_DEFINITION) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"unexpected node type", K(ret), K(node->type_));
  } else if (stmt::T_CREATE_TABLE == stmt_->get_stmt_type()) {
    ObCreateTableStmt *create_table_stmt = static_cast<ObCreateTableStmt*>(stmt_);
    tbl_schema = &create_table_stmt->get_create_table_arg().schema_;
  } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
    ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
    if (OB_FAIL(schema_checker_->get_table_schema(session_info_->get_effective_tenant_id(),
      database_name_, table_name_, false, tbl_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(session_info_->get_effective_tenant_id()),
        K(alter_table_stmt->get_alter_table_arg()));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported statement for TTL expression", K(ret), K(stmt_->get_stmt_type()));
  }

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
    if (OB_ISNULL(node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ttl expr is null", K(ret), K(i));
    } else if (OB_ISNULL(node->children_[i]) || T_TTL_EXPR != node->children_[i]->type_ ||
                node->children_[i]->num_child_ != 3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node of ttl definition is wrong", KR(ret), K(node->children_[i]));
    } else if (OB_ISNULL(node->children_[i]->children_[0]) || T_COLUMN_REF != node->children_[i]->children_[0]->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child node of ttl expr is wrong", KR(ret), K(node->children_[i]->children_[0]));
    } else {
      ObString column_name(node->children_[i]->children_[0]->str_len_, node->children_[i]->children_[0]->str_value_);
      if (OB_ISNULL(column_schema = tbl_schema->get_column_schema(column_name))) {
        ret = OB_TTL_COLUMN_NOT_EXIST;
        LOG_USER_ERROR(OB_TTL_COLUMN_NOT_EXIST, column_name.length(), column_name.ptr());
        LOG_WARN("ttl column is not exists", K(ret), K(column_name));
      } else if ((!ob_is_datetime_tc(column_schema->get_data_type()))) {
        ret = OB_TTL_COLUMN_TYPE_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_TTL_COLUMN_TYPE_NOT_SUPPORTED, column_name.length(), column_name.ptr());
        LOG_WARN("invalid ttl expression, ttl column type should be datetime or timestamp",
                  K(ret), K(column_name), K(column_schema->get_data_type()));
      }
    }
  }

  return ret;
}

int ObDDLResolver::resolve_index_column_group(const ParseNode *cg_node, obrpc::ObCreateIndexArg &create_index_arg)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_ISNULL(cg_node) || cg_node->num_child_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree, column group node is null or have no children!",
                  K(ret), KP(cg_node));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), compat_version))) {
    SQL_RESV_LOG(WARN, "fail to get min data version", K(ret));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    SQL_RESV_LOG(WARN, "data_version not support for index column_group", K(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3, create index with column group");
  } else {
    bool exist_all_column_group = false;
    if (OB_FAIL(parse_cg_node(*cg_node, create_index_arg))) {
      LOG_WARN("fail to parse cg node", KR(ret));
    }
  }
  return ret;
}

int ObDDLResolver::build_column_group(
    const ObTableSchema &table_schema,
    const ObColumnGroupType &cg_type,
    const ObString &cg_name,
    const ObIArray<uint64_t> &column_ids,
    const uint64_t cg_id,
    ObColumnGroupSchema &column_group)
{
  int ret = OB_SUCCESS;
  if (cg_name.empty() || (cg_type >= ObColumnGroupType::MAX_COLUMN_GROUP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cg_name), K(cg_type), "column_id_cnt", column_ids.count());
  } else if (OB_FAIL(ObSchemaUtils::build_column_group(table_schema, session_info_->get_effective_tenant_id(), cg_type, cg_name,
                                                column_ids, cg_id, column_group))) {
      LOG_WARN("fail to build column group", K(ret));
  }
  return ret;
}



bool ObDDLResolver::need_column_group(const ObTableSchema &table_schema)
{
  return table_schema.is_user_table() || table_schema.is_tmp_table() || table_schema.is_index_table();
}

int ObDDLResolver::resolve_column_skip_index(
    const ParseNode &skip_index_node,
    ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  const ParseNode *type_list_node = nullptr;
  ObSkipIndexColumnAttr skip_index_column_attr;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.2, skip index feature is not supported", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.2, skip index");
  } else if (OB_UNLIKELY(1 != skip_index_node.num_child_ || T_COL_SKIP_INDEX != skip_index_node.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid skip index node", K(ret), K(skip_index_node.num_child_), K(skip_index_node.type_));
  } else {
    if (OB_ISNULL(type_list_node = skip_index_node.children_[0])) {
      // empty specified type list, e.g:
      // alter table t1 modify column c1 skip_index()
      skip_index_column_attr.reset();
    } else if (OB_UNLIKELY(0 == type_list_node->num_child_ || T_COL_SKIP_INDEX_LIST != type_list_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid type list node", K(ret),
          K(type_list_node->num_child_), K(type_list_node->type_));
    } else if (is_skip_index_black_list_type(column_schema.get_data_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "build skip index on invalid type");
      LOG_WARN("not supported skip index on column with invalid column type", K(ret), K(column_schema));
    } else if (column_schema.get_skip_index_attr().has_sum() &&
               !can_agg_sum(column_schema.get_data_type())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "build skip index on invalid type");
      LOG_WARN("not supported skip index on column with invalid column type", K(ret), K(column_schema));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < type_list_node->num_child_; ++i) {
        const ParseNode *type_node = type_list_node->children_[i];
        if (OB_ISNULL(type_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null skip index type node", K(ret), KP(type_node));
        } else {
          switch (type_node->type_) {
          case T_COL_SKIP_INDEX_MIN_MAX: {
            skip_index_column_attr.set_min_max();
            break;
          }
          case T_COL_SKIP_INDEX_SUM: {
            skip_index_column_attr.set_sum();
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid skip index type", K(ret), K(i), K(type_node->type_));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "skip index type");
            break;
          }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      column_schema.set_skip_index_attr(skip_index_column_attr.get_packed_value());
    }
  }
  return ret;
}

int ObDDLResolver::check_skip_index(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(table_schema.check_skip_index_valid())) {
    LOG_WARN("failed to check if skip index schema is valid", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
