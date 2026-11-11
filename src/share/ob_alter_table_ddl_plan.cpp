/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_alter_table_ddl_plan.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_rpc_struct.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/schema/ob_column_schema.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;

namespace oceanbase {
namespace share {
const char *ONLINE_DDL_CATEGORY = "online";
const char *OFFLINE_DDL_CATEGORY = "offline";
const char *OTHER_ALGORITHM = "other";
const char *INSTANT_ALGORITHM = "instant";
const char *UNKNOWN_STATE = "unknown";
const char *IMPACT_STR_TRUE = "true";
const char *IMPACT_STR_FALSE = "false";

const char *ddl_impact_to_str(const ObAlterTableDDLPlan::ObDDLImpact impact)
{
  const char *impact_str = UNKNOWN_STATE;
  if (ObAlterTableDDLPlan::DDL_IMPACT_POSITIVE == impact) {
    impact_str = IMPACT_STR_TRUE;
  } else if (ObAlterTableDDLPlan::DDL_IMPACT_NEGATIVE == impact) {
    impact_str = IMPACT_STR_FALSE;
  }
  return impact_str;
}

ObAlterTableDDLPlan::ObAlterTableDDLPlan()
{
  reset();
}

void ObAlterTableDDLPlan::reset()
{
  async_task_flags_ = ALTER_TABLE_ASYNC_TASK_NOT_DECIDE;
  execution_ddl_type_ = DDL_INVALID;
  explain_ddl_type_ = DDL_INVALID;
  execution_path_ = ALTER_TABLE_PATH_INVALID;
  lock_strategy_ = ALTER_TABLE_LOCK_NOT_DECIDE;
  blocks_dml_ = DDL_IMPACT_UNKNOWN;
  data_accessed_ = DDL_IMPACT_UNKNOWN;
  table_rewritten_ = DDL_IMPACT_UNKNOWN;
  ddl_need_retry_at_executor_ = false;
  has_runtime_dependency_ = false;
}

bool is_blocking_lock_strategy(const ObAlterTableLockStrategy lock_strategy)
{
  return ALTER_TABLE_LOCK_EXCLUSIVE == lock_strategy ||
         ALTER_TABLE_LOCK_SHARE == lock_strategy ||
         ALTER_TABLE_LOCK_SHARE_ROW_EXCLUSIVE == lock_strategy;
}

bool ObAlterTableDDLPlan::is_offline_category() const
{
  return ALTER_TABLE_PATH_OFFLINE == execution_path_ ||
         is_double_table_long_running_ddl(explain_ddl_type_) ||
         is_drop_schema_block_concurrent_trans(explain_ddl_type_) ||
         is_blocking_lock_strategy(lock_strategy_);
}

bool ObAlterTableDDLPlan::is_online_category() const
{
  bool is_online = false;
  if (!is_offline_category() &&
      ObDDLType::DDL_INVALID != explain_ddl_type_ &&
      !has_runtime_dependency_) {
    if (ObDDLType::DDL_NORMAL_TYPE == explain_ddl_type_) {
      is_online = ALTER_TABLE_PATH_IN_TRANS == execution_path_;
    } else {
      is_online = true;
    }
  }
  return is_online;
}

const char *ObAlterTableDDLPlan::get_explain_ddl_type_str() const
{
  return get_ddl_type(explain_ddl_type_);
}

const char *ObAlterTableDDLPlan::get_explain_category_str() const
{
  const char *category = UNKNOWN_STATE;
  if (is_offline_category()) {
    category = OFFLINE_DDL_CATEGORY;
  } else if (is_online_category()) {
    category = ONLINE_DDL_CATEGORY;
  }
  return category;
}

const char *ObAlterTableDDLPlan::get_explain_blocks_dml_str() const
{
  return ddl_impact_to_str(blocks_dml_);
}

const char *ObAlterTableDDLPlan::get_explain_algorithm_str() const
{
  const char *algorithm = OTHER_ALGORITHM;
  if (ObDDLImpact::DDL_IMPACT_NEGATIVE == data_accessed_) {
    algorithm = INSTANT_ALGORITHM;
  }
  return algorithm;
}

int ObAlterTableDDLPlan::build_explain_message(ObSqlString &message) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(message.assign_fmt("data_accessed=%s,table_rewritten=%s",
                                 ddl_impact_to_str(data_accessed_),
                                 ddl_impact_to_str(table_rewritten_)))) {
    LOG_WARN("failed to assign explain ddl message", KR(ret), K(*this));
  }
  return ret;
}

void ObAlterTableDDLPlan::analyze_explain_attributes(
    const ObAlterTableArg *alter_table_arg,
    const ObTableSchema *orig_table_schema)
{
  if (is_online_category()) {
    blocks_dml_ = ObDDLImpact::DDL_IMPACT_NEGATIVE;
  } else if (is_offline_category()) {
    blocks_dml_ = ObDDLImpact::DDL_IMPACT_POSITIVE;
  } else {
    blocks_dml_ = ObDDLImpact::DDL_IMPACT_UNKNOWN;
  }

  if (is_double_table_long_running_ddl(explain_ddl_type_)) {
    table_rewritten_ = ObDDLImpact::DDL_IMPACT_POSITIVE;
  } else {
    table_rewritten_ = ObDDLImpact::DDL_IMPACT_NEGATIVE;
  }

  bool accessed = false;
  if (is_long_running_ddl(execution_ddl_type_)) {
    accessed = true;
  } else if (OB_NOT_NULL(alter_table_arg) && alter_table_arg->is_split_partition()) {
    accessed = true;
  } else if (ALTER_TABLE_ASYNC_TASK_NOT_DECIDE != async_task_flags_) {
    accessed = ObAlterTableDDLHelper::need_main_table_data_access_from_async_task(async_task_flags_);
  }

  if (accessed) {
    data_accessed_ = ObDDLImpact::DDL_IMPACT_POSITIVE;
  } else if (is_online_category() ||
             ALTER_TABLE_PATH_EXCHANGE_PARTITION == execution_path_ ||
             (OB_NOT_NULL(alter_table_arg) &&
              is_offline_category() &&
              !has_runtime_dependency_ &&
              ObAlterTableDDLHelper::is_only_alter_table_option(*alter_table_arg))) {
    data_accessed_ = ObDDLImpact::DDL_IMPACT_NEGATIVE;
  } else {
    data_accessed_ = ObDDLImpact::DDL_IMPACT_UNKNOWN;
  }
}

bool ObAlterTableDDLHelper::has_multiple_index_action_types(const ObAlterTableArg &alter_table_arg)
{
  bool has_multiple = false;
  ObIndexArg::IndexActionType seen_type = ObIndexArg::INVALID_ACTION;
  for (int64_t i = 0; !has_multiple && i < alter_table_arg.index_arg_list_.count(); ++i) {
    const ObIndexArg *index_arg = alter_table_arg.index_arg_list_.at(i);
    if (OB_ISNULL(index_arg) || ObIndexArg::INVALID_ACTION == index_arg->index_action_type_) {
    } else if (ObIndexArg::INVALID_ACTION == seen_type) {
      seen_type = index_arg->index_action_type_;
    } else if (seen_type != index_arg->index_action_type_) {
      has_multiple = true;
    }
  }
  if (!has_multiple && !alter_table_arg.rebuild_index_arg_list_.empty() && ObIndexArg::INVALID_ACTION != seen_type) {
    has_multiple = true;
  }
  return has_multiple;
}

bool ObAlterTableDDLHelper::is_hybrid(const ObAlterTableArg &alter_table_arg)
{
  int64_t op_cats = 0;
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  if (alter_table_arg.is_alter_columns_) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_indexs_ ||
      !alter_table_arg.index_arg_list_.empty() ||
      !alter_table_arg.rebuild_index_arg_list_.empty()) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_partitions_ ||
      alter_table_arg.is_alter_random_partition_ ||
      alter_table_arg.is_direct_load_partition_ ||
      ObAlterTableArg::NO_OPERATION != alter_table_arg.alter_part_type_ ||
      !alter_table_arg.get_part_storage_cache_policy().empty()) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_options_ || !alter_table_schema.alter_option_bitset_.is_empty()) {
    ++op_cats;
  }
  if (ObAlterTableArg::CONSTRAINT_NO_OPERATION != alter_table_arg.alter_constraint_type_) {
    ++op_cats;
  }
  if (!alter_table_arg.foreign_key_arg_list_.empty()) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_mview_attributes_) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_mlog_attributes_) {
    ++op_cats;
  }
  if (alter_table_arg.is_convert_to_character_) {
    ++op_cats;
  }
  if (alter_table_arg.is_alter_column_group_delayed_) {
    ++op_cats;
  }
  if (alter_table_arg.alter_auto_partition_attr_) {
    ++op_cats;
  }
  return op_cats > 1 || has_multiple_index_action_types(alter_table_arg);
}

bool ObAlterTableDDLHelper::is_rename_column_only(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema *orig_table_schema)
{
  bool is_rename_only = false;
  if (OB_NOT_NULL(orig_table_schema) && !is_hybrid(alter_table_arg) && alter_table_arg.is_alter_columns_) {
    const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    is_rename_only = it != it_end;
    for (; is_rename_only && it != it_end; ++it) {
      const AlterColumnSchema *alter_column_schema = static_cast<const AlterColumnSchema *>(*it);
      const ObColumnSchemaV2 *orig_column_schema = OB_ISNULL(alter_column_schema)
          ? nullptr
          : orig_table_schema->get_column_schema(alter_column_schema->get_origin_column_name());
      bool is_same_type = false;
      bool is_change_order = false;
      if (OB_ISNULL(alter_column_schema)
          || OB_ISNULL(orig_column_schema)
          || OB_DDL_CHANGE_COLUMN != alter_column_schema->alter_type_
          || ObColumnNameHashWrapper(alter_column_schema->get_origin_column_name())
              == ObColumnNameHashWrapper(alter_column_schema->get_column_name_str())
          || alter_column_schema->is_set_default_
          || alter_column_schema->is_drop_default_
          || alter_column_schema->is_set_nullable_
          || alter_column_schema->is_set_comment_
          || alter_column_schema->is_primary_key_
          || alter_column_schema->is_unique_key_
          || alter_column_schema->is_autoincrement() != orig_column_schema->is_autoincrement()
          || alter_column_schema->get_column_flags() != orig_column_schema->get_column_flags()
          || OB_SUCCESS != ObTableSchema::check_is_exactly_same_type(
                 *orig_column_schema, *alter_column_schema, is_same_type)
          || !is_same_type
          || OB_SUCCESS != ObAlterColumnDDLHelper::check_is_change_column_order(
                 *orig_table_schema, *alter_column_schema, is_change_order)
          || is_change_order) {
        is_rename_only = false;
      }
    }
  }
  return is_rename_only;
}

bool ObAlterTableDDLHelper::is_only_alter_table_option(const ObAlterTableArg &alter_table_arg)
{
  const ObBitSet<> &option_set = alter_table_arg.alter_table_schema_.alter_option_bitset_;
  return !is_hybrid(alter_table_arg) &&
         (alter_table_arg.is_alter_options_ || !option_set.is_empty());
}

bool ObAlterTableDDLHelper::is_fk_validate_required(const ObAlterTableArg &alter_table_arg)
{
  bool need_validate = false;
  if (1 == alter_table_arg.foreign_key_arg_list_.count()) {
    const ObCreateForeignKeyArg &fk_arg = alter_table_arg.foreign_key_arg_list_.at(0);
    need_validate = alter_table_arg.foreign_key_checks_ &&
                    fk_arg.need_validate_data_ &&
                    ((!fk_arg.is_modify_fk_state_ && fk_arg.validate_flag_)
                     || (fk_arg.is_modify_validate_flag_ && fk_arg.validate_flag_));
  }
  return need_validate;
}

bool ObAlterTableDDLHelper::has_single_validate_data_fk(const ObAlterTableArg &alter_table_arg)
{
  return alter_table_arg.foreign_key_checks_ &&
         1 == alter_table_arg.foreign_key_arg_list_.count() &&
         alter_table_arg.foreign_key_arg_list_.at(0).need_validate_data_;
}

bool ObAlterTableDDLHelper::is_rename_and_need_table_lock(
    const AlterTableSchema &alter_table_schema,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version)
{
  return alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::TABLE_NAME) &&
         orig_table_schema.is_user_table() &&
         ((MOCK_DATA_VERSION_4_2_3_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) ||
          DATA_VERSION_4_3_2_0 <= tenant_data_version);
}

bool ObAlterTableDDLHelper::is_alter_merge_engine(
    const AlterTableSchema &alter_table_schema,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version)
{
  return  alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::MERGE_ENGINE_TYPE) &&
          tenant_data_version >= DATA_VERSION_5_0_1_0 &&
          alter_table_schema.get_merge_engine_type() != orig_table_schema.get_merge_engine_type();
}

bool ObAlterTableDDLHelper::is_change_to_compaction_ttl_table(
    const AlterTableSchema &alter_table_schema,
    const ObTableSchema &orig_table_schema)
{
  return ObCompactionTTLUtil::check_change_to_compaction_ttl_table(alter_table_schema, orig_table_schema);
}

bool ObAlterTableDDLHelper::need_exclusive_table_lock_for_alter_table(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version)
{
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  bool need_lock = is_rename_and_need_table_lock(alter_table_schema, orig_table_schema, tenant_data_version) ||
                   is_alter_merge_engine(alter_table_schema, orig_table_schema, tenant_data_version) ||
                   is_change_to_compaction_ttl_table(alter_table_schema, orig_table_schema);
  return need_lock;
}

bool ObAlterTableDDLHelper::need_exclusive_partition_lock_for_alter_table(
    const ObAlterTableArg &alter_table_arg)
{
  bool need_lock = false;
  switch (alter_table_arg.alter_part_type_) {
    case ObAlterTableArg::DROP_PARTITION:
    case ObAlterTableArg::DROP_SUB_PARTITION:
    case ObAlterTableArg::TRUNCATE_PARTITION:
    case ObAlterTableArg::TRUNCATE_SUB_PARTITION:
    case ObAlterTableArg::RENAME_PARTITION:
    case ObAlterTableArg::RENAME_SUB_PARTITION:
    case ObAlterTableArg::EXCHANGE_PARTITION:
    case ObAlterTableArg::EXCHANGE_SUBPARTITION:
      need_lock = true;
      break;
    default:
      need_lock = false;
      break;
  }
  return need_lock;
}

bool ObAlterTableDDLHelper::need_check_constraint_validity(const ObAlterTableArg &alter_table_arg)
{
  bool need_check_validity = false;
  if (ObAlterTableArg::ADD_CONSTRAINT == alter_table_arg.alter_constraint_type_ ||
      ObAlterTableArg::ALTER_CONSTRAINT_STATE == alter_table_arg.alter_constraint_type_) {
    ObTableSchema::const_constraint_iterator iter = alter_table_arg.alter_table_schema_.constraint_begin();
    for (; !need_check_validity && iter != alter_table_arg.alter_table_schema_.constraint_end(); iter++) {
      need_check_validity = (*iter)->get_need_validate_data();
    }
  }
  return need_check_validity;
}

bool ObAlterTableDDLHelper::has_add_or_rebuild_index_in_alter_table(
    const ObAlterTableArg &alter_table_arg)
{
  bool has = false;
  if (alter_table_arg.is_alter_indexs_) {
    for (int64_t i = 0; !has && i < alter_table_arg.index_arg_list_.count(); ++i) {
      const ObIndexArg *index_arg = alter_table_arg.index_arg_list_.at(i);
      if (OB_NOT_NULL(index_arg)) {
        has = ObIndexArg::ADD_INDEX == index_arg->index_action_type_ ||
              ObIndexArg::REBUILD_INDEX == index_arg->index_action_type_;
      }
    }
  }
  return has;
}

bool ObAlterTableDDLHelper::need_alter_table_auto_increment_in_trans(
    const AlterTableSchema &alter_table_schema,
    const uint64_t autoinc_column_id)
{
  return alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::AUTO_INCREMENT) &&
         0 != autoinc_column_id;
}

bool ObAlterTableDDLHelper::need_exclusive_table_lock_for_reduce_auto_increment(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema)
{
  const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
  return alter_table_schema.alter_option_bitset_.has_member(ObAlterTableArg::AUTO_INCREMENT) &&
         0 != orig_table_schema.get_autoinc_column_id() &&
         alter_table_schema.get_auto_increment() < orig_table_schema.get_auto_increment();
}

bool ObAlterTableDDLHelper::need_read_main_table_for_modify_autoinc(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema)
{
  return need_exclusive_table_lock_for_reduce_auto_increment(alter_table_arg, orig_table_schema);
}

bool ObAlterTableDDLHelper::need_main_table_data_access_from_async_task(const uint64_t async_task_flags)
{
  const uint64_t main_table_flags = ALTER_TABLE_ASYNC_TASK_CREATE_INDEX |
                                    ALTER_TABLE_ASYNC_TASK_CHECK_CONSTRAINT |
                                    ALTER_TABLE_ASYNC_TASK_FK_VALIDATE |
                                    ALTER_TABLE_ASYNC_TASK_SPLIT_PARTITION |
                                    ALTER_TABLE_ASYNC_TASK_MODIFY_AUTOINC;
  return 0 != (async_task_flags & main_table_flags);
}

bool ObAlterTableDDLHelper::need_drop_column_aux_async_task(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema)
{
  bool need_aux = false;
  if (alter_table_arg.is_alter_columns_) {
    const AlterTableSchema &alter_table_schema = alter_table_arg.alter_table_schema_;
    ObTableSchema::const_column_iterator it = alter_table_schema.column_begin();
    ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
    for (; !need_aux && it != it_end; ++it) {
      const AlterColumnSchema *alter_column_schema = static_cast<const AlterColumnSchema *>(*it);
      if (OB_ISNULL(alter_column_schema) || OB_DDL_DROP_COLUMN != alter_column_schema->alter_type_) {
      } else {
        const ObColumnSchemaV2 *column_schema =
            orig_table_schema.get_column_schema(alter_column_schema->get_column_name_str());
        if (OB_NOT_NULL(column_schema) && is_lob_storage(column_schema->get_data_type())) {
          need_aux = true;
        }
      }
    }
  }
  return need_aux;
}

int ObAlterTableDDLHelper::need_exclusive_table_lock_for_rowkey_aux_index(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    bool &need_lock)
{
  int ret = OB_SUCCESS;
  need_lock = false;
  if (!alter_table_arg.is_alter_indexs_) {
  } else {
    ObDocIDType doc_id_type = ObDocIDType::INVALID;
    ObDocIDType vid_type = ObDocIDType::INVALID;
    if (OB_FAIL(ObFtsIndexBuilderUtil::determine_docid_type(orig_table_schema, doc_id_type))) {
      LOG_WARN("failed to determine docid type", KR(ret), K(orig_table_schema));
    } else if (OB_FAIL(ObVectorIndexUtil::determine_vid_type(orig_table_schema, vid_type))) {
      LOG_WARN("failed to determine vid type", KR(ret), K(orig_table_schema));
    } else {
      const bool need_rowkey_doc = ObDocIDType::TABLET_SEQUENCE == doc_id_type;
      const bool need_rowkey_vid = ObDocIDType::TABLET_SEQUENCE == vid_type;
      bool rowkey_doc_not_exist = false;
      bool rowkey_vid_not_exist = false;
      uint64_t rowkey_doc_tid = OB_INVALID_ID;
      uint64_t rowkey_vid_tid = OB_INVALID_ID;
      if (need_rowkey_doc && OB_FAIL(orig_table_schema.get_rowkey_doc_tid(rowkey_doc_tid))) {
        if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
          rowkey_doc_not_exist = true;
        } else {
          LOG_WARN("failed to get rowkey doc table id", KR(ret), K(orig_table_schema));
        }
      }
      if (OB_SUCC(ret) && need_rowkey_vid && OB_FAIL(orig_table_schema.get_rowkey_vid_tid(rowkey_vid_tid))) {
        if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
          rowkey_vid_not_exist = true;
        } else {
          LOG_WARN("failed to get rowkey vid table id", KR(ret), K(orig_table_schema));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && !need_lock && i < alter_table_arg.index_arg_list_.count(); ++i) {
        const ObIndexArg *index_arg = alter_table_arg.index_arg_list_.at(i);
        if (OB_ISNULL(index_arg) || ObIndexArg::ADD_INDEX != index_arg->index_action_type_) {
        } else {
          const ObCreateIndexArg *create_index_arg = static_cast<const ObCreateIndexArg *>(index_arg);
          const ObIndexType index_type = create_index_arg->index_type_;
          if (need_rowkey_doc &&
              (is_fts_or_multivalue_index(index_type) || is_vec_spiv_index(index_type)) && rowkey_doc_not_exist) {
            need_lock = true;
          } else if (need_rowkey_vid &&
                     ObIndexBuilderUtil::is_do_create_dense_vec_index(index_type) &&
                     is_vec_hnsw_index(index_type) &&
                     !create_index_arg->is_rebuild_index_ &&
                     rowkey_vid_not_exist) {
            need_lock = true;
          }
        }
      }
    }
  }
  return ret;
}

void ObAlterTableDDLHelper::refine_explain_ddl_type(
    const ObAlterTableArg &alter_table_arg,
    const schema::ObTableSchema *orig_table_schema,
    ObDDLType execution_ddl_type,
    ObDDLType &explain_ddl_type)
{
  const ObBitSet<> &option_set = alter_table_arg.alter_table_schema_.alter_option_bitset_;
  const bool is_modify_auto_increment_only = alter_table_arg.is_alter_options_
                                            && 1 == option_set.num_members()
                                            && option_set.has_member(ObAlterTableArg::AUTO_INCREMENT);
  if (ObDDLType::DDL_INVALID != execution_ddl_type && ObDDLType::DDL_NORMAL_TYPE != execution_ddl_type) {
      explain_ddl_type = execution_ddl_type;
  } else if (is_fk_validate_required(alter_table_arg)) {
    explain_ddl_type = ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT;
  } else if (is_modify_auto_increment_only) {
    explain_ddl_type = ObDDLType::DDL_MODIFY_AUTO_INCREMENT;
  } else if (is_rename_column_only(alter_table_arg, orig_table_schema)) {
    explain_ddl_type = ObDDLType::DDL_CHANGE_COLUMN_NAME;
  } else if (ObDDLType::DDL_INVALID == explain_ddl_type) {
    explain_ddl_type = ObDDLType::DDL_NORMAL_TYPE;
  }
}

int ObAlterTableDDLHelper::analyze_lock_strategy(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const uint64_t tenant_data_version,
    ObAlterTableLockStrategy &lock_strategy)
{
  int ret = OB_SUCCESS;
  bool need_lock = false;
  lock_strategy = ALTER_TABLE_LOCK_NOT_DECIDE;
  if (need_exclusive_table_lock_for_alter_table(alter_table_arg, orig_table_schema, tenant_data_version)) {
    lock_strategy = ALTER_TABLE_LOCK_EXCLUSIVE;
  } else if (need_exclusive_partition_lock_for_alter_table(alter_table_arg)) {
    lock_strategy = ALTER_TABLE_LOCK_EXCLUSIVE;
  } else if (alter_table_arg.is_split_partition()) {
    lock_strategy = ALTER_TABLE_LOCK_ROW_EXCLUSIVE;
  } else if (need_exclusive_table_lock_for_reduce_auto_increment(alter_table_arg, orig_table_schema)) {
    lock_strategy = ALTER_TABLE_LOCK_EXCLUSIVE;
  } else if (OB_FAIL(need_exclusive_table_lock_for_rowkey_aux_index(alter_table_arg, orig_table_schema, need_lock))) {
    LOG_WARN("failed to need exclusive table lock for rowkey aux index", KR(ret));
  } else if (need_lock) {
    lock_strategy = ALTER_TABLE_LOCK_EXCLUSIVE;
  } else if (need_check_constraint_validity(alter_table_arg) || is_fk_validate_required(alter_table_arg)) {
    lock_strategy = ALTER_TABLE_LOCK_ROW_EXCLUSIVE;
  }
  return ret;
}

int ObAlterTableDDLHelper::analyze_async_tasks(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    const ObAlterTableDDLPlan &plan,
    uint64_t &async_task_flags)
{
  int ret = OB_SUCCESS;
  async_task_flags = ALTER_TABLE_ASYNC_TASK_NOT_DECIDE;
  if (ALTER_TABLE_PATH_IN_TRANS == plan.get_execution_path()) {
    if (has_add_or_rebuild_index_in_alter_table(alter_table_arg)) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_CREATE_INDEX;
    }
    if (OB_SUCC(ret) && need_check_constraint_validity(alter_table_arg)) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_CHECK_CONSTRAINT;
    }
    if (OB_SUCC(ret) && is_fk_validate_required(alter_table_arg)) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_FK_VALIDATE;
    }
    if (OB_SUCC(ret) && alter_table_arg.is_split_partition()) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_SPLIT_PARTITION;
    }
    if (OB_SUCC(ret) &&
        ObDDLType::DDL_DROP_COLUMN_INSTANT == plan.get_execution_ddl_type() &&
        need_drop_column_aux_async_task(alter_table_arg, orig_table_schema)) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_DROP_COL_AUX;
    }
    if (OB_SUCC(ret) &&
        need_read_main_table_for_modify_autoinc(alter_table_arg, orig_table_schema)) {
      async_task_flags |= ALTER_TABLE_ASYNC_TASK_MODIFY_AUTOINC;
    }
  }
  return ret;
}

int ObAlterTableDDLHelper::analyze_runtime_dependency(
    const ObAlterTableArg &alter_table_arg,
    const ObTableSchema &orig_table_schema,
    bool &has_runtime_dependency)
{
  int ret = OB_SUCCESS;
  has_runtime_dependency = need_alter_table_auto_increment_in_trans(
      alter_table_arg.alter_table_schema_, orig_table_schema.get_autoinc_column_id())
          && !need_exclusive_table_lock_for_reduce_auto_increment(alter_table_arg, orig_table_schema);
  return ret;
}

int ObAlterTableDDLHelper::build_plan(
    ObAlterTableArg &alter_table_arg,
    const uint64_t tenant_data_version,
    const ObTableSchema *orig_table_schema,
    ObDDLType execution_ddl_type,
    const bool ddl_need_retry_at_executor,
    ObAlterTableDDLPlan &plan)
{
  int ret = OB_SUCCESS;
  plan.set_ddl_need_retry_at_executor(ddl_need_retry_at_executor);
  if (is_long_running_ddl(execution_ddl_type)) {
    plan.set_execution_path(ALTER_TABLE_PATH_OFFLINE, execution_ddl_type);
  } else {
    plan.set_execution_path(ALTER_TABLE_PATH_IN_TRANS, execution_ddl_type);
  }
  // explain_ddl_type / lock / async-task / attributes are display-only.
  // Real ALTER only consumes execution_ddl_type_ and ddl_need_retry_at_executor_.
  if (alter_table_arg.is_explain_) {
    ObDDLType explain_ddl_type = ObDDLType::DDL_NORMAL_TYPE;
    refine_explain_ddl_type(alter_table_arg, orig_table_schema, execution_ddl_type, explain_ddl_type);
    plan.set_explain_ddl_type(explain_ddl_type);
    if (OB_NOT_NULL(orig_table_schema)) {
      ObAlterTableLockStrategy lock_strategy = ALTER_TABLE_LOCK_NOT_DECIDE;
      uint64_t async_task_flags = 0;
      bool has_runtime_dependency = false;
      if (OB_FAIL(analyze_lock_strategy(alter_table_arg,
                                        *orig_table_schema,
                                        tenant_data_version,
                                        lock_strategy))) {
        LOG_WARN("failed to analyze lock strategy", KR(ret));
      } else if (OB_FAIL(analyze_async_tasks(alter_table_arg,
                                             *orig_table_schema,
                                             plan,
                                             async_task_flags))) {
        LOG_WARN("failed to analyze async tasks", KR(ret));
      } else if (OB_FAIL(analyze_runtime_dependency(alter_table_arg,
                                                    *orig_table_schema,
                                                    has_runtime_dependency))) {
        LOG_WARN("failed to analyze runtime dependency", KR(ret));
      } else {
        plan.set_lock_strategy(lock_strategy);
        plan.set_async_task_flags(async_task_flags);
        plan.set_has_runtime_dependency(has_runtime_dependency);
      }
    }
    if (OB_SUCC(ret)) {
      plan.analyze_explain_attributes(&alter_table_arg, orig_table_schema);
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
