/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "rootserver/ob_column_group_ddl_helper.h"
#include "rootserver/ob_ddl_operator.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace rootserver
{

int ObColumnGroupDDLHelper::add_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                                             const share::schema::ObTableSchema &ori_table_schema,
                                             share::schema::ObTableSchema &new_table_schema)
{
  UNUSED(ori_table_schema);
  int ret = OB_SUCCESS;
  if (alter_table_arg.alter_table_schema_.get_column_group_count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter table arg has no column groups", K(ret), K(alter_table_arg));
  } else if (alter_table_arg.based_schema_object_infos_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("based info object count <=0 cannot promise consist", K(ret));
  } else {
    ObTableSchema::const_column_group_iterator iter_begin =
        alter_table_arg.alter_table_schema_.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end =
        alter_table_arg.alter_table_schema_.column_group_end();

    for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
      ObColumnGroupSchema *column_group = *iter_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should not be null", K(ret), K(alter_table_arg));
      } else if (column_group->get_column_group_id() <= new_table_schema.get_max_used_column_group_id()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("added column group should have greater id than used column id",
                 K(ret), K(new_table_schema.get_max_used_column_group_id()),
                 K(column_group->get_column_group_id()));
      } else if (OB_FAIL(new_table_schema.add_column_group(*column_group))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_ERR_COLUMN_GROUP_DUPLICATE;
          LOG_WARN("fail to add column group, column group duplicate", K(ret), K(new_table_schema));
          char err_msg[OB_MAX_COLUMN_GROUP_NAME_LENGTH] = {'\0'};
          ObString err_msg_str(OB_MAX_COLUMN_GROUP_NAME_LENGTH, 0 /*length*/, err_msg);
          int tmp_ret = column_group->get_column_group_type_name(err_msg_str);
          if (tmp_ret != OB_SUCCESS) {
            LOG_WARN("fail to get readable column group name", K(tmp_ret), KPC(column_group));
          } else {
            LOG_USER_ERROR(OB_ERR_COLUMN_GROUP_DUPLICATE, err_msg_str.length(), err_msg_str.ptr());
          }
        } else {
          LOG_WARN("fail to add column group to table schema", K(ret), K(new_table_schema), KPC(column_group));
        }
      }
    }

    bool build_old_version_cg = false;
    if (FAILEDx(ObSchemaUtils::check_build_old_version_column_group(new_table_schema, build_old_version_cg))) {
      LOG_WARN("fail to check build old version column group", K(ret), K(new_table_schema));
    } else if (build_old_version_cg) {
      if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(new_table_schema))) {
        LOG_WARN("fail to adjust rowkey column group when add column group", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(new_table_schema))) {
        LOG_WARN("fail to alter default column group", K(ret));
      }
    } else {
      if (OB_FAIL(ObColumnGroupDDLHelper::rebuild_column_groups(new_table_schema))) {
        LOG_WARN("fail to adjust cg for offline", KR(ret));
      }
    }
  }
  return ret;
}

int ObColumnGroupDDLHelper::drop_column_group(const obrpc::ObAlterTableArg &alter_table_arg,
                                              const share::schema::ObTableSchema &ori_table_schema,
                                              share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  bool has_unused_column = false;
  if (alter_table_arg.alter_table_schema_.get_column_group_count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter table arg has no column group", K(ret), K(alter_table_arg.alter_table_schema_));
  } else if (alter_table_arg.based_schema_object_infos_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("based schema object info count <= 0, cannot promise column consist", K(ret));
  } else {
    ObTableSchema::const_column_group_iterator iter_begin =
        alter_table_arg.alter_table_schema_.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end =
        alter_table_arg.alter_table_schema_.column_group_end();

    for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
      const ObColumnGroupSchema *column_group = *iter_begin;
      ObColumnGroupSchema *ori_column_group = nullptr;
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group in origin table should not be null", K(ret));
      } else if (OB_FAIL(ori_table_schema.get_column_group_by_name(column_group->get_column_group_name(),
                                                                   ori_column_group))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_COLUMN_GROUP_NOT_FOUND;
          LOG_WARN("cannot found column group", K(ret), KPC(column_group), K(ori_table_schema));
          char err_msg[OB_MAX_COLUMN_GROUP_NAME_LENGTH] = {'\0'};
          ObString err_msg_str(OB_MAX_COLUMN_GROUP_NAME_LENGTH, 0, err_msg);
          int tmp_ret = column_group->get_column_group_type_name(err_msg_str);
          if (tmp_ret != OB_SUCCESS){
            LOG_WARN("fail to get readable column group name");
          } else {
            LOG_USER_ERROR(OB_COLUMN_GROUP_NOT_FOUND, err_msg_str.length(), err_msg_str.ptr());
          }
        } else {
          LOG_WARN("fail to get column group by name", K(ret), K(ori_table_schema), KPC(column_group));
        }
      } else if (OB_ISNULL(ori_column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should not be null", K(ret), KPC(column_group));
      } else if (OB_FAIL(ori_table_schema.has_unused_column(has_unused_column))) {
        LOG_WARN("fail to check orig table schema has unused column", KR(ret), K(ori_table_schema));
      } else if (OB_FAIL(new_table_schema.remove_column_group(ori_column_group->get_column_group_name()))) {
        if (OB_HASH_NOT_EXIST == ret && has_unused_column) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to remove column group from new table schema", KR(ret));
        }
      }
    }

    bool build_old_version_cg = false;
    if (FAILEDx(ObSchemaUtils::check_build_old_version_column_group(new_table_schema, build_old_version_cg))) {
      LOG_WARN("fail to check build old version column group", K(ret), K(new_table_schema));
    } else if (build_old_version_cg) {
      if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(new_table_schema))) {
        LOG_WARN("fail to adjust rowkey column group when add column group", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(new_table_schema))) {
        LOG_WARN("fail to alter default column group", K(ret));
      }
    } else {
      if (OB_FAIL(ObColumnGroupDDLHelper::rebuild_column_groups(new_table_schema))) {
        LOG_WARN("fail to adjust cg for offline", KR(ret));
      }
    }
  }
  return ret;
}

int ObColumnGroupDDLHelper::alter_column_group(obrpc::ObAlterTableArg &alter_table_arg,
                                               const share::schema::ObTableSchema &orig_table_schema,
                                               share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (alter_table_arg.alter_table_schema_.get_column_group_count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter table arg has no column group", K(ret), K(alter_table_arg.alter_table_schema_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(orig_table_schema.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to get compat version", K(ret), K(orig_table_schema), K(compat_version));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("compat version not support", K(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.3, alter column group");
  } else if (alter_table_arg.based_schema_object_infos_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there is no schema object infos to promise consit", K(ret));
  } else {
    new_table_schema.set_column_store(true);
    switch (alter_table_arg.alter_table_schema_.alter_type_) {
      case share::schema::OB_DDL_ADD_COLUMN_GROUP: {
        if (OB_FAIL(ObColumnGroupDDLHelper::add_column_group(alter_table_arg, orig_table_schema, new_table_schema))) {
          LOG_WARN("fail to add column group to new table schema", K(ret));
        }
        break;
      }
      case share::schema::OB_DDL_DROP_COLUMN_GROUP: {
        if (OB_FAIL(ObColumnGroupDDLHelper::drop_column_group(alter_table_arg, orig_table_schema, new_table_schema))) {
          LOG_WARN("fail to dorp column in new table schema", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("recevive unexpected alter table actions", K(ret),
                 K(alter_table_arg.alter_table_schema_.alter_type_));
      }
    }
    LOG_DEBUG("ddl service alter column group finish", K(ret), K(orig_table_schema), K(new_table_schema));
  }
  return ret;
}

int ObColumnGroupDDLHelper::update_column_group_table_inplace(const share::schema::ObTableSchema &origin_table_schema,
                                                              const share::schema::ObTableSchema &new_table_schema,
                                                              ObDDLOperator &ddl_operator,
                                                              common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!origin_table_schema.is_valid()
               || !new_table_schema.is_valid()
               || !new_table_schema.is_column_store_supported())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_table_schema));
  } else if (OB_FAIL(ddl_operator.update_origin_column_group_with_new_schema(trans,
                                                                             origin_table_schema,
                                                                             new_table_schema))) {
    LOG_WARN("fail to clear origin table schema and insert new schema", K(ret),
                                                                        K(origin_table_schema),
                                                                        K(new_table_schema));
  }
  return ret;
}

int ObColumnGroupDDLHelper::rebuild_column_groups(ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  bool is_each_cg_exist = false;
  bool is_all_cg_exist = false;
  if (!new_table_schema.is_column_store_supported()) {
  } else if (OB_FAIL(new_table_schema.is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
    LOG_WARN("fail to check is all column group exist", K(ret));
  } else if (OB_FAIL(new_table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
    LOG_WARN("fail to check is each column group exist", K(ret));
  } else {
    new_table_schema.reset_column_group_info();
    bool build_old_version_cg = false;
    if (OB_FAIL(ObSchemaUtils::check_build_old_version_column_group(new_table_schema, build_old_version_cg))) {
      LOG_WARN("fail to check build old version column group", K(ret), K(new_table_schema));
    }
    ObTableSchema::const_column_iterator col_iter = new_table_schema.column_begin();
    for (; OB_SUCC(ret) && is_each_cg_exist && col_iter != new_table_schema.column_end(); col_iter++) {
      ObColumnSchemaV2 *col = *col_iter;
      ObColumnGroupSchema new_single_cg;
      new_single_cg.reset();
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group pointer should not be null", K(ret));
      } else if (col->is_virtual_generated_column()) {
      } else if (OB_FAIL(ObSchemaUtils::build_single_column_group(new_table_schema, col, new_table_schema.get_tenant_id(),
                                                                  build_old_version_cg ? new_table_schema.get_max_used_column_group_id() + 1 : new_table_schema.get_next_single_column_group_id(),
                                                                  new_single_cg))) {
        LOG_WARN("fail to build single column group", K(ret));
      } else if (OB_FAIL(new_table_schema.add_column_group(new_single_cg))) {
        LOG_WARN("fail to add new column group to table schema", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_all_cg_exist) {
      ObColumnGroupSchema new_cg;
      new_cg.reset();
      if (OB_FAIL(ObSchemaUtils::build_all_column_group(
                                    new_table_schema, new_table_schema.get_tenant_id(),
                                    build_old_version_cg ? new_table_schema.get_max_used_column_group_id() + 1 : ALL_COLUMN_GROUP_ID, new_cg))) {
        LOG_WARN("fail to build new all column group schema", K(ret));
      } else if (OB_FAIL(new_table_schema.add_column_group(new_cg))) {
        LOG_WARN("fail to add new column group to table schema", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<uint64_t> column_ids;
      ObColumnGroupSchema default_cg;
      default_cg.reset();
      if (OB_FAIL(ObSchemaUtils::build_column_group(new_table_schema, new_table_schema.get_tenant_id(),
                                        ObColumnGroupType::DEFAULT_COLUMN_GROUP,
                                        OB_DEFAULT_COLUMN_GROUP_NAME, column_ids,
                                        DEFAULT_TYPE_COLUMN_GROUP_ID, default_cg))) {
        LOG_WARN("fail to build column group", K(ret));
      } else if (OB_FAIL(new_table_schema.add_column_group(default_cg))) {
        LOG_WARN("failt to add default column group", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::alter_rowkey_column_group(new_table_schema))) {
        LOG_WARN("fail to alter rowkey column group", K(ret));
      } else if (OB_FAIL(ObSchemaUtils::alter_default_column_group(new_table_schema))) {
        LOG_WARN("fail to alter default column grouop schema", K(ret));
      }
    }
    if (!build_old_version_cg && FAILEDx(new_table_schema.adjust_column_group_array())) {
      LOG_WARN("fail to adjust column group array", K(ret), K(new_table_schema));
    }
  }
  return ret;
}

int ObColumnGroupDDLHelper::check_alter_column_group(
    const obrpc::ObAlterTableArg &alter_table_arg,
    const share::schema::ObTableSchema &orig_schema,
    ObDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  const AlterTableSchema &new_schema = alter_table_arg.alter_table_schema_;

  if (OB_DDL_ADD_COLUMN_GROUP != new_schema.alter_type_ &&
      OB_DDL_DROP_COLUMN_GROUP != new_schema.alter_type_) {
  } else if (OB_UNLIKELY(new_schema.get_column_group_count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, alter table arg don't have any column group when alter column group",
             K(ret), K(alter_table_arg.alter_table_schema_));
  } else if (OB_DDL_ADD_COLUMN_GROUP == new_schema.alter_type_) {
    ddl_type = alter_table_arg.is_alter_column_group_delayed_
             ? ObDDLType::DDL_ALTER_COLUMN_GROUP_DELAYED
             : ObDDLType::DDL_ALTER_COLUMN_GROUP;
  } else if (!alter_table_arg.is_alter_column_group_delayed_) {
    ddl_type = ObDDLType::DDL_ALTER_COLUMN_GROUP;
  } else {
    bool orig_has_all_cg = false;
    bool orig_has_each_cg = false;
    bool new_drop_all_cg = false;
    bool new_drop_each_cg = false;

    if (OB_FAIL(orig_schema.has_column_group(ObColumnGroupType::SINGLE_COLUMN_GROUP, orig_has_each_cg))) {
      LOG_WARN("failed to check if has each cg", K(ret));
    } else if (OB_FAIL(orig_schema.has_column_group(ObColumnGroupType::ALL_COLUMN_GROUP, orig_has_all_cg))) {
      LOG_WARN("failed to check if has all cg", K(ret));
    } else if (OB_FAIL(new_schema.has_column_group(ObColumnGroupType::SINGLE_COLUMN_GROUP, new_drop_each_cg))) {
      LOG_WARN("failed to check if drop each cg", K(ret));
    } else if (OB_FAIL(new_schema.has_column_group(ObColumnGroupType::ALL_COLUMN_GROUP, new_drop_all_cg))) {
      LOG_WARN("failed to check if drop all cg", K(ret));
    } else {
      const bool is_old_redundant_cs_table = orig_has_each_cg
                                          && orig_has_all_cg
                                          && (nullptr == orig_schema.get_hidden_rowkey_column_group());
      const bool is_only_drop_all_cg = new_drop_all_cg && !new_drop_each_cg;
      if (is_old_redundant_cs_table && is_only_drop_all_cg) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("old all cg && each cg table cannot do online drop all cg ddl with delayed", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "old all cg && each cg table cannot do online drop all cg ddl with delayed");
      } else {
        ddl_type = ObDDLType::DDL_ALTER_COLUMN_GROUP_DELAYED;
      }
    }
  }
  return ret;
}

int ObColumnGroupDDLHelper::add_column_to_column_group(
    const share::schema::ObTableSchema &origin_table_schema,
    const share::schema::AlterTableSchema &alter_table_schema,
    share::schema::ObTableSchema &new_table_schema,
    ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t cur_column_group_id = origin_table_schema.get_next_single_column_group_id();
  ObArray<uint64_t> column_ids;
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  AlterColumnSchema *alter_column_schema = nullptr;

  bool build_old_version_cg = false;
  if (!origin_table_schema.is_valid() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(origin_table_schema), K(alter_table_schema));
  } else if (!new_table_schema.is_column_store_supported()) {
  } else {
    if (OB_FAIL(ObSchemaUtils::check_build_old_version_column_group(origin_table_schema, build_old_version_cg))) {
      LOG_WARN("fail to get next single column group id", K(ret), K(origin_table_schema));
    } else if (build_old_version_cg) {
      cur_column_group_id = origin_table_schema.get_max_used_column_group_id() + 1;
    }
    for(; OB_SUCC(ret) && it_begin != it_end; it_begin++) {
      if (OB_ISNULL(alter_column_schema = static_cast<AlterColumnSchema *>(*it_begin))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("*it_begin is NULL", K(ret));
      } else if (alter_column_schema->alter_type_ == OB_DDL_ADD_COLUMN) {
        const ObColumnSchemaV2 *column_schema = nullptr;
        if (OB_ISNULL(column_schema = new_table_schema.get_column_schema(alter_column_schema->get_column_name_str()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema", K(ret), KPC(alter_column_schema), K(new_table_schema));
        } else if (column_schema->is_virtual_generated_column()) {
        } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
          LOG_WARN("fali to push back column id", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (column_ids.count() == 0){
    } else {
      bool is_all_cg_exist = false;
      bool is_each_cg_exist = false;
      if (OB_FAIL(new_table_schema.is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
        LOG_WARN("fail to check whether all cg exist", K(ret), K(new_table_schema));
      } else if (OB_FAIL(new_table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
        LOG_WARN("fail to check whether each cg exist", K(ret), K(new_table_schema));
      }

      if (OB_FAIL(ret)) {
      } else if (is_each_cg_exist) {
        HEAP_VAR(ObTableSchema, tmp_table) {
        if (OB_FAIL(tmp_table.assign(new_table_schema))) {
          LOG_WARN("fail to assign", K(ret), K(new_table_schema), K(tmp_table));
        }
        tmp_table.reset_column_group_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          ObColumnGroupSchema cg_schema;
          if (OB_FAIL(ObSchemaUtils::build_single_column_group(new_table_schema,
                                                   new_table_schema.get_column_schema(column_ids.at(i)),
                                                   new_table_schema.get_tenant_id(),
                                                   cur_column_group_id++,
                                                   cg_schema))) {
            LOG_WARN("fail to build single column group", K(ret), K(new_table_schema), K(column_ids.at(i)));
          } else if (OB_FAIL(new_table_schema.add_column_group(cg_schema))) {
            LOG_WARN("fail to add new column group schema to table", K(ret), K(cg_schema));
          } else if (OB_FAIL(tmp_table.add_column_group(cg_schema))) {
            LOG_WARN("fail to add new column group schema to tmp_cg", K(ret), K(tmp_table), K(cg_schema));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (tmp_table.get_column_group_count() == 0){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group array should not be empty", K(ret), K(tmp_table));
        } else if (OB_FAIL(ddl_operator.insert_column_groups(trans, tmp_table))) {
          LOG_WARN("fail to insert new table_schema to each column gorup", K(ret), K(tmp_table));
        }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_all_cg_exist) {
        ObColumnGroupSchema* all_cg = nullptr;
        if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_ALL_COLUMN_GROUP_NAME, all_cg))) {
          LOG_WARN("fail to get all column group", K(ret), K(new_table_schema));
        } else if (OB_ISNULL(all_cg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          if (OB_FAIL(all_cg->add_column_id(column_ids.at(i)))) {
            LOG_WARN("fail to add column id", K(ret), K(new_table_schema), K(column_ids.at(i)));
          }
        }
        if (OB_FAIL(ret)){
        } else if (OB_FAIL(ddl_operator.insert_column_ids_into_column_group(trans, new_table_schema, column_ids, *all_cg))) {
          LOG_WARN("fail to insert column ids into inner table", K(ret), K(new_table_schema),K(column_ids));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (!is_all_cg_exist && !is_each_cg_exist) {
        ObColumnGroupSchema *default_cg = nullptr;
        if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_DEFAULT_COLUMN_GROUP_NAME, default_cg))) {
          LOG_WARN("fail get default column group", K(ret), K(new_table_schema));
        } else if (OB_ISNULL(default_cg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret), K(new_table_schema));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
          if (OB_FAIL(default_cg->add_column_id(column_ids.at(i)))) {
            LOG_WARN("fail to add column id", K(ret), K(new_table_schema), K(column_ids.at(i)));
          }
        }

        if (OB_FAIL(ret)){
        } else if (OB_FAIL(ddl_operator.insert_column_ids_into_column_group(trans, new_table_schema, column_ids, *default_cg))) {
          LOG_WARN("fail to insert column ids into inner table", K(ret), K(new_table_schema));
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
