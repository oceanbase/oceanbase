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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_root_service.h"
#include "share/tablet/ob_tablet_to_table_history_operator.h" // ObTabletToTableHistoryOperator
#include "share/scn.h"

namespace oceanbase
{
namespace rootserver
{

ObTableCreator::~ObTableCreator()
{
}

int ObTableCreator::init(const bool need_tablet_cnt_check)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableCreator init twice", KR(ret));
  } else if (OB_FAIL(tablet_creator_.init(need_tablet_cnt_check))) {
    LOG_WARN("fail to init tablet creator", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObTableCreator::reset()
{
  tablet_creator_.reset();
}

int ObTableCreator::execute()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_creator_.execute())) {
    LOG_WARN("fail to execute tablet creator", KR(ret));
  } else if (tablet_infos_.count() > 0
             && OB_FAIL(share::ObTabletToLSTableOperator::batch_update(trans_, tenant_id_, tablet_infos_))) {
    LOG_WARN("fail to batch update tablet info", KR(ret));
  }
  return ret;
}

int ObTableCreator::add_create_tablets_of_local_aux_tables_arg(
                    const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
                    const share::schema::ObTableSchema *data_table_schema,
                    const common::ObIArray<share::ObLSID> &ls_id_array,
                    const uint64_t tenant_data_version,
                    const common::ObIArray<bool> &need_create_empty_majors)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table_schema must be null, when table_schema is not local index", KR(ret));
  } else if (!data_table_schema->has_tablet() ||
      data_table_schema->is_index_table() ||
      data_table_schema->is_aux_lob_table() ||
      data_table_schema->is_mlog_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_table_schema must be data table", KR(ret), KPC(data_table_schema));
  } else if (OB_UNLIKELY(tenant_data_version <= 0 || need_create_empty_majors.count() != schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_data_version), "count_need_create_empty_majors", need_create_empty_majors.count(),
      "count_schemas", schemas.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    const share::schema::ObTableSchema *aux_schema = schemas.at(i);
    if (OB_ISNULL(aux_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ptr is null", KR(ret), K(schemas));
    } else if (!aux_schema->is_index_local_storage()
        && !aux_schema->is_aux_lob_table()
        && !aux_schema->is_mlog_table()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("aux_schema must be local aux table", KR(ret), K(schemas), KPC(aux_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_create_tablets_of_tables_arg_(
          schemas, data_table_schema, ls_id_array, tenant_data_version, need_create_empty_majors))) {
    LOG_WARN("fail to add_create_tablets_of_tables_arg_", KR(ret), K(schemas));
  }
  return ret;
}

int ObTableCreator::add_create_bind_tablets_of_hidden_table_arg(
                    const share::schema::ObTableSchema &orig_table_schema,
                    const share::schema::ObTableSchema &hidden_table_schema,
                    const common::ObIArray<share::ObLSID> &ls_id_array,
                    const uint64_t tenant_data_version)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableSchema *, 1> schemas;
  ObSEArray<bool, 1> need_create_empty_majors;
  if (OB_UNLIKELY(!orig_table_schema.has_tablet()
      || orig_table_schema.is_index_table()
      || hidden_table_schema.is_index_table()
      || orig_table_schema.is_mlog_table()
      || hidden_table_schema.is_mlog_table()
      || !hidden_table_schema.is_user_hidden_table())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("both orig and hidden table must be data table", K(ret), K(orig_table_schema), K(hidden_table_schema));
  } else if (OB_FAIL(schemas.push_back(&hidden_table_schema)) || OB_FAIL(need_create_empty_majors.push_back(false))) {
    LOG_WARN("failed to push back hidden table schema", K(ret));
  } else if (OB_FAIL(add_create_tablets_of_tables_arg_(
          schemas, &orig_table_schema, ls_id_array, tenant_data_version, need_create_empty_majors))) {
    LOG_WARN("failed to add arg", K(ret), K(schemas));
  }
  return ret;
}

int ObTableCreator::add_create_tablets_of_table_arg(
                    const share::schema::ObTableSchema &table_schema,
                    const common::ObIArray<share::ObLSID> &ls_id_array,
                    const uint64_t tenant_data_version,
                    const bool need_create_empty_major_sstable)
{
  int ret = OB_SUCCESS;
  ObSEArray<const share::schema::ObTableSchema*, 1> schemas;
  ObSEArray<bool, 1> need_create_empty_majors;
  if (!table_schema.has_tablet() || table_schema.is_index_local_storage() || table_schema.is_aux_lob_table()
    || table_schema.is_mlog_table() || tenant_data_version <= 0) {
    LOG_WARN("table_schema must be data table or global indexes", KR(ret), K(table_schema), K(tenant_data_version));
  } else if (OB_FAIL(schemas.push_back(&table_schema))
    || OB_FAIL(need_create_empty_majors.push_back(need_create_empty_major_sstable))) {
    LOG_WARN("failed to push_back", KR(ret), K(table_schema), K(need_create_empty_major_sstable));
  } else if (OB_FAIL(add_create_tablets_of_tables_arg_(
          schemas, NULL, ls_id_array, tenant_data_version, need_create_empty_majors))) {
    LOG_WARN("failed to add create tablet arg", KR(ret), K(table_schema));
  }
  return ret;
}

int ObTableCreator::add_create_tablets_of_tables_arg(
                    const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
                    const common::ObIArray<share::ObLSID> &ls_id_array,
                    const uint64_t tenant_data_version,
                    const common::ObIArray<bool> &need_create_empty_majors)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_data_version <= 0
    || schemas.count() != need_create_empty_majors.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_data_version), "count_schemas", schemas.count(),
      "count_need_create_empty_majors", need_create_empty_majors.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < schemas.count(); ++i) {
    const share::schema::ObTableSchema *table_schema = schemas.at(i);
    if (OB_ISNULL(table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ptr is null", KR(ret), K(schemas));
    } else if (0 == i) {
      if (!table_schema->has_tablet()
          || table_schema->is_index_table()
          || table_schema->is_aux_lob_table()
          || table_schema->is_mlog_table()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data_table_schema must be data table", KR(ret), KPC(table_schema));
      }
    } else {
      if (!table_schema->is_index_local_storage()
          && !table_schema->is_aux_lob_table()
          && !table_schema->is_mlog_table()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("table_schema must be local index", KR(ret), K(schemas), KPC(table_schema));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_create_tablets_of_tables_arg_(
          schemas, NULL, ls_id_array, tenant_data_version, need_create_empty_majors))) {
    LOG_WARN("fail to add_create_tablets_of_tables_arg_", KR(ret), K(schemas));
  }
  return ret;
}

// data_table_schema != null means to create local index tablets or hidden tablets
// and bind to corresponding data tablets in the tablet meta.
// data_tablet_schema == null means to create normal tablets without any binding.
int ObTableCreator::add_create_tablets_of_tables_arg_(
                    const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
                    const share::schema::ObTableSchema *data_table_schema,
                    const common::ObIArray<share::ObLSID> &ls_id_array,
                    const uint64_t tenant_data_version,
                    const common::ObIArray<bool> &need_create_empty_majors)
{
  int ret = OB_SUCCESS;
  const int64_t schema_cnt = schemas.count();
  if (OB_UNLIKELY(schema_cnt < 1 || tenant_data_version <= 0
    || schema_cnt != need_create_empty_majors.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schemas count is less 1", KR(ret), K(schema_cnt), K(tenant_data_version),
      "create_major_flag_cnt", need_create_empty_majors.count());
  } else if (OB_ISNULL(schemas.at(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", KR(ret), K(schemas));
  } else {
    const share::schema::ObTableSchema &table_schema = *schemas.at(0);
    int64_t all_part_num = table_schema.get_all_part_num();
    common::ObArray<share::ObTabletTablePair> pairs;
    bool is_oracle_mode = false;
    bool is_create_bind_hidden_tablets = false;
    if (table_schema.is_index_local_storage()
        || table_schema.is_aux_lob_table()
        || table_schema.is_mlog_table()) {
      if (OB_ISNULL(data_table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data_table_schema is NULL when create local_index", KR(ret));
      }
    } else if (table_schema.is_user_hidden_table()) {
      if (nullptr == data_table_schema) {
        // create as normal hidden table (no bind) if data_table_schema is null
        data_table_schema = &table_schema;
      } else {
        if (OB_UNLIKELY(schemas.count() != 1)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unexpected schemas", K(ret), K(schemas.count()));
        } else {
          is_create_bind_hidden_tablets = true;
        }
      }
    } else {
      if (OB_NOT_NULL(data_table_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data_table_schema must be null, when table_schema is not local index and not hidden table",
                 KR(ret), K(data_table_schema), K(table_schema));
      } else {
        data_table_schema = &table_schema;
      }
    }

    if (FAILEDx(pairs.reserve(all_part_num * schema_cnt))) {
      LOG_WARN("fail to reserve array", KR(ret), K(all_part_num), K(schema_cnt));
    } else if (OB_FAIL(ls_id_array_.reserve(all_part_num))) {
      LOG_WARN("fail to reserve array", KR(ret), K(all_part_num));
    } else if (OB_FAIL(ls_id_array_.assign(ls_id_array))) {
      LOG_WARN("fail to assign ls id array", KR(ret));
    } else if (ls_id_array_.count() != all_part_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the ls of tablet is not equal partition num", KR(ret),
               K(all_part_num), "ls_id_array_cnt", ls_id_array_.count());
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < schema_cnt; ++i) {
        if (OB_ISNULL(schemas.at(i))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("ptr is null", KR(ret), K(schemas), K(table_schema));
        } else if (schemas.at(i)->get_part_option().get_part_func_type()
                    != table_schema.get_part_option().get_part_func_type()
                   || schemas.at(i)->get_sub_part_option().get_sub_part_func_type()
                    != table_schema.get_sub_part_option().get_sub_part_func_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("part type of local index is equal to its datatable", KR(ret), K(i));
        }
      }
    }

    if (FAILEDx(data_table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      LOG_WARN("fail to check oracle mode", KR(ret), KPC(data_table_schema));
    } else {
      int64_t ls_idx = 0;
      ObPartitionLevel part_level = table_schema.get_part_level();
      lib::Worker::CompatMode compat_mode = is_oracle_mode ?
                                         lib::Worker::CompatMode::ORACLE :
                                         lib::Worker::CompatMode::MYSQL;
      if (part_level >= PARTITION_LEVEL_MAX) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part level is unexpected", K(table_schema), KR(ret));
      } else if (PARTITION_LEVEL_ZERO == part_level) {
        if (OB_FAIL(generate_create_tablet_arg_(
                    schemas,
                    *data_table_schema,
                    compat_mode,
                    ls_id_array_.at(ls_idx++),
                    pairs,
                    OB_INVALID_INDEX,
                    OB_INVALID_INDEX,
                    is_create_bind_hidden_tablets,
                    tenant_data_version,
                    need_create_empty_majors))) {
          LOG_WARN("fail to generate_create_tablet_arg",
                   K(table_schema), K(schemas), KR(ret), K(is_create_bind_hidden_tablets));
        }
      } else {
        ObPartition **part_array = table_schema.get_part_array();
        int64_t part_num = table_schema.get_partition_num();
        if (OB_ISNULL(part_array)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("part array is null", K(table_schema), KR(ret));
        } else {
          for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
            if (OB_ISNULL(part_array[i])) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("NULL ptr", K(i), K(table_schema), KR(ret));
            } else if (PARTITION_LEVEL_ONE == part_level) {
              if (OB_FAIL(generate_create_tablet_arg_(
                          schemas,
                          *data_table_schema,
                          compat_mode,
                          ls_id_array_.at(ls_idx++),
                          pairs,
                          i,
                          OB_INVALID_INDEX,
                          is_create_bind_hidden_tablets,
                          tenant_data_version,
                          need_create_empty_majors))) {
                LOG_WARN("fail to generate_create_tablet_arg",
                         K(table_schema), K(schemas), KR(ret), K(i), K(is_create_bind_hidden_tablets));
              }
            } else if (PARTITION_LEVEL_TWO == part_level) {
              ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
              int64_t sub_part_num = part_array[i]->get_subpartition_num();
              if (OB_ISNULL(subpart_array)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("part array is null", K(table_schema), KR(ret));
              } else {
                for (int64_t j = 0; j < sub_part_num && OB_SUCC(ret); j++) {
                  if (OB_ISNULL(subpart_array[j])) {
                    ret = OB_INVALID_ARGUMENT;
                    LOG_WARN("NULL ptr", K(i), K(j), K(table_schema), KR(ret));
                  } else {
                    if (OB_FAIL(generate_create_tablet_arg_(
                                schemas,
                                *data_table_schema,
                                compat_mode,
                                ls_id_array_.at(ls_idx++),
                                pairs,
                                i,
                                j,
                                is_create_bind_hidden_tablets,
                                tenant_data_version,
                                need_create_empty_majors))) {
                      LOG_WARN("fail to generate_create_tablet_arg",
                               K(table_schema), K(schemas), KR(ret), K(i), K(j), K(is_create_bind_hidden_tablets));
                    }
                  }
                }
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("4.0 not support part type", K(table_schema), KR(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !is_sys_table(table_schema.get_table_id())) {
      int64_t start_time = ObTimeUtility::current_time();
      int64_t schema_version = table_schema.get_schema_version();
      if (OB_FAIL(share::ObTabletToTableHistoryOperator::create_tablet_to_table_history(
                         trans_, tenant_id_, schema_version, pairs))) {
        LOG_WARN("fail to create tablet to table history",
                 KR(ret), K_(tenant_id), K(schema_version));
      }
      int64_t end_time = ObTimeUtility::current_time();
      LOG_INFO("finish create_tablet_to_table_history", KR(ret), K(table_schema.get_tenant_id()),
                                                        K(table_schema.get_table_id()), "cost_ts", end_time - start_time);
    }
  }
  return ret;
}

int ObTableCreator::generate_create_tablet_arg_(
                    const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
                    const ObTableSchema &data_table_schema,
                    const lib::Worker::CompatMode &mode,
                    const share::ObLSID &ls_id,
                    common::ObIArray<share::ObTabletTablePair> &pairs,
                    const int64_t part_idx,
                    const int64_t subpart_idx,
                    const bool is_create_bind_hidden_tablets,
                    const uint64_t tenant_data_version,
                    const common::ObIArray<bool> &need_create_empty_majors)
{
  int ret = OB_SUCCESS;
  ObTabletID data_tablet_id;
  ObTabletCreatorArg create_tablet_arg;
  common::ObArray<ObTabletID> tablet_id_array;
  ObTabletID tablet_id;
  ObBasePartition *data_part = NULL;
  ObBasePartition *part = NULL;
  if (PARTITION_LEVEL_ZERO == data_table_schema.get_part_level()) {
    data_tablet_id = data_table_schema.get_tablet_id();
  } else if (OB_FAIL(data_table_schema.get_part_by_idx(part_idx, subpart_idx, data_part))) {
    LOG_WARN("fail to get data part", KR(ret), K(data_table_schema), K(part_idx), K(subpart_idx));
  } else if (OB_ISNULL(data_part)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", K(data_table_schema), KR(ret), K(part_idx), K(subpart_idx));
  } else {
    data_tablet_id = data_part->get_tablet_id();
  }
  for (int r = 0; r < schemas.count() && OB_SUCC(ret); r++) {
    const share::schema::ObTableSchema *table_schema_ptr = schemas.at(r);
    uint64_t table_id = OB_INVALID_ID;
    if (OB_ISNULL(table_schema_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL ptr", K(r), K(schemas), KR(ret));
    } else if (FALSE_IT(table_id = table_schema_ptr->get_table_id())) {
    } else if (PARTITION_LEVEL_ZERO == table_schema_ptr->get_part_level()) {
      tablet_id = table_schema_ptr->get_tablet_id();
    } else if (OB_FAIL(table_schema_ptr->get_part_by_idx(part_idx, subpart_idx, part))) {
      LOG_WARN("fail to get index part", KR(ret), KPC(table_schema_ptr), K(part_idx), K(subpart_idx));
    } else if (OB_ISNULL(data_part) || OB_ISNULL(part)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("NULL ptr", K(data_table_schema), KPC(table_schema_ptr), KR(ret), K(part_idx), K(subpart_idx));
    } else if (OB_UNLIKELY(!data_part->same_base_partition(*part))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parts in table and index table is not equal", KR(ret), KPC(data_part), KPC(part));
    } else {
      tablet_id = part->get_tablet_id();
    }
    share::ObTabletTablePair pair;
    share::ObTabletToLSInfo tablet_info(tablet_id, ls_id, table_schema_ptr->get_table_id(), 0/*transfer_seq*/);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
      LOG_WARN("failed to assign table schema point", KR(ret));
    } else if (is_sys_table(table_schema_ptr->get_table_id())) {
    } else if (OB_FAIL(tablet_infos_.push_back(tablet_info))) {
      LOG_WARN("fail to push_back", KR(ret), K(tablet_info));
    } else if (OB_FAIL(pair.init(tablet_id, table_id))) {
      LOG_WARN("fail to init tablet-table pair", KR(ret), K(tablet_id), K(table_id));
    } else if (OB_FAIL(pairs.push_back(pair))) {
      LOG_WARN("fail to push back tablet-table pair", KR(ret), K(pair));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_tablet_arg.init(
                     tablet_id_array,
                     ls_id,
                     data_tablet_id,
                     schemas,
                     mode,
                     is_create_bind_hidden_tablets,
                     tenant_data_version,
                     need_create_empty_majors))) {
    LOG_WARN("fail to init create tablet arg", KR(ret), K(schemas), K(is_create_bind_hidden_tablets));
  } else if (OB_FAIL(tablet_creator_.add_create_tablet_arg(create_tablet_arg))) {
    LOG_WARN("fail to add create tablet arg", KR(ret), K(create_tablet_arg));
  }
  return ret;
}

} // rootserver
} // oceanbase
