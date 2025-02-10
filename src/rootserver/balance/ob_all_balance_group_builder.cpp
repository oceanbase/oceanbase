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
#define USING_LOG_PREFIX BALANCE

#include "ob_all_balance_group_builder.h"
#include "share/tablet/ob_tablet_table_iterator.h"    // ObTenantTabletMetaIterator
#include "share/schema/ob_part_mgr_util.h"            // ObPartitionSchemaIter
#include "share/ls/ob_ls_status_operator.h"           // ObLSStatusOperator
#include "rootserver/ob_partition_balance.h"         // ObPartitionHelper

#define ISTAT(fmt, args...) FLOG_INFO("[BALANCE_GROUP_BUILDER] " fmt, K_(mod), ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[BALANCE_GROUP_BUILDER] " fmt, K_(mod), ##args)

#define ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid) \
    do {\
      if (OB_FAIL(add_new_part_(bg, table_schema, part_object_id, tablet_id, part_group_uid))) {\
        LOG_WARN("add new partition fail", KR(ret), K(bg), K(part_object_id), K(part_group_uid), \
        K(pre_dest_ls_id_), K(pre_dup_to_normal_dest_ls_id_), K(pre_bg_id_), K(pre_part_group_uid_), K(table_schema));\
      }\
    } while (0)

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
namespace rootserver
{
int ObPartitionHelper::check_partition_option(const schema::ObSimpleTableSchemaV2 &t1, const schema::ObSimpleTableSchemaV2 &t2, bool is_subpart, bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  if (OB_FAIL(share::schema::ObSimpleTableSchemaV2::compare_partition_option(t1, t2, is_subpart, is_matched))) {
    LOG_WARN("fail to compare partition optition", KR(ret));
  }
  return ret;
}

int ObPartitionHelper::get_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, ObPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObObjectID part_id;
  ObObjectID first_level_part_id;
  if (OB_FAIL(table_schema.get_tablet_and_object_id_by_index(part_idx, -1, tablet_id, part_id, first_level_part_id))) {
    LOG_WARN("fail to get_tablet_and_object_id_by_index", KR(ret), K(table_schema), K(part_idx));
  } else if (OB_FAIL(part_info.init(tablet_id, part_id))) {
    LOG_WARN("fail init part_info", KR(ret), K(tablet_id), K(part_id));
  }
  return ret;
}

int ObPartitionHelper::get_sub_part_num(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t &sub_part_num)
{
  int ret = OB_SUCCESS;
  const schema::ObPartition *partition = NULL;
  if (OB_FAIL(table_schema.get_partition_by_partition_index(part_idx, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
    LOG_WARN("fail to get partition by part_idx", KR(ret), K(part_idx));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition not exist", KR(ret), K(table_schema), K(part_idx));
  } else {
    sub_part_num = partition->get_sub_part_num();
  }
  return ret;
}

int ObPartitionHelper::get_sub_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t sub_part_idx, ObPartInfo &part_info)
{
  int ret = OB_SUCCESS;

  ObTabletID tablet_id;
  ObObjectID part_id;
  ObObjectID first_level_part_id;
  if (OB_FAIL(table_schema.get_tablet_and_object_id_by_index(part_idx, sub_part_idx, tablet_id, part_id, first_level_part_id))) {
    LOG_WARN("fail to get_tablet_and_object_id_by_index", KR(ret), K(table_schema), K(part_idx), K(sub_part_idx));
  } else if (OB_FAIL(part_info.init(tablet_id, part_id))) {
    LOG_WARN("fail init part_info", KR(ret), K(tablet_id), K(part_id));
  }
  return ret;
}

ObAllBalanceGroupBuilder::ObAllBalanceGroupBuilder() :
    inited_(false),
    mod_(""),
    tenant_id_(OB_INVALID_TENANT_ID),
    dup_ls_id_(),
    pre_dest_ls_id_(),
    pre_dup_to_normal_dest_ls_id_(),
    pre_bg_id_(),
    pre_part_group_uid_(OB_INVALID_ID),
    callback_(NULL),
    sql_proxy_(NULL),
    schema_service_(NULL),
    schema_guard_(schema::ObSchemaMgrItem::MOD_PARTITION_BALANCE),
    tablet_to_ls_(),
    tablet_data_size_(),
    allocator_("AllBGBuilder"),
    related_tablets_map_(),
    sharding_none_tg_global_indexes_()
{
}

ObAllBalanceGroupBuilder::~ObAllBalanceGroupBuilder()
{
  destroy();
}

int ObAllBalanceGroupBuilder::init(const int64_t tenant_id,
    const char *mod,
    NewPartitionCallback &callback,
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAllBalanceGroupBuilder init twice", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id) || OB_ISNULL(mod)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(mod));
  } else if (OB_FAIL(tablet_to_ls_.init(MAP_BUCKET_NUM, lib::ObLabel("TabletToLS"), tenant_id))) {
    LOG_WARN("create map for tablet to LS fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_data_size_.create(MAP_BUCKET_NUM, lib::ObLabel("TabletSizeMap")))) {
    LOG_WARN("create map for tablet data size fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(related_tablets_map_.create(
      MAP_BUCKET_NUM,
      lib::ObLabel("RelatedTbltMap"),
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("create map for related tablet map failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sharding_none_tg_global_indexes_.create(SET_BUCKET_NUM))) {
    LOG_WARN("create set for sharding_none_tg_global_indexes_ failed", KR(ret), K(tenant_id));
  } else {
    mod_ = mod;
    tenant_id_ = tenant_id;
    dup_ls_id_.reset();
    pre_dest_ls_id_.reset();
    pre_dup_to_normal_dest_ls_id_.reset();
    pre_bg_id_.reset();
    pre_part_group_uid_ = OB_INVALID_ID;
    callback_ = &callback;
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    schema_guard_.reset();
    allocator_.set_tenant_id(tenant_id);
    inited_ = true;
  }
  return ret;
}

void ObAllBalanceGroupBuilder::destroy()
{
  inited_ = false;
  sharding_none_tg_global_indexes_.destroy();
  FOREACH(iter, related_tablets_map_) {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObIArray<ObTabletID>();
    }
  }
  related_tablets_map_.destroy();
  tablet_data_size_.destroy();
  tablet_to_ls_.destroy();
  schema_guard_.reset();
  schema_service_ = NULL;
  sql_proxy_ = NULL;
  callback_ = NULL;
  pre_part_group_uid_ = OB_INVALID_ID;
  pre_bg_id_.reset();
  pre_dup_to_normal_dest_ls_id_.reset();
  pre_dest_ls_id_.reset();
  dup_ls_id_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  mod_ = "";
  allocator_.reset();
}

int ObAllBalanceGroupBuilder::prepare(bool need_load_tablet_size)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t step_time = 0;
  ObLSStatusOperator ls_status_operator;
  ObLSStatusInfo dup_ls_status;

  if (OB_UNLIKELY(! inited_) || OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllBalanceGroupBuilder not inited", KR(ret), K(inited_), K(schema_service_),
        K(sql_proxy_));
  }
  // prepare schema first, to ensure that tablet_to_ls is newer than schema
  else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard_))) {
    LOG_WARN("get tenant schema guard fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(tablet_to_ls_.build(tenant_id_, *sql_proxy_))) {
    LOG_WARN("build tablet to LS info fail", KR(ret), K(tenant_id_));
  } else if (FALSE_IT(step_time = ObTimeUtility::current_time())) {
  } else if (!need_load_tablet_size || GCTX.is_shared_storage_mode()) {
    // finish
  } else if (OB_FAIL(prepare_related_tablets_map_())) {
    LOG_WARN("prepare related tablets map failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(prepare_tablet_data_size_())) {
    LOG_WARN("prepare tablet data size fail", KR(ret), K(tenant_id_));
  }
  if (FAILEDx(ls_status_operator.get_duplicate_ls_status_info(
      tenant_id_,
      *sql_proxy_,
      dup_ls_status))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      dup_ls_id_.reset();
    } else {
      LOG_WARN("get duplicate ls status info failed", KR(ret), K(tenant_id_));
    }
  } else {
    dup_ls_id_ = dup_ls_status.get_ls_id();
   }

  int64_t finish_time = ObTimeUtility::current_time();
  ISTAT("prepare data", KR(ret), K_(tenant_id), K_(dup_ls_id), "cost", finish_time - start_time,
      "tablet_to_ls_count", tablet_to_ls_.size(),
      "cost1", step_time - start_time,
      "cost2", finish_time - step_time);
  return ret;
}

int ObAllBalanceGroupBuilder::build()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();

  ISTAT("begin build balance group");

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllBalanceGroupBuilder not init", KR(ret), K(this));
  } else if (OB_FAIL(do_build_())) {
    LOG_WARN("do build balance group info fail", KR(ret));
  }

  ISTAT("finish build balance group", KR(ret),
      "cost", ObTimeUtility::current_time() - start_time);

  return ret;
}

int ObAllBalanceGroupBuilder::do_build_()
{
  int ret = OB_SUCCESS;
  // process table in tablegroup
  ObArray<const ObSimpleTablegroupSchema*> tablegroup_schemas;
  // process table not in tablegroup
  ObArray<const ObSimpleTableSchemaV2 *> tenant_table_schemas;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllBalanceGroupBuilder not init", KR(ret), K(inited_));
  } else if (OB_FAIL(schema_guard_.get_tablegroup_schemas_in_tenant(tenant_id_, tablegroup_schemas))) {
    LOG_WARN("get tablegroup schemas in tenant fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tenant(tenant_id_, tenant_table_schemas))) {
    LOG_WARN("get table schemas fail", KR(ret), K(tenant_id_));
  } else {
    for (int64_t tg = 0; OB_SUCC(ret) && tg < tablegroup_schemas.count(); tg++) {
      int max_part_level = PARTITION_LEVEL_ZERO;
      ObArray<const ObSimpleTableSchemaV2 *> tg_table_schemas;
      const ObSimpleTablegroupSchema *tablegroup_schema = tablegroup_schemas.at(tg);
      if (OB_ISNULL(tablegroup_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema is null", KR(ret), K(tenant_id_), K(tg));
      } else if (OB_FAIL(get_table_schemas_in_tablegroup_(*tablegroup_schema,
          tg_table_schemas, max_part_level))) {
        LOG_WARN("get table schemas in tablegroup fail", KR(ret), K(tenant_id_), K(tablegroup_schema));
      } else if (OB_FAIL(build_balance_group_for_tablegroup_(*tablegroup_schema, tg_table_schemas,
          max_part_level))) {
        LOG_WARN("build balance group for tablegroup fail", KR(ret), KPC(tablegroup_schema),
            K(max_part_level), K(tg_table_schemas));
      }
    }

    for (int64_t idx = 0; OB_SUCC(ret) && idx < tenant_table_schemas.count(); idx++) {
      const ObSimpleTableSchemaV2 *table_schema = tenant_table_schemas.at(idx);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(tenant_id_), K(idx));
      } else if (OB_FAIL(build_balance_group_for_table_not_in_tablegroup_(*table_schema))) {
        LOG_WARN("build balance group for table not in tablegroup fail", KR(ret), KPC(table_schema));
      }
    }
  }

  return ret;
}

int ObAllBalanceGroupBuilder::get_table_schemas_in_tablegroup_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    int &max_part_level)
{
  int ret = OB_SUCCESS;
  int64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();
  ObArray<const ObSimpleTableSchemaV2*> all_table_schemas;

  max_part_level = PARTITION_LEVEL_ZERO;

  if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(tenant_id_, tablegroup_id, all_table_schemas))) {
    LOG_WARN("get_table_schemas_in_tablegroup fail", KR(ret), K(tenant_id_), K(tablegroup_id));
  } else if (all_table_schemas.empty()) {
    //
  } else {
    for (int64_t t = 0; OB_SUCC(ret) && t < all_table_schemas.count(); t++) {
      if (OB_ISNULL(all_table_schemas.at(t))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema ptr is null", KR(ret), K(tablegroup_id), K(t));
      } else if (all_table_schemas.at(t)->is_global_index_table()) {
        //skip
      } else if (OB_FAIL(table_schemas.push_back(all_table_schemas.at(t)))) {
        LOG_WARN("push table_schema to array fail", KR(ret));
      } else if (all_table_schemas.at(t)->get_part_level() > max_part_level) {
        max_part_level = all_table_schemas.at(t)->get_part_level();
      }
    }
  }
  return ret;
}


int ObAllBalanceGroupBuilder::build_balance_group_for_tablegroup_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    const int max_part_level)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = tablegroup_schema.get_tablegroup_id();

  if (is_sys_tablegroup_id(tablegroup_id)) {
    // skip sys tablegroup
  } else if (table_schemas.empty()) {
    ISTAT("table_schemas empty in tablegroup, need not build balance group", K_(tenant_id), K(tablegroup_schema));
    // do nothing
  } else if (OB_FAIL(check_table_schemas_in_tablegroup_(table_schemas))) { // defensive check
    LOG_WARN("check table schemas for tablegroup failed", KR(ret), K(tenant_id_), K(tablegroup_schema));
  } else if (tablegroup_schema.is_sharding_none() || PARTITION_LEVEL_ZERO == max_part_level) {
    // sharding none tablegroup or non-partition tables always distribute together on same LS
    if (OB_FAIL(build_bg_for_tablegroup_sharding_none_(tablegroup_schema, table_schemas, max_part_level))) {
      LOG_WARN("fail to build balance group for tablegroup NONE sharding or non-part tables", KR(ret), K(tenant_id_),
          K(tablegroup_schema), K(max_part_level));
    }
  } else if (tablegroup_schema.is_sharding_partition() || PARTITION_LEVEL_ONE == max_part_level) {
    if (OB_FAIL(build_bg_for_tablegroup_sharding_partition_(tablegroup_schema, table_schemas, max_part_level))) {
      LOG_WARN("fail to build balance group for tablegroup PARTITION sharding or level-one part tables",
          KR(ret), K(tenant_id_), K(tablegroup_schema), K(max_part_level));
    }
  } else if (tablegroup_schema.is_sharding_adaptive()) {
    if (PARTITION_LEVEL_TWO != max_part_level) {
      // should be two-level part tables, other part level should not be here
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition level", KR(ret), K(max_part_level), K(tenant_id_), K(tablegroup_schema));
    } else if (OB_FAIL(build_bg_for_tablegroup_sharding_subpart_(tablegroup_schema, table_schemas,
        max_part_level))) {
      LOG_WARN("fail build balance group for tablegroup ADAPTIVE sharding and two level partition tables",
          KR(ret), K(tenant_id_), K(tablegroup_schema), K(max_part_level));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablegroup sharding", KR(ret), K(tenant_id_), K(tablegroup_schema),
        K(max_part_level));
  }

  return ret;
}

int ObAllBalanceGroupBuilder::check_table_schemas_in_tablegroup_(
    const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(table_schemas, idx) {
    const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(idx);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KR(ret), KP(table_schema), K(idx));
    } else if (OB_UNLIKELY(OB_INVALID_ID == table_schema->get_tablegroup_id()
        || table_schema->is_duplicate_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table_schema in tablegroup", KR(ret), KPC(table_schema));
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::build_balance_group_for_table_not_in_tablegroup_(
    const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!table_schema.is_global_index_table() && OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    // skip table in tablegroup
    // global index should not in tablegroup, here is defensive code
  } else if (FALSE_IT(tmp_ret = sharding_none_tg_global_indexes_.exist_refactored(table_schema.get_table_id()))) {
  } else if (OB_HASH_EXIST == tmp_ret) {
    LOG_TRACE("skip global index bound to sharding none tablegroup",
        K(tenant_id_), "table_id", table_schema.get_table_id(),
        "data_table_id", table_schema.get_data_table_id());
  } else if (OB_HASH_NOT_EXIST != tmp_ret) {
    ret = tmp_ret;
    LOG_WARN("exist_refactored failed", KR(ret), K(tmp_ret), K(table_schema));
  } else if (need_balance_table(table_schema)) {
    if (PARTITION_LEVEL_ZERO == table_schema.get_part_level())  {
      if (OB_FAIL(build_bg_for_partlevel_zero_(table_schema))) {
        LOG_WARN("fail build balance group for partlevel zero table", KR(ret), K(tenant_id_), K(table_schema));
      }
    } else if (PARTITION_LEVEL_ONE == table_schema.get_part_level())  {
      if (OB_FAIL(build_bg_for_partlevel_one_(table_schema))) {
        LOG_WARN("fail build balance group for partlevel one table", KR(ret), K(tenant_id_),
            K(table_schema));
      }
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level())  {
      if (OB_FAIL(build_bg_for_partlevel_two_(table_schema))) {
        LOG_WARN("fail build balance group for partlevel two table", KR(ret), K(tenant_id_), K(table_schema));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table part level", KR(ret), K(table_schema));
    }
  }
  return ret;
}

// all part in one partition group and balance group
// global indexes of primary table in sharding none tablegroup are in the same balance group
int ObAllBalanceGroupBuilder::build_bg_for_tablegroup_sharding_none_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    const int64_t max_part_level)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg;
  const ObString &tablegroup_name = tablegroup_schema.get_tablegroup_name();
  ObArray<const ObSimpleTableSchemaV2 *> global_index_schemas;
  bool in_new_pg = true; // in new partition group
  ObLSID dest_ls_id; // binding to the first table first tablet
  if (OB_FAIL(bg.init_by_tablegroup(tablegroup_schema, max_part_level))) {
    LOG_WARN("init balance group by tablegroup fail", KR(ret), K(bg), K(max_part_level),
        K(tablegroup_schema));
  } else if (OB_FAIL(get_global_indexes_of_tables_(table_schemas, global_index_schemas))) {
    LOG_WARN("get global indexes of tables failed", KR(ret), K(table_schemas));
  } else if (OB_FAIL(add_part_to_bg_for_tablegroup_sharding_none_(
      bg,
      table_schemas,
      dest_ls_id,
      in_new_pg))) {
    LOG_WARN("add part to bg for tablegroup sharding none failed",
        KR(ret), K(bg), K(table_schemas), K(in_new_pg));
  } else if (OB_FAIL(add_part_to_bg_for_tablegroup_sharding_none_(
      bg,
      global_index_schemas,
      dest_ls_id,
      in_new_pg))) {
    LOG_WARN("add global index part to bg for tablegroup sharding none failed",
        KR(ret), K(bg), K(global_index_schemas), K(in_new_pg));
  }

  ISTAT("build balance group for table group of NONE sharding or non-partition tables",
      KR(ret), K(max_part_level), K(tablegroup_schema), "table_count", table_schemas.count(),
      "global_index_count", global_index_schemas.count());
  return ret;
}

int ObAllBalanceGroupBuilder::add_part_to_bg_for_tablegroup_sharding_none_(
    const ObBalanceGroup &bg,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    ObLSID &dest_ls_id,
    bool &in_new_pg)
{
  int ret = OB_SUCCESS;
  const uint64_t part_group_uid = 0; // all partitions belong to the same partition group for each LS
  if (OB_UNLIKELY(bg.name().is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid balance group", KR(ret), K(bg));
  } else {
    ARRAY_FOREACH(table_schemas, idx) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(idx);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", KR(ret), K(table_schema), K(idx));
      } else {
        ObPartitionSchemaIter iter(*table_schema, CHECK_PARTITION_MODE_NORMAL);
        ObPartitionSchemaIter::Info info;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter.next_partition_info(info))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            ObObjectID part_object_id = info.object_id_;
            ObTabletID tablet_id = info.tablet_id_;

            ADD_NEW_PART(bg, *table_schema, part_object_id, tablet_id, part_group_uid);
          }
        } // end while
      }
    } // end ARRAY_FOREACH
  }
  return ret;
}

int ObAllBalanceGroupBuilder::get_global_indexes_of_tables_(
    const ObArray<const ObSimpleTableSchemaV2 *> &table_schemas,
    ObIArray<const ObSimpleTableSchemaV2 *> &global_index_schemas)
{
  int ret = OB_SUCCESS;
  global_index_schemas.reset();
  ObArray<const ObSimpleTableSchemaV2 *> single_table_indexes;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    ARRAY_FOREACH(table_schemas, i) {
      single_table_indexes.reset();
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", KR(ret), K(table_schema), K(i));
      } else if (OB_FAIL(schema_guard_.get_index_schemas_with_data_table_id(
          tenant_id_,
          table_schema->get_table_id(),
          single_table_indexes))) {
        LOG_WARN("get index schemas with data table id failed", KR(ret), K(tenant_id_), KPC(table_schema));
      } else {
        ARRAY_FOREACH(single_table_indexes, j) {
          const ObSimpleTableSchemaV2 *index_schema = single_table_indexes.at(j);
          if (OB_ISNULL(index_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index schema is null", KR(ret), K(table_schemas), K(i));
          } else if (index_schema->is_global_index_table()) {
            if (OB_FAIL(global_index_schemas.push_back(index_schema))) {
              LOG_WARN("push back failed", KR(ret), KPC(index_schema));
            } else if (OB_FAIL(sharding_none_tg_global_indexes_.set_refactored(index_schema->get_table_id()))) {
              LOG_WARN("set_refactored failed", KR(ret), KPC(index_schema));
            }
          }
        } // end ARRAY_FOREACH single_table_indexes
      }
    } // end ARRAY_FOREACH table_schemas
  }
  return ret;
}

int ObAllBalanceGroupBuilder::get_primary_schema_and_check_all_partition_matched_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    const ObSimpleTableSchemaV2* &primary_table_schema,
    const bool is_subpart)
{
  int ret = OB_SUCCESS;
  // check partition match
  for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
    bool match = false;
    if (OB_ISNULL(table_schemas.at(t))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KR(ret), K(tablegroup_schema), K(t));
    } else if (PARTITION_LEVEL_ZERO == table_schemas.at(t)->get_part_level()) {
      // other logic ensures that code will not be here
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("non-partition table should not be here", KR(ret), K(tablegroup_schema), KPC(table_schemas.at(t)));
    } else if (is_subpart && PARTITION_LEVEL_TWO != table_schemas.at(t)->get_part_level()) {
      // other logic ensures that code will not be here
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("only two-level partition table should be here", KR(ret), K(tablegroup_schema), KPC(table_schemas.at(t)));
    } else if (OB_ISNULL(primary_table_schema)) {
      primary_table_schema = table_schemas.at(t);
    } else if (OB_FAIL(ObPartitionHelper::check_partition_option(*table_schemas.at(t), *primary_table_schema, is_subpart, match))) {
      LOG_WARN("check partition option fail", KR(ret), K(*table_schemas.at(t)), K(*primary_table_schema), K(is_subpart));
    } else if (!match) {
      LOG_WARN("two tables’ partition method not consistent, skip balance for this table group",
          K(tablegroup_schema), "table", *table_schemas.at(t), "primary_table", *primary_table_schema);
      primary_table_schema = nullptr;
      break;
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_tablegroup_sharding_partition_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    const int64_t max_part_level)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg;
  const ObSimpleTableSchemaV2* primary_table_schema = nullptr;

  // tablegroup is one balance group
  if (OB_FAIL(bg.init_by_tablegroup(tablegroup_schema, max_part_level))) {
    LOG_WARN("init balance group by tablegroup fail", KR(ret), K(bg), K(max_part_level),
        K(tablegroup_schema));
  } else if (OB_FAIL(get_primary_schema_and_check_all_partition_matched_(tablegroup_schema,
      table_schemas, primary_table_schema, false/*is_subpart*/))) {
    LOG_WARN("get primary schema and check all partition matched in table group fail", KR(ret),
        K(tablegroup_schema), K(table_schemas.count()));
  }

  for (int64_t p = 0; OB_SUCC(ret) && primary_table_schema != nullptr && p < primary_table_schema->get_partition_num(); p++) {
    // partitions/subpartitions of all tables with same one-level-partition-value, are in the same partition group.
    // Here, partitions/subpartitions with same one-level-partition-index of all tables are in the same partition group
    const uint64_t part_group_uid = p; // all partitions/subpartitions with same one-level part index belong to the same partition group for each LS
    for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
      const ObSimpleTableSchemaV2 &table_schema = *table_schemas.at(t);
      if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
        ObPartitionHelper::ObPartInfo part_info;
        if (OB_FAIL(ObPartitionHelper::get_part_info(table_schema, p, part_info))) {
          LOG_WARN("get part info fail", KR(ret), K(table_schema), K(p));
        } else {
          // one-level partition object id
          ObObjectID part_object_id = part_info.get_part_id();
          ObTabletID tablet_id = part_info.get_tablet_id();

          ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
        }
      } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()) {
        int64_t sub_part_num = 0;
        if (OB_FAIL(ObPartitionHelper::get_sub_part_num(table_schema, p, sub_part_num))) {
          LOG_WARN("get sub partition number fail", KR(ret), K(p), K(table_schema));
        } else {
          for (int64_t sp = 0; OB_SUCC(ret) && sp < sub_part_num; sp++) {
            ObPartitionHelper::ObPartInfo part_info;
            if (OB_FAIL(ObPartitionHelper::get_sub_part_info(table_schema, p, sp, part_info))) {
              LOG_WARN("get sub partition info fail", KR(ret), K(table_schema), K(p), K(sp));
            } else {
              // two-level subpartition object id
              ObObjectID part_object_id = part_info.get_part_id();
              ObTabletID tablet_id = part_info.get_tablet_id();

              ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
            }
          }
        }
      } else {
        // PARTITION_LEVEL_ZERO should not be here
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table part_level", KR(ret), K(tablegroup_schema), K(table_schema));
      }
    }
  }
  ISTAT("build balance group for tablegroup of PARTITION sharding or one-level partition tables",
      KR(ret),
      K(max_part_level),
      K(tablegroup_schema),
      "table_count", table_schemas.count());
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_tablegroup_sharding_subpart_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    const int max_part_level)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2* primary_table_schema = nullptr;

  // check partition
  if (OB_FAIL(get_primary_schema_and_check_all_partition_matched_(tablegroup_schema, table_schemas,
      primary_table_schema, true/*is_subpart*/))) {
    LOG_WARN("get primary schema and check all partition is matched fail", KR(ret),
        K(tablegroup_schema), K(table_schemas.count()));
  }

  // every level one part(all table) as one balance group
  // every level two part(all table) as one partition group
  for (int64_t part_idx = 0; OB_SUCC(ret) && primary_table_schema != nullptr  && part_idx < primary_table_schema->get_partition_num(); part_idx++) {
    ObBalanceGroup bg;
    const ObPartition *partition = NULL;
    if (OB_FAIL(primary_table_schema->get_partition_by_partition_index(part_idx, CHECK_PARTITION_MODE_NORMAL, partition))) {
      LOG_WARN("get_partition_by_partition_index fail", KR(ret), K(part_idx), K(primary_table_schema));
    } else if (OB_ISNULL(partition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part ptr is null", KR(ret), K(part_idx), K(primary_table_schema));
    } else if (OB_FAIL(bg.init_by_tablegroup(tablegroup_schema, max_part_level, part_idx))) {
      LOG_WARN("init balance group by tablegroup fail", KR(ret), K(bg), K(max_part_level),
          K(tablegroup_schema), K(part_idx));
    } else {
      for (int64_t sp = 0; OB_SUCC(ret) && sp < partition->get_sub_part_num(); sp++) {
        const uint64_t part_group_uid = sp; // subpartitions with same sub_part_idx belong to the same partition group for each LS
        for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
          const ObSimpleTableSchemaV2 &table_schema = *table_schemas.at(t);
          ObPartitionHelper::ObPartInfo part_info;
          if (OB_FAIL(ObPartitionHelper::get_sub_part_info(table_schema, part_idx, sp, part_info))) {
            LOG_WARN("get sub partition info fail", KR(ret), K(table_schema), K(part_idx), K(sp));
          } else {
            // two-level subpartition object id
            ObObjectID part_object_id = part_info.get_part_id();
            ObTabletID tablet_id = part_info.get_tablet_id();

            ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
          }
        }
      }
    }
  }

  ISTAT("build balance group for table group of SUBPARTITION sharding",
      KR(ret),
      K(max_part_level),
      K(tablegroup_schema),
      "table_count", table_schemas.count());
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_partlevel_zero_(const ObSimpleTableSchemaV2 &table_schema)
{
  // all none partition tables belong to one balance group
  // every table tablet is a single partition group
  int ret = OB_SUCCESS;
  ObBalanceGroup bg;
  if (OB_FAIL(bg.init_by_table(table_schema, NULL/*partition*/))) {
    LOG_WARN("init balance group by table fail", KR(ret), K(bg), K(table_schema));
  } else {
    ObObjectID part_object_id = table_schema.get_object_id(); // equal to table_id
    ObTabletID tablet_id = table_schema.get_tablet_id();
    const uint64_t part_group_uid = table_schema.get_table_id(); // each table is an independent partition group

    ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
  }
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_partlevel_one_(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  // one-level partition table is oneself a balance group
  // every partition of table is a single partition group
  ObBalanceGroup bg;
  if (OB_FAIL(bg.init_by_table(table_schema, NULL/*partition*/))) {
    LOG_WARN("init balance group by table fail", KR(ret), K(bg), K(table_schema));
  } else {
    for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < table_schema.get_partition_num(); part_idx++) {
      const ObPartition *part = nullptr;
      if (OB_FAIL(table_schema.get_partition_by_partition_index(part_idx, CHECK_PARTITION_MODE_NORMAL, part))) {
        LOG_WARN("get partition by part_idx fail", KR(ret), K(table_schema), K(part_idx));
      } else if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(table_schema));
      } else {
        ObObjectID part_object_id = part->get_part_id();
        ObTabletID tablet_id = part->get_tablet_id();
        const uint64_t part_group_uid = part_object_id; // each partition is an independent partition group

        ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_partlevel_two_(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  ObPartIterator iter(table_schema, CHECK_PARTITION_MODE_NORMAL);
  while (OB_SUCC(ret)) {
    const ObPartition *part = nullptr;
    if (OB_FAIL(iter.next(part))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is null", KR(ret), K(table_schema));
    } else {
      // every partition is a independent balance group
      ObBalanceGroup bg;
      if (OB_FAIL(bg.init_by_table(table_schema, part/*partition*/))) {
        LOG_WARN("init balance group by table fail", KR(ret), K(bg), K(table_schema), KPC(part));
      } else {
        ObSubPartIterator subpart_iter(table_schema, *part, CHECK_PARTITION_MODE_NORMAL);
        while (OB_SUCC(ret)) {
          const ObSubPartition *sub_part = nullptr;
          if (OB_FAIL(subpart_iter.next(sub_part))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_ISNULL(sub_part)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub partition is null", KR(ret), K(table_schema));
          } else { // every subpartition is an independent partition group
            ObObjectID part_object_id = sub_part->get_sub_part_id();
            ObTabletID tablet_id = sub_part->get_tablet_id();
            const uint64_t part_group_uid = part_object_id; // each subpartition is an independent partition group

            ADD_NEW_PART(bg, table_schema, part_object_id, tablet_id, part_group_uid);
          }
        }
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::add_new_part_(
    const ObBalanceGroup &bg,
    const ObSimpleTableSchemaV2 &table_schema,
    const ObObjectID part_object_id,
    const ObTabletID tablet_id,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls_id;
  ObLSID dest_ls_id;
  bool in_new_part_group = false;
  const uint64_t table_id = table_schema.get_table_id();
  const bool is_dup_table = table_schema.is_duplicate_table();
  uint64_t tablet_size = 0;

  if (OB_UNLIKELY(!inited_) || OB_ISNULL(callback_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(callback_));
  } else if (OB_UNLIKELY(!bg.id().is_valid()
      || !is_valid_id(part_object_id)
      || !tablet_id.is_valid()
      || !is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(bg), K(part_object_id), K(tablet_id), K(part_group_uid), K(table_schema));
  } else if (OB_FAIL(tablet_to_ls_.get(tablet_id, src_ls_id))) {
    LOG_WARN("fail to get LS info for tablet, tablet may be dropped", KR(ret), K(tablet_id), K(table_schema));
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // skip this partition
    }
  } else {
    // parts with same bg_id and part_group_uid belong to the same part group
    in_new_part_group = (bg.id() != pre_bg_id_ || part_group_uid != pre_part_group_uid_);
    dest_ls_id = (in_new_part_group ? src_ls_id : pre_dest_ls_id_);
    if (is_dup_table) {
      if (OB_UNLIKELY(!dup_ls_id_.is_valid() || !in_new_part_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected", KR(ret), K(dup_ls_id_), K(in_new_part_group), K(bg),
            K(pre_bg_id_), K(part_group_uid), K(pre_part_group_uid_), K(pre_dest_ls_id_), K(table_schema));
      } else {
        dest_ls_id = dup_ls_id_;
      }
    } else if (dup_ls_id_.is_valid() && src_ls_id == dup_ls_id_) { // normal table on dup ls
      if (OB_FAIL(get_dup_to_normal_dest_ls_id_(dest_ls_id))) {
        LOG_WARN("get dup to normal dest ls id failed", KR(ret), K(dest_ls_id), K(table_schema));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!dest_ls_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dest ls id must be valid", KR(ret), K(dup_ls_id_), K(in_new_part_group), K(bg),
          K(pre_bg_id_), K(part_group_uid), K(pre_part_group_uid_), K(pre_dest_ls_id_),
          K(pre_dup_to_normal_dest_ls_id_), K(table_schema));
    } else if (OB_FAIL(get_data_size_with_related_tablets_(tablet_id, tablet_size))) {
      LOG_WARN("get data size with related tablets failed", KR(ret), K(bg),
          K(table_id), K(part_object_id), K(tablet_id), K(src_ls_id), K(dest_ls_id),
          K(tablet_size), K(in_new_part_group), K(part_group_uid));
    } else if (OB_FAIL(callback_->on_new_partition(
        bg,
        table_id,
        part_object_id,
        tablet_id,
        src_ls_id,
        dest_ls_id,
        tablet_size,
        in_new_part_group,
        part_group_uid))) {
      LOG_WARN("callback handle new partition fail", KR(ret), K(bg), K(table_id),
          K(part_object_id), K(tablet_id), K(src_ls_id), K(dest_ls_id), K(tablet_size),
          K(in_new_part_group), K(part_group_uid), K(pre_dest_ls_id_),
          K(pre_dup_to_normal_dest_ls_id_), K(pre_bg_id_), K(pre_part_group_uid_));
    } else {
      pre_bg_id_ = bg.id();
      pre_part_group_uid_ = part_group_uid;
      pre_dest_ls_id_ = dest_ls_id;
    }
  }
  return ret;
}

// TODO: @wanhong.wwh only load USER LS tablet data size
int ObAllBalanceGroupBuilder::prepare_tablet_data_size_()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObTenantTabletMetaIterator iter;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(iter.init(*sql_proxy_, tenant_id_))) {
    LOG_WARN("init tenant tablet meta table iterator fail", KR(ret), K(tenant_id_));
  } else {
    iter.set_batch_size(1000);
    ObTabletInfo tablet_info;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next(tablet_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter next tenant meta table info fail", KR(ret), K(tenant_id_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        int64_t tablet_size = 0;
        for (int64_t i = 0; i < tablet_info.get_replicas().count(); i++) {
          if (tablet_info.get_replicas().at(i).get_data_size() > tablet_size) {
            tablet_size = tablet_info.get_replicas().at(i).get_data_size();
          }
        }
        if (OB_FAIL(tablet_data_size_.set_refactored(tablet_info.get_tablet_id(), tablet_size))) {
          LOG_WARN("tablet_data_size set_refactored fail", KR(ret), K(tenant_id_), K(tablet_info));
        }
      }
    }
  }

  int64_t finish_time = ObTimeUtility::current_time();
  ISTAT("prepare tablet data_size", KR(ret), K_(tenant_id), "cost", finish_time - start_time, "count", tablet_data_size_.size());
  return ret;
}

int ObAllBalanceGroupBuilder::prepare_related_tablets_map_()
{
  int ret = OB_SUCCESS;
  ObArray<const ObSimpleTableSchemaV2 *> tenant_table_schemas;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tenant(tenant_id_, tenant_table_schemas))) {
    LOG_WARN("get table schemas fail", KR(ret), K(tenant_id_));
  } else {
    uint64_t data_table_id = OB_INVALID_ID;
    ARRAY_FOREACH(tenant_table_schemas, idx) {
      const ObSimpleTableSchemaV2 *table_schema = tenant_table_schemas.at(idx);
      const ObSimpleTableSchemaV2 *data_table_schema = NULL;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(tenant_id_), K(idx));
      } else if (is_inner_table(table_schema->get_table_id())
          || !is_related_table(table_schema->get_table_type(), table_schema->get_index_type())) {
        // skip
      } else if (FALSE_IT(data_table_id = table_schema->get_data_table_id())) {
      } else if (OB_FAIL(schema_guard_.get_simple_table_schema(
          tenant_id_,
          data_table_id,
          data_table_schema))) {
        LOG_WARN("get simple table schema failed", KR(ret), K(tenant_id_), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table schema can't be null", KR(ret),
            K(tenant_id_), K(data_table_id), KP(data_table_schema));
      } else if (OB_FAIL(add_to_related_tablets_map_(*data_table_schema, *table_schema))) {
        LOG_WARN("build related tablets map failed",
            KR(ret), KPC(data_table_schema), KPC(table_schema));
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::add_to_related_tablets_map_(
    const ObSimpleTableSchemaV2 &primary_table_schema,
    const ObSimpleTableSchemaV2 &related_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!is_related_table(related_table_schema.get_table_type(), related_table_schema.get_index_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not related table", KR(ret), K(primary_table_schema), K(related_table_schema));
  } else if (OB_UNLIKELY(primary_table_schema.get_all_part_num() != related_table_schema.get_all_part_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part_num of primary_table_schema and related_table_schema not match",
        KR(ret), K(primary_table_schema), K(related_table_schema));
  } else {
    ObPartitionSchemaIter primary_iter(primary_table_schema, CHECK_PARTITION_MODE_NORMAL);
    ObPartitionSchemaIter related_iter(related_table_schema, CHECK_PARTITION_MODE_NORMAL);
    while (OB_SUCC(ret)) {
      ObPartitionSchemaIter::Info primary_info;
      ObPartitionSchemaIter::Info related_info;
      ObIArray<ObTabletID> *tablet_array = NULL;
      if (OB_FAIL(primary_iter.next_partition_info(primary_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter next partition partition info failed", KR(ret));
        }
      } else if (OB_FAIL(related_iter.next_partition_info(related_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter next partition info failed", KR(ret));
        }
      } else if (OB_FAIL(related_tablets_map_.get_refactored(primary_info.tablet_id_, tablet_array))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // create new tablet_array
          void *ptr = allocator_.alloc(sizeof(ObArray<ObTabletID, ObIAllocator &>));
          if (OB_ISNULL(ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail alloc memory", KR(ret));
          } else {
            tablet_array = new (ptr)ObArray<ObTabletID, ObIAllocator &>(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_);
            if (OB_FAIL(related_tablets_map_.set_refactored(primary_info.tablet_id_, tablet_array))) {
              LOG_WARN("set refactored failed", KR(ret), K(primary_info));
            }
          }
        } else {
          LOG_WARN("get_refactored failed", KR(ret), K(primary_info));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tablet_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_array can not be null", KR(ret), K(primary_info), KP(tablet_array));
      } else if (OB_FAIL(tablet_array->push_back(related_info.tablet_id_))) {
        LOG_WARN("push back failed", KR(ret), K(primary_info), K(related_info));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::get_data_size_with_related_tablets_(
    const ObTabletID &tablet_id,
    uint64_t &data_size)
{
  int ret = OB_SUCCESS;
  data_size = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_id", KR(ret), K(tablet_id));
  } else {
    // tablet data size may not exist, as meta table may not be reported or tablet be dropped
    const uint64_t *tablet_size_ptr = tablet_data_size_.get(tablet_id);
    data_size = (tablet_size_ptr != NULL ? *tablet_size_ptr : 0);
    // related tablets may not exist
    ObIArray<ObTabletID> *related_tablets = NULL;
    if (OB_FAIL(related_tablets_map_.get_refactored(tablet_id, related_tablets))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore
      } else {
        LOG_WARN("get_refactored failed", KR(ret), K(tablet_id));
      }
    } else if (OB_ISNULL(related_tablets)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("related_tablets can not be null", KR(ret), K(tablet_id), KP(related_tablets));
    } else {
      ARRAY_FOREACH(*related_tablets, idx) {
        const ObTabletID &related_tablet = related_tablets->at(idx);
        const uint64_t *related_size_ptr = tablet_data_size_.get(related_tablet);
        data_size += (related_size_ptr != NULL ? *related_size_ptr : 0);
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::get_dup_to_normal_dest_ls_id_(share::ObLSID &dest_ls_id)
{
  int ret = OB_SUCCESS;
  dest_ls_id.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KP(sql_proxy_));
  } else if (pre_dup_to_normal_dest_ls_id_.is_valid()) {
    dest_ls_id = pre_dup_to_normal_dest_ls_id_;
  } else {
    // TODO: optimize to select a more suitable user ls as dest
    ObLSAttrOperator ls_operator(tenant_id_, sql_proxy_);
    if (OB_FAIL(ls_operator.get_random_normal_user_ls(dest_ls_id))) {
      LOG_WARN("get random normal user ls failed", KR(ret), K(tenant_id_), K(dest_ls_id));
    } else {
      pre_dup_to_normal_dest_ls_id_ = dest_ls_id;
    }
  }
  return ret;
}

}
}

#undef ISTAT
#undef WSTAT
#undef ADD_NEW_PART
