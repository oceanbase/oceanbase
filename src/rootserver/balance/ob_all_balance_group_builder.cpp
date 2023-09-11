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

#include "share/ob_balance_define.h"                  // need_balance_table()
#include "share/tablet/ob_tablet_to_ls_iterator.h"    // ObTenantTabletToLSIterator
#include "share/tablet/ob_tablet_table_iterator.h"    // ObTenantTabletMetaIterator
#include "share/schema/ob_part_mgr_util.h"            // ObPartitionSchemaIter
#include "share/schema/ob_schema_mgr_cache.h"         // ObSchemaMgrItem

#include "rootserver/ob_partition_balance.h"         // ObPartitionHelper
#include "ob_all_balance_group_builder.h"

#define ISTAT(fmt, args...) FLOG_INFO("[BALANCE_GROUP_BUILDER] " fmt, K_(mod), ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[BALANCE_GROUP_BUILDER] " fmt, K_(mod), ##args)

#define ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid) \
    do {\
      if (OB_FAIL(add_new_part_(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid))) {\
        LOG_WARN("add new partition fail", KR(ret), K(bg), K(table_id), K(part_object_id), \
            K(dest_ls_id), K(in_new_pg), K(part_group_uid));\
      }\
    } while (0)

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
namespace rootserver
{
ObAllBalanceGroupBuilder::ObAllBalanceGroupBuilder() :
    inited_(false),
    mod_(""),
    tenant_id_(OB_INVALID_TENANT_ID),
    callback_(NULL),
    sql_proxy_(NULL),
    schema_service_(NULL),
    schema_guard_(schema::ObSchemaMgrItem::MOD_PARTITION_BALANCE),
    tablet_to_ls_(),
    tablet_data_size_()
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
  } else {
    mod_ = mod;
    tenant_id_ = tenant_id;
    callback_ = &callback;
    sql_proxy_ = &sql_proxy;
    schema_service_ = &schema_service;
    schema_guard_.reset();
    inited_ = true;
  }
  return ret;
}

void ObAllBalanceGroupBuilder::destroy()
{
  inited_ = false;
  tablet_data_size_.destroy();
  tablet_to_ls_.destroy();
  schema_guard_.reset();
  schema_service_ = NULL;
  sql_proxy_ = NULL;
  callback_ = NULL;
  tenant_id_ = OB_INVALID_TENANT_ID;
  mod_ = "";
}

int ObAllBalanceGroupBuilder::prepare(bool need_load_tablet_size)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t step_time = 0;

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
  } else if (need_load_tablet_size && OB_FAIL(prepare_tablet_data_size_())) {
    LOG_WARN("prepare tablet data size fail", KR(ret));
  }

  int64_t finish_time = ObTimeUtility::current_time();
  ISTAT("prepare data", KR(ret), K_(tenant_id), "cost", finish_time - start_time,
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

int ObAllBalanceGroupBuilder::build_balance_group_for_table_not_in_tablegroup_(
    const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;

  if (!table_schema.is_global_index_table() && OB_INVALID_ID != table_schema.get_tablegroup_id()) {
    // skip table not in tablegroup
    // global index should not in tablegroup, here is defensive code
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
int ObAllBalanceGroupBuilder::build_bg_for_tablegroup_sharding_none_(
    const ObSimpleTablegroupSchema &tablegroup_schema,
    const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
    const int64_t max_part_level)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg;
  const ObString &tablegroup_name = tablegroup_schema.get_tablegroup_name();
  if (OB_FAIL(bg.init_by_tablegroup(tablegroup_schema, max_part_level))) {
    LOG_WARN("init balance group by tablegroup fail", KR(ret), K(bg), K(max_part_level),
        K(tablegroup_schema));
  } else {
    ObLSID dest_ls_id; // binding to the first table first tablet
    bool in_new_pg = true; // in new partition group
    const uint64_t part_group_uid = 0; // all partitions belong to the same partition group for each LS
    for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(t);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_schema is null", KR(ret), K(tenant_id_), K(tablegroup_schema), K(t));
      } else {
        const uint64_t table_id = table_schema->get_table_id();
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

            ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
          }
        }
      }
    }
  }

  ISTAT("build balance group for table group of NONE sharding or non-partition tables",
      KR(ret),
      K(max_part_level),
      K(tablegroup_schema),
      "table_count", table_schemas.count());
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
    ObLSID dest_ls_id;
    // partitions/subpartitions of all tables with same one-level-partition-value, are in the same partition group.
    // Here, partitions/subpartitions with same one-level-partition-index of all tables are in the same partition group
    bool in_new_pg = true; // in new partition group
    const uint64_t part_group_uid = p; // all partitions/subpartitions with same one-level part index belong to the same partition group for each LS
    for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
      const ObSimpleTableSchemaV2 &table_schema = *table_schemas.at(t);
      const uint64_t table_id = table_schema.get_table_id();
      if (PARTITION_LEVEL_ONE == table_schema.get_part_level()) {
        ObPartitionHelper::ObPartInfo part_info;
        if (OB_FAIL(ObPartitionHelper::get_part_info(table_schema, p, part_info))) {
          LOG_WARN("get part info fail", KR(ret), K(table_schema), K(p));
        } else {
          // one-level partition object id
          ObObjectID part_object_id = part_info.get_part_id();
          ObTabletID tablet_id = part_info.get_tablet_id();

          ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
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

              ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
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
        ObLSID dest_ls_id;
        bool in_new_pg = true; // in new partition group
        const uint64_t part_group_uid = sp; // subpartitions with same sub_part_idx belong to the same partition group for each LS
        for (int64_t t = 0; OB_SUCC(ret) && t < table_schemas.count(); t++) {
          const ObSimpleTableSchemaV2 &table_schema = *table_schemas.at(t);
          const uint64_t table_id = table_schema.get_table_id();
          ObPartitionHelper::ObPartInfo part_info;
          if (OB_FAIL(ObPartitionHelper::get_sub_part_info(table_schema, part_idx, sp, part_info))) {
            LOG_WARN("get sub partition info fail", KR(ret), K(table_schema), K(part_idx), K(sp));
          } else {
            // two-level subpartition object id
            ObObjectID part_object_id = part_info.get_part_id();
            ObTabletID tablet_id = part_info.get_tablet_id();

            ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
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
    bool in_new_pg = true; // in new partition group
    ObLSID dest_ls_id;
    const uint64_t table_id = table_schema.get_table_id();
    ObObjectID part_object_id = 0;
    ObTabletID tablet_id = table_schema.get_tablet_id();
    const uint64_t part_group_uid = table_id; // each table is an independent partition group

    ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
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
    const uint64_t table_id = table_schema.get_table_id();
    for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < table_schema.get_partition_num(); part_idx++) {
      const ObPartition *part = nullptr;
      if (OB_FAIL(table_schema.get_partition_by_partition_index(part_idx, CHECK_PARTITION_MODE_NORMAL, part))) {
        LOG_WARN("get partition by part_idx fail", KR(ret), K(table_schema), K(part_idx));
      } else if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", KR(ret), K(table_schema));
      } else {
        bool in_new_pg = true; // in new partition group
        ObLSID dest_ls_id;
        ObObjectID part_object_id = part->get_part_id();
        ObTabletID tablet_id = part->get_tablet_id();
        const uint64_t part_group_uid = part_object_id; // each partition is an independent partition group

        ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::build_bg_for_partlevel_two_(const ObSimpleTableSchemaV2 &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = table_schema.get_table_id();
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
          } else {
            // every subpartition is an independent partition group
            ObLSID dest_ls_id;
            bool in_new_pg = true; // in new partition group
            ObObjectID part_object_id = sub_part->get_sub_part_id();
            ObTabletID tablet_id = sub_part->get_tablet_id();
            const uint64_t part_group_uid = part_object_id; // each subpartition is an independent partition group

            ADD_NEW_PART(bg, table_id, part_object_id, tablet_id, dest_ls_id, in_new_pg, part_group_uid);
          }
        }
      }
    }
  }
  return ret;
}

int ObAllBalanceGroupBuilder::add_new_part_(
    const ObBalanceGroup &bg,
    const ObObjectID table_id,
    const ObObjectID part_object_id,
    const ObTabletID tablet_id,
    ObLSID &dest_ls_id,
    bool &in_new_partition_group,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls_id;

  // Tablet data size may not exist, as meta table may not be reported or tablet be dropped
  // Ignore tablet size not exist error
  const uint64_t *tablet_size_ptr = tablet_data_size_.get(tablet_id);
  const uint64_t tablet_size = (tablet_size_ptr != nullptr ? *tablet_size_ptr : 0);

  if (OB_ISNULL(callback_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("callback is NULL, unexpected", KR(ret), K(callback_));
  } else if (OB_FAIL(tablet_to_ls_.get(tablet_id, src_ls_id))) {
    LOG_WARN("fail to get LS info for tablet, tablet may be dropped", KR(ret), K(tablet_id), K(table_id));
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      // skip this partition
    }
  } else if ((in_new_partition_group && dest_ls_id.is_valid())
      || (!in_new_partition_group && !dest_ls_id.is_valid())
      || !is_valid_id(part_group_uid)) {
    // dest_ls_id should only be valid when this partition is the first partition in new partition group
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(in_new_partition_group),
        K(dest_ls_id), K(bg), K(table_id), K(part_object_id), K(tablet_id), K(part_group_uid));
  } else if (in_new_partition_group && FALSE_IT(dest_ls_id = src_ls_id)) {
    // use first partition's LS as all other partitions' LS in same partition group
  } else if (OB_FAIL(callback_->on_new_partition(
      bg,
      table_id,
      part_object_id,
      tablet_id,
      src_ls_id,
      dest_ls_id,
      tablet_size,
      in_new_partition_group,
      part_group_uid))) {
    LOG_WARN("callback handle new partition fail", KR(ret), K(bg), K(table_id), K(part_object_id),
        K(tablet_id), K(src_ls_id), K(dest_ls_id), K(tablet_size), K(in_new_partition_group), K(part_group_uid));
  } else {
    // auto clear flag
    in_new_partition_group = false;
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

}
}

#undef ISTAT
#undef WSTAT
#undef ADD_NEW_PART
