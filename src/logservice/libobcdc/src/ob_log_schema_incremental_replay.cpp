/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX OBLOG

#include "ob_log_schema_incremental_replay.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[LOG_META_DATA] [REPLAYER] [SCHMEA] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[LOG_META_DATA] [REPLAYER] [SCHMEA] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

namespace oceanbase
{
namespace libobcdc
{
ObLogSchemaIncReplay::ObLogSchemaIncReplay() :
    is_inited_(false),
    is_start_progress_(false)
{
}

ObLogSchemaIncReplay::~ObLogSchemaIncReplay()
{
  destroy();
}

int ObLogSchemaIncReplay::init(const bool is_start_progress)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogSchemaIncReplay has been inited twice", KR(ret));
  } else {
    is_inited_ = true;
    is_start_progress_ = is_start_progress;
    ISTAT("ObLogSchemaIncReplay init success", K(is_start_progress));
  }

  return ret;
}

void ObLogSchemaIncReplay::destroy()
{
  if (is_inited_) {
    is_start_progress_ = false;
    is_inited_ = false;
  }
}

int ObLogSchemaIncReplay::replay(
    const PartTransTask &part_trans_task,
    const ObIArray<const datadict::ObDictTenantMeta*> &tenant_metas,
    const ObIArray<const datadict::ObDictDatabaseMeta*> &database_metas,
    const ObIArray<const datadict::ObDictTableMeta*> &table_metas,
    ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSchemaIncReplay is not initialized", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_info.get_tenant_id();
    const char *tenant_name = tenant_info.get_tenant_name();
    const transaction::ObTransID &trans_id = part_trans_task.get_trans_id();
    const int64_t tenant_count = tenant_metas.count();
    const int64_t database_count = database_metas.count();
    const int64_t table_count = table_metas.count();
    ISTAT("replay begin", "IS_STARTING_UP", is_start_progress_, K(tenant_id), K(tenant_name), K(trans_id),
        K(tenant_count), K(database_count), K(table_count));

    if (OB_SUCC(ret) && tenant_count > 0) {
      if (OB_FAIL(replay_tenant_metas_(trans_id, tenant_metas, tenant_info))) {
        LOG_ERROR("replay_tenant_metas_ failed", KR(ret), K(tenant_metas), K(tenant_info));
      }
    }

    if (OB_SUCC(ret) && database_count > 0) {
      if (OB_FAIL(replay_database_metas_(trans_id, database_metas, tenant_info))) {
        LOG_ERROR("replay_database_metas_ failed", KR(ret), K(database_metas), K(tenant_info));
      }
    }

    if (OB_SUCC(ret) && table_count > 0) {
      if (OB_FAIL(replay_table_metas_(trans_id, table_metas, tenant_info))) {
        LOG_ERROR("replay_table_metas_ failed", KR(ret), K(table_metas), K(tenant_info));
      }
    }

    ISTAT("replay end", KR(ret), "IS_STARTING_UP", is_start_progress_, K(tenant_id), K(tenant_name), K(trans_id));
  }

  return ret;
}

int ObLogSchemaIncReplay::replay_tenant_metas_(
    const transaction::ObTransID &trans_id,
    const ObIArray<const datadict::ObDictTenantMeta*> &tenant_metas,
    ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSchemaIncReplay is not initialized", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_info.get_tenant_id();
    ISTAT("replay_tenant_metas begin", K(tenant_id), K(trans_id),
        "tenant_count", tenant_metas.count(), K(tenant_metas));

    ARRAY_FOREACH_N(tenant_metas, idx, count) {
      datadict::ObDictTenantMeta *tenant_meta = const_cast<datadict::ObDictTenantMeta *>(tenant_metas.at(idx));

      if (OB_ISNULL(tenant_meta)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", KR(ret), K(tenant_meta));
      } else {
        const uint64_t tenant_id = tenant_meta->get_tenant_id();

        if (OB_FAIL(tenant_info.replace_dict_tenant_meta(tenant_meta))) {
          LOG_ERROR("tenant_info replace_dict_tenant_meta failed", KR(ret), K(tenant_id), KPC(tenant_meta));
        }
      }
    } // ARRAY_FOREACH_N

    ISTAT("replay_tenant_metas end", KR(ret), "tenant_count", tenant_metas.count());
  }

  return ret;
}

int ObLogSchemaIncReplay::replay_database_metas_(
    const transaction::ObTransID &trans_id,
    const ObIArray<const datadict::ObDictDatabaseMeta*> &database_metas,
    ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSchemaIncReplay is not initialized", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_info.get_tenant_id();
    ISTAT("replay_database_metas begin", K(tenant_id), K(trans_id),
        "database_count", database_metas.count(), K(database_metas));

    ARRAY_FOREACH_N(database_metas, idx, count) {
      const datadict::ObDictDatabaseMeta *database_meta = database_metas.at(idx);

      if (OB_ISNULL(database_meta)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", KR(ret), K(database_meta));
      } else {
        const uint64_t db_id = database_meta->get_database_id();

        if (OB_FAIL(tenant_info.replace_dict_db_meta(*database_meta))) {
          LOG_ERROR("tenant_info replace_dict_db_meta failed", KR(ret), K(db_id), KPC(database_meta));
        }
      }
    } // ARRAY_FOREACH_N

    ISTAT("replay_database_metas end", KR(ret), "database_count", database_metas.count());
  }

  return ret;
}

int ObLogSchemaIncReplay::replay_table_metas_(
    const transaction::ObTransID &trans_id,
    const ObIArray<const datadict::ObDictTableMeta*> &table_metas,
    ObDictTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogSchemaIncReplay is not initialized", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_info.get_tenant_id();
    ISTAT("replay_table_metas begin", K(tenant_id), K(trans_id),
        "table_count", table_metas.count(), K(table_metas));

    ARRAY_FOREACH_N(table_metas, idx, count) {
      const datadict::ObDictTableMeta *table_meta = table_metas.at(idx);

      if (OB_ISNULL(table_meta)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", KR(ret), K(table_meta));
      } else {
        const uint64_t table_id = table_meta->get_table_id();

        if (OB_FAIL(tenant_info.replace_dict_table_meta(*table_meta))) {
          LOG_ERROR("tenant_info replace_dict_table_meta failed", KR(ret), K(table_id), KPC(table_meta));
        }
      }
    } // ARRAY_FOREACH_N

    ISTAT("replay_table_metas end", KR(ret), "table_count", table_metas.count());
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
