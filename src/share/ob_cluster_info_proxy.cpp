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

#define USING_LOG_PREFIX SHARE
#include "ob_cluster_info_proxy.h"
#include "share/ob_core_table_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_encryption_util.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
const char* ObClusterInfoProxy::OB_ALL_CLUSTER_INFO_TNAME = "__all_cluster";
const char* ObClusterInfoProxy::CLUSTER_ROLE = "cluster_role";
const char* ObClusterInfoProxy::SWITCHOVER_STATUS = "switchover_status";
const char* ObClusterInfoProxy::SWITCHOVER_TIMESTAMP = "switchover_timestamp";
const char* ObClusterInfoProxy::CLUSTER_STATUS = "cluster_status";
const char* ObClusterInfoProxy::ENCRYPTION_KEY = "eeeeffff";
const char* ObClusterInfoProxy::PROTECTION_MODE = "protection_mode";
const char* ObClusterInfoProxy::VERSION = "version";
const char* ObClusterInfoProxy::PROTECTION_LEVEL = "protection_level";

const char* ObClusterInfo::PERSISTENT_SWITCHOVER_STATUS_ARRAY[] =
{
  "SWITCHOVER_INVALID",
  "SWITCHOVER_NORMAL",
  "SWITCHOVER_SWITCHING",
  "FAILOVER_FLASHBACK",
  "FAILOVER_CLEANUP",
  "FAILOVER_FLASHBACK_INNER",
  "FAILOVER_FLASHBACK_USER",
  "FAILOVER_FLASHBACK_INNER",
  "FAILOVER_FLASHBACK_USER",
  "FAILOVER_FLASHBACK_USER",
  "DISCONNECT",
};

const char* ObClusterInfo::IN_MEMORY_SWITCHOVER_STATUS_ARRAY[] =
{
  "INVALID",
  "NOT ALLOWED",
  "TO STANDBY",
  "TO PRIMARY",
  "SWITCHOVER SWITCHING",
  "FAILOVER FLASHBACK",
  "FAILOVER CLEANUP",
  "DISCONNECT",
};

const char* ObClusterInfo::CLUSTER_STATUS_ARRAY[]=
{
  "VALID",
  "DISABLE",
};

int ObClusterInfo::str_to_in_memory_switchover_status(const ObString &status_str, InMemorySwitchOverStatus &status)
{
  int ret = OB_SUCCESS;
  status = I_MAX_STATUS;
  if (I_MAX_STATUS != ARRAYSIZEOF(IN_MEMORY_SWITCHOVER_STATUS_ARRAY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid array size", K(ret), K(I_MAX_STATUS), "array_size",
             ARRAYSIZEOF(IN_MEMORY_SWITCHOVER_STATUS_ARRAY));
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(IN_MEMORY_SWITCHOVER_STATUS_ARRAY) && OB_SUCC(ret); i++) {
      if (STRLEN(IN_MEMORY_SWITCHOVER_STATUS_ARRAY[i]) == status_str.length()
          && 0 == STRNCASECMP(IN_MEMORY_SWITCHOVER_STATUS_ARRAY[i], status_str.ptr(), status_str.length())) {
        status = static_cast<InMemorySwitchOverStatus>(i);
        break;
      }
    }
  }
  if (OB_SUCC(ret) && I_MAX_STATUS == status) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find cluster status", K(status_str));
  }
  return ret;
}

const char* ObClusterInfo::in_memory_switchover_status_to_str(const InMemorySwitchOverStatus &status)
{
  STATIC_ASSERT(ARRAYSIZEOF(IN_MEMORY_SWITCHOVER_STATUS_ARRAY) == I_MAX_STATUS,
              "type string array size mismatch with enum InMemorySwitchOverStatus count");

  const char* str = "UNKNOWN";
  if (status <= I_INVALID || status >= I_MAX_STATUS) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid in-memory switchover status", K(status));
  } else {
    str = IN_MEMORY_SWITCHOVER_STATUS_ARRAY[status];
  }
  return str;
}

const char* ObClusterInfo::persistent_switchover_status_to_str(const PersistentSwitchOverStatus &status)
{
  STATIC_ASSERT(ARRAYSIZEOF(PERSISTENT_SWITCHOVER_STATUS_ARRAY) == P_MAX_STATUS,
                "type string array size mismatch with enum PersistentSwitchOverStatus count");

  const char* str = "UNKNOWN";
  if (status < P_SWITCHOVER_INVALID || status >= P_MAX_STATUS) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid persistent switchover status", K(status));
  } else {
    str = PERSISTENT_SWITCHOVER_STATUS_ARRAY[status];
  }
  return str;
}

bool ObClusterInfo::is_in_failover(const PersistentSwitchOverStatus &status)
{
  return P_FAILOVER_CLEANUP == status
         || is_failover_flashback(status);
}

void ObClusterInfo::reset()
{
  cluster_id_ = -1;
  cluster_role_ = INVALID_CLUSTER_ROLE;
  switchover_status_ = ObClusterInfo::P_SWITCHOVER_INVALID;
  switch_timestamp_ = 0;
  is_sync_ = false;
  cluster_status_ = INVALID_CLUSTER_STATUS;
  protection_mode_ = common::MAXIMUM_PERFORMANCE_MODE;
  version_ = OB_INVALID_VERSION;
  protection_level_ = common::MAXIMUM_PERFORMANCE_LEVEL;
}

int ObClusterInfo::assign(const ObClusterInfo &other)
{
  int ret = OB_SUCCESS;
  cluster_role_ = other.cluster_role_;
  cluster_id_ = other.cluster_id_;
  switchover_status_ = other.switchover_status_;
  switch_timestamp_ = other.switch_timestamp_;
  cluster_status_ = other.cluster_status_;
  is_sync_ = other.is_sync_;
  protection_mode_ = other.protection_mode_;
  version_ = other.version_;
  protection_level_ = other.protection_level_;
  return ret;
}

ObClusterInfo::ObClusterInfo(const ObClusterInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(other))) {
    LOG_WARN("fail to assign", K(ret));
  }
}

ObClusterInfo& ObClusterInfo::operator= (const ObClusterInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("fail to assign", K(ret));
    }
  }
  return *this;
}

bool ObClusterInfo::is_valid() const
{
  bool bret = false;
  if (INVALID_CLUSTER_ROLE != cluster_role_
      && -1 != cluster_id_) {
    bret = true;
  }
  if (bret) {
    bret = OB_INVALID_VERSION != version_;
  }
  return bret;
}

bool ObClusterInfo::operator !=(const ObClusterInfo &other) const
{
  return cluster_id_ != other.cluster_id_
      || cluster_role_ != other.cluster_role_
      || switchover_status_ != other.switchover_status_
      || switch_timestamp_ != other.switch_timestamp_
      || cluster_status_ != other.cluster_status_
      || is_sync_ != other.is_sync_
      || protection_mode_ != other.protection_mode_
      || version_ != other.version_
      || protection_level_ != other.protection_level_;
}

int64_t ObClusterInfo::generate_switch_timestamp(const int64_t switch_timestamp)
{
  return switch_timestamp & (~SWITCH_TIMSTAMP_MASK);
}
int ObClusterInfo::inc_switch_timestamp()
{
  int ret = OB_SUCCESS;
  int64_t switch_times = switch_timestamp_ & SWITCH_TIMSTAMP_MASK;
  if (switch_times >= MAX_CHANGE_TIMES) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("switch retry too much times", KR(ret), K(switch_times));
  } else {
    ++ switch_timestamp_;
  }
  LOG_INFO("inc switch timestamp", K(switch_timestamp_), K(switch_times));
  return ret;
}

int64_t ObClusterInfo::get_pure_switch_timestamp(const int64_t switch_timestamp)
{
  return switch_timestamp & (~SWITCH_TIMSTAMP_MASK);
}

bool ObClusterInfo::is_less_than(const int64_t switch_timestamp) const
{
  return switch_timestamp_ < switch_timestamp;
}

OB_SERIALIZE_MEMBER(ObClusterInfo, cluster_role_, switchover_status_,
                    is_sync_, cluster_id_, switch_timestamp_, cluster_status_,
                    protection_mode_, version_, protection_level_);

//////////////////////////////////////////////////
//////////////////////////////////////////////////
ObClusterInfoProxy::ObClusterInfoProxy()
{}

ObClusterInfoProxy::~ObClusterInfoProxy()
{
}

// get the gmt_create value of specific row of __all_core_table as the cluster creation time
// table_name='__all_global_stat', column_name='snapshot_gc_scn'
int ObClusterInfoProxy::load_cluster_create_timestamp(ObISQLClient &sql_proxy,
    int64_t &cluster_create_ts)
{
  int ret = OB_SUCCESS;
  static const char* TABLE_NAME  = "__all_global_stat";
  static const char* COLUMN_NAME  = "snapshot_gc_scn";

  ObCoreTableProxy core_table(TABLE_NAME, sql_proxy, OB_SYS_TENANT_ID);
  if (OB_FAIL(core_table.load_gmt_create(COLUMN_NAME, cluster_create_ts))) {
    LOG_WARN("load gmt_create column of core table fail", KR(ret), K(TABLE_NAME), K(COLUMN_NAME));
  }

  LOG_INFO("load cluster create timestamp finish", KR(ret), K(cluster_create_ts), K(TABLE_NAME),
      K(COLUMN_NAME));
  return ret;
}

int ObClusterInfoProxy::load(ObISQLClient &sql_proxy, ObClusterInfo &cluster_info,
                             const bool for_update)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_CLUSTER_INFO_TNAME, sql_proxy, OB_SYS_TENANT_ID);
  int64_t cluster_role = -1;
  int64_t switchover_status = -1;
  int64_t passwd_length = 0;
  int64_t switch_timestamp = 0;
  int64_t cluster_status = 0;
  int64_t protection_mode = 0;
  int64_t protection_level = 0;

  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (for_update && OB_FAIL(core_table.load_for_update())) {
    LOG_WARN("failed to load for update", KR(ret), K(for_update));
  } else if (!for_update && OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  } else if (OB_FAIL(core_table.next())) {
    if (OB_ITER_END == ret) {
      LOG_WARN("get empty cluster info, maybe in bootstrap", K(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to next", K(ret));
    }
  } else if (OB_FAIL(core_table.get_int(CLUSTER_ROLE, cluster_role))) {
    LOG_WARN("fail to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(SWITCHOVER_STATUS, switchover_status))) {
    LOG_WARN("fail to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(SWITCHOVER_TIMESTAMP, switch_timestamp))) {
    LOG_WARN("fail to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(CLUSTER_STATUS, cluster_status))) {
    LOG_WARN("fail to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(PROTECTION_MODE, protection_mode))) {
    LOG_WARN("failed to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(VERSION, cluster_info.version_))) {
    LOG_WARN("failed to get int", KR(ret));
  } else if (OB_FAIL(core_table.get_int(PROTECTION_LEVEL, protection_level))) {
    LOG_WARN("failed to get int", KR(ret));
  } else {
    cluster_info.cluster_role_ = static_cast<ObClusterRole>(cluster_role);
    cluster_info.switchover_status_ = static_cast<ObClusterInfo::PersistentSwitchOverStatus>(switchover_status);
    cluster_info.set_switch_timestamp(switch_timestamp);
    cluster_info.cluster_id_ = GCTX.config_->cluster_id;
    cluster_info.cluster_status_ = static_cast<ObClusterStatus>(cluster_status);
    cluster_info.protection_mode_ = static_cast<ObProtectionMode>(protection_mode);
    cluster_info.protection_level_ = static_cast<ObProtectionLevel>(protection_level);
  }
  if (OB_SUCC(ret)) {
    if (OB_ITER_END != (ret = core_table.next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid next", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  LOG_INFO("load all cluster finish", K(ret), K(cluster_info));
  return ret;
}

int ObClusterInfoProxy::update(ObISQLClient &sql_proxy, const ObClusterInfo &cluster_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to update cluster info", K(cluster_info));
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObCoreTableProxy kv(OB_ALL_CLUSTER_INFO_TNAME, sql_proxy, OB_SYS_TENANT_ID);

  ObTimeoutCtx ctx;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(dml.add_column(CLUSTER_ROLE, cluster_info.cluster_role_))
             || OB_FAIL(dml.add_column(SWITCHOVER_STATUS, cluster_info.switchover_status_))
             || OB_FAIL(dml.add_column(SWITCHOVER_TIMESTAMP, cluster_info.get_switch_timestamp()))
             || OB_FAIL(dml.add_column(CLUSTER_STATUS, cluster_info.cluster_status_))
             || OB_FAIL(dml.add_column(PROTECTION_MODE, cluster_info.protection_mode_))
             || OB_FAIL(dml.add_column(VERSION, cluster_info.version_))
             || OB_FAIL(dml.add_column(PROTECTION_LEVEL, cluster_info.protection_level_))) {
    LOG_WARN("fail to add column", KR(ret), K(cluster_info));
  } else if (OB_FAIL(kv.load_for_update())) {
    LOG_WARN("fail to load for update", K(ret));
  } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
    LOG_WARN("fail to splice core cells", K(ret));
  } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
    LOG_WARN("fail to replace row", K(ret), K(cluster_info));
  }
  return ret;
}

}
}

