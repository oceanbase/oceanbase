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

#ifndef OCEANBASE_ROOTSERVER_OB_SYSTEM_ADMIN_UTIL_H_
#define OCEANBASE_ROOTSERVER_OB_SYSTEM_ADMIN_UTIL_H_

#include <stdlib.h>
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_role.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/omt/ob_tenant_config.h"

// system admin command (alter system ...) execute

namespace oceanbase
{
namespace common
{
class ObAddr;
class ObMySQLProxy;
class ObConfigManager;
}

namespace obrpc
{
class ObSrvRpcProxy;
class ObCommonRpcProxy;
struct ObAdminSwitchReplicaRoleArg;
struct ObAdminDropReplicaArg;
struct ObAdminChangeReplicaArg;
struct ObAdminMigrateReplicaArg;
struct ObServerZoneArg;
struct ObAdminReportReplicaArg;
struct ObAdminRecycleReplicaArg;
struct ObAdminMergeArg;
struct ObAdminClearRoottableArg;
struct ObAdminRefreshSchemaArg;
struct ObAdminSetConfigArg;
struct ObAdminClearLocationCacheArg;
struct ObAdminMigrateUnitArg;
struct ObRunJobArg;
struct ObAdminFlushCacheArg;
struct ObFlushCacheArg;
struct Bool;
}

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaGetterGuard;
}
}

namespace rootserver
{
class ObZoneManager;
class ObServerManager;
class ObDDLService;
class ObUnitManager;
class ObRootInspection;
class ObRootBalancer;
class ObRootService;
class ObSchemaSplitExecutor;
class ObUpgradeStorageFormatVersionExecutor;
class ObCreateInnerSchemaExecutor;
class ObRsStatus;
class ObRsGtsManager;
namespace config_error
{
const static char * const INVALID_DISK_WATERLEVEL = "cannot specify disk waterlevel to zero when tenant groups matrix is specified";
const static char * const NOT_ALLOW_MOIDFY_CONFIG_WITHOUT_UPGRADE = "cannot moidfy enable_major_freeze/enable_ddl while enable_upgrade_mode is off";
const static char * const NOT_ALLOW_ENABLE_ONE_PHASE_COMMIT_FOR_PRIMARY = "Cannot enable one phase commit while the primary cluster has standby cluster";
const static char * const NOT_ALLOW_ENABLE_ONE_PHASE_COMMIT_FOR_STANDBY = "Cannot enable one phase commit on standby cluster";
const static char * const NOT_ALLOW_ENABLE_ONE_PHASE_COMMIT_FOR_INVALID = "Cannot enable one phase commit on invalid cluster";
const static char * const NOT_ALLOW_ENABLE_ONE_PHASE_COMMIT = "enable_one_phase_commit not supported";
};

struct ObSystemAdminCtx
{
  ObSystemAdminCtx()
      : rs_status_(NULL), rpc_proxy_(NULL), sql_proxy_(NULL), server_mgr_(NULL),
      zone_mgr_(NULL), schema_service_(NULL),
      ddl_service_(NULL), config_mgr_(NULL), unit_mgr_(NULL), root_inspection_(NULL),
      root_service_(NULL), root_balancer_(NULL), upgrade_storage_format_executor_(nullptr),
      create_inner_schema_executor_(nullptr), inited_(false)
  {}

  bool is_inited() const { return inited_; }

  ObRsStatus *rs_status_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  ObServerManager *server_mgr_;
  ObZoneManager *zone_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObDDLService *ddl_service_;
  common::ObConfigManager *config_mgr_;
  ObUnitManager *unit_mgr_;
  ObRootInspection *root_inspection_;
  ObRootService *root_service_;
  ObRootBalancer *root_balancer_;
  ObUpgradeStorageFormatVersionExecutor *upgrade_storage_format_executor_;
  ObCreateInnerSchemaExecutor *create_inner_schema_executor_;
  bool inited_;
};

class ObSystemAdminUtil
{
public:
  const static int64_t WAIT_LEADER_SWITCH_TIMEOUT_US = 10 * 1000 * 1000; // 10s
  const static int64_t WAIT_LEADER_SWITCH_INTERVAL_US = 300 * 1000; // 300ms

  explicit ObSystemAdminUtil(const ObSystemAdminCtx &ctx) : ctx_(ctx) {}
  virtual ~ObSystemAdminUtil() {}

  int check_service() const;
protected:
    const ObSystemAdminCtx &ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSystemAdminUtil);
};

class ObAdminSwitchReplicaRole : public ObSystemAdminUtil
{
public:
  explicit ObAdminSwitchReplicaRole(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminSwitchReplicaRole() {}

  int execute(const obrpc::ObAdminSwitchReplicaRoleArg &arg);

private:
  static const int64_t TENANT_BUCKET_NUM = 1000;

  static int alloc_tenant_id_set(common::hash::ObHashSet<uint64_t> &tenant_id_set);
  template<typename T>
  static int convert_set_to_array(const common::hash::ObHashSet<T> &set,
      ObArray<T> &array);
  int get_tenants_of_zone(const common::ObZone &zone,
      common::hash::ObHashSet<uint64_t> &tenant_id_set);
  int get_switch_replica_tenants(const common::ObZone &zone, const common::ObAddr &server,
      const uint64_t &tenant_id, common::ObArray<uint64_t> &tenant_ids);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminSwitchReplicaRole);
};

class ObAdminCallServer : public ObSystemAdminUtil
{
public:
  explicit ObAdminCallServer(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminCallServer() {}

  int get_server_list(const obrpc::ObServerZoneArg &arg, ObIArray<ObAddr> &server_list);
  int call_all(const obrpc::ObServerZoneArg &arg);

  virtual int call_server(const common::ObAddr &server) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminCallServer);
};

class ObAdminReportReplica : public ObAdminCallServer
{
public:
  explicit ObAdminReportReplica(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminReportReplica() {}

  int execute(const obrpc::ObAdminReportReplicaArg &arg);

  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminReportReplica);
};

class ObAdminRecycleReplica : public ObAdminCallServer
{
public:
  explicit ObAdminRecycleReplica(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminRecycleReplica() {}

  int execute(const obrpc::ObAdminRecycleReplicaArg &arg);

  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRecycleReplica);
};

class ObAdminClearLocationCache : public ObAdminCallServer
{
public:
  explicit ObAdminClearLocationCache(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminClearLocationCache() {}

  int execute(const obrpc::ObAdminClearLocationCacheArg &arg);

  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminClearLocationCache);
};

class ObAdminRefreshMemStat : public ObAdminCallServer
{
public:
  explicit ObAdminRefreshMemStat(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminRefreshMemStat() {}

  int execute(const obrpc::ObAdminRefreshMemStatArg &arg);
  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRefreshMemStat);
};

class ObAdminWashMemFragmentation : public ObAdminCallServer
{
public:
  explicit ObAdminWashMemFragmentation(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminWashMemFragmentation() {}

  int execute(const obrpc::ObAdminWashMemFragmentationArg &arg);
  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminWashMemFragmentation);
};

class ObAdminReloadUnit : public ObSystemAdminUtil
{
public:
  explicit ObAdminReloadUnit(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminReloadUnit() {}

  int execute();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminReloadUnit);
};

class ObAdminReloadServer : public ObSystemAdminUtil
{
public:
  explicit ObAdminReloadServer(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminReloadServer() {}

  int execute();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminReloadServer);
};

class ObAdminReloadZone : public ObSystemAdminUtil
{
public:
  explicit ObAdminReloadZone(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminReloadZone() {}

  int execute();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminReloadZone);
};

class ObAdminClearMergeError: public ObSystemAdminUtil
{
public:
  explicit ObAdminClearMergeError(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminClearMergeError() {}

  int execute(const obrpc::ObAdminMergeArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminClearMergeError);
};

class ObAdminZoneFastRecovery : public ObSystemAdminUtil
{
public:
  explicit ObAdminZoneFastRecovery(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminZoneFastRecovery() {}

  int execute(const obrpc::ObAdminRecoveryArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminZoneFastRecovery);
};

class ObAdminMerge : public ObSystemAdminUtil
{
public:
  explicit ObAdminMerge(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminMerge() {}

  int execute(const obrpc::ObAdminMergeArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminMerge);
};

class ObAdminClearRoottable: public ObSystemAdminUtil
{
public:
  explicit ObAdminClearRoottable(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminClearRoottable() {}

  int execute(const obrpc::ObAdminClearRoottableArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminClearRoottable);
};

class ObAdminRefreshSchema: public ObAdminCallServer
{
public:
  explicit ObAdminRefreshSchema(const ObSystemAdminCtx &ctx)
      : ObAdminCallServer(ctx), schema_version_(0), schema_info_() {}
  virtual ~ObAdminRefreshSchema() {}

  int execute(const obrpc::ObAdminRefreshSchemaArg &arg);

  virtual int call_server(const common::ObAddr &server);
private:
  int64_t schema_version_;
  share::schema::ObRefreshSchemaInfo schema_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRefreshSchema);
};

class ObAdminSetConfig : public ObSystemAdminUtil
{
public:
  static const uint64_t OB_PARAMETER_SEED_ID = UINT64_MAX;
  explicit ObAdminSetConfig(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminSetConfig() {}

  int execute(obrpc::ObAdminSetConfigArg &arg);

private:
  class ObServerConfigChecker : public common::ObServerConfig
  {
  };
  class ObTenantConfigChecker : public omt::ObTenantConfig
  {
  };

private:
  int verify_config(obrpc::ObAdminSetConfigArg &arg);
  int update_config(obrpc::ObAdminSetConfigArg &arg, int64_t new_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminSetConfig);
};

class ObAdminMigrateUnit : public ObSystemAdminUtil
{
public:
  explicit ObAdminMigrateUnit(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminMigrateUnit() {}

  int execute(const obrpc::ObAdminMigrateUnitArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminMigrateUnit);
};

class ObAdminUpgradeVirtualSchema : public ObSystemAdminUtil
{
public:
  explicit ObAdminUpgradeVirtualSchema(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminUpgradeVirtualSchema() {}

  int execute();
  int execute(const uint64_t tenant_id, int64_t &upgrade_cnt);
private:
  int upgrade_(const uint64_t tenant_id, share::schema::ObTableSchema &table);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminUpgradeVirtualSchema);
};

class ObAdminUpgradeCmd : public ObSystemAdminUtil
{
public:
  explicit ObAdminUpgradeCmd(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminUpgradeCmd() {}

  int execute(const obrpc::Bool &upgrade);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminUpgradeCmd);
};

class ObAdminRollingUpgradeCmd : public ObSystemAdminUtil
{
public:
  explicit ObAdminRollingUpgradeCmd(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminRollingUpgradeCmd() {}

  int execute(const obrpc::ObAdminRollingUpgradeArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRollingUpgradeCmd);
};

#define OB_INNER_JOB_DEF(JOB)                                \
    JOB(INVALID_INNER_JOB, = 0)                              \
    JOB(CHECK_PARTITION_TABLE,)                              \
    JOB(ROOT_INSPECTION,)                                    \
    JOB(UPGRADE_STORAGE_FORMAT_VERSION,)                     \
    JOB(STOP_UPGRADE_STORAGE_FORMAT_VERSION,)                \
    JOB(CREATE_INNER_SCHEMA,)                                \
    JOB(IO_CALIBRATION,)                                     \
    JOB(MAX_INNER_JOB,)

DECLARE_ENUM(ObInnerJob, inner_job, OB_INNER_JOB_DEF);

class ObAdminRunJob : public ObSystemAdminUtil
{
public:
  explicit ObAdminRunJob(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminRunJob() {}

  int execute(const obrpc::ObRunJobArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRunJob);
};

class ObAdminCheckPartitionTable : public ObAdminCallServer
{
public:
  explicit ObAdminCheckPartitionTable(const ObSystemAdminCtx &ctx) : ObAdminCallServer(ctx) {}
  virtual ~ObAdminCheckPartitionTable() {}

  int execute(const obrpc::ObRunJobArg &arg);

  virtual int call_server(const common::ObAddr &server);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminCheckPartitionTable);
};

class ObAdminRootInspection : public ObSystemAdminUtil
{
public:
  explicit ObAdminRootInspection(const ObSystemAdminCtx &ctx) : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminRootInspection() {}

  int execute(const obrpc::ObRunJobArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRootInspection);
};

class ObAdminCreateInnerSchema : public ObSystemAdminUtil
{
public:
  explicit ObAdminCreateInnerSchema(const ObSystemAdminCtx &ctx)
    : ObSystemAdminUtil(ctx) {}
  virtual ~ObAdminCreateInnerSchema() {}

  int execute(const obrpc::ObRunJobArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminCreateInnerSchema);
};

class ObAdminIOCalibration : public ObAdminCallServer
{
public:
  explicit ObAdminIOCalibration(const ObSystemAdminCtx &ctx)
    : ObAdminCallServer(ctx) {}
  virtual ~ObAdminIOCalibration() {}

  int execute(const obrpc::ObRunJobArg &arg);
  virtual int call_server(const common::ObAddr &server) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminIOCalibration);
};

class ObAdminRefreshIOCalibration : public ObAdminCallServer
{
public:
  explicit ObAdminRefreshIOCalibration(const ObSystemAdminCtx &ctx)
    : ObAdminCallServer(ctx) {}
  virtual ~ObAdminRefreshIOCalibration() {}

  int execute(const obrpc::ObAdminRefreshIOCalibrationArg &arg);
  int call_server(const common::ObAddr &server);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminRefreshIOCalibration);
};

class ObTenantServerAdminUtil : public ObSystemAdminUtil
{
public:
  explicit ObTenantServerAdminUtil(const ObSystemAdminCtx &ctx)
            : ObSystemAdminUtil(ctx)
  {}

  int get_all_servers(common::ObIArray<ObAddr> &servers);
  int get_tenant_servers(const uint64_t tenant_id, common::ObIArray<ObAddr> &servers);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantServerAdminUtil);
};

class ObAdminUpgradeStorageFormatVersionExecutor: public ObSystemAdminUtil
{
public:
  explicit ObAdminUpgradeStorageFormatVersionExecutor(const ObSystemAdminCtx &ctx)
      : ObSystemAdminUtil(ctx)
  {}
  virtual ~ObAdminUpgradeStorageFormatVersionExecutor() = default;
  int execute(const obrpc::ObRunJobArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminUpgradeStorageFormatVersionExecutor);
};

class ObAdminFlushCache : public ObTenantServerAdminUtil
{
public:
  explicit ObAdminFlushCache(const ObSystemAdminCtx &ctx)
    : ObTenantServerAdminUtil(ctx)
  {}
  virtual ~ObAdminFlushCache() {}

  int call_server(const common::ObAddr &addr, const obrpc::ObFlushCacheArg &arg);

  int execute(const obrpc::ObAdminFlushCacheArg &arg);
private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminFlushCache);
};

#ifdef OB_BUILD_SPM
class ObAdminLoadBaseline : public ObTenantServerAdminUtil
{
public:
  explicit ObAdminLoadBaseline(const ObSystemAdminCtx &ctx)
    : ObTenantServerAdminUtil(ctx)
  {}
  virtual ~ObAdminLoadBaseline() {}

  int call_server(const common::ObAddr &server,
                  const obrpc::ObLoadPlanBaselineArg &arg);

  int execute(const obrpc::ObLoadPlanBaselineArg &arg);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminLoadBaseline);
};

class ObAdminLoadBaselineV2 : public ObTenantServerAdminUtil
{
public:
  explicit ObAdminLoadBaselineV2(const ObSystemAdminCtx &ctx)
    : ObTenantServerAdminUtil(ctx)
  {}
  virtual ~ObAdminLoadBaselineV2() {}

  int call_server(const common::ObAddr &server,
                  const obrpc::ObLoadPlanBaselineArg &arg,
                  obrpc::ObLoadBaselineRes &res);

  int execute(const obrpc::ObLoadPlanBaselineArg &arg, uint64_t &total_load_count);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminLoadBaselineV2);
};
#endif

class ObAdminSetTP : public ObAdminCallServer
{
public:
  explicit ObAdminSetTP(const ObSystemAdminCtx &ctx,
                        obrpc::ObAdminSetTPArg arg)
     : ObAdminCallServer(ctx),
       arg_(arg)
       {}
  virtual ~ObAdminSetTP() {}

  int execute(const obrpc::ObAdminSetTPArg &arg);
  virtual int call_server(const common::ObAddr &server);
private:
  obrpc::ObAdminSetTPArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminSetTP);
};

class ObAdminSyncRewriteRules : public ObTenantServerAdminUtil
{
public:
  explicit ObAdminSyncRewriteRules(const ObSystemAdminCtx &ctx)
    : ObTenantServerAdminUtil(ctx)
  {}
  virtual ~ObAdminSyncRewriteRules() {}

  int call_server(const common::ObAddr &server,
                  const obrpc::ObSyncRewriteRuleArg &arg);

  int execute(const obrpc::ObSyncRewriteRuleArg &arg);

private:
  DISALLOW_COPY_AND_ASSIGN(ObAdminSyncRewriteRules);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_SYSTEM_ADMIN_UTIL_H_
