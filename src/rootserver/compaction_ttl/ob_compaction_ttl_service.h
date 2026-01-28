//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_ROOTSERVER_COMPACTION_TTL_SERVICE_H_
#define OB_ROOTSERVER_COMPACTION_TTL_SERVICE_H_
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "share/compaction/ob_sync_mds_service.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/table/ob_ttl_util.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace rootserver
{

struct ObTTLFilterInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObTTLFilterInfoArg()
    : version_(TTL_FILTER_INFO_ARG_VERSION_V1),
      reserved_(0),
      ls_id_(),
      tablet_id_array_(),
      ttl_filter_info_()
  {}
  ~ObTTLFilterInfoArg() { destroy(); }
  void destroy() {
    ls_id_.reset();
    tablet_id_array_.reset();
    ttl_filter_info_.destroy();
  }
  bool is_valid() const {
    return TTL_FILTER_INFO_ARG_VERSION_V1 == version_
      && ls_id_.is_valid() && tablet_id_array_.count() > 0 && ttl_filter_info_.is_valid();
  }
  TO_STRING_KV(K_(version), K_(ls_id), "tablet_cnt", tablet_id_array_.count(), K_(tablet_id_array), K_(ttl_filter_info));
  static const int64_t TTL_FILTER_INFO_ARG_VERSION_V1 = 1;
  static const int32_t TIA_ONE_BYTE = 8;
  static const int32_t TIA_RESERVED_BITS = 56;
  union {
    uint64_t info_;
    struct
    {
      uint64_t version_     : TIA_ONE_BYTE;
      uint64_t reserved_    : TIA_RESERVED_BITS;
    };
  };
  share::ObLSID ls_id_;
  ObSEArray<common::ObTabletID, 8> tablet_id_array_;
  storage::ObTTLFilterInfo ttl_filter_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLFilterInfoArg);
};

struct ObTTLFilterInfoHelper
{
public:
  static int generate_ttl_filter_info(
    const share::schema::ObTableSchema &data_table_schema,
    ObTTLFilterInfo &ttl_filter_info);
};

class ObCompactionTTLService
{
public:
  ObCompactionTTLService(const uint64_t tenant_id, const uint64_t data_table_id)
    : allocator_("TTLService"),
      tenant_id_(tenant_id),
      data_table_id_(data_table_id),
      tablet_ls_pair_array_(),
      ttl_filter_arg_(),
      max_ddl_create_snapshot_(0),
      sync_mds_cost_us_(0)
  {}
  ~ObCompactionTTLService() = default;
  int execute(
    const int64_t tenant_task_start_us,
    common::ObMySQLProxy &sql_proxy,
    transaction::ObTransID &tx_id,
    common::ObTTLTaskStatus &task_status);
private:
  int get_latest_schema_guard(
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema *&latest_data_table_schema);
  int loop_index_table(
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &latest_data_table_schema);
  int get_schema_and_sync_mds(
    common::ObMySQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t table_id);
  int lock_table_and_sync_mds(
    common::ObMySQLTransaction &trans,
    const share::schema::ObTableSchema &schema);
  int loop_to_register_mds(common::ObMySQLTransaction &trans);
  int batch_get_tablet_info_before_trans(
    common::ObMySQLProxy &sql_proxy,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObTableSchema &schema,
    int64_t &tablet_cnt);
  int get_tablet_ls_info(
    common::ObISQLClient &sql_proxy,
    const share::schema::ObTableSchema &schema,
    ObIArray<share::ObTabletLSPair> &tablet_ls_pair_array);
private:
#ifdef ERRSIM
  static const int64_t TTL_SYNC_WAIT_DDL_LOCK_INTERVAL_US = 1000L * 1000L * 10L; // 10s in us
#else
  static const int64_t TTL_SYNC_WAIT_DDL_LOCK_INTERVAL_US = 1000L * 1000L * 60L * 30L; // 1hour in us
#endif
  static const int64_t TTL_SYNC_WAIT_DDL_CREATE_SNAPSHOT_INTERVAL_NS = 1000L * 1000L * 1000L * 60L * 60L; // 1hour in ns
  static const int64_t TIMEOUT_PER_PARTITION_US = 30 * 1000L; // 30ms
  static const int64_t TRANS_TIMEOUT_US = 3 * 1000L * 1000L; // 3s
  ObArenaAllocator allocator_;
  const uint64_t tenant_id_;
  const uint64_t data_table_id_;
  ObSEArray<share::ObTabletLSPair, 8> tablet_ls_pair_array_;
  ObTTLFilterInfoArg ttl_filter_arg_;
  int64_t max_ddl_create_snapshot_;
  int64_t sync_mds_cost_us_;
  DISALLOW_COPY_AND_ASSIGN(ObCompactionTTLService);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OB_ROOTSERVER_COMPACTION_TTL_SERVICE_H_
