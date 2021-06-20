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

#ifndef OCEANBASE_ROOTSERVER_OB_SINGLE_PARTITION_BALANCE_H_
#define OCEANBASE_ROOTSERVER_OB_SINGLE_PARTITION_BALANCE_H_

#include "share/ob_unit_replica_counter.h"
#include "common/ob_unit_info.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
class ObIPartitionTableIterator;
class ObPartitionReplica;
namespace schema {
class ObTenantSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObSchemaMgr;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObUnitManager;
class ObZoneManager;

class ObSinglePartBalance {
  // Outer calls need lock protection
public:
  ObSinglePartBalance();
  virtual ~ObSinglePartBalance()
  {
    destory_tenant_unit_array();
  }
  int init(ObUnitManager& unit_mgr, ObZoneManager& zone_mgr, share::ObPartitionTableOperator& pt_operator);
  int create_unit_replica_counter(const uint64_t tenant_id);
  int refresh_unit_replica_counter(const uint64_t tenant_id, const int64_t start,
      const common::ObIArray<share::ObUnitInfo>& unit_infos, share::schema::ObSchemaGetterGuard& schema_guard,
      common::ObIArray<share::TenantUnitRepCnt>& tmp_ten_unit_reps);
  int get_tenant_unit_array(common::ObIArray<share::TenantUnitRepCnt*>& ten_unit_arr, const uint64_t tenant_id);
  int set_tenant_unit_replica_capacity(const uint64_t tenant_id, const int64_t start,
      const common::ObIArray<share::ObUnitInfo>& unit_infos,
      const common::ObIArray<share::TenantUnitRepCnt>& tmp_ten_unit_reps);
  int destory_tenant_unit_array();
  int update_tenant_unit_replica_capacity(
      const uint64_t tenant_id, const common::ObIArray<share::TenantUnitRepCnt*>& tmp_ten_unit_reps);

private:
  inline static bool compare_with_tenant_id(
      const share::TenantUnitRepCnt* lhs, const share::TenantUnitRepCnt& ten_unit_rep);
  inline static bool compare_with_tenant_id_up(
      const share::TenantUnitRepCnt& ten_unit_rep, const share::TenantUnitRepCnt* lhs);
  inline static bool compare_with_unit_id(const uint64_t lhs, const uint64_t unit_id);
  int check_inner_stat() const;
  int prepare_replica_capacity(const uint64_t tenant_id, share::schema::ObSchemaGetterGuard& schema_guard,
      int64_t& index_num, common::ObIArray<uint64_t>& non_partition_table);
  int set_unit_replica_capacity(const share::ObPartitionReplica& r, share::TenantUnitRepCnt& tenant_unit_rep_cnt,
      const int64_t index_num, const int64_t now_time, const int64_t non_table_cnt);
  int copy_ten_unit_rep_data(common::ObIArray<share::TenantUnitRepCnt>& update_ten_unit_reps,
      share::TenantUnitRepCntVec::iterator& iter_upper);
  int update_ten_unit_rep(const common::ObIArray<share::ObUnitInfo>& unit_infos, const uint64_t tenant_id);
  int push_back_other_tenant_unit_rep(const common::ObIArray<share::TenantUnitRepCnt>& update_ten_unit_reps);
  int remove_tenant_unit_rep(share::TenantUnitRepCntVec::iterator& iter_lower);
  int sort_and_push_back_unit_ids(
      const common::ObIArray<share::ObUnitInfo>& unit_infos, common::ObArray<uint64_t>& unit_ids);
  int create_tenant_unit_rep(const common::ObIArray<uint64_t>& unit_ids, const uint64_t tenant_id);

private:
  bool inited_;
  ObUnitManager* unit_mgr_;
  ObZoneManager* zone_mgr_;
  share::ObPartitionTableOperator* pt_operator_;
  share::TenantUnitRepCntVec tenant_unit_rep_cnt_vec_;
  common::ObMalloc allocator_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_SINGLE_PARTITION_BALANCE_H_
