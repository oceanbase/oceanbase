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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_TABLE_UTIL_H_
#define OCEANBASE_ROOTSERVER_OB_PARTITION_TABLE_UTIL_H_

#include <utility>
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "common/ob_zone.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {

class ObZoneManager;
class ObServerManager;

class ObPartitionTableUtil {
public:
  struct ObZoneLeaderInfo {
    ObZoneLeaderInfo() : zone_(), leader_count_(0)
    {}
    TO_STRING_KV(K_(zone), K_(leader_count));
    common::ObZone zone_;
    int64_t leader_count_;
  };

  typedef common::ObSEArray<ObPartitionTableUtil::ObZoneLeaderInfo, common::MAX_ZONE_NUM> ObLeaderInfoArray;

  struct MergeProgress {
    common::ObZone zone_;

    int64_t unmerged_partition_cnt_;
    int64_t unmerged_data_size_;

    int64_t merged_partition_cnt_;
    int64_t merged_data_size_;

    int64_t smallest_data_version_;

    bool operator<(const MergeProgress& o) const
    {
      return zone_ < o.zone_;
    }
    bool operator<(const common::ObZone& zone) const
    {
      return zone_ < zone;
    }

    int64_t get_merged_partition_percentage() const;
    int64_t get_merged_data_percentage() const;

    MergeProgress()
        : zone_(),
          unmerged_partition_cnt_(0),
          unmerged_data_size_(0),
          merged_partition_cnt_(0),
          merged_data_size_(0),
          smallest_data_version_(0)
    {}
    ~MergeProgress()
    {}

    TO_STRING_KV(K_(zone), K_(unmerged_partition_cnt), K_(unmerged_data_size), K_(merged_partition_cnt),
        K_(merged_data_size), K_(smallest_data_version), "merged_partition_percentage",
        get_merged_partition_percentage(), "merged_data_percentage", get_merged_data_percentage());
  };

  typedef common::ObSEArray<MergeProgress, common::MAX_ZONE_NUM> ObZoneMergeProgress;

  ObPartitionTableUtil();
  virtual ~ObPartitionTableUtil();

  int init(ObZoneManager& zone_mgr, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObPartitionTableOperator& pt, ObServerManager& server_mgr, common::ObMySQLProxy& sql_proxy);

  virtual int check_merge_progress(
      const volatile bool& stop, const int64_t version, ObZoneMergeProgress& all_progress, bool& all_majority_merged);

  // set leader backup flag (used for backup and restore leader pos in daily merge)
  // virtual int set_leader_backup_flag(const volatile bool &stop,
  //    const bool flag,
  //    ObLeaderInfoArray *leader_info_array);

private:
  template <typename SCHEMA>
  int get_associated_replica_num(share::schema::ObSchemaGetterGuard& schema_guard, const SCHEMA& schema,
      int64_t& paxos_replica_num, int64_t& full_replica_num, int64_t& all_replica_num, int64_t& majority);

private:
  bool inited_;
  ObZoneManager* zone_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_;
  ObServerManager* server_mgr_;
  common::ObMySQLProxy* sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionTableUtil);
};

template <typename SCHEMA>
int ObPartitionTableUtil::get_associated_replica_num(share::schema::ObSchemaGetterGuard& schema_guard,
    const SCHEMA& schema, int64_t& paxos_replica_num, int64_t& full_replica_num, int64_t& all_replica_num,
    int64_t& majority)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
    RS_LOG(WARN, "fail to get table paxos replica num", K(ret));
  } else if (OB_FAIL(schema.get_full_replica_num(schema_guard, full_replica_num))) {
    RS_LOG(WARN, "fail to get full replica num", K(ret));
  } else if (OB_FAIL(schema.get_all_replica_num(schema_guard, all_replica_num))) {
    RS_LOG(WARN, "fail to get all replica num", K(ret));
  } else if (OB_UNLIKELY(paxos_replica_num <= 0) || OB_UNLIKELY(full_replica_num <= 0) ||
             OB_UNLIKELY(all_replica_num <= 0)) {  // no index and virtual table, greater than 0
    ret = common::OB_ERR_UNEXPECTED;
    RS_LOG(WARN, "paxos replica num error", K(ret), K(paxos_replica_num), K(full_replica_num), K(all_replica_num));
  } else {
    majority = rootserver::majority(paxos_replica_num);
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_PARTITION_TABLE_UTIL_H_
