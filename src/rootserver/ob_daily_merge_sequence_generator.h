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

#ifndef OCEANBASE_ROOTSERVER_OB_DAILY_MERGE_SEQUENCE_GENERATOR
#define OCEANBASE_ROOTSERVER_OB_DAILY_MERGE_SEQUENCE_GENERATOR

#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_bit_set.h"
#include "share/ob_zone_info.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_zone_type.h"
#include "ob_zone_manager.h"
#include "ob_server_manager.h"
namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObDailyMergeSequenceGenerator {
public:
  friend class TestDailyMergeSequenceGenerator_get_merge_unit_index_Test;
  friend class TestDailyMergeSequenceGenerator_build_merge_unit_Test;
  friend class TestDailyMergeSequenceGenerator_calc_distribution_Test;
  friend class TestDailyMergeSequenceGenerator_calc_partition_distribution_Test;
  friend class TestDailyMergeSequenceGenerator_generate_conflict_pairs_Test;
  friend class TestDailyMergeSequenceGenerator_can_switch_to_leader_Test;
  friend class TestDailyMergeSequenceGenerator_can_start_merge_Test;
  friend class TestDailyMergeSequenceGenerator_choose_candidate_Test;
  friend class TestDailyMergeSequenceGenerator_check_conflict_pair_Test;
  friend class TestDailyMergeSequenceGenerator_get_next_zone_by_conflict_pair_Test;
  const static int64_t DEFAULT_MERGE_UNIT_COUNT = 5;
  typedef common::ObBitSet<common::MAX_ZONE_NUM> ObPartitionDistribution;
  typedef common::ObArray<ObPartitionDistribution> ObPartitionsDistribution;
  typedef common::ObArray<common::ObAddr> ObServerArray;
  struct ObMergeUnit {
    common::ObZone zone_;
    common::ObRegion region_;
    ObServerArray servers_;
    int64_t leader_cnt_;
    int64_t replica_cnt_;
    common::ObZoneType type_;
    TO_STRING_KV(K_(zone), K_(region), K_(servers), K_(leader_cnt), K_(type), K_(replica_cnt));
    ObMergeUnit() : zone_(), region_(), servers_(), leader_cnt_(0), replica_cnt_(0), type_(common::ZONE_TYPE_INVALID)
    {}
    ~ObMergeUnit()
    {}
  };

  struct ObConflictPair {
    common::ObZone first;
    common::ObZone second;
    int64_t same_partition_cnt_;
    ObConflictPair() : first(), second(), same_partition_cnt_(0)
    {}
    ~ObConflictPair()
    {}
    TO_STRING_KV(K(first), K(second), K(same_partition_cnt_));
  };

  typedef common::ObSEArray<ObMergeUnit, DEFAULT_MERGE_UNIT_COUNT> ObMergeUnitArray;
  typedef common::ObSEArray<ObConflictPair, DEFAULT_MERGE_UNIT_COUNT> ObConflictPairs;
  class ObMergeUnitGenerator {
  public:
    ObMergeUnitGenerator() : inited_(false), zone_mgr_(NULL), server_manager_(NULL)
    {}
    ~ObMergeUnitGenerator()
    {}
    void init(ObZoneManager& zone_manager, ObServerManager& server_manager);
    int build_merge_unit(ObMergeUnitArray& merge_units);

  private:
    bool inited_;
    int build_merge_unit_by_zone(ObMergeUnitArray& merge_units);
    ObZoneManager* zone_mgr_;
    ObServerManager* server_manager_;
  };
  class ObConflictPairPriorityCmp {
  public:
    ObConflictPairPriorityCmp(ObDailyMergeSequenceGenerator& generator) : generator_(generator)
    {}
    ~ObConflictPairPriorityCmp()
    {}
    bool operator()(const ObConflictPair& first, const ObConflictPair& second);

  private:
    ObDailyMergeSequenceGenerator& generator_;
  };
  class ObMergePriorityCmp {
  public:
    ObMergePriorityCmp(ObZoneManager* zone_mgr) : zone_mgr_(zone_mgr)
    {}
    ~ObMergePriorityCmp()
    {}
    bool operator()(const ObConflictPair& first, const ObConflictPair& second);

  private:
    ObZoneManager* zone_mgr_;
  };

  ObDailyMergeSequenceGenerator();
  virtual ~ObDailyMergeSequenceGenerator();
  void init(ObZoneManager& zone_manager, ObServerManager& server_manager, share::ObPartitionTableOperator& pt,
      share::schema::ObMultiVersionSchemaService& schema_service);
  int get_next_zone(bool merge_by_turn, const int64_t concurrency_count, common::ObIArray<common::ObZone>& zones);

private:
  int get_next_zone_by_turn(common::ObIArray<common::ObZone>& zones);
  int get_next_zone_no_turn(common::ObIArray<common::ObZone>& zones);
  int get_next_zone_by_priority(common::ObIArray<common::ObZone>& to_merge);
  int get_next_zone_by_conflict_pair(common::ObIArray<common::ObZone>& to_merge);
  int get_next_zone_by_concurrency_count(common::ObIArray<common::ObZone>& to_merge);
  int add_in_merging_zone(common::ObIArray<common::ObZone>& to_merge);
  int filter_merging_zone(common::ObIArray<common::ObZone>& to_merge);
  void reset();
  int ensure_all_zone_exist();
  bool can_merge_concurrently_in_region();
  int get_merge_unit_index(const common::ObAddr& server, int64_t& index);
  int get_merge_unit_index(const common::ObZone& zone, int64_t& index);
  int can_switch_to_leader(
      const common::ObIArray<common::ObZone>& to_merge, const common::ObZone& zone, bool& can_switch);
  int can_start_merge(const common::ObIArray<common::ObZone>& to_merge, const common::ObZone& zone, bool& can_start);

  bool exist_in_conflict_pair(const ObZone& zone);
  bool is_need_merge(const common::ObZone& zone);
  int rebuild();
  int generate_conflict_pairs();
  int generate_conflict_pair(const int64_t merge_unit_index, const ObPartitionsDistribution& distribution);
  int calc_distributions();
  int calc_partition_distribution(const share::ObPartitionInfo& partition_info, const common::ObZoneType& zone_type,
      const common::ObReplicaType& type, ObPartitionDistribution& distribution);

  int add_conflict_pair(const ObZone& first, const ObZone& second);
  int get_leader_count(const ObZone& zone, int64_t& leader_count);

private:
  ObZoneManager* zone_mgr_;
  ObServerManager* server_manager_;
  share::ObPartitionTableOperator* pt_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObPartitionsDistribution full_partitions_distribution_;
  ObPartitionsDistribution readonly_partitions_distribution_;
  ObMergeUnitArray merge_units_;
  ObConflictPairs conflict_pairs_;
  int64_t concurrency_count_;
  DISALLOW_COPY_AND_ASSIGN(ObDailyMergeSequenceGenerator);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif
