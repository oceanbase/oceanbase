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

#ifndef _OB_PARTITION_GROUP_COORDINATOR_H
#define _OB_PARTITION_GROUP_COORDINATOR_H 1
#include "ob_balance_info.h"
#include "ob_root_utils.h"
namespace oceanbase {
namespace rootserver {
class ObRebalanceTaskMgr;
class ObLocalityChecker;
class TestPartitionGroupCoordinate;
class ObPartitionGroupCoordinator {
public:
  friend class TestPg_try_coordinate_primary_by_unit1_Test;
  friend class TestPg_try_coordinate_primary_by_unit2_Test;
  friend class TestPg_try_coordinate_primary_by_unit3_Test;
  friend class TestPg_coordinate_test1_Test;
  friend class TestPg_coordinate_test2_Test;
  friend class TestPg_coordinate_test3_Test;
  friend class TestPg_coordinate_test4_Test;
  friend class TestPg_coordinate_test5_Test;
  friend class TestPg_coordinate_test6_Test;
  friend class TestPg_coordinate_test7_Test;
  friend class TestPg_coordinate_test8_Test;
  friend class TestPg_coordinate_test9_Test;
  friend class TestPg_coordinate_test10_Test;
  ObPartitionGroupCoordinator();
  virtual ~ObPartitionGroupCoordinator()
  {}
  int init(
      ObRebalanceTaskMgr& task_mgr, TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider);
  // In each zone, replicas of the same partition group are migrated to the same unit
  int coordinate_pg_member(int64_t& task_cnt);

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupCoordinator);
  int prepare();
  // Check that only logonly replical can be stored on the logonly unit;
  int try_coordinate_primary_by_unit(
      const Partition& primary_partition, bool& primary_partition_migrated, int64_t& task_count);

  // function members
  int coordinate_partition_member(
      const Partition& primary_partition, Partition& partition, bool& primary_partition_migrated, int64_t& task_cnt);
  // When detecting the machine block where the primary_partition is located,
  // the replica of ppr needs to be migrated to the location where the partition is located;
  int migrate_primary_partition(const Partition& primary_partition, const Partition& partition,
      bool& primary_partition_migrated, int64_t& task_cnt);

  // When relocating ppr,
  // select a suitable location from the location of the partition as the destination;
  int get_migrate_dest(
      const Partition& primary_partition, const Replica& ppr, const Partition& partition, Replica& dest);
  virtual int get_random_dest(const Partition& primary_partition, const Replica& ppr, Replica& dest);
  // There are two steps to coordinate. The first step is to align the replica of the paxos type.
  int coordinate_paxos_replica(const Partition& primary_partition, const Partition& partition, int64_t& task_count,
      bool& skip_coordinate_paxos_replica);
  // If there are two replicas of paxos in a zone, special treatment is required
  int coordinate_specific_paxos_replica(const Partition& primary_partition, const Partition& partition,
      int64_t& task_count, bool& skip_coordinate_paxos_replica);
  // Handle scenarios where there is only one replica of paxos in a zone
  int coordinate_normal_paxos_replica(const Partition& primary_partition, const Partition& partition,
      int64_t& task_count, bool& skip_coordinate_paxos_replica);

  // The second step of coordinate is to align the replica of the non-paxos type
  int coordinate_non_paxos_replica(const Partition& primary_partition, Partition& partition, int64_t& task_cnt);
  // When coordinate is a replica of non-paxos type,
  // there are two types:
  //(1) The position information of the replica has been aligned
  int process_non_paxos_replica_in_same_zone(const Partition& primary_partition, Partition& partition,
      ObIArray<Replica*>& ppr_coordinated_replica, ObIArray<Replica*>& pr_coordinated_replica, int64_t& task_count);
  //(2) replica location information is not aligned
  int process_non_paxos_replica_in_same_addr(const Partition& primary_partition, Partition& partition,
      ObIArray<Replica*>& ppr_coordinated_replica, ObIArray<Replica*>& pr_coordinated_replica, int64_t& task_count);

  // partition: the partition being processed
  // to_migrate_replica: The replica that needs to be migrated;
  // to_delete_replica: the replica to be deleted;
  // ppr: The position of the alignment target in the primary partition, which is also migrate_dest
  virtual int try_remove_and_migrate(const Partition& partition, const Replica& to_migrate_replica,
      const Replica& to_delete_replica, const Replica& migrate_dest_in_ppr, bool& can_add_task);

  // When processing the alignment operation of the paxos type replica,
  // there is already a non-paxos replica on the machine where the ppr is located
  // It is not possible to directly migrate the paxos replica of the partition back,
  // need to delete the non-paxos replica on the ppr first, and then do the migration.
  // partition: the partition being processed
  // replica: the target that needs to be migrated
  // ppr: The target address that needs to be deleted; it is also the target end of the migration
  virtual int do_remove_and_migrate(
      const Partition& partition, const Replica& replica, const Replica& ppr, bool& can_add_task);
  // Delete the specified replica of the partition
  virtual int do_remove_replica(const Partition& partition, const Replica& replica);
  // Migrate the specified replica of the partition to the machine
  // and unit where dest_replica is located
  virtual int do_migrate_replica(
      const Partition& partition, const Replica& replica, const Replica& dest_replica, bool& can_add_task);
  bool can_do_coordinate(const Partition& primary_partition);

  // return OB_CANCELED if stop, else return OB_SUCCESS
  int check_stop() const
  {
    return check_stop_provider_->check_stop();
  }

  int normal_pg_member_coordinate(
      common::ObArray<Partition*>::iterator& p, Partition& primary_partition, int64_t& task_cnt);

private:
  // data members
  bool inited_;
  ObRebalanceTaskMgr* task_mgr_;
  TenantBalanceStat* tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
  common::ObArray<common::ObZone> two_paxos_zones_;
  // Record the zone where there are copies of both F and L in this tenant
  common::ObArray<UnitStat*> logonly_units_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_PARTITION_GROUP_COORDINATOR_H */
