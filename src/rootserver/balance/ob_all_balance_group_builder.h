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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_BALANCE_GROUP_BUILDER_H
#define OCEANBASE_ROOTSERVER_OB_ALL_BALANCE_GROUP_BUILDER_H

#include "lib/ob_define.h"                                  // ObObjectID
#include "lib/container/ob_array.h"                         //ObArray
#include "common/ob_tablet_id.h"                            // ObTabletID
#include "share/ob_ls_id.h"                                 // ObLSID
#include "share/schema/ob_multi_version_schema_service.h"   // share::schema
#include "share/schema/ob_schema_getter_guard.h"            // ObSchemaGetterGuard
#include "share/tablet/ob_tenant_tablet_to_ls_map.h"        // ObTenantTabletToLSMap

#include "ob_balance_group_define.h"                         //ObBalanceGroupID, ObBalanceGroup

namespace oceanbase
{
namespace rootserver
{
class ObPartitionHelper
{
public:
  class ObPartInfo {
  public:
    ObPartInfo() {}
    int init(common::ObTabletID tablet_id, common::ObObjectID part_id)
    {
      int ret = OB_SUCCESS;
      tablet_id_ = tablet_id;
      part_id_ = part_id;
      return ret;
    }
    common::ObTabletID get_tablet_id() { return tablet_id_; }
    common::ObObjectID get_part_id() { return part_id_; }
    TO_STRING_KV(K_(tablet_id), K_(part_id));
  private:
    common::ObTabletID tablet_id_;
    common::ObObjectID part_id_;
  };
  static int get_part_info(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      int64_t part_idx,
      ObPartInfo &part_info);
  static int get_sub_part_num(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      int64_t part_idx,
      int64_t &sub_part_num);
  static int get_sub_part_info(
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      int64_t part_idx,
      int64_t sub_part_idx,
      ObPartInfo &part_info);
  static int check_partition_option(
      const share::schema::ObSimpleTableSchemaV2 &t1,
      const share::schema::ObSimpleTableSchemaV2 &t2,
      bool is_subpart, bool &is_matched);
  static int check_partition_match(
      const share::schema::ObSimpleTableSchemaV2 &t1,
      const share::schema::ObSimpleTableSchemaV2 &t2,
      bool &match);
};

// Tenant all balance group builder
//
// You can use the builder to build all balance groups of tenant.
// It will iterate all partitions of user table/global index/tmp table to decide balance group info.
// You can register NewPartitionCallback to handle every partiitons, to get their balance group info.
//
// Partition belongs to one Partition Group. one or more Partition Groups forms a balance group.
// Tenant have multiple balance groups. Every table have one or more balance groups.
//
// USAGE:
// 1. define a type derived from ObAllBalanceGroupBuilder::NewPartitionCallback to handle new
//    partition info of every balance group
//
// 2. define ObAllBalanceGroupBuilder object
//    1) call prepare() function to prepare data for this build
//    2) call build() function to build all balance groups for all partitions.
class ObAllBalanceGroupBuilder final
{
public:
  ObAllBalanceGroupBuilder();
  ~ObAllBalanceGroupBuilder();

  class NewPartitionCallback;
  int init(const int64_t tenant_id,
      const char *mod,
      NewPartitionCallback &callback,
      common::ObMySQLProxy &sql_proxy,
      share::schema::ObMultiVersionSchemaService &schema_service);
  void destroy();

  // do prepare work
  // prepare data before build
  int prepare(bool need_load_tablet_size = false);

  // Iterator tenant all partitions, to build balance group info
  //
  // This function will call NewPartitionCallback to handle new partition
  int build();

public:
  class NewPartitionCallback
  {
  public:
    virtual ~NewPartitionCallback() {}

    // callback function callled when find new partition in one balance group
    // NOTE: partitions in same partition group will output successively.
    //       You can check 'in_new_partition_group' to find whether new partition group found
    //
    // @param [in]  bg                        balance group
    // @param [in]  table_schema              table schema of the table which this partition belongs to
    // @param [in]  part_object_id            partition object id: part id for one-level part table,
    //                                        subpart id for two-level part table
    // @param [in]  src_ls_id                 the LS that partition is current located
    // @param [in]  dest_ls_id                the LS that partition should be located
    // @param [in]  tablet_size               tablet data size
    // @param [in]  in_new_partition_group    is this partition in new partition group
    // @param [in]  part_group_uid            partition group unique id
    virtual int on_new_partition(
        const ObBalanceGroup &bg,
        const share::schema::ObSimpleTableSchemaV2 &table_schema,
        const common::ObObjectID part_object_id,
        const share::ObLSID &src_ls_id,
        const share::ObLSID &dest_ls_id,
        const int64_t tablet_size,
        const bool in_new_partition_group,
        const uint64_t part_group_uid) = 0;
  };

private:
  int prepare_tablet_to_ls_(
      const uint64_t tenant_id,
      common::ObMySQLProxy &sql_proxy);
  int do_build_();
  int get_table_schemas_in_tablegroup_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      common::ObArray<const share::schema::ObSimpleTableSchemaV2*> &table_schemas,
      int &max_part_level);
  int build_balance_group_for_tablegroup_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      const common::ObArray<const share::schema::ObSimpleTableSchemaV2 *> &table_schemas,
      const int max_part_level);
  int build_balance_group_for_table_not_in_tablegroup_(
      const share::schema::ObSimpleTableSchemaV2 &table_schema);
  int build_bg_for_tablegroup_sharding_none_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      const common::ObArray<const share::schema::ObSimpleTableSchemaV2*> &table_schemas,
      const int64_t max_part_level);
  int get_primary_schema_and_check_all_partition_matched_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      const common::ObArray<const share::schema::ObSimpleTableSchemaV2*> &table_schemas,
      const share::schema::ObSimpleTableSchemaV2* &primary_table_schema,
      const bool is_subpart);
  int build_bg_for_tablegroup_sharding_partition_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      const common::ObArray<const share::schema::ObSimpleTableSchemaV2*> &table_schemas,
      const int64_t max_part_level);
  int build_bg_for_tablegroup_sharding_subpart_(
      const share::schema::ObSimpleTablegroupSchema &tablegroup_schema,
      const common::ObArray<const share::schema::ObSimpleTableSchemaV2*> &table_schemas,
      const int max_part_level);
  int build_bg_for_partlevel_zero_(const share::schema::ObSimpleTableSchemaV2 &table_schema);
  int build_bg_for_partlevel_one_(const share::schema::ObSimpleTableSchemaV2 &table_schema);
  int build_bg_for_partlevel_two_(const share::schema::ObSimpleTableSchemaV2 &table_schema);
  int add_new_part_(
      const ObBalanceGroup &bg,
      const share::schema::ObSimpleTableSchemaV2 &table_schema,
      const common::ObObjectID part_object_id,
      const common::ObTabletID tablet_id,
      const uint64_t part_group_uid);
  int prepare_tablet_data_size_();
  int prepare_related_tablets_map_();
  int add_to_related_tablets_map_(
      const share::schema::ObSimpleTableSchemaV2 &primary_table_schema,
      const share::schema::ObSimpleTableSchemaV2 &related_table_schema);
  int get_data_size_with_related_tablets_(const ObTabletID &tablet_id, uint64_t &data_size);
  // defensive check
  int check_table_schemas_in_tablegroup_(
      const ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_dup_to_normal_dest_ls_id_(share::ObLSID &dest_ls_id);
  int add_part_to_bg_for_tablegroup_sharding_none_(
      const ObBalanceGroup &bg,
      const ObArray<const ObSimpleTableSchemaV2*> &table_schemas,
      bool &in_new_pg);
  int get_global_indexes_of_tables_(
      const ObArray<const ObSimpleTableSchemaV2 *> &table_schemas,
      ObIArray<const ObSimpleTableSchemaV2 *> &global_index_schemas);
private:
  static const int64_t MAP_BUCKET_NUM = 40960;
  static const int64_t SET_BUCKET_NUM = 1024;

  bool inited_;
  const char* mod_;
  uint64_t tenant_id_;
  share::ObLSID dup_ls_id_;
  // previous info is used for all_new_part_
  share::ObLSID pre_dest_ls_id_;
  share::ObLSID pre_dup_to_normal_dest_ls_id_;
  ObBalanceGroupID pre_bg_id_;
  int64_t pre_part_group_uid_;

  NewPartitionCallback *callback_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;

  share::schema::ObSchemaGetterGuard schema_guard_;
  share::ObTenantTabletToLSMap tablet_to_ls_;

  hash::ObHashMap<ObTabletID, uint64_t> tablet_data_size_;
  ObArenaAllocator allocator_;
  hash::ObHashMap<ObTabletID, common::ObIArray<ObTabletID> *> related_tablets_map_;
  hash::ObHashSet<uint64_t> sharding_none_tg_global_indexes_; // global indexes of the primary table in tablegroup sharding none
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_ALL_BALANCE_GROUP_BUILDER_H */
