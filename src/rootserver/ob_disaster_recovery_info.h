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

#ifndef OCEANBASE_ROOTSERVICE_DISASTER_RECOVERY_INFO_H_
#define OCEANBASE_ROOTSERVICE_DISASTER_RECOVERY_INFO_H_ 1
#include "share/unit/ob_unit_info.h"
#include "common/ob_member_list.h"
#include "lib/hash/ob_refered_map.h"
#include "share/ob_define.h"
#include "share/ob_replica_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ls/ob_ls_info.h"
#include "share/ls/ob_ls_status_operator.h"
namespace oceanbase
{

namespace share
{
class ObLSID;
class ObLSReplica;
namespace schema
{
class ObMultiVersionSchemaService;
}
};

namespace rootserver
{

class ObUnitManager;
class ObZoneManager;

struct DRServerStatInfo
{
public:
  DRServerStatInfo() : server_(),
                       alive_(false),
                       active_(false),
                       permanent_offline_(true),
                       block_(false),
                       stopped_(true) {}
public:
  // interface for ReferedMap::Item
  void set_key(const common::ObAddr &server) { server_ = server; }
  const common::ObAddr &get_key() const { return server_; }

  int assign(
      const DRServerStatInfo &that);

  int init(
      const common::ObAddr &server,
      const bool alive,
      const bool active,
      const bool permanent_offline,
      const bool block,
      const bool stopped);

  TO_STRING_KV(K_(server),
               K_(alive),
               K_(active),
               K_(permanent_offline),
               K_(block),
               K_(stopped));
public:
  const common::ObAddr &get_server() const { return server_; }
  bool is_alive() const { return alive_; }
  bool is_active() const { return active_; }
  bool is_permanent_offline() const { return permanent_offline_; }
  bool is_block() const { return block_; }
  bool is_stopped() const { return stopped_; }
private:
  common::ObAddr server_;
  bool alive_;
  bool active_;
  bool permanent_offline_;
  bool block_;
  bool stopped_;
};

struct DRUnitStatInfo
{
public:
  DRUnitStatInfo() : unit_id_(common::OB_INVALID_ID),
                     in_pool_(false),
                     unit_(),
                     server_stat_(nullptr),
                     outside_replica_cnt_(0) {}
public:
  // interface for ReferedMap::Item
  void set_key(const uint64_t unit_id) { unit_id_ = unit_id; }
  const uint64_t &get_key() const { return unit_id_; }
  
  TO_STRING_KV(K_(unit_id),
               K_(in_pool),
               K_(unit),
               KPC_(server_stat));
  
  int assign(
      const DRUnitStatInfo &that);
  
  int init(
      const uint64_t unit_id,
      const bool in_pool,
      const share::ObUnit &unit,
      DRServerStatInfo *server_stat,
      const int64_t outside_replica_cnt);
public:
  uint64_t get_unit_id() const { return unit_id_; }
  bool is_in_pool() const { return in_pool_; }
  const share::ObUnit &get_unit() const { return unit_; }
  const DRServerStatInfo *get_server_stat() const { return server_stat_; }
  int64_t get_outside_replica_cnt() const { return outside_replica_cnt_; }
  void inc_outside_replica_cnt() { ++outside_replica_cnt_; }
private:
  uint64_t unit_id_;
  bool in_pool_;
  share::ObUnit unit_;
  DRServerStatInfo *server_stat_;
  int64_t outside_replica_cnt_;
};

typedef common::hash::ObReferedMap<uint64_t,
                                   DRUnitStatInfo> UnitStatInfoMap;

typedef common::hash::ObReferedMap<common::ObAddr,
                                   DRServerStatInfo> ServerStatInfoMap;


class DRLSInfo
{
public:
  DRLSInfo(const uint64_t resource_tenant_id,
           ObZoneManager *zone_mgr,
           share::schema::ObMultiVersionSchemaService *schema_service)
    : resource_tenant_id_(resource_tenant_id),
      sys_schema_guard_(),
      zone_mgr_(zone_mgr),
      schema_service_(schema_service),
      unit_stat_info_map_("DRUnitStatMap"),
      server_stat_info_map_("DRSerStatMap"),
      zone_locality_array_(),
      inner_ls_info_(),
      ls_status_info_(),
      server_stat_array_(),
      unit_stat_array_(),
      unit_in_group_stat_array_(),
      schema_replica_cnt_(0),
      schema_full_replica_cnt_(0),
      member_list_cnt_(0),
      paxos_replica_number_(0),
      has_leader_(false),
      inited_(false) {}
  virtual ~DRLSInfo() {}
public:
  // use user_tenant_id to init unit and locality
  int init();
  int build_disaster_ls_info(
      const share::ObLSInfo &ls_info,
      const share::ObLSStatusInfo &ls_status_info,
      const bool &filter_readonly_replicas_with_flag);
public:
  const common::ObIArray<share::ObZoneReplicaAttrSet> &get_locality() const {
    return zone_locality_array_;
  } 
  const UnitStatInfoMap &get_unit_stat_info_map() const {
    return unit_stat_info_map_;
  }
  const ServerStatInfoMap &get_server_stat_info_map() const {
    return server_stat_info_map_;
  }
  int64_t get_schema_replica_cnt() const { return schema_replica_cnt_; }
  int64_t get_schema_full_replica_cnt() const { return schema_full_replica_cnt_; }
  int64_t get_member_list_cnt() const { return member_list_cnt_; }
  int64_t get_paxos_replica_number() const { return paxos_replica_number_; }
  bool has_leader() const { return has_leader_; }
  bool is_duplicate_ls() const { return ls_status_info_.is_duplicate_ls(); }
  int get_tenant_id(
      uint64_t &tenant_id) const;
  int get_ls_id(
      uint64_t &tenant_id,
      share::ObLSID &ls_id) const;
  int get_replica_cnt(
      int64_t &replica_cnt) const;
  int get_replica_stat(
      const int64_t index,
      share::ObLSReplica *&ls_replica,
      DRServerStatInfo *&server_stat_info,
      DRUnitStatInfo *&unit_stat_info,
      DRUnitStatInfo *&unit_in_group_stat_info);
  int get_ls_status_info(
      const share::ObLSStatusInfo *&ls_status_info);
  int get_inner_ls_info(share::ObLSInfo &inner_ls_info) const;
  int get_leader(
      common::ObAddr &leader_addr) const;
  int get_leader_and_member_list(
      common::ObAddr &leader_addr,
      common::ObMemberList &member_list,
      GlobalLearnerList &learner_list);

  // get data_source from leader replcia
  // @param [out] data_source, leader replica
  // @param [out] data_size, leader replica data_size
  int get_default_data_source(
      ObReplicaMember &data_source,
      int64_t &data_size) const;
private:
  int construct_filtered_ls_info_to_use_(
      const share::ObLSInfo &input_ls_info,
      share::ObLSInfo &output_ls_info,
      const bool &filter_readonly_replicas_with_flag);
  // init related private func
  int gather_server_unit_stat();
  int fill_servers();
  int fill_units();
  // dr_ls related
  void reset_last_disaster_recovery_ls();
  int append_replica_server_unit_stat(
      DRServerStatInfo *server_stat_info,
      DRUnitStatInfo *unit_stat_info,
      DRUnitStatInfo *unit_in_group_stat_info);
public:
  TO_STRING_KV(K(resource_tenant_id_),
               K(zone_locality_array_),
               K(inner_ls_info_),
               K(ls_status_info_),
               K(server_stat_array_),
               K(unit_stat_array_),
               K(unit_in_group_stat_array_),
               K(schema_replica_cnt_),
               K(schema_full_replica_cnt_),
               K(member_list_cnt_),
               K(paxos_replica_number_),
               K(has_leader_));
private:
  const int64_t UNIT_MAP_BUCKET_NUM = 500000;
  const int64_t SERVER_MAP_BUCKET_NUM = 5000;
private:
  uint64_t resource_tenant_id_;
  share::schema::ObSchemaGetterGuard sys_schema_guard_;
  share::ObUnitTableOperator unit_operator_;
  ObZoneManager *zone_mgr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  UnitStatInfoMap unit_stat_info_map_;
  ServerStatInfoMap server_stat_info_map_;
  common::ObArray<share::ObZoneReplicaAttrSet> zone_locality_array_;
  share::ObLSInfo inner_ls_info_;
  share::ObLSStatusInfo ls_status_info_;
  common::ObArray<DRServerStatInfo *> server_stat_array_;
  common::ObArray<DRUnitStatInfo *> unit_stat_array_;
  common::ObArray<DRUnitStatInfo *> unit_in_group_stat_array_;
  int64_t schema_replica_cnt_;
  int64_t schema_full_replica_cnt_;
  int64_t member_list_cnt_;
  int64_t paxos_replica_number_;
  bool has_leader_;
  bool inited_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVICE_DISASTER_RECOVERY_INFO_H_ */
