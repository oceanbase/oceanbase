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

#ifndef LOGSERVICE_COORDINATOR_TABLE_ACCESSOR_H
#define LOGSERVICE_COORDINATOR_TABLE_ACCESSOR_H
#include "lib/container/ob_iarray.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string_holder.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"
#include "share/ob_table_access_helper.h"
#include "common_define.h"
#include <cstring>
#include "ob_leader_coordinator.h"
namespace oceanbase
{
namespace unittest
{
class TestElectionPriority;
}
namespace logservice
{
namespace coordinator
{

enum class InsertElectionBlacklistReason
{
  SWITCH_REPLICA = 1,
  MIGRATE = 2,
};

inline const char *to_cstring(InsertElectionBlacklistReason reason)
{
  switch (reason) {
  case InsertElectionBlacklistReason::SWITCH_REPLICA:
    return "SWITCH REPLICA";
  case InsertElectionBlacklistReason::MIGRATE:
    return "MIGRATE";
  default:
    return "unknown reason";
  }
}

class LsElectionReferenceInfoRow
{
  friend class unittest::TestElectionPriority;
  typedef ObTuple<int64_t/*tenant_id*/,
                int64_t/*ls_id*/,
                ObStringHolder/*zone_priority*/,
                ObStringHolder/*manual_leader_server*/,
                ObStringHolder/*remove_member_info*/> LsElectionReferenceInfoRowTypeForTable;
  typedef ObTuple<uint64_t/*tenant_id*/,
                  share::ObLSID/*ls_id*/,
                  ObArray<ObArray<ObStringHolder>>/*zone_priority*/,
                  common::ObAddr/*manual_leader_server*/,
                  ObArray<ObTuple<ObAddr/*removed_server*/,
                                  ObStringHolder/*removed_reason*/>>/*remove_member_info*/> LsElectionReferenceInfoRowTypeForUser;
public:
  /**
   * @description: 创建出一个数据结构映射自__all_ls_election_reference_info中的某一行，由租户ID和日志流ID确定，通过该数据结构的接口可以修改__all_ls_election_reference_info中对应行的内容
   * @param {uint64_t} tenant_id 租户ID
   * @param {ObLSID &} ls_id 日志流ID
   * @Date: 2022-01-29 16:45:33
   */
  LsElectionReferenceInfoRow(const uint64_t tenant_id, const share::ObLSID &ls_id);
  ~LsElectionReferenceInfoRow();
  /**
   * @description: 当且仅当对应的行在__all_ls_election_reference_info表存在时，修改zone_priority列的内容
   * @param {ObArray<ObArray<ObStringHolder>>} &zone_list_list 选举参考的zone优先级，表示为二维数组，形如{{z1,z2},{z3},{z4,z5}}：z1和z2具有最高优先级，z3次之，z4和z5最低
   * @return {int} 错误码
   * @Date: 2022-01-29 16:41:02
   */
  int change_zone_priority(const ObArray<ObArray<ObStringHolder>> &zone_list_list);
  /**
   * @description: 当且仅当对应的行在__all_ls_election_reference_info表存在时，修改manual_leader_server列的内容
   * @param {ObAddr} &manual_leader_server 指定的server地址
   * @return {int} 错误码
   * @Date: 2022-01-29 16:41:53
   */
  int change_manual_leader(const common::ObAddr &manual_leader_server);
  /**
   * @description: 当且仅当对应的行在__all_ls_election_reference_info表存在时，且要移除的server在removed_member_info中不存在时，在removed_member_info列中增加被删除的server及原因
   * @param {ObAddr} &server 不允许当leader的server
   * @param {ObString} &reason 不允许当leader的原因
   * @return {*}
   * @Date: 2022-01-29 16:43:19
   */
  int add_server_to_blacklist(const common::ObAddr &server, InsertElectionBlacklistReason reason);
  /**
   * @description: 当且仅当对应的行在__all_ls_election_reference_info表存在时，且要移除的server在removed_member_info中存在时，在removed_member_info列中移除被删除的server
   * @param {ObAddr} &server 已经被记录的不允许当leader的server
   * @return {*}
   * @Date: 2022-01-29 16:44:51
   */
  int delete_server_from_blacklist(const common::ObAddr &server);
  /**
   * @description: 将该原因的选举黑名单设置为对应 server,会清除掉选举黑名单中已有的该原因的 server
   * @param {ObAddr} &server 不允许当leader的server
   * @param {ObString} &reason 不允许当leader的原因
   * @return {*}
   * @Date: 2022-01-29 16:43:19
   */
  int set_or_replace_server_in_blacklist(const common::ObAddr &server, InsertElectionBlacklistReason reason);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(exec_tenant_id), K_(row_for_table), K_(row_for_user));
private:
  int begin_();
  int end_(const bool true_to_commit);
  int convert_table_info_to_user_info_();
  int convert_user_info_to_table_info_();
  int update_row_to_table_();
  int get_row_from_table_();
  int start_and_read_();
  int write_and_commit_();
  int schedule_refresh_priority_task_();
  int set_user_row_for_specific_reason_(const common::ObAddr &server, InsertElectionBlacklistReason reason);
private:
  const uint64_t tenant_id_;
  const share::ObLSID ls_id_;
  uint64_t exec_tenant_id_;// 事务执行读写操作时使用的tenant_id
  ObMySQLTransaction trans_;
  LsElectionReferenceInfoRowTypeForTable row_for_table_;// 对内部表友好的数据组织形式
  LsElectionReferenceInfoRowTypeForUser row_for_user_;// 对用户友好的数据组织形式
};

class TableAccessor
{
  struct ServerZoneNameCache// zone name获取成功一次即可，避免每次都访问内部表
  {
    void set_zone_name_to_global_cache(const ObStringHolder &zone_name) {
      ObSpinLockGuard lg(lock_);
      if (!server_zone_name_.empty()) {
        server_zone_name_.assign(zone_name);
      }
    }
    int get_zone_name_from_global_cache(ObStringHolder &zone_name) {
      int ret = OB_SUCCESS;
      ObSpinLockGuard lg(lock_);
      if (server_zone_name_.empty()) {
        ret = OB_CACHE_INVALID;
      } else if (OB_FAIL(zone_name.assign(server_zone_name_))) {
      }
      return ret;
    }
  private:
    ObSpinLock lock_;
    ObStringHolder server_zone_name_;
  };
  static ServerZoneNameCache SERVER_ZONE_NAME_CACHE;
public:
  static int get_all_ls_election_reference_info(common::ObIArray<LsElectionReferenceInfo> &all_ls_election_reference_info);
  static int get_self_zone_name(ObStringHolder &zone_name_holder);
  static int get_self_zone_region(const ObStringHolder &zone_name_holder, ObStringHolder &region_name_holder);
  static int is_primary_region(const ObStringHolder &region_name_holder, bool &is_primary_region);
  static int calculate_zone_priority_score(ObStringHolder &zone_priority, ObStringHolder &self_zone_name, int64_t &score);
  static int get_removed_status_and_reason(ObStringHolder &remove_member_info, bool &status, ObStringHolder &reason);
  static int get_zone_stop_status(ObStringHolder &zone_name, bool &is_zone_stopped);
  static int get_server_stop_status(bool &is_server_stopped);
};

}
}
}
#endif