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

#ifndef OCEANBASE_LOGSERVICE_PALF_CALLBACK_
#define OCEANBASE_LOGSERVICE_PALF_CALLBACK_
#include <stdint.h>
#include "common/ob_role.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "log_meta_info.h"
#include "lsn.h"
namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace palf
{
class PalfFSCb
{
public:
  // end_lsn返回的是最后一条已确认日志的下一位置
  virtual int update_end_lsn(int64_t id, const LSN &end_lsn, const int64_t proposal_id) = 0;
};

class PalfRoleChangeCb
{
public:
  virtual int on_role_change(const int64_t id) = 0;
  virtual int on_need_change_leader(const int64_t ls_id, const common::ObAddr &new_leader) = 0;
};

class PalfRebuildCb
{
public:
  // lsn 表示触发rebuild时源端的基线lsn位点
  virtual int on_rebuild(const int64_t id, const LSN &lsn) = 0;
};

class PalfLocationCacheCb
{
public:
  virtual int get_leader(const int64_t id, common::ObAddr &leader) = 0;
  virtual int nonblock_get_leader(const int64_t id, common::ObAddr &leader) = 0;
  virtual int nonblock_renew_leader(const int64_t id) = 0;
};

class PalfMonitorCb
{
public:
  // record events
  virtual int record_set_initial_member_list_event(const int64_t palf_id,
                                                   const int64_t replica_num,
                                                   const char *member_list = NULL,
                                                   const char *extra_info = NULL) = 0;
  virtual int record_election_leader_change_event(const int64_t palf_id, const common::ObAddr &dest_addr) = 0;
  virtual int record_reconfiguration_event(const char *sub_event,
                                           const int64_t palf_id,
                                           const LogConfigVersion& config_version,
                                           const int64_t prev_replica_num,
                                           const int64_t curr_replica_num,
                                           const char *extra_info = NULL) = 0;
  virtual int record_replica_type_change_event(const int64_t palf_id,
                                               const LogConfigVersion& config_version,
                                               const char *prev_replica_type,
                                               const char *curr_replica_type,
                                               const char *extra_info = NULL) = 0;
  virtual int record_access_mode_change_event(const int64_t palf_id,
                                              const int64_t prev_mode_version,
                                              const int64_t curr_mode_verion,
                                              const AccessMode& prev_access_mode,
                                              const AccessMode& curr_access_mode,
                                              const char *extra_info = NULL) = 0;
  virtual int record_set_base_lsn_event(const int64_t palf_id, const LSN &new_base_lsn) = 0;
  virtual int record_enable_sync_event(const int64_t palf_id) = 0;
  virtual int record_disable_sync_event(const int64_t palf_id) = 0;
  virtual int record_enable_vote_event(const int64_t palf_id) = 0;
  virtual int record_disable_vote_event(const int64_t palf_id) = 0;
  virtual int record_advance_base_info_event(const int64_t palf_id, const PalfBaseInfo &palf_base_info) = 0;
  virtual int record_rebuild_event(const int64_t palf_id,
                                   const common::ObAddr &server,
                                   const LSN &base_lsn) = 0;
  virtual int record_flashback_event(const int64_t palf_id,
                                     const int64_t mode_version,
                                     const share::SCN &flashback_scn,
                                     const share::SCN &curr_end_scn,
                                     const share::SCN &curr_max_scn) = 0;
  virtual int record_truncate_event(const int64_t palf_id,
                                    const LSN &lsn,
                                    const int64_t min_block_id,
                                    const int64_t max_block_id,
                                    const int64_t truncate_end_block_id) = 0;
  virtual int record_role_change_event(const int64_t palf_id,
                                       const common::ObRole &prev_role,
                                       const palf::ObReplicaState &prev_state,
                                       const common::ObRole &curr_role,
                                       const palf::ObReplicaState &curr_state,
                                       const char *extra_info = NULL) = 0;

  // performance statistic
  virtual int add_log_write_stat(const int64_t palf_id, const int64_t log_write_size) = 0;
};

class PalfLiteMonitorCb
{
public:
  // @desc: record creating or deleting events
  // add/remove cluster: valid cluster_id, invalid tenant_id, invalid ls_id,
  // add/remove tenant: valid cluster_id, valid tenant_id, invalid ls_id,
  // add/remove ls: valid cluster_id, valid tenant_id, valid ls_id,
  virtual int record_create_or_delete_event(const int64_t cluster_id,
                                            const uint64_t tenant_id,
                                            const int64_t ls_id,
                                            const bool is_create,
                                            const char *extra_info) = 0;
};

} // end namespace palf
} // end namespace oceanbase
#endif
