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

#ifndef OCEANBASE_LS_OB_INMEMORY_LS_TABLE_H_
#define OCEANBASE_LS_OB_INMEMORY_LS_TABLE_H_

#include "share/ls/ob_ls_table.h"                               // for ObLSTable
#include "share/ls/ob_ls_info.h"                                // for ObLSInfo
#include "lib/lock/ob_tc_rwlock.h"                              // for common::RWLock

namespace oceanbase
{
namespace share
{

class ObIRsListChangeCb
{
public:
  virtual int submit_update_rslist_task(const bool force_update = false) = 0;
  virtual int submit_report_replica() = 0;
  virtual int submit_report_replica(const int64_t tenant_id, const ObLSID &ls_id) = 0;
};

// [class_full_name] ObInMemoryLSTable
// [class_functions] use this class to report sys_log_stream's info on rs
// [class_attention] only rs could use this class
class ObInMemoryLSTable : public ObLSTable
{
public:
  // initial related functions
  explicit ObInMemoryLSTable();
  virtual ~ObInMemoryLSTable();
  int init(ObIRsListChangeCb &rs_list_change_cb);
  inline bool is_inited() const { return inited_; }
  void reuse();

  // get informations about a certain ls directly in memory
  // @param [in] cluster_id, belong to which cluste
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] mode, should not be ObLSTable::INNER_TABLE_ONLY_MODE
  // @param [out] ls_info, informations about a certain ls
  // TODO: enable cluster_id
  virtual int get(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObLSTable::Mode mode,
      ObLSInfo &ls_info) override;

  // update a certain log stream's replica info to meta table
  // @param [in] replica, the new replica infos to update
  // @param [in] inner_table_only, should be false.
  virtual int update(const ObLSReplica &replica, const bool inner_table_only) override;

  // remove ls replica
  // @param [in] tenant_id, which tenant's log stream
  // @param [in] ls_id, identifier for log stream
  // @param [in] server, where is this replica
  // @param [in] inner_table_only, should be false.
  virtual int remove(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      const ObAddr &server,
      const bool inner_table_only) override;

private:
  // the real action to read LSinfo directly from memory
  // @parma [in] tenant_id, get whose ls info
  // @param [in] ls_id, get which ls info
  // @param [in] filter_flag_replica, whether to filter flag-replica
  // @param [out] ls_info, informations about a certain ls
  // TODO: make it clear what is a flag_replica, replica_status or data_version/rebuild?
  int inner_get_(const uint64_t tenant_id,
                 const ObLSID &ls_id,
                 const bool filter_flag_replica,
                 ObLSInfo &ls_info);

  // update a leader replica
  // @param [in] replica, new leader replica informations to udpate
  int update_leader_replica_(const ObLSReplica &replica);

  // update a follower replica
  // @param [in] replica, new follower replica informations to update
  int update_follower_replica_(const ObLSReplica &replica);

private:
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;
  bool inited_;                                  //whether this class is inited
  ObLSInfo ls_info_;                             //informations about this log stream
  common::RWLock lock_;                          //locks to do concurrency-control
  ObIRsListChangeCb *rs_list_change_cb_;         //to do submit_update_rslist_task()

  DISALLOW_COPY_AND_ASSIGN(ObInMemoryLSTable);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_LS_OB_INMEMORY_LS_TABLE_H_
