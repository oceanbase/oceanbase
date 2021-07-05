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

#ifndef __OB_RS_RESTORE_UTIL_H__
#define __OB_RS_RESTORE_UTIL_H__

#include "share/ob_rpc_struct.h"
#include "share/restore/ob_restore_args.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/restore/ob_restore_info.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase {

namespace rootserver {
class ObRecoveryHelper {

public:
  struct PkeyInfo {
    int64_t pkey_index_;
    bool part_valid_;
    PkeyInfo()
    {
      reset();
    }
    ~PkeyInfo()
    {}
    void reset()
    {
      pkey_index_ = -1;
      part_valid_ = false;
    }
    TO_STRING_KV(K_(pkey_index), K_(part_valid));
  };

  struct ObMemberListPkeyInfo {
  public:
    ObMemberListPkeyInfo()
    {
      reset();
    }
    ~ObMemberListPkeyInfo()
    {}
    void reset();
    TO_STRING_KV(K_(member_list), K_(pkey_info));
    common::ObMemberList member_list_;
    common::ObArray<PkeyInfo> pkey_info_;
  };
  struct ObMemberListPkeyList {
  public:
    ObMemberListPkeyList()
    {
      reset();
    }
    ~ObMemberListPkeyList()
    {}
    void reset();
    int add_partition(const share::ObPartitionInfo& partition);
    int find_member_list(const share::ObPartitionInfo& partition, int64_t& member_list_index);
    int add_member_list(const share::ObPartitionInfo& partition);
    int add_partition_valid(const obrpc::ObBatchCheckRes& result);
    bool all_partitions_valid() const;
    int compaction();

    TO_STRING_KV(K_(epoch), K_(pkey_array), K_(ml_pk_array));

  public:
    common::ObArray<common::ObPartitionKey> pkey_array_;
    common::ObArray<ObMemberListPkeyInfo> ml_pk_array_;
    int64_t epoch_;
  };
  struct ObLeaderPkeyList {
  public:
    ObLeaderPkeyList() : leader_(), pkey_list_()
    {}
    ~ObLeaderPkeyList()
    {}
    void reset();
    TO_STRING_KV(K_(leader), K_(pkey_list));

  public:
    ObAddr leader_;
    common::ObArray<common::ObPartitionKey> pkey_list_;
  };

  struct ObLeaderPkeyLists {
  public:
    ObLeaderPkeyLists() : user_leader_pkeys_(), inner_leader_pkeys_()
    {}
    ~ObLeaderPkeyLists()
    {}
    int add_partition(const share::ObPartitionInfo& partition_info);
    void reset();
    TO_STRING_KV(K_(inner_leader_pkeys), K_(user_leader_pkeys));
    int64_t user_count() const
    {
      return user_leader_pkeys_.count();
    }
    int64_t leader_count() const
    {
      return inner_leader_pkeys_.count();
    }

  public:
    common::ObArray<ObLeaderPkeyList> user_leader_pkeys_;
    common::ObArray<ObLeaderPkeyList> inner_leader_pkeys_;
  };
};

class ObRestoreUtil {
public:
  ObRestoreUtil();
  ~ObRestoreUtil();
  int init(observer::ObRestoreCtx& restore_ctx, int64_t job_id);
  int execute(const obrpc::ObRestoreTenantArg& arg);

public:
  static int check_has_job(common::ObMySQLProxy* sql_client, const obrpc::ObRestoreTenantArg& arg, bool& has_job);
  static int check_has_job(common::ObMySQLProxy* sql_client, bool& has_job);
  static int fill_physical_restore_job(
      const int64_t job_id, const obrpc::ObPhysicalRestoreTenantArg& arg, share::ObPhysicalRestoreJob& job);
  static int record_physical_restore_job(common::ObISQLClient& sql_client, const share::ObPhysicalRestoreJob& job);
  static int update_job_status(
      common::ObISQLClient& sql_client, const int64_t job_id, share::PhysicalRestoreStatus status);
  static int check_has_physical_restore_job(
      common::ObISQLClient& sql_client, const common::ObString& tenant_name, bool& has_job);

private:
  int record_job(const obrpc::ObRestoreTenantArg& arg);
  static int check_has_job_without_lock(
      common::ObISQLClient& sql_client, const common::ObString& tenant_name, bool& has_job);

private:
  /* variables */
  bool inited_;
  int64_t job_id_;
  observer::ObRestoreCtx* restore_ctx_;
  share::ObRestoreArgs restore_args_;
  static lib::ObMutex check_job_sync_lock_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreUtil);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OB_RS_RESTORE_UTIL_H__ */
//// end of header file
