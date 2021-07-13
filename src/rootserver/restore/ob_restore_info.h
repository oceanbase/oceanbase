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

#ifndef __OB_RS_RESTORE_INFO_H__
#define __OB_RS_RESTORE_INFO_H__

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_thread_cond.h"
#include "share/schema/ob_schema_struct.h"
#include "share/restore/ob_restore_args.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_physical_restore_info.h"
#include "share/ob_errno.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace observer {
class ObRestoreCtx;
}

namespace rootserver {

enum RestoreTaskStatus { RESTORE_INIT = 0, RESTORE_DOING, RESTORE_DONE, RESTORE_STOP, RESTORE_FAIL };

struct RestoreJob {
public:
  RestoreJob()
      : job_id_(common::OB_INVALID_ID),
        level_(0),
        status_(RESTORE_INIT),
        start_time_(0),
        backup_end_time_(0),
        recycle_end_time_(0),
        tenant_id_(common::OB_INVALID_ID),
        database_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        backup_table_id_(common::OB_INVALID_ID),
        partition_id_(common::OB_INVALID_ID)
  {}
  int64_t job_id_;
  // logical restore level:
  // 0: tenant
  // 1: database
  // 2: table
  // 3: partition
  // For now, we only support logical restore by tenant.
  int64_t level_;
  int64_t status_;
  uint64_t start_time_;
  uint64_t backup_end_time_;
  uint64_t recycle_end_time_;
  common::ObString backup_uri_;
  common::ObString tenant_name_;

private:
  uint64_t tenant_id_;       /* valid if level_ >= 0 */
  uint64_t database_id_;     /* valid if level_ >= 1 */
  uint64_t table_id_;        /* valid if level_ >= 2 */
  uint64_t backup_table_id_; /* valid if level_ >= 2 */
  uint64_t partition_id_;    /* valid if level_ >= 3 */

public:
  bool is_valid()
  {
    return common::OB_INVALID_ID != job_id_;
  }

  int assign(const RestoreJob& other)
  {
    int ret = common::OB_SUCCESS;
    allocator_.reset();
    if (OB_FAIL(ob_write_string(allocator_, other.backup_uri_, backup_uri_))) {
      OB_LOG(WARN, "fail copy backup_uri", K_(backup_uri), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, other.tenant_name_, tenant_name_))) {
      OB_LOG(WARN, "fail copy tenant_name", K_(tenant_name), K(ret));
    }

    job_id_ = other.job_id_;
    level_ = other.level_;
    status_ = other.status_;
    start_time_ = other.start_time_;
    backup_end_time_ = other.backup_end_time_;
    recycle_end_time_ = other.recycle_end_time_;

    tenant_id_ = other.tenant_id_;
    database_id_ = other.database_id_;
    table_id_ = other.table_id_;
    backup_table_id_ = other.backup_table_id_;
    partition_id_ = other.partition_id_;

    return ret;
  }

  TO_STRING_KV(K_(job_id), K_(level), K_(status), K_(start_time), K_(backup_end_time), K_(recycle_end_time),
      K_(backup_uri), K_(tenant_name), K_(tenant_id), K_(table_id), K_(backup_table_id), K_(partition_id));

private:
  common::ObArenaAllocator allocator_;
};

struct PartitionRestoreTask {
public:
  typedef common::ObSEArray<share::ObSchemaIdPair, 10> ObSchemaIdPairs;
  PartitionRestoreTask()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        table_id_(common::OB_INVALID_ID),
        backup_table_id_(common::OB_INVALID_ID),
        schema_id_pairs_(),
        partition_id_(common::OB_INVALID_ID),
        start_time_(0),
        job_id_(common::OB_INVALID_ID),
        status_(RESTORE_INIT),
        job_(NULL)
  {}
  ~PartitionRestoreTask() = default;

public:
  uint64_t tenant_id_;
  union {
    uint64_t table_id_;
    uint64_t tablegroup_id_;
  };
  union {
    uint64_t backup_table_id_;
    uint64_t backup_tablegroup_id_;
  };
  // 1. For PG, schema_id_pairs_ contains table_ids in pg and table related index_ids.
  // 2. For standalone partition, schema_id_pairs_ only contains related index_ids.
  common::ObSEArray<share::ObSchemaIdPair, 10> schema_id_pairs_;
  uint64_t partition_id_;
  uint64_t start_time_;
  int64_t job_id_;
  int64_t status_;
  RestoreJob* job_;
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(backup_table_id), K_(schema_id_pairs), K_(partition_id), K_(start_time),
      K_(job_id), K_(status));
};

// not used
class ObRestoreProgressTracker {
public:
  ObRestoreProgressTracker() : cond_(), has_task_(true), lock_(false)
  {}
  ~ObRestoreProgressTracker()
  {}
  int init()
  {
    int ret = common::OB_SUCCESS;
    has_task_ = true;
    lock_ = false;
    if (OB_FAIL(cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT))) {
      // fail
    }
    return ret;
  }

  bool has_restore_task()
  {
    common::ObThreadCondGuard guard(cond_);
    return has_task_;
  }

  int set_restore_lock()
  {
    int ret = common::OB_SUCCESS;
    common::ObThreadCondGuard guard(cond_);
    if (has_task_ || lock_) {
      ret = common::OB_RESTORE_IN_PROGRESS;
    } else {
      has_task_ = false;
      lock_ = true;
    }
    return ret;
  }

  void unset_restore_lock()
  {
    common::ObThreadCondGuard guard(cond_);
    lock_ = false;
  }

  void begin_restore()
  {
    common::ObThreadCondGuard guard(cond_);
    if (lock_) {
      has_task_ = true;
    }
  }

  void finish_restore()
  {
    common::ObThreadCondGuard guard(cond_);
    has_task_ = false;
    lock_ = false;
  }

private:
  common::ObThreadCond cond_;
  volatile bool has_task_;
  volatile bool lock_;
};

class ObRebalanceTaskMgr;
class TenantBalanceStat;
class ObDDLService;
class ObRootBalancer;
class ObRestoreMgrCtx {
public:
  ObRestoreMgrCtx()
      : conn_env_(NULL),
        task_mgr_(NULL),
        tenant_stat_(NULL),
        schema_service_(NULL),
        ddl_service_(NULL),
        sql_proxy_(NULL),
        root_balancer_(NULL)
  {}

  ~ObRestoreMgrCtx()
  {}

  bool is_valid()
  {
    return NULL != conn_env_ && NULL != task_mgr_ && NULL != tenant_stat_ && NULL != schema_service_ &&
           NULL != ddl_service_ && NULL != sql_proxy_ && NULL != root_balancer_;
  }

  observer::ObRestoreCtx* conn_env_;
  ObRebalanceTaskMgr* task_mgr_;
  TenantBalanceStat* tenant_stat_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObDDLService* ddl_service_;
  common::ObMySQLProxy* sql_proxy_;
  ObRootBalancer* root_balancer_;
};

}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OB_RS_RESTORE_INFO_H__ */
//// end of header file
