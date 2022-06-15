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

#ifndef _OB_TABLE_TTL_TASK_H
#define _OB_TABLE_TTL_TASK_H 1
#include "ob_table_ttl_common.h"
#include "sql/ob_sql_trans_control.h"
#include "share/scheduler/ob_dag_scheduler.h"
namespace oceanbase
{
namespace observer
{

class ObTableTTLDeleteTask : public share::ObITask
{
public:
  ObTableTTLDeleteTask();
  ~ObTableTTLDeleteTask();
  int init(const ObTTLPara &ttl_para, ObTTLTaskInfo &ttl_info);
  virtual int process() override;

private:
  static const int64_t RETRY_INTERVAL = 30 * 60 * 1000 * 1000l; // 30min
  static const int64_t PER_TASK_DEL_ROWS = 10000l; 
  static const int64_t ONE_TASK_TIMEOUT = 1 * 60 * 1000 * 1000l; // 1min

  int process_one();
  int start_trans();
  int end_trans(bool is_rollback);
  int sync_end_trans(bool is_rollback, int64_t timeout_ts);

private:
  bool is_inited_;
  const ObTTLPara *param_;
  ObTTLTaskInfo *info_;
  common::ObArenaAllocator allocator_;
  // txn control
  sql::TransState trans_state_;
  transaction::ObTransDesc trans_desc_;
  ObPartitionLeaderArray participants_leaders_;
  transaction::ObPartitionEpochArray part_epoch_list_;
  ObString first_key_;
  ObString second_key_;

  DISALLOW_COPY_AND_ASSIGN(ObTableTTLDeleteTask);
};

class ObTableTTLDag final: public share::ObIDag
{
public:
  ObTableTTLDag();
  virtual ~ObTableTTLDag();
  virtual bool operator==(const ObIDag& other) const override;
  virtual int64_t hash() const override;
  int init(const ObTTLPara &param, ObTTLTaskInfo &info);
  virtual int64_t get_tenant_id() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int64_t get_compat_mode() const override { return static_cast<int64_t>(compat_mode_);}

private:
  bool is_inited_;
  ObTTLPara param_;
  ObTTLTaskInfo info_;
  share::ObWorker::CompatMode compat_mode_;

  DISALLOW_COPY_AND_ASSIGN(ObTableTTLDag);
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OB_TABLE_TTL_TASK_H */
