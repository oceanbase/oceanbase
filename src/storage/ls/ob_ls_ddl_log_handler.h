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

#ifndef OCEANBASE_STORAGE_LS_DDL_LOG_HANDLER_
#define OCEANBASE_STORAGE_LS_DDL_LOG_HANDLER_

#include "logservice/ob_log_base_type.h"
#include "storage/ddl/ob_ddl_redo_log_replayer.h"

namespace oceanbase
{

namespace storage
{

class ObLS;

class ObLSDDLLogHandler : public logservice::ObIReplaySubHandler,
                          public logservice::ObIRoleChangeSubHandler,
                          public logservice::ObICheckpointSubHandler
{
public:
  ObLSDDLLogHandler() : is_inited_(false), is_online_(false), ls_(nullptr), last_rec_scn_() {}
  ~ObLSDDLLogHandler() { reset(); }

public:
  int init(ObLS *ls);
  void reset();

  // for migrate and rebuild
  int offline();
  int online();

  // for replay
  int replay(const void *buffer,
             const int64_t buf_size,
             const palf::LSN &lsn,
             const share::SCN &log_ts) override final;

  // for role change
  void switch_to_follower_forcedly() override final;
  int switch_to_leader() override final;
  int switch_to_follower_gracefully() override final;
  int resume_leader() override final;

  // for checkpoint
  int flush(share::SCN &rec_scn) override final;
  share::SCN get_rec_scn() override final;
private:
  int replay_ddl_redo_log_(const char *log_buf, const int64_t buf_size, int64_t pos, const share::SCN &scn);
  int replay_ddl_commit_log_(const char *log_buf, const int64_t buf_size, int64_t pos, const share::SCN &scn);
  int replay_ddl_tablet_schema_version_change_log_(const char *log_buf, const int64_t buf_size, int64_t pos, const share::SCN &scn);
  int replay_ddl_start_log_(const char *log_buf, const int64_t buf_size, int64_t pos, const share::SCN &scn);
private:
  bool is_inited_;
  bool is_online_;
  ObLS *ls_;
  common::TCRWLock online_lock_;
  ObDDLRedoLogReplayer ddl_log_replayer_;
  share::SCN last_rec_scn_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_LS_DDL_LOG_HANDLER_
