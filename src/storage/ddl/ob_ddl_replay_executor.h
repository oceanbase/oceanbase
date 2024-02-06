/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_DDL_REPLAY_EXECUTOR_H
#define OCEANBASE_STORAGE_OB_DDL_REPLAY_EXECUTOR_H

#include "common/ob_tablet_id.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTabletHandle;

class ObDDLReplayExecutor : public logservice::ObTabletReplayExecutor
{
public:

  ObDDLReplayExecutor();

protected:
  bool is_replay_update_tablet_status_() const override final
  {
    return false;
  }

  int check_need_replay_ddl_log_(
      const ObTabletHandle &tablet_handle,
      const share::SCN &ddl_start_scn,
      const share::SCN &scn,
      bool &need_replay) const;

  virtual bool is_replay_update_mds_table_() const override
  {
    return false;
  }

protected:
  ObLS *ls_;
  common::ObTabletID tablet_id_;
  share::SCN scn_;
};


class ObDDLStartReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLStartReplayExecutor();

  int init(
      ObLS *ls,
      const ObDDLStartLog &log,
      const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  const ObDDLStartLog *log_;
};


class ObDDLRedoReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLRedoReplayExecutor();

  int init(
      ObLS *ls,
      const ObDDLRedoLog &log,
      const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  const ObDDLRedoLog *log_;
};


class ObDDLCommitReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLCommitReplayExecutor();

  int init(
      ObLS *ls,
      const ObDDLCommitLog &log,
      const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return OB_TASK_EXPIRED, ddl task expired.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  const ObDDLCommitLog *log_;
};


class ObSchemaChangeReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObSchemaChangeReplayExecutor();

  int init(
      const ObTabletSchemaVersionChangeLog &log,
      const share::SCN &scn);

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }

  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return OB_TASK_EXPIRED, ddl task expired.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

  virtual bool is_replay_update_mds_table_() const override
  {
    return false;
  }

private:
  const ObTabletSchemaVersionChangeLog *log_;
  share::SCN scn_;
};


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_DDL_REDO_LOG_REPLAYER_H
