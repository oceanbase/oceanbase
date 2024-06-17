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
#include "storage/ddl/ob_ddl_inc_clog.h"
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

  static int check_need_replay_ddl_log_(
      const ObLS *ls,
      const ObTabletHandle &tablet_handle,
      const share::SCN &ddl_start_scn,
      const share::SCN &scn,
      bool &need_replay);
  static int check_need_replay_ddl_inc_log_(
      const ObLS *ls,
      const ObTabletHandle &tablet_handle,
      const share::SCN &scn,
      bool &need_replay);

  static int get_lob_meta_tablet_id(
      const ObTabletHandle &tablet_handle,
      const common::ObTabletID &possible_lob_meta_tablet_id,
      common::ObTabletID &lob_meta_tablet_id);


  virtual bool is_replay_update_mds_table_() const override
  {
    return false;
  }
private:
  static int check_need_replay_(
      const ObLS *ls,
      const ObTabletHandle &tablet_handle,
      bool &need_replay);
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
  int replay_ddl_start(ObTabletHandle &handle, const bool is_lob_meta_tablet);

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
  int do_inc_replay_(
      ObTabletHandle &tablet_handle,
      blocksstable::ObMacroBlockWriteInfo &write_info,
      storage::ObDDLMacroBlock &macro_block);
  int do_full_replay_(
      ObTabletHandle &tablet_handle,
      blocksstable::ObMacroBlockWriteInfo &write_info,
      storage::ObDDLMacroBlock &macro_block);
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
  int replay_ddl_commit(ObTabletHandle &handle);

private:
  const ObDDLCommitLog *log_;
};

class ObDDLIncStartReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncStartReplayExecutor();

  int init(ObLS *ls, const ObDDLIncStartLog &log, const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  const ObDDLIncStartLog *log_;
};

class ObDDLIncCommitReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncCommitReplayExecutor();

  int init(ObLS *ls, const ObDDLIncCommitLog &log, const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  const ObDDLIncCommitLog *log_;
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
