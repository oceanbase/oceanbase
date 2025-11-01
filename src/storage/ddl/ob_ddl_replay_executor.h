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
#include "storage/ddl/ob_tablet_split_task.h"
#include "storage/ddl/ob_tablet_lob_split_task.h"
#include "storage/ddl/ob_direct_load_struct.h"
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
  ~ObDDLReplayExecutor() = default;
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
      const uint64_t data_format_version,
      bool &need_replay);
  static int check_need_replay_ddl_inc_log_(
      ObLS *ls,
      const ObTabletHandle &tablet_handle,
      const share::SCN &scn,
      const ObDirectLoadType direct_load_type,
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
  ~ObDDLStartReplayExecutor() = default;

  virtual bool is_replay_ddl_control_log_() const override final { return true; }

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
  int pre_process_for_cs_replica(
      ObTabletDirectLoadInsertParam &direct_load_param,
      ObITable::TableKey &table_key,
      ObTabletHandle &tablet_handle,
      const ObTabletID &tablet_id);
private:
  const ObDDLStartLog *log_;
};

class ObDDLRedoReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLRedoReplayExecutor();
  ~ObDDLRedoReplayExecutor() = default;

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
  virtual int do_replay_(ObTabletHandle &handle) override;

#ifdef OB_BUILD_SHARED_STORAGE
  int write_ss_block(blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &macro_handle);
#endif
private:
  int do_inc_replay_(
      ObTabletHandle &tablet_handle,
      blocksstable::ObMacroBlockWriteInfo &write_info,
      storage::ObDDLMacroBlock &macro_block,
      const ObDirectLoadType direct_load_type);
  int do_full_replay_(
      ObTabletHandle &tablet_handle,
      blocksstable::ObMacroBlockWriteInfo &write_info,
      storage::ObDDLMacroBlock &macro_block);
  int filter_redo_log_(
      const ObDDLMacroBlockRedoInfo &redo_info,
      const ObTabletHandle &tablet_handle,
      bool &can_skip);
private:
  const ObDDLRedoLog *log_;
};

class ObDDLCommitReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLCommitReplayExecutor();
  ~ObDDLCommitReplayExecutor() = default;

  virtual bool is_replay_ddl_control_log_() const override final { return true; }

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

#ifdef OB_BUILD_SHARED_STORAGE
class ObDDLFinishReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLFinishReplayExecutor();

  int init(
      ObLS *ls,
      const ObDDLFinishLog &log,
      const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return OB_TASK_EXPIRED, ddl task expired.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;
  int replay_ddl_finish(ObTabletHandle &handle);

private:
  const ObDDLFinishLog *log_;
};
#endif

class ObDDLIncMinorStartReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncMinorStartReplayExecutor();

  int init(ObLS *ls, const common::ObTabletID &tablet_id, const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  common::ObTabletID tablet_id_;
};

class ObDDLIncMinorCommitReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncMinorCommitReplayExecutor();

  int init(ObLS *ls, const common::ObTabletID &tablet_id, const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  common::ObTabletID tablet_id_;
};

class ObDDLIncMajorStartReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncMajorStartReplayExecutor();

  int init(ObLS *ls,
          const common::ObTabletID &tablet_id,
          const share::SCN &scn,
          const bool has_cs_replica,
          const bool is_lob,
          const ObStorageSchema *storage_schema);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;
private:
  int update_tablet_meta_for_cs_replica_(ObTabletHandle &tablet_handle);
  int update_storage_schema_to_tablet(ObTabletHandle &tablet_handle);

private:
  common::ObTabletID tablet_id_;
  bool has_cs_replica_;
  bool is_lob_;
  const ObStorageSchema *storage_schema_;
};

class ObDDLIncMajorCommitReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObDDLIncMajorCommitReplayExecutor();

  int init(ObLS *ls,
           const common::ObTabletID &tablet_id,
           const share::SCN &scn,
           const transaction::ObTransID &trans_id,
           const transaction::ObTxSEQ &seq_no,
           const int64_t snapshot_version,
           const uint64_t data_format_version,
           const bool is_rollback);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  int do_replay_(ObTabletHandle &handle) override;

private:
  common::ObTabletID tablet_id_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
  int64_t snapshot_version_;
  uint64_t data_format_version_;
  bool is_rollback_;
};

class ObSplitStartReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObSplitStartReplayExecutor();
  ~ObSplitStartReplayExecutor() = default;

  int init(
      ObLS *ls,
      const ObTabletSplitStartLog &log,
      const share::SCN &scn);
  static int prepare_param_from_log(
      const share::ObLSID &ls_id,
      const ObTabletHandle &handle,
      const ObTabletSplitInfo &info,
      const share::SCN &scn,
      ObTabletSplitParam &param);
  static int prepare_param_from_log(
      const share::ObLSID &ls_id,
      const ObTabletHandle &handle,
      const ObTabletSplitInfo &info,
      const share::SCN &scn,
      ObLobSplitParam &param);
  static bool is_split_log_retry_ret(const int ret_code) {
    return OB_EAGAIN == ret_code || OB_SIZE_OVERFLOW == ret_code || OB_NEED_RETRY == ret_code;
  }

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  virtual int do_replay_(ObTabletHandle &handle) override;

private:
  int check_need_wait_split_finished(
      const share::ObLSID &ls_id,
      const ObTabletHandle &handle,
      const ObIArray<ObTabletID> &dest_tablets_id,
      bool &need_wait_split_finished);

private:
  const ObTabletSplitStartLog *log_;
};

class ObSplitFinishReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObSplitFinishReplayExecutor();
  ~ObSplitFinishReplayExecutor() = default;

  int init(
      ObLS *ls,
      const ObTabletSplitFinishLog &log,
      const share::SCN &scn);

protected:
  // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  virtual int do_replay_(ObTabletHandle &handle) override;
private:
  static int modify_tablet_restore_status_if_need(
      const ObIArray<ObTabletID> &dest_tablet_ids,
      const ObTabletHandle &src_tablet_handle,
      ObLS* ls);
  int check_can_skip_replay(ObTabletHandle &handle, bool &can_skip);
private:
  const ObTabletSplitFinishLog *log_;
};

class ObTabletFreezeReplayExecutor final : public ObDDLReplayExecutor
{
public:
  ObTabletFreezeReplayExecutor();
  ~ObTabletFreezeReplayExecutor() = default;

  int init(
      ObLS *ls,
      const ObTabletFreezeLog &log,
      const share::SCN &scn);

protected:
   // replay to the tablet
  // @return OB_SUCCESS, replay successfully, data has written to tablet.
  // @return OB_EAGAIN, failed to replay, need retry.
  // @return OB_NO_NEED_UPDATE, this log needs to be ignored.
  // @return other error codes, failed to replay.
  virtual int do_replay_(ObTabletHandle &handle) override;

private:
  const ObTabletFreezeLog *log_;
};


class ObSchemaChangeReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObSchemaChangeReplayExecutor();
  ~ObSchemaChangeReplayExecutor() = default;

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
  virtual int do_replay_(ObTabletHandle &handle) override;

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
