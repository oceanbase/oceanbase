/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef STORAGE_LOG_STREAM_BACKUP_ARCHIVE_PIECE_COPIER_H_
#define STORAGE_LOG_STREAM_BACKUP_ARCHIVE_PIECE_COPIER_H_

#include "share/ob_rpc_struct.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"

namespace oceanbase {
namespace backup {

struct ObBackupArchivePieceCopyParam final : public share::ObIDagInitParam
{
  ObBackupArchivePieceCopyParam();
  virtual ~ObBackupArchivePieceCopyParam() {}
  virtual bool is_valid() const override;
  int assign(const ObBackupArchivePieceCopyParam &other);
  int set(const obrpc::ObNotifyBackupArchiveArg &arg);
  TO_STRING_KV(K_(trace_id), K_(job_id), K_(tenant_id), K_(dest_id), K_(round_id), K_(piece_id), K_(backup_path));
  share::ObTaskId trace_id_; // not dag trace id
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  share::ObBackupPathString backup_path_;
};

struct ObBackupArchivePieceCopyCtx
{
public:
  explicit ObBackupArchivePieceCopyCtx(ObBackupArchivePieceCopyParam &param)
    : param_(param), src_dest_(), dst_dest_(), ls_ids_(), buf_(nullptr), allocator_("GlobalBkpArcCtx"), is_inited_(false) {}
  ~ObBackupArchivePieceCopyCtx() {}

  int init();

  // copy a single file from src to dst using multipart write; buf is a reusable buffer of buf_len bytes
  int copy_single_file_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path, char *buf, const int64_t buf_len) const;

  // copy all files and subdirectories recursively from src_dir to dst_dir;
  // when skip_ls_subdir is true, top-level logstream_* subdirs are skipped (only meaningful at the piece dir level)
  int copy_dir_recursively(const share::ObBackupPath &src_dir, const share::ObBackupPath &dst_dir, common::ObIAllocator &allocator, char *buf, const int64_t buf_len, const bool skip_ls_subdir) const;

  // log_archive_dest/rounds/: ROUND_DESTID_ROUNDID_start.obarc && ROUND_DESTID_ROUNDID_end.obarc
  int copy_round_files() const;

  // copy the piece start/end placeholder under log_archive_dest/pieces/
  int copy_piece_placeholder(const bool copy_end_file);
  TO_STRING_KV(K_(param), K_(src_dest), K_(dst_dest), K_(src_piece_dir), K_(dst_piece_dir), K_(ls_ids));
public:
  static const int64_t MAX_PART_SIZE = 2 * 1024 * 1024;  // 2MB, same chunk size as clog complement copy
  ObBackupArchivePieceCopyParam &param_;
  share::ObBackupDest src_dest_; // LOG ARCHIVE DEST
  share::ObBackupDest dst_dest_; // BACKUP ARCHIVE DEST
  share::ObBackupPath src_piece_dir_;
  share::ObBackupPath dst_piece_dir_;
  common::ObArray<share::ObLSID> ls_ids_;
  char *buf_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};


class ObBackupArchivePieceCopyDagNet : public share::ObIDagNet
{
public:
  ObBackupArchivePieceCopyDagNet();
  virtual ~ObBackupArchivePieceCopyDagNet() {}
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual bool is_valid() const override { return param_.is_valid(); }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(is_inited), K_(param));
private:
  bool is_inited_;
  ObBackupArchivePieceCopyParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchivePieceCopyDagNet);
};


class ObBackupArchivePieceCopyDag : public share::ObIDag
{
public:
  ObBackupArchivePieceCopyDag();
  virtual ~ObBackupArchivePieceCopyDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  const ObBackupArchivePieceCopyParam &get_param() const { return param_; }
  ObBackupArchivePieceCopyCtx &get_ctx() { return ctx_; }
  const ObBackupArchivePieceCopyCtx &get_ctx() const { return ctx_; }
  int generate_copy_tasks(share::ObITask *prepare_task);
  INHERIT_TO_STRING_KV("ObIDag", share::ObIDag, K_(is_inited), K_(param), K_(ctx));
private:
  bool is_inited_;
  ObBackupArchivePieceCopyParam param_;
  lib::Worker::CompatMode compat_mode_;
  ObBackupArchivePieceCopyCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchivePieceCopyDag);
};


class ObBackupArchivePieceCopyPrepareTask : public share::ObITask
{
public:
  ObBackupArchivePieceCopyPrepareTask()
   : ObITask(ObITask::TASK_TYPE_BACKUP_ARCHIVELOG), is_inited_(false), ctx_(nullptr) {}
  virtual ~ObBackupArchivePieceCopyPrepareTask() {}
  int init(ObBackupArchivePieceCopyCtx &copy_ctx);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", share::ObITask, K_(is_inited));
private:
  bool is_inited_;
  ObBackupArchivePieceCopyCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchivePieceCopyPrepareTask);
};


class ObBackupArchivePieceCopyTask : public share::ObITask
{
public:
  ObBackupArchivePieceCopyTask()
   : ObITask(ObITask::TASK_TYPE_BACKUP_ARCHIVELOG), is_inited_(false), idx_(0), ctx_(nullptr) {}
  virtual ~ObBackupArchivePieceCopyTask() {}
  int init(const int64_t idx, ObBackupArchivePieceCopyCtx &copy_ctx);
  virtual int process() override;
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int64_t get_sub_task_id() const override { return idx_; }
  INHERIT_TO_STRING_KV("ObITask", share::ObITask, K_(is_inited), K_(idx), KPC_(ctx));
private:
  bool is_inited_;
  int64_t idx_;
  ObBackupArchivePieceCopyCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchivePieceCopyTask);
};

class ObBackupArchivePieceCopyFinishTask : public share::ObITask
{
public:
  ObBackupArchivePieceCopyFinishTask()
   : ObITask(ObITask::TASK_TYPE_BACKUP_ARCHIVELOG), is_inited_(false), ctx_(nullptr) {}
  virtual ~ObBackupArchivePieceCopyFinishTask() {}
  int init(ObBackupArchivePieceCopyCtx &copy_ctx);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", share::ObITask, K_(is_inited), KPC_(ctx));
private:
  bool is_inited_;
  ObBackupArchivePieceCopyCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupArchivePieceCopyFinishTask);
};


class ObBackupArchiveHandler
{
public:
  static int schedule_piece_copy_dag(const obrpc::ObNotifyBackupArchiveArg &arg);
  static int report_piece_copy_result(const ObBackupArchivePieceCopyParam &param, const int result);
};

}  // namespace backup
}  // namespace oceanbase

#endif  // STORAGE_LOG_STREAM_BACKUP_ARCHIVE_PIECE_COPIER_H_
