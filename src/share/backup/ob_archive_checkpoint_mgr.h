// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef SRC_SHARE_BACKUP_OB_ARCHIVE_CHECKPOINT_MGR_H_
#define SRC_SHARE_BACKUP_OB_ARCHIVE_CHECKPOINT_MGR_H_

#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_archive_store.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{
class ObGetMaxCheckpointOp : public ObBaseDirEntryOperator
{
public:
  ObGetMaxCheckpointOp(
      uint64_t& max_checkpoint_scn,
      const char *file_name,
      const ObBackupFileSuffix &type)
    : max_checkpoint_scn_(max_checkpoint_scn),
      file_name_(file_name),
      type_(type) {}
  virtual ~ObGetMaxCheckpointOp() {}
  bool is_valid() const;
  int func(const dirent *entry);
private:
  uint64_t& max_checkpoint_scn_;
  const char *file_name_;
  ObBackupFileSuffix type_;

  DISALLOW_COPY_AND_ASSIGN(ObGetMaxCheckpointOp);
};

//delete files with smaller checkpoint_scn in file name
class ObDelHisCheckpointFileOp : public ObBaseDirEntryOperator
{
public:
  ObDelHisCheckpointFileOp(
      const uint64_t checkpoint_scn,
      const ObBackupPath &path,
      const char *file_name,
      const ObBackupFileSuffix &type,
      const share::ObBackupStorageInfo *storage_info)
    : checkpoint_scn_(checkpoint_scn),
      path_(path),
      file_name_(file_name),
      type_(type),
      storage_info_(storage_info) {}
  virtual ~ObDelHisCheckpointFileOp() {}
  bool is_valid() const;
  int func(const dirent *entry) ;

private:
  uint64_t checkpoint_scn_;
  ObBackupPath path_;
  const char *file_name_;
  ObBackupFileSuffix type_;
  const share::ObBackupStorageInfo *storage_info_;

  DISALLOW_COPY_AND_ASSIGN(ObDelHisCheckpointFileOp);
};

class ObArchiveCheckpointMgr final
{
public:
  ObArchiveCheckpointMgr()
    : is_inited_(false),
      path_(),
      file_name_(nullptr),
      type_(),
      storage_info_(nullptr) {}
  ~ObArchiveCheckpointMgr() {}
  int init(
      const ObBackupPath &path,
      const char *file_name,
      const ObBackupFileSuffix &type,
      const ObBackupStorageInfo *storage_info);
  void reset();
  bool is_valid() const;
  int write(const uint64_t checkpoint_scn) const;
  int read(uint64_t &checkpoint_scn) const;
private:
  int get_max_checkpoint_scn_(const ObBackupPath &path, uint64_t &max_checkpoint_scn) const;
  int del_history_files_(const ObBackupPath &dir_path, const uint64_t write_checkpoint_scn) const;
  int write_checkpoint_file_(const ObBackupPath &path) const;
  int check_is_tagging_(const ObBackupStorageInfo *storage_info, bool &is_tagging) const;

  TO_STRING_KV(K_(is_inited), K_(path), KP_(file_name), K_(type));

private:
  bool is_inited_;
  ObBackupPath path_;
  const char *file_name_;
  ObBackupFileSuffix type_;
  const ObBackupStorageInfo *storage_info_;
  DISALLOW_COPY_AND_ASSIGN(ObArchiveCheckpointMgr);
};

}
}

#endif /* SRC_SHARE_BACKUP_OB_ARCHIVE_CHECKPOINT_MGR_H_*/
