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

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_store.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/ls/ob_ls_meta_package.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/backup/ob_backup_ctx.h"

#ifndef STORAGE_LOG_STREAM_BACKUP_EXTERN_INFO_MGR_H_
#define STORAGE_LOG_STREAM_BACKUP_EXTERN_INFO_MGR_H_
namespace oceanbase {
namespace backup {

struct ObBackupLSMetaInfo final {
  OB_UNIS_VERSION(1);

public:
  ObBackupLSMetaInfo();
  ~ObBackupLSMetaInfo();
  bool is_valid() const;
  int64_t get_total_serialize_buf_size() const;
  int serialize_to(char *buf, int64_t buf_size, int64_t &pos) const;
  int deserialize_from(char *buf, int64_t buf_size);
  TO_STRING_KV(K_(ls_meta_package));
  ObLSMetaPackage ls_meta_package_;
};

class ObExternLSMetaMgr {
public:
  ObExternLSMetaMgr();
  virtual ~ObExternLSMetaMgr();
  int init(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &backup_set_desc,
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);
  int write_ls_meta_info(const ObBackupLSMetaInfo &ls_meta);
  int read_ls_meta_info(ObBackupLSMetaInfo &ls_meta);

private:
  int get_ls_meta_backup_path_(share::ObBackupPath &path);

private:
  bool is_inited_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  DISALLOW_COPY_AND_ASSIGN(ObExternLSMetaMgr);
};

struct ObTabletInfoTrailer final : public ObExternBackupDataDesc
{
public:
  static const uint8_t FILE_VERSION = 1;
  OB_UNIS_VERSION_V(1);
public:
ObTabletInfoTrailer():
  ObExternBackupDataDesc(share::ObBackupFileType::BACKUP_TABLET_METAS_INFO, FILE_VERSION),
  file_id_(0), tablet_cnt_(0), offset_(0), length_(0) {}
virtual ~ObTabletInfoTrailer() {}

void reset();
bool is_valid() const override;
int assign(const ObTabletInfoTrailer &that);
TO_STRING_KV(K_(file_id), K_(tablet_cnt), K_(offset), K_(length));

int64_t file_id_;
int64_t tablet_cnt_;
int64_t offset_;
int64_t length_;
};

class ObExternTabletMetaWriter final
{
public:
  static const int64_t BUF_SIZE = 4 * 1024 * 1024;
  static const int64_t TRAILER_BUF = 1024;
  ObExternTabletMetaWriter(): is_inited_(false), backup_set_dest_(), ls_id_(), turn_id_(0), retry_id_(0),
      io_fd_(0), dev_handle_(nullptr), file_write_ctx_(), file_trailer_(), tmp_buffer_("BackupExtInfo") {};
  ~ObExternTabletMetaWriter() {};
  int init(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id,
           const int64_t turn_id, const int64_t retry_id);
  int write_meta_data(const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id);
  int close();
private:
  int write_meta_data_(const blocksstable::ObBufferReader &meta_data, const common::ObTabletID &tablet_id);
  int prepare_backup_file_(const int64_t file_id);
  int64_t get_data_file_size() const { return GCONF.backup_data_file_size; }
  bool need_switch_file_(const blocksstable::ObBufferReader &buffer);
  int switch_file_();
  int flush_trailer_();
  int write_data_align_(
      const blocksstable::ObBufferReader &buffer, const share::ObBackupFileType &type, const int64_t alignment);
private:
  bool is_inited_;
  share::ObBackupDest backup_set_dest_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObIOFd io_fd_;
  common::ObIODevice *dev_handle_;
  ObBackupFileWriteCtx file_write_ctx_;
  ObTabletInfoTrailer file_trailer_;
  blocksstable::ObSelfBufferWriter tmp_buffer_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTabletMetaWriter);
};

class ObExternTabletMetaReader final
{
public:
  ObExternTabletMetaReader(): is_inited_(false), cur_tablet_idx_(-1), cur_trailer_idx_(-1), cur_buf_offset_(-1),
                              tablet_meta_array_(), tablet_info_trailer_array_() {}
  ~ObExternTabletMetaReader() {};
  int init(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id);
  int get_next(storage::ObMigrationTabletParam &tablet_meta);
private:
  bool end_();
  int read_next_batch_();
  int read_next_range_tablet_metas_();
  int fill_tablet_info_trailer_(const share::ObBackupDest &backup_set_dest, const share::ObLSID &ls_id);
  int read_file_trailer_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, ObTabletInfoTrailer &trailer);
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  int64_t cur_tablet_idx_;
  int64_t cur_trailer_idx_;
  int64_t cur_buf_offset_;
  ObArray<storage::ObMigrationTabletParam> tablet_meta_array_;
  ObArray<ObTabletInfoTrailer> tablet_info_trailer_array_;
  int64_t retry_id_;
  int64_t turn_id_;
  share::ObBackupDest backup_set_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObExternTabletMetaReader);
};

class ObExternBackupInfoIdGetter final
{
public:
  class ObLSMetaInfoDirFilter final : public ObBaseDirEntryOperator
  {
  public:
  public:
    ObLSMetaInfoDirFilter(): turn_id_(0), retry_id_(0) {}
    ~ObLSMetaInfoDirFilter() {}
    int func(const dirent *entry) override;
    const int64_t &turn_id() const { return turn_id_; }
    const int64_t &retry_id() const { return retry_id_; }
    TO_STRING_KV(K_(turn_id), K_(retry_id));
  private:
    int64_t turn_id_;
    int64_t retry_id_;
    DISALLOW_COPY_AND_ASSIGN(ObLSMetaInfoDirFilter);
  };

  class ObLSTabletInfoIdFilter final : public ObBaseDirEntryOperator
  {
  public:
    using FileIdSet = common::hash::ObHashSet<int64_t, common::hash::NoPthreadDefendMode>;
  public:
    ObLSTabletInfoIdFilter(): is_inited_(false), file_id_set_() {}
    ~ObLSTabletInfoIdFilter() {}
    int init();
    int get_file_id_array(ObIArray<int64_t> &file_id_array);
    int func(const dirent *entry) override;
  private:
    bool is_inited_;
    FileIdSet file_id_set_;
    DISALLOW_COPY_AND_ASSIGN(ObLSTabletInfoIdFilter);
  };

public:
  ObExternBackupInfoIdGetter(): is_inited_(false), backup_set_dest_() {}
  ~ObExternBackupInfoIdGetter() {}
  int init(const share::ObBackupDest &backup_set_dest);
  int get_max_turn_id_and_retry_id(const share::ObLSID &ls_id, int64_t &turn_id, int64_t &retry_id);
  int get_tablet_info_file_ids(
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, ObIArray<int64_t> &file_id_array);
private:
  bool is_inited_;
  share::ObBackupDest backup_set_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObExternBackupInfoIdGetter);
};

}  // namespace backup
}  // namespace oceanbase

#endif
