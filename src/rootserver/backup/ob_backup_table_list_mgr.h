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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_TABLE_LIST_MGR_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_BACKUP_TABLE_LIST_MGR_H_

#include "share/backup/ob_backup_store.h"
#include "share/backup/ob_backup_path.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "rootserver/backup/ob_backup_service.h"

namespace oceanbase
{
namespace storage
{
class ObBackupPartialTableListDesc;
}
namespace rootserver
{


class ObGetMaxTableListPartNoOp : public ObBaseDirEntryOperator
{
public:
  ObGetMaxTableListPartNoOp(
      const share::SCN &scn,
      int64_t &max_table_list_part_no)
    : scn_(scn),
      max_table_list_part_no_(max_table_list_part_no) {}
  virtual ~ObGetMaxTableListPartNoOp() {}
  bool is_valid() const;
  int func(const dirent *entry);
private:
  const share::SCN scn_;
  int64_t &max_table_list_part_no_;

  DISALLOW_COPY_AND_ASSIGN(ObGetMaxTableListPartNoOp);
};

class ObGetMaxTableListSCNOp : public ObBaseDirEntryOperator
{
public:
  ObGetMaxTableListSCNOp(share::SCN &max_scn)
    : max_scn_(max_scn) {}
  virtual ~ObGetMaxTableListSCNOp() {}
  bool is_valid() const;
  int func(const dirent *entry) override;
private:
  share::SCN &max_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObGetMaxTableListSCNOp);
};

class ObGetTableListPartialMetasOp : public ObBaseDirEntryOperator
{
public:
  ObGetTableListPartialMetasOp(
      const share::SCN &scn,
      const ObBackupDest *backup_set_dest,
      ObIArray<ObBackupPartialTableListMeta> &partial_metas)
    : scn_(scn),
      backup_set_dest_(backup_set_dest),
      partial_metas_(partial_metas) {}
  virtual ~ObGetTableListPartialMetasOp() {}
  bool is_valid() const;
  int func(const dirent *entry) override;
private:
  const share::SCN &scn_;
  const ObBackupDest *backup_set_dest_;
  ObIArray<ObBackupPartialTableListMeta> &partial_metas_;
  DISALLOW_COPY_AND_ASSIGN(ObGetTableListPartialMetasOp);
};

class ObBackupTableListMgr final
{
public:
  ObBackupTableListMgr();
  ~ObBackupTableListMgr() { reset(); }

#ifdef ERRSIM
  const int64_t BATCH_SIZE = GCONF.errsim_backup_table_list_batch_size;
#else
  static const int64_t BATCH_SIZE = 20000;
#endif
  int init(
      const uint64_t tenant_id,
      const share::SCN &snapshot_point,
      const ObBackupDest &backup_set_dest,
      rootserver::ObBackupDataService &backup_service,
      common::ObISQLClient &sql_proxy);
  int backup_table_list();
  void reset();
private:
  int backup_table_list_to_tmp_file_(int64_t &count, ObIArray<int64_t> &serialize_size_array);
  int backup_table_list_to_extern_device_(const int64_t &count, const ObIArray<int64_t> &serialize_size_array);
  int write_to_tmp_file_(const storage::ObBackupPartialTableListDesc &table_list, int64_t &serialize_size);
  int read_from_tmp_file_(const int64_t read_size, const int64_t offset, storage::ObBackupPartialTableListDesc &table_list);
  int do_backup_table_list_(const int64_t read_size, const int64_t offset, const int64_t part_no);
  int write_table_list_meta_(const int64_t total_count, const int64_t batch_size);
  int get_max_complete_file_part_no_(int64_t &part_no); // file part_no starts from 1

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::SCN snapshot_point_;
  ObBackupDest backup_set_dest_;
  backup::ObBackupTmpFile tmp_file_;
  common::ObISQLClient *sql_proxy_;
  rootserver::ObBackupDataService *backup_service_;
  ObSArray<ObBackupPartialTableListMeta> partial_metas_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTableListMgr);
};
}// rootserver
}// oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_TABLE_LIST_MGR_H_ */