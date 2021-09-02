// Copyright 2021 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@alibaba-inc.com>

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_RESTORE_PREVIEW_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_SHOW_RESTORE_PREVIEW_H_

#include "share/ob_virtual_table_iterator.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace observer {

class ObTenantShowRestorePreview : public common::ObVirtualTableIterator {
  enum BackupType {
    BACKUP_TYPE_SET = 0,
    BACKUP_TYPE_PIECE = 1,
    BACKUP_TYPE_MAX = 2,
  };
  enum RestorePreviewColumn {
    BACKUP_TYPE = common::OB_APP_MIN_COLUMN_ID,
    BACKUP_ID = common::OB_APP_MIN_COLUMN_ID + 1,
    COPY_ID = common::OB_APP_MIN_COLUMN_ID + 2,
    PREVIEW_PATH = common::OB_APP_MIN_COLUMN_ID + 3,
    FILE_STATUS = common::OB_APP_MIN_COLUMN_ID + 4,
  };
  static const int64_t MAX_INT64_STR_LENGTH = 100;

public:
  ObTenantShowRestorePreview();
  virtual ~ObTenantShowRestorePreview();
  int init();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  int parse_tenant_id_from_session(uint64_t& tenant_id);
  int parse_cluster_id_from_session(int64_t& cluster_id);
  int parse_restore_timestamp_from_session(int64_t& timestamp);

private:
  int inner_get_next_row();
  int get_backup_type(BackupType& type);
  int get_backup_id(int64_t& backup_id);
  int get_copy_id(int64_t& copy_id);
  int get_backup_path(common::ObString& str);
  int get_file_status(common::ObString& str);

private:
  bool is_inited_;
  int64_t idx_;  // index in array
  int64_t total_cnt_;
  common::ObArray<share::ObSimpleBackupSetPath> backup_set_list_;
  common::ObArray<share::ObSimpleBackupPiecePath> backup_piece_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantShowRestorePreview);
};

}  // end namespace observer
}  // end namespace oceanbase

#endif
