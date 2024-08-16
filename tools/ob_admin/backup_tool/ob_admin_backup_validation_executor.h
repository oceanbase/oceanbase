/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OB_ADMIN_BACKUP_VALIDATION_EXECUTOR_H_
#define OB_ADMIN_BACKUP_VALIDATION_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"

namespace oceanbase
{
namespace tools
{

enum class ObAdminBackupValidationType {
  DATABASE_VALIDATION,
  BACKUPSET_VALIDATION,
  BACKUPPIECE_VALIDATION,
  MAX_VALIDATION
};
struct ObAdminBackupValidationCtx;
class ObAdminBackupValidationExecutor final : public ObAdminExecutor
{
public:
  ObAdminBackupValidationExecutor(ObAdminBackupValidationType validation_type);
  virtual ~ObAdminBackupValidationExecutor();
  virtual int execute(int argc, char *argv[]) override;
  static const int64_t MAX_TABLET_BATCH_COUNT = 512;      // for one dag
  static const int64_t MAX_MACRO_BLOCK_BATCH_COUNT = 128; // for one task
  static const int64_t DEFAULT_BACKUP_SET_BUCKET_NUM = 16;
  static const int64_t DEFAULT_BACKUP_PIECE_BUCKET_NUM = 64;
  static const int64_t DEFAULT_BACKUP_LS_BUCKET_NUM = 16;
  static const int64_t DEFAULT_BACKUP_TABLET_BUCKET_NUM = 4096;

private:
  int parse_cmd_(int argc, char *argv[]);
  int print_usage_();
  int init_();
  int schedule_log_archive_validation_();
  int wait_log_archive_validation_();
  int schedule_data_backup_validation_();
  int wait_data_backup_validation_();

private:
  int is_inited_;
  ObAdminBackupValidationType validation_type_;
  ObAdminBackupValidationCtx *ctx_;
  common::ObArenaAllocator allocator_;
};

}; // namespace tools
}; // namespace oceanbase

#endif
