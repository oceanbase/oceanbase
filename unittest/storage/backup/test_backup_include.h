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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#define private public
#define protected public

namespace oceanbase
{
namespace backup
{

int64_t max_tablet_id = 0;

/* ObFakeBackupMacroIndexMerger */

class ObFakeBackupMacroIndexMerger : public ObBackupMacroBlockIndexMerger {
public:
  ObFakeBackupMacroIndexMerger();
  virtual ~ObFakeBackupMacroIndexMerger();
  void set_count(const int64_t file_count, const int64_t per_file_item_count)
  {
    file_count_ = file_count;
    per_file_item_count_ = per_file_item_count;
  }

private:
  virtual int get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
      const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy,
      common::ObIArray<ObBackupRetryDesc> &retry_list) override;
  virtual int alloc_merge_iter_(const bool tenant_level, const ObBackupIndexMergeParam &merge_param,
      const ObBackupRetryDesc &retry_desc, ObIMacroBlockIndexIterator *&iter) override;

private:
  int64_t file_count_;
  int64_t per_file_item_count_;
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupMacroIndexMerger);
};

}
}
