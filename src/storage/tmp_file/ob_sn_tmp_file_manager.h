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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_i_tmp_file_manager.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"

namespace oceanbase
{
namespace tmp_file
{
class ObSNTenantTmpFileManager : public ObITenantTmpFileManager
{
public:
  ObSNTenantTmpFileManager();
  virtual ~ObSNTenantTmpFileManager();

public:
  virtual int alloc_dir(int64_t &dir_id) override;
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label) override;
  int get_tmp_file(const int64_t fd, ObSNTmpFileHandle &file_handle) const;
  int get_macro_block_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list);
  virtual int get_tmp_file_disk_usage(int64_t &disk_data_size, int64_t &occupied_disk_size) override;
  virtual int get_suggested_max_tmp_file_num(int64_t& suggested_max_tmp_file_num,
                const int64_t write_cache_size_expected_reside_in_memory) override;
  OB_INLINE ObTmpFileBlockManager &get_tmp_file_block_manager() { return tmp_file_block_manager_; }
  OB_INLINE ObTmpFileWriteCache &get_write_cache() { return write_cache_; }
private:
  virtual int init_sub_module_() override;
  virtual int start_sub_module_() override;
  virtual int stop_sub_module_() override;
  virtual int wait_sub_module_() override;
  virtual int destroy_sub_module_() override;

private:
  ObTmpFileWriteCache write_cache_;
  ObTmpFileBlockManager tmp_file_block_manager_;

  static int64_t current_fd_;
  static int64_t current_dir_id_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SN_TMP_FILE_MANAGER_H_
