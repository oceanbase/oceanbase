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

#ifndef OB_ADMIN_DUMPSST_EXECUTOR_H_
#define OB_ADMIN_DUMPSST_EXECUTOR_H_
#include "../ob_admin_executor.h"
#include "../ob_admin_common_utils.h"
#include "lib/container/ob_array.h"
#include "storage/ob_i_table.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_reload_config.h"

namespace oceanbase
{
namespace storage {
  struct ObHotTabletInfoIndex;
}
namespace tools
{

enum ObAdminDumpsstCmd
{
  DUMP_SUPER_BLOCK = 0,
  DUMP_MACRO_DATA = 1,
  PRINT_MACRO_BLOCK = 2,
  DUMP_TABLET_META = 3,
  DUMP_TABLE_STORE = 4,
  DUMP_STORAGE_SCHEMA = 5,
  DUMP_SSTABLE = 6,
  DUMP_IS_DELETED_OBJ = 7,
  DUMP_META_LIST = 8,
  DUMP_GC_INFO = 9,
  DUMP_PREWARM_INDEX = 10,
  DUMP_PREWARM_DATA = 11,
  DUMP_MAX,
};

class ObAdminDumpsstExecutor : public ObAdminExecutor
{
public:
  ObAdminDumpsstExecutor();
  virtual ~ObAdminDumpsstExecutor();
  virtual int execute(int argc, char *argv[]);
private:
  int parse_cmd(int argc, char *argv[]);
  int parse_macro_id(const char *optarg, ObDumpMacroBlockContext &context);
  void print_macro_block();
  void print_usage();
  void print_macro_meta();
  void print_super_block();
  void dump_sstable();
  void dump_sstable_meta();
  void dump_macro_block(const ObDumpMacroBlockContext &macro_block_context);
  void dump_tablet_meta(const ObDumpMacroBlockContext &macro_block_context);
  void dump_table_store(const ObDumpMacroBlockContext &macro_block_context);
  void dump_storage_schema(const ObDumpMacroBlockContext &macro_block_context);
#ifdef OB_BUILD_SHARED_STORAGE
  void dump_prewarm_index(const ObDumpMacroBlockContext &macro_block_context);
  void dump_prewarm_data(const ObDumpMacroBlockContext &macro_block_context);
  void dump_is_deleted_obj(const ObDumpMacroBlockContext &macro_block_context);
  void dump_meta_list(const ObDumpMacroBlockContext &macro_block_context);
  void dump_gc_info(const ObDumpMacroBlockContext &macro_block_context);
  int do_dump_prewarm_index(const char *path, storage::ObHotTabletInfoIndex &index);
#endif

#ifdef OB_BUILD_TDE_SECURITY
  int init_master_key_getter();
#endif

  bool is_quiet_;
  ObAdminDumpsstCmd cmd_;
  bool hex_print_;
  ObDumpMacroBlockContext dump_macro_context_;
  char *key_hex_str_;
  int64_t master_key_id_;
  common::ObArenaAllocator io_allocator_;
};

} //namespace tools
} //namespace oceanbase

#endif /* OB_ADMIN_DUMPSST_EXECUTOR_H_ */
