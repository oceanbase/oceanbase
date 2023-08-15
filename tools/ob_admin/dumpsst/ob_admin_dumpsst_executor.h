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
#include "lib/container/ob_array.h"
#include "storage/ob_i_table.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_reload_config.h"

namespace oceanbase
{
namespace tools
{

enum ObAdminDumpsstCmd
{
  DUMP_SUPER_BLOCK,
  DUMP_MACRO_DATA,
  PRINT_MACRO_BLOCK,
  DUMP_MAX,
};

struct ObDumpMacroBlockContext final
{
public:
  ObDumpMacroBlockContext()
    : first_id_(-1), second_id_(-1), micro_id_(-1), tablet_id_(0), scn_(-1)
  {}
  ~ObDumpMacroBlockContext() = default;
  bool is_valid() const { return second_id_ >= 0; }
  TO_STRING_KV(K(first_id_), K(second_id_), K(micro_id_), K_(tablet_id), K_(scn));
  uint64_t first_id_;
  int64_t second_id_;
  int64_t micro_id_;
  uint64_t tablet_id_;
  int64_t scn_;
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
  int dump_macro_block(const ObDumpMacroBlockContext &context);
  int dump_single_macro_block(const char* buf, const int64_t size);
  int dump_shared_macro_block(const char* buf, const int64_t size);
  void dump_sstable();
  void dump_sstable_meta();
#ifdef OB_BUILD_TDE_SECURITY
  int init_master_key_getter();
#endif

  bool is_quiet_;
  ObAdminDumpsstCmd cmd_;
  bool hex_print_;
  ObDumpMacroBlockContext dump_macro_context_;
  char *key_hex_str_;
  int64_t master_key_id_;
};

} //namespace tools
} //namespace oceanbase

#endif /* OB_ADMIN_DUMPSST_EXECUTOR_H_ */
