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

#ifndef OB_ADMIN_DUMP_CKPT_EXECUTOR_H_
#define OB_ADMIN_DUMP_CKPT_EXECUTOR_H_

#include "../ob_admin_executor.h"
#include "observer/omt/ob_tenant_meta.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
namespace oceanbase
{
namespace tools
{
class ObAdminDumpCkptExecutor : public ObAdminExecutor
{
public:
  ObAdminDumpCkptExecutor();
  virtual ~ObAdminDumpCkptExecutor() = default;
  virtual int execute(int argc, char *argv[]);
  void reset();


private:
  enum class ObCheckpointType
  {
    TENANT_META = 0,
    LS_META,
    TABLET,
    MAX_TYPE_NUM
  };
  static const char *get_checkpoint_meta_type_str(const ObCheckpointType type);

  int get_tenant_meta_from_ckpt(const uint64_t tenant_id, common::ObIArray<omt::ObTenantMeta> &metas);
  int dump_tenant_metas(common::ObIArray<omt::ObTenantMeta> &metas, FILE *stream);
  int dump_all_ls_metas_from_ckpt(const blocksstable::MacroBlockId &entry_block, FILE *stream);
  int dump_ls_meta(const storage::ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len, FILE *stream);
  int dump_all_tablets_from_ckpt(const blocksstable::MacroBlockId &entry_block, FILE *stream);
  int dump_tablet(const storage::ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len, FILE *stream);
  int parse_args(int argc, char *argv[]);
  void print_usage();
  int dump_server_ckpt(FILE *stream);
  int dump_tenant_ckpt(FILE *stream);

private:
  static const char *checkpoint_meta_type_strs_[];
  uint64_t tenant_id_;
  ObCheckpointType meta_type_;

};

}
}

#endif
