/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_SSTABLE_COPY_START_TASK_
#define OCEANBASE_STORAGE_SSTABLE_COPY_START_TASK_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_i_table.h"
#include "ob_physical_copy_ctx.h"
#include "ob_sstable_copy_finish_task.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_ha_reader.h"

namespace oceanbase
{
namespace storage
{
class ObSSTableCopyStartTask: public share::ObITask
{
public:
  ObSSTableCopyStartTask();
  virtual ~ObSSTableCopyStartTask();
  int init(ObPhysicalCopyCtx* copy_ctx, ObSSTableCopyFinishTask *finish_task);
  virtual int process() override;
private:
  int do_with_shared_sstable_();
  // fetch macro block id (logical/physical) array from src (backup medium/migration src)
  int fetch_sstable_macro_id_info_(ObCopySSTableMacroIdInfo &macro_block_id_info, bool &is_rpc_not_support);
  // split id array, init macro range info
  int prepare_sstable_macro_range_info_(bool &is_rpc_not_support);
  int get_macro_id_info_reader_(ObICopySSTableMacroIdInfoReader *&reader, bool &is_rpc_not_support);
  int build_macro_id_info_reader_init_param_(ObCopySSTableMacroIdInfoReaderInitParam &init_param);
  int get_macro_id_info_ob_reader_(const ObCopySSTableMacroIdInfoReaderInitParam &init_param, ObICopySSTableMacroIdInfoReader *&reader);
  void free_macro_id_info_reader_(ObICopySSTableMacroIdInfoReader *&reader);
private:
  bool is_inited_;
  ObPhysicalCopyCtx *copy_ctx_;
  ObSSTableCopyFinishTask *finish_task_;
  ObICopySSTableMacroIdInfoReader *reader_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableCopyStartTask);
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SSTABLE_COPY_START_TASK_
