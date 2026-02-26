/*************************************************************************
  * Copyright (c) 2025 OceanBase
  * OceanBase is licensed under Mulan PubL v2.
  * You can use this software according to the terms and conditions of the Mulan PubL v2
  * You may obtain a copy of Mulan PubL v2 at:
  *          http://license.coscl.org.cn/MulanPubL-2.0
  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
  * See the Mulan PubL v2 for more details.
  * File Name   : ob_old_row_check_dumper.h
  * Created  on : 04/27/2025
 ************************************************************************/
#ifndef _OB_OLD_ROW_CHECK_DUMPER_H
#define _OB_OLD_ROW_CHECK_DUMPER_H
#include "ob_table_access_context.h"
#include "storage/ob_dml_running_ctx.h"

namespace oceanbase
{
namespace storage
{

class ObOldRowCheckDumper
{
public:
  ObOldRowCheckDumper(ObDMLRunningCtx &run_ctx, const blocksstable::ObDatumRow &datum_row);
  ~ObOldRowCheckDumper() = default;
  int dump_diag_log();
private:
  int prepare_read_ctx();
  int dump_diag_tables();
  int dump_diag_merge();
  storage::ObDMLRunningCtx &run_ctx_;
  const blocksstable::ObDatumRow &datum_row_;
  common::ObSEArray<int32_t, 16> out_col_pros_;
  ObArenaAllocator allocator_;
  blocksstable::ObDatumRowkeyHelper rowkey_helper_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_ctx_;
  ObDatumRowkey datum_rowkey_;
};

}
}

#endif
