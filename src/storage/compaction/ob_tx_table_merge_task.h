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

#ifndef STORAGE_COMPACTION_OB_TX_TABLE_MERGE_TASK_H_
#define STORAGE_COMPACTION_OB_TX_TABLE_MERGE_TASK_H_
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/compaction/ob_tablet_merge_task.h"

namespace oceanbase
{

namespace blocksstable
{
class ObSSTable;
}

namespace compaction
{
class ObTxTableMergeDag;
class ObTabletMergeCtx;

// for mini merge
class ObTxTableMergePrepareTask: public ObTabletMergePrepareTask
{
public:
  ObTxTableMergePrepareTask();
  virtual ~ObTxTableMergePrepareTask();
  int init();
private:
  int pre_process_tx_data_table_merge_(ObTabletMergeCtx &ctx);
  virtual int inner_init_ctx(ObTabletMergeCtx &ctx, bool &skip_merge_task_flag) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxTableMergePrepareTask);
};

class ObTxTableMergeDag: public ObBasicTabletMergeDag
{
public:
  ObTxTableMergeDag();
  virtual ~ObTxTableMergeDag() {}
  virtual int create_first_task() override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxTableMergeDag);
};

} // namespace compaction
} // namespace oceanbase
#endif
