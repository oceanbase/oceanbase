/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_DDL_OB_GROUP_WRTIE_MACRO_BLOCK_TASK_H_
#define _OCEANBASE_STORAGE_DDL_OB_GROUP_WRTIE_MACRO_BLOCK_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
struct ObMacroDataSeq;
}

namespace storage
{

class ObDDLIndependentDag;
struct ObDDLTabletContext;
class ObCGBlockFile;

class ObGroupWriteMacroBlockTask: public share::ObITask
{
public:
  ObGroupWriteMacroBlockTask();
  virtual ~ObGroupWriteMacroBlockTask();
  int init(ObDDLIndependentDag *ddl_dag);
  int init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id);
  int process();
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  int group_write_macro_block(const ObTabletID &tablet_id);
  int schedule_write_task(const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t cg_idx, ObIArray<ObCGBlockFile *> &group_files);
private:
  ObDDLIndependentDag *ddl_dag_;
  ObTabletID tablet_id_;
  ObArray<ObITask *> group_write_tasks_;
};

class ObGroupCGBlockFileWriteTask : public share::ObITask
{
public:
  ObGroupCGBlockFileWriteTask();
  virtual ~ObGroupCGBlockFileWriteTask();
  int init(ObDDLIndependentDag *ddl_dag, const ObTabletID &tablet_id, const int64_t slice_idx, const int64_t cg_idx, ObIArray<ObCGBlockFile *> &group_files);
  int process();
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  void reset();

private:
  bool is_inited_;
  ObDDLIndependentDag *ddl_dag_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t cg_idx_;
  ObArray<ObCGBlockFile *> block_files_;

};

} // end namespace storage
} // end namespace oceanbase

#endif//_OCEANBASE_STORAGE_DDL_OB_GROUP_WRTIE_MACRO_BLOCK_TASK_H_
