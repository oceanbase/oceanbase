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

#ifndef OCEANBASE_STORAGE_DDL_MERGE_TASK_V2_
#define OCEANBASE_STORAGE_DDL_MERGE_TASK_V2_

#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_ddl_merge_helper.h"

namespace oceanbase
{
namespace storage
{
class ObIDDLMergeHelper;
/*
new ddl merge task sequence can be describle as the following graph
for  a data tablet, it has three parts, prepare , merge_cg & assemble
at the same time data tablet should also control the dependency on lob tablet
                +--------------------+
                | prepare_for_merge()|
                +--------------------+
                              |
            +-----------------+----------------------+
            |                 |                      |
            v                 v                      |
+-------------------+  +-------------------+         |
| merge_slice_cg()  |  | merge_slice_cg()  |         |
+-------------------+  +-------------------+         |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          |                   |                      |
          +-------------------+                      |
                              |                      |
                              |                      |
                              v                      |
                      +-------------------+          |
                      | assemble_task()   |          |
                      +-------------------+          |
                              |                      |
                              +----------------------+
                              |
                              |
                              v
                      +-------------------+
                      | guard_task()      |
                      +-------------------+



two major class are build to fullfil the dag progress
1. Task  Class，which mainly control dependecy relationship, including Prepare Task， MergeSlice Task， Assemble Task
   Guard Task is a task that used for holding tablet merge guard
2. Heper Class, which real execute those actions, since too many diffrent type need to be supported
*/
class ObDDLMergeGuardTask: public share::ObITask
{
public:
  ObDDLMergeGuardTask(): ObITask(ObITaskType::TASK_TYPE_DDL_MERGE_GUARD), tablet_id_(), is_inited_(false) {}
  ~ObDDLMergeGuardTask();
  int init(const bool for_replay, const ObTabletID &tablet_id);
  int process();
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  INHERIT_TO_STRING_KV("MergeGuardTask", share::ObITask, K(tablet_id_));
public:
  ObTabletID tablet_id_;
  bool is_inited_;
};
class ObDDLMergePrepareTask: public share::ObITask
{
public:
  ObDDLMergePrepareTask();
  ~ObDDLMergePrepareTask();

  int init(const ObDDLTabletMergeDagParamV2 &merge_param);
  int inner_process();
  virtual int process() override;
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  INHERIT_TO_STRING_KV("MergePrepareTask", share::ObITask, K(merge_param_), KP(guard_task_), K(is_inited_));
private:
  ObArenaAllocator allocator_;
  ObDDLTabletMergeDagParamV2 merge_param_;
  ObDDLMergeGuardTask *guard_task_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDDLMergePrepareTask);
};

class ObDDLScheduleAnotherMergeTask: public share::ObITask
{
public:
  ObDDLScheduleAnotherMergeTask() : ObITask(ObITaskType::TASK_TYPE_DDL_SCHEDULE_ANOTHER_MERGE), ls_id_(), tablet_id_(), is_inited_(false) {}
  int init(const share::ObLSID &ls_id, const ObTabletID &tablet_id);
  int process() override;
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  INHERIT_TO_STRING_KV("ScheduleAnotherMergeTask", share::ObITask, K(ls_id_), K(tablet_id_), K(is_inited_));
private:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  bool is_inited_;
};

class ObDDLMergeCgSliceTask: public share::ObITask
{
public:
  ObDDLMergeCgSliceTask();
  int init(const ObDDLTabletMergeDagParamV2 &ddl_merge_param,
           const int64_t cg_idx,
           const int64_t start_slice_idx,
           const int64_t end_slice_idx);
  virtual int process() override;
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  INHERIT_TO_STRING_KV("MergeCgSliceTask", share::ObITask, K(merge_param_), K(cg_idx_), K(start_slice_idx_), K(is_inited_));
private:
  ObDDLTabletMergeDagParamV2 merge_param_;
  int64_t cg_idx_;
  int64_t start_slice_idx_;
  int64_t end_slice_idx_;
  bool is_inited_;
};

class ObDDLMergeAssembleTask: public share::ObITask
{
public:
  ObDDLMergeAssembleTask();
  int init(const ObDDLTabletMergeDagParamV2 &ddl_merge_param);
  int process() override;
  virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override;
  INHERIT_TO_STRING_KV("MergeAssembleTask", share::ObITask, K(merge_param_), K(is_inited_));
private:
  ObDDLTabletMergeDagParamV2 merge_param_;
  bool is_inited_;
};
} // namespace storage
} // namespace oceanbase

#endif