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

#ifndef OCEANBASE_ROOTSERVER_OB_TRANSFER_PARTITION_TASK_H
#define OCEANBASE_ROOTSERVER_OB_TRANSFER_PARTITION_TASK_H

#include "share/balance/ob_balance_task_table_operator.h"//ObBalanceTask
#include "share/balance/ob_balance_job_table_operator.h"//ObBalanceJob
#include "share/balance/ob_transfer_partition_task_table_operator.h"//ObTransferPartitionTask
#include "lib/container/ob_array.h"//ObArray
#include "lib/allocator/page_arena.h"//allocator
#include "rootserver/ob_partition_balance.h"//logical task

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace share
{
struct ObBalanceJob;
namespace schema{
class ObSimpleTableSchemaV2;
}
}
namespace rootserver
{
struct ObTransferPartitionInfo
{
public:
  ObTransferPartitionInfo() : task_(NULL), tablet_id_(), src_ls_(){}
  ~ObTransferPartitionInfo() {}
  int init(share::ObTransferPartitionTask &task,
         const ObTabletID &tablet_id);
  int set_src_ls(const ObLSID &ls_id);
  int assign(const ObTransferPartitionInfo &other);
  bool is_valid() const
  {
    return OB_NOT_NULL(task_) && task_->is_valid()
      && tablet_id_.is_valid() && src_ls_.is_valid();
  }
  const ObTabletID& get_tablet_id() const
  {
    return tablet_id_;
  }
  const ObLSID& get_ls_id() const
  {
    return src_ls_;
  }
  const share::ObTransferPartitionTask* get_task() const
  {
    return task_;
  }
  TO_STRING_KV(KPC_(task), K_(tablet_id), K_(src_ls));
private:
  share::ObTransferPartitionTask* task_;
  ObTabletID tablet_id_;
  ObLSID src_ls_;
};

class ObTransferPartitionHelper
{
public:
  ObTransferPartitionHelper(const uint64_t tenant_id,
      common::ObMySQLProxy *sql_proxy) :
    is_inited_(false), tenant_id_(tenant_id), sql_proxy_(sql_proxy),
    allocator_("TRANFER_PART", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id),
    task_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_, "TrPTaskArray")),
    part_info_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_, "PartInfoArray")),
    transfer_logical_tasks_(), balance_job_(),
    balance_tasks_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator_, "BalanceArray")),
    max_task_id_() {}
  ~ObTransferPartitionHelper()
  {
    destroy();
  }
  void destroy();

  share::ObBalanceJob& get_balance_job()
  {
    return balance_job_;
  }
  ObArray<share::ObBalanceTask>& get_balance_tasks()
  {
    return balance_tasks_;
  }
  //构造好每个任务所需要的源端日志流信息
  int build(bool &has_job);
  //加锁构造逻辑任务和物理任务并写入表中
  int process_in_trans(const share::ObLSStatusInfoIArray &status_info_array,
      int64_t unit_num, int64_t primary_zone_num,
      ObMySQLTransaction &trans);
  /*
  * 从按table_id递增顺序数组（table_schema_array）中查找指定分区的tablet_id信息。该函数可以反复调用，用于查找一组分区的tablet_id信息。
  * 使用方法:
  *     1.保证table_schema_array按table_id从小到大排序
  *     2.一组分区信息part_info，按<table_id, part_id>从小到大排序
  *     3. 初始化table_index = 0，按从小到大顺序指定part_info反复调用该函数，获取对应分区的tablet_id信息
  *
  * @param[in] table_schema_array: 按照table_id升序排列的table_schema_array
  * @param[in] part_info: 指定的part_info，table_id一定大于等于上次指定的part_info
  * @param[in/out] table_index: 当前遍历的位置，调用者只需要第一次初始化为0，后续不要修改该变量值，否则可能导致结果报错
  * @param[out] tablet_id:tablet_id of part_info
  * @return OB_SUCCESS if success
  *         OB_TABLE_NOT_EXIST :表不存在
  *         OB_PARTITION_NOT_EXIST : part_object_id不存在
  * */
  static int get_tablet_in_order_array(
      const ObArray<share::schema::ObSimpleTableSchemaV2*> &table_schema_array,
      const ObTransferPartInfo &part_info,
      int64_t &table_index,
      ObTabletID &tablet_id);
private:
  //no need check is_inited_, after rebuild, is_inited_ = true
  int check_inner_stat_();
  int try_process_dest_not_exist_task_(
      const share::ObLSStatusInfoIArray &status_info_array,
      int64_t& task_cnt);
  int try_process_object_not_exist_task_();
  int set_task_src_ls_();
  int try_finish_failed_task_(const ObTransferPartList &part_list,
      const ObString &comment);
  int construct_logical_task_(const ObArray<share::ObTransferPartitionTask> &task_array);
  static int get_ls_group_id(
      const ObLSStatusInfoIArray &status_info_array,
      const ObLSID &src_ls, const ObLSID &dest_ls,
      uint64_t &src_ls_group, uint64_t &dest_ls_group);
  //通过按照tablet_id,object_id排查的part_list或者按照table_id排查过的table_schema
 int batch_get_table_schema_in_order_(
      common::ObArenaAllocator &allocator,
      ObArray<share::schema::ObSimpleTableSchemaV2*> &table_schema_array);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObArenaAllocator allocator_;
  ObArray<share::ObTransferPartitionTask> task_array_;
  //part_info_中的task指针使用了task_array的内存
  //在初始化part_info_后，task_array不能在发生变化
  ObArray<ObTransferPartitionInfo> part_info_;
  hash::ObHashMap<ObPartitionBalance::ObTransferTaskKey, ObTransferPartList> transfer_logical_tasks_;
  ObBalanceJob balance_job_;
  ObArray<ObBalanceTask> balance_tasks_;
  share::ObTransferPartitionTaskID max_task_id_;
};
}
}

#endif /* !OB_TRANSFER_PARTITION_TASK_H */
