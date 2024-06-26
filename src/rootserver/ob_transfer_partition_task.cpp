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
#define USING_LOG_PREFIX BALANCE

#include "rootserver/ob_transfer_partition_task.h"
#include "rootserver/ob_bootstrap.h"
#include "rootserver/ob_tenant_balance_service.h"//gather_ls_status
#include "share/schema/ob_schema_utils.h"//batch_get_latest_table_schemas
#include "share/tablet/ob_tablet_info.h" //ObTabletTablePair/ObTabletLSPair
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusInfo
#include "share/transfer/ob_transfer_info.h" //ObTransferPartList
#include "share/tablet/ob_tablet_to_ls_iterator.h"//batch_get_tablet_to_ls
#include "storage/ob_common_id_utils.h" //gen_unique_id


#define ISTAT(fmt, args...) FLOG_INFO("[TRANSFER_PARTITION] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[TRANSFER_PARTITION] " fmt, ##args)

namespace oceanbase
{
using namespace share;
namespace rootserver
{

int ObTransferPartitionInfo::init(share::ObTransferPartitionTask &task,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    task_ = &task;
  }
  return ret;
}

int ObTransferPartitionInfo::set_src_ls(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() )) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", KR(ret), K(ls_id));
  } else if (OB_ISNULL(task_)) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("task is null", KR(ret), KP(task_));
  } else {
    src_ls_ = ls_id;

  }
  return ret;
}

int ObTransferPartitionInfo::assign(const ObTransferPartitionInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    task_ = other.task_;
    tablet_id_ = other.tablet_id_;
    src_ls_ = other.src_ls_;
  }
  return ret;
}

void ObTransferPartitionHelper::destroy()
{
  is_inited_ = false;
  sql_proxy_ = NULL;
  //part_info的内存依赖于task_array，要先于task_array释放
  part_info_.reset();
  task_array_.reset();
  transfer_logical_tasks_.destroy();
  balance_tasks_.reset();
  allocator_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  balance_job_.reset();
  max_task_id_.reset();
}
int ObTransferPartitionHelper::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  }
  return ret;
}

int ObTransferPartitionHelper::build(bool &has_job)
{
  int ret = OB_SUCCESS;
  has_job = true;
  int64_t task_cnt = 0;
  ObLSStatusInfoArray status_info_array;
  ObTransferPartitionTaskID tmp_max_task_id(INT64_MAX);//第一次获取所有的任务列表，设置了一个最大值
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::load_all_wait_task_in_part_info_order(
                             tenant_id_, false, tmp_max_task_id, task_array_,
                             *sql_proxy_))) {
    LOG_WARN("failed to load all wait task", KR(ret), K(tenant_id_));
  } else if (0 == task_array_.count()) {
    has_job = false;
    ISTAT("no transfer partition task", K(tenant_id_));
    //防止出现load_ls_status成功很久后，才load_task成功，导致出现一些日志流不存在的误判
    //所以先load任务，然后在获取ls_status，保证用户看到日志流存在后再去生成的任务一定不会
    //被判断称目标的不存在
  } else if (OB_FAIL(ObTenantBalanceService::gather_ls_status_stat(
                 tenant_id_, status_info_array))) {
    LOG_WARN("failed to gather ls status stat", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(try_process_dest_not_exist_task_(status_info_array,
                                                      task_cnt))) {
    LOG_WARN("failed to process dest not exist task", KR(ret), K(status_info_array));
    //由于一部分任务不能成功执行，重新load，防止后面的程序还需要处理这部分非法的任务不合理
  } else if (!max_task_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max task id is invalid", KR(ret), K(max_task_id_));
  } else if (0 != task_cnt
             // 这次load的范围不能超过上一次的范围
             && OB_FAIL(ObTransferPartitionTaskTableOperator::
                            load_all_wait_task_in_part_info_order(
                                tenant_id_, false, max_task_id_, task_array_,
                                *sql_proxy_))) {
    LOG_WARN("failed to load all wait task", KR(ret), K(tenant_id_), K(max_task_id_));
  } else if (0 == task_array_.count()) {
    has_job = false;
    ISTAT("no transfer partition task", K(tenant_id_));
    //检查分区是否存在，不需要重新reload，分区不存在不会被放在part_info_中
  } else if (OB_FAIL(try_process_object_not_exist_task_())) {
    LOG_WARN("failed to process object not exist task", KR(ret));
  } else if (0 == part_info_.count()) {
    has_job = false;
    ISTAT("no transfer partition task", K(tenant_id_), K(task_array_));
    //根据part_info_中的task设置源端
  } else if (OB_FAIL(set_task_src_ls_())) {
    LOG_WARN("failed to construct task info", KR(ret), K(status_info_array));
  } else {
    has_job = true;
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObTransferPartitionHelper::try_process_dest_not_exist_task_(
    const share::ObLSStatusInfoIArray &status_info_array,
    int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  task_cnt = 0;
  ObTransferPartList dest_not_exist_list;
  ObTransferPartList dest_not_valid_list;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (0 >= status_info_array.count() || 0 >= task_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status info array is null", KR(ret), K(status_info_array), K(task_array_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      const ObTransferPartitionTask &task = task_array_.at(i);
      if (!max_task_id_.is_valid() || max_task_id_ < task.get_task_id()) {
        max_task_id_ = task.get_task_id();
      }

      bool found = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < status_info_array.count() && !found; ++j) {
        const ObLSStatusInfo &ls_info = status_info_array.at(j);
        if (task.get_dest_ls() == ls_info.get_ls_id()) {
          found = true;
          if (ls_info.ls_is_normal() && !ls_info.ls_is_block_tablet_in()) {
            //the dest ls is valid
          } else if (OB_FAIL(dest_not_valid_list.push_back(task.get_part_info()))) {
            LOG_WARN("failed to push back", KR(ret), K(task));
          } else {
            ISTAT("dest ls is not valid to transfer in", K(task), K(ls_info));
          }
        }
      }//end for check ls is valid
      if (OB_SUCC(ret) && !found) {
        ISTAT("dest ls is not exist to transfer in", K(task));
        if (OB_FAIL(dest_not_exist_list.push_back(task.get_part_info()))) {
          LOG_WARN("failed to push back", KR(ret), K(task));
        }
      }
    }//end for check all task
    if (OB_SUCC(ret) && dest_not_exist_list.count() > 0) {
      ObString comment("LS not exist or may be in DROPPING/WAIT_OFFLINE status");
      if (OB_FAIL(try_finish_failed_task_(dest_not_exist_list, comment))) {
        LOG_WARN("failed to finish failed task", KR(ret), K(dest_not_exist_list));
      }
    }
    if (OB_SUCC(ret) && dest_not_valid_list.count() > 0) {
      ObString comment("LS status is not NORMAL or is in BLOCK_TABLET_IN state");
      if (OB_FAIL(try_finish_failed_task_(dest_not_valid_list, comment))) {
        LOG_WARN("failed to finish failed task", KR(ret), K(dest_not_valid_list));
      }
    }
  }
  task_cnt = dest_not_exist_list.count() + dest_not_valid_list.count();
  ISTAT("finish check dest LS", KR(ret), K(task_cnt), K(dest_not_exist_list),
      K(dest_not_valid_list));
  return ret;
}

int ObTransferPartitionHelper::try_finish_failed_task_(const ObTransferPartList &part_list,
    const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (0 >= part_list.count() || comment.empty() || !max_task_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_list), K(comment), K(max_task_id_));
  } else {
    ObTransferPartitionTaskStatus status = ObTransferPartitionTaskStatus::TRP_TASK_STATUS_FAILED;
    START_TRANSACTION(sql_proxy_, tenant_id_)
    if (FAILEDx(ObTransferPartitionTaskTableOperator::finish_task(
            tenant_id_, part_list, max_task_id_, status, comment, trans))) {
      LOG_WARN("failed to finish task", KR(ret), K(tenant_id_), K(part_list),
      K(max_task_id_));
    }
    END_TRANSACTION(trans)
  }
  return ret;
}


int ObTransferPartitionHelper::try_process_object_not_exist_task_()
{
  int ret = OB_SUCCESS;
  ObTransferPartList table_not_exist_list;
  ObTransferPartList part_not_exist_list;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (0 >= task_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task array is null", KR(ret), K(task_array_));
  } else {
    common::ObArenaAllocator tmp_allocator("SCHEMA_ARRAY", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObArray<ObSimpleTableSchemaV2*> table_schema_array;
    int64_t table_index = 0;
    ObTabletID tablet_id;
    if (OB_FAIL(batch_get_table_schema_in_order_(
            tmp_allocator, table_schema_array))) {
      LOG_WARN("failed to construct table schema", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      ObTransferPartitionTask &task = task_array_.at(i);
      if (OB_FAIL(get_tablet_in_order_array(table_schema_array,
              task.get_part_info(), table_index, tablet_id))) {
        if (OB_TABLE_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          ISTAT("table is no exist", K(task), K(table_index));
          if (OB_FAIL(table_not_exist_list.push_back(task.get_part_info()))) {
            LOG_WARN("failed to push back", KR(ret), K(task));
          }
        } else if (OB_PARTITION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          ISTAT("part is no exist", K(task));
          if (OB_FAIL(part_not_exist_list.push_back(task.get_part_info()))) {
            LOG_WARN("failed to push back", KR(ret), K(task));
          }
        } else {
          LOG_WARN("failed to get tablet", KR(ret), K(task));
        }
      } else {
        ObTransferPartitionInfo info;
        if (OB_FAIL(info.init(task, tablet_id))) {
          LOG_WARN("failed to init partition info", KR(ret), K(task), K(tablet_id));
        } else if (OB_FAIL(part_info_.push_back(info))) {
          LOG_WARN("failed to push back", KR(ret), K(info));
        }
      }
    }//end for get tablet_id
    if (OB_SUCC(ret) && table_not_exist_list.count() > 0) {
      ObString comment("Table has beed dropped");
      if (OB_FAIL(try_finish_failed_task_(table_not_exist_list, comment))) {
        LOG_WARN("failed to finish failed task", KR(ret), K(table_not_exist_list));
      }
    }
    if (OB_SUCC(ret) && part_not_exist_list.count() > 0) {
      ObString comment("Partition has beed dropped");
      if (OB_FAIL(try_finish_failed_task_(part_not_exist_list, comment))) {
        LOG_WARN("failed to finish failed task", KR(ret), K(part_not_exist_list));
      }
    }
  }
  ISTAT("finish check object exist", KR(ret), K(table_not_exist_list),
      K(part_not_exist_list));

  return ret;
}

int ObTransferPartitionHelper::batch_get_table_schema_in_order_(
    common::ObArenaAllocator &allocator,
    ObArray<ObSimpleTableSchemaV2*> &table_schema_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (0 >= task_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task array is null", KR(ret), K(task_array_));
  } else {
    ObArray<ObObjectID> table_id_array;
    ObObjectID last_table_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array_.count(); ++i) {
      const ObTransferPartitionTask &task = task_array_.at(i);
      if (OB_INVALID_ID == last_table_id
          || last_table_id != task.get_part_info().table_id()) {
        last_table_id = task.get_part_info().table_id();
        if (OB_FAIL(table_id_array.push_back(last_table_id)))	{
          LOG_WARN("failed to push back", KR(ret), K(last_table_id));
        }
      }
    }//end for get table_id array
    if (FAILEDx(ObSchemaUtils::batch_get_latest_table_schemas(
            *sql_proxy_, allocator, tenant_id_, table_id_array, table_schema_array))) {
      LOG_WARN("failed to get latest table schema", KR(ret),
          K(tenant_id_), K(table_id_array));
    } else {
      //按照table_id的顺序对table_schema_array进行排序
      ObBootstrap::TableIdCompare compare;
      lib::ob_sort(table_schema_array.begin(), table_schema_array.end(), compare);
      if (OB_FAIL(compare.get_ret())) {
        LOG_WARN("failed to sort table schema", KR(ret), K(table_schema_array));
      }
    }
  }
  return ret;
}
//table_schema_array maybe empty, table maybe dropped
int ObTransferPartitionHelper::get_tablet_in_order_array(
      const ObArray<ObSimpleTableSchemaV2*> &table_schema_array,
      const ObTransferPartInfo &part_info,
      int64_t &table_index, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!part_info.is_valid() || 0 > table_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_info), K(table_schema_array));
  } else {
    const uint64_t table_id = part_info.table_id();
    while(OB_SUCC(ret) && table_index < table_schema_array.count()) {
      const ObSimpleTableSchemaV2 * table_schema = table_schema_array.at(table_index);
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(table_index));
      } else if (table_schema->get_table_id() < table_id) {
        table_index++;
      } else if (table_schema->get_table_id() == table_id) {
        if (OB_FAIL(table_schema->get_tablet_id_by_object_id(
              part_info.part_object_id(), tablet_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_PARTITION_NOT_EXIST;
            LOG_WARN("partition not exist", KR(ret), K(part_info), KPC(table_schema));
          } else {
            LOG_WARN("failed to get tablet id from object_id", KR(ret),
                K(part_info), KPC(table_schema));
          }
        }
        break;
      } else {
        //table_id < table_schema->get_table_id()
        //the table is delete
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", KR(ret), K(part_info), K(table_index), KPC(table_schema));
      }
    }//end for get tablet for task
    if (OB_SUCC(ret) && table_index >= table_schema_array.count()) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(table_index), K(table_schema_array));
    }
  }
  return ret;
}


int ObTransferPartitionHelper::set_task_src_ls_()
{
  int ret = OB_SUCCESS;
  ObTransferPartList dest_src_equal_list;
  if (IS_INIT || 0 == part_info_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(is_inited_), K(part_info_));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else {
    ObArray<ObTabletID> tablet_array;
    ObArray<ObLSID> ls_id_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_info_.count(); ++i) {
      const ObTransferPartitionInfo &info = part_info_.at(i);
      if (OB_FAIL(tablet_array.push_back(info.get_tablet_id()))) {
        LOG_WARN("failed to push back", KR(ret), K(info));
      }
    }  // end for get all tablet
    if (FAILEDx(ObTabletToLSTableOperator::batch_get_ls(
            *sql_proxy_, tenant_id_, tablet_array, ls_id_array))) {
      if (OB_ITEM_NOT_MATCH == ret) {
        WSTAT("has partition beed dropped, try again", KR(ret),
        K(tablet_array), K(ls_id_array));
      } else {
        LOG_WARN("failed to batch get", KR(ret), K(tenant_id_), K(tablet_array));
      }
    } else if (tablet_array.count() != ls_id_array.count() ||
        tablet_array.count() != part_info_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet and ls count not match", KR(ret), "tablet_array",
          tablet_array.count(), "ls count", ls_id_array.count(),
          "task info count", part_info_.count(), K(tablet_array),
          K(ls_id_array));
    }
    //tablet_to_ls_infos has same order of tablet_array
    for (int64_t i = 0; OB_SUCC(ret) && i < part_info_.count(); ++i) {
      ObTransferPartitionInfo &info = part_info_.at(i);
      const ObLSID &ls_id = ls_id_array.at(i);
      if (OB_FAIL(info.set_src_ls(ls_id))) {
        LOG_WARN("failed to set to ls", KR(ret), K(ls_id), K(info));
      } else if (OB_ISNULL(info.get_task())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info is invalid", KR(ret), K(info));
      } else if (info.get_task()->get_dest_ls() == ls_id) {
        //tranfer_partition task dest_ls is equal to src ls, no need transfer
        ISTAT("partition already in dest LS", K(info), K(ls_id));
        if (OB_FAIL(dest_src_equal_list.push_back(info.get_task()->get_part_info()))) {
          LOG_WARN("push back error", KR(ret), K(info));
        }
      }
    }//end for
    if (OB_SUCC(ret) && dest_src_equal_list.count() > 0) {
      ObString comment("Partition is already in dest LS");
      if (OB_FAIL(try_finish_failed_task_(dest_src_equal_list, comment))) {
        LOG_WARN("failed to finish failed task", KR(ret), K(dest_src_equal_list));
      }
    }
  }
  ISTAT("finish set src ls", KR(ret), K(dest_src_equal_list));
  return ret;
}

int ObTransferPartitionHelper::process_in_trans(
    const share::ObLSStatusInfoIArray &status_info_array,
    int64_t unit_num, int64_t primary_zone_num,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObTransferPartitionTask> task_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(0 >= status_info_array.count()
        || OB_INVALID_ID == unit_num || OB_INVALID_ID == primary_zone_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_num), K(primary_zone_num),
        K(status_info_array));
  } else if (OB_FAIL(ObTransferPartitionTaskTableOperator::load_all_wait_task_in_part_info_order(
          tenant_id_, true, max_task_id_, task_array, trans))) {
    LOG_WARN("failed to load all task", KR(ret), K(tenant_id_), K(max_task_id_));
  } else if (0 == task_array.count()) {
    //no task
  } else if (OB_FAIL(construct_logical_task_(task_array))) {
    LOG_WARN("failed to construct logical task", KR(ret), K(task_array));
  } else {
    ObBalanceJobID job_id;
    ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
    ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_TRANSFER_PARTITION);
    const char* balance_stradegy = "manual transfer partition"; // TODO
    ObString comment;

    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, job_id))) {
      LOG_WARN("gen_unique_id", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(balance_job_.init(tenant_id_, job_id, job_type, job_status,
            primary_zone_num, unit_num,
            comment, ObString(balance_stradegy)))) {
      LOG_WARN("job init fail", KR(ret), K(tenant_id_), K(job_id),
          K(primary_zone_num), K(unit_num));
    } else {
      hash::ObHashMap<ObPartitionBalance::ObTransferTaskKey, ObTransferPartList>::iterator
        iter = transfer_logical_tasks_.begin();
      for (; OB_SUCC(ret) && iter != transfer_logical_tasks_.end(); ++iter) {
        ObLSID src_ls = iter->first.get_src_ls_id();
        ObLSID dest_ls = iter->first.get_dest_ls_id();
        uint64_t src_ls_group = 0;
        uint64_t dest_ls_group = 0;
        if (OB_FAIL(get_ls_group_id(status_info_array, src_ls, dest_ls,
                src_ls_group, dest_ls_group))) {
          LOG_WARN("failed to get ls group", KR(ret), K(status_info_array),
              K(src_ls), K(dest_ls));
        } else if (OB_FAIL(ObPartitionBalance::transfer_logical_task_to_balance_task(
                tenant_id_, balance_job_.get_job_id(), src_ls, dest_ls,
                src_ls_group, dest_ls_group, iter->second,
                balance_tasks_))) {
          LOG_WARN("failed to transfer logical task", KR(ret), K(tenant_id_), K(balance_job_),
              K(src_ls), K(dest_ls), K(iter->second));
        }
      }//end for
    }
    if (FAILEDx(ObTransferPartitionTaskTableOperator::set_all_tasks_schedule(
            tenant_id_, max_task_id_, balance_job_.get_job_id(), task_array.count(), trans))) {
      LOG_WARN("failed to set all tasks to schedule", KR(ret), K(tenant_id_), K(max_task_id_),
          K(balance_job_), "count", task_array.count());
    }
  }
  return ret;
}

int ObTransferPartitionHelper::construct_logical_task_(
    const ObArray<share::ObTransferPartitionTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (task_array.count() > part_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task array can not larger than part info", KR(ret),
        "part_info count", part_info_.count(), "task_array count", task_array.count(),
        K(part_info_), K(task_array));
  } else if (OB_FAIL(transfer_logical_tasks_.create(1024,
                      lib::ObLabel("Trans_Part"), lib::ObLabel("Trans_Part"),
                      tenant_id_))) {
    LOG_WARN("map create fail", KR(ret), K(tenant_id_));
  } else {
    //task_array and part_info is order by table_id, object_id
    int64_t part_info_index = 0;
    bool found = true;
    //每个task_array肯定都可以在part_info_中找到
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      const ObTransferPartitionTask &task = task_array.at(i);
      found = false;
      for (; OB_SUCC(ret) && !found && part_info_index < part_info_.count(); ++part_info_index) {
        const ObTransferPartitionInfo &part_info = part_info_.at(part_info_index);
        if (OB_ISNULL(part_info.get_task())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task is null in part_info", KR(ret), K(part_info));
        } else if (task.get_part_info() == part_info.get_task()->get_part_info()) {
          if (task.get_task_id() != part_info.get_task()->get_task_id()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("same transfer partition task has different task id", KR(ret),
                      K(task), K(part_info));
          } else {
            found = true;
            ObPartitionBalance::ObTransferTaskKey key(part_info.get_ls_id(),
                                                      task.get_dest_ls());
            ObTransferPartList *tansfer_part_info = transfer_logical_tasks_.get(key);
            if (OB_ISNULL(tansfer_part_info)) {
              ObTransferPartList part_arr;
              if (OB_FAIL(transfer_logical_tasks_.set_refactored(key, part_arr))) {
                LOG_WARN("fail to init transfer task into map", KR(ret), K(key),
                         K(part_arr));
              } else {
                tansfer_part_info = transfer_logical_tasks_.get(key);
              }
            }
            if (OB_SUCC(ret) && OB_ISNULL(tansfer_part_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail get transfer task from map", KR(ret), K(key));
            }
            if (FAILEDx(tansfer_part_info->push_back(task.get_part_info()))) {
              LOG_WARN("failed to push back", KR(ret), K(task));
            }
          }
        }
      }//end for find part info
      if (OB_SUCC(ret) && !found) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part info index is out of range",
            KR(ret), K(part_info_index), K(i),
            K(task), K(part_info_), K(task_array));
      }
    }//end for process each task
  }//end else
  return ret;
}

int ObTransferPartitionHelper::get_ls_group_id(
    const share::ObLSStatusInfoIArray &status_info_array,
    const ObLSID &src_ls, const ObLSID &dest_ls,
    uint64_t &src_ls_group, uint64_t &dest_ls_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == status_info_array.count()
        || !src_ls.is_valid() || !dest_ls.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status_info_array), K(src_ls), K(dest_ls));
  }
  int found_cnt = 0;
  src_ls_group = OB_INVALID_ID;
  dest_ls_group = OB_INVALID_ID;
  for (int64_t i = 0;
      OB_SUCC(ret) && found_cnt < 2 && i < status_info_array.count(); ++i) {
    const ObLSStatusInfo &info = status_info_array.at(i);
    if (info.get_ls_id() == src_ls && OB_INVALID_ID == src_ls_group) {
      src_ls_group = info.ls_group_id_;
      found_cnt++;
    }
    if (info.get_ls_id() == dest_ls && OB_INVALID_ID == dest_ls_group) {
      dest_ls_group = info.ls_group_id_;
      found_cnt++;
    }
  }
  if (OB_SUCC(ret) && (2 != found_cnt || OB_INVALID_ID == src_ls_group
        || OB_INVALID_ID == dest_ls_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest or src ls not exist", KR(ret), K(found_cnt), K(src_ls),
        K(dest_ls), K(dest_ls_group), K(src_ls_group),
        K(status_info_array));
  }
  return ret;
}
#undef ISTAT
#undef WSTAT

}
}
