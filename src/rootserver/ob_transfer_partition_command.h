/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_PARTIION_COMMAND_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_PARTIION_COMMAND_H
#include "share/ob_ls_id.h"
#include "share/transfer/ob_transfer_info.h"
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
}
namespace share
{
class ObTransferPartitionTask;
}
namespace rootserver
{
struct ObTransferPartitionArg
{
public:
  enum ObTransferPartitionType {
    INVALID_TYPE = -1,
    TRANSFER_PARTITION_TO_LS = 0,
    CANCEL_TRANSFER_PARTITION,
    CANCEL_TRANSFER_PARTITION_ALL,
    CANCEL_BALANCE_JOB,
    MAX_TYPE
  };
  ObTransferPartitionArg() : type_(INVALID_TYPE), target_tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID), object_id_(OB_INVALID_OBJECT_ID), ls_id_() {};
  ~ObTransferPartitionArg() {};
  int init_for_transfer_partition_to_ls(
      const uint64_t target_tenant_id,
      const uint64_t table_id,
      const ObObjectID &object_id,
      const share::ObLSID &ls_id);
  int init_for_cancel_transfer_partition(
      const uint64_t target_tenant_id,
      const ObTransferPartitionType type,
      const uint64_t table_id,
      const ObObjectID &object_id);
  int init_for_cancel_balance_job(
      const uint64_t target_tenant_id);
  bool is_valid() const;
  int assign(const ObTransferPartitionArg &other);
  void reset()
  {
    type_ = INVALID_TYPE;
    target_tenant_id_ = OB_INVALID_TENANT_ID;
    table_id_ = OB_INVALID_ID;
    object_id_ = OB_INVALID_OBJECT_ID;
    ls_id_.reset();
  }
  TO_STRING_KV(K_(type), K_(target_tenant_id), K_(table_id), K_(object_id), K_(ls_id));
  bool is_transfer_partition_to_ls() const { return TRANSFER_PARTITION_TO_LS == type_; }
  bool is_cancel_transfer_partition() const
  {
    return CANCEL_TRANSFER_PARTITION == type_;
  }
  bool is_cancel_transfer_partition_all() const
  {
    return CANCEL_TRANSFER_PARTITION_ALL == type_;
  }
  bool is_cancel_balance_job() const
  {
    return CANCEL_BALANCE_JOB == type_;
  }
  const uint64_t &get_target_tenant_id() const { return target_tenant_id_; }
  const uint64_t &get_table_id() const { return table_id_; }
  const ObObjectID &get_object_id() const { return object_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
private:
  ObTransferPartitionType type_;
  uint64_t target_tenant_id_;
  uint64_t table_id_;
  ObObjectID object_id_;
  share::ObLSID ls_id_;
};

class ObTransferPartitionCommand
{
public:
  ObTransferPartitionCommand() {};
  ~ObTransferPartitionCommand() {};
  int execute(const ObTransferPartitionArg &arg);
private:
  int execute_transfer_partition_(const ObTransferPartitionArg &arg);
  int execute_cancel_transfer_partition_(const ObTransferPartitionArg &arg);
  int execute_cancel_transfer_partition_all_(const ObTransferPartitionArg &arg);
  int execute_cancel_balance_job_(const ObTransferPartitionArg &arg);
  int check_tenant_status_(const uint64_t tenant_id);
  int check_data_version_for_cancel_(const uint64_t tenant_id);
  int check_data_version_and_config_(const uint64_t tenant_id);
  int check_table_schema_(const uint64_t tenant_id, const uint64_t table_id, const ObObjectID &object_id);
  int check_ls_(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int try_cancel_transfer_partition_(const share::ObTransferPartitionTask &task,
      common::ObMySQLTransaction &trans);
  int cancel_balance_job_in_trans_(const uint64_t tenant_id, common::ObMySQLTransaction &trans);
  int cancel_transfer_partition_in_init_(const share::ObTransferPartitionTask &task,
      common::ObMySQLTransaction &trans);
  int get_transfer_part_list_(const uint64_t tenant_id,
      share::ObTransferPartList &wait_list, share::ObTransferPartList &init_list,
      share::ObTransferPartList &doing_list, share::ObTransferPartitionTaskID &max_task_id,
      common::ObMySQLTransaction &trans);
  int cancel_all_init_transfer_partition_(const uint64_t tenant_id,
      const share::ObTransferPartList &init_list,
      common::ObMySQLTransaction &trans);

};
}
}
#endif
