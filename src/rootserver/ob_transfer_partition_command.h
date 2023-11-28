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
namespace oceanbase
{
namespace rootserver
{
struct ObTransferPartitionArg
{
public:
  enum ObTransferPartitionType {
    INVALID_TYPE = -1,
    TRANSFER_PARTITION_TO_LS = 0,
    MAX_TYPE
  };
  ObTransferPartitionArg() : type_(INVALID_TYPE), target_tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID), object_id_(OB_INVALID_OBJECT_ID), ls_id_() {};
  ~ObTransferPartitionArg() {};
  int init_for_transfer_partition_to_ls(
      const uint64_t target_tenant_id,
      const uint64_t table_id,
      const ObObjectID object_id,
      const share::ObLSID &ls_id);
  bool is_valid() const;
  int assign(const ObTransferPartitionArg &other);
  TO_STRING_KV(K_(type), K_(target_tenant_id), K_(table_id), K_(object_id), K_(ls_id));
  bool is_transfer_partition_to_ls() const { return TRANSFER_PARTITION_TO_LS == type_; }
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
  int check_tenant_status_data_version_and_config_(const uint64_t tenant_id);
  int check_table_schema_(const uint64_t tenant_id, const uint64_t table_id, const ObObjectID &object_id);
  int check_ls_(const uint64_t tenant_id, const share::ObLSID &ls_id);
};
}
}
#endif
