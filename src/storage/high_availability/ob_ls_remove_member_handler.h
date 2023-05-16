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

#ifndef OCEABASE_STORAGE_LS_REMOVE_MEMBER_HANDLER_
#define OCEABASE_STORAGE_LS_REMOVE_MEMBER_HANDLER_

#include "share/ob_ls_id.h"
#include "common/ob_member.h"
#include "lib/container/ob_array.h"
#include "share/ob_rpc_struct.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

class ObLSChangeMemberType final
{
public:
  enum TYPE : uint8_t
  {
    LS_REMOVE_MEMBER = 0,
    LS_MODIFY_REPLICA_NUMBER = 1,
    LS_TRANSFORM_MEMBER = 2,
    MAX,
  };

public:
  ObLSChangeMemberType() : type_(MAX) {}
  ~ObLSChangeMemberType() = default;
  explicit ObLSChangeMemberType(const TYPE &type);
  ObLSChangeMemberType &operator=(const TYPE &type);
  bool operator ==(const ObLSChangeMemberType &other) const { return type_ == other.type_; }
  bool operator !=(const ObLSChangeMemberType &other) const { return type_ != other.type_; }
  operator TYPE() const { return type_; }
  static const char *get_type_str(const ObLSChangeMemberType &type);
  bool is_valid() const { return type_ >= TYPE::LS_REMOVE_MEMBER && type_ < TYPE::MAX; }
  bool is_remove_member() const { return TYPE::LS_REMOVE_MEMBER == type_; }
  bool is_modify_replica_number() const { return TYPE::LS_MODIFY_REPLICA_NUMBER == type_; }
  bool is_transform_member() const { return TYPE::LS_TRANSFORM_MEMBER == type_; }
  TYPE get_type() const { return type_; }
  int set_type(int32_t type);
  void reset();

  TO_STRING_KV(K_(type));

private:
  TYPE type_;
};

struct ObLSRemoveMemberArg final
{
  ObLSRemoveMemberArg();
  ~ObLSRemoveMemberArg() = default;
  void reset();
  bool is_valid() const;

  TO_STRING_KV(
      K_(task_id),
      K_(tenant_id),
      K_(ls_id),
      K_(type),
      K_(remove_member),
      K_(orig_paxos_replica_number),
      K_(new_paxos_replica_number),
      K_(is_paxos_member),
      K_(member_list),
      K_(src),
      K_(dest));

  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObLSChangeMemberType type_;
  common::ObReplicaMember remove_member_;
  int64_t orig_paxos_replica_number_;
  int64_t new_paxos_replica_number_;
  bool is_paxos_member_;
  common::ObMemberList member_list_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dest_;
};

class ObLSRemoveMemberHandler
{
public:
  ObLSRemoveMemberHandler();
  virtual ~ObLSRemoveMemberHandler();
  int init(
      ObLS *ls,
      ObStorageRpc *storage_rpc);

  int remove_paxos_member(const obrpc::ObLSDropPaxosReplicaArg &arg);
  int remove_learner_member(const obrpc::ObLSDropNonPaxosReplicaArg &arg);
  int modify_paxos_replica_number(const obrpc::ObLSModifyPaxosReplicaNumberArg &arg);
  int transform_member(const obrpc::ObLSChangeReplicaArg &arg);
  int check_task_exist(const share::ObTaskId &task_id, bool &is_exist);
  void destroy();
private:
  int generate_remove_member_dag_(const ObLSRemoveMemberArg &remove_member_arg);

private:
  bool is_inited_;
  ObLS *ls_;
  ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRemoveMemberHandler);
};



}
}
#endif
