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

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_remove_member_handler.h"
#include "ob_ls_remove_member_dag.h"
#include "observer/ob_server.h"

using namespace oceanbase;
using namespace share;
using namespace storage;

ObLSChangeMemberType::ObLSChangeMemberType(const TYPE &type)
  : type_(type)
{
}

const char *ObLSChangeMemberType::get_type_str(const ObLSChangeMemberType &type)
{
  const char *str = "UNKNOWN";
  const char *type_str[] = {
      "LS_REMOVE_MEMBER",
      "LS_MODIFY_REPLICA_NUMBER",
      "LS_TRANSFORM_MEMBER",
  };
  STATIC_ASSERT(MAX == ARRAYSIZEOF(type_str), "type count mismatch");
  if (type.type_ < 0 || type.type_ >= MAX) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid type", K(type));
  } else {
    str = type_str[type.type_];
  }
  return str;

}

int ObLSChangeMemberType::set_type(int32_t type)
{
  int ret = OB_SUCCESS;
  if (0 > type || MAX <= type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(type));
  } else {
    type_ = static_cast<TYPE>(type);
  }
  return ret;
}

ObLSChangeMemberType &ObLSChangeMemberType::operator=(const TYPE &type)
{
  type_ = type;
  return *this;
}

void ObLSChangeMemberType::reset()
{
  type_ = MAX;
}


ObLSRemoveMemberArg::ObLSRemoveMemberArg()
  : task_id_(),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    type_(),
    remove_member_(),
    orig_paxos_replica_number_(0),
    new_paxos_replica_number_(0),
    is_paxos_member_(false),
    member_list_(),
    src_(),
    dest_()
{
}

void ObLSRemoveMemberArg::reset()
{
  task_id_.reset();
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  type_.reset();
  remove_member_.reset();
  orig_paxos_replica_number_ = 0;
  new_paxos_replica_number_ = 0;
  is_paxos_member_ = false;
  member_list_.reset();
  src_.reset();
  dest_.reset();
}

bool ObLSRemoveMemberArg::is_valid() const
{
  bool bool_ret = false;
  bool_ret = !task_id_.is_invalid()
      && OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && type_.is_valid();

  if (bool_ret) {
    if (type_.is_remove_member()) {
      bool_ret = remove_member_.is_valid();
    } else if (type_.is_modify_replica_number()) {
      bool_ret = member_list_.is_valid();
    } else if (type_.is_transform_member()) {
      bool_ret = src_.is_valid() && dest_.is_valid();
    } else {
      bool_ret = false;
    }
  }

  if (bool_ret && is_paxos_member_) {
    bool_ret = orig_paxos_replica_number_ > 0 && new_paxos_replica_number_ > 0;
  }
  return bool_ret;
}


ObLSRemoveMemberHandler::ObLSRemoveMemberHandler()
  : is_inited_(false),
    ls_(nullptr),
    storage_rpc_(nullptr)
{
}

ObLSRemoveMemberHandler::~ObLSRemoveMemberHandler()
{
}

int ObLSRemoveMemberHandler::init(
    ObLS *ls,
    ObStorageRpc *storage_rpc)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls remove member handler init tiwce", K(ret));
  } else if (OB_ISNULL(ls) || OB_ISNULL(storage_rpc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ls remove member handler get invalid argument", K(ret), KP(ls), KP(storage_rpc));
  } else {
    ls_ = ls;
    storage_rpc_ = storage_rpc;
    is_inited_ = true;
  }
  return ret;
}

void ObLSRemoveMemberHandler::destroy()
{
  ls_ = nullptr;
  is_inited_ = false;
}

int ObLSRemoveMemberHandler::remove_paxos_member(
    const obrpc::ObLSDropPaxosReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberArg remove_member_arg;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remove paxos member get invalid argument", K(ret), K(arg));
  } else {
    remove_member_arg.tenant_id_ = arg.tenant_id_;
    remove_member_arg.ls_id_ = arg.ls_id_;
    remove_member_arg.task_id_ = arg.task_id_;
    remove_member_arg.remove_member_ = arg.remove_member_;
    remove_member_arg.new_paxos_replica_number_ = arg.new_paxos_replica_number_;
    remove_member_arg.orig_paxos_replica_number_ = arg.orig_paxos_replica_number_;
    remove_member_arg.is_paxos_member_ = true;
    remove_member_arg.type_ = ObLSChangeMemberType::LS_REMOVE_MEMBER;

    if (OB_FAIL(generate_remove_member_dag_(remove_member_arg))) {
      LOG_WARN("failed to generate remove member dag", K(ret), K(arg), K(remove_member_arg));
    }
  }
  return ret;
}

int ObLSRemoveMemberHandler::remove_learner_member(const obrpc::ObLSDropNonPaxosReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberArg remove_member_arg;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", K(ret), K(arg));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("remoev learner member get invalid argument", K(ret), K(arg));
  } else {
    remove_member_arg.tenant_id_ = arg.tenant_id_;
    remove_member_arg.ls_id_ = arg.ls_id_;
    remove_member_arg.task_id_ = arg.task_id_;
    remove_member_arg.remove_member_ = arg.remove_member_;
    remove_member_arg.is_paxos_member_ = false;
    remove_member_arg.type_ = ObLSChangeMemberType::LS_REMOVE_MEMBER;

    if (OB_FAIL(generate_remove_member_dag_(remove_member_arg))) {
      LOG_WARN("failed to generate remove member dag", K(ret), K(arg), K(remove_member_arg));
    }
  }
  return ret;
}

int ObLSRemoveMemberHandler::modify_paxos_replica_number(const obrpc::ObLSModifyPaxosReplicaNumberArg &arg)
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberArg remove_member_arg;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", KR(ret), K(arg));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("modify paxos replica number get invalid argument", KR(ret), K(arg));
  } else {
    remove_member_arg.tenant_id_ = arg.tenant_id_;
    remove_member_arg.ls_id_ = arg.ls_id_;
    remove_member_arg.task_id_ = arg.task_id_;
    remove_member_arg.new_paxos_replica_number_ = arg.new_paxos_replica_number_;
    remove_member_arg.orig_paxos_replica_number_ = arg.orig_paxos_replica_number_;
    remove_member_arg.member_list_ = arg.member_list_;
    remove_member_arg.is_paxos_member_ = true;
    remove_member_arg.type_ = ObLSChangeMemberType::LS_MODIFY_REPLICA_NUMBER;

    if (OB_FAIL(generate_remove_member_dag_(remove_member_arg))) {
      LOG_WARN("failed to generate remove member dag", KR(ret), K(arg), K(remove_member_arg));
    }
  }
  return ret;
}

int ObLSRemoveMemberHandler::transform_member(const obrpc::ObLSChangeReplicaArg &arg)
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberArg remove_member_arg;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", KR(ret), K(arg));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transform member get invalid argument", KR(ret), K(arg));
  } else if (arg.src_.get_replica_type() == arg.dst_.get_replica_type()
      || !ObReplicaTypeCheck::change_replica_op_allow(arg.src_.get_replica_type(), arg.dst_.get_replica_type())) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("change replica op not allow", K(ret), K(arg));
  } else {
    remove_member_arg.tenant_id_ = arg.tenant_id_;
    remove_member_arg.ls_id_ = arg.ls_id_;
    remove_member_arg.task_id_ = arg.task_id_;
    remove_member_arg.new_paxos_replica_number_ = arg.new_paxos_replica_number_;
    remove_member_arg.orig_paxos_replica_number_ = arg.orig_paxos_replica_number_;
    remove_member_arg.src_ = arg.src_;
    remove_member_arg.dest_ = arg.dst_;
    remove_member_arg.is_paxos_member_ = true;
    remove_member_arg.type_ = ObLSChangeMemberType::LS_TRANSFORM_MEMBER;

    if (OB_FAIL(generate_remove_member_dag_(remove_member_arg))) {
      LOG_WARN("failed to generate remove member dag", KR(ret), K(arg), K(remove_member_arg));
    }
  }
  return ret;
}

int ObLSRemoveMemberHandler::generate_remove_member_dag_(
    const ObLSRemoveMemberArg &remove_member_arg)
{
  int ret = OB_SUCCESS;
  ObLSRemoveMemberDagParam param;
  ObLSRemoveMemberDag *ls_remove_member_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", K(ret));
  } else if (!remove_member_arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate remove member dag get invalid argument", K(ret), K(remove_member_arg));
  } else if (FALSE_IT(param.storage_rpc_ = storage_rpc_)) {
  } else if (FALSE_IT(param.arg_ = remove_member_arg)) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler should not be NULL", K(ret), KP(scheduler));
  } else if (OB_FAIL(scheduler->create_dag(&param, ls_remove_member_dag))) {
    LOG_WARN("failed to create ls remove member dag", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(ls_remove_member_dag))) {
    LOG_WARN("fail to add dag into dag_scheduler", K(ret));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ls_remove_member_dag)) {
    scheduler->free_dag(*ls_remove_member_dag);
  }
  return ret;
}

int ObLSRemoveMemberHandler::check_task_exist(
    const share::ObTaskId &task_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObTenantDagScheduler *scheduler = nullptr;
  ObLSRemoveMemberArg mock_remove_member_arg;
  ObLSRemoveMemberDag *exist_dag = nullptr;
  ObLSRemoveMemberDagParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member handler do not init", K(ret));
  } else if (task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check task exist get invalid argument", K(ret), K(task_id));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler should not be NULL", K(ret), KP(scheduler));
  } else {
    ObMember mock_member(MYADDR, OB_INVALID_TIMESTAMP);
    mock_remove_member_arg.tenant_id_ = MTL_ID();
    mock_remove_member_arg.ls_id_ = ls_->get_ls_id();
    mock_remove_member_arg.task_id_ = task_id;
    mock_remove_member_arg.is_paxos_member_ = false;
    mock_remove_member_arg.type_ = ObLSChangeMemberType::LS_REMOVE_MEMBER;
    param.arg_ = mock_remove_member_arg;

    if (OB_FAIL(mock_remove_member_arg.remove_member_.set_member(mock_member))) {
      LOG_WARN("failed to set member", K(ret), K(mock_member), K(mock_remove_member_arg));
    } else if (OB_FAIL(scheduler->create_dag(&param, exist_dag))) {
      LOG_WARN("failed to create ls remove member dag", K(ret));
    } else if (OB_FAIL(scheduler->check_dag_exist(exist_dag, is_exist))) {
      LOG_WARN("failed to check dag exist", K(ret), KPC(exist_dag));
    }

    if (OB_NOT_NULL(exist_dag)) {
      scheduler->free_dag(*exist_dag);
    }
  }
  return ret;
}


