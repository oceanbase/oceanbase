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

#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_errno.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
ObTabletCreateDeleteMdsUserData::ObTabletCreateDeleteMdsUserData()
  : tablet_status_(),
    transfer_scn_(share::SCN::invalid_scn()),
    transfer_ls_id_(),
    data_type_(ObTabletMdsUserDataType::NONE),
    create_commit_scn_(share::SCN::invalid_scn()),
    create_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    delete_commit_scn_(share::SCN::invalid_scn()),
    delete_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    transfer_out_commit_version_(ObTransVersion::INVALID_TRANS_VERSION)
{
}

ObTabletCreateDeleteMdsUserData::ObTabletCreateDeleteMdsUserData(const ObTabletStatus::Status &status, const ObTabletMdsUserDataType type)
  : tablet_status_(status),
    transfer_scn_(share::SCN::invalid_scn()),
    transfer_ls_id_(),
    data_type_(type),
    create_commit_scn_(share::SCN::invalid_scn()),
    create_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    delete_commit_scn_(share::SCN::invalid_scn()),
    delete_commit_version_(ObTransVersion::INVALID_TRANS_VERSION),
    transfer_out_commit_version_(ObTransVersion::INVALID_TRANS_VERSION)
{
}

int ObTabletCreateDeleteMdsUserData::assign(const ObTabletCreateDeleteMdsUserData &other)
{
  int ret = OB_SUCCESS;
  tablet_status_ = other.tablet_status_;
  transfer_scn_ = other.transfer_scn_;
  transfer_ls_id_ = other.transfer_ls_id_;
  data_type_ = other.data_type_;
  create_commit_scn_ = other.create_commit_scn_;
  create_commit_version_ = other.create_commit_version_;
  delete_commit_scn_ = other.delete_commit_scn_;
  delete_commit_version_ = other.delete_commit_version_;
  transfer_out_commit_version_ = other.transfer_out_commit_version_;
  return ret;
}

void ObTabletCreateDeleteMdsUserData::reset()
{
  tablet_status_ = ObTabletStatus::MAX;
  transfer_scn_.set_invalid();
  transfer_ls_id_.reset();
  data_type_ = ObTabletMdsUserDataType::NONE;
  create_commit_scn_.set_invalid();
  create_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  delete_commit_scn_.set_invalid();
  create_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
  transfer_out_commit_version_ = ObTransVersion::INVALID_TRANS_VERSION;
}

void ObTabletCreateDeleteMdsUserData::on_redo(const share::SCN &redo_scn)
{
  int ret = OB_SUCCESS;
  switch (data_type_) {
  case ObTabletMdsUserDataType::NONE :
  case ObTabletMdsUserDataType::CREATE_TABLET :
  case ObTabletMdsUserDataType::REMOVE_TABLET :
  case ObTabletMdsUserDataType::START_TRANSFER_IN :
  case ObTabletMdsUserDataType::FINISH_TRANSFER_OUT : {
    break;
  }
  case ObTabletMdsUserDataType::START_TRANSFER_OUT : {
    start_transfer_out_on_redo_(redo_scn);
    break;
  }
  case ObTabletMdsUserDataType::FINISH_TRANSFER_IN : {
    finish_transfer_in_on_redo_(redo_scn);
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid cur status for fail", K(ret), KPC(this));
  }
  }
}

void ObTabletCreateDeleteMdsUserData::start_transfer_out_on_redo_(const share::SCN &redo_scn)
{
  transfer_scn_ = redo_scn;
  LOG_INFO("[TRANSFER] start transfer out on redo", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::finish_transfer_in_on_redo_(const share::SCN &redo_scn)
{
  transfer_scn_ = redo_scn;
  LOG_INFO("[TRANSFER] finish transfer in on redo", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  switch (data_type_) {
  case ObTabletMdsUserDataType::NONE :
  case ObTabletMdsUserDataType::FINISH_TRANSFER_IN : {
    break;
  }
  case ObTabletMdsUserDataType::CREATE_TABLET : {
    create_tablet_on_commit_(commit_version, commit_scn);
    break;
  }
  case ObTabletMdsUserDataType::START_TRANSFER_IN : {
    start_transfer_in_on_commit_(commit_version, commit_scn);
    break;
  }
  case ObTabletMdsUserDataType::REMOVE_TABLET : {
    delete_tablet_on_commit_(commit_version, commit_scn);
    break;
  }
  case ObTabletMdsUserDataType::FINISH_TRANSFER_OUT : {
    finish_transfer_out_on_commit_(commit_version, commit_scn);
    break;
  }
  case ObTabletMdsUserDataType::START_TRANSFER_OUT : {
    start_transfer_out_on_commit_(commit_version);
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid cur status for fail", K(ret), KPC(this));
  }
  }
}

void ObTabletCreateDeleteMdsUserData::create_tablet_on_commit_(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  create_commit_scn_ = commit_scn;
  create_commit_version_ = commit_version.get_val_for_tx();
  LOG_INFO("create tablet commit", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::start_transfer_in_on_commit_(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  LOG_INFO("[TRANSFER] transfer in create tablet commit", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::delete_tablet_on_commit_(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  delete_commit_scn_ = commit_scn;
  delete_commit_version_ = commit_version.get_val_for_tx();
  LOG_INFO("delete tablet commit", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::finish_transfer_out_on_commit_(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  delete_commit_scn_ = commit_scn;
  delete_commit_version_ = commit_version.get_val_for_tx();
  LOG_INFO("[TRANSFER] transfer out delete tablet commit", KPC(this));
}

void ObTabletCreateDeleteMdsUserData::start_transfer_out_on_commit_(
    const share::SCN &commit_version)
{
  transfer_out_commit_version_ = commit_version.get_val_for_tx();
  LOG_INFO("[TRANSFER] transfer out on commit", KPC(this));
}

int ObTabletCreateDeleteMdsUserData::set_tablet_gc_trigger(
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSService *ls_service = MTL(ObLSService*);
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else {
    ls->get_tablet_gc_handler()->set_tablet_gc_trigger();
  }
  return ret;
}

int ObTabletCreateDeleteMdsUserData::set_tablet_empty_shell_trigger(
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSService *ls_service = MTL(ObLSService*);
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id), K(ls_handle));
  } else {
    ls->get_tablet_empty_shell_handler()->set_empty_shell_trigger(true);
    LOG_INFO("set_tablet_empty_shell_trigger", K(ls_id), "handler", ls->get_tablet_empty_shell_handler());
  }
  return ret;
}

OB_SERIALIZE_MEMBER(
    ObTabletCreateDeleteMdsUserData,
    tablet_status_,
    transfer_scn_,
    transfer_ls_id_,
    data_type_,
    create_commit_scn_,
    create_commit_version_,
    delete_commit_scn_,
    delete_commit_version_,
    transfer_out_commit_version_)
} // namespace storage
} // namespace oceanbase
