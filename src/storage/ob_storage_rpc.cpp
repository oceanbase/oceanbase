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
#include "share/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_task_define.h"
#include "share/ob_force_print_log.h"
#include "ob_storage_rpc.h"
#include "storage/high_availability/ob_storage_ha_reader.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_handler.h"
#include "storage/restore/ob_ls_restore_handler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "lib/thread/thread.h"

namespace oceanbase
{
using namespace lib;
using namespace common;
using namespace share;
using namespace obrpc;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;

namespace obrpc
{

static bool is_copy_ls_inner_tablet(const common::ObIArray<common::ObTabletID> &tablet_id_list)
{
  bool is_inner = false;
  for (int64_t i = 0; i < tablet_id_list.count(); ++i) {
    is_inner = tablet_id_list.at(i).is_ls_inner_tablet();
    if (is_inner) {
      break;
    }
  }
  return is_inner;
}

static int compare_ls_rebuild_seq(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t remote_rebuild_seq)
{
  int ret = OB_SUCCESS;
  int64_t local_rebuild_seq = 0;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObMigrationUtils::get_ls_rebuild_seq(tenant_id, ls_id, local_rebuild_seq))) {
    LOG_WARN("failed to get ls rebuild seq", K(ret), K(tenant_id), K(ls_id));
  } else {
    if (local_rebuild_seq != remote_rebuild_seq) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      LOG_WARN("rebuild seq has changed", K(ret), K(local_rebuild_seq), K(remote_rebuild_seq));
      SERVER_EVENT_ADD("storage_ha", "compare_ls_rebuild_seq",
                       "tenant_id", tenant_id,
                       "ls_id", ls_id.id(),
                       "local_rebuild_seq", local_rebuild_seq,
                       "remote_rebuild_seq", remote_rebuild_seq);
    }
  }
  return ret;
}

ObCopyMacroBlockArg::ObCopyMacroBlockArg()
  : logic_macro_block_id_()
{
}

void ObCopyMacroBlockArg::reset()
{
  logic_macro_block_id_.reset();
}

bool ObCopyMacroBlockArg::is_valid() const
{
  return logic_macro_block_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockArg,
    logic_macro_block_id_);


ObCopyMacroBlockListArg::ObCopyMacroBlockListArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    arg_list_()
{
}

void ObCopyMacroBlockListArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  table_key_.reset();
  arg_list_.reset();
}

bool ObCopyMacroBlockListArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && table_key_.is_valid()
      && arg_list_.count() > 0;
}

int ObCopyMacroBlockListArg::assign(const ObCopyMacroBlockListArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch macro block list get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(arg_list_.assign(arg.arg_list_))) {
    LOG_WARN("failed to assign arg list", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    table_key_ = arg.table_key_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockListArg, tenant_id_, ls_id_, table_key_, arg_list_);

ObCopyMacroBlockRangeArg::ObCopyMacroBlockRangeArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    data_version_(0),
    backfill_tx_scn_(SCN::min_scn()),
    copy_macro_range_info_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

void ObCopyMacroBlockRangeArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  table_key_.reset();
  data_version_ = 0;
  backfill_tx_scn_.set_min();
  copy_macro_range_info_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

bool ObCopyMacroBlockRangeArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && table_key_.is_valid()
      && data_version_ >= 0
      && backfill_tx_scn_ >= SCN::min_scn()
      && copy_macro_range_info_.is_valid()
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
}

int ObCopyMacroBlockRangeArg::assign(const ObCopyMacroBlockRangeArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign copy macro block range arg get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(copy_macro_range_info_.assign(arg.copy_macro_range_info_))) {
    LOG_WARN("failed to assign copy macro range info", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    table_key_ = arg.table_key_;
    data_version_ = arg.data_version_;
    backfill_tx_scn_ = arg.backfill_tx_scn_;
    need_check_seq_ = arg.need_check_seq_;
    ls_rebuild_seq_ = arg.ls_rebuild_seq_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockRangeArg, tenant_id_, ls_id_, table_key_, data_version_,
    backfill_tx_scn_, copy_macro_range_info_, need_check_seq_, ls_rebuild_seq_);

ObCopyMacroBlockHeader::ObCopyMacroBlockHeader()
  : is_reuse_macro_block_(false),
    occupy_size_(0),
    macro_meta_row_(),
    allocator_("CMBlockHeader")
{
}

void ObCopyMacroBlockHeader::reset()
{
  is_reuse_macro_block_ = false;
  occupy_size_ = 0;
  macro_meta_row_.reset();
  allocator_.reset();
}

bool ObCopyMacroBlockHeader::is_valid() const
{
  return occupy_size_ > 0;
}

OB_SERIALIZE_MEMBER(ObCopyMacroBlockHeader, is_reuse_macro_block_, occupy_size_);

ObCopyTabletInfoArg::ObCopyTabletInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_list_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    is_only_copy_major_(false),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_list_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  is_only_copy_major_ = false;
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletInfoArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && tablet_id_list_.count() > 0
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletInfoArg,
    tenant_id_, ls_id_, tablet_id_list_, need_check_seq_, ls_rebuild_seq_, is_only_copy_major_, version_);

ObCopyTabletInfo::ObCopyTabletInfo()
  : tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    param_(),
    data_size_(0),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletInfo::reset()
{
  tablet_id_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
  param_.reset();
  data_size_ = 0;
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletInfo::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && param_.is_valid() && data_size_ >= 0)
        || ObCopyTabletStatus::TABLET_NOT_EXIST == status_)
      && version_ != OB_INVALID_ID;
}

int ObCopyTabletInfo::assign(const ObCopyTabletInfo &info)
{
  int ret = OB_SUCCESS;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy tablet info is invalid", K(ret), K(info));
  } else if (OB_FAIL(param_.assign(info.param_))) {
    LOG_WARN("failed to assign copy info param", K(ret), K(info));
  } else {
    status_ = info.status_;
    data_size_ = info.data_size_;
    version_ = info.version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopyTabletInfo, tablet_id_, status_, param_, data_size_, version_);

/******************ObCopyTabletSSTableInfoArg*********************/
ObCopyTabletSSTableInfoArg::ObCopyTabletSSTableInfoArg()
  : tablet_id_(),
    max_major_sstable_snapshot_(0),
    minor_sstable_scn_range_(),
    ddl_sstable_scn_range_()
{
}

ObCopyTabletSSTableInfoArg::~ObCopyTabletSSTableInfoArg()
{
}

void ObCopyTabletSSTableInfoArg::reset()
{
  tablet_id_.reset();
  max_major_sstable_snapshot_ = 0;
  minor_sstable_scn_range_.reset();
  ddl_sstable_scn_range_.reset();
}

bool ObCopyTabletSSTableInfoArg::is_valid() const
{
  return tablet_id_.is_valid()
      && max_major_sstable_snapshot_ >= 0
      && minor_sstable_scn_range_.is_valid()
      && ddl_sstable_scn_range_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableInfoArg,
    tablet_id_, max_major_sstable_snapshot_, minor_sstable_scn_range_, ddl_sstable_scn_range_);

ObCopyTabletsSSTableInfoArg::ObCopyTabletsSSTableInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    is_only_copy_major_(false),
    tablet_sstable_info_arg_list_(),
    version_(OB_INVALID_ID)
{
}

ObCopyTabletsSSTableInfoArg::~ObCopyTabletsSSTableInfoArg()
{
  reset();
}

void ObCopyTabletsSSTableInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  is_only_copy_major_ = false;
  tablet_sstable_info_arg_list_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletsSSTableInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
      && tablet_sstable_info_arg_list_.count() >= 0
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletsSSTableInfoArg,
    tenant_id_, ls_id_, need_check_seq_, ls_rebuild_seq_, is_only_copy_major_, tablet_sstable_info_arg_list_, version_);


ObCopyTabletSSTableInfo::ObCopyTabletSSTableInfo()
  : tablet_id_(),
    table_key_(),
    param_()
{
}

void ObCopyTabletSSTableInfo::reset()
{
  tablet_id_.reset();
  table_key_.reset();
  param_.reset();
}

bool ObCopyTabletSSTableInfo::is_valid() const
{
  return tablet_id_.is_valid()
      && table_key_.is_valid()
      && param_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableInfo,
    tablet_id_, table_key_, param_);


ObCopyLSInfoArg::ObCopyLSInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    version_(OB_INVALID_ID)
{
}

void ObCopyLSInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyLSInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyLSInfoArg,
    tenant_id_, ls_id_, version_);


ObCopyLSInfo::ObCopyLSInfo()
  : ls_meta_package_(),
    tablet_id_array_(),
    is_log_sync_(false),
    version_(OB_INVALID_ID)
{
}

void ObCopyLSInfo::reset()
{
  ls_meta_package_.reset();
  tablet_id_array_.reset();
  is_log_sync_ = false;
  version_ = OB_INVALID_ID;
}

bool ObCopyLSInfo::is_valid() const
{
  return ls_meta_package_.is_valid() && tablet_id_array_.count() > 0 && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyLSInfo,
    ls_meta_package_, tablet_id_array_, is_log_sync_, version_);

ObFetchLSMetaInfoArg::ObFetchLSMetaInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    version_(OB_INVALID_ID)
{
}

void ObFetchLSMetaInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  version_ = OB_INVALID_ID;
}

bool ObFetchLSMetaInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMetaInfoArg, tenant_id_, ls_id_, version_);


ObFetchLSMetaInfoResp::ObFetchLSMetaInfoResp()
  : ls_meta_package_(),
    version_(OB_INVALID_ID),
    has_transfer_table_(false)
{
}

void ObFetchLSMetaInfoResp::reset()
{
  ls_meta_package_.reset();
  version_ = OB_INVALID_ID;
  has_transfer_table_ = false;
}

bool ObFetchLSMetaInfoResp::is_valid() const
{
  return ls_meta_package_.is_valid()
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMetaInfoResp, ls_meta_package_, version_, has_transfer_table_);

ObFetchLSMemberListArg::ObFetchLSMemberListArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

bool ObFetchLSMemberListArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid();
}

void ObFetchLSMemberListArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberListArg, tenant_id_, ls_id_);

ObFetchLSMemberListInfo::ObFetchLSMemberListInfo()
  : member_list_()
{
}

bool ObFetchLSMemberListInfo::is_valid() const
{
  return member_list_.is_valid();
}

void ObFetchLSMemberListInfo::reset()
{
  member_list_.reset();
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberListInfo, member_list_);

ObFetchLSMemberAndLearnerListArg::ObFetchLSMemberAndLearnerListArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

bool ObFetchLSMemberAndLearnerListArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid();
}

void ObFetchLSMemberAndLearnerListArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberAndLearnerListArg, tenant_id_, ls_id_);

ObFetchLSMemberAndLearnerListInfo::ObFetchLSMemberAndLearnerListInfo()
  : member_list_(),
    learner_list_()
{
}

bool ObFetchLSMemberAndLearnerListInfo::is_valid() const
{
  return member_list_.is_valid() || learner_list_.is_valid();
}

void ObFetchLSMemberAndLearnerListInfo::reset()
{
  member_list_.reset();
  learner_list_.reset();
}

OB_SERIALIZE_MEMBER(ObFetchLSMemberAndLearnerListInfo, member_list_, learner_list_);

ObCopySSTableMacroRangeInfoArg::ObCopySSTableMacroRangeInfoArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    copy_table_key_array_(),
    macro_range_max_marco_count_(0),
    need_check_seq_(false),
    ls_rebuild_seq_(0)
{
}

ObCopySSTableMacroRangeInfoArg::~ObCopySSTableMacroRangeInfoArg()
{
}

void ObCopySSTableMacroRangeInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  copy_table_key_array_.reset();
  macro_range_max_marco_count_ = 0;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

bool ObCopySSTableMacroRangeInfoArg::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && copy_table_key_array_.count() > 0
      && macro_range_max_marco_count_ > 0;
}

int ObCopySSTableMacroRangeInfoArg::assign(const ObCopySSTableMacroRangeInfoArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy sstable macro range info arg is invalid", K(ret), K(arg));
  } else if (OB_FAIL(copy_table_key_array_.assign(arg.copy_table_key_array_))) {
    LOG_WARN("failed to assign src table array", K(ret), K(arg));
  } else {
    tenant_id_ = arg.tenant_id_;
    ls_id_ = arg.ls_id_;
    tablet_id_ = arg.tablet_id_;
    macro_range_max_marco_count_ = arg.macro_range_max_marco_count_;
    need_check_seq_ = arg.need_check_seq_;
    ls_rebuild_seq_ = arg.ls_rebuild_seq_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCopySSTableMacroRangeInfoArg, tenant_id_, ls_id_,
    tablet_id_, copy_table_key_array_, macro_range_max_marco_count_,
    need_check_seq_, ls_rebuild_seq_);

ObCopySSTableMacroRangeInfoHeader::ObCopySSTableMacroRangeInfoHeader()
  : copy_table_key_(),
    macro_range_count_(0)
{
}

ObCopySSTableMacroRangeInfoHeader::~ObCopySSTableMacroRangeInfoHeader()
{
}

void ObCopySSTableMacroRangeInfoHeader::reset()
{
  copy_table_key_.reset();
  macro_range_count_ = 0;
}

bool ObCopySSTableMacroRangeInfoHeader::is_valid() const
{
  return copy_table_key_.is_valid()
      && macro_range_count_ >= 0;
}

OB_SERIALIZE_MEMBER(ObCopySSTableMacroRangeInfoHeader,
    copy_table_key_, macro_range_count_);

ObCopyTabletSSTableHeader::ObCopyTabletSSTableHeader()
  : tablet_id_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    sstable_count_(0),
    tablet_meta_(),
    version_(OB_INVALID_ID)
{
}

void ObCopyTabletSSTableHeader::reset()
{
  tablet_id_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
  sstable_count_ = 0;
  tablet_meta_.reset();
  version_ = OB_INVALID_ID;
}

bool ObCopyTabletSSTableHeader::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && sstable_count_ >= 0
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && tablet_meta_.is_valid())
          || ObCopyTabletStatus::TABLET_NOT_EXIST == status_)
      && version_ != OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObCopyTabletSSTableHeader,
    tablet_id_, status_, sstable_count_, tablet_meta_, version_);

ObNotifyRestoreTabletsArg::ObNotifyRestoreTabletsArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), tablet_id_array_(), restore_status_(), leader_proposal_id_(0)
{
}

void ObNotifyRestoreTabletsArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_array_.reset();
  leader_proposal_id_ = 0;
}

bool ObNotifyRestoreTabletsArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid()
         && leader_proposal_id_ > 0;
}

OB_SERIALIZE_MEMBER(ObNotifyRestoreTabletsArg, tenant_id_, ls_id_, tablet_id_array_, restore_status_, leader_proposal_id_);


ObNotifyRestoreTabletsResp::ObNotifyRestoreTabletsResp()
  : tenant_id_(OB_INVALID_ID), ls_id_(), restore_status_()
{
}

void ObNotifyRestoreTabletsResp::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObNotifyRestoreTabletsResp::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObNotifyRestoreTabletsResp, tenant_id_, ls_id_, restore_status_);


ObInquireRestoreArg::ObInquireRestoreArg()
  : tenant_id_(OB_INVALID_ID), ls_id_(), restore_status_()
{
}

void ObInquireRestoreArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObInquireRestoreArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
	       && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObInquireRestoreArg, tenant_id_, ls_id_, restore_status_);

ObInquireRestoreResp::ObInquireRestoreResp()
  : tenant_id_(OB_INVALID_ID), ls_id_(), is_leader_(false), restore_status_()
{
}

void ObInquireRestoreResp::reset()
{
  tenant_id_ = OB_INVALID_ID;
  is_leader_ = false;
  ls_id_.reset();
}

bool ObInquireRestoreResp::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && restore_status_.is_valid();
}

OB_SERIALIZE_MEMBER(ObInquireRestoreResp, tenant_id_, ls_id_, is_leader_, restore_status_);


ObRestoreUpdateLSMetaArg::ObRestoreUpdateLSMetaArg()
  : tenant_id_(OB_INVALID_ID), ls_meta_package_()
{
}

void ObRestoreUpdateLSMetaArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_meta_package_.reset();
}

bool ObRestoreUpdateLSMetaArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_meta_package_.is_valid();
}

OB_SERIALIZE_MEMBER(ObRestoreUpdateLSMetaArg, tenant_id_, ls_meta_package_);


ObCheckSrcTransferTabletsArg::ObCheckSrcTransferTabletsArg()
  : tenant_id_(OB_INVALID_ID),
    src_ls_id_(),
    tablet_info_array_()
{
}

void ObCheckSrcTransferTabletsArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  src_ls_id_.reset();
  tablet_info_array_.reset();
}


bool ObCheckSrcTransferTabletsArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && src_ls_id_.is_valid()
         && !tablet_info_array_.empty();
}

OB_SERIALIZE_MEMBER(ObCheckSrcTransferTabletsArg, tenant_id_, src_ls_id_, tablet_info_array_);


ObGetLSActiveTransCountArg::ObGetLSActiveTransCountArg()
  : tenant_id_(OB_INVALID_ID),
    src_ls_id_()
{
}

void ObGetLSActiveTransCountArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  src_ls_id_.reset();
}

bool ObGetLSActiveTransCountArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && src_ls_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObGetLSActiveTransCountArg, tenant_id_, src_ls_id_);

ObGetLSActiveTransCountRes::ObGetLSActiveTransCountRes()
  : active_trans_count_(-1)
{
}

void ObGetLSActiveTransCountRes::reset()
{
  active_trans_count_ = -1;
}

bool ObGetLSActiveTransCountRes::is_valid() const
{
  return active_trans_count_ >= 0;
}

OB_SERIALIZE_MEMBER(ObGetLSActiveTransCountRes, active_trans_count_);


ObGetTransferStartScnArg::ObGetTransferStartScnArg()
  : tenant_id_(OB_INVALID_ID),
    src_ls_id_(),
    tablet_list_()
{
}

void ObGetTransferStartScnArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  src_ls_id_.reset();
  tablet_list_.reset();
}

bool ObGetTransferStartScnArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && src_ls_id_.is_valid()
         && !tablet_list_.empty();
}

OB_SERIALIZE_MEMBER(ObGetTransferStartScnArg, tenant_id_, src_ls_id_, tablet_list_);


ObGetTransferStartScnRes::ObGetTransferStartScnRes()
  : start_scn_()
{
}

void ObGetTransferStartScnRes::reset()
{
  start_scn_.reset();
}

bool ObGetTransferStartScnRes::is_valid() const
{
  return start_scn_.is_valid();
}

OB_SERIALIZE_MEMBER(ObGetTransferStartScnRes, start_scn_);

ObStorageTransferCommonArg::ObStorageTransferCommonArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

void ObStorageTransferCommonArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObStorageTransferCommonArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
    && ls_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObStorageTransferCommonArg, tenant_id_, ls_id_);

ObTransferTabletInfoArg::ObTransferTabletInfoArg()
  : tenant_id_(OB_INVALID_ID),
    src_ls_id_(),
    dest_ls_id_(),
    tablet_list_(),
    data_version_(0)
{
}

void ObTransferTabletInfoArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  src_ls_id_.reset();
  dest_ls_id_.reset();
  tablet_list_.reset();
  data_version_ = 0;
}

int ObTransferTabletInfoArg::assign(const ObTransferTabletInfoArg &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else if (OB_FAIL(tablet_list_.assign(other.tablet_list_))) {
    LOG_WARN("failed to assign tablet list", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    src_ls_id_ = other.src_ls_id_;
    dest_ls_id_ = other.dest_ls_id_;
    data_version_ = other.data_version_;
  }
  return ret;
}

bool ObTransferTabletInfoArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && src_ls_id_.is_valid()
         && dest_ls_id_.is_valid()
         && !tablet_list_.empty();
}

OB_SERIALIZE_MEMBER(ObTransferTabletInfoArg, tenant_id_, src_ls_id_, dest_ls_id_, tablet_list_, data_version_);

ObFetchLSReplayScnArg::ObFetchLSReplayScnArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}
void ObFetchLSReplayScnArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}
bool ObFetchLSReplayScnArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid();
}
OB_SERIALIZE_MEMBER(ObFetchLSReplayScnArg, tenant_id_, ls_id_);
ObFetchLSReplayScnRes::ObFetchLSReplayScnRes()
  : replay_scn_()
{
}
void ObFetchLSReplayScnRes::reset()
{
  replay_scn_.reset();
}
bool ObFetchLSReplayScnRes::is_valid() const
{
  return replay_scn_.is_valid();
}
OB_SERIALIZE_MEMBER(ObFetchLSReplayScnRes, replay_scn_);


ObCheckTransferTabletBackfillArg::ObCheckTransferTabletBackfillArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_list_()
{
}

bool ObCheckTransferTabletBackfillArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
         && ls_id_.is_valid()
         && !tablet_list_.empty();
}

void ObCheckTransferTabletBackfillArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_list_.reset();
}

int ObCheckTransferTabletBackfillArg::assign(const ObCheckTransferTabletBackfillArg &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else if (OB_FAIL(tablet_list_.assign(other.tablet_list_))) {
    LOG_WARN("failed to assign tablet list", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObCheckTransferTabletBackfillArg, tenant_id_, ls_id_, tablet_list_);

ObCheckTransferTabletBackfillRes::ObCheckTransferTabletBackfillRes()
  : backfill_finished_(false)
{
}

void ObCheckTransferTabletBackfillRes::reset()
{
  backfill_finished_ = false;
}

OB_SERIALIZE_MEMBER(ObCheckTransferTabletBackfillRes, backfill_finished_);


ObStorageChangeMemberArg::ObStorageChangeMemberArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    need_get_config_version_(true)
{
}

bool ObStorageChangeMemberArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid();
}

void ObStorageChangeMemberArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

int ObStorageChangeMemberArg::assign(const ObStorageChangeMemberArg &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    need_get_config_version_ = other.need_get_config_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObStorageChangeMemberArg, tenant_id_, ls_id_, need_get_config_version_);

ObStorageChangeMemberRes::ObStorageChangeMemberRes()
  : config_version_(),
    transfer_scn_()
{
}

void ObStorageChangeMemberRes::reset()
{
  config_version_.reset();
  transfer_scn_.reset();
}

OB_SERIALIZE_MEMBER(ObStorageChangeMemberRes, config_version_, transfer_scn_);

ObCopyLSViewArg::ObCopyLSViewArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

void ObCopyLSViewArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
}

bool ObCopyLSViewArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ls_id_.is_valid();
}

OB_SERIALIZE_MEMBER(ObCopyLSViewArg,
    tenant_id_, ls_id_);


ObStorageWakeupTransferServiceArg::ObStorageWakeupTransferServiceArg()
  : tenant_id_(OB_INVALID_ID)
{
}

bool ObStorageWakeupTransferServiceArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_;
}

void ObStorageWakeupTransferServiceArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
}

OB_SERIALIZE_MEMBER(ObStorageWakeupTransferServiceArg, tenant_id_);


ObStorageConfigChangeOpArg::ObStorageConfigChangeOpArg()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    type_(MAX),
    lock_owner_(0),
    lock_timeout_(0)
{
}

bool ObStorageConfigChangeOpArg::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
    && ls_id_.is_valid()
    && type_ >= LOCK_CONFIG_CHANGE
    && type_ < MAX;
}

void ObStorageConfigChangeOpArg::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  type_ = MAX;
  lock_owner_ = 0;
  lock_timeout_ = 0;
}

OB_SERIALIZE_MEMBER(ObStorageConfigChangeOpArg, tenant_id_,
  ls_id_, type_, lock_owner_, lock_timeout_);

ObStorageConfigChangeOpRes::ObStorageConfigChangeOpRes()
  : palf_lock_owner_(0),
    is_locked_(false),
    op_succ_(false)
{
}

void ObStorageConfigChangeOpRes::reset()
{
  palf_lock_owner_ = 0;
  is_locked_ = false;
  op_succ_ = false;
}

OB_SERIALIZE_MEMBER(ObStorageConfigChangeOpRes, palf_lock_owner_, is_locked_, op_succ_);

template <ObRpcPacketCode RPC_CODE>
ObStorageStreamRpcP<RPC_CODE>::ObStorageStreamRpcP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : bandwidth_throttle_(bandwidth_throttle),
    last_send_time_(0),
    allocator_("SSRpcP")
{
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data(const Data &data)
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts = ObTimeUtil::current_time();
  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()
      || (curr_ts - last_send_time_ >= FLUSH_TIME_INTERVAL
          && this->result_.get_capacity() != this->result_.get_remain())) {
    LOG_INFO("flush", K(this->result_));
    if (0 == this->result_.get_position()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "data is too large", K(ret));
    } else if (OB_FAIL(flush_and_wait())) {
      STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(this->result_.get_data(),
                                      this->result_.get_capacity(),
                                      this->result_.get_position(),
                                      data))) {
      STORAGE_LOG(WARN, "failed to encode", K(ret));
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::fill_buffer(blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts = ObTimeUtil::current_time();
  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    while (OB_SUCC(ret) && data.remain() > 0) {
      if (0 == this->result_.get_remain()
          || (curr_ts - last_send_time_ >= FLUSH_TIME_INTERVAL
              && this->result_.get_capacity() != this->result_.get_remain())) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      } else {
        int64_t fill_length = std::min(this->result_.get_remain(), data.remain());
        if (fill_length <= 0) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "fill_length must larger than 0", K(ret), K(fill_length), K(this->result_), K(data));
        } else {
          MEMCPY(this->result_.get_cur_pos(), data.current(), fill_length);
          this->result_.get_position() += fill_length;
          if (OB_FAIL(data.advance(fill_length))) {
            STORAGE_LOG(WARN, "failed to advance fill length", K(ret), K(fill_length), K(data));
          }
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data_list(ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;
  const int64_t curr_ts = ObTimeUtil::current_time();

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_list.count(); ++i) {
      Data &data = data_list.at(i);
      if (data.get_serialize_size() > this->result_.get_remain()
          || (curr_ts - last_send_time_ >= FLUSH_TIME_INTERVAL
              && this->result_.get_capacity() != this->result_.get_remain())) {
        if (OB_FAIL(flush_and_wait())) {
          STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(data.serialize(this->result_.get_data(),
                                   this->result_.get_capacity(),
                                   this->result_.get_position()))) {
          STORAGE_LOG(WARN, "failed to encode data", K(ret));
        } else {
          STORAGE_LOG(DEBUG, "fill data", K(data), K(this->result_));
        }
      }
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcP<RPC_CODE>::fill_data_immediate(const Data &data)
{
  int ret = OB_SUCCESS;

  if (NULL == (this->result_.get_data())) {
    STORAGE_LOG(WARN, "fail to alloc migration data buffer.");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (serialization::encoded_length(data) > this->result_.get_remain()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data length is larger than result get_remain size, can not send",
        K(ret), K(serialization::encoded_length(data)), K(serialization::encoded_length(data)));
  } else if (OB_FAIL(serialization::encode(this->result_.get_data(),
                                           this->result_.get_capacity(),
                                           this->result_.get_position(),
                                           data))) {
    STORAGE_LOG(WARN, "failed to encode", K(ret));

  } else if (OB_FAIL(flush_and_wait())) {
    STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
  } else {
    LOG_INFO("flush", K(this->result_));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::flush_and_wait()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (NULL == bandwidth_throttle_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "bandwidth_throttle_ must not null", K(ret));
  } else {
    Thread::WaitGuard guard(Thread::WAIT_FOR_IO_EVENT);
    if (OB_SUCCESS != (tmp_ret = bandwidth_throttle_->limit_out_and_sleep(
        this->result_.get_position(), last_send_time_, max_idle_time))) {
      STORAGE_LOG(WARN, "failed limit out band", K(tmp_ret));
    }

    if (OB_FAIL(this->check_timeout())) {
      LOG_WARN("rpc is timeout, no need flush", K(ret));
    } else if (OB_FAIL(this->flush())) {
      STORAGE_LOG(WARN, "failed to flush", K(ret));
    } else {
      this->result_.get_position() = 0;
      last_send_time_ = ObTimeUtility::current_time();
    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::alloc_buffer()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!this->result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcP<RPC_CODE>::is_follower_ls(logservice::ObLogService *log_srv, ObLS *ls, bool &is_ls_follower)
{
  int ret = OB_SUCCESS;
  logservice::ObLogHandler *log_handler = nullptr;
  int64_t proposal_id = 0;
  ObRole role;
  if (OB_ISNULL(log_srv) || OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret), KP(log_srv), K(ls));
  } else if (OB_FAIL(log_srv->get_palf_role(ls->get_ls_id(), role, proposal_id))) {
    LOG_WARN("fail to get role", K(ret), "ls_id", ls->get_ls_id());
  } else if (!is_follower(role)) {
    is_ls_follower = false;
    STORAGE_LOG(WARN, "I am not follower", K(ret), K(role), K(proposal_id));
  } else {
    is_ls_follower = true;
  }
  return ret;
}

ObHAFetchMacroBlockP::ObHAFetchMacroBlockP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : ObStorageStreamRpcP(bandwidth_throttle)
  , total_macro_block_count_(0)
{
}

int ObHAFetchMacroBlockP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    blocksstable::ObBufferReader data;
    char *buf = NULL;
    last_send_time_ = this->get_receive_timestamp();
    int64_t occupy_size = 0;
    ObCopyMacroBlockHeader copy_macro_block_header;
    const int64_t start_ts = ObTimeUtil::current_time();
    const int64_t first_receive_ts = this->get_receive_timestamp();

    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle must not null", K(ret), KP_(bandwidth_throttle));
    } else {
#ifdef ERRSIM
          if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !arg_.table_key_.tablet_id_.is_ls_inner_tablet()) {
            FLOG_INFO("errsim storage ha fetch macro block", K_(arg));
            SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_macro_block");
            DEBUG_SYNC(BEFORE_MIGRATE_FETCH_MACRO_BLOCK);
          }
#endif

      SMART_VARS_2((storage::ObCopyMacroBlockObProducer, producer), (ObCopyMacroBlockRangeArg, arg)) {
        if (OB_FAIL(arg.assign(arg_))) {
          LOG_WARN("failed to assign copy macro block range arg", K(ret), K(arg_));
        } else if (OB_FAIL(producer.init(arg.tenant_id_, arg.ls_id_, arg.table_key_, arg.copy_macro_range_info_,
            arg.data_version_, arg.backfill_tx_scn_))) {
          LOG_WARN("failed to init macro block producer", K(ret), K(arg));
        } else {
          while (OB_SUCC(ret)) {
            copy_macro_block_header.reset();
            if (OB_FAIL(producer.get_next_macro_block(data, copy_macro_block_header))) {
              if (OB_ITER_END != ret) {
                STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
              } else {
                ret = OB_SUCCESS;
              }
              break;
            } else if (!copy_macro_block_header.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "copy_macro_block_header should not be invalid", K(ret), K(copy_macro_block_header));
            } else if (OB_FAIL(fill_data(copy_macro_block_header))) {
              STORAGE_LOG(WARN, "failed to fill data length", K(ret), K(data.pos()), K(copy_macro_block_header));
            } else if (OB_FAIL(fill_buffer(data))) {
              STORAGE_LOG(WARN, "failed to fill data", K(ret), K(copy_macro_block_header));
            } else {
              STORAGE_LOG(INFO, "succeed to fill macro block",
                  "idx", total_macro_block_count_);
              ++total_macro_block_count_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (arg.need_check_seq_) {
            if (OB_FAIL(compare_ls_rebuild_seq(arg.tenant_id_, arg.ls_id_, arg.ls_rebuild_seq_))) {
              LOG_WARN("failed to compare ls rebuild seq", K(ret), K(arg));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (total_macro_block_count_ != arg.copy_macro_range_info_.macro_block_count_) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR, "macro block count not match",
                K(ret), K(total_macro_block_count_), K(arg.copy_macro_range_info_));
          }
        }
      }
    }

    LOG_INFO("finish fetch macro block", K(ret), K(total_macro_block_count_),
        "cost_ts", ObTimeUtil::current_time() - start_ts,
        "in rpc queue time", start_ts - first_receive_ts);
  }
  return ret;
}

ObFetchTabletInfoP::ObFetchTabletInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchTabletInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    char * buf = NULL;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
    ObCopyTabletInfoObProducer producer;
    ObCopyTabletInfo tablet_info;
    int64_t max_tablet_num = 32;
    int64_t tablet_count = 0;
    const int64_t start_ts = ObTimeUtil::current_time();
    const int64_t first_receive_ts = this->get_receive_timestamp();

    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      const int64_t tmp_max_tablet_num = tenant_config->_ha_tablet_info_batch_count;
      if (0 != tmp_max_tablet_num) {
        max_tablet_num = tmp_max_tablet_num;
      }
    }

    LOG_INFO("start to fetch tablet info", K(arg_));

    last_send_time_ = this->get_receive_timestamp();
    const int64_t cost_time = 10 * 1000 * 1000;
    common::ObTimeGuard timeguard("ObFetchTabletInfoP", cost_time);
    timeguard.click();
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                  KP_(bandwidth_throttle));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
      LOG_WARN("failed to log stream get migration status", K(ret), K(migration_status));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
    } else if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_list_))) {
      LOG_WARN("failed to init copy tablet info producer", K(ret), K(arg_));
    } else {
#ifdef ERRSIM
      if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !is_copy_ls_inner_tablet(arg_.tablet_id_list_)) {
        FLOG_INFO("errsim storage ha fetch tablet info", K_(arg));
        SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_tablet_info");
        DEBUG_SYNC(BEFORE_MIGRATE_FETCH_TABLET_INFO);
      }
#endif

      while (OB_SUCC(ret)) {
        tablet_info.reset();
        if (OB_FAIL(producer.get_next_tablet_info(tablet_info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            STORAGE_LOG(WARN, "failed to get next tablet meta info", K(ret));
          }
        } else if (tablet_count >= max_tablet_num) {
          timeguard.click();
          if (this->result_.get_position() > 0 && OB_FAIL(flush_and_wait())) {
            LOG_WARN("failed to flush and wait", K(ret), K(tablet_info));
          } else {
            tablet_count = 0;
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(fill_data(tablet_info))) {
          STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(tablet_info));
        } else {
          tablet_count++;
        }
      }
      timeguard.click();
      if (OB_SUCC(ret)) {
        if (arg_.need_check_seq_) {
          if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
            LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
          }
        }
      }
    }

    LOG_INFO("finish fetch tablet info", K(ret), "cost_ts", ObTimeUtil::current_time() - start_ts,
        "in rpc queue time", start_ts - first_receive_ts);
  }
  return ret;
}

ObFetchSSTableInfoP::ObFetchSSTableInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchSSTableInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    char * buf = NULL;
    ObCopyTabletSSTableInfo sstable_info;
    ObMigrationStatus migration_status;
    ObLS *ls = nullptr;
    const int64_t start_ts = ObTimeUtil::current_time();
    const int64_t first_receive_ts = this->get_receive_timestamp();
    LOG_INFO("start to fetch tablet sstable info", K(arg_));

    last_send_time_ = this->get_receive_timestamp();

    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                  KP_(bandwidth_throttle));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(arg_));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
    } else if (OB_FAIL(build_tablet_sstable_info_(ls))) {
      LOG_WARN("failed to build tablet sstable info", K(ret), K(arg_));
    } else {
      if (arg_.need_check_seq_) {
        if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
          LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
        }
      }
    }
    LOG_INFO("finish fetch sstable info", K(ret), "cost_ts", ObTimeUtil::current_time() - start_ts,
        "in rpc queue time", start_ts - first_receive_ts);
  }
  return ret;
}

int ObFetchSSTableInfoP::build_tablet_sstable_info_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObCopyTabletsSSTableInfoObProducer producer;
  obrpc::ObCopyTabletSSTableInfoArg arg;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet sstable info get invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_sstable_info_arg_list_))) {
    LOG_WARN("failed to init copy tablets sstable info ob producer", K(ret), K(arg_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(producer.get_next_tablet_sstable_info(arg))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet sstable info", K(ret), K(arg_));
        }
      } else if (OB_FAIL(build_sstable_info_(arg, ls))) {
        LOG_WARN("failed to get next tablet sstable info", K(arg));
      }
    }
  }
  return ret;
}

int ObFetchSSTableInfoP::build_sstable_info_(
    const obrpc::ObCopyTabletSSTableInfoArg &arg,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObCopySSTableInfoObProducer producer;
  obrpc::ObCopyTabletSSTableInfo sstable_info;
  obrpc::ObCopyTabletSSTableHeader tablet_sstable_header;

  if (!arg.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next tablet sstable info get invalid argument", K(ret), K(arg), KP(ls));
  } else if (OB_FAIL(producer.init(arg, ls))) {
    LOG_WARN("failed to init copy sstable info ob producer", K(ret), K(arg));
  } else if (OB_FAIL(producer.get_copy_tablet_sstable_header(tablet_sstable_header))) {
    LOG_WARN("failed to get copy tablet sstable header", K(ret), K(arg));
  } else if (OB_FAIL(fill_data(tablet_sstable_header))) {
    LOG_WARN("failed to fill tablet sstable header", K(ret), K(arg));
  } else if (0 == tablet_sstable_header.sstable_count_) {
    //do nothing
  } else {
    while (OB_SUCC(ret)) {
      sstable_info.reset();
      if (OB_FAIL(producer.get_next_sstable_info(sstable_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next sstable info", K(ret), K(arg));
        }
      } else if (OB_FAIL(fill_data(sstable_info))) {
        STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(sstable_info));
      }
    }
  }
  return ret;
}


ObFetchLSInfoP::ObFetchLSInfoP()
{
}


int ObFetchLSInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogHandler *log_handler = nullptr;
    ObRole role;
    int64_t proposal_id = 0;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObMigrationStatus migration_status;
    ObLSMetaPackage ls_meta_package;
    bool is_need_rebuild = false;
    bool is_log_sync = false;
    const bool check_archive = true;

    LOG_INFO("start to fetch log stream info", K(arg_.ls_id_), K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetch ls info get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot be migrate src", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("faield to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret), KP(log_handler), K(arg_));
    } else if (OB_FAIL(ls->get_ls_meta_package_and_tablet_ids(check_archive,
            result_.ls_meta_package_, result_.tablet_id_array_))) {
      LOG_WARN("failed to get ls meta package and tablet ids", K(ret));
    } else if (OB_FAIL(result_.ls_meta_package_.ls_meta_.get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(result_));
    } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status) || ls->is_stopped()
        || ls->is_offline()) {
      ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
      STORAGE_LOG(WARN, "src migration status do not allow to migrate out", K(ret), "src migration status",
          migration_status, KPC(ls));
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(result_.version_))) {
      LOG_WARN("failed to get server version", K(ret), K_(arg));
    } else if (OB_FAIL(log_handler->get_role(role, proposal_id))) {
      LOG_WARN("failed to get role", K(ret), K(arg_));
    } else if (is_strong_leader(role)) {
      result_.is_log_sync_ = true;
    } else if (OB_FAIL(log_handler->is_in_sync(is_log_sync, is_need_rebuild))) {
      LOG_WARN("failed to check is in sync", K(ret), K(arg_));
    } else if (!is_log_sync || is_need_rebuild) {
      result_.is_log_sync_ = false;
    } else {
      result_.is_log_sync_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(DEBUG, "succ to get partition group info", K_(result), K(ret));
  }
  return ret;

}

ObFetchLSMetaInfoP::ObFetchLSMetaInfoP()
{
}


int ObFetchLSMetaInfoP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogHandler *log_handler = nullptr;
    ObLSMetaPackage ls_meta_package;
    const bool check_archive = true;
    LOG_INFO("start to fetch log stream info", K(arg_.ls_id_), K(arg_));
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fetch ls info get invalid argument", K(ret), K(arg_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("faield to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret), KP(log_handler), K(arg_));
    } else if (OB_FAIL(ls->get_ls_meta_package(check_archive, result_.ls_meta_package_))) {
      LOG_WARN("failed to get ls meta package", K(ret), K(arg_));
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(result_.version_))) {
      LOG_WARN("failed to get server version", K(ret), K_(arg));
    } else if (OB_FAIL(check_has_transfer_logical_table_(ls))) {
      LOG_WARN("failed to check has tranfer logical table", K(ret));
    }
  }
  return ret;
}

int ObFetchLSMetaInfoP::check_has_transfer_logical_table_(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObHasTransferTableFilterOp op;
  ObLSTabletFastIter tablet_iter(op, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls not should be null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->build_tablet_iter(tablet_iter, true/*except_inner*/))) {
    LOG_WARN("failed to build ls tablet iter", K(ret));
  } else {
    bool has_logical_table = true;
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      tablet = nullptr;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), KPC(ls));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else {
        result_.has_transfer_table_ = true;
        LOG_INFO("tablet still has logical table", K(tablet_handle));
        break;
      }
    }
  }
  return ret;
}

ObFetchLSMemberListP::ObFetchLSMemberListP()
{
}

int ObFetchLSMemberListP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  MTL_SWITCH(tenant_id) {
    ObLSService *ls_svr = NULL;
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    common::ObMemberList member_list;
    int64_t paxos_replica_num = 0;
    logservice::ObLogService *log_service = nullptr;
    ObRole role;
    int64_t proposal_id = 0;
    ObAddr election_leader;
    if (tenant_id != MTL_ID()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc get member list tenant not match", K(ret), K(tenant_id));
    } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service should not be NULL", K(ret), KP(log_service));
    } else if (OB_FAIL(log_service->get_palf_role(ls_id, role, proposal_id))) {
      LOG_WARN("failed to get role", K(ret), "arg", arg_);
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", K(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret));
    } else if (OB_FAIL(log_handler->get_election_leader(election_leader))) {
      LOG_WARN("failed to get election leader", K(ret), KPC(ls));
    } else if (!is_strong_leader(role) && GCTX.self_addr() != election_leader) {
      ret = OB_LS_NOT_LEADER;
      LOG_WARN("ls is not leader, cannot get member list", K(ret), K(role), K(arg_),
          K(election_leader), "self", GCTX.self_addr());
    } else if (OB_FAIL(log_handler->get_paxos_member_list(member_list, paxos_replica_num))) {
      LOG_WARN("failed to get paxos member list", K(ret));
    } else if (OB_FAIL(result_.member_list_.deep_copy(member_list))) {
      LOG_WARN("failed to assign", K(ret), K(member_list));
    }
  }
  return ret;
}

ObFetchLSMemberAndLearnerListP::ObFetchLSMemberAndLearnerListP()
{
}

int ObFetchLSMemberAndLearnerListP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  MTL_SWITCH(tenant_id) {
    ObLSService *ls_svr = NULL;
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    logservice::ObLogHandler *log_handler = NULL;
    common::ObMemberList member_list;
    int64_t paxos_replica_num = 0;
    logservice::ObLogService *log_service = nullptr;
    ObRole role;
    int64_t proposal_id = 0;
    common::GlobalLearnerList learner_list;
    if (tenant_id != MTL_ID()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc get member list tenant not match", K(ret), K(tenant_id));
    } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log service should not be NULL", K(ret), KP(log_service));
    } else if (OB_FAIL(log_service->get_palf_role(ls_id, role, proposal_id))) {
      LOG_WARN("failed to get role", K(ret), "arg", arg_);
    } else if (!is_strong_leader(role)) {
      ret = OB_PARTITION_NOT_LEADER;
      LOG_WARN("ls is not leader, cannot get member list", K(ret), K(role), K(arg_));
    } else if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret));
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be null", K(ret));
    } else if (OB_ISNULL(log_handler = ls->get_log_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log handler should not be NULL", K(ret));
    } else if (OB_FAIL(log_handler->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list))) {
      LOG_WARN("failed to get paxos member list and learner list", K(ret));
    } else if (OB_FAIL(result_.member_list_.deep_copy(member_list))) {
      LOG_WARN("failed to assign member list", K(ret), K(member_list));
    } else if (OB_FAIL(result_.learner_list_.deep_copy(learner_list))) {
      LOG_WARN("failed to assign learner list", K(ret), K(learner_list));
    }

  }

  return ret;
}

ObFetchSSTableMacroInfoP::ObFetchSSTableMacroInfoP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObFetchSSTableMacroInfoP::process()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  char * buf = NULL;
  ObMigrationStatus migration_status;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t first_receive_ts = this->get_receive_timestamp();
  LOG_INFO("start to fetch sstable macro info", K(arg_));

  last_send_time_ = this->get_receive_timestamp();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  if (OB_FAIL(guard.switch_to(arg_.tenant_id_))) {
    LOG_ERROR("switch tenant fail", K(ret), K(arg_));
  } else if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc migrate data buffer.", K(ret));
  } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed set data to result", K(ret));
  } else if (OB_ISNULL(bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "bandwidth_throttle_ must not null", K(ret),
                KP_(bandwidth_throttle));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(arg_));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(arg_));
  } else if (!ObMigrationStatusHelper::check_can_migrate_out(migration_status)) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    STORAGE_LOG(WARN, "src migrate status do not allow migrate out", K(ret), K(migration_status));
  } else if (OB_FAIL(fetch_sstable_macro_info_header_())) {
    LOG_WARN("failed to fetch sstable macro info header", K(ret), K(arg_));
  } else {
    if (arg_.need_check_seq_) {
      if (OB_FAIL(compare_ls_rebuild_seq(arg_.tenant_id_, arg_.ls_id_, arg_.ls_rebuild_seq_))) {
        LOG_WARN("failed to compare ls rebuild seq", K(ret), K_(arg));
      }
    }
  }

  LOG_INFO("finish fetch sstable macro info", K(ret), "cost_ts", ObTimeUtil::current_time() - start_ts,
      "in rpc queue time", start_ts - first_receive_ts);
  return ret;
}

int ObFetchSSTableMacroInfoP::fetch_sstable_macro_info_header_()
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroObProducer producer;
  obrpc::ObCopySSTableMacroRangeInfoHeader header;
  if (OB_FAIL(producer.init(arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_,
      arg_.copy_table_key_array_, arg_.macro_range_max_marco_count_))) {
    LOG_WARN("failed to init copy tablet info producer", K(ret), K(arg_));
  } else {
#ifdef ERRSIM
    if (!is_meta_tenant(arg_.tenant_id_) && 1001 == arg_.ls_id_.id() && !arg_.tablet_id_.is_ls_inner_tablet()) {
      FLOG_INFO("errsim storage ha fetch sstable info", K_(arg));
      SERVER_EVENT_SYNC_ADD("errsim_storage_ha", "fetch_sstable_info");
      DEBUG_SYNC(BEFORE_MIGRATE_FETCH_SSTABLE_MACRO_INFO);
    }
#endif
    while (OB_SUCC(ret)) {
      header.reset();
      if (OB_FAIL(producer.get_next_sstable_macro_range_info(header))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "failed to get next sstable macro range info", K(ret));
        }
      } else if (OB_FAIL(fill_data(header))) {
        STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(header));
      } else if (OB_FAIL(fetch_sstable_macro_range_info_(header))) {
        LOG_WARN("failed to fetch sstable macro range info", K(ret), K(header));
      }
    }
  }
  return ret;
}

int ObFetchSSTableMacroInfoP::fetch_sstable_macro_range_info_(const obrpc::ObCopySSTableMacroRangeInfoHeader &header)
{
  int ret = OB_SUCCESS;

  if (!header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch sstable macro range info get invalid argument", K(ret) ,K(header));
  } else {
    SMART_VARS_2((ObCopySSTableMacroRangeObProducer, macro_range_producer), (ObCopyMacroRangeInfo, macro_range_info)) {
      if (OB_FAIL(macro_range_producer.init(
          arg_.tenant_id_, arg_.ls_id_, arg_.tablet_id_, header, arg_.macro_range_max_marco_count_))) {
        LOG_WARN("failed to init macro range producer", K(ret), K(arg_), K(header));
      } else {
        while (OB_SUCC(ret)) {
          macro_range_info.reuse();
          if (OB_FAIL(macro_range_producer.get_next_macro_range_info(macro_range_info))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next macro range info", K(ret), K(header), K(arg_));
            }
          } else if (OB_FAIL(fill_data(macro_range_info))) {
            LOG_WARN("failed to fill macro range info", K(ret), K(macro_range_info), K(arg_));
          }
        }
      }
    }
  }
  return ret;
}

ObCheckStartTransferTabletsDelegate::ObCheckStartTransferTabletsDelegate()
  : is_inited_(false),
    arg_()
{}

int ObCheckStartTransferTabletsDelegate::init(const obrpc::ObTransferTabletInfoArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("check start transfer tablets delegate init twice", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("failed to assign arg", K(ret), K(arg));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCheckStartTransferTabletsDelegate::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check src transfer tablets get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_FAIL(ObStorageHAUtils::check_disk_space())) {
      LOG_WARN("failed to check disk space", K(ret), K(arg_));
    } else if (OB_FAIL(check_start_transfer_out_tablets_())) {
      LOG_WARN("failed to check start transfer out tablets", K(ret), K(arg_));
    } else if (OB_FAIL(check_start_transfer_in_tablets_())) {
      LOG_WARN("failed to check start transfer in tablets", K(ret), K(arg_));
    }
  }
  return ret;
}

// In addition to the tablet in the recovery process, if the major sstable does not exist on the tablet, the transfer start will fail.
// For tablets with ddl sstable, you need to wait for ddl merge to complete
int ObCheckStartTransferTabletsDelegate::check_transfer_out_tablet_sstable_(const ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_iter;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  const int64_t emergency_sstable_count = ObTabletTableStore::EMERGENCY_SSTABLE_CNT;

  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is null", K(ret));
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KP(tablet));
  } else if (!wrapper.get_member()->get_major_sstables().empty()) {
    // do nothing
  } else if (wrapper.get_member()->get_table_count() > emergency_sstable_count) {
    ret = OB_TOO_MANY_SSTABLE;
    LOG_WARN("transfer src tablet has too many sstable, cannot transfer, need retry", K(ret),
        "table_count", wrapper.get_member()->get_table_count(), "emergency sstable count", emergency_sstable_count);
  } else if (OB_FAIL(tablet->get_ddl_sstables(ddl_iter))) {
    LOG_WARN("failed to get ddl sstable", K(ret));
  } else if (ddl_iter.is_valid()) { // indicates the existence of ddl sstable
    ret = OB_MAJOR_SSTABLE_NOT_EXIST;
    LOG_WARN("major sstable do not exit, need to wait ddl merge", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_WARN("neither major sstable nor ddl sstable exists", K(ret), K(ddl_iter));
  }
  return ret;
}

int ObCheckStartTransferTabletsDelegate::check_start_transfer_out_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (arg_.data_version_ != CLUSTER_CURRENT_VERSION) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_WARN("transfer data version is not match with cluster current version, cannot do transfer", K(ret), K(arg_),
        K(CLUSTER_CURRENT_VERSION));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(arg_.src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(arg_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls should not be NULL", K(ret), K(arg_), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(arg_));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("src ls migration status is not none", K(ret), K(migration_status), KPC(ls), K(arg_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &tablet_info = arg_.tablet_list_.at(i);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      ObTabletCreateDeleteMdsUserData user_data;
      bool committed_flag = false;
      if (!tablet_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet info is invalid", K(ret), K(tablet_info), K(user_data), K(arg_));
      } else if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0,
          ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, committed_flag))) {
        LOG_WARN("failed to get lastest tablet status", K(ret), KPC(tablet));
      } else if (!committed_flag) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("transfer src tablet still has uncommitted user data", K(ret), K(user_data), KPC(tablet));
      } else if (ObTabletStatus::NORMAL != user_data.tablet_status_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("transfer src tablet status is not match", K(ret), KPC(tablet), K(tablet_info), K(user_data));
      } else if (tablet_info.transfer_seq_ != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
        ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
        LOG_WARN("tablet transfer seq is is not match", K(ret), KPC(tablet), K(tablet_info), K(user_data));
      } else if (OB_FAIL(check_transfer_out_tablet_sstable_(tablet))) {
        LOG_WARN("failed to check sstable", K(ret), KPC(tablet), K(user_data));
      }
    }
  }
  return ret;
}

int ObCheckStartTransferTabletsDelegate::check_start_transfer_in_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(arg_.dest_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(arg_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls should not be NULL", K(ret), K(arg_), KP(ls));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(arg_));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("src ls migration status is not none", K(ret), K(migration_status), KPC(ls), K(arg_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.tablet_list_.count(); ++i) {
      const ObTransferTabletInfo &tablet_info = arg_.tablet_list_.at(i);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0,
          ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (!tablet->is_empty_shell()) {
        ret = OB_EAGAIN;
        LOG_WARN("dest ls in start status should not exist transfer tablet, need retry", K(ret), KPC(tablet), K(tablet_info));
      } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ > tablet_info.transfer_seq_ + 1) {
        ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
        LOG_WARN("tablet is in empty shell but transfer seq not match", K(ret), KPC(tablet), K(tablet_info));
      }
    }
  }
  return ret;
}

ObNotifyRestoreTabletsP::ObNotifyRestoreTabletsP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}

int ObNotifyRestoreTabletsP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    logservice::ObLogService *log_srv = nullptr;
    int64_t disk_abnormal_time = 0;
    bool is_follower = false;

    LOG_INFO("start to notify follower restore tablets", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore tablets get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", K(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_srv = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log srv should not be null", K(ret), KP(log_srv));
    } else if (OB_FAIL(is_follower_ls(log_srv, ls, is_follower))) {
      LOG_WARN("failed to check is follower", K(ret), KP(ls), K(arg_));
    } else if (!is_follower) {
      ret = OB_NOT_FOLLOWER;
      STORAGE_LOG(WARN, "I am not follower", K(ret), K(arg_));
    } else {
      ObLSRestoreHandler *ls_restore_handler = ls->get_ls_restore_handler();
      if (OB_FAIL(ls_restore_handler->handle_pull_tablet(
          arg_.tablet_id_array_, arg_.restore_status_, arg_.leader_proposal_id_))) {
        LOG_WARN("fail to handle pull tablet", K(ret), K(arg_));
      } else if (OB_FAIL(ls->get_restore_status(result_.restore_status_))) {
        LOG_WARN("fail to get restore status", K(ret));
      } else {
        result_.tenant_id_ = arg_.tenant_id_;
        result_.ls_id_ = arg_.ls_id_;
      }
    }
  }
  return ret;
}


ObInquireRestoreP::ObInquireRestoreP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}

int ObInquireRestoreP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    logservice::ObLogService *log_srv = nullptr;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    bool is_follower = false;

    LOG_INFO("start to inquire restore status", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif

    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls), K(arg_));
    } else if (OB_ISNULL(log_srv = MTL(logservice::ObLogService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log srv should not be null", K(ret), KP(log_srv));
    } else if (OB_FAIL(is_follower_ls(log_srv, ls, is_follower))) {
      LOG_WARN("failed to check is follower", K(ret), KP(ls), K(arg_));
    } else if (OB_FAIL(ls->get_restore_status(result_.restore_status_))) {
      LOG_WARN("fail to get restore status", K(ret));
    } else if (is_follower) {
      result_.tenant_id_ = arg_.tenant_id_;
      result_.ls_id_ = arg_.ls_id_;
      result_.is_leader_ = false;
      LOG_INFO("succ to inquire restore status from follower", K(result_));
    } else {
      result_.tenant_id_ = arg_.tenant_id_;
      result_.ls_id_ = arg_.ls_id_;
      result_.is_leader_ = true;
      LOG_INFO("succ to inquire restore status from leader", K(ret), K(arg_), K(result_));
    }
  }
  return ret;
}

ObUpdateLSMetaP::ObUpdateLSMetaP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{

}

int ObUpdateLSMetaP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    bool is_follower = false;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;

    LOG_INFO("start to update ls meta", K(arg_));

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("notify follower restore get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->restore_update_ls(arg_.ls_meta_package_))) {
      LOG_WARN("failed to get log stream", K(ret), K(arg_));
    } else {
      LOG_INFO("succ to update ls meta", K(ret), K(arg_));
    }
  }
  return ret;
}

ObLobQueryP::ObLobQueryP(common::ObInOutBandwidthThrottle *bandwidth_throttle)
  : ObStorageStreamRpcP(bandwidth_throttle)
{
  // the streaming interface may return multi packet. The memory may be freed after the first packet has been sended.
  // the deserialization of arg_ is shallow copy, so we need deep copy data to processor
  set_preserve_recv_data();
}

int ObLobQueryP::process_read()
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobQueryBlock header;
  blocksstable::ObBufferReader data;
  char *out_buf = nullptr;
  int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN - sizeof(ObLobQueryBlock);
  if (OB_ISNULL(out_buf = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc out data buffer.", K(ret));
  } else {
    ObString out;
    ObLobAccessParam param;
    param.scan_backward_ = arg_.scan_backward_;
    param.from_rpc_ = true;
    ObLobQueryIter *iter = nullptr;
    int64_t timeout = rpc_pkt_->get_timeout() + get_send_timestamp();
    if (OB_FAIL(lob_mngr->build_lob_param(param, allocator_, arg_.cs_type_, arg_.offset_,
        arg_.len_, timeout, arg_.lob_locator_))) {
      LOG_WARN("failed to build lob param", K(ret));
    } else if (OB_FAIL(lob_mngr->query(param, iter))) {
      LOG_WARN("failed to query lob.", K(ret), K(param));
    } else {
      while (OB_SUCC(ret)) {
        out.assign_buffer(out_buf, buf_len);
        if (OB_FAIL(iter->get_next_row(out))) {
          if (OB_ITER_END != ret) {
            STORAGE_LOG(WARN, "failed to get next buffer", K(ret));
          }
        } else {
          header.size_ = out.length();
          data.assign(out.ptr(), out.length());
          // only scan backward need header
          if (OB_FAIL(fill_data(header))) {
            STORAGE_LOG(WARN, "failed to fill header", K(ret), K(header));
          } else if (OB_FAIL(fill_buffer(data))) {
            STORAGE_LOG(WARN, "failed to fill buffer", K(ret), K(data));
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      OB_DELETE(ObLobQueryIter, "unused", iter);
    }
  }
  return ret;
}

int ObLobQueryP::process_getlength()
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobQueryBlock header;
  blocksstable::ObBufferReader data;
  ObLobAccessParam param;
  param.scan_backward_ = arg_.scan_backward_;
  param.from_rpc_ = true;
  header.reset();
  uint64_t len = 0;
  int64_t timeout = rpc_pkt_->get_timeout() + get_send_timestamp();
  if (OB_FAIL(lob_mngr->build_lob_param(param, allocator_, arg_.cs_type_, arg_.offset_,
      arg_.len_, timeout, arg_.lob_locator_))) {
    LOG_WARN("failed to build lob param", K(ret));
  } else if (OB_FAIL(lob_mngr->getlength(param, len))) { // reuse size_ for lob_len
    LOG_WARN("failed to getlength lob.", K(ret), K(param));
  } else if (FALSE_IT(header.size_ = static_cast<int64_t>(len))) {
  } else if (OB_FAIL(fill_data(header))) {
    STORAGE_LOG(WARN, "failed to fill header", K(ret), K(header));
  }
  return ret;
}

int ObLobQueryP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLobManager *lob_mngr = MTL(ObLobManager*);
    // init result_
    char *buf = nullptr;
    int64_t buf_len = ObLobQueryArg::OB_LOB_QUERY_BUFFER_LEN;
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc result data buffer.", K(ret));
    } else if (!result_.set_data(buf, buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed set data to result", K(ret));
    } else if (!arg_.lob_locator_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "lob locator is invalid", K(ret));
    } else if (!arg_.lob_locator_.is_persist_lob()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupport remote query non-persist lob.", K(ret), K(arg_.lob_locator_));
    } else if (arg_.qtype_ == ObLobQueryArg::QueryType::READ) {
      if (OB_FAIL(process_read())) {
        LOG_WARN("fail to process read", K(ret), K(arg_));
      }
    } else if (arg_.qtype_ == ObLobQueryArg::QueryType::GET_LENGTH) {
      if (OB_FAIL(process_getlength())) {
        LOG_WARN("fail to process read", K(ret), K(arg_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg qtype.", K(ret), K(arg_));
    }
  }
  return ret;
}

ObGetLSActiveTransCountP::ObGetLSActiveTransCountP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObGetLSActiveTransCountP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    bool is_follower = false;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get ls active trans count get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls should not be NULL", K(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
      LOG_WARN("failed to get migration status", K(ret), K(arg_));
    } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls migration status is not none", K(ret), K(migration_status), KPC(ls), K(arg_));
    } else if (OB_FAIL(ls->get_active_tx_count(result_.active_trans_count_))) {
      LOG_WARN("failed to get active trans count", K(ret), KPC(ls), K(arg_));
    } else if (0 == result_.active_trans_count_) {
      // do nothing
    } else if (OB_FAIL(ls->print_all_tx_ctx(1/*print_num*/))) {
      LOG_WARN("failed to print all tx ctx", K(ret), KPC(ls));
    }
  }
  return ret;
}

ObGetTransferStartScnP::ObGetTransferStartScnP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObGetTransferStartScnP::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    bool is_follower = false;
    ObDeviceHealthStatus dhs = DEVICE_HEALTH_NORMAL;
    int64_t disk_abnormal_time = 0;
    ObTabletHandle tablet_handle;
    ObTabletCreateDeleteMdsUserData user_data;

#ifdef ERRSIM
    if (OB_SUCC(ret) && DEVICE_HEALTH_NORMAL == dhs && GCONF.fake_disk_error) {
      dhs = DEVICE_HEALTH_ERROR;
    }
#endif
    if (!arg_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get ls active trans count get invalid argument", K(ret), K(arg_));
    } else if (DEVICE_HEALTH_NORMAL == dhs
        && OB_FAIL(ObIOManager::get_instance().get_device_health_status(dhs, disk_abnormal_time))) {
      STORAGE_LOG(WARN, "failed to check is disk error", KR(ret));
    } else if (DEVICE_HEALTH_ERROR == dhs) {
      ret = OB_DISK_ERROR;
      STORAGE_LOG(ERROR, "observer has disk error, cannot restore", KR(ret),
          "disk_health_status", device_health_status_to_str(dhs), K(disk_abnormal_time));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.src_ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls should not be NULL", K(ret), K(arg_), KP(ls));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < arg_.tablet_list_.count(); ++i) {
        const ObTransferTabletInfo &tablet_info = arg_.tablet_list_.at(i);
        ObTablet *tablet = nullptr;
        if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0,
            ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("failed to get tablet", K(ret), K(arg_));
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
        } else if (tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet_info.transfer_seq_) {
          ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
          LOG_WARN("transfer tablet seq is unexpected", K(ret), K(user_data), K(tablet_info), KPC(tablet));
        } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet_handle, user_data))) {
          LOG_WARN("failed to get tablet status", K(ret), K(tablet_info));
        } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_) {
          if (ObTabletStatus::NORMAL == user_data.tablet_status_) {
            //tablet status is normal, set start_scn min which means get start scn need retry.
            result_.start_scn_.set_min();
            LOG_INFO("tablet status is normal, get min start scn", K(result_), K(tablet_handle), K(user_data));
            break;
          }
        } else {
          if (i > 0) {
            if (user_data.transfer_scn_ != result_.start_scn_) {
              ret = OB_EAGAIN;
              LOG_WARN("tx data is not same, need retry", K(ret), K(tablet_handle), K(user_data), K(result_));
            }
          } else if (user_data.transfer_scn_.is_min()) {
            result_.start_scn_.set_min();
            LOG_INFO("tablet status is transfer out, but on_redo is not  executed. Retry is required");
            break;
          } else {
            result_.start_scn_ = user_data.transfer_scn_;
            LOG_INFO("succeed get start scn", K(result_), K(user_data));
          }
        }
      }
    }
  }
  return ret;
}

OFetchLSReplayScnDelegate::OFetchLSReplayScnDelegate(obrpc::ObFetchLSReplayScnRes &result)
  : is_inited_(false),
    arg_(),
    result_(result)
{
}

int OFetchLSReplayScnDelegate::init(
    const obrpc::ObFetchLSReplayScnArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fetch ls replay scn delegate init twice", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(arg));
  } else {
    arg_ = arg;
    is_inited_ = true;
  }
  return ret;
}

int OFetchLSReplayScnDelegate::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    SCN max_decided_scn;
    LOG_INFO("start to fetch ls replay scn", K(arg_));
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("failed to log stream get migration status", K(ret), K_(arg));
    } else {
      result_.replay_scn_ = max_decided_scn;
      LOG_INFO("get ls replay scn", K(max_decided_scn), K(arg_));
    }
  }
  return ret;
}

int ObFetchLSReplayScnP::process()
{
  int ret = OB_SUCCESS;
  OFetchLSReplayScnDelegate delegate(result_);
  if (OB_FAIL(delegate.init(arg_))) {
    LOG_WARN("failed to init delegate", K(ret));
  } else if (OB_FAIL(delegate.process())) {
    LOG_WARN("failed to do process", K(ret), K_(arg));
  }
  return ret;
}

ObCheckTransferTabletsBackfillDelegate::ObCheckTransferTabletsBackfillDelegate(obrpc::ObCheckTransferTabletBackfillRes &result)
  : is_inited_(false),
    arg_(),
    result_(result)
{}

int ObCheckTransferTabletsBackfillDelegate::init(
    const obrpc::ObCheckTransferTabletBackfillArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("check start transfer backfill delegate init twice", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("failed to assign arg", K(ret), K(arg));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCheckTransferTabletsBackfillDelegate::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    ObTransferService *transfer_service = NULL;
    LOG_INFO("check transfer tablet", K(arg_));
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer service should not be null", K(ret), K_(arg));
    } else {
      bool backfill_finished = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < arg_.tablet_list_.count(); ++i) {
        bool has_transfer_table = false;
        const ObTransferTabletInfo &tablet_info = arg_.tablet_list_.at(i);
        if (OB_FAIL(check_has_transfer_table_(tablet_info, ls, has_transfer_table))) {
          LOG_WARN("failed to check has transfer table", K(ret), K(tablet_info));
        } else if (has_transfer_table) {
          backfill_finished = false;
          LOG_INFO("[TRANSFER]tablet still has transfer table, backfill not finished", K(tablet_info), K(arg_));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        result_.backfill_finished_ = backfill_finished;
        if (!backfill_finished) {
          transfer_service->wakeup();
        }
        LOG_INFO("[TRANSFER]check backfill tx finish", K(backfill_finished), K(arg_.tablet_list_), K(ls->get_ls_id()));
      }
    }
  }
  return ret;
}

int ObCheckTransferTabletsBackfillP::process()
{
  int ret = OB_SUCCESS;
  ObCheckTransferTabletsBackfillDelegate delegate(result_);
  if (OB_FAIL(delegate.init(arg_))) {
    LOG_WARN("failed to init delegate", K(ret));
  } else if (OB_FAIL(delegate.process())) {
    LOG_WARN("failed to do process", K(ret), K_(arg));
  }
  return ret;
}

int ObStorageGetConfigVersionAndTransferScnP::process()
{
  int ret = OB_SUCCESS;
  ObStorageGetConfigVersionAndTransferScnDelegate delegate(result_);
  if (OB_FAIL(delegate.init(arg_))) {
    LOG_WARN("failed to init delegate", K(ret));
  } else if (OB_FAIL(delegate.process())) {
    LOG_WARN("failed to do process", K(ret), K_(arg));
  }
  return ret;
}

int ObCheckStartTransferTabletsP::process()
{
  int ret = OB_SUCCESS;
  ObCheckStartTransferTabletsDelegate delegate;
  if (OB_FAIL(delegate.init(arg_))) {
    LOG_WARN("failed to init delegate", K(ret));
  } else if (OB_FAIL(delegate.process())) {
    LOG_WARN("failed to do process", K(ret), K_(arg));
  }
  return ret;
}

int ObCheckTransferTabletsBackfillDelegate::check_has_transfer_table_(
    const ObTransferTabletInfo &tablet_info, storage::ObLS *ls, bool &has_transfer_table)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  has_transfer_table = false;
  if (OB_ISNULL(ls) || !tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null or tablet info is invalid", K(ret), K(tablet_info));
  } else if (OB_FAIL(ls->get_tablet(tablet_info.tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (tablet_info.transfer_seq_ + 1 != tablet->get_tablet_meta().transfer_info_.transfer_seq_) {
    ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
    LOG_WARN("tablet transfer seq not match", K(ret), K(tablet_info), "transfer_seq", tablet->get_tablet_meta().transfer_info_.transfer_seq_);
  } else if (tablet->get_tablet_meta().has_transfer_table()) {
    has_transfer_table = true;
    LOG_INFO("transfer table exist", K(tablet_info), K(tablet->get_tablet_meta()));
  }
  return ret;
}

ObStorageGetConfigVersionAndTransferScnDelegate::ObStorageGetConfigVersionAndTransferScnDelegate(obrpc::ObStorageChangeMemberRes &result)
  : is_inited_(false),
    arg_(),
    result_(result)
{
}

int ObStorageGetConfigVersionAndTransferScnDelegate::init(const obrpc::ObStorageChangeMemberArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("check start transfer backfill delegate init twice", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(arg_.assign(arg))) {
    LOG_WARN("failed to assign arg", K(ret), K(arg));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStorageGetConfigVersionAndTransferScnDelegate::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  const bool need_get_config_version = arg_.need_get_config_version_;
#ifdef ERRSIM
  if (!GCONF.migrate_check_member_list_error_zone.get_value_string().empty()) {
    if (0 == strcmp(GCONF.zone.str(), GCONF.migrate_check_member_list_error_zone.str())) {
      SERVER_EVENT_SYNC_ADD("storage_ha", "before_get_config_version_and_transfer_scn");
      DEBUG_SYNC(BEFORE_GET_CONFIG_VERSION_AND_TRANSFER_SCN);
    }
  }
#endif
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    int64_t local_transfer_scn = 0;
    if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_config_version_and_transfer_scn(need_get_config_version,
                                                               result_.config_version_,
                                                               result_.transfer_scn_))) {
      LOG_WARN("failed to get config version and transfer scn", K(ret), K(tenant_id), K(ls_id));
    } else {
      LOG_INFO("get config version and transfer scn succ", K(tenant_id), K(ls_id), K(result_));
    }
  }

  return ret;
}

ObStorageFetchLSViewP::ObStorageFetchLSViewP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle),
      max_tablet_num_(0)
{
}

int ObStorageFetchLSViewP::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive fetch ls view request", K_(arg));
  MTL_SWITCH(arg_.tenant_id_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    char * buf = NULL;
    max_tablet_num_ = 32;
    const int64_t start_ts = ObTimeUtil::current_time();
    const int64_t first_receive_ts = this->get_receive_timestamp();

    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      const int64_t tmp_max_tablet_num = tenant_config->_ha_tablet_info_batch_count;
      if (0 != tmp_max_tablet_num) {
        max_tablet_num_ = tmp_max_tablet_num;
      }
    }

    int64_t filled_tablet_count = 0;
    int64_t total_tablet_count = 0;
    last_send_time_ = this->get_receive_timestamp();

    auto fill_ls_meta_f = [this](const ObLSMetaPackage &ls_meta)->int {
      int ret = OB_SUCCESS;
      if (OB_FAIL(fill_data(ls_meta))) {
        LOG_WARN("failed to fill ls meta", K(ret), K(ls_meta));
      }
      return ret;
    };

    auto fill_tablet_meta_f = [this, &filled_tablet_count, &total_tablet_count]
        (const obrpc::ObCopyTabletInfo &tablet_info, const ObTabletHandle &tablet_handle)->int {
      int ret = OB_SUCCESS;
      if (!tablet_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fill tablet meta info get invalid argument", K(ret), K(tablet_info));
      } else if (filled_tablet_count >= this->max_tablet_num_) {
        if (this->result_.get_position() > 0 && OB_FAIL(flush_and_wait())) {
          LOG_WARN("failed to flush and wait", K(ret), K(tablet_info));
        } else {
          LOG_INFO("batch flush and wait", K(filled_tablet_count), K(total_tablet_count));
          filled_tablet_count = 0;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fill_data(tablet_info))) {
        STORAGE_LOG(WARN, "fill to fill tablet info", K(ret), K(tablet_info));
      } else {
        ++filled_tablet_count;
        ++total_tablet_count;
      }
      return ret;
    };
    const int64_t cost_time = 10 * 1000 * 1000;
    common::ObTimeGuard timeguard("ObStorageFetchLSViewP", cost_time);
    timeguard.click();
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc migrate data buffer.", K(ret));
    } else if (!result_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed set data to result", K(ret));
    } else if (OB_ISNULL(bandwidth_throttle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bandwidth_throttle_ must not null", K(ret), KP_(bandwidth_throttle));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
    }

    timeguard.click();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_service->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", K(ret), K_(arg));
    }

    timeguard.click();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K_(arg), KP(ls));
    } else if (OB_FAIL(ls->get_ls_meta_package_and_tablet_metas(
                       false/* no need check archive */,
                       fill_ls_meta_f,
                       fill_tablet_meta_f))) {
      LOG_WARN("failed to get ls meta package and tablet metas", K(ret), K_(arg));
    }
    LOG_INFO("finish fetch ls view", K(ret), K(total_tablet_count), "cost_ts", ObTimeUtil::current_time() - start_ts,
        "in rpc queue time", start_ts - first_receive_ts);
    timeguard.click();
  }
  return ret;
}

ObStorageSubmitTxLogP::ObStorageSubmitTxLogP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageSubmitTxLogP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;

  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    transaction::ObTransID failed_tx_id;
    SCN scn;
    if (!arg_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K_(arg));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("ls_srv->get_ls() fail", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
    } else if (OB_FAIL(ls->get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id))) {
      LOG_WARN("failed to submit tx log", K(ret), KPC(ls), K(failed_tx_id));
    } else if (OB_FAIL(ls->get_log_handler()->get_max_scn(scn))) {
      LOG_WARN("log_handler get_max_scn failed", K(ret), K(ls_id));
    } else {
      result_ = scn;
      LOG_INFO("success to submit tx log", K(ret), K_(arg));
    }
  }
  return ret;
}

ObStorageGetTransferDestPrepareSCNP::ObStorageGetTransferDestPrepareSCNP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageGetTransferDestPrepareSCNP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;

  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLS *ls = NULL;
    bool enable = false;
    SCN scn;
    if (!arg_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K_(arg));
    } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("ls_srv->get_ls() fail", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is NULL", KR(ret), K(ls_handle));
    } else if (OB_FAIL(ls->get_transfer_status().get_transfer_prepare_status(enable, scn))) {
      LOG_WARN("failed to get wrs handler transfer_prepare status", K(ret));
    } else if (!enable) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wrs handler not enter transfer_prepare status", K(ret), K_(arg));
    } else {
      result_ = scn;
      LOG_INFO("success to get wrs handler transfer_dest_prepare_scn", K(ret), K_(arg), K(scn));
    }
  }
  return ret;
}

ObStorageLockConfigChangeP::ObStorageLockConfigChangeP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageLockConfigChangeP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  const int64_t lock_owner = arg_.lock_owner_;
  const int64_t lock_timeout = arg_.lock_timeout_;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    if (!arg_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K_(arg));
    } else if (arg_.type_ != ObStorageConfigChangeOpArg::LOCK_CONFIG_CHANGE) {
      ret = OB_ERR_SYS;
      LOG_WARN("type not match", K(ret), K_(arg));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->try_lock_config_change(lock_owner, lock_timeout))) {
      LOG_WARN("failed to try lock config config", K(ret), K_(arg));
    } else {
      result_.op_succ_ = true;
      LOG_INFO("lock config change success", K(arg_));
    }
  }
  return ret;
}

ObStorageUnlockConfigChangeP::ObStorageUnlockConfigChangeP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageUnlockConfigChangeP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  const int64_t lock_owner = arg_.lock_owner_;
  const int64_t lock_timeout = arg_.lock_timeout_;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    if (!arg_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K_(arg));
    } else if (arg_.type_ != ObStorageConfigChangeOpArg::UNLOCK_CONFIG_CHANGE) {
      ret = OB_ERR_SYS;
      LOG_WARN("type not match", K(ret), K_(arg));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->unlock_config_change(lock_owner, lock_timeout))) {
      LOG_WARN("failed to try lock config config", K(ret), K_(arg));
    } else {
      result_.op_succ_ = true;
      LOG_INFO("unlock config change success", K(arg_));
    }
  }
  return ret;
}

ObStorageGetLogConfigStatP::ObStorageGetLogConfigStatP(
    common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageGetLogConfigStatP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  const share::ObLSID &ls_id = arg_.ls_id_;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = NULL;
    ObLS *ls = NULL;
    if (!arg_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K_(arg));
    } else if (arg_.type_ != ObStorageConfigChangeOpArg::GET_CONFIG_CHANGE_LOCK_STAT) {
      ret = OB_ERR_SYS;
      LOG_WARN("type not match", K(ret), K_(arg));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get log stream", KR(ret), K(arg_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(arg_), KP(ls));
    } else if (OB_FAIL(ls->get_config_change_lock_stat(result_.palf_lock_owner_, result_.is_locked_))) {
      LOG_WARN("failed to try lock config config", K(ret), K_(arg));
    } else {
      result_.op_succ_ = true;
      LOG_INFO("get config change lock stat success", K(arg_), K(result_));
    }
  }
  return ret;
}

ObStorageWakeupTransferServiceP::ObStorageWakeupTransferServiceP(
      common::ObInOutBandwidthThrottle *bandwidth_throttle)
    : ObStorageStreamRpcP(bandwidth_throttle)
{
}

int ObStorageWakeupTransferServiceP::process()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = arg_.tenant_id_;
  MTL_SWITCH(tenant_id) {
    ObTransferService *transfer_service = MTL(ObTransferService*);
    if (OB_ISNULL(transfer_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("transfer service should not be NULL", K(ret), KP(transfer_service));
    } else {
      transfer_service->wakeup();
    }
  }
  return ret;
}


} //namespace obrpc

namespace storage
{

ObStorageRpc::ObStorageRpc()
    : is_inited_(false),
      rpc_proxy_(NULL),
      rs_rpc_proxy_(NULL)
{
}

ObStorageRpc::~ObStorageRpc()
{
  destroy();
}

int ObStorageRpc::init(
    obrpc::ObStorageRpcProxy *rpc_proxy,
    const common::ObAddr &self,
    obrpc::ObCommonRpcProxy *rs_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "storage rpc has inited", K(ret));
  } else if (OB_ISNULL(rpc_proxy) || !self.is_valid() || OB_ISNULL(rs_rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObStorageRpc init with invalid argument",
        KP(rpc_proxy), K(self), KP(rs_rpc_proxy));
  } else {
    rpc_proxy_ = rpc_proxy;
    self_ = self;
    rs_rpc_proxy_ = rs_rpc_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObStorageRpc::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    rpc_proxy_ = NULL;
    self_ = ObAddr();
    rs_rpc_proxy_ = NULL;
  }
}

int ObStorageRpc::post_ls_info_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObCopyLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "post ls info request get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObCopyLSInfoArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(ls_id));
    } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .fetch_ls_info(arg, ls_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls info successfully", K(ls_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_meta_info_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObFetchLSMetaInfoResp &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "post ls info request get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObFetchLSMetaInfoArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(ObStorageHAUtils::get_server_version(arg.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(arg));
    } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .fetch_ls_meta_info(arg, ls_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls meta info successfully", K(ls_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_member_list_request(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    obrpc::ObFetchLSMemberListInfo &member_info)
{
  int ret = OB_SUCCESS;
  member_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage rpc is not inited", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("post ls member list request get invalid argument", K(ret), K(tenant_id), K(ls_id), K(src_info));
  } else {
    obrpc::ObFetchLSMemberListArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .fetch_ls_member_list(arg, member_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("fetch ls member list successfully", K(member_info));
    }
  }
  return ret;
}

int ObStorageRpc::post_ls_disaster_recovery_res(const common::ObAddr &server,
                         const obrpc::ObDRTaskReplyResult &res)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!server.is_valid() || !res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(server), K(res));
  } else if (OB_FAIL(rs_rpc_proxy_->to(server).disaster_recovery_task_reply(res))) {
    STORAGE_LOG(WARN, "post ls migration result failed", K(ret), K(res), K(server));
  } else {
    STORAGE_LOG(TRACE, "post_ls_disaster_recovery_res successfully", K(res), K(server));
  }
  return ret;
}

int ObStorageRpc::notify_restore_tablets(
      const uint64_t tenant_id,
      const ObStorageHASrcInfo &follower_info,
      const share::ObLSID &ls_id,
      const int64_t &proposal_id,
      const common::ObIArray<common::ObTabletID>& tablet_id_array,
      const share::ObLSRestoreStatus &restore_status,
      obrpc::ObNotifyRestoreTabletsResp &restore_resp)
{
  int ret = OB_SUCCESS;
  ObNotifyRestoreTabletsArg arg;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!follower_info.is_valid() || !ls_id.is_valid()
      || (tablet_id_array.empty() && (restore_status.is_restore_tablets_meta() || restore_status.is_quick_restore() || restore_status.is_restore_major_data()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "notify follower restore get invalid argument", K(ret), K(follower_info), K(ls_id), K(tablet_id_array));
  } else if (OB_FAIL(arg.tablet_id_array_.assign(tablet_id_array))) {
    STORAGE_LOG(WARN, "failed to assign tablet id array", K(ret), K(follower_info), K(ls_id), K(tablet_id_array));
  } else {
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.restore_status_ = restore_status;
    arg.leader_proposal_id_ = proposal_id;
    if (OB_FAIL(rpc_proxy_->to(follower_info.src_addr_).dst_cluster_id(follower_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .notify_restore_tablets(arg, restore_resp))) {
      LOG_WARN("failed to notify follower restore tablets", K(ret), K(arg), K(follower_info), K(ls_id), K(tablet_id_array));
    } else {
      FLOG_INFO("notify follower restore tablets successfully", K(arg), K(follower_info), K(ls_id), K(tablet_id_array));
    }
  }
  return ret;
}

int ObStorageRpc::inquire_restore(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const share::ObLSRestoreStatus &restore_status,
    obrpc::ObInquireRestoreResp &restore_resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "inquire restore get invalid argument", K(ret), K(src_info), K(ls_id));
  } else {
    ObInquireRestoreArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.restore_status_ = restore_status;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .inquire_restore(arg, restore_resp))) {
      LOG_WARN("failed to inquire restore", K(ret), K(arg), K(src_info));
    } else {
      FLOG_INFO("inquire restore status successfully", K(arg), K(src_info));
    }
  }
  return ret;
}

int ObStorageRpc::update_ls_meta(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &dest_info,
    const storage::ObLSMetaPackage &ls_meta)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (!dest_info.is_valid() || !ls_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(dest_info), K(ls_meta));
  } else {
    ObRestoreUpdateLSMetaArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_meta_package_ = ls_meta;
    if (OB_FAIL(rpc_proxy_->to(dest_info.src_addr_).dst_cluster_id(dest_info.cluster_id_)
        .by(tenant_id)
        .group_id(share::OBCG_STORAGE)
        .update_ls_meta(arg))) {
      LOG_WARN("failed to update ls meta", K(ret), K(dest_info), K(ls_meta));
    } else {
      FLOG_INFO("update ls meta succ", K(dest_info), K(ls_meta));
    }
  }

  return ret;
}

int ObStorageRpc::get_ls_active_trans_count(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    int64_t &active_trans_count)
{
  int ret = OB_SUCCESS;
  active_trans_count = -1;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else {
    ObGetLSActiveTransCountRes res;
    ObGetLSActiveTransCountArg arg;
    arg.tenant_id_ = tenant_id;
    arg.src_ls_id_ = ls_id;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(share::OBCG_STORAGE)
                           .get_ls_active_trans_count(arg, res))) {
      LOG_WARN("failed to get ls active trans count", K(ret), K(src_info), K(arg));
    } else {
      active_trans_count = res.active_trans_count_;
    }
  }
  return ret;
}

int ObStorageRpc::get_transfer_start_scn(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const common::ObIArray<share::ObTransferTabletInfo> &tablet_list,
    SCN &transfer_start_scn)
{
  int ret = OB_SUCCESS;
  transfer_start_scn.reset();
  int64_t get_transfer_start_scn_timeout = 10_s;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    get_transfer_start_scn_timeout = tenant_config->_transfer_start_rpc_timeout;
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else {
    ObGetTransferStartScnRes res;
    ObGetTransferStartScnArg arg;
    arg.tenant_id_ = tenant_id;
    arg.src_ls_id_ = ls_id;
    if (OB_FAIL(arg.tablet_list_.assign(tablet_list))) {
      LOG_WARN("failed to assign tablet list", K(ret), K(tablet_list));
    } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                                  .by(tenant_id)
                                  .dst_cluster_id(src_info.cluster_id_)
                                  .timeout(get_transfer_start_scn_timeout)
                                  .group_id(share::OBCG_TRANSFER)
                                  .get_transfer_start_scn(arg, res))) {
      LOG_WARN("failed to get transfer start scn", K(ret), K(src_info), K(arg));
    } else {
      transfer_start_scn = res.start_scn_;
    }
  }
  return ret;
}


int ObStorageRpc::submit_tx_log(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    SCN &data_end_scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else {
    ObStorageTransferCommonArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    SCN end_scn;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(share::OBCG_STORAGE)
                           .submit_tx_log(arg, end_scn))) {
      LOG_WARN("failed to submit tx log", K(ret), K(src_info), K(arg));
    } else {
      data_end_scn = end_scn;
    }
  }
  return ret;
}

int ObStorageRpc::get_transfer_dest_prepare_scn(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    SCN &scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else {
    ObStorageTransferCommonArg arg;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    SCN ret_scn;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(share::OBCG_STORAGE)
                           .get_transfer_dest_prepare_scn(arg, ret_scn))) {
      LOG_WARN("failed to get transfer_dest_prepare_scn", K(ret), K(src_info), K(arg));
    } else {
      scn = ret_scn;
    }
  }
  return ret;
}

int ObStorageRpc::lock_config_change(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const int64_t lock_owner,
    const int64_t lock_timeout,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid() || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id), K(group_id));
  } else {
    ObStorageConfigChangeOpArg arg;
    ObStorageConfigChangeOpRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.type_ = ObStorageConfigChangeOpArg::LOCK_CONFIG_CHANGE;
    arg.lock_owner_ = lock_owner;
    arg.lock_timeout_ = lock_timeout;
    const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .timeout(timeout)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(group_id)
                           .lock_config_change(arg, res))) {
      LOG_WARN("failed to replace member", K(ret), K(src_info), K(arg));
    }
  }
  return ret;
}

int ObStorageRpc::unlock_config_change(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const int64_t lock_owner,
    const int64_t lock_timeout,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid() || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id), K(group_id));
  } else {
    ObStorageConfigChangeOpArg arg;
    ObStorageConfigChangeOpRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.type_ = ObStorageConfigChangeOpArg::UNLOCK_CONFIG_CHANGE;
    arg.lock_owner_ = lock_owner;
    arg.lock_timeout_ = lock_timeout;
    const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .timeout(timeout)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(group_id)
                           .unlock_config_change(arg, res))) {
      LOG_WARN("failed to replace member", K(ret), K(src_info), K(arg));
    }
  }
  return ret;
}

int ObStorageRpc::get_config_change_lock_stat(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info,
    const share::ObLSID &ls_id,
    const int32_t group_id,
    int64_t &palf_lock_owner,
    bool &is_locked)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid() || !ls_id.is_valid() || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info), K(ls_id), K(group_id));
  } else {
    ObStorageConfigChangeOpArg arg;
    ObStorageConfigChangeOpRes res;
    arg.tenant_id_ = tenant_id;
    arg.ls_id_ = ls_id;
    arg.type_ = ObStorageConfigChangeOpArg::GET_CONFIG_CHANGE_LOCK_STAT;
    const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .timeout(timeout)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(group_id)
                           .get_config_change_lock_stat(arg, res))) {
      LOG_WARN("failed to replace member", K(ret), K(src_info), K(arg));
    } else {
      palf_lock_owner = res.palf_lock_owner_;
      is_locked = res.is_locked_;
    }
  }
  return ret;
}


int ObStorageRpc::wakeup_transfer_service(
    const uint64_t tenant_id,
    const ObStorageHASrcInfo &src_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (tenant_id == OB_INVALID_ID || !src_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(src_info));
  } else {
    ObStorageWakeupTransferServiceArg arg;
    arg.tenant_id_ = tenant_id;
    if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_)
                           .by(tenant_id)
                           .dst_cluster_id(src_info.cluster_id_)
                           .group_id(share::OBCG_STORAGE)
                           .wakeup_transfer_service(arg))) {
      LOG_WARN("failed to wakeup transfer service", K(ret), K(src_info), K(arg));
    }
  }
  return ret;
}

int ObStorageRpc::fetch_ls_member_and_learner_list(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObStorageHASrcInfo &src_info,
    obrpc::ObFetchLSMemberAndLearnerListInfo &member_info)
{
  int ret = OB_SUCCESS;
  obrpc::ObFetchLSMemberAndLearnerListArg arg;
  arg.tenant_id_ = tenant_id;
  arg.ls_id_ = ls_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "storage rpc is not inited", K(ret));
  } else if (OB_FAIL(rpc_proxy_->to(src_info.src_addr_).dst_cluster_id(src_info.cluster_id_)
      .by(tenant_id)
      .group_id(share::OBCG_STORAGE)
      .fetch_ls_member_and_learner_list(arg, member_info))) {
    LOG_WARN("fail to check ls is valid member", K(ret), K(tenant_id), K(ls_id));
  }

  return ret;
}
} // storage
} // oceanbase
