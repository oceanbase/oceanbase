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
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_errno.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_direct_load_struct.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
ObTabletSplitInfoMdsUserData::ObTabletSplitInfoMdsUserData()
  : allocator_("SplitRangeRPC"),
    table_id_(OB_INVALID_ID), lob_table_id_(OB_INVALID_ID),
    schema_version_(0), dest_schema_version_(OB_INVALID_VERSION), tablet_task_id_(0), task_id_(0), source_tablet_id_(),
    dest_tablets_id_(), data_format_version_(0),
    consumer_group_id_(0), can_reuse_macro_block_(false), is_no_logging_(false), split_sstable_type_(share::ObSplitSSTableType::SPLIT_BOTH),
    lob_col_idxs_(), parallel_datum_rowkey_list_(), storage_schema_()
  {}

bool ObTabletSplitInfoMdsUserData::is_valid() const
{
  bool is_valid = OB_INVALID_ID != table_id_
      && schema_version_ > 0 && task_id_ > 0
      && source_tablet_id_.is_valid() && dest_tablets_id_.count() > 0
      && data_format_version_ > 0 && consumer_group_id_ >= 0
      && split_sstable_type_ >= share::ObSplitSSTableType::SPLIT_BOTH
      && split_sstable_type_ <= share::ObSplitSSTableType::SPLIT_MINOR
      && storage_schema_.is_valid()
      && share::ObDDLType::DDL_INVALID != task_type_
      && parallelism_ > 0
      && OB_INVALID_ID != dest_schema_version_
      && tablet_task_id_ != 0;
  if (!lob_col_idxs_.empty()) {
    is_valid = is_valid && (OB_INVALID_ID != lob_table_id_);
  }
  return is_valid;
}

int ObTabletSplitInfoMdsUserData::assign(const ObTabletSplitInfoMdsUserData &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(dest_tablets_id_.assign(other.dest_tablets_id_))) {
    LOG_WARN("assign failed", K(ret), K(other));
  } else if (OB_FAIL(lob_col_idxs_.assign(other.lob_col_idxs_))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(assign_datum_rowkey_list(other.parallel_datum_rowkey_list_))) { // deep cpy.
    LOG_WARN("assign failed", K(ret), K(other));
  } else if (OB_FAIL(storage_schema_.assign(allocator_, other.storage_schema_))) {
    LOG_WARN("assign failed", K(ret), K(storage_schema_));
  } else {
    table_id_              = other.table_id_;
    lob_table_id_          = other.lob_table_id_;
    schema_version_        = other.schema_version_;
    dest_schema_version_   = other.dest_schema_version_;
    tablet_task_id_        = other.tablet_task_id_;
    task_id_               = other.task_id_;
    task_type_             = other.task_type_;
    parallelism_           = other.parallelism_;
    source_tablet_id_      = other.source_tablet_id_;
    data_format_version_   = other.data_format_version_;
    consumer_group_id_     = other.consumer_group_id_;
    can_reuse_macro_block_ = other.can_reuse_macro_block_;
    is_no_logging_         = other.is_no_logging_;
    split_sstable_type_    = other.split_sstable_type_;
  }
  return ret;
}

int ObTabletSplitInfoMdsUserData::assign_datum_rowkey_list(const common::ObSArray<blocksstable::ObDatumRowkey> &other_datum_rowkey_list)
{
  int ret = OB_SUCCESS;
  parallel_datum_rowkey_list_.reuse();
  if (OB_FAIL(parallel_datum_rowkey_list_.prepare_allocate(other_datum_rowkey_list.count()))) {
    LOG_WARN("prepare allocate failed", K(ret), K(other_datum_rowkey_list));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other_datum_rowkey_list.count(); ++i) {
    if (OB_FAIL(other_datum_rowkey_list.at(i).deep_copy(parallel_datum_rowkey_list_.at(i), allocator_))) {
      LOG_WARN("failed to deep copy datum", K(ret), K(other_datum_rowkey_list.at(i)));
    }
  }
  return ret;
}

void ObTabletSplitInfoMdsUserData::reset()
{
  table_id_ = OB_INVALID_ID; // scan rows needed, index table id or main table id.
  lob_table_id_ = OB_INVALID_ID; // scan rows needed, valid when split lob tablet.
  schema_version_ = 0; // report replica build status needed.
  task_id_ = 0; // report replica build status needed.
  task_type_ = ObDDLType::DDL_INVALID;
  parallelism_ = 0;
  source_tablet_id_.reset();
  dest_tablets_id_.reset();
  int64_t data_format_version_ = 0;
  uint64_t consumer_group_id_ = 0;
  bool can_reuse_macro_block_ = false;
  split_sstable_type_ = SPLIT_BOTH;
  lob_col_idxs_.reset();
  parallel_datum_rowkey_list_.reset();
  storage_schema_.reset();
  allocator_.reset();
}

OB_DEF_SERIALIZE(ObTabletSplitInfoMdsUserData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, table_id_, lob_table_id_,
    schema_version_, dest_schema_version_, tablet_task_id_, task_id_, task_type_, parallelism_, source_tablet_id_,
    dest_tablets_id_, data_format_version_,
    consumer_group_id_, can_reuse_macro_block_, is_no_logging_, split_sstable_type_,
    lob_col_idxs_, parallel_datum_rowkey_list_, storage_schema_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletSplitInfoMdsUserData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, table_id_, lob_table_id_,
    schema_version_, dest_schema_version_, tablet_task_id_, task_id_, task_type_, parallelism_, source_tablet_id_,
    dest_tablets_id_, data_format_version_,
    consumer_group_id_, can_reuse_macro_block_, is_no_logging_, split_sstable_type_,
    lob_col_idxs_);
  if (FAILEDx(ObSplitUtil::deserializ_parallel_datum_rowkey(
      allocator_, buf, data_len, pos, parallel_datum_rowkey_list_))) {
    LOG_WARN("deserialzie parallel info failed", K(ret));
  } else if (pos < data_len && OB_FAIL(storage_schema_.deserialize(allocator_, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize storage schema", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletSplitInfoMdsUserData)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, table_id_, lob_table_id_,
    schema_version_, dest_schema_version_, tablet_task_id_, task_id_, task_type_, parallelism_, source_tablet_id_,
    dest_tablets_id_, data_format_version_,
    consumer_group_id_, can_reuse_macro_block_, is_no_logging_, split_sstable_type_,
    lob_col_idxs_, parallel_datum_rowkey_list_, storage_schema_);
  return len;
}

} // namespace storage
} // namespace oceanbase