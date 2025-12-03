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

#include "storage/multi_data_source/ob_mds_minor_compaction_filter.h"

#include "src/storage/multi_data_source/mds_table_impl.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{
ObMdsMinorFilter::ObMdsMinorFilter()
  : ObICompactionFilter(),
    is_inited_(false),
    last_major_snapshot_(0),
    allocator_(ObMemAttr(MTL_ID(), "MdsMinorFilter"))
{
}

int ObMdsMinorFilter::init(
  const int64_t first_major_snapshot,
  const int64_t last_major_snapshot,
  const int64_t multi_version_start)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(first_major_snapshot < 0 || last_major_snapshot < 0 || multi_version_start < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(first_major_snapshot), K(last_major_snapshot), K(multi_version_start));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret), K(first_major_snapshot), K(last_major_snapshot), K(multi_version_start));
  } else {
    last_major_snapshot_ = last_major_snapshot;
    truncate_filter_snapshot_ = MIN(multi_version_start, first_major_snapshot);
    LOG_INFO("truncate info filter snapshot", KR(ret), K(first_major_snapshot), K(multi_version_start), K_(truncate_filter_snapshot));
    is_inited_ = true;
  }
  return ret;
}

int ObMdsMinorFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  mds::MdsDumpKVStorageAdapter kv_adapter;
  constexpr uint8_t medium_info_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<ObMediumCompactionInfoKey, ObMediumCompactionInfo>>::value;
  constexpr uint8_t truncateinfo_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<ObTruncateInfoKey, ObTruncateInfo>>::value;


  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(kv_adapter.convert_from_mds_multi_version_row(row))) {
    LOG_WARN("fail to convert from mds multi version row", K(ret), K(row));
  } else if (medium_info_mds_unit_id == kv_adapter.get_type()) {
    ret = filter_medium_info(row, kv_adapter, filter_ret);
  } else if (truncateinfo_mds_unit_id == kv_adapter.get_type()) {
    ret = filter_truncate_info(row, kv_adapter, filter_ret);
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
    LOG_DEBUG("not medium info/truncate info", K(ret), K(row), K_(last_major_snapshot), K_(truncate_filter_snapshot), K(kv_adapter));
  }

  return ret;
}
int ObMdsMinorFilter::filter_medium_info(
  const blocksstable::ObDatumRow &row,
  const mds::MdsDumpKVStorageAdapter &kv_adapter,
  ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMediumCompactionInfoKey medium_info_key;
  // filter medium compaction info
  if (OB_UNLIKELY(row.is_uncommitted_row()
      || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
  } else if (OB_FAIL(medium_info_key.mds_deserialize(kv_adapter.get_key().ptr(), kv_adapter.get_key().length(), pos))) {
    LOG_WARN("fail to deserialize medium_info_key", K(ret), K(kv_adapter));
  } else if (medium_info_key.get_medium_snapshot() <= last_major_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_DEBUG("medium info is filtered", K(ret), K(row), K(last_major_snapshot_), K(medium_info_key), K(kv_adapter));
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
    LOG_DEBUG("medium info is not filtered", K(ret), K(row), K(last_major_snapshot_), K(medium_info_key), K(kv_adapter));
  }
  return ret;
}

int ObMdsMinorFilter::filter_truncate_info(
  const blocksstable::ObDatumRow &row,
  const mds::MdsDumpKVStorageAdapter &kv_adapter,
  ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  allocator_.reuse();
  ObTruncateInfo truncate_info;
  if (OB_UNLIKELY(row.is_uncommitted_row()
      || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
  } else if (OB_FAIL(truncate_info.deserialize(allocator_, kv_adapter.get_user_data().ptr(), kv_adapter.get_user_data().length(), pos))) {
    LOG_WARN("fail to deserialize truncate info", K(ret), K(kv_adapter));
  } else if (truncate_info.commit_version_ < truncate_filter_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
    // TODO change into debug log later
    LOG_INFO("truncate info is filtered", K(ret), K(row), K(truncate_filter_snapshot_), K(truncate_info), K(kv_adapter));
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
  }
  return ret;
}

ObCrossLSMdsMinorFilter::ObCrossLSMdsMinorFilter()
  : ObICompactionFilter()
{
}

int ObCrossLSMdsMinorFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  mds::MdsDumpKVStorageAdapter kv_adapter;
  constexpr uint8_t tablet_status_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;

  if (OB_FAIL(kv_adapter.convert_from_mds_multi_version_row(row))) {
    LOG_WARN("fail to convert from mds multi version row", K(ret), K(row));
  } else if (tablet_status_mds_unit_id == kv_adapter.get_type()) {
    if (OB_UNLIKELY(row.is_uncommitted_row()
        || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
    } else {
      filter_ret = FILTER_RET_REMOVE;
      LOG_DEBUG("filter tablet status for cross ls mds minor merge", K(ret));
    }
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
