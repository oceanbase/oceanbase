//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <sstream>
#define protected public
#define private public
#include "ob_truncate_info_helper.h"
#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/truncate_info/ob_mds_info_distinct_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
namespace storage
{

static int64_t inc_seq = 0;
void TruncateInfoHelper::mock_truncate_info(
    common::ObIAllocator &allocator,
    const int64_t trans_id,
    const int64_t schema_version,
    ObTruncateInfo &info)
{
  info.allocator_ = &allocator;
  info.key_.tx_id_ = trans_id;
  info.key_.inc_seq_ = inc_seq++;
  info.schema_version_ = schema_version;
}

void TruncateInfoHelper::mock_truncate_info(
    common::ObIAllocator &allocator,
    const int64_t trans_id,
    const int64_t schema_version,
    const int64_t commit_version,
    ObTruncateInfo &info)
{
  mock_truncate_info(allocator, trans_id, schema_version, info);
  info.commit_version_ = commit_version;
}

int TruncateInfoHelper::mock_truncate_partition(
  common::ObIAllocator &allocator,
  const int64_t low_bound_val,
  const int64_t high_bound_val,
  ObTruncatePartition &part)
{
  int ret = OB_SUCCESS;
  part.part_type_ = ObTruncatePartition::RANGE_PART;
  part.part_op_ = ObTruncatePartition::INCLUDE;
  ObObj tmp_obj;
  ObRowkey tmp_rowkey(&tmp_obj, 1);
  tmp_obj.set_int(low_bound_val);
  if (OB_FAIL(tmp_rowkey.deep_copy(part.low_bound_val_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy low_bound_val", K(ret));
  }
  tmp_obj.set_int(high_bound_val);
  if (FAILEDx(tmp_rowkey.deep_copy(part.high_bound_val_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy high_bound_val", K(ret));
  }
  return ret;
}

int TruncateInfoHelper::mock_truncate_partition(
    common::ObIAllocator &allocator,
    const ObRowkey &low_bound_val,
    const ObRowkey &high_bound_val,
    ObTruncatePartition &part)
{
  int ret = OB_SUCCESS;
  part.part_type_ = low_bound_val.length() > 1 ? ObTruncatePartition::RANGE_COLUMNS_PART : ObTruncatePartition::RANGE_PART;
  part.part_op_ = ObTruncatePartition::INCLUDE;
  if (OB_FAIL(low_bound_val.deep_copy(part.low_bound_val_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy low_bound_val", K(ret));
  }
  if (FAILEDx(high_bound_val.deep_copy(part.high_bound_val_, allocator))) {
    COMMON_LOG(WARN, "Fail to deep copy high_bound_val", K(ret));
  }
  return ret;
}

int TruncateInfoHelper::mock_truncate_partition(
  common::ObIAllocator &allocator,
  const int64_t *list_val,
  const int64_t list_val_cnt,
  ObTruncatePartition &part)
{
  int ret = OB_SUCCESS;
  ObListRowValues tmp_values(allocator);
  part.part_type_ = ObTruncatePartition::LIST_PART;
  part.part_op_ = ObTruncatePartition::INCLUDE;
  ObObj tmp_obj;
  ObNewRow tmp_row(&tmp_obj, 1);
  for (int64_t idx = 0; OB_SUCC(ret) && idx < list_val_cnt; ++idx) {
    ObNewRow new_row;
    tmp_obj.set_int(list_val[idx]);
    if (OB_FAIL(ob_write_row(allocator, tmp_row, new_row))) {
      COMMON_LOG(WARN, "success to push row", K(ret), K(tmp_row));
    } else if (OB_FAIL(tmp_values.push_back(new_row))) {
      COMMON_LOG(WARN, "Fail to push row", K(ret), K(new_row));
    }
  }
  if (FAILEDx(tmp_values.sort_array())) {
    COMMON_LOG(WARN, "Fail to sort array", K(ret), K(tmp_row));
  } else if (OB_FAIL(part.list_row_values_.init(allocator, tmp_values.get_values()))) {
    COMMON_LOG(WARN, "failed to init list row values", KR(ret), K(tmp_values));
  }
  return ret;
}

int TruncateInfoHelper::get_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    COMMON_LOG(WARN, "failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    COMMON_LOG(WARN, "failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

int TruncateInfoHelper::mock_part_key_idxs(
    common::ObIAllocator &allocator,
    const int64_t cnt,
    const int64_t *col_idxs,
    ObTruncatePartition &part)
{
  int ret = OB_SUCCESS;
  int64_t *buf = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * cnt));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, col_idxs, sizeof(int64_t) * cnt);
    part.part_key_idxs_.col_idxs_ = buf;
    part.part_key_idxs_.cnt_ = cnt;
  }
  return ret;
}

int TruncateInfoHelper::read_distinct_truncate_info_array(
    common::ObArenaAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObVersionRange &read_version_range,
    ObTruncateInfoArray &truncate_info_array)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObMdsInfoDistinctMgr distinct_mgr;
  truncate_info_array.reset();
  if (OB_FAIL(TruncateInfoHelper::get_tablet(ls_id, tablet_id, tablet_handle))) {
    COMMON_LOG(WARN, "failed to get tablet", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(distinct_mgr.init(allocator, *tablet_handle.get_obj(), nullptr, read_version_range, false/*access*/))) {
    COMMON_LOG(WARN, "failed to init distinct mgr", KR(ret), K(read_version_range));
  } else if (OB_FAIL(truncate_info_array.init_for_first_creation(allocator))) {
    COMMON_LOG(WARN, "failed to init truncate info array", KR(ret));
  } else if (OB_FAIL(distinct_mgr.get_distinct_truncate_info_array(truncate_info_array))) {
    COMMON_LOG(WARN, "failed to get array from distinct mgr", KR(ret), K(distinct_mgr));
  }
  return ret;
}

int TruncateInfoHelper::batch_mock_truncate_info(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    ObTruncateInfoArray &truncate_info_array)
{
  int ret = OB_SUCCESS;
  ObMockIterator key_iter;
  key_iter.from(key_data);
  const ObStoreRow *row = nullptr;
  const ObObj *cells = nullptr;
  if (OB_FAIL(truncate_info_array.init_for_first_creation(allocator))) {
    COMMON_LOG(WARN, "failed to init truncate info array", KR(ret), K(truncate_info_array));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < key_iter.count(); ++i) {
    key_iter.get_row(i, row);
    cells = row->row_val_.cells_;

    #define GET_INT(idx) cells[idx].get_int()
    ObTruncateInfo truncate_info;
    TruncateInfoHelper::mock_truncate_info(allocator, GET_INT(TRANS_ID), GET_INT(SCHEMA_VERSION), truncate_info);
    truncate_info.commit_version_ = GET_INT(COMMIT_VERSION);
    if (OB_FAIL(TruncateInfoHelper::mock_truncate_partition(allocator, GET_INT(LOWER_BOUND), GET_INT(UPPER_BOUND), truncate_info.truncate_part_))) {
      COMMON_LOG(WARN, "failed to mock truncate partition", KR(ret));
    } else if (OB_FAIL(TruncateInfoHelper::mock_part_key_idxs(allocator, 1/*part_key_idx*/, truncate_info.truncate_part_))) {
      COMMON_LOG(WARN, "failed to mock part key index", KR(ret));
    } else if (OB_FAIL(truncate_info_array.append_with_deep_copy(truncate_info))) {
      COMMON_LOG(WARN, "failed to append truncate info", KR(ret), K(truncate_info));
    }
    #undef GET_INT
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
