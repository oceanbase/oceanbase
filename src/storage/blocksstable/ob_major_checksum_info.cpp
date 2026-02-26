//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "ob_major_checksum_info.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"
namespace oceanbase
{
using namespace compaction;
namespace blocksstable
{
ObMajorChecksumInfo::ObMajorChecksumInfo()
  : version_(MAJOR_CHECKSUM_INFO_VERSION_V1),
    exec_mode_(EXEC_MODE_MAX),
    reserved_(0),
    compaction_scn_(0),
    row_count_(0),
    data_checksum_(0),
    column_ckm_struct_()
{
}

void ObMajorChecksumInfo::reset()
{
  exec_mode_ = EXEC_MODE_MAX;
  reserved_ = 0;
  compaction_scn_ = 0;
  row_count_ = 0;
  data_checksum_ = 0;
  column_ckm_struct_.reset();
}

bool ObMajorChecksumInfo::is_empty() const
{
  return compaction_scn_ == 0;
}

bool ObMajorChecksumInfo::is_valid() const
{
  return row_count_ >= 0
    && ((0 == compaction_scn_ && column_ckm_struct_.is_empty())
    || (compaction_scn_ > 0 && !column_ckm_struct_.is_empty() && compaction::is_valid_exec_mode(get_exec_mode())));
}

int ObMajorChecksumInfo::assign(
  const ObMajorChecksumInfo &other,
  ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  reset();
  info_ = other.info_;
  compaction_scn_ = other.compaction_scn_;
  if (other.is_empty()) {
    // do nothing
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input allocator is null", KR(ret), K(allocator));
  } else {
    row_count_ = other.row_count_;
    data_checksum_ = other.data_checksum_;
    if (OB_FAIL(column_ckm_struct_.assign(*allocator, other.column_ckm_struct_))) {
      LOG_WARN("failed to assgin column checksums", KR(ret), K(other));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObMajorChecksumInfo is invalid after assign", KR(ret), KPC(this), K(other));
  }
  return ret;
}

int ObMajorChecksumInfo::deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObMajorChecksumInfo &dest) const
{
  int ret = OB_SUCCESS;
  dest.reset();
  dest.info_ = info_;
  dest.compaction_scn_ = compaction_scn_;
  if (!is_empty()) {
    dest.row_count_ = row_count_;
    dest.data_checksum_ = data_checksum_;
    if (OB_FAIL(column_ckm_struct_.deep_copy(buf, buf_len, pos, dest.column_ckm_struct_))) {
      LOG_WARN("failed to deep copy column checksum", KR(ret), K_(column_ckm_struct));
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!dest.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObMajorChecksumInfo is invalid after deep copy", KR(ret), KPC(this), K(dest));
  }
  return ret;
}

int ObMajorChecksumInfo::init_from_merge_result(
    ObArenaAllocator &allocator,
    const compaction::ObBasicTabletMergeCtx &ctx,
    const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else {
    compaction_scn_ = ctx.get_merge_version();
    row_count_ = res.row_count_;
    data_checksum_ = res.data_checksum_;
    exec_mode_ = ctx.get_exec_mode();
    if (OB_FAIL(column_ckm_struct_.assign(allocator, res.data_column_checksums_))) {
      LOG_WARN("fail to assign column checksum array", K(ret), KPC(this), K(res));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("after init from merge result, major checksum info is not valid", KR(ret), KPC(this));
    } else {
      LOG_INFO("success to init ckm info from merge result", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObMajorChecksumInfo::init_from_sstable(
    ObArenaAllocator &allocator,
    const compaction::ObExecMode exec_mode,
    const storage::ObStorageSchema &storage_schema,
    const blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!sstable.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable));
  } else {
    compaction_scn_ = sstable.get_snapshot_version();
    row_count_ = sstable.get_row_count();
    data_checksum_ = sstable.get_data_checksum();
    exec_mode_ = exec_mode;
    ObSEArray<int64_t, OB_ROW_DEFAULT_COLUMNS_COUNT> tmp_col_ckm_array;
    tmp_col_ckm_array.set_attr(ObMemAttr(MTL_ID(), "MajorCkmInfo"));
    if (sstable.is_co_sstable()) {
      const ObCOSSTableV2 &co_sstable = static_cast<const ObCOSSTableV2 &>(sstable);
      if (OB_FAIL(co_sstable.fill_column_ckm_array(storage_schema, tmp_col_ckm_array))) {
        LOG_WARN("fail to fill column checksum array", K(ret), KPC(this), K(sstable));
      }
    } else if (OB_FAIL(sstable.fill_column_ckm_array(tmp_col_ckm_array))) {
      LOG_WARN("fail to fill column checksum array", K(ret), KPC(this), K(sstable));
    }
    if (FAILEDx(column_ckm_struct_.assign(allocator, tmp_col_ckm_array))) {
      LOG_WARN("fail to assign column checksum array", K(ret), KPC(this), K(sstable));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("after init from sstable, major checksum info is not valid", KR(ret), KPC(this));
    } else {
      LOG_INFO("success to init ckm info from sstable", KR(ret), KPC(this));
    }
  }
  return ret;
}

int64_t ObMajorChecksumInfo::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, info_, compaction_scn_);
  if (!is_empty()) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, row_count_, data_checksum_);
    len += column_ckm_struct_.get_serialize_size();
  }
  return len;
}

int ObMajorChecksumInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, info_, compaction_scn_);
  if (!is_empty()) {
    LST_DO_CODE(OB_UNIS_ENCODE, row_count_, data_checksum_);
    if (OB_FAIL(column_ckm_struct_.serialize(buf, buf_len, pos))) {
      LOG_WARN("Fail to serialize column checksum", K(ret), K_(column_ckm_struct));
    }
  }
  return ret;
}

int ObMajorChecksumInfo::deserialize(common::ObArenaAllocator &allocator, const char *buf,
                                     const int64_t data_len, int64_t &pos) {
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, info_, compaction_scn_);
  if (!is_empty()) {
    LST_DO_CODE(OB_UNIS_DECODE, row_count_, data_checksum_);
    if (OB_FAIL(column_ckm_struct_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("Fail to deserialize column count", K(ret));
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize ckm info is not valid", K(ret), KPC(this));
    }
  }
  return ret;
}

void ObMajorChecksumInfo::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    BUF_PRINTF("ObMajorChecksumInfo");
    J_COLON();
    J_KV("compaction_scn", get_compaction_scn(), "exec_mode", exec_mode_to_str(get_exec_mode()));
    J_OBJ_END();
  }
}

int ObCOMajorChecksumInfo::prepare_column_ckm_array(
  ObArenaAllocator &allocator,
  const compaction::ObBasicTabletMergeCtx &ctx,
  const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  int64_t column_count = 0;
  if (OB_UNLIKELY(!res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else if (OB_FAIL(ctx.get_schema()->get_stored_column_count_in_sstable(column_count))) {
    LOG_WARN("failed to get column count", KR(ret), K(column_count));
  } else if (OB_FAIL(column_ckm_struct_.reserve(allocator, column_count))) {
    LOG_WARN("failed to reserve array", KR(ret), K(column_count));
  } else {
    compaction_scn_ = ctx.get_merge_version();
    exec_mode_ = ctx.get_exec_mode();
    row_count_ = res.row_count_;
  }
  return ret;
}

int64_t ObMajorChecksumInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("is_empty", is_empty());
    if (!is_empty() || !is_valid()) {
      J_COMMA();
      J_KV(K_(compaction_scn),
        "exec_mode", compaction::exec_mode_to_str(get_exec_mode()),
        K_(row_count), K_(data_checksum), K_(column_ckm_struct));
    }
    J_OBJ_END();
  }
  return pos;
}

int ObCOMajorChecksumInfo::init_from_merge_result(
    ObArenaAllocator &allocator,
    const compaction::ObBasicTabletMergeCtx &ctx,
    const storage::ObStorageColumnGroupSchema &cg_schema,
    const blocksstable::ObSSTableMergeRes &res)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  if (is_empty() && OB_FAIL(prepare_column_ckm_array(allocator, ctx, res))) {
    LOG_WARN("failed to prepare struct", KR(ret), K(ctx));
  } else if (OB_UNLIKELY(!column_ckm_struct_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("struct is invalid", KR(ret), K(column_ckm_struct_));
  } else {
    data_checksum_ += res.data_checksum_;
  }

  const bool is_build_row_store_merge = ctx.static_param_.is_build_row_store();
  const int64_t loop_cnt = is_build_row_store_merge ? column_ckm_struct_.count_ : cg_schema.get_column_count();
  for (int64_t j = 0; OB_SUCC(ret) && j < loop_cnt; ++j) {
    const uint16_t column_idx = is_build_row_store_merge ? j : cg_schema.get_column_idx(j);
    if (OB_UNLIKELY(column_idx >= column_ckm_struct_.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column idx", K(ret), K(column_idx), KPC(this));
    } else if (column_ckm_struct_.column_checksums_[column_idx] == 0) {
      column_ckm_struct_.column_checksums_[column_idx] = res.data_column_checksums_[j];
    } else if (OB_UNLIKELY(column_ckm_struct_.column_checksums_[column_idx] != res.data_column_checksums_[j])) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("unexpected col_checksum, may have checksum error", K(ret),
               K(column_ckm_struct_.column_checksums_[column_idx]),
               K(res.data_column_checksums_[j]));
    }
  } // for
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("after init from merge result, major checksum info is not valid", KR(ret), KPC(this), K(res));
  }
  return ret;
}


} // namespace compaction
} // namespace oceanbase
