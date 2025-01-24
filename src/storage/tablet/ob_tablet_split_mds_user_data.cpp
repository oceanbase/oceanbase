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


#include "ob_tablet_split_mds_user_data.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet_split_mds_helper.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::share;
using oceanbase::blocksstable::ObDatumRowkey;
using oceanbase::blocksstable::ObStorageDatum;
using oceanbase::blocksstable::ObStorageDatumUtils;
using oceanbase::blocksstable::ObStoreCmpFuncs;

namespace oceanbase
{
namespace storage
{

void ObTabletSplitMdsUserData::reset()
{
  auto_part_size_ = OB_INVALID_SIZE;
  status_ = NO_SPLIT;
  ref_tablet_ids_.reset();
  partkey_projector_.reset();
  end_partkey_.reset();
  storage_schema_.reset();
  allocator_.reset();
  return;
}

int ObTabletSplitMdsUserData::assign(const ObTabletSplitMdsUserData &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ref_tablet_ids_.assign(other.ref_tablet_ids_))) {
    LOG_WARN("failed to assign ref tablet ids", K(ret));
  } else if (0 != other.end_partkey_.get_datum_cnt() && OB_FAIL(other.end_partkey_.deep_copy(end_partkey_, allocator_))) {
    LOG_WARN("failed to write rowkey", K(ret));
  } else if (OB_FAIL(partkey_projector_.assign(other.partkey_projector_))) {
    LOG_WARN("failed to assign partkey projector", K(ret));
  } else if (other.storage_schema_.is_valid() && OB_FAIL(storage_schema_.assign(allocator_, other.storage_schema_))) {
    LOG_WARN("failed to assign storage schema", K(ret));
  } else {
    auto_part_size_ = other.auto_part_size_;
    status_ = other.status_;
  }
  return ret;
}

int ObTabletSplitMdsUserData::init_no_split(const int64_t auto_part_size)
{
  int ret = OB_SUCCESS;
  reset();
  auto_part_size_ = auto_part_size;
  return ret;
}

int ObTabletSplitMdsUserData::init_range_part_split_src(
    const ObIArray<ObTabletID> &dst_tablet_ids,
    const ObIArray<uint64_t> &partkey_projector,
    const ObTableSchema &table_schema,
    const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  reset();
  auto_part_size_ = OB_INVALID_SIZE; // split src tablet can no longer auto split
  status_ = RANGE_PART_SPLIT_SRC;
  const lib::Worker::CompatMode compat_mode = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
  if (OB_FAIL(ref_tablet_ids_.assign(dst_tablet_ids))) {
    LOG_WARN("failed to assign ref tablet ids", K(ret));
  } else if (OB_FAIL(partkey_projector_.assign(partkey_projector))) {
    LOG_WARN("failed to assign partkey projector", K(ret));
  } else if (OB_FAIL(storage_schema_.init(allocator_, table_schema, compat_mode, false/*skip_column_info*/))) {
    LOG_WARN("failed to assign partkey projector", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsUserData::init_range_part_split_dst(
    const int64_t auto_part_size,
    const ObTabletID &src_tablet_id,
    const ObDatumRowkey &end_partkey)
{
  int ret = OB_SUCCESS;
  reset();
  auto_part_size_ = auto_part_size;
  status_ = RANGE_PART_SPLIT_DST;
  if (OB_FAIL(ref_tablet_ids_.push_back(src_tablet_id))) {
    LOG_WARN("failed to assign ref tablet ids", K(ret));
  } else if (end_partkey.get_datum_cnt() > 0 && OB_FAIL(end_partkey.deep_copy(end_partkey_, allocator_))) {
    LOG_WARN("failed to write rowkey", K(ret));
  }
  return ret;
}

int ObTabletSplitMdsUserData::modify_auto_part_size(const int64_t auto_part_size)
{
  int ret = OB_SUCCESS;
  auto_part_size_ = auto_part_size;
  return ret;
}

int ObTabletSplitMdsUserData::get_auto_part_size(int64_t &auto_part_size) const
{
  int ret = OB_SUCCESS;
  auto_part_size = auto_part_size_;
  return ret;
}

int ObTabletSplitMdsUserData::calc_split_dst(ObLS &ls, const ObDatumRowkey &rowkey, const int64_t abs_timeout_us, ObTabletID &dst_tablet_id) const
{
  int ret = OB_SUCCESS;
  dst_tablet_id.reset();
  if (RANGE_PART_SPLIT_SRC == status_) {
    int64_t left_boundary = 0;
    int64_t right_boundary = ref_tablet_ids_.count() - 1;
    int64_t dst_tablet_id_loc = ref_tablet_ids_.count();
    while (OB_SUCC(ret) && left_boundary <= right_boundary) {
      int64_t middle_loc = (left_boundary + right_boundary) / 2;
      const ObTabletID &tablet_id = ref_tablet_ids_.at(middle_loc);
      ObTabletHandle tablet_handle;
      int cmp_ret = 0;
      int64_t timeout_us = 0;
      if (OB_FAIL(ls.get_tablet_with_timeout(tablet_id, tablet_handle, abs_timeout_us))) {
        LOG_WARN("failed to get split dst tablet", K(ret));
      } else if (OB_FAIL(ObTabletSplitMdsHelper::get_valid_timeout(abs_timeout_us, timeout_us))) {
        LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
      } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::split_partkey_compare(
              rowkey, tablet_handle.get_obj()->get_rowkey_read_info(), partkey_projector_, cmp_ret, timeout_us))) {
        LOG_WARN("failed to part key compare", K(ret));
        if (OB_NOT_SUPPORTED == ret) {
          ret = OB_SCHEMA_EAGAIN;
          LOG_WARN("no longer split dst, retry", K(ret), K(tablet_id));
        }
      } else if (cmp_ret > 0) { // dst partition high bound > rowkey's partkey
        right_boundary = middle_loc - 1;
        dst_tablet_id_loc = middle_loc;
      } else {
        left_boundary = middle_loc + 1;
      }
    }
    if (OB_SUCC(ret) && 0 <= dst_tablet_id_loc && dst_tablet_id_loc < ref_tablet_ids_.count()) {
      dst_tablet_id = ref_tablet_ids_.at(dst_tablet_id_loc);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get dst_tablet_id", K(ret), K(dst_tablet_id_loc), K(dst_tablet_id));
    } else if (OB_UNLIKELY(!dst_tablet_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey not in any split dst tablet", K(ret), K(ls.get_ls_id()), K(rowkey), K(*this));
      print_ref_tablets_split_data(ls);
   }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

// TODO(lihongqin.lhq): optimize by binary search
// caller guarantee dst_split_datas.is_split_dst() == true
int ObTabletSplitMdsUserData::calc_split_dst(
    const ObITableReadInfo &rowkey_read_info,
    const ObIArray<ObTabletSplitMdsUserData> &dst_split_datas,
    const ObDatumRowkey &rowkey,
    ObTabletID &dst_tablet_id,
    int64_t &dst_idx) const
{
  int ret = OB_SUCCESS;
  dst_tablet_id.reset();
  if (RANGE_PART_SPLIT_SRC == status_) {
    if (OB_UNLIKELY(dst_split_datas.count() != ref_tablet_ids_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid dst split datas", K(ret), K(*this), K(dst_split_datas));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !dst_tablet_id.is_valid() && i < ref_tablet_ids_.count(); i++) {
      const ObTabletID &tablet_id = ref_tablet_ids_.at(i);
      ObTabletHandle tablet_handle;
      int cmp_ret = 0;
      if (OB_FAIL(dst_split_datas.at(i).partkey_compare(rowkey, rowkey_read_info, partkey_projector_, cmp_ret))) {
        LOG_WARN("failed to part key compare", K(ret));
      } else if (cmp_ret > 0) { // dst partition high bound > rowkey's partkey
        dst_tablet_id = tablet_id;
        dst_idx = i;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!dst_tablet_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey not in any split dst tablet", K(ret), K(rowkey), K(*this), K(dst_split_datas));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_split_dst_tablet_ids(ObIArray<ObTabletID> &dst_tablet_ids) const
{
  int ret = OB_SUCCESS;
  if (RANGE_PART_SPLIT_SRC == status_) {
    if (OB_FAIL(dst_tablet_ids.assign(ref_tablet_ids_))) {
      LOG_WARN("failed to assign", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_split_dst_tablet_cnt(int64_t &split_cnt) const
{
  int ret = OB_SUCCESS;
  if (RANGE_PART_SPLIT_SRC == status_) {
    split_cnt = ref_tablet_ids_.count();
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_storage_schema(const ObStorageSchema *&storage_schema) const
{
  int ret = OB_SUCCESS;
  if (RANGE_PART_SPLIT_SRC == status_) {
    storage_schema = &storage_schema_;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::partkey_compare(
    const ObDatumRowkey &rowkey,
    const ObITableReadInfo &rowkey_read_info,
    const ObIArray<uint64_t> &partkey_projector,
    int &cmp_ret) const
{
  int ret = OB_SUCCESS;
  const ObStorageDatumUtils &datum_utils = rowkey_read_info.get_datum_utils();
  const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
  cmp_ret = 0;
  if (RANGE_PART_SPLIT_DST == status_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < partkey_projector.count() && 0 == cmp_ret; i++) {
      int64_t col_idx = partkey_projector.at(i);
      if (OB_UNLIKELY(col_idx >= cmp_funcs.count() || col_idx >= rowkey.get_datum_cnt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cmp func cnt or rowkey cnt", K(ret), K(i), K(*this), K(rowkey), K(cmp_funcs));
      } else if (OB_FAIL(cmp_funcs.at(col_idx).compare(end_partkey_.datums_[i], rowkey.datums_[col_idx], cmp_ret))) {
        LOG_WARN("Failed to compare datum rowkey", K(ret), K(i), K(*this), K(rowkey));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_split_src_tablet_id(ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  if (RANGE_PART_SPLIT_DST == status_) {
    if (OB_UNLIKELY(ref_tablet_ids_.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid status", K(ret), K(*this));
    } else {
      tablet_id = ref_tablet_ids_[0];
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_split_src_tablet(ObLS &ls, ObTabletHandle &src_tablet_handle) const
{
  int ret = OB_SUCCESS;
  ObTabletID src_tablet_id;
  src_tablet_handle.reset();
  if (OB_FAIL(get_split_src_tablet_id(src_tablet_id))) {
    LOG_WARN("failed to get split src tablet id", K(ret));
  } else if (OB_FAIL(ls.get_tablet(src_tablet_id, src_tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(src_tablet_id), K(ls.get_ls_id()));
  } else if (OB_ISNULL(src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet", K(ret), K(ls.get_ls_id()), K(src_tablet_id));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_end_partkey(ObDatumRowkey &end_partkey)
{
  int ret = OB_SUCCESS;
  end_partkey.reset();
  if (RANGE_PART_SPLIT_DST == status_) {
    if (OB_UNLIKELY(!end_partkey_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid end partkey, maybe not a split dst data/local index tablet", K(ret), KPC(this));
    } else if (OB_FAIL(end_partkey.assign(end_partkey_.datums_, end_partkey_.datum_cnt_))) {
      LOG_WARN("failed to assign partkey", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletSplitMdsUserData::get_tsc_split_info(
    const ObTabletID &tablet_id,
    ObLS &ls,
    const int64_t abs_timeout_us,
    ObIAllocator &allocator,
    ObTabletSplitTscInfo &split_info)
{
  int ret = OB_SUCCESS;
  split_info.reset();
  if (RANGE_PART_SPLIT_DST == status_) {
    ObTabletSplitMdsUserData src_data;
    int64_t timeout_us = 0;
    if (OB_FAIL(get_split_src_tablet(ls, split_info.src_tablet_handle_))) {
      LOG_WARN("failed to get src tablet handle", K(ret));
    } else if (OB_FAIL(ObTabletSplitMdsHelper::get_valid_timeout(abs_timeout_us, timeout_us))) {
      LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
    } else if (OB_FAIL(split_info.src_tablet_handle_.get_obj()->ObITabletMdsInterface::get_split_data(src_data, timeout_us))) {
      LOG_WARN("failed to get split data", K(ret));
    } else {
      split_info.split_cnt_ = src_data.ref_tablet_ids_.count();
    }

    if (OB_SUCC(ret) && 0 != end_partkey_.get_datum_cnt()) { // split dst lob tablet has no partkey
      if (OB_FAIL(end_partkey_.deep_copy(split_info.end_partkey_, allocator))) {
        LOG_WARN("failed to deep copy", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < src_data.ref_tablet_ids_.count(); i++) {
        if (tablet_id == src_data.ref_tablet_ids_.at(i)) {
          if (i == 0) {
            split_info.start_partkey_.set_min_rowkey();
          } else {
            ObTabletHandle prev_dst_tablet_handle;
            ObTabletSplitMdsUserData prev_dst_data;
            ObTabletID prev_dst_tablet_id = src_data.ref_tablet_ids_.at(i-1);
            if (OB_FAIL(ls.get_tablet(prev_dst_tablet_id, prev_dst_tablet_handle))) {
              LOG_WARN("failed to get tablet", K(ret));
            } else if (OB_ISNULL(prev_dst_tablet_handle.get_obj())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid tablet", K(ret));
            } else if (OB_FAIL(ObTabletSplitMdsHelper::get_valid_timeout(abs_timeout_us, timeout_us))) {
              LOG_WARN("failed to get valid timeout", K(ret), K(abs_timeout_us));
            } else if (OB_FAIL(prev_dst_tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(prev_dst_data, timeout_us))) {
              LOG_WARN("failed to get split data", K(ret));
            } else if (OB_UNLIKELY(!prev_dst_data.is_split_dst())) {
              ret = OB_SCHEMA_EAGAIN;
              LOG_WARN("dst tablet is no longer split dst, retry", K(ret), K(tablet_id), K(prev_dst_tablet_id));
            } else if (OB_FAIL(prev_dst_data.end_partkey_.deep_copy(split_info.start_partkey_, allocator))) {
              LOG_WARN("failed to deep copy", K(ret), K(i), K(src_data), K(prev_dst_data));
            }
          }
          break;
        }
      }

      const int64_t partkey_cnt = src_data.partkey_projector_.count();
      if (OB_FAIL(ret)) {
      } else if (split_info.start_partkey_.is_min_rowkey() && OB_FAIL(fill_min_max_datums(partkey_cnt, allocator, split_info.start_partkey_))) {
        LOG_WARN("failed to fill min max datums", K(ret), K(split_info));
      } else if (split_info.end_partkey_.is_max_rowkey() && OB_FAIL(fill_min_max_datums(partkey_cnt, allocator, split_info.end_partkey_))) {
        LOG_WARN("failed to fill min max datums", K(ret), K(split_info));
      } else if (OB_UNLIKELY(!split_info.start_partkey_.is_valid()
            || split_info.start_partkey_.get_datum_cnt() != partkey_cnt
            || split_info.end_partkey_.get_datum_cnt() != partkey_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid split info", K(ret), K(tablet_id), K(partkey_cnt), K(src_data), K(split_info));
      }
    }

    if (OB_SUCC(ret)) {
      bool is_prefix = true;
      for (int64_t i = 0; is_prefix && i < src_data.partkey_projector_.count(); i++) {
        if (src_data.partkey_projector_[i] != i) {
          is_prefix = false;
        }
      }
      split_info.partkey_is_rowkey_prefix_ = is_prefix;
      split_info.split_type_ = ObTabletSplitType::RANGE;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported such op", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!(split_info.is_split_dst_with_partkey() || split_info.is_split_dst_without_partkey()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tsc split info", K(ret), K(tablet_id), K(ls.get_ls_id()), K(split_info));
  }
  return ret;
}

int ObTabletSplitMdsUserData::fill_min_max_datums(const int64_t datum_cnt, ObIAllocator &allocator, ObDatumRowkey &partkey)
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datums = nullptr;
  if (OB_UNLIKELY(!partkey.is_min_rowkey() && !partkey.is_max_rowkey())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not min or max rowkey", K(ret), K(partkey));
  } else if (OB_ISNULL(datums = (ObStorageDatum *)allocator.alloc(sizeof(ObStorageDatum) * datum_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc", K(ret), K(datum_cnt));
  } else {
    datums = new (datums) ObStorageDatum[datum_cnt];
    for (int64_t i = 0; i < datum_cnt; ++ i) {
      if (partkey.is_min_rowkey()) {
        datums[i].set_min();
      } else {
        datums[i].set_max();
      }
    }
    if (OB_FAIL(partkey.assign(datums, datum_cnt))) {
      LOG_WARN("failed to assign datum rowkey", K(ret), KP(datums), K(datum_cnt));
    }
  }
  return ret;
}

void ObTabletSplitMdsUserData::print_ref_tablets_split_data(ObLS &ls) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletSplitMdsUserData data;
  for (int64_t i = 0; i < ref_tablet_ids_.count(); i++) { // overwrite ret
    const ObTabletID &tablet_id = ref_tablet_ids_.at(i);
    if (OB_FAIL(ls.get_tablet(tablet_id, tablet_handle))) {
      LOG_WARN("failed to get split dst tablet", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
      LOG_WARN("failed to get split data", K(ret));
    } else {
      LOG_WARN("dst split info", K(ret), K(tablet_id), K(data));
    }
  }
  return;
}

OB_DEF_SERIALIZE(ObTabletSplitMdsUserData)
{
  int ret = OB_SUCCESS;
  const bool storage_schema_is_valid = storage_schema_.is_valid();
  LST_DO_CODE(OB_UNIS_ENCODE, auto_part_size_, status_, ref_tablet_ids_, partkey_projector_);
  OB_UNIS_ENCODE_ARRAY(end_partkey_.datums_, end_partkey_.datum_cnt_);
  LST_DO_CODE(OB_UNIS_ENCODE, storage_schema_is_valid);
  if (OB_SUCC(ret) && storage_schema_is_valid) {
    LST_DO_CODE(OB_UNIS_ENCODE, storage_schema_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletSplitMdsUserData)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, auto_part_size_, status_, ref_tablet_ids_, partkey_projector_);

  int64_t datum_cnt = 0;
  OB_UNIS_DECODE(datum_cnt);
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(datum_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid datum cnt", K(ret), K(datum_cnt));
  } else if (datum_cnt == 0) {
    end_partkey_.reset();
  } else if (datum_cnt > 0) {
    void *datum_buf = nullptr;
    if (OB_ISNULL(datum_buf = allocator_.alloc(sizeof(ObStorageDatum) * datum_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret), K(datum_cnt));
    } else {
      ObStorageDatum *datums = new (datum_buf) ObStorageDatum[datum_cnt];
      OB_UNIS_DECODE_ARRAY(datums, datum_cnt);
      if (OB_SUCC(ret) && OB_FAIL(end_partkey_.assign(datums, datum_cnt))) {
        LOG_WARN("failed to assign", K(ret));
      }
    }
  }

  bool storage_schema_is_valid = false;
  OB_UNIS_DECODE(storage_schema_is_valid);
  if (OB_SUCC(ret) && pos < data_len) {
    if (storage_schema_is_valid) {
      if (OB_FAIL(storage_schema_.deserialize(allocator_, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize storage schema", K(ret));
      }
    } else {
      storage_schema_.reset();
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletSplitMdsUserData)
{
  int64_t len = 0;
  const bool storage_schema_is_valid = storage_schema_.is_valid();
  LST_DO_CODE(OB_UNIS_ADD_LEN, auto_part_size_, status_, ref_tablet_ids_, partkey_projector_);
  OB_UNIS_ADD_LEN_ARRAY(end_partkey_.datums_, end_partkey_.datum_cnt_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, storage_schema_is_valid);
  if (storage_schema_is_valid) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, storage_schema_);
  }
  return len;
}

} // namespace storage
} // namespace oceanbase
