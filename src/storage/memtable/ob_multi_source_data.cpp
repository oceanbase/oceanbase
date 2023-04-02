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

#include "storage/memtable/ob_multi_source_data.h"

namespace oceanbase
{
namespace memtable
{

ObMultiSourceData::ObMultiSourceData(common::ObIAllocator &allocator)
  : allocator_(allocator),
    units_()
{
  MEMSET(units_, 0, sizeof(units_));
}

ObMultiSourceData::~ObMultiSourceData()
{
  reset();
}

void ObMultiSourceData::reset()
{
  for (int i = 0; i < MAX_PTR_COUNT; ++i) {
    ObIMultiSourceDataUnit *value = units_[i];
    if (nullptr != value) {
      value->~ObIMultiSourceDataUnit();
      allocator_.free(value);
      units_[i] = nullptr;
    }
  }
  for (int i = 0; i < MAX_LIST_COUNT; ++i) {
    free_unit_list(i);
  }
}

bool ObMultiSourceData::is_valid() const
{
  return true;
}

bool ObMultiSourceData::has_multi_source_data_unit(const MultiSourceDataUnitType type) const
{
  bool bret = false;
  const int pos = static_cast<int>(type);
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  if (pos >= 0 && pos < type_count) {
    if (ObIMultiSourceDataUnit::is_unit_list(static_cast<MultiSourceDataUnitType>(pos))) {
      const int64_t list_pos = get_unit_list_array_idx(pos);
      if (list_pos >= 0 && list_pos < MAX_LIST_COUNT) {
        DLIST_FOREACH_NORET(item, unit_list_array_[list_pos]) {
          if (item->is_valid()) {
            bret = true;
            break;
          }
        }
      }
    } else if (pos < MAX_PTR_COUNT) {
      bret = nullptr != units_[pos] && units_[pos]->is_valid();
    }
  }
  return bret;
}

int ObMultiSourceData::get_multi_source_data_unit(
    ObIMultiSourceDataUnit *const dst,
    ObIAllocator *allocator,
    bool get_lastest)
{
  int ret = OB_SUCCESS;
  const int pos = static_cast<int>(dst->type());
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  ObIMultiSourceDataUnit *src = nullptr;

  if (OB_UNLIKELY(pos < 0 || pos >= type_count)) {
    ret = OB_DATA_OUT_OF_RANGE;
    TRANS_LOG(WARN, "out of range", K(ret), K(pos), K(type_count));
  } else if (ObIMultiSourceDataUnit::is_unit_list(static_cast<MultiSourceDataUnitType>(pos))) {
    const int64_t list_pos = get_unit_list_array_idx(pos);
    if (OB_UNLIKELY(list_pos < 0 || list_pos >= MAX_LIST_COUNT)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "wrong unit type", K(ret), K(list_pos));
    }
    if (get_lastest) {
      DLIST_FOREACH_BACKWARD_X(item, unit_list_array_[list_pos], OB_SUCC(ret)) {
        if (item->is_sync_finish()) {
          src = item;
          break;
        }
      }
    } else {
      DLIST_FOREACH_X(item, unit_list_array_[list_pos], OB_SUCC(ret)) {
        if (item->is_sync_finish()) {
          src = item;
          break;
        }
      }
    }
    if (nullptr == src) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else if (pos >= MAX_PTR_COUNT) {
    ret = OB_DATA_OUT_OF_RANGE;
    TRANS_LOG(WARN, "pos is out of range", K(ret), K(pos));
  } else if (OB_ISNULL(src = units_[pos])) {
    ret = OB_ENTRY_NOT_EXIST;
    TRANS_LOG(DEBUG, "src is null", K(ret), KP(src), K(pos));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dst->deep_copy_unit(src, allocator))) {
    TRANS_LOG(WARN, "fail to deep copy", K(ret), KP(dst), KP(src), K(pos));
  }

  return ret;
}

int ObMultiSourceData::free_unit_list(const int64_t list_pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(list_pos < 0 || list_pos >= MAX_LIST_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "wrong unit type", K(ret), K(list_pos));
  } else {
    DLIST_FOREACH_REMOVESAFE(item, unit_list_array_[list_pos]) {
      unit_list_array_[list_pos].remove(item);
      item->~ObIMultiSourceDataUnit();
      allocator_.free(item);
    }
  }
  return ret;
}

void ObMultiSourceData::inner_release_rest_unit_data(
    const int64_t list_pos,
    const int64_t unit_version)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_REMOVESAFE(item, unit_list_array_[list_pos]) {
    if (item->get_version() != unit_version) {
      unit_list_array_[list_pos].remove(item);
      item->~ObIMultiSourceDataUnit();
      allocator_.free(item);
    }
  }
}

// pos is valid
int ObMultiSourceData::inner_mark_unit_sync_finish(
    const int64_t list_pos,
    const int64_t unit_version,
    bool save_last_flag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(list_pos < 0 || list_pos >= MAX_LIST_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "wrong unit list pos", K(ret), K(list_pos));
  } else if (OB_UNLIKELY(unit_list_array_[list_pos].is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unit list is empty", K(ret), K(list_pos), K(unit_version));
  } else {
    ObIMultiSourceDataUnit *last_item = static_cast<ObIMultiSourceDataUnit*>(unit_list_array_[list_pos].get_last());
    if (OB_UNLIKELY(nullptr == last_item || last_item->get_version() != unit_version || last_item->is_sync_finish())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mark unit sync finish meet unexpected failure", K(ret), K(list_pos), K(unit_version),
          KPC(last_item));
    } else {
      last_item->set_sync_finish(true);

      if (save_last_flag) {
        (void)inner_release_rest_unit_data(list_pos, unit_version);
      }
    }
  }
  return ret;
}

int ObMultiSourceData::update_unsync_cnt_for_multi_data(const MultiSourceDataUnitType multi_source_type, const bool is_inc)
{
  int ret = OB_SUCCESS;
  const int pos = static_cast<int>(multi_source_type);
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  ObIMultiSourceDataUnit *src = nullptr;

  if (OB_UNLIKELY(pos < 0 || pos >= type_count)) {
    ret = OB_DATA_OUT_OF_RANGE;
    TRANS_LOG(WARN, "out of range", K(ret), K(pos), K(type_count));
  } else if (MultiSourceDataUnitType::TABLET_TX_DATA == multi_source_type
             || MultiSourceDataUnitType::TABLET_BINDING_INFO == multi_source_type
             || MultiSourceDataUnitType::TABLET_SEQ == multi_source_type) {
    if (OB_ISNULL(src = units_[pos])) {
      ret = OB_ENTRY_NOT_EXIST;
      TRANS_LOG(DEBUG, "src is null", K(ret), KP(src), K(pos));
    } else if (is_inc) {
      src->inc_unsync_cnt_for_multi_data();
    } else {
      src->dec_unsync_cnt_for_multi_data();
    }
  }

  return ret;
}

int ObMultiSourceData::get_unsync_cnt_for_multi_data(const MultiSourceDataUnitType multi_source_type, int &unsynced_cnt_for_multi_data)
{
  int ret = OB_SUCCESS;
  const int pos = static_cast<int>(multi_source_type);
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  ObIMultiSourceDataUnit *src = nullptr;
  unsynced_cnt_for_multi_data = -1;

  if (OB_UNLIKELY(pos < 0 || pos >= type_count)) {
    ret = OB_DATA_OUT_OF_RANGE;
    TRANS_LOG(WARN, "out of range", K(ret), K(pos), K(type_count));
  } else if (MultiSourceDataUnitType::TABLET_TX_DATA == multi_source_type
             || MultiSourceDataUnitType::TABLET_BINDING_INFO == multi_source_type
             || MultiSourceDataUnitType::TABLET_SEQ == multi_source_type) {
    if (OB_ISNULL(src = units_[pos])) {
      ret = OB_ENTRY_NOT_EXIST;
      TRANS_LOG(DEBUG, "src is null", K(ret), KP(src), K(pos));
    } else {
      unsynced_cnt_for_multi_data = src->get_unsync_cnt_for_multi_data();
    }
  }

  return ret;
}

int64_t ObMultiSourceData::get_all_unsync_cnt_for_multi_data()
{
  int ret = OB_SUCCESS;
  int64_t unsynced_cnt_for_multi_data = 0;
  const int64_t num = MAX_LIST_COUNT + MAX_PTR_COUNT;

  for (int i = 0; i < num; ++i) {
    if (i < MAX_PTR_COUNT) {
      ObIMultiSourceDataUnit *value = units_[i];
      if (nullptr != value && value->is_valid()) {
        int64_t cnt = value->get_unsync_cnt_for_multi_data();
        unsynced_cnt_for_multi_data += cnt;
        if (0 != cnt) {
          TRANS_LOG(INFO, "unsync_cnt of unsync_cnt of data_unit is not 0", KPC(value));
        }
      }
    } else {
      const int64_t list_pos = get_unit_list_array_idx(i);
      if (unit_list_array_[list_pos].get_size() > 0) {
        const ObIMultiSourceDataUnit *last_data_unit = unit_list_array_[list_pos].get_last();
        int64_t cnt = last_data_unit->get_unsync_cnt_for_multi_data();
        if (0 != cnt) {
          TRANS_LOG(INFO, "unsync_cnt of last_data_unit in unit_list_array is not 0", KPC(last_data_unit), K(unit_list_array_[list_pos].get_size()));
        }
        unsynced_cnt_for_multi_data += cnt;
      }
    }
  }

  return unsynced_cnt_for_multi_data;
}
} // namespace memtable
} // namespace oceanbase
