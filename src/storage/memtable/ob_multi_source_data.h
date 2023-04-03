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

#ifndef OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_
#define OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
}
namespace memtable
{

enum class MultiSourceDataUnitType
{
  // unit ptr type
  TABLET_TX_DATA = 0,
  TABLET_BINDING_INFO = 1,
  TABLET_SEQ = 2,
  // unit list type
  STORAGE_SCHEMA = 3,
  MEDIUM_COMPACTION_INFO = 4,
  MAX_TYPE
};

class ObIMultiSourceDataUnit: public common::ObDLinkBase<ObIMultiSourceDataUnit>
{
public:
  ObIMultiSourceDataUnit()
    :
      is_tx_end_(false),
      unsynced_cnt_for_multi_data_(0),
      sync_finish_(true)
  {}
  virtual ~ObIMultiSourceDataUnit() = default;

  static bool is_unit_list(const MultiSourceDataUnitType type)
  {
    return type >= MultiSourceDataUnitType::STORAGE_SCHEMA && type < MultiSourceDataUnitType::MAX_TYPE;
  }

  // allocator maybe useless for some data type
  virtual int deep_copy_unit(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator = nullptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(src)) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), KP(src));
    } else {
      sync_finish_ = src->sync_finish_;
      is_tx_end_ = src->is_tx_end_;
      unsynced_cnt_for_multi_data_ = src->unsynced_cnt_for_multi_data_;
      ret = deep_copy(src, allocator);
    }
    return ret;
  }
  virtual void reset() = 0;

  virtual int dump() { return common::OB_NOT_IMPLEMENT; }
  virtual int load(storage::ObTablet *tablet) { return common::OB_NOT_IMPLEMENT; }

  virtual bool is_valid() const = 0;
  virtual int64_t get_data_size() const = 0;
  virtual MultiSourceDataUnitType type() const = 0;
  virtual int set_scn(const share::SCN &scn)
  {
    UNUSED(scn);
    return common::OB_SUCCESS;
  }
  bool is_sync_finish() const { return sync_finish_; }
  void set_sync_finish(bool sync_finish) { sync_finish_ = sync_finish; }
  bool is_tx_end() const { return is_tx_end_; }
  void set_tx_end(bool is_tx_end) { is_tx_end_ = is_tx_end; }
  void inc_unsync_cnt_for_multi_data() {
    unsynced_cnt_for_multi_data_++;
    TRANS_LOG(INFO, "unsync_cnt_for_multi_data inc", KPC(this));
  }
  void dec_unsync_cnt_for_multi_data() {
    unsynced_cnt_for_multi_data_--;
    TRANS_LOG(INFO, "unsync_cnt_for_multi_data dec", KPC(this));
  }
  int get_unsync_cnt_for_multi_data() const { return unsynced_cnt_for_multi_data_; }
  virtual bool is_save_last() const { return true; } // only store one data unit with sync_finish=true
  virtual int64_t get_version() const { return common::OB_INVALID_VERSION; } // have to implement for unit list type
  VIRTUAL_TO_STRING_KV(K_(is_tx_end),
                       K_(unsynced_cnt_for_multi_data),
                       K_(sync_finish));
protected:
  bool is_tx_end_;
  int unsynced_cnt_for_multi_data_;
private:
  virtual int deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator = nullptr) = 0;
  bool sync_finish_;
};

class ObMultiSourceData
{
public:
  typedef common::ObDList<ObIMultiSourceDataUnit> ObIMultiSourceDataUnitList;

  ObMultiSourceData(common::ObIAllocator &allocator);
  ~ObMultiSourceData();

  void reset();

  bool is_valid() const;

  bool has_multi_source_data_unit(const MultiSourceDataUnitType type) const;

  int get_multi_source_data_unit(
      ObIMultiSourceDataUnit *const dst,
      ObIAllocator *allocator,
      bool get_lastest = true);
  template<class T>
  int get_multi_source_data_unit_list(
      const T * const useless_unit,
      ObIMultiSourceDataUnitList &dst_list,
      ObIAllocator *allocator);
  template<class T>
  int save_multi_source_data_unit(const T *const src, bool is_callback);
  int update_unsync_cnt_for_multi_data(const MultiSourceDataUnitType multi_source_type, const bool is_inc);
  int get_unsync_cnt_for_multi_data(const MultiSourceDataUnitType multi_source_type, int &unsynced_cnt_for_multi_data);
  int64_t get_all_unsync_cnt_for_multi_data();
private:
  int inner_mark_unit_sync_finish(
      const int64_t unit_type,
      const int64_t unit_version,
      bool save_last_flag);
  void inner_release_rest_unit_data(
      const int64_t list_pos,
      const int64_t unit_version);
  template<class T>
  int deep_copy_data_unit(const T *const src, T *&dst, ObIAllocator &allocator);
  template<class T>
  int save_multi_source_data_unit_in_list(const T *const src, bool is_callback);
  int free_unit_list(const int64_t list_pos);
private:
  static const int64_t MAX_PTR_COUNT = static_cast<int>(MultiSourceDataUnitType::STORAGE_SCHEMA);
  static const int64_t MAX_LIST_COUNT = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE) - static_cast<int>(MultiSourceDataUnitType::STORAGE_SCHEMA);
  static int64_t get_unit_list_array_idx(int64_t type) {
    OB_ASSERT(type >= static_cast<int>(MultiSourceDataUnitType::STORAGE_SCHEMA) && type < static_cast<int>(MultiSourceDataUnitType::MAX_TYPE));
    return type - static_cast<int>(MultiSourceDataUnitType::STORAGE_SCHEMA);
  }
  common::ObIAllocator &allocator_;
  ObIMultiSourceDataUnit *units_[MAX_PTR_COUNT];
  ObIMultiSourceDataUnitList unit_list_array_[MAX_LIST_COUNT];
};

template<class T>
int ObMultiSourceData::deep_copy_data_unit(const T *const src, T *&dst, ObIAllocator &input_allocator)
{
  int ret = OB_SUCCESS;
  dst = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = input_allocator.alloc(src->get_data_size()))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "fail to alloc memory", K(ret));
  } else if (FALSE_IT(dst = new (buf) T())) {
  } else if (OB_FAIL(dst->deep_copy_unit(src, &input_allocator))) {
    TRANS_LOG(WARN, "fail to deep copy", K(ret), KP(dst), KP(src));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != buf) {
      dst->~ObIMultiSourceDataUnit();
      input_allocator.free(buf);
      dst = nullptr;
    }
  }
  return ret;
}

template<class T>
int ObMultiSourceData::save_multi_source_data_unit_in_list(const T *const src, bool is_callback)
{
  int ret = OB_SUCCESS;
  T *dst = nullptr;
  const int64_t list_pos = get_unit_list_array_idx((int64_t)src->type());
  if (!is_callback) { // first save
    if (unit_list_array_[list_pos].get_size() > 0) {
      const ObIMultiSourceDataUnit *last_data_unit = unit_list_array_[list_pos].get_last();
      if (OB_UNLIKELY(last_data_unit->is_sync_finish() && src->get_version() < last_data_unit->get_version())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected order", K(ret), K(list_pos), KPC(last_data_unit), KPC(src));
      }
    }
    if (FAILEDx(deep_copy_data_unit(src, dst, allocator_))) {
      TRANS_LOG(WARN, "failed to deep copy unit", K(ret), K(list_pos), KPC(src));
    } else if (!unit_list_array_[list_pos].add_last(dst)) {
      ret = common::OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "failed to add last", K(ret), K(list_pos), K(unit_list_array_[list_pos]));
    } else if (src->is_save_last() && src->is_sync_finish()) {
      (void)inner_release_rest_unit_data(list_pos, src->get_version());
    }
  } else if (src->is_sync_finish()
      && OB_FAIL(inner_mark_unit_sync_finish(list_pos, src->get_version(), src->is_save_last()))) { // mark finish
    TRANS_LOG(WARN, "failed to makr unit sync finish", K(ret), K(list_pos), KPC(src));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != dst) {
      dst->~ObIMultiSourceDataUnit();
      allocator_.free(dst);
    }
  }
  return ret;
}

template<class T>
int ObMultiSourceData::save_multi_source_data_unit(const T *const src, bool is_callback)
{
  int ret = common::OB_SUCCESS;
  const int pos = static_cast<int>(src->type());
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  T *dst = nullptr;
  const int64_t data_size = src->get_data_size();

  if (OB_UNLIKELY(pos < 0 || pos >= type_count)) {
    ret = common::OB_DATA_OUT_OF_RANGE;
    TRANS_LOG(WARN, "out of range", K(ret), K(pos), K(type_count));
  } else if (ObIMultiSourceDataUnit::is_unit_list(src->type())) {
    if (OB_FAIL(save_multi_source_data_unit_in_list(src, is_callback))) {
      TRANS_LOG(WARN, "failed to save unit in list", K(ret), KPC(src));
    }
  } else if (pos < MAX_PTR_COUNT) {
    if (!is_callback) {
      // overwrite data
      if (OB_FAIL(deep_copy_data_unit(src, dst, allocator_))) {
        TRANS_LOG(WARN, "fail to deep copy data unit", K(ret), KP(dst), KP(src), K(pos));
      } else {
        ObIMultiSourceDataUnit *old_value = units_[pos];
        units_[pos] = dst;
        if (nullptr != old_value) {
          old_value->~ObIMultiSourceDataUnit();
          allocator_.free(old_value);
        }
      }
    } else {
      // update data
      if (units_[pos]->get_data_size() < data_size) {
        ret = common::OB_SIZE_OVERFLOW;
        TRANS_LOG(WARN, "no enough space to update multi_data_source_unit", K(ret), KP(src), K(pos));
      } else if (OB_FAIL(units_[pos]->deep_copy_unit(src))) {
        TRANS_LOG(WARN, "fail to deep copy to update data", K(ret), KP(src), KP(units_[pos]), K(pos));
      }
    }
  }

  return ret;
}

template<class T>
int ObMultiSourceData::get_multi_source_data_unit_list(
    const T * const useless_unit,
    ObIMultiSourceDataUnitList &dst_list,
    ObIAllocator *input_allocator)
{
  int ret = OB_SUCCESS;
  int64_t type = 0;
  int64_t list_pos = -1;
  const int type_count = static_cast<int>(MultiSourceDataUnitType::MAX_TYPE);
  if (OB_UNLIKELY(nullptr == useless_unit
      || FALSE_IT(type = (int64_t)useless_unit->type())
      || type < 0 || type >= type_count
      || nullptr == input_allocator)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(useless_unit), KP(input_allocator));
  } else if (!ObIMultiSourceDataUnit::is_unit_list(static_cast<MultiSourceDataUnitType>(type))) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "not supported for cur data unit", K(ret), K(type));
  } else if (FALSE_IT(list_pos = get_unit_list_array_idx(type))) {
  } else if (OB_UNLIKELY(list_pos < 0 || list_pos >= MAX_LIST_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "wrong unit type", K(ret), K(list_pos), K(type));
  } else {
    T *dst = nullptr;
    DLIST_FOREACH_X(item, unit_list_array_[list_pos], OB_SUCC(ret)) {
      if (OB_UNLIKELY(!item->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "data unit is invalid", K(ret), KPC(item));
      } else if (item->is_sync_finish()) {
        if (OB_FAIL(deep_copy_data_unit(static_cast<const T *>(item), dst, *input_allocator))) {
          TRANS_LOG(WARN, "failed to deep copy unit", K(ret), KPC(item));
        } else if (!dst_list.add_last(dst)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "failed to add data unit into list", K(ret), KPC(dst), K(dst_list));
        }
        if (OB_FAIL(ret) && nullptr != dst) {
          dst->~ObIMultiSourceDataUnit();
          input_allocator->free(dst);
        }
      }
    }
  }
  return ret;
}


} // namespace memtable
} // namespace oceanbase

#endif // OCEABASE_MEMTABLE_OB_MULTI_SOURCE_DATA_
