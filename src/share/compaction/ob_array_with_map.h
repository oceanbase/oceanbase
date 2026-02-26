//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_ARRAY_WITH_MAP_H_
#define OB_SHARE_COMPACTION_ARRAY_WITH_MAP_H_
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_delegate.h"
#include "common/ob_tablet_id.h"
namespace oceanbase
{
namespace share
{

template <typename ITEM>
class ObArrayWithMap
{
public:
  ObArrayWithMap(const bool need_map = true)
    : tablet_cnt_(0),
      array_(),
      map_(),
      need_index_map_(need_map)
  {}
  ~ObArrayWithMap() {}
  int init(const int64_t tenant_id, const int64_t expect_val_cnt);
  int init(const int64_t tenant_id, const ObArrayWithMap &other);
  int reserve(const int64_t cnt) { return array_.reserve(cnt); }
  int push_back(const ITEM &item);
  int get(const common::ObTabletID &tablet_id, const ITEM *&item_ptr) const;
  const common::ObArray<ITEM> &get_array() const { return array_; }
  int64_t get_tablet_cnt() const { return tablet_cnt_; }
  CONST_DELEGATE_WITH_RET(array_, empty, bool);
  CONST_DELEGATE_WITH_RET(array_, count, int64_t);
  CONST_DELEGATE_WITH_RET(array_, at, const ITEM &);
  void reset() {
    tablet_cnt_ = 0;
    array_.reset();
    if (map_.created()) {
      map_.reuse();
    }
  }
  TO_STRING_KV(K_(tablet_cnt), K_(array), K_(need_index_map));
private:
  static const int64_t BUILD_HASH_MAP_THRESHOLD = 16;
  typedef common::hash::ObHashMap<common::ObTabletID, int64_t> KeyIndexMap;
  int64_t tablet_cnt_;
  common::ObArray<ITEM> array_;
  KeyIndexMap map_;
  bool need_index_map_;
};

template <typename ITEM>
int ObArrayWithMap<ITEM>::init(
  const int64_t tenant_id,
  const int64_t expect_val_cnt)
{
  int ret = OB_SUCCESS;
  array_.set_attr(ObMemAttr(tenant_id, "ArrayIdxArr"));
  if (OB_FAIL(array_.reserve(expect_val_cnt))) {
    STORAGE_LOG(WARN, "failed to reserve array", K(ret), K(expect_val_cnt));
  } else if (need_index_map_ && expect_val_cnt > BUILD_HASH_MAP_THRESHOLD && !map_.created()) {
    if (OB_FAIL(map_.create(expect_val_cnt, "ArrayIdxMap", "ArrayIdxMap", tenant_id))) {
      STORAGE_LOG(WARN, "failed to build map", K(ret), K(expect_val_cnt));
    }
  }
  return ret;
}

template <typename ITEM>
int ObArrayWithMap<ITEM>::init(
  const int64_t tenant_id,
  const ObArrayWithMap &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(tenant_id, other.array_.count()))) {
    STORAGE_LOG(WARN, "failed to init", K(ret), K(tenant_id), K(other));
  } else if (map_.created()) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < other.array_.count(); ++idx) {
      if (OB_FAIL(push_back(other.at(idx)))) {
         STORAGE_LOG(WARN, "failed to push item", K(ret), K(idx), K(other.at(idx)));
      }
    }
  } else if (OB_FAIL(array_.assign(other.array_))) {
    STORAGE_LOG(WARN, "failed to assign array", K(ret), K(other));
  }
  return ret;
}

template <typename ITEM>
int ObArrayWithMap<ITEM>::push_back(const ITEM &item)
{
  int ret = OB_SUCCESS;
  const int64_t last_idx = array_.count() - 1;
  if (OB_FAIL(array_.push_back(item))) {
    STORAGE_LOG(WARN, "failed to push item", K(ret), K(item));
  } else if (last_idx >= 0 && array_.at(last_idx).get_tablet_id() == item.get_tablet_id()) {
    // same tablet
  } else {
    ++tablet_cnt_;
    if (map_.created() && OB_FAIL(map_.set_refactored(item.get_tablet_id(), array_.count() - 1))) {
      STORAGE_LOG(WARN, "failed to push item", K(ret), K(item));
    }
  }
  return ret;
}

template <typename ITEM>
int ObArrayWithMap<ITEM>::get(const common::ObTabletID &tablet_id,  const ITEM *&item_ptr) const
{
  int ret = OB_SUCCESS;
  item_ptr = NULL;
  if (map_.created()) {
    int64_t idx = -1;
    if (OB_FAIL(map_.get_refactored(tablet_id, idx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        STORAGE_LOG(WARN, "failed to push item", K(ret), K(tablet_id));
      }
    } else if (OB_UNLIKELY(idx < 0 || idx >= array_.count())) {
      ret = OB_INVALID_DATA;
      STORAGE_LOG(WARN, "invalid idx", K(ret), K(tablet_id), K(idx), KPC(this));
    } else {
      item_ptr = &array_.at(idx);
    }
  } else {
    for (int64_t idx = 0; idx < array_.count(); ++idx) {
      if (array_.at(idx).get_tablet_id() == tablet_id) {
        item_ptr = &array_.at(idx);
        break;
      }
    }
    if (NULL == item_ptr) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_ARRAY_WITH_MAP_H_
