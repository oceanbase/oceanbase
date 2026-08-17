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
#define USING_LOG_PREFIX BALANCE

#include "ob_balance_group_info.h"
#include "lib/utility/ob_sort.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{
namespace
{
struct ObPartGroupIndexCmp
{
  ObPartGroupIndexCmp(
      const common::ObIArray<int64_t> &part_group_data_sizes,
      const bool is_ascending)
      : part_group_data_sizes_(part_group_data_sizes),
        is_ascending_(is_ascending)
  {
  }

  bool operator()(const int64_t left, const int64_t right) const
  {
    bool cmp_ret = false;
    const int64_t left_size = part_group_data_sizes_.at(left);
    const int64_t right_size = part_group_data_sizes_.at(right);
    if (left_size == right_size) {
      cmp_ret = left < right;
    } else if (is_ascending_) {
      cmp_ret = left_size < right_size;
    } else {
      cmp_ret = left_size > right_size;
    }
    return cmp_ret;
  }

  const common::ObIArray<int64_t> &part_group_data_sizes_;
  bool is_ascending_;
};
} // namespace

int ObTransferPartGroup::add_part(const ObTransferPartInfo &part, int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! part.is_valid() || data_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(part_list_.push_back(part))) {
    LOG_WARN("push back part into part info fail", KR(ret), K(part), K(part_list_));
  } else {
    data_size_ += data_size;
  }
  return ret;
}

///////////////////////////////////////////////

ObBalanceGroupInfo::~ObBalanceGroupInfo()
{
  // for each partition group in array, release its memory
  for (int64_t i = 0; i < part_groups_.count(); i++) {
    ObTransferPartGroup *part_group = part_groups_.at(i);
    if (OB_NOT_NULL(part_group)) {
      part_group->~ObTransferPartGroup();
      alloc_.free(part_group);
      part_group = NULL;
    }
  }

  part_groups_.destroy();
}


int ObBalanceGroupInfo::append_part(ObTransferPartInfo &part,
    const int64_t data_size,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! part.is_valid() || data_size < 0 || !is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(create_new_part_group_if_needed_(part_group_uid))) {
    LOG_WARN("create new part group if needed failed", KR(ret), K(part_group_uid), K_(last_part_group_uid));
  } else if (OB_UNLIKELY(part_groups_.count() <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no partition groups in this balance group", KPC(this), KR(ret), K(part));
  } else {
    ObTransferPartGroup *part_group = part_groups_.at(part_groups_.count() - 1);

    if (OB_ISNULL(part_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data", KR(ret), KPC(part_group), KPC(this));
    } else if (OB_FAIL(part_group->add_part(part, data_size))) {
      LOG_WARN("add part into partition group fail", KR(ret),
          KPC(part_group), K(part), K(data_size), K(part_group_uid), KPC(this));
    }

    LOG_TRACE("[ObBalanceGroupInfo] append part", K(part), K(data_size), K(part_group_uid),
        "part_group_count", part_groups_.count(), KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupInfo::create_new_part_group_if_needed_(const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_group_uid", KR(ret), K(part_group_uid));
  } else if (part_group_uid != last_part_group_uid_) {
    // only create new part group when part_group_uid is different from last_part_group_uid_
    // (Scenarios with invalid last_part_group_uid_ have been included)
    ObTransferPartGroup *part_group = NULL;
    const int64_t part_group_size = sizeof(ObTransferPartGroup);
    void *buf = alloc_.alloc(part_group_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_ISNULL(part_group = new(buf) ObTransferPartGroup(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("construct ObTransferPartGroup fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_FAIL(part_groups_.push_back(part_group))) {
      LOG_WARN("push back new partition group fail", KR(ret), K(part_group), K(part_groups_));
    } else {
      last_part_group_uid_ = part_group_uid;
    }
  }
  return ret;
}

int64_t ObBalanceGroupInfo::get_data_size() const
{
  int64_t data_size = 0;
  for (int64_t i = 0; i < part_groups_.count(); ++i) {
    if (OB_NOT_NULL(part_groups_.at(i))) {
      data_size += part_groups_.at(i)->get_data_size();
    }
  }
  return data_size;
}

int ObBalanceGroupInfo::select_part_groups_(
    const int64_t part_group_count,
    const int64_t data_size_threshold,
    common::ObArray<int64_t> &selected_indexes,
    int64_t &selected_data_size) const
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> unselected_indexes;
  selected_indexes.reset();
  selected_data_size = 0;
  if (OB_UNLIKELY(part_group_count <= 0 || part_group_count > part_groups_.count()
      || data_size_threshold < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_group_count), K(data_size_threshold),
        "part_group_count_in_bg", part_groups_.count());
  } else {
    // Keep the historical pop-back order as the initial selection. Only replace
    // oversized selected groups with smaller unselected groups when necessary.
    const int64_t first_selected_idx = part_groups_.count() - part_group_count;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_groups_.count(); ++i) {
      ObTransferPartGroup *pg = part_groups_.at(i);
      if (OB_ISNULL(pg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group is null", KR(ret), K(i), K(part_groups_));
      } else {
        if (i < first_selected_idx) {
          if (OB_FAIL(unselected_indexes.push_back(i))) {
            LOG_WARN("push back unselected index failed", KR(ret), K(i));
          }
        } else {
          // The suffix is the historical initial selection.
          if (OB_FAIL(selected_indexes.push_back(i))) {
            LOG_WARN("push back selected index failed", KR(ret), K(i));
          } else {
            selected_data_size += pg->get_data_size();
          }
        }
      }
    }

    if (OB_SUCC(ret) && selected_data_size > data_size_threshold) {
      common::ObArray<int64_t> part_group_data_sizes;
      if (OB_FAIL(part_group_data_sizes.reserve(part_groups_.count()))) {
        LOG_WARN("reserve part group data sizes failed", KR(ret),
            "part_group_count", part_groups_.count());
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < part_groups_.count(); ++i) {
        ObTransferPartGroup *pg = part_groups_.at(i);
        if (OB_ISNULL(pg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part group is null", KR(ret), K(i), K(part_groups_));
        } else if (OB_FAIL(part_group_data_sizes.push_back(pg->get_data_size()))) {
          LOG_WARN("push back part group data size failed", KR(ret), K(i), KPC(pg));
        }
      }
      if (OB_SUCC(ret)
          && OB_UNLIKELY(part_group_data_sizes.count() != part_groups_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group data size count mismatch", KR(ret),
            "data_size_count", part_group_data_sizes.count(),
            "part_group_count", part_groups_.count());
      }
      if (OB_SUCC(ret)) {
        ObPartGroupIndexCmp selected_cmp(
            part_group_data_sizes, false /* is_ascending */);
        ObPartGroupIndexCmp unselected_cmp(
            part_group_data_sizes, true /* is_ascending */);
        lib::ob_sort(selected_indexes.begin(), selected_indexes.end(), selected_cmp);
        lib::ob_sort(unselected_indexes.begin(), unselected_indexes.end(), unselected_cmp);
        int64_t swap_count = 0;
        for (int64_t i = 0;
            i < selected_indexes.count()
                && i < unselected_indexes.count()
                && selected_data_size > data_size_threshold;
            ++i) {
          const int64_t selected_idx = selected_indexes.at(i);
          const int64_t unselected_idx = unselected_indexes.at(i);
          const int64_t selected_size = part_group_data_sizes.at(selected_idx);
          const int64_t unselected_size = part_group_data_sizes.at(unselected_idx);
          if (unselected_size >= selected_size) {
            break;
          } else {
            selected_indexes.at(i) = unselected_idx;
            selected_data_size -= selected_size;
            selected_data_size += unselected_size;
            ++swap_count;
          }
        }
        LOG_TRACE("select part groups by data size", KR(ret), K(part_group_count),
            K(data_size_threshold), K(selected_data_size), K(swap_count), K(selected_indexes));
      }
    }
  }
  return ret;
}

int ObBalanceGroupInfo::pop_back(const int64_t part_group_count,
    const int64_t data_size_threshold,
    share::ObTransferPartList &part,
    int64_t &popped_part_count,
    int64_t &popped_data_size)
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> selected_indexes;

  popped_part_count = 0;
  popped_data_size = 0;
  if (OB_UNLIKELY(part_group_count > part_groups_.count() || part_group_count <= 0
      || data_size_threshold < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_group_count), K(data_size_threshold),
        K(part_groups_.count()));
  } else if (OB_FAIL(select_part_groups_(
      part_group_count, data_size_threshold, selected_indexes, popped_data_size))) {
    LOG_WARN("select part groups failed", KR(ret), K(part_group_count), K(data_size_threshold));
  } else {
    popped_data_size = 0;
    lib::ob_sort(selected_indexes.begin(), selected_indexes.end());
    for (int64_t i = selected_indexes.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const int64_t pg_idx = selected_indexes.at(i);
      const bool remove_last = (pg_idx == part_groups_.count() - 1);
      bool is_removed = false;
      ObTransferPartGroup *pg = part_groups_.at(pg_idx);
      if (OB_ISNULL(pg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part group, is NULL, unexpected", K(pg), K(pg_idx), K(part_groups_));
      } else if (OB_FAIL(part_groups_.remove(pg_idx))) {
        LOG_WARN("remove part group from array fail", KR(ret), K(pg_idx), K(part_groups_));
      } else {
        is_removed = true;
        if (remove_last) {
          last_part_group_uid_ = OB_INVALID_ID;
        }
        if (OB_FAIL(append(part, pg->get_part_list()))) {
          LOG_WARN("append array to part list fail", KR(ret), K(part), KPC(pg));
        } else {
          popped_part_count += pg->count();
          popped_data_size += pg->get_data_size();
        }
      }

      // Free only after the pointer has been removed from part_groups_.
      if (is_removed && OB_NOT_NULL(pg)) {
        pg->~ObTransferPartGroup();
        alloc_.free(pg);
        pg = NULL;
      }
    }
  }
  return ret;
}

}
}
