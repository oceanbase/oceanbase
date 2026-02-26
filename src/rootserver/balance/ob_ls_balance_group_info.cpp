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


#include "ob_ls_balance_group_info.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{

int ObLSBalanceGroupInfo::init(const ObLSID &ls_id, const int64_t balanced_ls_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_), K(ls_id), KPC(this));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || balanced_ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(balanced_ls_num));
  } else if (OB_FAIL(bg_map_.create(MAP_BUCKET_NUM, "LSBGMap"))) {
    LOG_WARN("create map for balance group fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else if (OB_FAIL(orig_part_group_cnt_map_.create(MAP_BUCKET_NUM, "LSPGCntMap"))) {
    LOG_WARN("create map for balance group original partition group cnt fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else {
    ls_id_ = ls_id;
    balanced_ls_num_ = balanced_ls_num;
    inited_ = true;
  }
  return ret;
}

void ObLSBalanceGroupInfo::destroy()
{
  FOREACH(iter, bg_map_) {
    ObBalanceGroupInfo *bg = iter->second;
    if (OB_NOT_NULL(bg)) {
      bg->~ObBalanceGroupInfo();
      alloc_.free(bg);
      bg = NULL;
    }
  }

  bg_map_.destroy();
  orig_part_group_cnt_map_.destroy();
  ls_id_.reset();
  balanced_ls_num_ = 0;
  inited_ = false;
}

int ObLSBalanceGroupInfo::append_part_into_balance_group(
    const ObBalanceGroupID &bg_id,
    const schema::ObSimpleTableSchemaV2 &table_schema,
    share::ObTransferPartInfo &part,
    const int64_t data_size,
    const uint64_t part_group_uid,
    const int64_t balance_weight)
{
  int ret = OB_SUCCESS;
  ObBalanceGroupInfo *bg = NULL;
  int64_t part_group_cnt = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || !is_valid_id(part_group_uid) || balance_weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(part), K(data_size), K(part_group_uid), K(balance_weight));
  } else if (OB_FAIL(get_or_create_(bg_id, bg))) {
    LOG_WARN("get or create balance group fail", KR(ret), K(bg_id));
  } else if (OB_ISNULL(bg)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("balance group is invalid", KR(ret), KPC(bg), K(bg_id));
  } else if (OB_FAIL(bg->append_part(table_schema, part, data_size, part_group_uid, balance_weight))) {
    LOG_WARN("append part info balance group fail", KR(ret), K(part),
        K(data_size), K(part_group_uid), K(balance_weight), K(table_schema));
  } else if (FALSE_IT(part_group_cnt = bg->get_part_groups_count())) {
  } else if (OB_FAIL(orig_part_group_cnt_map_.set_refactored(bg_id, part_group_cnt, 1/*overwrite*/))) {
    LOG_WARN("overwrite partition group count map fail", KR(ret), K(bg_id), K(part_group_cnt));
  }

  return ret;
}

int ObLSBalanceGroupInfo::get_or_create_(
    const ObBalanceGroupID &bg_id,
    ObBalanceGroupInfo *&bg)
{
  int ret = OB_SUCCESS;
  int64_t bg_size = sizeof(ObBalanceGroupInfo);
  void *buf = nullptr;
  bg = nullptr;
  if (OB_FAIL(bg_map_.get_refactored(bg_id, bg))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = alloc_.alloc(bg_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for ObBalanceGroupInfo fail", KR(ret), K(bg_size), K(buf));
      } else if (OB_ISNULL(bg = new(buf) ObBalanceGroupInfo(alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("construct ObBalanceGroupInfo fail", KR(ret), K(bg_size), K(buf), K(bg_id));
      } else if (OB_FAIL(bg->init(bg_id, ls_id_, balanced_ls_num_))) {
        LOG_WARN("init ObBalanceGroupInfo fail", KR(ret), KPC(bg));
      } else if (OB_FAIL(bg_map_.set_refactored(bg_id, bg))) {
        LOG_WARN("set into balance group map fail", KR(ret), K(bg_id), KPC(bg));
      } else if (OB_FAIL(orig_part_group_cnt_map_.set_refactored(bg_id, 0))) {
        LOG_WARN("set into original partition group count map fail", KR(ret), K(bg_id));
      }
    } else {
      LOG_WARN("get from balance group map fail", KR(ret), K(bg_id));
    }
  }
  return ret;
}

int ObLSBalanceGroupInfo::transfer_out_by_factor(
    ObLSBalanceGroupInfo &dst_ls_bg_info,
    const float factor,
    share::ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_ls_bg_info.is_inited() || factor <= OB_FLOAT_EPSINON)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst_ls_bg_info or factor is invalid", KR(ret), K(factor));
  } else {
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      const ObBalanceGroupID &bg_id = iter->first;
      ObBalanceGroupInfo *src_bg_info = iter->second;
      ObBalanceGroupInfo *dst_bg_info = nullptr;

      // original count before transfer-out partitions
      const int64_t *total_count_ptr = orig_part_group_cnt_map_.get(bg_id);

      if (OB_ISNULL(src_bg_info)
          || OB_ISNULL(total_count_ptr)
          || OB_UNLIKELY(0 == *total_count_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("original part group count or balance group info is invalid, unexpected", KR(ret),
            K(total_count_ptr), K(bg_id), KPC(src_bg_info));
      } else if (OB_UNLIKELY(src_bg_info->get_part_groups_count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group can not be empty, unexpected", KR(ret), K(bg_id), KPC(src_bg_info));
      } else if (OB_FAIL(dst_ls_bg_info.get_or_create_(bg_id, dst_bg_info))) {
        LOG_WARN("get or create dst_bg_info fail", KR(ret), K(dst_ls_bg_info), K(bg_id));
      } else if (OB_ISNULL(dst_bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dst_bg_info is expected to be not null", KR(ret), K(dst_bg_info));
      } else {
        // Remove the appropriate proportion of partitions from the balance group
        //
        // expected remove count should be computed by factor of total count.
        // It should be ceil()
        const int64_t total_count = *total_count_ptr;
        const int64_t expected_remove_count = ceil(factor * total_count);
        const int64_t avail_count = src_bg_info->get_part_groups_count();

        // left count after remove should be greater than remove count
        const int64_t left_count_lower_bound = expected_remove_count;
        const int64_t can_remove_count_upper_bound = avail_count - left_count_lower_bound;

        const int64_t remove_count = std::min(can_remove_count_upper_bound, expected_remove_count);
        // Get data size threshold
        const float data_size_ratio = static_cast<float>(remove_count) / total_count;
        const int64_t data_size_threshold = ceil(src_bg_info->get_part_groups_data_size() * data_size_ratio);

        int64_t removed_part_count = 0;
        int64_t selected_data_size = 0;
        ObArray<ObPartGroupInfo *> selected_part_groups;

        if (OB_UNLIKELY(remove_count >= avail_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("remove count should be greater than avail count", K(avail_count), K(remove_count));
          // transfer out part only when remove count > 0
        } else {
          // Step 1: Use round-robin to select part groups (for even distribution by count)
          // Collect selected part groups temporarily to check data size
          for (int64_t i = 0; OB_SUCC(ret) && i < remove_count; i++) {
            ObPartGroupInfo *part_group = nullptr;
            if (OB_FAIL(src_bg_info->transfer_out_by_round_robin(*dst_bg_info, part_group))) {
              LOG_WARN("fail to transfer out part group", KR(ret), K(dst_bg_info));
            } else if (OB_ISNULL(part_group)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part group is null", KR(ret), KP(part_group));
            } else if (OB_FAIL(selected_part_groups.push_back(part_group))) {
              LOG_WARN("fail to push back part group", KR(ret), KPC(part_group));
            }
          }

          // Step 2: If selected data size is too large, replace large part groups with smaller ones
          if (FAILEDx(try_reduce_selected_data_size_by_swap_(
              data_size_threshold,
              src_bg_info,
              dst_bg_info,
              selected_part_groups))) {
            LOG_WARN("fail to reduce selected data size by swap", KR(ret),
                K(data_size_threshold), K(selected_part_groups));
          }

          // Step 3: Append all selected part groups to part_list
          ARRAY_FOREACH(selected_part_groups, j) {
            ObPartGroupInfo *part_group = selected_part_groups.at(j);
            if (OB_ISNULL(part_group)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part group is null", KR(ret), KP(part_group));
            } else if (OB_FAIL(append(part_list, part_group->get_part_list()))) {
              LOG_WARN("fail to append part list", KR(ret), K(part_list), KPC(part_group));
            } else {
              removed_part_count += part_group->get_part_list().count();
              selected_data_size += part_group->get_data_size();
            }
          }
        }
        FLOG_INFO("transfer out partition groups from LS one balance group by factor", KR(ret),
            K(factor), K_(ls_id), K(bg_id),
            "removed_part_group_count", remove_count,
            K(removed_part_count), K(data_size_threshold), K(selected_data_size),
            K(total_count), K(avail_count), K(expected_remove_count));
      }
    }
  }
  return ret;
}

// Use swap strategy: swap large part groups in dst with small ones in src
int ObLSBalanceGroupInfo::try_reduce_selected_data_size_by_swap_(
    const int64_t data_size_threshold,
    ObBalanceGroupInfo *src_bg_info,
    ObBalanceGroupInfo *dst_bg_info,
    common::ObArray<ObPartGroupInfo *> &selected_part_groups)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_bg_info) || OB_ISNULL(dst_bg_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_bg_info or dst_bg_info is null", KR(ret), KP(src_bg_info), KP(dst_bg_info));
  } else if (selected_part_groups.empty()
      || sum_pg_data_size_(selected_part_groups) <= data_size_threshold) {
    // No need to reduce, skip
  } else { // Try multiple swaps to reduce total data size
    int64_t total_data_size = sum_pg_data_size_(selected_part_groups);
    const int64_t original_total_data_size = total_data_size;
    const int64_t max_swap_attempts = selected_part_groups.count(); // avoid an infinite loop
    int64_t swap_count = 0;
    while (OB_SUCC(ret)
        && swap_count < max_swap_attempts
        && total_data_size > data_size_threshold) {
      ObPartGroupInfo *selected_largest_pg = nullptr;
      ObPartGroupInfo *src_smallest_pg = nullptr;
      int64_t largest_pg_idx = OB_INVALID_INDEX;
      // Get largest from selected_part_groups and smallest from src_bg_info
      // selected_largest_pg must be in dst_bg_info
      if (OB_FAIL(get_largest_part_group_from_array_(
          selected_part_groups,
          selected_largest_pg,
          largest_pg_idx))) {
        LOG_WARN("fail to get largest part group from array", KR(ret), K(selected_part_groups));
      } else if (OB_FAIL(src_bg_info->get_smallest_part_group(src_smallest_pg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS; // No more part groups in src
          break;
        } else {
          LOG_WARN("fail to get smallest part group from src", KR(ret), KPC(src_bg_info));
        }
      } else if (OB_ISNULL(selected_largest_pg) || OB_ISNULL(src_smallest_pg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null part group", KR(ret), KP(selected_largest_pg), KP(src_smallest_pg));
      } else if (src_smallest_pg->get_data_size() >= selected_largest_pg->get_data_size()) {
        // No benefit to swap, break early
        break;
      } else if (OB_FAIL(dst_bg_info->swap_for_smallest_pg(selected_largest_pg, *src_bg_info))) {
        LOG_WARN("swap for smallest pg failed", KR(ret), KPC(dst_bg_info),
            KPC(src_bg_info), KPC(selected_largest_pg), KPC(src_smallest_pg));
      } else if (OB_UNLIKELY(largest_pg_idx < 0 || largest_pg_idx >= selected_part_groups.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid largest pg index", KR(ret), K(largest_pg_idx),
            K(selected_part_groups), KPC(selected_largest_pg));
      } else {
        // replace dst_largest_pg with src_smallest_pg
        selected_part_groups.at(largest_pg_idx) = src_smallest_pg;
        total_data_size -= selected_largest_pg->get_data_size();
        total_data_size += src_smallest_pg->get_data_size();
        swap_count++;
        LOG_TRACE("swap part group successfully", KR(ret), K(data_size_threshold),
            KPC(selected_largest_pg), KPC(src_smallest_pg), K(total_data_size), K(swap_count));
      }
    }
    LOG_INFO("reduce selected data size by swap finished",
        KR(ret), K(swap_count), K(max_swap_attempts), K(data_size_threshold),
        K(original_total_data_size), K(total_data_size));
  }
  return ret;
}

int ObLSBalanceGroupInfo::get_largest_part_group_from_array_(
    const common::ObArray<ObPartGroupInfo *> &selected_part_groups,
    ObPartGroupInfo *&largest_part_group,
    int64_t &index)
{
  int ret = OB_SUCCESS;
  largest_part_group = nullptr;
  index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(selected_part_groups.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("selected part groups is empty", KR(ret), K(selected_part_groups));
  } else {
    int64_t max_data_size = OB_INVALID_SIZE; // -1
    ARRAY_FOREACH(selected_part_groups, i) {
      if (OB_NOT_NULL(selected_part_groups.at(i))) {
        if (selected_part_groups.at(i)->get_data_size() > max_data_size) {
          max_data_size = selected_part_groups.at(i)->get_data_size();
          largest_part_group = selected_part_groups.at(i);
          index = i;
        }
      }
    }
  }
  return ret;
}

}
}
