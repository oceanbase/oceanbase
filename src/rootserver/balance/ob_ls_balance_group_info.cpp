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

#include "lib/ob_define.h"              // OB_FLOAT_EPSINON
#include "ob_ls_balance_group_info.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace rootserver
{

int ObLSBalanceGroupInfo::init(const ObLSID &ls_id, const int64_t balanced_ls_num,
                              const ObPartDistributionMode &part_distribution_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_), K(ls_id), KPC(this));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || balanced_ls_num <= 0
                        || part_distribution_mode <= ObPartDistributionMode::INVALID
                        || part_distribution_mode >= ObPartDistributionMode::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(balanced_ls_num));
  } else if (OB_FAIL(bg_map_.create(MAP_BUCKET_NUM, "LSBGMap"))) {
    LOG_WARN("create map for balance group fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else if (OB_FAIL(orig_part_group_cnt_map_.create(MAP_BUCKET_NUM, "LSPGCntMap"))) {
    LOG_WARN("create map for balance group original partition group cnt fail", KR(ret), LITERAL_K(MAP_BUCKET_NUM));
  } else {
    ls_id_ = ls_id;
    balanced_ls_num_ = balanced_ls_num;
    part_distribution_mode_ = part_distribution_mode;
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
  part_distribution_mode_ = ObPartDistributionMode::INVALID;
  inited_ = false;
}

int ObLSBalanceGroupInfo::append_part_into_balance_group(
    const ObBalanceGroupID &bg_id,
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    share::ObTransferPartInfo &part,
    const int64_t data_size)
{
  int ret = OB_SUCCESS;
  ObBalanceGroupInfo *bg = NULL;
  int64_t part_group_cnt = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || !is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(part), K(data_size), K(part_group_uid));
  } else if (OB_FAIL(get_or_create_(bg_id, bg))) {
    LOG_WARN("get or create balance group fail", KR(ret), K(bg_id));
  } else if (OB_ISNULL(bg)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("balance group is invalid", KR(ret), KPC(bg), K(bg_id));
  } else if (OB_FAIL(bg->append_part(table_schema, part_group_uid, part, data_size))) {
    LOG_WARN("append part info balance group fail", KR(ret), K(part), K(data_size),
            K(part_group_uid), K(table_schema));
  } else if (FALSE_IT(part_group_cnt = bg->get_part_group_count())) {
  } else if (OB_FAIL(orig_part_group_cnt_map_.set_refactored(bg_id, part_group_cnt, 1/*overwrite*/))) {
    LOG_WARN("overwrite partition group count map fail", KR(ret), K(bg_id), K(part_group_cnt));
  }

  return ret;
}

int ObLSBalanceGroupInfo::get_or_create_(const ObBalanceGroupID &bg_id,
                                         ObBalanceGroupInfo *&bg)
{
  int ret = OB_SUCCESS;
  int64_t bg_size = sizeof(ObBalanceGroupInfo);
  void *buf = NULL;
  bg = NULL;
  if (OB_FAIL(bg_map_.get_refactored(bg_id, bg))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(buf = alloc_.alloc(bg_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for ObBalanceGroupInfo fail", KR(ret), K(bg_size), K(buf));
      } else if (OB_ISNULL(bg = new(buf) ObBalanceGroupInfo(alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("construct ObBalanceGroupInfo fail", KR(ret), K(bg_size), K(buf), K(bg_id));
      } else if (OB_FAIL(bg->init(bg_id, ls_id_, balanced_ls_num_, part_distribution_mode_))) {
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

int ObLSBalanceGroupInfo::transfer_out_by_factor(ObLSBalanceGroupInfo &dst_ls_bg_info,
                                                const float factor,
                                                share::ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *pg_info = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_ls_bg_info.is_valid() || factor <= OB_FLOAT_EPSINON)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst_ls_bg_info or factor is invalid", KR(ret));
  } else {
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      const ObBalanceGroupID &bg_id = iter->first;
      ObBalanceGroupInfo *src_bg_info = iter->second;
      ObBalanceGroupInfo *dst_bg_info = NULL;

      // original count before transfer-out partitions
      const int64_t *total_count_ptr = orig_part_group_cnt_map_.get(bg_id);

      if (OB_ISNULL(src_bg_info) || OB_ISNULL(total_count_ptr)
          || OB_UNLIKELY(0 == *total_count_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("original part group count or balance group info is invalid, unexpected", KR(ret),
            K(total_count_ptr), K(bg_id), KPC(src_bg_info));
      } else if (OB_UNLIKELY(src_bg_info->get_part_group_count() <= 0)) {
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
        const int64_t avail_count = src_bg_info->get_part_group_count();

        // left count after remove should be greater than remove count
        const int64_t left_count_lower_bound = expected_remove_count;
        const int64_t can_remove_count_upper_bound = avail_count - left_count_lower_bound;

        const int64_t remove_count = std::min(can_remove_count_upper_bound, expected_remove_count);

        int64_t removed_part_count = 0;

        if (OB_UNLIKELY(remove_count >= avail_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("remove count should be greater than avail count", K(avail_count), K(remove_count));
          // transfer out part only when remove count > 0
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < remove_count; i++) {
            if (OB_FAIL(src_bg_info->transfer_out(*dst_bg_info, pg_info))) {
              LOG_WARN("fail to transfer out part group", KR(ret), K(dst_bg_info));
            } else if (OB_UNLIKELY(nullptr == pg_info
                                  || !pg_info->is_valid()
                                  || nullptr == pg_info->part_group())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("part group info is invalid", KR(ret), KPC(pg_info));
            } else if (OB_FAIL(append(part_list, pg_info->part_group()->get_part_list()))) {
              LOG_WARN("fail to append part list", KR(ret), K(part_list), KPC(pg_info->part_group()));
            } else {
              removed_part_count += pg_info->part_group()->get_part_list().count();
            }
          }
        }
        FLOG_INFO("transfer out partition groups from LS one balance group by factor", KR(ret),
            K(factor), K_(ls_id), K(bg_id),
            "removed_part_group_count", remove_count,
            K(removed_part_count),
            K(total_count), K(avail_count), K(expected_remove_count));
      }
    }
  }
  return ret;
}

}
}
