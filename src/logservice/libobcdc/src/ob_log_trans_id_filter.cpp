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
 *
 * Trans ID Filter: filter specified (tenant_id, trans_id) transactions in CDC
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_trans_id_filter.h"
#include "ob_log_utils.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

const int64_t ObLogTransIDFilter::DEFAULT_TENANT_MAP_BUCKET;
const int64_t ObLogTransIDFilter::DEFAULT_TRANS_ID_SET_BUCKET;

ObLogTransIDFilter::ObLogTransIDFilter() :
    inited_(false),
    tenant_trans_map_()
{}

ObLogTransIDFilter::~ObLogTransIDFilter()
{
  destroy();
}

int ObLogTransIDFilter::init(const char *filter_trans_id_list)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(parse_filter_list_(filter_trans_id_list))) {
    LOG_ERROR("parse filter_trans_id_list fail", KR(ret), K(filter_trans_id_list));
  } else {
    inited_ = true;
  }

  return ret;
}

void ObLogTransIDFilter::destroy()
{
  if (inited_) {
    for (common::hash::ObHashMap<uint64_t, common::hash::ObHashSet<int64_t> *>::iterator it =
        tenant_trans_map_.begin(); it != tenant_trans_map_.end(); ++it) {
      common::hash::ObHashSet<int64_t> *trans_set = it->second;
      if (OB_NOT_NULL(trans_set)) {
        trans_set->destroy();
        trans_set->~ObHashSet();
        ob_cdc_free(trans_set);
        trans_set = NULL;
      }
    }
    tenant_trans_map_.destroy();
    inited_ = false;
  }
}

bool ObLogTransIDFilter::should_filter(uint64_t tenant_id, const transaction::ObTransID &tx_id)
{
  bool need_filter = false;

  if (inited_ && tenant_trans_map_.created()) {
    common::hash::ObHashSet<int64_t> *trans_set = NULL;
    if (OB_SUCCESS == tenant_trans_map_.get_refactored(tenant_id, trans_set) && OB_NOT_NULL(trans_set)) {
      if (OB_HASH_EXIST == trans_set->exist_refactored(tx_id.get_id())) {
        need_filter = true;
      }
    }
  }

  return need_filter;
}

int ObLogTransIDFilter::parse_filter_list_(const char *filter_trans_id_list)
{
  int ret = OB_SUCCESS;
  const char *tenant_delimiter = "|";
  const char *tenant_trans_delimiter = ":";
  const char *trans_delimiter = ",";

  if (OB_ISNULL(filter_trans_id_list) || 0 == strlen(filter_trans_id_list)) {
    // empty list, no filter
    if (OB_FAIL(tenant_trans_map_.create(DEFAULT_TENANT_MAP_BUCKET,
        ObModIds::OB_LOG_TEMP_MEMORY,
        ObModIds::OB_LOG_TEMP_MEMORY))) {
      LOG_ERROR("create tenant_trans_map_ fail", KR(ret));
    }
    return ret;
  }

  const int64_t str_len = strlen(filter_trans_id_list);
  char *buf = static_cast<char *>(ob_cdc_malloc(str_len + 1));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate buffer for filter_trans_id_list fail", KR(ret), K(str_len));
    return ret;
  }

  MEMSET(buf, 0, str_len + 1);
  MEMCPY(buf, filter_trans_id_list, str_len);

  if (OB_FAIL(tenant_trans_map_.create(DEFAULT_TENANT_MAP_BUCKET,
      ObModIds::OB_LOG_TEMP_MEMORY,
      ObModIds::OB_LOG_TEMP_MEMORY))) {
    LOG_ERROR("create tenant_trans_map_ fail", KR(ret));
  } else {
    char *tenant_seg = NULL;
    char *save_tenant = NULL;
    tenant_seg = strtok_r(buf, tenant_delimiter, &save_tenant);

    while (OB_SUCC(ret) && NULL != tenant_seg) {
      char *colon = strchr(tenant_seg, ':');
      if (OB_ISNULL(colon)) {
        // ignore ret
        LOG_WARN("invalid filter segment, missing ':'", K(tenant_seg), K(filter_trans_id_list));
        tenant_seg = strtok_r(NULL, tenant_delimiter, &save_tenant);
        continue;
      }

      *colon = '\0';
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      if (OB_FAIL(c_str_to_uint64(tenant_seg, tenant_id)) || OB_INVALID_TENANT_ID == tenant_id) {
        LOG_WARN("invalid tenant_id in filter", KR(ret), K(tenant_seg), K(filter_trans_id_list));
        ret = OB_SUCCESS;
        tenant_seg = strtok_r(NULL, tenant_delimiter, &save_tenant);
        continue;
      }

      common::ObSEArray<int64_t, 16> trans_ids;
      common::ObString trans_list_str(colon + 1);
      if (OB_FAIL(split_int64(trans_list_str, trans_delimiter[0], trans_ids))) {
        LOG_WARN("split trans_id list fail", KR(ret), K(trans_list_str), K(filter_trans_id_list));
        ret = OB_SUCCESS;
        tenant_seg = strtok_r(NULL, tenant_delimiter, &save_tenant);
        continue;
      }

      if (trans_ids.count() <= 0) {
        tenant_seg = strtok_r(NULL, tenant_delimiter, &save_tenant);
        continue;
      }

      void *set_buf = ob_cdc_malloc(sizeof(common::hash::ObHashSet<int64_t>));
      common::hash::ObHashSet<int64_t> *trans_set = NULL;
      if (OB_ISNULL(set_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate ObHashSet fail", KR(ret));
        break;
      } else {
        trans_set = new(set_buf) common::hash::ObHashSet<int64_t>();
      }

      const int64_t set_bucket = std::max(DEFAULT_TRANS_ID_SET_BUCKET, trans_ids.count());
      if (OB_FAIL(trans_set->create(set_bucket,
          ObModIds::OB_LOG_TEMP_MEMORY,
          ObModIds::OB_LOG_TEMP_MEMORY))) {
        LOG_ERROR("create trans_set fail", KR(ret), K(tenant_id));
        trans_set->~ObHashSet();
        ob_cdc_free(trans_set);
        trans_set = NULL;
        break;
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < trans_ids.count(); ++i) {
        int64_t tx_id_val = trans_ids.at(i);
        if (OB_FAIL(trans_set->set_refactored(tx_id_val))) {
          if (OB_HASH_EXIST != ret) {
            LOG_ERROR("set_refactored trans_id fail", KR(ret), K(tx_id_val), K(tenant_id));
          } else {
            ret = OB_SUCCESS;
          }
        }
      }

      if (OB_SUCC(ret)) {
        common::hash::ObHashSet<int64_t> *existing_set = NULL;
        if (OB_SUCCESS == tenant_trans_map_.get_refactored(tenant_id, existing_set)) {
          for (int64_t i = 0; OB_SUCC(ret) && i < trans_ids.count(); ++i) {
            int64_t tx_id_val = trans_ids.at(i);
            if (OB_FAIL(existing_set->set_refactored(tx_id_val)) && OB_HASH_EXIST != ret) {
              LOG_ERROR("set_refactored trans_id fail", KR(ret), K(tx_id_val), K(tenant_id));
            } else if (OB_HASH_EXIST == ret) {
              ret = OB_SUCCESS;
            }
          }
          trans_set->destroy();
          trans_set->~ObHashSet();
          ob_cdc_free(trans_set);
          trans_set = NULL;
        } else if (OB_FAIL(tenant_trans_map_.set_refactored(tenant_id, trans_set))) {
          LOG_ERROR("set_refactored tenant_trans_map_ fail", KR(ret), K(tenant_id));
        } else {
          trans_set = NULL;
        }
      }

      if (OB_NOT_NULL(trans_set)) {
        trans_set->destroy();
        trans_set->~ObHashSet();
        ob_cdc_free(trans_set);
        trans_set = NULL;
      }

      if (OB_FAIL(ret)) {
        break;
      }

      tenant_seg = strtok_r(NULL, tenant_delimiter, &save_tenant);
    }
  }

  if (NULL != buf) {
    ob_cdc_free(buf);
    buf = NULL;
  }

  if (OB_FAIL(ret) && tenant_trans_map_.created()) {
    for (common::hash::ObHashMap<uint64_t, common::hash::ObHashSet<int64_t> *>::iterator it =
        tenant_trans_map_.begin(); it != tenant_trans_map_.end(); ++it) {
      common::hash::ObHashSet<int64_t> *s = it->second;
      if (OB_NOT_NULL(s)) {
        s->destroy();
        s->~ObHashSet();
        ob_cdc_free(s);
      }
    }
    tenant_trans_map_.destroy();
  }

  return ret;
}

}
}
