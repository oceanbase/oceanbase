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
 * Log LSN Filter
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_lsn_filter.h"       // ObLogLsnFilter
#include "lib/string/ob_string.h"    // ObString
#include "ob_log_utils.h"            // split_int64
#include "lib/oblog/ob_log_module.h" // LOG

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogLsnFilter::ObLogLsnFilter() :
    inited_(false),
    empty_(true),
    lsn_black_list_()
{}

ObLogLsnFilter::~ObLogLsnFilter()
{
    destroy();
}

int ObLogLsnFilter::init(const char *lsn_black_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_lsn_black_list_(lsn_black_list))) {
    LOG_ERROR("fail to parse lsn black list");
  } else {
    inited_ = true;
  }
  return ret;
}

void ObLogLsnFilter::destroy()
{
  lsn_black_list_.reset();
}

bool ObLogLsnFilter::filter(int64_t tenant_id, int64_t ls_id, int64_t lsn)
{
  bool need_filter = false;
  if (!empty_) {
    for (int64_t i = 0; i < lsn_black_list_.count(); i++) {
      const TLSN &tlsn = lsn_black_list_[i];
      if (tlsn.get_tenant_id() == tenant_id && tlsn.get_ls_id() == ls_id && tlsn.get_lsn() == lsn) {
        LOG_INFO("find lsn in lsn black list, need filter", K(tenant_id), K(ls_id), K(lsn));
        need_filter = true;
        break;
      }
    }
  }
  return need_filter;
}

int ObLogLsnFilter::parse_lsn_black_list_(const char *lsn_black_list)
{
  int ret = OB_SUCCESS;
  const char *lsn_delimiter = "|";
  const char *param_delimiter = ".";
  const int64_t expected_param_delimiter_num = 3; // expect parm: tenant_id, ls_id, lsn
  char *lsn_ptr = NULL;
  char *p = NULL;
  const int64_t str_len = strlen(lsn_black_list);
  char log_lsn_black_list_copy[str_len + 1];
  if (OB_ISNULL(lsn_black_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid lsn_black_list", KR(ret), K(str_len));
  } else {
    MEMSET(log_lsn_black_list_copy, '\0', sizeof(char) * (str_len + 1));
    MEMCPY(log_lsn_black_list_copy, lsn_black_list, str_len);
    lsn_ptr = strtok_r(log_lsn_black_list_copy, lsn_delimiter, &p);
    while (OB_SUCC(ret) && lsn_ptr != NULL) {
      int64_t lsn_param_cnt = 0;
      const char *lsn_param_res[3];
      if (OB_ISNULL(lsn_ptr) || (0 == strlen(lsn_ptr))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lsn ptr", KR(ret), K(lsn_black_list));
      } else if (OB_FAIL(split(lsn_ptr, param_delimiter, expected_param_delimiter_num, lsn_param_res, lsn_param_cnt))) {
        LOG_WARN("fail to split lsn_info_str", KR(ret), K(lsn_ptr));
      } else if (expected_param_delimiter_num != lsn_param_cnt) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lsn param count", KR(ret), K(lsn_ptr), K(lsn_black_list), K(expected_param_delimiter_num));
      } else {
        uint64_t tenant_id = OB_INVALID_TENANT_ID;
        int64_t ls_id = share::ObLSID::INVALID_LS_ID;
        uint64_t lsn = palf::LOG_INVALID_LSN_VAL;
        if (OB_ISNULL(lsn_param_res[0]) || OB_ISNULL(lsn_param_res[1]) || OB_ISNULL(lsn_param_res[2])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid lsn param", KR(ret), K(lsn_black_list));
        } else if (OB_FAIL(c_str_to_uint64(lsn_param_res[0], tenant_id))) {
          LOG_ERROR("fail to convert tenant_id_str to tenant_id", KR(ret), K(lsn_param_res[0]), K(tenant_id));
        } else if (OB_FAIL(c_str_to_int(lsn_param_res[1], ls_id))) {
          LOG_ERROR("fail to convert ls_id_str to ls_id", KR(ret), K(lsn_param_res[1]), K(ls_id));
        } else if (OB_FAIL(c_str_to_uint64(lsn_param_res[2], lsn))) {
          LOG_ERROR("fail to convert lsn_str to lsn", KR(ret), K(lsn_param_res[2]), K(lsn));
        }
        const TLSN tlsn(tenant_id, ls_id, lsn);
        if (tlsn.is_valid()) {
          lsn_black_list_.push_back(tlsn);
        } else {
          ret = OB_INVALID_DATA;
          LOG_ERROR("invalid tlsn", KR(ret), K(tlsn));
        }
      }
      lsn_ptr = strtok_r(NULL, lsn_delimiter, &p);
    }
  }
  empty_ = (0 == lsn_black_list_.count());
  return ret;
}

}
}
