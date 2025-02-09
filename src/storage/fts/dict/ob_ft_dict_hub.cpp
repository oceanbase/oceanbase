/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/rc/ob_tenant_base.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_dict.h"
#include "storage/fts/dict/ob_ft_dict.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
int ObFTDictHub::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.init())) {
    LOG_WARN("init dict map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  return ret;
}

int ObFTDictHub::push_dict_version(const uint64_t &name)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dict hub not init", K(ret));
  }
  return ret;
}

int ObFTDictHub::get_dict_info(const uint64_t &name, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dict hub not init", K(ret));
  } else if (OB_FAIL(dict_map_.get_refactored(name, info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get dict info failed", K(ret));
    }
  }
  return ret;
}

int ObFTDictHub::put_dict_info(const uint64_t &name, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  ObFTDictInfo tmp;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dict hub not init", K(ret));
  } else {
    // remove old if exist
    if (OB_FAIL(dict_map_.get_refactored(name, tmp))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get dict info failed", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(dict_map_.remove_refactored(name))) {
      LOG_WARN("remove dict info failed", K(ret));
    }

    // put new
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dict_map_.put_refactored(name, info))) {
      LOG_WARN("put dict info failed", K(ret));
    }
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
