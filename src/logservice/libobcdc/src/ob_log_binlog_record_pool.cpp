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
 * Binlog Record Pool
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_binlog_record_pool.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObLogBRPool::ObLogBRPool() : inited_(false), unserilized_pool_()
{
}

ObLogBRPool::~ObLogBRPool()
{
  destroy();
}

int ObLogBRPool::init(const int64_t fixed_br_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("BRPool has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(fixed_br_count <= 0)) {
    LOG_ERROR("invalid argument", K(fixed_br_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(unserilized_pool_.init(fixed_br_count, ObModIds::OB_LOG_BINLOG_RECORD_POOL))) {
    LOG_ERROR("initialize binlog record pool fail", KR(ret), K(fixed_br_count));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObLogBRPool::destroy()
{
  inited_ = false;
  unserilized_pool_.destroy();
}

int ObLogBRPool::alloc(ObLogBR *&br, void *host/* = NULL */, void *stmt_task/* nullptr */)
{
  int ret = OB_SUCCESS;
  ObLogUnserilizedBR *unserilized_br = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("BRPool has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(unserilized_pool_.alloc(unserilized_br))) {
    LOG_ERROR("alloc binlog record fail", KR(ret));
  } else {
    br = unserilized_br;
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(br)) {
      LOG_ERROR("alloc binlog record fail", K(br));
      ret = OB_ERR_UNEXPECTED;
    } else {
      br->set_host(host);
      br->set_stmt_task(stmt_task);
    }
  }

  return ret;
}

void ObLogBRPool::free(ObLogBR *br)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(inited_) && OB_LIKELY(NULL != br)) {
    // recycle memory
    br->reset();

    ObLogUnserilizedBR *unserilized_br = NULL;

    if (OB_ISNULL(unserilized_br = static_cast<ObLogUnserilizedBR *>(br))) {
      LOG_ERROR("unserilized_br is NULL");
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(unserilized_pool_.free(unserilized_br))) {
      LOG_ERROR("free binlog record fail", KR(ret), K(br));
    } else {
      br = NULL;
    }
  }
}

void ObLogBRPool::print_stat_info() const
{
  _LOG_INFO("[STAT] [BR_POOL] [UNSER](TOTAL=%ld FREE=%ld FIXED=%ld)",
      unserilized_pool_.get_alloc_count(), unserilized_pool_.get_free_count(), unserilized_pool_.get_fixed_count());
}

} // namespace libobcdc
} // namespace oceanbase
