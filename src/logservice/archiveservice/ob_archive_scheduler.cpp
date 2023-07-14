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

#include "ob_archive_scheduler.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "objit/common/ob_item_type.h"        // print
#include "observer/omt/ob_tenant_config_mgr.h"   // tenant_config
#include "ob_archive_sender.h"                // ObArchiveSender
#include "ob_archive_fetcher.h"               // ObArchiveFetcher
#include "ob_archive_allocator.h"             // ObArchiveAllocator
#include "observer/omt/ob_tenant.h"           // ObTenant
#include <cstdint>

namespace oceanbase
{
namespace archive
{
using namespace oceanbase::common;
ObArchiveScheduler::ObArchiveScheduler() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  fetcher_(nullptr),
  sender_(nullptr),
  allocator_(nullptr)
{}

ObArchiveScheduler::~ObArchiveScheduler()
{
  destroy();
}

int ObArchiveScheduler::init(const uint64_t tenant_id,
    ObArchiveFetcher *fetcher,
    ObArchiveSender *sender,
    ObArchiveAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchiveScheduler init twice", K(ret), K(inited_));
  } else if (OB_INVALID_TENANT_ID == tenant_id
      || nullptr == fetcher
      || nullptr == sender
      || nullptr == allocator) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(fetcher), K(sender), K(allocator));
  } else {
    tenant_id_ = tenant_id;
    fetcher_ = fetcher;
    sender_ = sender;
    allocator_ = allocator;
    inited_ = true;
    ARCHIVE_LOG(INFO, "archive scheduler init succ", K(tenant_id));
  }
  return ret;
}

void ObArchiveScheduler::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  fetcher_ = nullptr;
  sender_ = nullptr;
  allocator_ = nullptr;
}

int ObArchiveScheduler::schedule()
{
  int ret = OB_SUCCESS;
  if (! inited_) {
    ret = OB_NOT_INIT;
  } else {
    (void)modify_thread_count_();
    (void)purge_cached_buffer_();
  }
  return ret;
}

int ObArchiveScheduler::modify_thread_count_()
{
  int ret = OB_SUCCESS;
  int64_t archive_concurrency = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  const int64_t log_archive_concurrency =
    tenant_config.is_valid() ? tenant_config->log_archive_concurrency : 0L;
  omt::ObTenant *tenant = NULL;
  if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id_, tenant))) {
    ARCHIVE_LOG(WARN, "get tenant failed", K(tenant_id_));
  } else {
    // if parameter log_archive_concurrency is default zero, set archive_concurrency = max_cpu / 4
    // otherwise set archive_concurrency = log_archive_concurrency
    if (0 == log_archive_concurrency) {
      archive_concurrency = (int64_t)tenant->unit_max_cpu() / 4;
    } else {
      archive_concurrency = log_archive_concurrency;
    }
    const int64_t fetcher_currency = std::max(1L, archive_concurrency / 3);
    const int64_t sender_concurrency = std::max(1L, archive_concurrency - fetcher_currency);
    if (OB_FAIL(sender_->modify_thread_count(sender_concurrency))) {
      ARCHIVE_LOG(WARN, "modify sender thread failed", K(ret));
    } else if (OB_FAIL(fetcher_->modify_thread_count(fetcher_currency))) {
      ARCHIVE_LOG(WARN, "modify fetcher thread failed", K(ret));
    }
  }
  return ret;
}

int ObArchiveScheduler::purge_cached_buffer_()
{
  int ret = OB_SUCCESS;
  allocator_->weed_out_send_task();
  return ret;
}

} // namespace archive
} // namespace oceanbase
