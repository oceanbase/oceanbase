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
#include "lib/thread/ob_thread_name.h"
#include "ob_archive_allocator.h"          // ObArchiveAllocator
#include "ob_archive_clog_split_engine.h"  // ObArCLogSplitEngine
#include "ob_archive_sender.h"             // ObArchiveSender

namespace oceanbase {
namespace archive {
ObArchiveScheduler::ObArchiveScheduler()
    : inited_(false), stop_flag_(false), archive_allocator_(NULL), clog_split_eg_(NULL), archive_sender_(NULL)
{}

ObArchiveScheduler::~ObArchiveScheduler()
{
  destroy();

  ARCHIVE_LOG(INFO, "ObArchiveScheduler destroy");
}

void ObArchiveScheduler::destroy()
{
  inited_ = false;
  stop_flag_ = true;
  archive_allocator_ = NULL;
  clog_split_eg_ = NULL;
  archive_sender_ = NULL;
}

int ObArchiveScheduler::init(ObArchiveAllocator* allocator, ObArCLogSplitEngine* clog_split_eg, ObArchiveSender* sender)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator) || OB_ISNULL(clog_split_eg) || OB_ISNULL(sender)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(ret), K(allocator), K(clog_split_eg), K(sender));
  } else {
    archive_allocator_ = allocator;
    clog_split_eg_ = clog_split_eg;
    archive_sender_ = sender;
    stop_flag_ = true;
    inited_ = true;

    ARCHIVE_LOG(INFO, "ObArchiveScheduler init succ");
  }

  return ret;
}

int ObArchiveScheduler::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(ERROR, "ObArchiveScheduler not init", KR(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    ARCHIVE_LOG(WARN, "ObArchiveScheduler start fail", KR(ret));
  } else {
    stop_flag_ = false;
    ARCHIVE_LOG(INFO, "ObArchiveScheduler start succ");
  }

  return ret;
}

void ObArchiveScheduler::stop()
{
  ARCHIVE_LOG(INFO, "ObArchiveScheduler stop begin");

  stop_flag_ = true;

  ObThreadPool::stop();

  ARCHIVE_LOG(INFO, "ObArchiveScheduler stop end");
}

void ObArchiveScheduler::wait()
{
  ARCHIVE_LOG(INFO, "ObArchiveScheduler wait begin");

  ObThreadPool::wait();

  ARCHIVE_LOG(INFO, "ObArchiveScheduler wait end");
}

void ObArchiveScheduler::run1()
{
  ARCHIVE_LOG(INFO, "ObArchiveScheduler thread run");

  lib::set_thread_name("ObArchiveScheduler");

  if (OB_UNLIKELY(!inited_)) {
    ARCHIVE_LOG(ERROR, "ObArchiveScheduler not init");
  } else {
    while (!has_set_stop() && !stop_flag_) {
      do_thread_task_();
      usleep(1000 * 1000L);
    }
  }
}

void ObArchiveScheduler::do_thread_task_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "ObArchiveScheduler not init", KR(ret));
  } else {
    if (OB_FAIL(archive_sender_->modify_thread_count())) {
      ARCHIVE_LOG(WARN, "modify_sender_thread_count fail", KR(ret));
    } else if (OB_FAIL(clog_split_eg_->modify_thread_count())) {
      ARCHIVE_LOG(WARN, "modify clog_split_eg thread_count fail", KR(ret));
    } else if (OB_FAIL(archive_allocator_->set_archive_batch_buffer_limit())) {
      ARCHIVE_LOG(WARN, "set_archive_batch_buffer_limit fail", KR(ret));
    }
  }
}
}  // namespace archive
}  // namespace oceanbase
