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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_COMPENSATE_ENGINE_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_COMPENSATE_ENGINE_H_

#include "logservice/log_define.h"
#include "common/ob_queue_thread.h"        // ObCond

namespace oceanbase
{
namespace archive
{
/*
 * 为满足归档熟虑压缩率, 归档数据需要凑够一定块大小才会压缩并归档出去,
 * 对于冷的日志流, 可能较长时间无法写入足够多的数据进行压缩以及归档;
 *
 * 另外某些存储介质不支持修改(worm), 需要整个日志文件写满之后一次性归档出去,
 * 这同样可能需要很长时间才能写满一个日志文件(64M大小)
 *
 * 为兼顾压缩效率/worm以及归档时效性, 需要为日志文件周期性补占位日志, 来达到一定时间
 * 内可以凑够足够大小数据进行压缩归档以及一定时间内可以写满日志文件满足worm归档需求
 * */
class ObArchiveCompensateEngine : public share::ObThreadPool
{
//TODO
public:
  ObArchiveCompensateEngine();
  ~ObArchiveCompensateEngine();

public:
  int start();
  void stop();
  void wait();

private:
  void run1();
  void do_thread_task_();

private:
  bool inited_;
  bool need_force_check_;
  common::ObCond cond_;
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_COMPENSATE_ENGINE_H_ */
