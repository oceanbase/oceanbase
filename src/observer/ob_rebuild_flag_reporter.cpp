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

#define USING_LOG_PREFIX SERVER

#include "ob_rebuild_flag_reporter.h"
#include "lib/hash_func/murmur_hash.h"
#include "observer/ob_server_struct.h"
#include "share/partition_table/ob_partition_table_operator.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace observer {

ObRebuildFlagReporter::ObRebuildFlagReporter()
    : inited_(false), part_key_(), server_(), rebuild_flag_(OB_REBUILD_INVALID)
{}

ObRebuildFlagReporter::~ObRebuildFlagReporter()
{}

int ObRebuildFlagReporter::init(
    const common::ObPartitionKey& part_key, const common::ObAddr& server, const storage::ObRebuildSwitch& rebuild_flag)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!part_key.is_valid() || !server.is_valid() || OB_REBUILD_INVALID == rebuild_flag) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_key), K(server), K(rebuild_flag));
  } else {
    part_key_ = part_key;
    server_ = server;
    rebuild_flag_ = rebuild_flag;
    inited_ = true;
  }
  return ret;
}

bool ObRebuildFlagReporter::is_valid() const
{
  return is_inited() && part_key_.is_valid() && server_.is_valid() && OB_REBUILD_INVALID != rebuild_flag_;
}

int64_t ObRebuildFlagReporter::hash() const
{
  int64_t val = 0;
  if (!is_valid()) {
    LOG_WARN("invalid argument", "self", *this);
  } else {
    val = part_key_.hash();
    murmurhash(&server_, sizeof(server_), val);
    murmurhash(&rebuild_flag_, sizeof(rebuild_flag_), val);
  }
  return val;
}

bool ObRebuildFlagReporter::operator==(const ObRebuildFlagReporter& o) const
{
  bool equal = false;
  if (!is_valid() || !o.is_valid()) {
    LOG_WARN("invalid argument", "self", *this, "other", o);
  } else {
    if (&o == this) {
      equal = true;
    } else {
      equal = (part_key_ == o.part_key_ && server_ == o.server_ && rebuild_flag_ == o.rebuild_flag_);
    }
  }
  return equal;
}

int ObRebuildFlagReporter::process(const volatile bool& stop)
{
  int ret = OB_EAGAIN;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t start_time = ObTimeUtility::current_time();

    if (OB_FAIL(do_process())) {
      LOG_WARN("do  failed", K(ret), "escape time", ObTimeUtility::current_time() - start_time, "task", *this, K(stop));
    }
  }

  return ret;
}

int ObRebuildFlagReporter::do_process()
{
  int ret = OB_SUCCESS;
  bool rebuild = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table operator", K(ret));
  } else {
    switch (rebuild_flag_) {
      case storage::OB_REBUILD_ON:
        rebuild = true;
        break;
      case storage::OB_REBUILD_OFF:
        rebuild = false;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown rebuild switch", K_(part_key), K_(server), K_(rebuild_flag), K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(GCTX.pt_operator_->update_rebuild_flag(
              part_key_.get_table_id(), part_key_.get_partition_id(), server_, rebuild))) {
        LOG_WARN("update partition table rebuild flag failed", K(ret), K_(part_key), K_(server), K(rebuild));
      }
    }
  }
  LOG_INFO("process rebuild flag report task", K(ret), "task", *this);

  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
