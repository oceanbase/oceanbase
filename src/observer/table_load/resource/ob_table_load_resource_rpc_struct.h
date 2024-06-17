/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/container/ob_array_serialization.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/table/ob_table_load_define.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/table_load/ob_table_load_struct.h"

namespace oceanbase
{
namespace observer
{
enum class ObDirectLoadResourceCommandType
{
  APPLY = 0,
  RELEASE = 1,
  UPDATE = 2,
  CHECK = 3,
  MAX_TYPE
};

class ObDirectLoadResourceUnit
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceUnit()
    : thread_count_(0), memory_size_(0)
  {
  }
  ObDirectLoadResourceUnit(const ObDirectLoadResourceUnit &unit)
    : addr_(unit.addr_), thread_count_(unit.thread_count_), memory_size_(unit.memory_size_)
  {
  }
  TO_STRING_KV(K_(addr), K_(thread_count), K_(memory_size));
public:
  common::ObAddr addr_;
  int64_t thread_count_;
  int64_t memory_size_;
};

class ObDirectLoadResourceOpRequest
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceOpRequest()
    : command_type_(observer::ObDirectLoadResourceCommandType::MAX_TYPE)
  {
  }
  template <class Arg>
  int set_arg(const Arg &arg, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = arg.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory", KR(ret), K(size));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      SERVER_LOG(WARN, "fail to serialize arg", KR(ret), K(arg));
    } else {
      arg_content_.assign_ptr(buf, size);
    }

    return ret;
  }
  template <class Arg>
  int get_arg(Arg &arg) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(arg_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", KR(ret), KPC(this));
    } else if (OB_FAIL(arg.deserialize(arg_content_.ptr(), arg_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize arg content", KR(ret), KPC(this));
    }

    return ret;
  }
  TO_STRING_KV(K_(command_type), "arg_content", common::ObHexStringWrap(arg_content_));
public:
  observer::ObDirectLoadResourceCommandType command_type_;
  ObString arg_content_;
};

class ObDirectLoadResourceOpResult
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceOpResult()
    : allocator_(nullptr), command_type_(observer::ObDirectLoadResourceCommandType::MAX_TYPE)
  {
  }
  template <class Res>
  int set_res(const Res &res, common::ObIAllocator &allocator)
  {
    int ret = common::OB_SUCCESS;
    const int64_t size = res.get_serialize_size();
    if (size > 0) {
      char *buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to alloc memory", KR(ret), K(size));
      } else if (OB_FAIL(res.serialize(buf, size, pos))) {
        SERVER_LOG(WARN, "fail to serialize res", KR(ret), K(res));
      } else {
        res_content_.assign_ptr(buf, size);
      }
    }

    return ret;
  }
  template <class Res>
  int get_res(Res &res) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    if (OB_UNLIKELY(res_content_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid args", KR(ret), KPC(this));
    } else if (OB_FAIL(res.deserialize(res_content_.ptr(), res_content_.length(), pos))) {
      SERVER_LOG(WARN, "fail to deserialize res content", KR(ret), KPC(this));
    }

    return ret;
  }
  TO_STRING_KV(K_(command_type), "res_content", common::ObHexStringWrap(res_content_));
public:
  common::ObIAllocator *allocator_; // for deserialize
  observer::ObDirectLoadResourceCommandType command_type_;
  ObString res_content_;
};

//////////////////////////////////////////////////////////////////////

class ObDirectLoadResourceApplyArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceApplyArg()
    : tenant_id_(common::OB_INVALID_ID)
  {
  }
  ObDirectLoadResourceApplyArg(const ObDirectLoadResourceApplyArg& arg)
    : tenant_id_(arg.tenant_id_), task_key_(arg.task_key_), apply_array_(arg.apply_array_)
  {
  }
  bool is_valid()
  {
    bool valid = tenant_id_ != common::OB_INVALID_ID && task_key_.is_valid();
    for (int64_t i = 0; valid && i < apply_array_.count(); i++) {
      valid = (apply_array_[i].addr_.is_valid() && apply_array_[i].thread_count_ >= 0 && apply_array_[i].memory_size_ >= 0);
    }
    return valid;
  }
  TO_STRING_KV(K_(tenant_id), K_(task_key), K_(apply_array));
public:
  uint64_t tenant_id_;
	ObTableLoadUniqueKey task_key_;
	common::ObSArray<ObDirectLoadResourceUnit> apply_array_;
};

class ObDirectLoadResourceReleaseArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceReleaseArg()
    : tenant_id_(common::OB_INVALID_ID)
  {
  }
  ObDirectLoadResourceReleaseArg(const ObDirectLoadResourceReleaseArg &arg)
    : tenant_id_(arg.tenant_id_), task_key_(arg.task_key_)
  {
  }
  bool is_valid()
  {
    return tenant_id_ != common::OB_INVALID_ID && task_key_.is_valid();
  }
  TO_STRING_KV(K_(tenant_id), K_(task_key));
public:
  uint64_t tenant_id_;
	ObTableLoadUniqueKey task_key_;
};

class ObDirectLoadResourceUpdateArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceUpdateArg()
   : tenant_id_(common::OB_INVALID_ID), thread_count_(0), memory_size_(0)
  {
  }
  ObDirectLoadResourceUpdateArg(const ObDirectLoadResourceUpdateArg &arg)
    : tenant_id_(arg.tenant_id_),
      thread_count_(arg.thread_count_),
      memory_size_(arg.memory_size_),
      addrs_(arg.addrs_)
  {
  }
  bool is_valid()
  {
    return tenant_id_ != common::OB_INVALID_ID && thread_count_ >= 0 && memory_size_ >= 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(thread_count), K_(memory_size), K_(addrs));
public:
  uint64_t tenant_id_;
  int64_t thread_count_;
  int64_t memory_size_;
  common::ObSArray<ObAddr> addrs_;
};

class ObDirectLoadResourceCheckArg final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceCheckArg()
    : tenant_id_(common::OB_INVALID_ID), avail_memory_(0), first_check_(false)
  {
  }
  ObDirectLoadResourceCheckArg(const ObDirectLoadResourceCheckArg &arg)
    : tenant_id_(arg.tenant_id_),
      avail_memory_(arg.avail_memory_),
      first_check_(arg.first_check_)
  {
  }
  bool is_valid()
  {
    return tenant_id_ != common::OB_INVALID_ID;
  }
  TO_STRING_KV(K_(tenant_id), K_(avail_memory));
public:
  uint64_t tenant_id_;
  int64_t avail_memory_;
  bool first_check_;
};

class ObDirectLoadResourceOpRes final
{
  OB_UNIS_VERSION(1);

public:
  ObDirectLoadResourceOpRes()
    : error_code_(common::OB_SUCCESS),
      avail_memory_(0)
  {
  }
  ObDirectLoadResourceOpRes(const ObDirectLoadResourceOpRes &res)
    : error_code_(res.error_code_),
      avail_memory_(res.avail_memory_),
      assigned_array_(res.assigned_array_)
  {
  }
  TO_STRING_KV(K_(error_code), K_(avail_memory), K_(assigned_array));
public:
  int error_code_;
  int64_t avail_memory_;
  common::ObSArray<ObDirectLoadResourceApplyArg> assigned_array_;
};

} // namespace observer
} // namespace oceanbase
