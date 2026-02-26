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

#ifndef OCEANBASE_LOGSERVICE_I_LOG_STORAGE_
#define OCEANBASE_LOGSERVICE_I_LOG_STORAGE_
#include <cstdint>
#include "lib/utility/ob_print_utils.h"                 // VIRTUAL_TO_STRING_KV
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif
namespace oceanbase
{
namespace palf
{
class ReadBuf;
class LSN;
class LogIOContext;
enum class ILogStorageType {
  MEMORY_STORAGE = 0,
  DISK_STORAGE = 1,
  HYBRID_STORAGE = 2
};

class ILogStorage
{
public:
  ILogStorage(const ILogStorageType type) : type_(type) {}
  virtual ~ILogStorage() {}
  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_UNEXPECTED, file maybe deleted by human.
  virtual int pread(const LSN &lsn,
                    const int64_t in_read_size,
                    ReadBuf &read_buf,
                    int64_t &out_read_size,
                    LogIOContext &io_ctx) = 0;

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  virtual const libpalf::LibPalfIteratorMemoryStorageFFI * get_memory_storage() = 0;
#endif

  ILogStorageType get_log_storage_type()
  { return type_; }
  const char *get_log_storage_type_str()
  {
    if (ILogStorageType::MEMORY_STORAGE == type_) {
      return "MEMORY_STORAGE";
    } else if (ILogStorageType::DISK_STORAGE == type_) {
      return "DISK_STORAGE";
    } else {
      return "HYBRID_STORAGE";
    }
  }
  VIRTUAL_TO_STRING_KV(K_(type));
private:
  ILogStorageType type_;
};
}
}
#endif
