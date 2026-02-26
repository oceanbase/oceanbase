//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_STORAGE_I_PRE_WARMER_H_
#define OB_SHARE_STORAGE_I_PRE_WARMER_H_
#include "/usr/include/stdint.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_param.h"
namespace oceanbase
{
namespace storage
{
class ObITableReadInfo;
}
namespace blocksstable
{
struct ObMicroBlockDesc;
}
namespace common
{
class ObTabletID;
}
namespace share
{
class ObLSID;
class ObIPreWarmer
{
public:
  ObIPreWarmer() : is_inited_(false) {}
  virtual ~ObIPreWarmer() {}
  virtual int init(const storage::ObITableReadInfo *table_read_info) = 0;
  virtual void reuse() = 0;
  virtual int reserve(const blocksstable::ObMicroBlockDesc &micro_block_desc,
                      bool &reserve_succ_flag,
                      const int64_t level = 0) = 0;
  virtual int add(const blocksstable::ObMicroBlockDesc &micro_block_desc, const bool reserve_succ_flag) = 0;
  virtual int close() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;

protected:
  bool is_inited_;
};

enum ObPreWarmerType : uint8_t
{
  PRE_WARM_TYPE_NONE = 0,
  MEM_PRE_WARM, // ObDataBlockCachePreWarmer
  MEM_AND_FILE_PRE_WARM, // for SS
  PRE_WARM_TYPE_MAX
};

struct ObPreWarmerParam
{
public:
  ObPreWarmerParam()
    : type_(PRE_WARM_TYPE_MAX), fixed_percentage_(0)
  {}
  ObPreWarmerParam(const ObPreWarmerType type)
    : type_(type)
  {}
  virtual ~ObPreWarmerParam() { reset(); }
  void reset() { type_ = PRE_WARM_TYPE_MAX; }
  virtual bool is_valid() const { return type_ >= PRE_WARM_TYPE_NONE && type_ < PRE_WARM_TYPE_MAX; }
  virtual int init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id, const bool use_fixed_percentage = false);
  VIRTUAL_TO_STRING_KV(K_(type), K_(fixed_percentage));
  ObPreWarmerType type_;
  int64_t fixed_percentage_;
};

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_I_PRE_WARMER_H_
