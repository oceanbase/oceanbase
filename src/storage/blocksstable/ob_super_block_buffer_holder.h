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

#ifndef OB_SUPER_BLOCK_BUFFER_HOLDER_H_
#define OB_SUPER_BLOCK_BUFFER_HOLDER_H_

#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSuperBlockBufferHolder
{
public:
  ObSuperBlockBufferHolder();
  virtual ~ObSuperBlockBufferHolder();

  int init(const int64_t buf_size);
  void reset();

  int serialize_super_block(const storage::ObServerSuperBlock &super_block);
  template<typename SuperBlockClass>
  int deserialize_super_block(SuperBlockClass &super_block);
  int deserialize_super_block_header_version(int64_t &version);
  int assign(const char *buf, const int64_t buf_len);

  OB_INLINE char *get_buffer() { return buf_; }
  OB_INLINE int64_t get_len() { return len_; }

  TO_STRING_KV(KP_(buf), K_(len));

private:
  bool is_inited_;
  char *buf_;
  int64_t len_;
  common::ObArenaAllocator allocator_;
};

template<typename SuperBlockClass>
int ObSuperBlockBufferHolder::deserialize_super_block(SuperBlockClass &super_block)
{
  int ret = common::OB_SUCCESS;
  int64_t pos = 0;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(super_block.deserialize(buf_, len_, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize super block", K(ret));
  } else {
    STORAGE_LOG(INFO, "load superblock ok.", K(super_block));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

#endif /* OB_SUPER_BLOCK_BUFFER_HOLDER_H_ */
