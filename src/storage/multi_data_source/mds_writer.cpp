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

#include "mds_writer.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

MdsWriter::MdsWriter(const WriterType writer_type, const int64_t writer_id) : writer_type_(writer_type), writer_id_(writer_id) {}

MdsWriter::MdsWriter(const transaction::ObTransID &tx_id) : writer_type_(WriterType::TRANSACTION), writer_id_(tx_id.get_id()) {}

bool MdsWriter::is_valid() const
{
  return writer_type_ != WriterType::UNKNOWN_WRITER && writer_type_ < WriterType::END;
}

}
}
}