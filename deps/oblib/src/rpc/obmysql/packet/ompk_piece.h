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

#ifndef OCEANBASE_RPC_OBMYSQL_OMPK_PIECE_H_
#define OCEANBASE_RPC_OBMYSQL_OMPK_PIECE_H_

#include "ompk_piece.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_row.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKPiece : public ObMySQLPacket
{
public:
  explicit OMPKPiece();
  OMPKPiece(int8_t piece_mode, bool is_null, int64_t data_length, common::ObString &data)
    : piece_mode_(piece_mode),
      is_null_(is_null ? 1 : 0),
      data_length_(data_length),
      is_array_(false),
      data_(data)
    {}
  // OMPKPiece(int8_t piece_mode, bool is_null, ObMySQLRow &row)
  //   : piece_mode_(piece_mode),
  //     is_null_(is_null ? 1 : 0),
  //     data_length_(0),
  //     is_array_(true),
  //     row_(row)
  //   {}
  virtual ~OMPKPiece() { }

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_PIECE; }

  inline void set_piece_mode(int8_t  piece_mode) { piece_mode_ = piece_mode; }
  inline void set_data(common::ObString &data) { data_ = data; }
private:
  DISALLOW_COPY_AND_ASSIGN(OMPKPiece);
  int8_t  piece_mode_;
  int8_t  is_null_;
  int64_t data_length_;
  bool    is_array_;
  common::ObString data_;
  //ObMySQLRow row_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBMYSQL_OMPK_PIECE_H_
