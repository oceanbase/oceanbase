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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_ID_
#define OCEANBASE_TRANSACTION_OB_TRANS_ID_

#include <cstdint>

namespace oceanbase
{
namespace transaction
{
class ObTransID
{
  OB_UNIS_VERSION(1);
public:
  ObTransID() : tx_id_(0) {}
  ObTransID(const int64_t tx_id) : tx_id_(tx_id) {}
  ~ObTransID() { tx_id_ = 0; }
  ObTransID &operator=(const ObTransID &r) {
    if (this != &r) {
      tx_id_ = r.tx_id_;
    }
    return *this;
  }
  ObTransID &operator=(const int64_t &id) {
    tx_id_ = id;
    return *this;
  }
  bool operator<(const ObTransID &id) {
    bool bool_ret = false;
    if (this->compare(id) < 0) {
      bool_ret = true;
    }
    return bool_ret;
  }
  bool operator>(const ObTransID &id) {
    bool bool_ret = false;
    if (this->compare(id) > 0) {
      bool_ret = true;
    }
    return bool_ret;
  }
  int64_t get_id() const { return tx_id_; }
  uint64_t hash() const
  {
    return murmurhash(&tx_id_, sizeof(tx_id_), 0);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool is_valid() const { return tx_id_ > 0; }
  void reset() { tx_id_ = 0; }
  int compare(const ObTransID& other) const;
  operator int64_t() const { return tx_id_; }
  bool operator==(const ObTransID &other) const
  { return tx_id_ == other.tx_id_; }
  bool operator!=(const ObTransID &other) const
  { return tx_id_ != other.tx_id_; }
  /*  XA  */
  int parse(char *b) {
    UNUSED(b);
    return OB_SUCCESS;
  }
  TO_STRING_AND_YSON(OB_ID(txid), tx_id_);
private:
  int64_t tx_id_;
};

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_ID_
