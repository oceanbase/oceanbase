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

class BytesLimiter: public ITCLimiter
{
public:
  BytesLimiter(int id, const char* name): ITCLimiter(id, TCLIMIT_BYTES, name, 0) {}
  BytesLimiter(int id, const char* name, uint64_t storage_key): ITCLimiter(id, TCLIMIT_BYTES, name, storage_key) {}
  ~BytesLimiter() {}
  int64_t get_cost(TCRequest* req) { return (req->norm_bytes_ == 0) ? req->bytes_ : req->norm_bytes_; }
};

class CountLimiter: public ITCLimiter
{
public:
  CountLimiter(int id, const char* name): ITCLimiter(id, TCLIMIT_COUNT, name) {}
  CountLimiter(int id, const char* name, uint64_t storage_key): ITCLimiter(id, TCLIMIT_COUNT, name, storage_key) {}
  ~CountLimiter() {}
  int64_t get_cost(TCRequest* req) { return 1; }
};
