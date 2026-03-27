/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
