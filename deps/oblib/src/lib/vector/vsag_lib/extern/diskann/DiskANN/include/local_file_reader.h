
#pragma once

#ifndef _WINDOWS

#include <malloc.h>
#include <cstdio>
#include <mutex>
#include <thread>
#include "tsl/robin_map.h"
#include "utils.h"
#include <functional>

namespace vsag {
enum class IOErrorCode;
};

typedef std::function<void(vsag::IOErrorCode code, const std::string& message)> CallBack;
typedef std::vector<std::tuple<uint64_t, uint64_t, void*>> batch_request;
typedef std::function<void(batch_request, bool, CallBack)> reader_function;

struct AlignedRead
{
    uint64_t offset; // where to read from
    uint64_t len;    // how much to read
    void *buf;       // where to read into
    AlignedRead() : offset(0), len(0), buf(nullptr)
    {
    }
    AlignedRead(uint64_t offset, uint64_t len, void *buf) : offset(offset), len(len), buf(buf)
    {
    }
};

class LocalFileReader
{
private:
    reader_function func_;
public:
    LocalFileReader(reader_function func): func_(func) {}
    ~LocalFileReader() = default;

    // de-register thread-id for a context
    void deregister_thread() {}
    void deregister_all_threads() {}

    // Open & close ops
    // Blocking calls
    void open(const std::string &fname) {}
    void close() {}

    // process batch of aligned requests in parallel
    // NOTE :: blocking call
    void read(std::vector<AlignedRead> &read_reqs, bool async = false, CallBack callBack = nullptr);
};



#endif
