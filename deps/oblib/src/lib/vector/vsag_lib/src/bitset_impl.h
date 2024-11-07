
// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <roaring.hh>
#include <vector>

#include "vsag/bitset.h"

namespace vsag {

class BitsetImpl : public Bitset {
public:
    BitsetImpl() = default;
    virtual ~BitsetImpl() = default;

    BitsetImpl(const BitsetImpl&) = delete;
    BitsetImpl&
    operator=(const BitsetImpl&) = delete;
    BitsetImpl(BitsetImpl&&) = delete;

public:
    virtual void
    Set(int64_t pos, bool value) override;

    virtual bool
    Test(int64_t pos) override;

    virtual uint64_t
    Count() override;

    virtual std::string
    Dump() override;

private:
    std::mutex mutex_;
    roaring::Roaring r_;
};

}  //namespace vsag
