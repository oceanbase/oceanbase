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

#include <string>

namespace vsag {

class Allocator {
public:
    // Return the name of the allocator.
    virtual std::string
    Name() = 0;

    // Allocate a block of at least size.
    virtual void*
    Allocate(size_t size) = 0;

    // Deallocate previously allocated block.
    virtual void
    Deallocate(void* p) = 0;

    // Reallocate the previously allocated block with long size.
    virtual void*
    Reallocate(void* p, size_t size) = 0;

public:
    virtual ~Allocator() = default;
};

}  // namespace vsag
