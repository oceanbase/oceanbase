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

#include <sstream>
#include <string>

namespace vsag {

enum class ErrorType {
    // start with 1, 0 is reserved

    // [common errors]
    UNKNOWN_ERROR = 1,  // unknown error
    INTERNAL_ERROR,     // some internal errors occupied in algorithm
    INVALID_ARGUMENT,   // invalid argument

    // [behavior errors]
    BUILD_TWICE,                  // index has been build, cannot build again
    INDEX_NOT_EMPTY,              // index object is NOT empty so that should not deserialize on it
    UNSUPPORTED_INDEX,            // trying to create an unsupported index
    UNSUPPORTED_INDEX_OPERATION,  // the index does not support this function
    DIMENSION_NOT_EQUAL,          // the dimension of add/build/search request is NOT equal to index
    INDEX_EMPTY,                  // index is empty, cannot search or serialize

    // [runtime errors]
    NO_ENOUGH_MEMORY,  // failed to alloc memory
    READ_ERROR,        // cannot read from binary
    MISSING_FILE,      // some file missing in index diskann deserialization
    INVALID_BINARY,    // the content of binary is invalid
};

struct Error {
    Error(ErrorType t, const std::string& msg) : type(t), message(msg) {
    }

    ErrorType type;
    std::string message;
};

template <typename T>
void
_concate(std::stringstream& ss, const T& value) {
    ss << value;
}

template <typename T, typename... Args>
void
_concate(std::stringstream& ss, const T& value, const Args&... args) {
    ss << value;
    _concate(ss, args...);
}

#define LOG_ERROR_AND_RETURNS(t, ...) \
    std::stringstream ss;             \
    _concate(ss, __VA_ARGS__);        \
    logger::error(ss.str());          \
    return tl::unexpected(Error(t, ss.str()));

}  //namespace vsag
