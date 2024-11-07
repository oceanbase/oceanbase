
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

#define SAFE_CALL(stmt)                                                              \
    try {                                                                            \
        stmt;                                                                        \
    } catch (const std::exception& e) {                                              \
        LOG_ERROR_AND_RETURNS(ErrorType::UNKNOWN_ERROR, "unknownError: ", e.what()); \
    } catch (...) {                                                                  \
        LOG_ERROR_AND_RETURNS(ErrorType::UNKNOWN_ERROR, "unknown error");            \
    }

#define CHECK_ARGUMENT(expr, message)             \
    do {                                          \
        if (not(expr)) {                          \
            throw std::invalid_argument(message); \
        }                                         \
    } while (0);

#define ROW_ID_MASK 0xFFFFFFFFLL
