/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 */

#include "ob_fast_delta.h"
#include "ob_sse_to_neon.h"

namespace oceanbase
{
namespace common
{

// write to output the successive differences of input (input[0]-starting_point, input[1]-input[2], ...)
// there are "length" values in input and output
// input and output must be distinct
void compute_deltas(const uint32_t * __restrict__ input, size_t length, uint32_t * __restrict__ output, uint32_t starting_point) {
    __m128i prev = _mm_set1_epi32(starting_point);
    size_t i = 0;
    for(; i  < length/4; i++) {
        __m128i curr =  _mm_lddqu_si128 (((( const __m128i*) input) + i) );
        __m128i delta = _mm_sub_epi32(curr,
                                     _mm_alignr_epi8(curr, prev, 12));
        _mm_storeu_si128((__m128i*)output + i,delta);
        prev = curr;
    }
    uint32_t lastprev = _mm_extract_epi32(prev,3);
    for(i = 4 * i; i < length; ++i) {
        uint32_t curr = input[i];
        output[i] = curr - lastprev;
        lastprev = curr;
    }
}

// write to buffer the successive differences of buffer (buffer[0]-starting_point, buffer[1]-buffer[2], ...)
// there are "length" values in buffer
void compute_deltas_inplace(uint32_t * buffer, size_t length, uint32_t starting_point) {
    __m128i prev = _mm_set1_epi32(starting_point);
    size_t i = 0;
    for(; i  < length/4; i++) {
        __m128i curr =  _mm_lddqu_si128 (( const __m128i*) buffer + i );
        __m128i delta = _mm_sub_epi32(curr,
                                      _mm_alignr_epi8(curr, prev, 12));
        _mm_storeu_si128((__m128i*)buffer + i,delta);
        prev = curr;
    }
    uint32_t lastprev = _mm_extract_epi32(prev,3);
    for(i = 4 * i; i < length; ++i) {
        uint32_t curr = buffer[i];
        buffer[i] = curr - lastprev;
        lastprev = curr;
    }
}

// write to output the successive differences of input (input[0]-starting_point, input[1]-input[2], ...)
// there are "length" values in input and output
// input and output must be distinct
void compute_prefix_sum(const uint32_t * __restrict__ input, size_t length, uint32_t * __restrict__ output, uint32_t starting_point) {
    __m128i prev = _mm_set1_epi32(starting_point);
    size_t i = 0;
    for(; i  < length/4; i++) {
        __m128i curr =  _mm_lddqu_si128 (( const __m128i*) input + i );
        const __m128i _tmp1 = _mm_add_epi32(_mm_slli_si128(curr, 8), curr);
        const __m128i _tmp2 = _mm_add_epi32(_mm_slli_si128(_tmp1, 4), _tmp1);
        prev = _mm_add_epi32(_tmp2, _mm_shuffle_epi32(prev, 0xff));
        _mm_storeu_si128((__m128i*)output + i,prev);
    }
    uint32_t lastprev = _mm_extract_epi32(prev,3);
    for(i = 4 * i; i < length; ++i) {
        lastprev = lastprev + input[i];
        output[i] = lastprev;
    }
}

// write to buffer the successive differences of buffer (buffer[0]-starting_point, buffer[1]-buffer[2], ...)
// there are "length" values in buffer
void compute_prefix_sum_inplace(uint32_t * buffer, size_t length, uint32_t starting_point) {
    __m128i prev = _mm_set1_epi32(starting_point);
    size_t i = 0;
    for(; i  < length/4; i++) {
        __m128i curr =  _mm_lddqu_si128 (( const __m128i*) buffer + i );
        const __m128i _tmp1 = _mm_add_epi32(_mm_slli_si128(curr, 8), curr);
        const __m128i _tmp2 = _mm_add_epi32(_mm_slli_si128(_tmp1, 4), _tmp1);
        prev = _mm_add_epi32(_tmp2, _mm_shuffle_epi32(prev, 0xff));
        _mm_storeu_si128((__m128i*)buffer + i,prev);
    }
    uint32_t lastprev = _mm_extract_epi32(prev,3);
    for(i = 4 * i ; i < length; ++i) {
        lastprev = lastprev + buffer[i];
        buffer[i] = lastprev;
    }
}

} // namespace oceanbase
} // namespace common
