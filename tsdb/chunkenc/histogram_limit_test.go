// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
)

// craftedHistogramChunkBytes returns the bytes of a histogram chunk with the
// given number of samples and the given layout, without any sample data.
func craftedHistogramChunkBytes(numSamples uint16, schema int32, posSpans, negSpans []histogram.Span, customValues []float64) []byte {
	b := make([]byte, histogramHeaderSize)
	binary.BigEndian.PutUint16(b, numSamples)
	var bs bstream
	writeHistogramChunkLayout(&bs, schema, 0.001, posSpans, negSpans, customValues)
	return append(b, bs.bytes()...)
}

// TestHistogramChunkRejectsCraftedBucketCounts checks that a chunk declaring
// more buckets than its encoded size allows is rejected before any memory is
// allocated based on the declared count.
func TestHistogramChunkRejectsCraftedBucketCounts(t *testing.T) {
	// A span length of 1<<31 would make the iterator allocate slices of
	// 1<<31 buckets, i.e. several GiB, from a handful of bytes.
	b := craftedHistogramChunkBytes(1, 1, []histogram.Span{{Offset: 0, Length: 1 << 31}}, nil, nil)

	t.Run("histogram", func(t *testing.T) {
		c, err := FromData(EncHistogram, b)
		require.NoError(t, err)
		it := c.Iterator(nil)
		require.Equal(t, ValNone, it.Next())
		require.ErrorIs(t, it.Err(), ErrHistogramChunkTooManyBuckets)
		require.ErrorContains(t, it.Err(), "bits remain")
		// No memory was allocated based on the declared bucket count.
		hit := it.(*histogramIterator)
		require.Empty(t, hit.pBuckets)
		require.Empty(t, hit.pBucketsDelta)
		require.Empty(t, hit.pFloatBuckets)
		require.Empty(t, hit.nBuckets)
		require.Empty(t, hit.nBucketsDelta)
		require.Empty(t, hit.nFloatBuckets)
	})

	t.Run("float histogram", func(t *testing.T) {
		c, err := FromData(EncFloatHistogram, b)
		require.NoError(t, err)
		it := c.Iterator(nil)
		require.Equal(t, ValNone, it.Next())
		require.ErrorIs(t, it.Err(), ErrHistogramChunkTooManyBuckets)
		require.ErrorContains(t, it.Err(), "bits remain")
		// No memory was allocated based on the declared bucket count.
		fit := it.(*floatHistogramIterator)
		require.Empty(t, fit.pBuckets)
		require.Empty(t, fit.pBucketsLeading)
		require.Empty(t, fit.pBucketsTrailing)
		require.Empty(t, fit.nBuckets)
		require.Empty(t, fit.nBucketsLeading)
		require.Empty(t, fit.nBucketsTrailing)
	})
}

// TestHistogramChunkRejectsBucketCountAboveLimit checks that a chunk
// declaring more buckets than the configured limit is rejected, even if the
// declared count is plausible for the chunk size.
func TestHistogramChunkRejectsBucketCountAboveLimit(t *testing.T) {
	defer SetMaxHistogramChunkBuckets(MaxHistogramChunkBuckets())

	// The chunk declares 100 buckets and contains enough zero bits to
	// actually decode them, so it is only rejected by the configured limit.
	b := craftedHistogramChunkBytes(1, 1, []histogram.Span{{Offset: 0, Length: 100}}, nil, nil)
	b = append(b, make([]byte, 32)...)

	SetMaxHistogramChunkBuckets(10)
	require.Equal(t, 10, MaxHistogramChunkBuckets())

	c, err := FromData(EncHistogram, b)
	require.NoError(t, err)
	it := c.Iterator(nil)
	require.Equal(t, ValNone, it.Next())
	require.ErrorIs(t, it.Err(), ErrHistogramChunkTooManyBuckets)
	require.ErrorContains(t, it.Err(), "exceeding the configured limit of 10")
	require.Empty(t, it.(*histogramIterator).pBuckets)

	// With the default limit the same chunk iterates normally.
	SetMaxHistogramChunkBuckets(0)
	require.Equal(t, DefaultMaxHistogramChunkBuckets, MaxHistogramChunkBuckets())
	it = c.Iterator(nil)
	require.Equal(t, ValHistogram, it.Next())
	require.NoError(t, it.Err())
	require.Equal(t, ValNone, it.Next())

	// The limit is clamped to the absolute maximum.
	SetMaxHistogramChunkBuckets(AbsoluteMaxHistogramChunkBuckets + 1)
	require.Equal(t, AbsoluteMaxHistogramChunkBuckets, MaxHistogramChunkBuckets())
}

// TestHistogramChunkRejectsCraftedSpanCount checks that a chunk declaring
// more spans than its encoded size allows is rejected before iterating them.
func TestHistogramChunkRejectsCraftedSpanCount(t *testing.T) {
	b := make([]byte, histogramHeaderSize)
	binary.BigEndian.PutUint16(b, 1)
	var bs bstream
	putZeroThreshold(&bs, 0.001)
	putVarbitInt(&bs, 1)      // Schema.
	putVarbitUint(&bs, 1<<40) // Absurd positive span count.
	b = append(b, bs.bytes()...)

	c, err := FromData(EncHistogram, b)
	require.NoError(t, err)
	it := c.Iterator(nil)
	require.Equal(t, ValNone, it.Next())
	require.ErrorContains(t, it.Err(), "spans")
}

// TestFloatHistogramChunkRejectsCraftedCustomBoundCount checks that a chunk
// declaring more custom bounds than its encoded size allows is rejected
// before iterating them.
func TestFloatHistogramChunkRejectsCraftedCustomBoundCount(t *testing.T) {
	b := make([]byte, histogramHeaderSize)
	binary.BigEndian.PutUint16(b, 1)
	var bs bstream
	putZeroThreshold(&bs, 0.001)
	putVarbitInt(&bs, int64(histogram.CustomBucketsSchema))
	putVarbitUint(&bs, 0)     // No positive spans.
	putVarbitUint(&bs, 0)     // No negative spans.
	putVarbitUint(&bs, 1<<40) // Absurd custom bound count.
	b = append(b, bs.bytes()...)

	c, err := FromData(EncFloatHistogram, b)
	require.NoError(t, err)
	it := c.Iterator(nil)
	require.Equal(t, ValNone, it.Next())
	require.ErrorContains(t, it.Err(), "custom bounds")
}

// TestHistogramChunkIterationUnaffectedByValidation checks that the
// validation does not change the iteration results of legitimate chunks.
func TestHistogramChunkIterationUnaffectedByValidation(t *testing.T) {
	defer SetMaxHistogramChunkBuckets(MaxHistogramChunkBuckets())

	hists := []*histogram.Histogram{
		{
			Schema:        1,
			Count:         10,
			Sum:           15.0,
			ZeroThreshold: 0.001,
			ZeroCount:     2,
			PositiveSpans: []histogram.Span{
				{Offset: 0, Length: 2},
				{Offset: 1, Length: 2},
			},
			PositiveBuckets: []int64{1, 2, 3, 4},
		},
		{
			Schema:        1,
			Count:         15,
			Sum:           20.0,
			ZeroThreshold: 0.001,
			ZeroCount:     3,
			PositiveSpans: []histogram.Span{
				{Offset: 0, Length: 2},
				{Offset: 1, Length: 2},
			},
			PositiveBuckets: []int64{2, 3, 4, 5},
		},
	}

	c := NewHistogramChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	for i, h := range hists {
		_, _, _, err = app.AppendHistogram(nil, 0, int64(i+1), h, false)
		require.NoError(t, err)
	}

	// The chunk declares 4 buckets. It iterates fine with a limit of 4.
	SetMaxHistogramChunkBuckets(4)
	it := c.Iterator(nil)
	for i, exp := range hists {
		require.Equal(t, ValHistogram, it.Next())
		ts, h := it.AtHistogram(nil)
		require.Equal(t, int64(i+1), ts)
		// The counter reset hint is derived by the appender, ignore it.
		expWithoutHint := *exp
		expWithoutHint.CounterResetHint = h.CounterResetHint
		require.Equal(t, &expWithoutHint, h)
	}
	require.Equal(t, ValNone, it.Next())
	require.NoError(t, it.Err())

	// But it is rejected with a limit of 3.
	SetMaxHistogramChunkBuckets(3)
	it = c.Iterator(nil)
	require.Equal(t, ValNone, it.Next())
	require.ErrorIs(t, it.Err(), ErrHistogramChunkTooManyBuckets)
}
