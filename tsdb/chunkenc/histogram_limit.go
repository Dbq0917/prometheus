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
	"errors"
	"sync/atomic"
)

const (
	// DefaultMaxHistogramChunkBuckets is the default upper bound on the
	// number of buckets a single histogram chunk may declare. Legitimate
	// native histograms rarely exceed a few hundred buckets, while each
	// declared bucket makes an iterator allocate slices of int64, float64
	// and uint8 values, so this bound keeps the memory a single chunk can
	// force an iterator to allocate in the low hundreds of KiB.
	DefaultMaxHistogramChunkBuckets = 4096

	// AbsoluteMaxHistogramChunkBuckets is the hard upper bound for
	// SetMaxHistogramChunkBuckets. Even with this many buckets, a single
	// chunk cannot make an iterator allocate more than a few tens of MiB.
	// Configuring a higher limit is rejected to avoid re-exposing the
	// process to excessive per-chunk allocations.
	AbsoluteMaxHistogramChunkBuckets = 1 << 20
)

// ErrHistogramChunkTooManyBuckets is returned (wrapped) when a histogram
// chunk declares more buckets than is plausible for its encoded size or
// allowed by the configured limit. Such a chunk is corrupt or was crafted
// to force excessive memory allocations and is rejected before any memory
// is allocated based on the declared count.
var ErrHistogramChunkTooManyBuckets = errors.New("histogram chunk declares too many buckets")

// maxHistogramChunkBuckets holds the configured limit. It is a package
// level variable because iterators are created deep inside read paths that
// do not carry configuration. It is updated via SetMaxHistogramChunkBuckets,
// which is safe to call at runtime, e.g. on configuration reloads.
var maxHistogramChunkBuckets atomic.Int64

func init() {
	maxHistogramChunkBuckets.Store(DefaultMaxHistogramChunkBuckets)
}

// SetMaxHistogramChunkBuckets sets the maximum number of buckets a single
// histogram chunk may declare before it is rejected during iteration.
// Values <= 0 restore the default. Values above
// AbsoluteMaxHistogramChunkBuckets are clamped to it.
func SetMaxHistogramChunkBuckets(n int) {
	if n <= 0 {
		n = DefaultMaxHistogramChunkBuckets
	}
	if n > AbsoluteMaxHistogramChunkBuckets {
		n = AbsoluteMaxHistogramChunkBuckets
	}
	maxHistogramChunkBuckets.Store(int64(n))
}

// MaxHistogramChunkBuckets returns the currently configured limit.
func MaxHistogramChunkBuckets() int {
	return int(maxHistogramChunkBuckets.Load())
}
