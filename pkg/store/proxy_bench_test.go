// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestProxySeries(t *testing.T) {
	tb := testutil.NewTB(t)
	tb.Run("200e3SeriesWithOneSample", func(tb testutil.TB) {
		benchProxySeries(tb, 200e3, seriesDimension, 200e3)
	})
	tb.Run("OneSeriesWith200e3Samples", func(tb testutil.TB) {
		benchProxySeries(tb, 200e3, samplesDimension, 200e3)
	})
}

func BenchmarkProxySeries(b *testing.B) {
	tb := testutil.NewTB(b)
	tb.Run("10e6SeriesWithOneSample", func(tb testutil.TB) {
		benchProxySeries(tb, 10e6, seriesDimension, 1, 10, 10e1, 10e2, 10e3, 10e4, 10e5)
	})
	tb.Run("OneSeriesWith100e6Samples", func(tb testutil.TB) {
		// 100e6 samples = ~17361 days with 15s scrape.
		benchProxySeries(tb, 100e6, samplesDimension, 1, 10, 10e1, 10e2, 10e3, 10e4, 10e5, 10e6)
	})
}

type nopAppender struct{}

func (nopAppender) Add(labels.Labels, int64, float64) (uint64, error) { return 0, nil }

func (nopAppender) AddFast(uint64, int64, float64) error { return nil }

func (nopAppender) Commit() error { return nil }

func (nopAppender) Rollback() error { return nil }

func benchProxySeries(t testutil.TB, number int, dimension Dimension, cases ...int) {
	const numOfClients = 4

	var (
		numberPerClient = number / 4
		series          []storepb.Series
		lbls            = labels.FromStrings("ext1", "1", "foo", "bar", "i", postingsBenchSuffix)
		random          = rand.New(rand.NewSource(120))
	)
	switch dimension {
	case seriesDimension:
		series = make([]storepb.Series, 0, numOfClients*numberPerClient)
	case samplesDimension:
		series = []storepb.Series{newSeries(t, lbls, nil)}
	default:
		t.Fatal("unknown dimension", dimension)
	}

	var lset storepb.LabelSet
	for _, l := range lbls {
		lset.Labels = append(lset.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}

	clients := make([]Client, numOfClients)
	var resps []*storepb.SeriesResponse
	for j := range clients {
		switch dimension {
		case seriesDimension:
			fmt.Println("Building client with numSeries:", numberPerClient)

			resps = resps[:0]
			for _, s := range createSeriesWithOneSample(t, j, numberPerClient, nopAppender{}) {
				series = append(series, s)
				resps = append(resps, storepb.NewSeriesResponse(&s))
			}

			clients[j] = &testClient{
				StoreClient: &mockedStoreAPI{
					RespSeries: resps,
				},
				labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
				minTime:   int64(j * numberPerClient),
				maxTime:   series[len(series)-1].Chunks[len(series[len(series)-1].Chunks)-1].MaxTime,
			}
		case samplesDimension:
			fmt.Println("Building client with one series with numSamples:", numberPerClient)

			lblsSize := 0
			for _, l := range lset.Labels {
				lblsSize += l.Size()
			}

			c := chunkenc.NewXORChunk()
			a, err := c.Appender()
			testutil.Ok(t, err)

			sBytes := lblsSize
			resps = resps[:0]
			lastResps := 0

			i := 0
			samples := 0
			nextTs := int64(j * numberPerClient)
			for {
				a.Append(int64(j*numberPerClient+i), random.Float64())
				i++
				samples++
				if i < numOfClients && samples < maxSamplesPerChunk {
					continue
				}

				series[0].Chunks = append(series[0].Chunks, storepb.AggrChunk{
					MinTime: nextTs,
					MaxTime: int64(j*numberPerClient + i - 1),
					Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: c.Bytes()},
				})
				sBytes += len(c.Bytes())

				// Compose many frames as remote read would do (so sidecar StoreAPI): 1048576
				if i >= numOfClients || sBytes >= 1048576 {
					resps = append(resps, storepb.NewSeriesResponse(&storepb.Series{
						Labels: lset.Labels,
						Chunks: series[0].Chunks[lastResps:],
					}))
					lastResps = len(series[0].Chunks) - 1
				}
				if i >= numOfClients {
					break
				}

				nextTs = int64(j*numberPerClient + i)
			}

			clients[j] = &testClient{
				StoreClient: &mockedStoreAPI{
					RespSeries: resps,
				},
				labelSets: []storepb.LabelSet{{Labels: []storepb.Label{{Name: "ext1", Value: "1"}}}},
				minTime:   int64(j * numberPerClient),
				maxTime:   nextTs,
			}
		default:
			t.Fatal("unknown dimension", dimension)
		}
	}

	logger := log.NewNopLogger()
	store := &ProxyStore{
		logger:          logger,
		stores:          func() []Client { return clients },
		metrics:         newProxyStoreMetrics(nil),
		responseTimeout: 0,
	}

	var bCases []*benchSeriesCase
	for _, c := range cases {
		var expected []storepb.Series

		switch dimension {
		case seriesDimension:
			expected = series[:c]
		case samplesDimension:
			expected = series
		}

		bCases = append(bCases, &benchSeriesCase{
			name: fmt.Sprintf("%dof%d", c, numOfClients*numberPerClient),
			req: &storepb.SeriesRequest{
				MinTime: 0,
				MaxTime: int64(c) - 1,
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"},
				},
			},
			expected: expected,
		})
	}

	benchmarkSeries(t, store, bCases)
}
