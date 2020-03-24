// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/client"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
	"github.com/thanos-io/thanos/pkg/promclient"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/e2eutil"
	"github.com/thanos-io/thanos/test/e2e/e2ethanos"
)

func TestCompactWithStoreGateway(t *testing.T) {
	t.Parallel()

	l := log.NewLogfmtLogger(os.Stdout)
	type blockDesc struct {
		series  []labels.Labels
		extLset labels.Labels
		mint    int64
		maxt    int64
	}

	delay := 30 * time.Minute
	now := time.Now()

	// Simulate real scenario, including more complex cases like overlaps if needed.
	// TODO(bwplotka): Add blocks to compact, downsample and test delayed delete.
	blocks := []blockDesc{
		// ext1=value1: Non overlapping blocks, not ready for compaction.
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("ext1", "value1", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("ext1", "value1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("ext1", "value1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
		{
			series:  []labels.Labels{labels.FromStrings("a", "1", "b", "2")},
			extLset: labels.FromStrings("ext1", "value1", "replica", "2"),
			mint:    timestamp.FromTime(now.Add(6 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(8 * time.Hour)),
		},
	}
	blocks = append(blocks,
		// ext1=value2: Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("ext1", "value2", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("ext1", "value2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("ext1", "value2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(3 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("ext1", "value2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// ext1=value3: Multi-Replica partial overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("ext1", "value3", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("ext1", "value3", "rule_replica", "2", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("ext1", "value3", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(1 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("ext1", "value3", "rule_replica", "1", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},

		// ext1=value4: Replica full overlapping blocks, not ready for compaction, among no-overlapping blocks.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
			},
			extLset: labels.FromStrings("ext1", "value4", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "3"),
			},
			extLset: labels.FromStrings("ext1", "value4", "replica", "1"),
			mint:    timestamp.FromTime(now),
			maxt:    timestamp.FromTime(now.Add(2 * time.Hour)),
		},
		// Extra.
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "1", "b", "4"),
			},
			extLset: labels.FromStrings("ext1", "value4", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(2 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(4 * time.Hour)),
		},
		blockDesc{
			series: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "5"),
			},
			extLset: labels.FromStrings("ext1", "value4", "replica", "1"),
			mint:    timestamp.FromTime(now.Add(4 * time.Hour)),
			maxt:    timestamp.FromTime(now.Add(6 * time.Hour)),
		},
	)

	s, err := e2e.NewScenario("e2e_test_compact")
	testutil.Ok(t, err)
	defer s.Close() // TODO(kakkoyun): Change with t.CleanUp after go 1.14 update.

	dir := filepath.Join(s.SharedDir(), "tmp")
	testutil.Ok(t, os.MkdirAll(dir, os.ModePerm))

	const bucket = "compact_test"
	m := e2edb.NewMinio(8080, bucket)
	testutil.Ok(t, s.StartAndWaitReady(m))

	bkt, err := s3.NewBucketWithConfig(l, s3.Config{
		Bucket:    bucket,
		AccessKey: e2edb.MinioAccessKey,
		SecretKey: e2edb.MinioSecretKey,
		Endpoint:  m.HTTPEndpoint(), // We need separate client config, when connecting to minio from outside.
		Insecure:  true,
	}, "test-feed")
	testutil.Ok(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel() // TODO(kakkoyun): Change with t.CleanUp after go 1.14 update.

	rawBlockIDs := map[ulid.ULID]struct{}{}
	for _, b := range blocks {
		id, err := e2eutil.CreateBlockWithBlockDelay(ctx, dir, b.series, 120, b.mint, b.maxt, delay, b.extLset, 0)
		testutil.Ok(t, err)
		testutil.Ok(t, objstore.UploadDir(ctx, l, bkt, path.Join(dir, id.String()), id.String()))
		rawBlockIDs[id] = struct{}{}
	}

	svcConfig := client.BucketConfig{
		Type: client.S3,
		Config: s3.Config{
			Bucket:    bucket,
			AccessKey: e2edb.MinioAccessKey,
			SecretKey: e2edb.MinioSecretKey,
			Endpoint:  m.NetworkHTTPEndpoint(),
			Insecure:  true,
		},
	}
	str, err := e2ethanos.NewStoreGW(s.SharedDir(), "1", svcConfig)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(str))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs))), "thanos_blocks_meta_synced"))
	testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

	q, err := e2ethanos.NewQuerier(s.SharedDir(), "1", []string{str.GRPCNetworkEndpoint()}, nil)
	testutil.Ok(t, err)
	testutil.Ok(t, s.StartAndWaitReady(q))

	t.Run("no replica label with overlaps should halt compactor", func(t *testing.T) {
		c, err := e2ethanos.NewCompactor(s.SharedDir(), "expect-to-halt", svcConfig, nil)
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(c))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs))), "thanos_blocks_meta_synced"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_modified"))

		// Expect compactor halted.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(1), "thanos_compactor_halted"))

		// We expect no ops.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compactor_iterations_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compactor_blocks_cleaned_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compactor_blocks_marked_for_deletion_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_failures_total"))
		testutil.Ok(t, s.Stop(c))
	})
	t.Run("native vertical deduplication shoukd kick in", func(t *testing.T) {
		c, err := e2ethanos.NewCompactor(s.SharedDir(), "working", svcConfig, nil, "--deduplication.replica-label=replica", "--deduplication.replica-label=rule_replica")
		testutil.Ok(t, err)
		testutil.Ok(t, s.StartAndWaitReady(c))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs))), "thanos_blocks_meta_synced"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(16), "thanos_blocks_meta_modified"))

		// Wait for first iteration.
		testutil.Ok(t, c.WaitSumMetrics(e2e.Greater(0), "thanos_compactor_iterations_total"))

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(7), "thanos_compactor_blocks_cleaned_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(7), "thanos_compactor_blocks_marked_for_deletion_total")) // ?
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(3), "thanos_compact_group_compactions_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(3), "thanos_compact_group_compaction_runs_started_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_total"))
		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compact_downsample_failures_total"))

		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(float64(len(rawBlockIDs)-7+3)), "thanos_blocks_meta_synced"))
		testutil.Ok(t, str.WaitSumMetrics(e2e.Equals(0), "thanos_blocks_meta_sync_failures_total"))

		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		// Check if query detects new blocks.
		queryAndAssert(t, ctx, q.HTTPEndpoint(),
			`{ext1!="give me all metrics, I am curious"}`,
			promclient.QueryOptions{
				Deduplicate: false, // This should be false, so that we can be sure deduplication was offline.
			},
			[]model.Metric{},
		)

		testutil.Ok(t, c.WaitSumMetrics(e2e.Equals(0), "thanos_compactor_halted"))
		// Make sure compactor does not modify anything.
		testutil.Ok(t, s.Stop(c))

		// Check the blocks status.
		var seenMetas []metadata.Meta
		testutil.Ok(t, bkt.Iter(ctx, "", func(n string) error {
			id, ok := block.IsBlockDir(n)
			if !ok {
				return nil
			}

			meta, err := block.DownloadMeta(ctx, l, bkt, id)
			if err != nil {
				return err
			}

			seenMetas = append(seenMetas, meta)
			return nil
		}))

		// Sort by ULID.
		sort.Slice(seenMetas, func(i, j int) bool {
			return seenMetas[i].ULID.Compare(seenMetas[j].ULID) < 0
		})

		fmt.Println(rawBlockIDs)
		fmt.Println(seenMetas)
	})
}
