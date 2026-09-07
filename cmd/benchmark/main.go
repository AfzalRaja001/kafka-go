// Command benchmark drives real, concurrent Produce and Fetch load against a
// running kafka-go broker over the actual Kafka wire protocol (via
// github.com/twmb/franz-go, a real client - not calls into this project's
// own internal packages), then prints throughput and latency numbers for
// each phase. It's a one-off measurement tool, not a long-running service.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokerAddr := flag.String("broker-addr", "localhost:9092", "broker address to benchmark")
	topic := flag.String("topic", "benchmark", "topic to create, produce to, and fetch from")
	partitions := flag.Int("partitions", 1, "partitions to create the topic with")
	producers := flag.Int("producers", 4, "concurrent producer goroutines, each its own client connection")
	consumers := flag.Int("consumers", 4, "concurrent consumer goroutines, each independently reading the whole topic")
	recordSize := flag.Int("record-size", 256, "size in bytes of each produced record's value")
	duration := flag.Duration("duration", 10*time.Second, "how long the produce phase runs")
	fetchTimeout := flag.Duration("fetch-timeout", 30*time.Second, "safety cap on how long the fetch phase waits to catch up")
	fetchMaxBytes := flag.Int("fetch-max-bytes", 64*1024, "max bytes per partition per Fetch request - kept well below franz-go's 1MB default so the fetch phase measures many realistically-sized round trips instead of one or two giant ones")
	flag.Parse()

	ctx := context.Background()

	if err := ensureTopic(ctx, *brokerAddr, *topic, int32(*partitions)); err != nil {
		log.Fatalf("create topic: %v", err)
	}

	fmt.Printf("kafka-go benchmark: broker=%s topic=%s partitions=%d producers=%d consumers=%d record-size=%dB duration=%s\n\n",
		*brokerAddr, *topic, *partitions, *producers, *consumers, *recordSize, *duration)

	produced, _ := runProducePhase(ctx, *brokerAddr, *topic, *producers, *recordSize, *duration)
	printSummary("Produce", produced)

	target, err := endOffsetTotal(ctx, *brokerAddr, *topic)
	if err != nil {
		log.Fatalf("read end offsets: %v", err)
	}

	fetchCtx, cancel := context.WithTimeout(ctx, *fetchTimeout)
	defer cancel()
	fetched := runFetchPhase(fetchCtx, *brokerAddr, *topic, *consumers, target, int32(*fetchMaxBytes))
	printSummary("Fetch", fetched)
}

// ensureTopic creates the benchmark topic via the real CreateTopics wire
// call (kadm, the franz-go admin client) rather than requiring it to be
// pre-created by hand - one command should be enough to run this tool.
// "topic already exists" is not an error here: re-running the benchmark
// against the same broker without restarting it is a normal thing to do.
func ensureTopic(ctx context.Context, brokerAddr, topic string, partitions int32) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		return err
	}
	defer client.Close()

	// admin.CreateTopic's returned error IS its CreateTopicResponse.Err (see
	// kadm's own source: `return response, response.Err`) - a single value,
	// not two independent ones - so the "already exists is fine" check has
	// to happen on this error, not on a separate resp.Err field.
	admin := kadm.NewClient(client)
	_, err = admin.CreateTopic(ctx, partitions, 1, nil, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	return nil
}

// endOffsetTotal sums the latest offset across every partition of topic -
// this is how the fetch phase knows when it has genuinely read everything
// the produce phase wrote, rather than guessing from a local counter that
// never saw records lost to a produce error.
func endOffsetTotal(ctx context.Context, brokerAddr, topic string) (int64, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
	if err != nil {
		return 0, err
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	offsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, err
	}
	if err := offsets.Error(); err != nil {
		return 0, err
	}

	var total int64
	offsets.Each(func(o kadm.ListedOffset) { total += o.Offset })
	return total, nil
}

// runProducePhase runs `producers` goroutines, each its own client
// connection, each producing as fast as it can until duration elapses.
// Every goroutine keeps its own latency samples and only merges them into
// the shared result under a lock once it's done, so the hot produce loop
// itself never contends on a shared mutex.
func runProducePhase(ctx context.Context, brokerAddr, topic string, producers, recordSize int, duration time.Duration) (Summary, int64) {
	value := make([]byte, recordSize)
	if _, err := rand.Read(value); err != nil {
		log.Fatalf("generate record payload: %v", err)
	}

	deadline := time.Now().Add(duration)
	start := time.Now()

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		allLatencies []time.Duration
		totalBytes   int64
		totalRecords int64
	)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := kgo.NewClient(
				kgo.SeedBrokers(brokerAddr),
				kgo.DefaultProduceTopic(topic),
			)
			if err != nil {
				log.Printf("producer: connect: %v", err)
				return
			}
			defer client.Close()

			var latencies []time.Duration
			var bytes, records int64

			for time.Now().Before(deadline) {
				reqStart := time.Now()
				results := client.ProduceSync(ctx, &kgo.Record{Value: value})
				latency := time.Since(reqStart)

				if err := results.FirstErr(); err != nil {
					log.Printf("producer: produce error: %v", err)
					continue
				}
				latencies = append(latencies, latency)
				bytes += int64(len(value))
				records++
			}

			mu.Lock()
			allLatencies = append(allLatencies, latencies...)
			totalBytes += bytes
			totalRecords += records
			mu.Unlock()
		}()
	}
	wg.Wait()

	return Summarize(allLatencies, totalRecords, totalBytes, time.Since(start)), totalRecords
}

// runFetchPhase runs `consumers` goroutines, each its own client connection
// independently reading the whole topic from the beginning - simulating
// that many separate consuming applications reading this broker
// concurrently, rather than splitting the work up between them. Each
// goroutine stops once it has personally seen `target` records; ctx's
// deadline (fetchTimeout in main) is the safety net if a goroutine never
// gets there.
//
// fetchMaxBytes is capped well below franz-go's 1MB-per-partition default
// deliberately: live-verification against a real broker found that one
// giant Fetch draining thousands of small records at once produced only
// one or two multi-second-latency samples - technically correct, but
// useless as a latency distribution. A smaller per-request cap trades a
// slightly less efficient wire format for what a throughput benchmark
// actually needs: many realistically-sized round trips to compute a
// meaningful p50/p95/p99 from.
func runFetchPhase(ctx context.Context, brokerAddr, topic string, consumers int, target int64, fetchMaxBytes int32) Summary {
	start := time.Now()

	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		allLatencies []time.Duration
		totalBytes   int64
		totalRecords int64
	)

	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := kgo.NewClient(
				kgo.SeedBrokers(brokerAddr),
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxPartitionBytes(fetchMaxBytes),
			)
			if err != nil {
				log.Printf("consumer: connect: %v", err)
				return
			}
			defer client.Close()

			var latencies []time.Duration
			var bytes, records int64

			for records < target {
				reqStart := time.Now()
				fetches := client.PollFetches(ctx)
				latency := time.Since(reqStart)

				if ctx.Err() != nil {
					break
				}
				if err := fetches.Err(); err != nil {
					log.Printf("consumer: fetch error: %v", err)
					continue
				}

				latencies = append(latencies, latency)
				fetches.EachRecord(func(r *kgo.Record) {
					bytes += int64(len(r.Value))
					records++
				})
			}

			mu.Lock()
			allLatencies = append(allLatencies, latencies...)
			totalBytes += bytes
			totalRecords += records
			mu.Unlock()
		}()
	}
	wg.Wait()

	return Summarize(allLatencies, totalRecords, totalBytes, time.Since(start))
}

func printSummary(phase string, s Summary) {
	fmt.Printf("%s: %d records, %.2f MB in %s\n", phase, s.Records, float64(s.Bytes)/(1024*1024), s.Elapsed.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f records/sec, %.2f MB/sec\n", s.RecordsPerSec, s.MBPerSec)
	fmt.Printf("  latency:    p50=%s p95=%s p99=%s\n\n", s.P50, s.P95, s.P99)
}
