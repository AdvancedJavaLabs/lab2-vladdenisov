package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

type WorkerResult struct {
	ParagraphIndex    int               `json:"paragraph_index"`
	WordCount         int               `json:"word_count"`
	WordFreq          map[string]int    `json:"word_freq"`
	PosCount          int               `json:"pos_count"`
	NegCount          int               `json:"neg_count"`
	SentimentScore    int               `json:"sentiment_score"`
	ReplacedText      string            `json:"replaced_text"`
	SortedSentences   []string          `json:"sorted_sentences"`
	TopN              []WordFrequency   `json:"top_n"`
	TopNSize          int               `json:"top_n_size"`
	OriginalMeta      map[string]string `json:"original_metadata,omitempty"`
	OriginalTimestamp int64             `json:"original_timestamp"`
	Timestamp         int64             `json:"timestamp"`
}

type WordFrequency struct {
	Word  string `json:"word"`
	Count int    `json:"count"`
}

type AggregateResult struct {
	AvgTimeProcessing int64             `json:"avg_time_processing_ms"`
	AvgTimeAll        int64             `json:"avg_time_all_ms"`
	AvgTimeCount      int64             `json:"avg_time_count"`
	TotalParagraphs   int               `json:"total_paragraphs"`
	TotalWordCount    int               `json:"total_word_count"`
	GlobalWordFreq    map[string]int    `json:"global_word_freq"`
	GlobalTopN        []WordFrequency   `json:"global_top_n"`
	TopNSize          int               `json:"top_n_size"`
	TotalPos          int               `json:"total_positive_words"`
	TotalNeg          int               `json:"total_negative_words"`
	TotalSentiment    int               `json:"total_sentiment_score"`
	ModifiedText      []string          `json:"modified_paragraphs"`
	AllSentences      []string          `json:"all_sorted_sentences"`
	Metadata          map[string]string `json:"metadata"`
}

type ControlMessage struct {
	TotalChunks int `json:"total_chunks"`
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	sourceTopic := getEnv("RESULT_TOPIC", "text-chunks-results")
	groupID := getEnv("CONSUMER_GROUP", "text-aggregator-group")
	outputFile := getEnv("OUTPUT_FILE", "results.json")
	topN := getEnvInt("TOP_N", 10)
	idleSeconds := getEnvInt("IDLE_SECONDS", 0)
	controlTopic := getEnv("CONTROL_TOPIC", "text-chunks-control")

	fmt.Printf("Aggregator starting. brokers=%s, sourceTopic=%s, group=%s, output=%s, topN=%d\n",
		brokers, sourceTopic, groupID, outputFile, topN)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(sourceTopic, controlTopic),
		kgo.ConsumerGroup(groupID),
	}

	cl_producer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
	)
	if err != nil {
		panic(err)
	}
	defer cl_producer.Close()

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	agg := &AggregateResult{
		GlobalWordFreq:    make(map[string]int),
		ModifiedText:      make([]string, 0),
		AllSentences:      make([]string, 0),
		Metadata:          make(map[string]string),
		TopNSize:          topN,
		AvgTimeProcessing: 0,
		AvgTimeAll:        0,
		AvgTimeCount:      0,
	}

	expectedChunks := 0
	gotResults := 0

	for {
		select {
		case <-sigCh:
			fmt.Println("Received shutdown signal, finalizing aggregation...")
			finalizeAggregation(ctx, cl_producer, agg, outputFile, sourceTopic)
			return
		default:
		}

		// Контекст с таймаутом, чтобы периодически проверять idle-таймер.
		pollCtx := ctx
		var cancelPoll context.CancelFunc
		if idleSeconds > 0 {
			pollCtx, cancelPoll = context.WithTimeout(ctx, 5*time.Second)
		}

		fetches := cl.PollFetches(pollCtx)
		if cancelPoll != nil {
			cancelPoll()
		}

		if fetches.IsClientClosed() {
			break
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				fmt.Printf("fetch error on topic %s partition %d: %v\n", e.Topic, e.Partition, e.Err)
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			switch rec.Topic {
			case sourceTopic:
				var wr WorkerResult
				if err := json.Unmarshal(rec.Value, &wr); err != nil {
					fmt.Printf("failed to unmarshal worker result: %v\n", err)
					return
				}
				updateAggregate(agg, &wr)
				gotResults++
			case controlTopic:
				var cm ControlMessage
				if err := json.Unmarshal(rec.Value, &cm); err != nil {
					fmt.Printf("failed to unmarshal control message: %v\n", err)
					return
				}
				expectedChunks = cm.TotalChunks
				fmt.Printf("Received control message: total_chunks=%d\n", expectedChunks)
			default:
				// ignore other topics
			}
		})

		// Если знаем, сколько чанков ждать — завершаем сразу после получения всех результатов.
		if expectedChunks > 0 && gotResults >= expectedChunks {
			fmt.Printf("Got all results: %d/%d, finalizing aggregation...\n", gotResults, expectedChunks)
			finalizeAggregation(ctx, cl_producer, agg, outputFile, sourceTopic)
			return
		}
	}
}

func updateAggregate(agg *AggregateResult, wr *WorkerResult) {
	agg.TotalParagraphs++
	agg.TotalWordCount += wr.WordCount
	agg.TotalPos += wr.PosCount
	agg.TotalNeg += wr.NegCount
	agg.TotalSentiment += wr.SentimentScore
	nowMs := time.Now().UnixMilli()

	if wr.OriginalTimestamp > 0 {
		agg.AvgTimeAll += nowMs - wr.OriginalTimestamp
		agg.AvgTimeCount++
	}
	if wr.OriginalTimestamp > 0 && wr.Timestamp > 0 {
		agg.AvgTimeProcessing += wr.Timestamp - wr.OriginalTimestamp
	}

	for w, c := range wr.WordFreq {
		agg.GlobalWordFreq[w] += c
	}

	if wr.ReplacedText != "" {
		agg.ModifiedText = append(agg.ModifiedText, wr.ReplacedText)
	}
	if len(wr.SortedSentences) > 0 {
		agg.AllSentences = append(agg.AllSentences, wr.SortedSentences...)
	}
}

func finalizeAggregation(ctx context.Context, producer *kgo.Client, agg *AggregateResult, outputFile string, topic string) {
	if agg.AvgTimeCount > 0 {
		agg.AvgTimeAll /= agg.AvgTimeCount
		agg.AvgTimeProcessing /= agg.AvgTimeCount
	}

	if len(agg.AllSentences) > 0 {
		sort.Slice(agg.AllSentences, func(i, j int) bool {
			return len(agg.AllSentences[i]) < len(agg.AllSentences[j])
		})
	}
	if len(agg.GlobalWordFreq) > 0 {
		agg.GlobalTopN = topNWords(agg.GlobalWordFreq, agg.TopNSize)
	}

	payload, err := json.Marshal(agg)
	if err != nil {
		fmt.Printf("Failed to marshal aggregate for Kafka: %v\n", err)
		return
	}

	done := make(chan struct{})
	producer.Produce(ctx, &kgo.Record{
		Topic: topic,
		Value: payload,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			writeAggregateToFile(fmt.Sprintf("results-%d.json", time.Now().Unix()), agg)
			fmt.Printf("Failed to produce aggregated result: %v\n", err)
		}
		close(done)
	})
	<-done
}

func writeAggregateToFile(path string, agg *AggregateResult) error {
	data, err := json.MarshalIndent(agg, "", "  ")
	if err != nil {
		return err
	}
	log.Printf("Write aggregate to file: %s, Time: %s", path, time.Now().Format(time.RFC3339))
	return os.WriteFile(path, data, 0o644)
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var parsed int
		_, err := fmt.Sscanf(v, "%d", &parsed)
		if err == nil {
			return parsed
		}
	}
	return def
}

func topNWords(freq map[string]int, n int) []WordFrequency {
	list := make([]WordFrequency, 0, len(freq))
	for w, c := range freq {
		list = append(list, WordFrequency{Word: w, Count: c})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Count == list[j].Count {
			return list[i].Word < list[j].Word
		}
		return list[i].Count > list[j].Count
	})
	if len(list) > n {
		list = list[:n]
	}
	return list
}
