package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	brokers := getEnv("KAFKA_BROKERS", "localhost:19092")
	sourceTopic := getEnv("RESULT_TOPIC", "text-chunks-results")

	fmt.Printf("Sink worker starting. brokers=%s, sourceTopic=%s\n",
		brokers, sourceTopic)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(sourceTopic),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	for {
		pollCtx := ctx
		var cancelPoll context.CancelFunc

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
			var wr AggregateResult
			if err := json.Unmarshal(rec.Value, &wr); err != nil {
				fmt.Printf("failed to unmarshal worker result: %v\n", err)
				return
			}
			fmt.Printf("Received result\n")
			writeAggregateToFile(fmt.Sprintf("results-%d.json", time.Now().Unix()), &wr)
		})
	}
}

func writeAggregateToFile(path string, agg *AggregateResult) error {
	data, err := json.MarshalIndent(agg, "", "  ")
	if err != nil {
		return err
	}
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
