package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	println("Hello, World!")
	println("args: ", strings.Join(os.Args[1:], " "))
	filename := os.Args[1]
	println("filename: ", filename)

	startTime := time.Now()

	filePath, err := filepath.Abs(filename)
	if err != nil {
		println("Error resolving path: ", err)
		return
	}

	seeds := []string{"localhost:19092"}
	controlTopic := "text-chunks-control"
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()

	file, err := os.Open(filePath)
	if err != nil {
		println("Error opening file: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	chunkSize := 100
	chunkNumber := 0
	var buffer strings.Builder
	var sentences []string
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()

		if buffer.Len() > 0 {
			buffer.WriteByte(' ')
		}
		buffer.WriteString(line)

		newSentences, remainder := extractSentences(buffer.String())
		buffer.Reset()
		if strings.TrimSpace(remainder) != "" {
			buffer.WriteString(remainder)
		}

		sentences = append(sentences, newSentences...)

		for len(sentences) >= chunkSize {
			chunkNumber++
			chunkSentences := sentences[:chunkSize]
			chunkText := strings.Join(chunkSentences, " ")
			chunkLength := len(chunkText)

			wg.Add(1)
			record := &kgo.Record{Topic: "text-chunks", Value: []byte(chunkText)}
			currentChunk := chunkNumber
			currentLength := chunkLength

			cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					fmt.Printf("Chunk %d had a produce error: %v\n", currentChunk, err)
				} else {
					fmt.Printf("Chunk %d: length = %d bytes (sent to Redpanda)\n", currentChunk, currentLength)
				}
			})

			sentences = sentences[chunkSize:]
		}
	}

	if err := scanner.Err(); err != nil {
		println("Error reading file: ", err)
		return
	}

	if strings.TrimSpace(buffer.String()) != "" {
		sentences = append(sentences, strings.TrimSpace(buffer.String()))
	}

	for len(sentences) > 0 {
		size := chunkSize
		if len(sentences) < chunkSize {
			size = len(sentences)
		}

		chunkNumber++
		chunkSentences := sentences[:size]
		chunkText := strings.Join(chunkSentences, " ")
		chunkLength := len(chunkText)

		wg.Add(1)
		record := &kgo.Record{Topic: "text-chunks", Value: []byte(chunkText)}
		currentChunk := chunkNumber
		currentLength := chunkLength

		cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				fmt.Printf("Chunk %d had a produce error: %v\n", currentChunk, err)
			} else {
				fmt.Printf("Chunk %d: length = %d bytes (sent to Redpanda)\n", currentChunk, currentLength)
			}
		})

		sentences = sentences[size:]
	}

	wg.Wait()
	log.Printf("All sentence chunks sent to Redpanda! Time: %s, Start time: %s", time.Since(startTime).String(), startTime.Format(time.RFC3339))

	controlPayload := []byte(fmt.Sprintf(`{"total_chunks": %d}`, chunkNumber))
	done := make(chan struct{})
	cl.Produce(ctx, &kgo.Record{
		Topic: controlTopic,
		Value: controlPayload,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("Control message produce error: %v\n", err)
		} else {
			fmt.Printf("Control message sent with total_chunks=%d\n", chunkNumber)
		}
		close(done)
	})
	<-done
}

func extractSentences(text string) ([]string, string) {
	re := regexp.MustCompile(`[^.!?]+[.!?]`)
	indices := re.FindAllStringIndex(text, -1)
	if len(indices) == 0 {
		return nil, text
	}

	sentences := make([]string, 0, len(indices))
	lastEnd := 0
	for _, idx := range indices {
		start, end := idx[0], idx[1]
		s := strings.TrimSpace(text[start:end])
		if s != "" {
			sentences = append(sentences, s)
		}
		lastEnd = end
	}

	remainder := strings.TrimSpace(text[lastEnd:])
	return sentences, remainder
}
