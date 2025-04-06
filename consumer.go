package main

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	consumerGroup, err := sarama.NewConsumerGroup([]string{"47.76.77.29:9092"}, "peak_consumer_group", config)
	if err != nil {
		log.Fatal("Failed to create consumer group:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动消费者组
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"peak_traffic"}, &consumerHandler{}); err != nil {
				log.Printf("Consumer error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 优雅关闭
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	cancel()
	wg.Wait()
	consumerGroup.Close()
}

type consumerHandler struct{}

func (h *consumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 控制消费速率（如每秒处理 100 条）
	rateLimiter := time.Tick(time.Millisecond * 10)
	for msg := range claim.Messages() {
		<-rateLimiter

		// 业务处理逻辑
		if err := processMessage(msg.Value); err != nil {
			log.Printf("Process failed: %v", err)
			continue
		}

		// 手动提交偏移量
		sess.MarkMessage(msg, "")
	}
	return nil
}

func processMessage(data []byte) error {
	// 模拟业务处理（如写入数据库）
	time.Sleep(10 * time.Millisecond)
	return nil
}
