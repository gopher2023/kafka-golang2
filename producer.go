package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"math/rand"
	"time"
)

func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // 高可靠性模式
	config.Producer.Retry.Max = 3                    // 重试次数
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"47.76.77.29:9092"}, config)
	if err != nil {
		log.Fatal("Failed to start producer:", err)
	}
	defer producer.Close()

	// 模拟突发请求（如每秒 10,000 次请求）
	for i := 0; i < 100; i++ {
		msg := &sarama.ProducerMessage{
			Topic: "order-topic",
			//Value: sarama.StringEncoder(generateMockData()), // 模拟业务数据
			Value: sarama.StringEncoder(fmt.Sprintf("%d, from fujg 25.4.6", i)),
		}

		// 异步发送（使用 goroutine 避免阻塞）
		go func() {
			_, _, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Failed to send message: %v", err)
				// 可在此处添加重试或死信队列处理
			}

		}()
		time.Sleep(1000 * time.Millisecond)
		fmt.Println("发送一个msg, ", i)

		// 控制突发速率（实际场景中可能不需要）
		//time.Sleep(time.Microsecond * 100)
	}
}

func generateMockData() string {
	// 生成模拟业务数据
	return string(rand.Intn(1000000))
}
