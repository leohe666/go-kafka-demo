package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// 连接并生产消息
	// config := sarama.NewConfig()
	// config.Producer.RequiredAcks = sarama.WaitForAll
	// config.Producer.Partitioner = sarama.NewRandomPartitioner
	// config.Producer.Return.Successes = true

	// msg := &sarama.ProducerMessage{}
	// msg.Topic = "test-topic"
	// msg.Value = sarama.StringEncoder("p")
	// client, err := sarama.NewSyncProducer([]string{"192.168.5.128:29092"}, config)
	// if err != nil {
	// 	fmt.Println("producer closed, err:", err)
	// 	return
	// }
	// defer client.Close()
	// pid, offset, err := client.SendMessage(msg)
	// if err != nil {
	// 	fmt.Println("send msg failed, err:", err)
	// 	return
	// }
	// fmt.Printf("pid:%v offset:%v\n", pid, offset)

	// 连接并消费消息
	// consumer, err := sarama.NewConsumer([]string{"192.168.5.128:29092"}, nil)
	// if err != nil {
	// 	fmt.Printf("fail to start consumer, err:%v\n", err)
	// 	return
	// }
	// partitionList, err := consumer.Partitions("test-topic") // 订阅test-topic下所有的partitions
	// fmt.Println("p", partitionList)
	// for partition := range partitionList { // 遍历所有的分区
	// 	pc, err := consumer.ConsumePartition("test-topic", int32(partition), sarama.OffsetNewest)
	// 	if err != nil {
	// 		fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
	// 		return
	// 	}
	// 	defer pc.AsyncClose()
	// 	// 异步从每个分区消费信息
	// 	go func(sarama.PartitionConsumer) {
	// 		for msg := range pc.Messages() {
	// 			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v\n", msg.Partition, msg.Offset, msg.Key, string(msg.Value))
	// 		}
	// 	}(pc)
	// }
	// time.Sleep(100 * time.Second)

	// // 持续监听消费
	// config := sarama.NewConfig()
	// config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest // 从最早消息开始
	// consumer, err := sarama.NewConsumer([]string{"192.168.5.128:29092"}, config)
	// if err != nil {
	// 	fmt.Printf("Failed to create consumer: %v\n", err)
	// 	return
	// }
	// defer consumer.Close()

	// topics := []string{"test-topic"}
	// partitionConsumer, err := consumer.ConsumePartition(topics[0], 0, sarama.OffsetOldest)
	// if err != nil {
	// 	fmt.Printf("Failed to start partition consumer: %v\n", err)
	// 	return
	// }
	// defer partitionConsumer.Close()

	// for {
	// 	select {
	// 	case msg := <-partitionConsumer.Messages():
	// 		fmt.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Value=%s\n",
	// 			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	// 	case err := <-partitionConsumer.Errors():
	// 		fmt.Printf("Error: %v\n", err)
	// 	}
	// }

	// // 多分区发送消息
	// config := sarama.NewConfig()
	// config.Producer.Return.Successes = true // 需要成功交付的消息 channel

	// producer, err := sarama.NewSyncProducer([]string{"192.168.5.128:29092"}, config)
	// if err != nil {
	// 	log.Fatalln("Failed to start Sarama producer:", err)
	// }
	// defer producer.Close()

	// topic := "test-multi-partition"

	// messages := []*sarama.ProducerMessage{
	// 	// 消息 without Key (将由生产者按轮询策略分配分区)
	// 	{Topic: topic, Value: sarama.StringEncoder("Message without Key 1")},
	// 	{Topic: topic, Value: sarama.StringEncoder("Message without Key 2")},
	// 	{Topic: topic, Value: sarama.StringEncoder("Message without Key 3")},

	// 	// 消息 with the same Key (将被分配到同一个分区)
	// 	{Topic: topic, Key: sarama.StringEncoder("user_1"), Value: sarama.StringEncoder("Order for user_1")},
	// 	{Topic: topic, Key: sarama.StringEncoder("user_2"), Value: sarama.StringEncoder("Order for user_2")},
	// 	{Topic: topic, Key: sarama.StringEncoder("user_1"), Value: sarama.StringEncoder("Payment for user_1")},  // 相同Key
	// 	{Topic: topic, Key: sarama.StringEncoder("user_1"), Value: sarama.StringEncoder("Shipment for user_1")}, // 相同Key
	// }

	// for _, msg := range messages {
	// 	partition, offset, err := producer.SendMessage(msg)
	// 	if err != nil {
	// 		log.Printf("FAILED to send message: %s\n", err)
	// 	} else {
	// 		key := "nil"
	// 		if msg.Key != nil {
	// 			key = string(msg.Key.(sarama.StringEncoder))
	// 		}
	// 		// 这里可以直接打印出每条消息被发送到了哪个分区的哪个偏移量！
	// 		fmt.Printf("Message sent to topic %s. Key: '%s' -> Partition: %d, Offset: %d\n",
	// 			topic, key, partition, offset)
	// 	}
	// }

	// 多分区消费所有分区信息
	// consumer, err := sarama.NewConsumer([]string{"192.168.5.128:29092"}, nil)
	// if err != nil {
	// 	log.Fatalln("Failed to start Sarama consumer:", err)
	// }
	// defer consumer.Close()

	// topic := "test-multi-partition"
	// partitionList, err := consumer.Partitions(topic)
	// if err != nil {
	// 	log.Fatalln("Failed to get the list of partitions:", err)
	// }

	// fmt.Printf("Partitions for topic %s: %v\n", topic, partitionList)

	// var consumers []sarama.PartitionConsumer
	// for _, partition := range partitionList {
	// 	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	// 	if err != nil {
	// 		log.Printf("Failed to start consumer for partition %d: %s", partition, err)
	// 		continue
	// 	}
	// 	defer pc.Close()
	// 	consumers = append(consumers, pc)

	// 	// 为每个分区启动一个goroutine来消费消息
	// 	go func(pc sarama.PartitionConsumer, partition int32) {
	// 		for msg := range pc.Messages() {
	// 			key := string(msg.Key)
	// 			if key == "" {
	// 				key = "NULL"
	// 			}
	// 			fmt.Printf("Received from Partition: %d, Offset: %d, Key: '%s', Value: '%s'\n",
	// 				msg.Partition, msg.Offset, key, string(msg.Value))
	// 		}
	// 	}(pc, partition)
	// }
	// // 等待中断信号以退出
	// sigchan := make(chan os.Signal, 1)
	// signal.Notify(sigchan, os.Interrupt)
	// <-sigchan
	// fmt.Println("Interrupt is detected, shutting down...")

	// 多分区消费指定单分区
	// 	// 配置消费者
	// 	config := sarama.NewConfig()
	// 	config.Consumer.Return.Errors = true

	// 	// 创建消费者实例
	// 	consumer, err := sarama.NewConsumer([]string{"192.168.5.128:29092"}, config)
	// 	if err != nil {
	// 		log.Fatalf("Error creating consumer: %v", err)
	// 	}
	// 	defer func() {
	// 		if err := consumer.Close(); err != nil {
	// 			log.Fatalf("Error closing consumer: %v", err)
	// 		}
	// 	}()

	// 	topic := "test-multi-partition"
	// 	// 指定要消费的分区号
	// 	partitionToConsume := int32(1)
	// 	// 指定起始偏移量，sarama.OffsetOldest 从最旧的消息开始
	// 	offset := sarama.OffsetOldest

	// 	// 创建分区消费者，只消费指定的分区
	// 	partitionConsumer, err := consumer.ConsumePartition(topic, partitionToConsume, offset)
	// 	if err != nil {
	// 		log.Fatalf("Error creating partition consumer: %v", err)
	// 	}
	// 	defer func() {
	// 		if err := partitionConsumer.Close(); err != nil {
	// 			log.Fatalf("Error closing partition consumer: %v", err)
	// 		}
	// 	}()

	// 	fmt.Printf("Started consuming partition %d of topic '%s'\n", partitionToConsume, topic)

	// 	// 设置信号通道，用于优雅退出
	// 	signals := make(chan os.Signal, 1)
	// 	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	// 	// 循环消费消息
	// ConsumerLoop:
	// 	for {
	// 		select {
	// 		case msg := <-partitionConsumer.Messages():
	// 			// 成功收到一条来自指定分区的消息
	// 			fmt.Printf("Partition: %d | Offset: %d | Key: %s | Value: %s\n",
	// 				msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

	// 		case err := <-partitionConsumer.Errors():
	// 			// 处理消费时遇到的错误
	// 			log.Printf("Error: %v\n", err)

	//		case <-signals:
	//			// 收到中断信号，退出循环
	//			fmt.Println("Interrupt received, shutting down...")
	//			break ConsumerLoop
	//		}
	//	}

	// 多分区消费指定多分区
	// 配置消费者
	// config := sarama.NewConfig()
	// config.Consumer.Return.Errors = true

	// // 创建消费者实例
	// consumer, err := sarama.NewConsumer([]string{"192.168.5.128:29092"}, config)
	// if err != nil {
	// 	log.Fatalf("Error creating consumer: %v", err)
	// }
	// defer func() {
	// 	if err := consumer.Close(); err != nil {
	// 		log.Fatalf("Error closing consumer: %v", err)
	// 	}
	// }()

	// topic := "test-multi-partition"
	// // 指定要消费的分区号
	// partitionsToConsume := []int32{0, 2} // 指定要消费的分区列表
	// // 指定起始偏移量，sarama.OffsetOldest 从最旧的消息开始

	// for _, partition := range partitionsToConsume {
	// 	// 为每个分区创建消费者
	// 	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	// 	if err != nil {
	// 		log.Printf("Failed to consume partition %d: %v", partition, err)
	// 		continue
	// 	}
	// 	defer pc.Close()

	// 	// 为每个分区启动一个消费goroutine
	// 	go func(pc sarama.PartitionConsumer, p int32) {
	// 		for msg := range pc.Messages() {
	// 			fmt.Printf("Partition %d: Offset %d | Key: %s | Value: %s\n",
	// 				p, msg.Offset, string(msg.Key), string(msg.Value))
	// 		}
	// 	}(pc, partition)
	// }

	// // 等待中断信号以退出
	// sigchan := make(chan os.Signal, 1)
	// signal.Notify(sigchan, os.Interrupt)
	// <-sigchan
	// fmt.Println("Interrupt is detected, shutting down...")

	// 带重试逻辑的异常消息处理的发送者
	// Kafka Broker 地址
	brokers := []string{"192.168.5.128:29092"}
	topic := "test-multi-partition"

	// 创建生产者
	producer, err := newProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// 示例消息
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("key-1"),
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	// 自定义重试逻辑，最大重试 3 次
	err = retryMessage(producer, msg, 3)
	if err != nil {
		log.Printf("Failed to send message after retries: %v", err)
	} else {
		log.Println("Message sent successfully")
	}

}

// 配置 Kafka 生产者
func newProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true                // 启用成功回调
	config.Producer.Return.Errors = true                   // 启用错误回调
	config.Producer.Retry.Max = 3                          // 内置重试次数
	config.Producer.Retry.Backoff = 100 * time.Millisecond // 重试间隔
	config.Producer.RequiredAcks = sarama.WaitForAll       // 等同于 acks=-1
	//	控制每个 Kafka Broker 连接上允许的最大未完成请求数（in-flight requests）。
	// 	默认值：5。
	// 	设置为 1 可确保消息按顺序发送，避免重试导致乱序，但会降低吞吐量。
	// 	较高的值（如默认的 5）允许更多并发请求，提升吞吐量，但可能导致消息乱序。
	// 与幂等性的关系：当启用 Producer.Idempotent = true 时，必须设置 Net.MaxOpenRequests = 1，以保证幂等性生效（避免重复消息）。
	config.Producer.Idempotent = true                       // 启用幂等性
	config.Net.MaxOpenRequests = 1                          // 确保顺序性（更正后的参数）
	config.Producer.Partitioner = sarama.NewHashPartitioner // 使用哈希分区器

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}
	return producer, nil
}

// 模拟写入死信队列（这里写入本地文件）
func writeToDeadLetterQueue(msg *sarama.ProducerMessage, err error) {
	file, _ := os.OpenFile("dead_letter_queue.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer file.Close()
	logEntry := fmt.Sprintf("Failed message: Topic=%s, Key=%s, Value=%s, Error=%v\n",
		msg.Topic, string(msg.Key.(sarama.StringEncoder)), string(msg.Value.(sarama.StringEncoder)), err)
	file.WriteString(logEntry)
}

// 自定义重试逻辑
func retryMessage(producer sarama.AsyncProducer, msg *sarama.ProducerMessage, maxRetries int) error {
	ctx := context.Background()
	attempt := 0
	baseDelay := 200 * time.Millisecond // 基础重试间隔

	for attempt < maxRetries {
		attempt++
		log.Printf("Attempt %d to send message to topic %s", attempt, msg.Topic)

		// 异步发送消息
		producer.Input() <- msg

		// 等待发送结果
		select {
		case success := <-producer.Successes():
			log.Printf("Message sent successfully to partition %d, offset %d", success.Partition, success.Offset)
			return nil
		case err := <-producer.Errors():
			log.Printf("Failed to send message: %v", err.Err)
			if attempt == maxRetries {
				// 达到最大重试次数，写入死信队列
				writeToDeadLetterQueue(msg, err.Err)
				return fmt.Errorf("max retries reached: %w", err.Err)
			}
			// 指数退避
			delay := baseDelay * time.Duration(1<<attempt) // 200ms, 400ms, 800ms...
			log.Printf("Retrying after %v", delay)
			time.Sleep(delay)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
