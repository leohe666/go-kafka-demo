// Package producer 提供了一个具有重试机制和死信队列功能的Kafka生产者实现
package producer

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Config 用于配置Kafka生产者
type Config struct {
	Brokers         []string
	DeadLetterTopic string
	MaxRetries      int
	NumWorkers      int
	RetryQueueSize  int
	ProducerConfig  *sarama.Config
}

const (
	DefaultDeadLetterTopic = "dead-letter-topic"
	DefaultMaxRetries      = 3
	DefaultNumWorkers      = 1
	DefaultRetryQueueSize  = 100
)

// NewConfig 创建一个默认配置
func NewConfig(brokers []string) *Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 1
	config.Producer.RequiredAcks = sarama.WaitForAll

	return &Config{
		Brokers:         brokers,
		DeadLetterTopic: DefaultDeadLetterTopic,
		MaxRetries:      DefaultMaxRetries,
		NumWorkers:      DefaultNumWorkers,
		RetryQueueSize:  DefaultRetryQueueSize,
		ProducerConfig:  config,
	}
}

// Producer 封装了具有重试和死信队列功能的Kafka生产者
type Producer struct {
	asyncProducer sarama.AsyncProducer
	config        *Config
	retryQueue    chan *sarama.ProducerMessage
	wg            sync.WaitGroup
}

// NewProducer 创建一个新的Producer实例
func NewProducer(config *Config) (*Producer, error) {
	asyncProducer, err := sarama.NewAsyncProducer(config.Brokers, config.ProducerConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 Kafka 生产者失败: %w", err)
	}

	producer := &Producer{
		asyncProducer: asyncProducer,
		config:        config,
		retryQueue:    make(chan *sarama.ProducerMessage, config.RetryQueueSize),
	}

	// 启动处理goroutine
	go producer.handleSuccesses()
	go producer.handleErrors()

	// 启动重试worker
	for i := 0; i < config.NumWorkers; i++ {
		producer.wg.Add(1)
		go producer.retryWorker(i)
	}

	return producer, nil
}

// Input 返回用于发送消息的通道
func (p *Producer) Input() chan<- *sarama.ProducerMessage {
	return p.asyncProducer.Input()
}

// Close 关闭生产者并等待所有消息处理完成
func (p *Producer) Close() {
	// 关闭重试队列通道，通知所有重试worker停止
	close(p.retryQueue)

	// 等待所有重试worker完成
	p.wg.Wait()

	// 关闭生产者
	p.asyncProducer.Close()
}

// handleSuccesses 处理成功发送的消息
func (p *Producer) handleSuccesses() {
	for msg := range p.asyncProducer.Successes() {
		log.Printf("成功发送: 主题 %s, 消息 %s, 分区 %d, 偏移量 %d\n", msg.Topic, msg.Key, msg.Partition, msg.Offset)
	}
}

// handleErrors 处理发送失败的消息
func (p *Producer) handleErrors() {
	for err := range p.asyncProducer.Errors() {
		// 尝试将失败的消息放入重试队列
		select {
		case p.retryQueue <- err.Msg:
			log.Printf("消息发送失败，已放入重试队列: %v", err.Msg)
		default:
			// 重试队列已满，直接发送到死信队列
			log.Printf("重试队列已满，将消息直接发送到死信队列：%v", err.Msg)
			p.sendToDLQ(err.Msg)
		}
	}
}

// retryWorker 是处理重试的worker
func (p *Producer) retryWorker(workerID int) {
	defer p.wg.Done()

	for msg := range p.retryQueue {
		retryCount := p.getRetryCount(msg)

		// 如果重试次数达到上限，发送到死信队列
		if retryCount >= p.config.MaxRetries {
			log.Printf("Worker %d: 消息重试次数达到上限 (%d), 移动到死信队列...", workerID, p.config.MaxRetries)
			p.sendToDLQ(msg)
			continue
		}

		// 增加重试次数
		newMsg := p.increaseRetryCount(msg)

		// 指数退避
		backoff := time.Duration(1<<retryCount) * time.Second
		log.Printf("Worker %d: 正在重试消息 %s, 第 %d 次重试, 等待 %s...", workerID, newMsg.Key, retryCount+1, backoff)
		time.Sleep(backoff)

		// 发送消息
		p.asyncProducer.Input() <- newMsg
	}

	log.Printf("Worker %d: 重试队列已关闭，正在退出...", workerID)
}

// sendToDLQ 将消息发送到死信队列
func (p *Producer) sendToDLQ(originalMsg *sarama.ProducerMessage) {
	headers := originalMsg.Headers
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("failure-reason"),
		Value: []byte("failed after max retries"),
	})

	dlqMsg := &sarama.ProducerMessage{
		Topic:   p.config.DeadLetterTopic,
		Key:     originalMsg.Key,
		Value:   originalMsg.Value,
		Headers: headers,
	}

	p.asyncProducer.Input() <- dlqMsg
}

// getRetryCount 获取消息的重试次数
func (p *Producer) getRetryCount(msg *sarama.ProducerMessage) int {
	for _, h := range msg.Headers {
		if string(h.Key) == "retry-count" {
			count, _ := strconv.Atoi(string(h.Value))
			return count
		}
	}
	return 0
}

// increaseRetryCount 增加消息的重试次数
func (p *Producer) increaseRetryCount(msg *sarama.ProducerMessage) *sarama.ProducerMessage {
	headers := msg.Headers
	retryCount := p.getRetryCount(msg)

	found := false
	for i, h := range headers {
		if string(h.Key) == "retry-count" {
			headers[i].Value = []byte(strconv.Itoa(retryCount + 1))
			found = true
			break
		}
	}

	if !found {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte("retry-count"),
			Value: []byte(strconv.Itoa(1)),
		})
	}

	return &sarama.ProducerMessage{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}
}
