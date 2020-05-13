package rocketmq

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type HTTPConsumer struct {
	consumer    rmq.MQConsumer
	consumeFunc ConsumeFunc
	client      rmq.MQClient

	topic          string
	expression     string
	maxFetch       int32
	pullTimeout    int64
	consumeOrderly bool

	stopSignal chan struct{}

	limiter *rate.Limiter
}

func (p *HTTPConsumer) Start() (err error) {

	p.stopSignal = make(chan struct{})

	p.pull()

	return
}

type httpRMQConsumeFunc func(respChan chan rmq.ConsumeMessageResponse, errChan chan error, numOfMessages int32, waitseconds int64)

func (p *HTTPConsumer) pull() {

	ctx, _ := context.WithCancel(context.TODO())

	respChan := make(chan rmq.ConsumeMessageResponse)
	errChan := make(chan error)
	stopChan := make(chan struct{})

	var consumeFn httpRMQConsumeFunc = p.consumer.ConsumeMessageOrderly
	if !p.consumeOrderly {
		consumeFn = p.consumer.ConsumeMessage
	}

	go func(respChan chan rmq.ConsumeMessageResponse, errChan chan error, stopChan chan struct{}) {
		for {
			consumeFn(respChan, errChan,
				p.maxFetch,
				p.pullTimeout,
			)

			select {
			case <-p.stopSignal:
				{
					logrus.WithFields(logrus.Fields{
						"topic":      p.topic,
						"expression": p.expression,
						"endpoint":   p.client.GetEndpoint(),
					}).Info("stopping http consumer")

					stopChan <- struct{}{}
					<-stopChan
					p.stopSignal <- struct{}{}

					return
				}
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}
	}(respChan, errChan, stopChan)

	genAckFunc := func(message rmq.ConsumeMessageEntry) func() {
		return func() {
			if len(message.ReceiptHandle) == 0 {
				return
			}

			p.consumer.AckMessage([]string{message.ReceiptHandle})
		}
	}

	go func(respChan <-chan rmq.ConsumeMessageResponse, errChan <-chan error, stopChan chan struct{}) {

		for {
			select {
			case resp := <-respChan:
				{
					p.limiter.Wait(ctx)

					for i := 0; i < len(resp.Messages); i++ {

						err := p.consumeFunc(resp.Messages[i], genAckFunc(resp.Messages[i]))
						if err != nil {
							logrus.WithError(err).WithFields(
								logrus.Fields{
									"host_id":    resp.Messages[i].HostId,
									"message_id": resp.Messages[i].MessageId,
									"topic":      p.topic,
									"endpoint":   p.client.GetEndpoint(),
								},
							).Errorln("consume response failure")
							continue
						}

					}
				}
			case err := <-errChan:
				{
					logrus.WithError(err).WithFields(
						logrus.Fields{
							"topic":    p.topic,
							"endpoint": p.client.GetEndpoint(),
						},
					).Errorln("an error from consume message")
				}
			case <-stopChan:
				{
					stopChan <- struct{}{}
				}
			default:
				time.Sleep(time.Millisecond * 10)
			}
		}

	}(respChan, errChan, stopChan)

}

func (p *HTTPConsumer) Stop() (err error) {

	if p.stopSignal != nil {

		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil

		logrus.WithFields(logrus.Fields{
			"topic":      p.topic,
			"expression": p.expression,
			"endpoint":   p.client.GetEndpoint(),
		}).Info("http consumer stopped")
	}

	return nil
}

func NewHTTPConsumer(conf config.Configuration, fn ConsumeFunc) (consumer *HTTPConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	endpoint := consumerConf.GetString("endpoint")
	groupID := consumerConf.GetString("group-id")
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	maxFetch := consumerConf.GetInt32("max-fetch", 16)
	pullTimeout := consumerConf.GetInt64("pull-timeout", 30)
	instanceID := consumerConf.GetString("instance-id", "")
	consumeOrderly := consumerConf.GetBoolean("consume-orderly", true)

	if maxFetch <= 0 {
		maxFetch = 1
	} else if maxFetch > 16 {
		maxFetch = 16
	}

	if pullTimeout > 30 {
		pullTimeout = 30
	}

	credentialName := consumerConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	securityToken := conf.GetString("credentials." + credentialName + ".security-token")

	client := rmq.NewAliyunMQClient(endpoint, accessKey, secretKey, securityToken)
	mqConsumer := client.GetConsumer(instanceID, topic, groupID, expression)

	qps := consumerConf.GetFloat64("rate-limit.qps", 1000)
	bucketSize := consumerConf.GetInt32("rate-limit.bucket-size", 1)

	logrus.WithFields(
		logrus.Fields{
			"topic":       topic,
			"expression":  expression,
			"endpoint":    endpoint,
			"instance_id": instanceID,
			"qps":         qps,
			"bucket_size": bucketSize,
		},
	).Debug("rate limit configured")

	consumer = &HTTPConsumer{
		topic:          topic,
		expression:     expression,
		maxFetch:       maxFetch,
		pullTimeout:    pullTimeout,
		consumeOrderly: consumeOrderly,

		consumer: mqConsumer,
		client:   client,

		consumeFunc: fn,

		limiter: rate.NewLimiter(rate.Limit(qps), int(bucketSize)),
	}

	return
}
