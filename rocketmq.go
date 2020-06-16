package rocketmq

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	rmq "github.com/aliyunmq/mq-http-go-sdk"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(msg rmq.ConsumeMessageEntry, ackFunc func()) (err error)

type HTTPRocketMQComponent struct {
	opts component.Options

	consumer *HTTPConsumer

	alias string

	clients   sync.Map
	producers sync.Map
}

func init() {
	component.RegisterComponent("httprmq", NewHTTPRocketMQComponent)
	doc.RegisterDocumenter("httprmq", &HTTPRocketMQComponent{})
}

func (p *HTTPRocketMQComponent) Alias() string {
	if p == nil {
		return ""
	}
	return p.alias
}

func (p *HTTPRocketMQComponent) Start() error {
	if p.consumer != nil {
		return p.consumer.Start()
	}

	logrus.Debugf("httprmq component of '%s' Start()", p.alias)

	return nil
}

func (p *HTTPRocketMQComponent) Stop() (err error) {

	if p.consumer != nil {
		err = p.consumer.Stop()
		if err != nil {
			return
		}
	}

	logrus.Debugf("httprmq component of '%s' Stopped()", p.alias)

	return
}

func NewHTTPRocketMQComponent(alias string, opts ...component.Option) (comp component.Component, err error) {

	rmqComp := &HTTPRocketMQComponent{
		alias: alias,
	}

	err = rmqComp.init(opts...)
	if err != nil {
		return
	}

	comp = rmqComp

	return
}

func (p *HTTPRocketMQComponent) init(opts ...component.Option) (err error) {

	for _, o := range opts {
		o(&p.opts)
	}

	if p.opts.Config == nil {
		err = errors.New("httprmq component config is nil")
		return
	}

	if p.opts.Config.HasPath("consumer") {
		p.consumer, err = NewHTTPConsumer(p.opts.Config, p.postMessage)
		if err != nil {
			return
		}
	}

	return
}

func (p *HTTPRocketMQComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

type MQSendResult struct {
	Topic         string `json:"topic"`
	InstanceID    string `json:"instance_id"`
	Endpoint      string `json:"endpoint"`
	Tags          string `json:"tags"`
	Keys          string `json:"keys"`
	MsgID         string `json:"msg_id"`
	ReceiptHandle string `json:"receipt_handle"`
}

func (p *HTTPRocketMQComponent) sendMessage(session mail.Session) (err error) {

	logrus.WithField("component", "httprmq").WithField("To", session.To()).Debugln("send message")

	setBody := session.Query("setbody")

	if len(setBody) == 0 {
		fbp.BreakSession(session)
	}

	port := fbp.GetSessionPort(session)

	if port == nil {
		err = errors.New("port info not exist")
		return
	}

	topic := session.Query("topic")
	tags := session.Query("tags")

	if len(topic) == 0 {
		err = fmt.Errorf("query of %s is empty", "topic")
		return
	}

	endpoint := session.Query("endpoint")
	accessKey := session.Query("access_key")
	secretKey := session.Query("secret_key")
	securityToken := session.Query("security-token")
	instanceID := session.Query("instance_id")
	credentialName := session.Query("credential_name")

	if len(endpoint) == 0 {
		endpoint = port.Metadata["endpoint"]
	}

	if len(endpoint) == 0 {
		err = fmt.Errorf("unknown endpoint in http rocketmq component while send message, port to url: %s", port.Url)
		return
	}

	if len(instanceID) == 0 {
		instanceID = port.Metadata["instance_id"]
	}

	if len(instanceID) == 0 {
		err = fmt.Errorf("unknown instance id in http rocketmq component while send message, port to url: %s", port.Url)
		return
	}

	if len(credentialName) == 0 {
		credentialName = port.Metadata["credential_name"]
	}

	if len(credentialName) > 0 {
		accessKey = p.opts.Config.GetString("credentials." + credentialName + ".access-key")
		secretKey = p.opts.Config.GetString("credentials." + credentialName + ".secret-key")
		securityToken = p.opts.Config.GetString("credentials." + credentialName + ".security-token")
	}

	if len(accessKey) == 0 {
		accessKey = port.Metadata["access_key"]
	}

	if len(secretKey) == 0 {
		secretKey = port.Metadata["secret_key"]
	}

	if len(securityToken) == 0 {
		securityToken = port.Metadata["security_token"]
	}

	partition := session.Query("partition")
	errorGraph := session.Query("error")
	entrypointGraph := session.Query("entrypoint")

	var sendSession mail.Session = session

	if partition == "1" {
		var newSession mail.Session
		newSession, err = fbp.PartitionFromSession(session, entrypointGraph, errorGraph)
		if err != nil {
			return
		}
		sendSession = newSession
	}

	payload, ok := sendSession.Payload().Copy().Interface().(*protocol.Payload)
	if !ok {
		err = errors.New("could not convert session payload to *protocol.Payload")
		return
	}

	data, err := payload.ToBytes()
	if err != nil {
		return
	}

	msgBody := base64.StdEncoding.EncodeToString(data)

	propertyMap := map[string]string{}

	property, _ := port.Metadata["property"]

	if len(property) > 0 {
		err = json.Unmarshal([]byte(property), &propertyMap)
		if err != nil {
			err = errors.WithMessage(err, "property format error")
			return
		}
	}

	if mqttClientID, ok := session.Payload().Content().HeaderOf("MQTT-TO-CLIENT-ID"); ok && len(mqttClientID) > 0 {
		propertyMap["mqttSecondTopic"] = "/p2p/" + mqttClientID
	}

	msg := rmq.PublishMessageRequest{
		MessageBody: msgBody,
		MessageTag:  tags,
		Properties:  propertyMap,
		MessageKey:  payload.GetId(),
	}

	sendResult, err := p.sendMessageToRMQ(endpoint, accessKey, secretKey, securityToken, instanceID, topic, msg)

	if err != nil {
		return
	}

	if setBody == "1" {
		err = session.Payload().Content().SetBody(
			&MQSendResult{
				Topic:         topic,
				InstanceID:    instanceID,
				Endpoint:      endpoint,
				Tags:          tags,
				Keys:          payload.GetId(),
				MsgID:         sendResult.MessageId,
				ReceiptHandle: sendResult.ReceiptHandle,
			})
		if err != nil {
			return
		}
	}

	return
}

func (p *HTTPRocketMQComponent) getProducer(endpoint, accessKeyID, accessKeySecret, securityToken, instanceID, topic string) (ret rmq.MQProducer, err error) {

	clientKey := fmt.Sprintf("%s:%s", endpoint, accessKeyID)
	producerKey := fmt.Sprintf("%s:%s:%s:%s", endpoint, accessKeyID, instanceID, topic)

	var client rmq.MQClient
	var producer rmq.MQProducer

	producerValue, exist := p.producers.Load(producerKey)

	if exist {
		producer = producerValue.(rmq.MQProducer)
		return producer, nil
	}

	clientValue, exist := p.clients.Load(clientKey)

	if !exist {
		client = rmq.NewAliyunMQClient(endpoint, accessKeyID, accessKeySecret, securityToken)
		p.clients.Store(clientKey, client)
	} else {
		client = clientValue.(rmq.MQClient)
	}

	producer = client.GetProducer(instanceID, topic)

	p.producers.Store(producerKey, producer)

	ret = producer

	return
}

func (p *HTTPRocketMQComponent) sendMessageToRMQ(endpoint, accessKeyID, accessKeySecret, securityToken, instanceID, topic string, msg rmq.PublishMessageRequest) (result *rmq.PublishMessageResponse, err error) {
	producer, err := p.getProducer(endpoint, accessKeyID, accessKeySecret, securityToken, instanceID, topic)

	if err != nil {
		err = errors.WithMessage(err, "get producer failed")
		return
	}

	sendResult, err := producer.PublishMessage(msg)
	if err != nil {
		err = fmt.Errorf("%w, endpoint: %s", err, endpoint)
		return
	}

	logrus.WithFields(
		logrus.Fields{
			"code":        sendResult.Code,
			"message":     sendResult.Message,
			"request_id":  sendResult.RequestId,
			"host_id":     sendResult.HostId,
			"instance_id": producer.InstanceId(),
			"component":   "httprmq",
			"alias":       p.alias,
			"topic":       topic,
			"tags":        msg.MessageTag,
			"endpoint":    endpoint,
			"key":         msg.MessageKey,
		},
	).Debugln("Message sent")

	result = &sendResult

	return
}

func (p *HTTPRocketMQComponent) postMessage(resp rmq.ConsumeMessageEntry, ackFunc func()) (err error) {

	data, err := base64.StdEncoding.DecodeString(resp.MessageBody)
	if err != nil {
		ackFunc()
		err = errors.WithMessagef(err, "host_id: %s ,message_id: %s", resp.HostId, resp.MessageId)
		return
	}

	payload := &protocol.Payload{}
	err = protocol.Unmarshal(data, payload)

	if err != nil {
		ackFunc()
		err = errors.WithMessagef(err, "host_id: %s ,message_id: %s", resp.HostId, resp.MessageId)
		return
	}

	graph, exist := payload.GetGraph(payload.GetCurrentGraph())

	if !exist {
		ackFunc()
		err = fmt.Errorf("could not get graph of %s in HTTPRocketMQComponent.postMessage, host_id: %s ,message_id: %s", payload.GetCurrentGraph(), resp.HostId, resp.MessageId)
		return
	}

	graph.MoveForward()

	port, err := graph.CurrentPort()
	if err != nil {
		ackFunc()
		return
	}

	fromUrl := ""
	prePort, preErr := graph.PrevPort()
	if preErr == nil {
		fromUrl = prePort.GetUrl()
	}

	session := mail.NewSession()

	session.WithPayload(payload)
	session.WithFromTo(fromUrl, port.GetUrl())

	fbp.SessionWithPort(session, graph.GetName(), port.GetUrl(), port.GetMetadata())

	err = p.opts.Postman.Post(
		message.NewUserMessage(session),
	)

	if err != nil {
		err = errors.WithMessagef(err, "host_id: %s ,message_id: %s", resp.HostId, resp.MessageId)
		return
	}

	ackFunc()

	return
}

func (p *HTTPRocketMQComponent) Document() doc.Document {

	document := doc.Document{
		Title:       "HTTP RocketMQ Sender And Receiver",
		Description: "we could receive queue message from rocketmq and send message to rocketmq with http protocol",
	}

	return document
}
