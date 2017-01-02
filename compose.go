package main

import (
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

func main() {
	configData, err := ioutil.ReadFile("rabbit-compose.yaml")
	failOnError(err, "Error reading config")

	config := new(Deployment)

	err = yaml.Unmarshal(configData, config)
	failOnError(err, "Error parsing config")

	err = config.Setup()
	failOnError(err, "Error applying config")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type Deployment struct {
	Url              string                     `yaml:"url"`
	Exchanges        map[string]Exchange        `yaml:"exchanges"`
	Queues           map[string]Queue           `yaml:"queues"`
	Bindings         map[string]QueueBinding    `yaml:"bindings"`
	ExchangeBindings map[string]ExchangeBinding `yaml:"exchangeBindings"`

	connection *amqp.Connection
	channel    *amqp.Channel
}

func (d *Deployment) Setup() error {
	conn, err := amqp.Dial(d.Url)
	if err != nil {
		return err
	}
	d.connection = conn

	ch, err := conn.Channel()
	if err != nil {
		return nil
	}
	d.channel = ch

	for name, x := range d.Exchanges {
		err := x.Setup(name, d)
		if err != nil {
			return err
		}
	}

	for name, x := range d.Queues {
		err := x.Setup(name, d)
		if err != nil {
			return err
		}
	}

	for _, x := range d.Bindings {
		err := x.Setup(d)
		if err != nil {
			return err
		}
	}

	for _, x := range d.ExchangeBindings {
		err := x.Setup(d)
		if err != nil {
			return err
		}
	}

	return nil
}

func defName(object, key string) string {
	if object != "" {
		return object
	}
	return key
}

type Exchange struct {
	Name       string     `yaml:"name"`
	Kind       string     `yaml:"kind"`
	Durable    bool       `yaml:"durable"`
	AutoDelete bool       `yaml:"autoDelete"`
	Internal   bool       `yaml:"internal"`
	Args       amqp.Table `yaml:"args"`
}

func (x *Exchange) Setup(name string, d *Deployment) error {
	return d.channel.ExchangeDeclare(defName(x.Name, name), x.Kind, x.Durable, x.AutoDelete, x.Internal, false, x.Args)
}

/*
func(x *Exchange) Teardown(ch *amqp.Channel) error{
	ch.ExchangeDelete(x.Name,true,x.NoWait)
}
*/

type QueueBinding struct {
	Destination string     `yaml:"destination"`
	Key         string     `yaml:"key"`
	Source      string     `yaml:"source"`
	Args        amqp.Table `yaml:"args"`
}

func (b *QueueBinding) Setup(d *Deployment) error {
	return d.channel.ExchangeBind(b.Destination, b.Key, b.Source, false, b.Args)
}

type ExchangeBinding QueueBinding

func (b *ExchangeBinding) Setup(d *Deployment) error {
	return d.channel.ExchangeBind(b.Destination, b.Key, b.Source, false, b.Args)
}

type Queue struct {
	Name       string     `yaml:"name"`
	Durable    bool       `yaml:"durable"`
	AutoDelete bool       `yaml:"autoDelete"`
	Args       amqp.Table `yaml:"args"`
}

func (q *Queue) Setup(name string, d *Deployment) error {
	name = defName(q.Name, name)
	log.Print("Declare queue ", name)

	err := q.setup(name, d)

	switch err.(type) {
	case *amqp.Error:
		aerr := err.(*amqp.Error)
		log.Printf("recover: %v", aerr.Recover)
		switch aerr.Code {
		case 406:
			channel, erropen := d.connection.Channel()
			if erropen != nil {
				log.Print("Error while creating queue ", err)
				log.Print("Error while reopening channel: ", erropen)
				return err
			}
			d.channel = channel
			errdel := q.del(name, d)
			if errdel != nil {
				log.Print("Error while creating queue ", err)
				log.Print("Error while deleting old queue: ", errdel)
				return err
			}
			err2 := q.setup(name, d)
			if err2 != nil {
				log.Print("Error while creating queue ", err)
				log.Print("Error while recreating queue: ", err2)
			}
			return nil
		default:
			return err
		}
	default:
		return err
	}

}

func (q *Queue) setup(name string, d *Deployment) error {
	queue, err := d.channel.QueueDeclare(name, q.Durable, q.AutoDelete, false, false, q.Args)
	if err == nil {
		log.Printf("%s consumers: %d messages: %d", queue.Name, queue.Consumers, queue.Messages)
	}
	return err
}

func (q *Queue) del(name string, d *Deployment) error {
	_, err := d.channel.QueueDelete(name, true, true, false)
	return err
}
