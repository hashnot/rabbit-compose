package rabbit

import (
	"github.com/streadway/amqp"
	"log"
)

type Deployment struct {
	Url        string              `yaml:"url"`
	Exchanges  map[string]Exchange `yaml:"exchanges"`
	Queues     map[string]Queue    `yaml:"queues"`

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

	for _, x := range d.Exchanges {
		err := x.SetupBindings()
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

	return nil
}

func defName(value, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}

type Exchange struct {
	Name       string              `yaml:"name"`
	Kind       string              `yaml:"kind"`
	Durable    bool                `yaml:"durable"`
	AutoDelete bool                `yaml:"autoDelete"`
	Internal   bool                `yaml:"internal"`
	Args       amqp.Table          `yaml:"args"`
	Bindings   map[string]*Binding `yaml:"bindings"`

	deplyment  *Deployment
}

func (x *Exchange) Setup(name string, d *Deployment) error {
	x.deplyment = d

	if x.Name == "" {
		x.Name = name
	}

	return d.channel.ExchangeDeclare(x.Name, x.Kind, x.Durable, x.AutoDelete, x.Internal, false, x.Args)
}

func (x *Exchange) SetupBindings() error {
	for name, b := range x.Bindings {
		if err := x.SetupBinding(name, b); err != nil {
			return err
		}
	}
	return nil
}

/*
func(x *Exchange) Teardown(ch *amqp.Channel) error{
	ch.ExchangeDelete(x.Name,true,x.NoWait)
}
*/

type Binding struct {
	Key    string     `yaml:"key"`
	Source string     `yaml:"source"`
	Args   amqp.Table `yaml:"args"`
}

func (q *Queue) SetupBinding(name string, b *Binding) error {
	return q.deployment.channel.QueueBind(defName(q.Name, name), b.Key, b.Source, false, b.Args)
}

func (x *Exchange) SetupBinding(name string, b *Binding) error {
	return x.deplyment.channel.ExchangeBind(defName(x.Name, name), b.Key, b.Source, false, b.Args)
}

type Queue struct {
	Name       string              `yaml:"name"`
	Durable    bool                `yaml:"durable"`
	AutoDelete bool                `yaml:"autoDelete"`
	Args       amqp.Table          `yaml:"args"`
	Bindings   map[string]*Binding `yaml:"bindings"`

	deployment *Deployment
}

func (q *Queue) Setup(name string, d *Deployment) error {
	q.deployment = d
	if q.Name == "" {
		q.Name = name
	}

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

	for name, b := range q.Bindings {
		if err := q.SetupBinding(name, b); err != nil {
			return err
		}
	}
	return nil
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
