package rabbit

import (
	"github.com/streadway/amqp"
	"log"
)

type Deployment struct {
	Url       string               `yaml:"url"`
	Exchanges map[string]*Exchange `yaml:"exchanges"`
	Queues    map[string]*Queue    `yaml:"queues"`

	connection *amqp.Connection
	channel    *amqp.Channel
}

func (d *Deployment) Setup() error {
	if err := d.initTree(); err != nil {
		return err
	}

	if err := d.dial(); err != nil {
		return err
	}

	objects := make([]AmqpObject, 0, len(d.Exchanges)+len(d.Queues))

	for _, x := range d.Exchanges {
		objects = append(objects, x)
	}

	for _, x := range d.Queues {
		objects = append(objects, x)
	}

	for _, o := range objects {
		err := d.DeclareWithRecover(o)
		if err != nil {
			return err
		}
	}

	for _, o := range objects {
		err := o.Bind()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Deployment) dial() error {
	conn, err := amqp.Dial(d.Url)
	if err != nil {
		return err
	}
	d.connection = conn
	err = d.newChannel()
	return err
}

func (d *Deployment) newChannel() error {
	ch, err := d.connection.Channel()
	if err != nil {
		return err
	}
	d.channel = ch
	return nil
}

func (d *Deployment) initTree() error {
	for name, x := range d.Exchanges {
		err := x.initTree(name, d)
		if err != nil {
			return err
		}
	}

	for name, x := range d.Queues {
		err := x.initTree(name, d)
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

	deplyment *Deployment
}

func (x *Exchange) initTree(name string, d *Deployment) error {
	x.deplyment = d

	if x.Name == "" {
		x.Name = name
	}

	return nil
}

func (x *Exchange) Declare() error {
	return x.deplyment.channel.ExchangeDeclare(x.Name, x.Kind, x.Durable, x.AutoDelete, x.Internal, false, x.Args)
}

func (x *Exchange) Bind() error {
	for name, b := range x.Bindings {
		if err := x.SetupBinding(name, b); err != nil {
			return err
		}
	}
	return nil
}

func (x *Exchange) SetupBinding(name string, b *Binding) error {
	src := defName(b.Source, name)
	srcName := x.deplyment.Exchanges[src].Name
	return x.deplyment.channel.ExchangeBind(x.Name, b.Key, srcName, false, b.Args)
}

func (x *Exchange) Unbind() error {
	log.Print("Exchange.Unbind not implemented")
	return nil
}

func (x *Exchange) Delete() error {
	return x.deplyment.channel.ExchangeDelete(x.Name, true, false)
}

type Binding struct {
	Key    string     `yaml:"key"`
	Source string     `yaml:"source"`
	Args   amqp.Table `yaml:"args"`
}

type Queue struct {
	Name       string              `yaml:"name"`
	Durable    bool                `yaml:"durable"`
	AutoDelete bool                `yaml:"autoDelete"`
	Args       amqp.Table          `yaml:"args"`
	Bindings   map[string]*Binding `yaml:"bindings"`

	deployment *Deployment
}

func (q *Queue) initTree(name string, d *Deployment) error {
	q.deployment = d
	if q.Name == "" {
		q.Name = name
	}
	return nil
}

func (q *Queue) Declare() error {
	queue, err := q.deployment.channel.QueueDeclare(q.Name, q.Durable, q.AutoDelete, false, false, q.Args)
	if err == nil {
		log.Printf("%s consumers: %d messages: %d", queue.Name, queue.Consumers, queue.Messages)
	}
	q.Name = queue.Name
	return err
}

func (q *Queue) Bind() error {
	for name, b := range q.Bindings {
		if err := q.SetupBinding(name, b); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) SetupBinding(name string, b *Binding) error {
	return q.deployment.channel.QueueBind(defName(q.Name, name), b.Key, b.Source, false, b.Args)
}

func (q *Queue) Unbind() error {
	log.Print("Queue.Unbind not implemented")
	return nil
}

func (q *Queue) Delete() error {
	_, err := q.deployment.channel.QueueDelete(q.Name, true, true, false)
	return err
}

type AmqpObject interface {
	Declare() error
	Bind() error
	Unbind() error
	Delete() error
}

func (d *Deployment) DeclareWithRecover(o AmqpObject) error {
	err := o.Declare()

	switch err.(type) {
	case *amqp.Error:
		aerr := err.(*amqp.Error)
		log.Printf("recover: %v", aerr.Recover)
		switch aerr.Code {
		case 406:
			erropen := d.newChannel()
			if erropen != nil {
				log.Print("Error while creating queue ", err)
				log.Print("Error while reopening channel: ", erropen)
				return err
			}

			errdel := o.Delete()
			if errdel != nil {
				log.Print("Error while creating queue ", err)
				log.Print("Error while deleting old queue: ", errdel)
				return err
			}
			err2 := o.Declare()
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

	return nil
}
