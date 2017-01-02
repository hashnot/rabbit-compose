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

type Binding struct {
	Key    string     `yaml:"key"`
	Source string     `yaml:"source"`
	Args   amqp.Table `yaml:"args"`
}

type AmqpObject interface {
	Declare() error
	Bind() error
	Unbind() error
	Delete() error
}

func (d *Deployment) DeclareWithRecover(o AmqpObject) error {
	err := o.Declare()

	log.Print("Error while creating queue", err, "Trying to recover")
	switch err.(type) {
	case *amqp.Error:
		aerr := err.(*amqp.Error)
		log.Printf("recover: %v", aerr.Recover)
		switch aerr.Code {
		case 406:
			if err := d.newChannel(); err != nil {
				log.Print("Error while reopening channel: ", err)
				return err
			}

			if err := o.Delete(); err != nil {
				log.Print("Error while deleting old queue: ", err)
				return err
			}
			if err := o.Declare(); err != nil {
				log.Print("Error while recreating queue: ", err)
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
