package rabbit

import (
	"github.com/streadway/amqp"
	"log"
)

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool `yaml:"autoDelete"`
	Internal   bool
	Args       amqp.Table
	Bindings   map[string]Binding

	deployment *Deployment
}

func (x *Exchange) initTree(name string, d *Deployment) error {
	x.deployment = d

	if x.Name == "" {
		x.Name = name
	}

	return nil
}

func (x *Exchange) Declare() error {
	return x.deployment.channel.ExchangeDeclare(x.Name, x.Kind, x.Durable, x.AutoDelete, x.Internal, false, x.Args)
}

func (x *Exchange) Bind() error {
	for name, b := range x.Bindings {
		if err := x.SetupBinding(name, &b); err != nil {
			return err
		}
	}
	return nil
}

func (x *Exchange) SetupBinding(name string, b *Binding) error {
	src := defName(b.Source, name)
	srcName := x.deployment.Exchanges[src].Name
	log.Print("Bind exchange ", srcName, " to exchange ", x.Name)
	return x.deployment.channel.ExchangeBind(x.Name, b.Key, srcName, false, b.Args)
}

func (x *Exchange) Unbind() error {
	log.Print("Exchange.Unbind not implemented")
	return nil
}

func (x *Exchange) Delete() error {
	return x.deployment.channel.ExchangeDelete(x.Name, true, false)
}
