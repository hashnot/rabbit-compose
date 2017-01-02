package rabbit

import (
	"log"
	"github.com/streadway/amqp"
)

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
