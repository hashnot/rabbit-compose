package rabbit

import (
	"github.com/streadway/amqp"
	"log"
)

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
		q.Name = queue.Name
	}
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
