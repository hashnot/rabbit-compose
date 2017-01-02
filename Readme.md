Rabbit-Compose
=

Declare AMQP exchanges, queues and bindings using yaml file

```yaml
url: amqp://guest:guest@localhost:5672/
queues:
 errors:
  name: myQueue
  durable: true
  args:
   comment: All errors detected by the function framework land here
exchanges:
 an-exchange:
  name: this exchange
bindings:
 source:
 destination:
 key:
 args:
exchangeBindigs: # RabbitMQ extension
```