const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '.env') })

const redis = require('redis');

const REDIS_PORT = process.env.PORT || 6379;

const client = redis.createClient(REDIS_PORT);

let amqp = require('amqplib/callback_api');
// TODO: Декомпозировать все очереди
amqp.connect('amqp://localhost:5672', function(error0, connection) {
  if (error0) {
    throw error0;
  }
  connection.createChannel((error1, channel) => {
    if (error1) {
      throw error1;
    }

    // Check JWT 
    let queue_check_jwt = 'rpc_check_jwt';

    channel.assertQueue(queue_check_jwt, {
      durable: false
    });
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests');
    channel.consume(queue_check_jwt, async function reply(msg) {
      
      const {login} = JSON.parse(msg.content)

      answer = 0

      burfferToken = await new Promise((res, rej) => {
        client.get(login, (err, reply) => {
          res(reply)
        })
      })
      
      if(burfferToken) {
        answer = 1
      }
      
      channel.sendToQueue(msg.properties.replyTo,
        Buffer.alloc(1, answer), {
          correlationId: msg.properties.correlationId
        });

      channel.ack(msg);
    });

    // Create JWT
    let queue_create_jwt = 'rpc_create_jwt';

    channel.assertQueue(queue_create_jwt, {
      durable: false
    });
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests');
    channel.consume(queue_create_jwt, async function reply(msg) {
      
      const {login, token} = JSON.parse(msg.content)

      amswer = client.setex(login, 7200, token) ? 1 : 0
      
      channel.sendToQueue(msg.properties.replyTo,
        Buffer.alloc(1, answer), {
          correlationId: msg.properties.correlationId
        });

      channel.ack(msg);
    });

    // Check URL
    let queue_check_url = 'rpc_check_url';

    channel.assertQueue(queue_check_url, {
      durable: false
    });
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests');
    channel.consume(queue_check_url, async function reply(msg) {
      
      const {login, url} = JSON.parse(msg.content)

      amswer = 1

      burfferUrl = await new Promise((res, rej) => {
        client.get(`${login}_url`, (err, reply) => {
          res(reply)
        })
      })

      if (!burfferUrl) {
        amswer = 0
      }

      if (burfferUrl != url) {
        answer != 0
      }
      
      channel.sendToQueue(msg.properties.replyTo,
        Buffer.alloc(1, answer), {
          correlationId: msg.properties.correlationId
        });

      channel.ack(msg);
    });

    // Create URL
    let queue_create_url = 'rpc_create_url';

    channel.assertQueue(queue_create_url, {
      durable: false
    });
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests');
    channel.consume(queue_create_url, async function reply(msg) {
      
      const {login, token} = JSON.parse(msg.content)

      amswer = client.setex(`${login}_url`, 7200, token) ? 1 : 0
      
      channel.sendToQueue(msg.properties.replyTo,
        Buffer.alloc(1, answer), {
          correlationId: msg.properties.correlationId
        });

      channel.ack(msg);
    });

  });
});
