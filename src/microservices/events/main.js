import express from 'express';
import { Kafka } from 'kafkajs';

const app = express();
const port = Number(process.env.PORT ?? 8082);

const kafka = new Kafka({
  clientId: 'cinemaabyss-events',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map(broker => broker.trim()),
})

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'cinemaabyss-events' });

const checkNumberArg = (arg) => !Number.isInteger(arg) || arg <= 0;
const checkEmptyStringArg = (arg) => typeof arg !== 'string' || arg.trim() === ''

const publishEvent = async (topic, key, event, res) => {
  try {
    await producer.send({
      topic,
      messages: [
        {
          key: String(key),
          value: JSON.stringify(event),
        },
      ],
    });

    return res.status(201).json({ status: 'success' });
  } catch (error) {
    console.error(`Ошибка отправки в ${topic}:`, error);

    return res.status(503).json({
      error: 'Не удалось отправить событие в Kafka',
    });
  }
};

app.use(express.json({ limit: '64kb' }));

app.get('/api/events/health', (req, res) => {
  res.json({ status: true });
});

app.post('/api/events/movie', async (req, res) => {
  const event = req.body;

  if (checkNumberArg(event?.movie_id)) {
    return res.status(400).json({
      error: 'Нужен положительный movie_id',
    });
  }

  if (checkNumberArg(event?.user_id)) {
    return res.status(400).json({
      error: 'Нужен положительный user_id',
    });
  }

  if (checkEmptyStringArg(event?.action)) {
    return res.status(400).json({
      error: 'Нужен непустой action',
    });
  }

  return publishEvent('movie-events', event.movie_id, event, res);
})

app.post('/api/events/user', async (req, res) => {
  const event = req.body;

  if (checkNumberArg(event?.user_id)) {
    return res.status(400).json({
      error: 'Нужен положительный user_id',
    });
  }

  if (checkEmptyStringArg(event?.action)) {
    return res.status(400).json({
      error: 'Нужен непустой action',
    });
  }

  if (checkEmptyStringArg(event?.username)) {
    return res.status(400).json({
      error: 'Нужен непустой username',
    });
  }

  return publishEvent('user-events', event.user_id, event, res)
})

app.post('/api/events/payment', async (req, res) => {
  const event = req.body;

  if (checkNumberArg(event?.payment_id)) {
    return res.status(400).json({
      error: 'Нужен положительный payment_id',
    });
  }

  if (checkNumberArg(event?.user_id)) {
    return res.status(400).json({
      error: 'Нужен положительный user_id',
    });
  }

  if (checkEmptyStringArg(event?.status)) {
    return res.status(400).json({
      error: 'Нужен непустой status платежа',
    });
  }

  if (!Number.isFinite(event?.amount) || event.amount <= 0) {
    return res.status(400).json({
      error: 'Нужна положительная сумма платежа',
    });
  }

  return publishEvent('payment-events', event.payment_id, event, res)
})

try {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topics:  ['movie-events', 'user-events', 'payment-events'], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message}) => {
      console.log('Получено событие:', {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
        value: message.value?.toString(),
      })
    }
  })

  app.listen(port, () => {
    console.log(`Events service listening on port ${port}`);
  })
} catch (error) {
  console.error('Ошибка при подключении к Kafka:', error);
  process.exit(1);
}

const shutdown = async () => {
  try {
    await producer.disconnect();
    await consumer.disconnect();
    process.exit(0);
  } catch (error) {
    console.error('Ошибка при отключении от Kafka:', error);
    process.exit(1);
  }
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);