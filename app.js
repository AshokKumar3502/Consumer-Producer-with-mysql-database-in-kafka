const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');
const mysql = require('mysql');

const app = express();
const port = 3000;

// Configure Kafka

const kafka = new Kafka({
    clientId: 'my-rest-api',
    brokers: ['localhost:9092'],
  });
  
  const connection_kafka = kafka.producer();
  
  connection_kafka.connect()
    .then(() => {
      console.log('Connected to Kafka');
    })
    .catch((error) => {
      console.error('Error connecting to Kafka:', error);
    });
  

/// PRODCUCER

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

// Middleware to parse JSON requests
app.use(express.json());



// REST API endpoint to produce messages to Kafka
app.post('/produce', async (req, res) => {
  const { message } = req.body;

  try {
    // Connect to Kafka
    await producer.connect();

    // Produce message to Kafka topic 'my-first-topic'
    await producer.send({
      topic: 'my-first-topic',
      messages: [{ value: message }],
    });

    console.log(`Produced message: ${message}`);
    res.status(200).json({ status: 'Success', message: 'Message produced to Kafka.' });
  } catch (error) {
    console.error(`Error producing message: ${error.message}`);
    res.status(500).json({ status: 'Error', message: 'Internal Server Error' });
  } finally {
    // Disconnect from Kafka
    await producer.disconnect();
  }
});



//  CONSUMER

// const runConsumer = async () => {
// const consumer = kafka.consumer({ groupId: 'test-group' })

// await consumer.connect()
// await consumer.subscribe({ topic: 'my-first-topic', fromBeginning: true })

// await consumer.run({
//   eachMessage: async ({ topic, partition, message }) => {
//     console.log({
//       value: message.value.toString(),
//     })
//   },
// })}
// runConsumer().catch((error) => {
//     console.error('Error in consumer:', error);
//   });


const consumer = kafka.consumer({ groupId: 'test-group' });

// MySQL database connection
const connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'password',
  database: 'kafka',
});

connection.connect();

consumer.connect().then(() => {
  consumer.subscribe({ topic: 'my-first-topic', fromBeginning: true });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value.toString();

      // Insert the data into MySQL
      connection.query('INSERT INTO my_data (value) VALUES (?)', [value], (error, results, fields) => {
        if (error) throw error;
        console.log(`Data inserted into MySQL: ${value}`);
      });
    },
  });
});

// Start the Express server

app.listen(port, () => {
  console.log(`REST API listening at http://localhost:${port}`);
});
