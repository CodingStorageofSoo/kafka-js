require("dotenv").config();
const { TOPIC } = process.env;

const { Kafka, logLevel } = require("kafkajs");

// Replace 'localhost:9092' with your Kafka broker address
const kafka = new Kafka({
  clientId: "my-kafka-app",
  brokers: [`localhost:${KAFKA_PORT}`],
});

// Create the second consumer instance with logLevel set to "WARN"
const consumer2 = kafka.consumer({
  groupId: "consumer-group-2",
  logLevel: logLevel.WARN,
});

const consumeMessage = async (consumer) => {
  try {
    // Connect to the Kafka broker
    await consumer.connect();

    // Subscribe to the Kafka topic
    await consumer.subscribe({ topic: TOPIC });

    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ message }) => {
        const data = JSON.parse(message.value.toString());
        console.log(data);
      },
    });
  } catch (error) {
    console.error("Error consuming message:", error);
  }
};

// Start consuming messages for both consumers
consumeMessage(consumer2);
