require("dotenv").config();
const { TOPIC, KAFKA_ID, KAFKA_BROKER } = process.env;

const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: KAFKA_ID,
  brokers: [KAFKA_BROKER],
});

// Create the first consumer instance with logLevel set to "WARN"
const consumer1 = kafka.consumer({
  groupId: "consumer-group-1",
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
consumeMessage(consumer1);
