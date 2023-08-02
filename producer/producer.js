require("dotenv").config();
const { TOPIC, KAFKA_ID, KAFKA_BROKER, PRODUCE_PORT } = process.env;

const express = require("express");
const app = express();
app.use(express.json());

const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: KAFKA_ID,
  brokers: [KAFKA_BROKER],
});

const producer = kafka.producer();

app.post("/send-to-kafka", async (req, res) => {
  try {
    const message = req.body;

    console.log(message);

    await producer.connect();

    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(message) }],
    });

    console.log("Message sent to Kafka:", message);
    res
      .status(200)
      .json({ status: "success", message: "Message sent to Kafka" });
  } catch (error) {
    console.error("Error sending message to Kafka:", error);
    res
      .status(500)
      .json({ status: "error", message: "Failed to send message to Kafka" });
  } finally {
    producer.disconnect();
  }
});

// KAFKAJS_NO_PARTITIONER_WARNING=1 node producer.js
app.listen(PRODUCE_PORT, () => {
  console.log(`Server listening on port ${PRODUCE_PORT}`);
});
