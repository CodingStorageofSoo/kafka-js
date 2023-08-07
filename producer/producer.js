const express = require("express");
const app = express();
app.use(express.json());

const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-kafka-app",
  brokers: ["kafka:9092"],
});

const producer = kafka.producer();

app.get("/", (req, res) => {
  res.json({ succ: "connected" });
});

app.post("/send-to-kafka", async (req, res) => {
  try {
    const message = req.body;

    console.log(message);

    await producer.connect();

    await producer.send({
      topic: "topic_name",
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
app.listen(3000, () => {
  console.log(`Server listening on port 3000`);
});
