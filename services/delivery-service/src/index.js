const express = require("express");
const { Pool } = require("pg");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

// PostgreSQL connection
const pool = new Pool({
  host: "localhost",
  port: 5433,
  database: "orders_db",
  user: "admin",
  password: "password",
});

// Kafka setup
const kafka = new Kafka({
  clientId: "delivery-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "delivery-group" });
const producer = kafka.producer();

const initDB = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS deliveries (
        id SERIAL PRIMARY KEY,
        order_id INTEGER UNIQUE NOT NULL,
        driver_id INTEGER,
        driver_name VARCHAR(255),
        restaurant_id VARCHAR(255),
        restaurant_name VARCHAR(255),
        delivery_address TEXT,
        status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
        assigned_at TIMESTAMP,
        picked_up_at TIMESTAMP,
        delivered_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    console.log("Delivery database initialized");
  } catch (error) {
    console.error("Database initialization failed:", error);
  }
};

// Start Kafka consumer
const startConsumer = async () => {
  try {
    await consumer.connect();
    await producer.connect();
    console.log("Kafka consumer connected");

    await consumer.subscribe({ topic: "order-events", fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());

        console.log(`Received event: ${event.eventType}`);

        // create delivery when kitchen marks order as READY,
        if (
          event.eventType === "ORDER_STATUS_UPDATED" &&
          event.status === "READY"
        ) {
          console.log(`Order ${event.orderId} is ready for delivery!`);

          try {
            // Get order details
            const orderResult = await pool.query(
              "SELECT * FROM orders WHERE id = $1",
              [event.orderId]
            );

            if (orderResult.rows.length > 0) {
              const order = orderResult.rows[0];

              // Create delivery record
              await pool.query(
                `INSERT INTO deliveries (order_id, restaurant_id, restaurant_name, delivery_address, status)
                 VALUES ($1, $2, $3, $4, 'PENDING')
                 ON CONFLICT (order_id) DO NOTHING`,
                [
                  event.orderId,
                  order.restaurant_id,
                  order.restaurant_name,
                  order.delivery_address,
                ]
              );

              console.log(`Delivery created for order ${event.orderId}`);

              // Publish delivery event created
              await producer.send({
                topic: "order-events",
                messages: [
                  {
                    key: event.orderId.toString(),
                    value: JSON.stringify({
                      eventType: "DELIVERY_CREATED",
                      orderId: event.orderId,
                      timestamp: new Date().toISOString(),
                    }),
                  },
                ],
              });
            }
          } catch (error) {
            console.error("Failed to create delivery:", error);
          }
        }

        // Listen for delivery status updates from other instances
        if (event.eventType === "DELIVERY_STATUS_UPDATED") {
          console.log(`Delivery ${event.orderId} status: ${event.status}`);
        }
      },
    });
  } catch (error) {
    console.error("Kafka consumer error:", error);
  }
};

// Initialize
initDB().then(() => startConsumer());

// HealthCheck Route
app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "healthy", service: "delivery-service" });
  } catch (error) {
    res.status(503).json({ status: "unhealthy", error: error.message });
  }
});

// TODO : Get all available deliveries for drivers
// route : /deliveries/available

// TODO : Get deliveries assigned to a driver
// route :/deliveries/driver/:driverId

// TODO : Get specific delivery
// route : /deliveries/:id

process.on("SIGTERM", async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

const PORT = process.env.PORT || 3005;
app.listen(PORT, () => {
  console.log(`Delivery Service running on port ${PORT}`);
});
