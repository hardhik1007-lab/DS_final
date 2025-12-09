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

// Kafka Setup
const kafka = new Kafka({
  clientId: "kitchen-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "kitchen-group" });
const producer = kafka.producer();

const initDB = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS kitchen_orders (
        id SERIAL PRIMARY KEY,
        order_id INTEGER UNIQUE NOT NULL,
        restaurant_id VARCHAR(255) NOT NULL,
        restaurant_name VARCHAR(255),
        items JSONB NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
        received_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    console.log("Kitchen database initialized");
  } catch (error) {
    console.error("Database initialization failed:", error);
  }
};

// Kafka consumer start
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

        if (event.eventType === "ORDER_PLACED") {
          console.log(`New order ${event.orderId} for ${event.restaurantName}`);

          try {
            // Store in kitchen database
            await pool.query(
              `INSERT INTO kitchen_orders (order_id, restaurant_id, restaurant_name, items, status)
               VALUES ($1, $2, $3, $4, 'PENDING')
               ON CONFLICT (order_id) DO NOTHING`,
              [
                event.orderId,
                event.restaurantId,
                event.restaurantName,
                JSON.stringify(event.items),
              ]
            );

            console.log(`Order ${event.orderId} added to kitchen queue`);
          } catch (error) {
            console.error("Failed to store kitchen order:", error);
          }
        }
      },
    });
  } catch (error) {
    console.error("Kafka consumer error:", error);
  }
};

// Initialise
initDB().then(() => startConsumer());

// HealthCheck Route
app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "healthy", service: "kitchen-service" });
  } catch (error) {
    res.status(503).json({ status: "unhealthy", error: error.message });
  }
});

// Get Pending orders for a restaurant
app.get("/kitchen/orders/:restaurantId", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM kitchen_orders 
       WHERE restaurant_id = $1 AND status != 'COMPLETED'
       ORDER BY received_at DESC`,
      [req.params.restaurantId]
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch orders" });
  }
});
// Get all pending orders trial
app.get("/kitchen/orders", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM kitchen_orders 
       WHERE status != 'COMPLETED'
       ORDER BY received_at DESC`
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch orders" });
  }
});
//update order status
app.patch("/kitchen/orders/:id/status", async (req, res) => {
  try {
    const { status } = req.body; // 'ACCEPTED', 'COOKING', 'READY', 'COMPLETED'

    if (!["ACCEPTED", "COOKING", "READY", "COMPLETED"].includes(status)) {
      return res.status(400).json({ error: "Invalid status" });
    }

    const result = await pool.query(
      `UPDATE kitchen_orders 
       SET status = $1, updated_at = NOW()
       WHERE id = $2 
       RETURNING *`,
      [status, req.params.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Order not found" });
    }

    const order = result.rows[0];

    // Publish status update event
    await producer.send({
      topic: "order-events",
      messages: [
        {
          key: order.order_id.toString(),
          value: JSON.stringify({
            eventType: "ORDER_STATUS_UPDATED",
            orderId: order.order_id,
            kitchenOrderId: order.id,
            status: status,
            restaurantId: order.restaurant_id,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });

    console.log(
      `Status update published: Order ${order.order_id} is now ${status}`
    );

    // update the main orders table
    await pool.query(
      `UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2`,
      [status, order.order_id]
    );

    res.json({
      message: "Order status updated",
      order: order,
    });
  } catch (error) {
    console.error("Failed to update status:", error);
    res.status(500).json({ error: "Failed to update order status" });
  }
});

process.on("SIGTERM", async () => {
  await consumer.disconnect();
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => {
  console.log(`Kitchen Service running on port ${PORT}`);
});
