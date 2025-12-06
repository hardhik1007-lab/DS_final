const express = require("express");
const { Pool } = require("pg");
const { Kafka } = require("kafkajs");

const app = express();
app.use(express.json());

// Postgres DB connection
const pool = new Pool({
  host: "localhost",
  port: 5433,
  database: "orders_db",
  user: "admin",
  password: "password",
});

// Kafka
const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

// Initialize db and kafka
const init = async () => {
  try {
    // Creating orders table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        restaurant_id VARCHAR(255) NOT NULL,
        restaurant_name VARCHAR(255),
        items JSONB NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'PLACED',
        delivery_address TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    console.log("Database initialized");

    // Connecting Kafka producer
    await producer.connect();
    console.log("Kafka producer connected");
  } catch (error) {
    console.error("Initialization failed:", error);
  }
};

init();

// Healthcheck route
app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ status: "healthy", service: "order-service" });
  } catch (error) {
    res.status(503).json({ status: "unhealthy", error: error.message });
  }
});

// Create order
app.post("/orders", async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      userId,
      restaurantId,
      restaurantName,
      items,
      totalAmount,
      deliveryAddress,
    } = req.body;

    if (
      !userId ||
      !restaurantId ||
      !items ||
      !totalAmount ||
      !deliveryAddress
    ) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    await client.query("BEGIN");

    // Insert order into database
    const result = await client.query(
      `INSERT INTO orders (user_id, restaurant_id, restaurant_name, items, total_amount, delivery_address, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'PLACED') RETURNING *`,
      [
        userId,
        restaurantId,
        restaurantName || "Unknown",
        JSON.stringify(items),
        totalAmount,
        deliveryAddress,
      ]
    );

    const order = result.rows[0];

    // Publish event to Kafka
    await producer.send({
      topic: "order-events",
      messages: [
        {
          key: order.id.toString(),
          value: JSON.stringify({
            eventType: "ORDER_PLACED",
            orderId: order.id,
            restaurantId: order.restaurant_id,
            restaurantName: order.restaurant_name,
            userId: order.user_id,
            items: order.items,
            totalAmount: parseFloat(order.total_amount),
            deliveryAddress: order.delivery_address,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });

    await client.query("COMMIT");

    console.log(`Order ${order.id} placed and event published`);

    res.status(201).json({
      message: "Order placed successfully",
      order: {
        id: order.id,
        status: order.status,
        totalAmount: order.total_amount,
        createdAt: order.created_at,
      },
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Order creation failed:", error);
    res.status(500).json({ error: "Order creation failed" });
  } finally {
    client.release();
  }
});

// Get order by ID
app.get("/orders/:id", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM orders WHERE id = $1", [
      req.params.id,
    ]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Order not found" });
    }

    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch order" });
  }
});

// Get orders by user
app.get("/orders/user/:userId", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at DESC",
      [req.params.userId]
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch orders" });
  }
});

// Update order status (internal use)
app.patch("/orders/:id/status", async (req, res) => {
  try {
    const { status } = req.body;

    const result = await pool.query(
      `UPDATE orders 
       SET status = $1, updated_at = NOW()
       WHERE id = $2 
       RETURNING *`,
      [status, req.params.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Order not found" });
    }

    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: "Failed to update order" });
  }
});

// shutdown
process.on("SIGTERM", async () => {
  await producer.disconnect();
  await pool.end();
  process.exit(0);
});

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => {
  console.log(`Order Service running on port ${PORT}`);
});
