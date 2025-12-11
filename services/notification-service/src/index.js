const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { Kafka } = require("kafkajs");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});
const kafka = new Kafka({
  clientId: "notification-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "notification-group" });

// Store active user connections { userId: socketId }
const userConnections = new Map();

// Store active order subscriptions { orderId: [socketIds] }
const orderSubscriptions = new Map();

io.on("connection", (socket) => {
  console.log(`Client connected: ${socket.id}`);

  // User register with their userId
  socket.on("register", (userId) => {
    userConnections.set(userId, socket.id);
    console.log(`User ${userId} registered with socket ${socket.id}`);

    socket.emit("registered", {
      message: "Successfully registered for notifications",
      userId: userId,
    });
  });

  // Subscribe to specific order updates
  socket.on("subscribe-order", (orderId) => {
    if (!orderSubscriptions.has(orderId)) {
      orderSubscriptions.set(orderId, []);
    }
    orderSubscriptions.get(orderId).push(socket.id);
    console.log(`Socket ${socket.id} subscribed to order ${orderId}`);

    socket.emit("subscribed", {
      message: `Subscribed to order ${orderId}`,
      orderId: orderId,
    });
  });

  // Unsubscribe from order
  socket.on("unsubscribe-order", (orderId) => {
    if (orderSubscriptions.has(orderId)) {
      const sockets = orderSubscriptions.get(orderId);
      const index = sockets.indexOf(socket.id);
      if (index > -1) {
        sockets.splice(index, 1);
      }
      if (sockets.length === 0) {
        orderSubscriptions.delete(orderId);
      }
    }
    console.log(`Socket ${socket.id} unsubscribed from order ${orderId}`);
  });

  // Handle disconnect
  socket.on("disconnect", () => {
    console.log(`Client disconnected: ${socket.id}`);

    // Remove from user connections
    for (let [userId, socketId] of userConnections) {
      if (socketId === socket.id) {
        userConnections.delete(userId);
        break;
      }
    }

    // Remove from order subscriptions
    for (let [orderId, sockets] of orderSubscriptions) {
      const index = sockets.indexOf(socket.id);
      if (index > -1) {
        sockets.splice(index, 1);
      }
      if (sockets.length === 0) {
        orderSubscriptions.delete(orderId);
      }
    }
  });
});

// Start Kafka consumer
const startConsumer = async () => {
  try {
    await consumer.connect();
    console.log("Kafka consumer connected");

    await consumer.subscribe({ topic: "order-events", fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const event = JSON.parse(message.value.toString());

        console.log(
          `Received event: ${event.eventType} for order ${event.orderId}`
        );

        // Notification payload
        const notification = {
          type: event.eventType,
          orderId: event.orderId,
          timestamp: event.timestamp,
          data: event,
        };

        // Send to clients subscribed to this specific order
        if (orderSubscriptions.has(event.orderId)) {
          const subscribedSockets = orderSubscriptions.get(event.orderId);
          subscribedSockets.forEach((socketId) => {
            io.to(socketId).emit("order-update", notification);
          });
          console.log(
            `Sent to ${subscribedSockets.length} subscribed client(s)`
          );
        }

        // Send to the user who placed the order , only connected
        if (event.userId && userConnections.has(event.userId)) {
          const socketId = userConnections.get(event.userId);
          io.to(socketId).emit("order-update", notification);
          console.log(`Sent to user ${event.userId}`);
        }

        // Broadcast important events to all connected clients
        if (
          [
            "ORDER_PLACED",
            "ORDER_STATUS_UPDATED",
            "DELIVERY_ASSIGNED",
            "DELIVERY_STATUS_UPDATED",
          ].includes(event.eventType)
        ) {
          io.emit("global-update", {
            type: "order-event",
            event: event.eventType,
            orderId: event.orderId,
          });
        }
      },
    });
  } catch (error) {
    console.error("Kafka consumer error:", error);
  }
};

startConsumer();

// Healthcheck Route
app.get("/health", (req, res) => {
  res.json({
    status: "healthy",
    service: "notification-service",
    connectedClients: io.engine.clientsCount,
    registeredUsers: userConnections.size,
    activeOrderSubscriptions: orderSubscriptions.size,
  });
});

// Get stats (for debugging)
app.get("/stats", (req, res) => {
  res.json({
    connectedClients: io.engine.clientsCount,
    registeredUsers: Array.from(userConnections.keys()),
    orderSubscriptions: Array.from(orderSubscriptions.keys()),
  });
});

process.on("SIGTERM", async () => {
  await consumer.disconnect();
  io.close();
  process.exit(0);
});

const PORT = process.env.PORT || 3006;
server.listen(PORT, () => {
  console.log(`🚀 Notification Service running on port ${PORT}`);
});
