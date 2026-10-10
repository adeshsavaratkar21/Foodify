const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// MySQL Connection Pool (Change user & password to match your MySQL setup)
const db = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'Adesh@123',
    database: 'distributed_food_db',
    waitForConnections: true,
    connectionLimit: 10
});

let logicalClock = 0;
let masterOnline = true;

function logTransaction(type, node, details) {
    const sql = 'INSERT INTO transaction_audit_log (operation_type, node_executed, details) VALUES (?, ?, ?)';
    db.query(sql, [type, node, details]);
}

// Exp 1 & 2: RPC Place Order
app.post('/api/orders/place', (req, res) => {
    const { customerName, items, billAmount } = req.body;
    logicalClock += 1;
    const orderId = 'ORD-' + Math.floor(1000 + Math.random() * 9000);

    const sql = 'INSERT INTO orders (order_id, customer_name, items, bill_amount, status, logical_timestamp) VALUES (?, ?, ?, ?, ?, ?)';
    db.query(sql, [orderId, customerName || 'Om Raval', items || 'Margherita Pizza', billAmount || 350, 'COOKING', logicalClock], (err) => {
        if (err) return res.status(500).json({ error: err.message });

        logTransaction('WRITE', masterOnline ? 'Master-Node' : 'Promoted-Replica', `Placed ${orderId} by ${customerName}`);

        setTimeout(() => {
            db.query('UPDATE orders SET status = "READY" WHERE order_id = ?', [orderId]);
        }, 3000);

        res.json({ success: true, orderId, logicalTimestamp: logicalClock, status: 'COOKING' });
    });
});

// Exp 3: Berkeley Clock Sync
app.get('/api/clocks/sync', (req, res) => {
    const masterTime = Date.now();
    const node1Time = masterTime + 2400;
    const node2Time = masterTime - 1800;
    const averageTime = Math.round((masterTime + node1Time + node2Time) / 3);

    const offsets = {
        masterOffset: (averageTime - masterTime) / 1000,
        node1Offset: (averageTime - node1Time) / 1000,
        node2Offset: (averageTime - node2Time) / 1000,
        synchronizedTime: new Date(averageTime).toTimeString().split(' ')[0]
    };

    logTransaction('SYNC', 'Master-Coordinator', `Berkeley sync: ${offsets.synchronizedTime}`);
    res.json(offsets);
});

// Exp 4: Bully Leader Election
app.post('/api/cluster/elect', (req, res) => {
    const { failNodeId } = req.body;

    if (failNodeId) {
        db.query('UPDATE cluster_nodes SET status = "OFFLINE" WHERE node_id = ?', [failNodeId], () => {
            db.query('SELECT * FROM cluster_nodes WHERE status = "ONLINE" ORDER BY node_id DESC LIMIT 1', (err, rows) => {
                if (err || rows.length === 0) return res.status(500).json({ error: 'No active nodes available' });

                const newLeader = rows[0];
                db.query('UPDATE cluster_nodes SET role = "LEADER" WHERE node_id = ?', [newLeader.node_id]);
                db.query('UPDATE cluster_nodes SET role = "FOLLOWER" WHERE node_id != ? AND status = "ONLINE"', [newLeader.node_id]);

                logTransaction('ELECTION', newLeader.node_name, `Node ${newLeader.node_id} elected Leader`);
                res.json({ message: `Node ${newLeader.node_id} is new Leader`, leader: newLeader });
            });
        });
    } else {
        db.query('UPDATE cluster_nodes SET status = "ONLINE", role = "FOLLOWER" WHERE node_id != 3');
        db.query('UPDATE cluster_nodes SET status = "ONLINE", role = "LEADER" WHERE node_id = 3', () => {
            res.json({ message: 'Cluster reset. Node 3 restored as Leader.' });
        });
    }
});

// Exp 5: Audit Log
app.get('/api/audit-logs', (req, res) => {
    db.query('SELECT * FROM transaction_audit_log ORDER BY log_id DESC LIMIT 10', (err, rows) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(rows);
    });
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Foodify Distributed Systems Backend listening on http://localhost:${PORT}`);
});