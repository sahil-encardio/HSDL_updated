const mqtt = require('mqtt');
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { InfluxDB, Point } = require('@influxdata/influxdb-client');

// =============================
// CONFIGURACIÓN
// =============================
const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://localhost:1883';
const WS_PORT = parseInt(process.env.WS_PORT) || 8080;
const SEND_INTERVAL_MS = 100; // 10 Hz (envío cada 100 ms)

// InfluxDB Configuration
const INFLUX_URL = process.env.INFLUX_URL || 'http://localhost:8086';
const INFLUX_TOKEN = process.env.INFLUX_TOKEN || 'your-token-here';
const INFLUX_ORG = process.env.INFLUX_ORG || 'encardio';
const INFLUX_BUCKET = process.env.INFLUX_BUCKET || 'nexus';

// Initialize InfluxDB client
const influxDB = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const writeApi = influxDB.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 'ms', {
    batchSize: 100,
    flushInterval: 5000,
    maxRetries: 3,
    maxBufferLines: 10000
});
writeApi.useDefaultTags({ source: 'datalogger' });

console.log('✓ InfluxDB write API initialized');
console.log(`  URL: ${INFLUX_URL}`);
console.log(`  Org: ${INFLUX_ORG}`);
console.log(`  Bucket: ${INFLUX_BUCKET}`);

// Test InfluxDB connection
(async () => {
    try {
        const testQueryApi = influxDB.getQueryApi(INFLUX_ORG);
        const testQuery = `buckets() |> filter(fn: (r) => r.name == "${INFLUX_BUCKET}")`;
        let bucketFound = false;
        await testQueryApi.collectRows(testQuery, () => { bucketFound = true; });
        if (bucketFound) {
            console.log('✓ InfluxDB connection successful - bucket found');
        } else {
            console.log('⚠ InfluxDB connected but bucket not found');
        }
    } catch (err) {
        console.error('✗ InfluxDB connection test failed:', err.message);
        console.error('  Data will NOT be written to InfluxDB!');
    }
})();

// InfluxDB Write Statistics
let influxWriteStats = {
    totalPoints: 0,
    successCount: 0,
    errorCount: 0,
    lastError: null
};

// =============================
// MOCK DATA CONFIGURATION
// =============================
let mockData = [];
let mockPlaybackState = {
    isPlaying: false,
    currentIndex: 0,
    speed: 1.0,
    intervalId: null
};

console.log('=================================');
console.log('High Speed Datalogger Server');
console.log('=================================');
console.log('MQTT Broker:', MQTT_BROKER);
console.log('WebSocket Port:', WS_PORT);
console.log('Send Rate:', 1000/SEND_INTERVAL_MS, 'Hz');

// =============================
// SERVIDOR HTTP
// =============================
const server = http.createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.url === '/' || req.url === '/index.html') {
        try {
            const html = fs.readFileSync(path.join(__dirname, 'index.html'));
            res.writeHead(200, { 
                'Content-Type': 'text/html; charset=utf-8',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache',
                'Expires': '0'
            });
            res.end(html);
        } catch (err) {
            console.error('Error loading index.html:', err.message);
            res.writeHead(500);
            res.end('Error loading page');
        }
    } else if (req.url === '/logo.png') {
        try {
            const logo = fs.readFileSync(path.join(__dirname, 'logo.png'));
            res.writeHead(200, {
                'Content-Type': 'image/png',
                'Cache-Control': 'public, max-age=86400'
            });
            res.end(logo);
        } catch (err) {
            res.writeHead(404);
            res.end('Logo not found');
        }
    } else if (req.url === '/favicon.ico') {
        res.writeHead(204);
        res.end();
    } else if (req.url.startsWith('/api/query')) {
        // Handle InfluxDB query requests
        try {
            const url = new URL(req.url, `http://${req.headers.host}`);
            const startTime = url.searchParams.get('start');
            const endTime = url.searchParams.get('end');
            
            if (!startTime || !endTime) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Missing start or end time' }));
                return;
            }

            const queryApi = influxDB.getQueryApi(INFLUX_ORG);
            
            // Query for all 4 channels (without pivot - we'll group manually)
            const fluxQuery = `
                from(bucket: "${INFLUX_BUCKET}")
                    |> range(start: ${startTime}, stop: ${endTime})
                    |> filter(fn: (r) => r["_measurement"] == "sensor_data")
                    |> filter(fn: (r) => r["_field"] == "ch1" or r["_field"] == "ch2" or r["_field"] == "ch3" or r["_field"] == "ch4")
                    |> sort(columns: ["_time"])
            `;

            const rawData = [];
            
            try {
                await queryApi.collectRows(fluxQuery, (row, tableMeta) => {
                    // The row is an array, we need to use tableMeta to get column indices
                    const timeIndex = tableMeta.columns.findIndex(col => col.label === '_time');
                    const fieldIndex = tableMeta.columns.findIndex(col => col.label === '_field');
                    const valueIndex = tableMeta.columns.findIndex(col => col.label === '_value');
                    
                    if (rawData.length === 0) {
                        console.log('📊 Column indices - time:', timeIndex, 'field:', fieldIndex, 'value:', valueIndex);
                    }
                    
                    const time = row[timeIndex];
                    const field = row[fieldIndex];
                    const value = row[valueIndex];
                    
                    if (rawData.length < 3) {
                        console.log(`📊 Row ${rawData.length + 1}:`, {
                            time: time,
                            field: field,
                            value: value
                        });
                    }
                    
                    if (time && field !== undefined && value !== undefined) {
                        rawData.push({
                            time: new Date(time).getTime(),
                            field: field,
                            value: parseFloat(value)
                        });
                    }
                });
                
                console.log(`✓ Retrieved ${rawData.length} raw data points from InfluxDB`);
            } catch (queryError) {
                console.error('Query error:', queryError.message);
                console.error('Query:', fluxQuery);
                // Return empty array if query fails
            }

            // Group data by timestamp
            const dataMap = new Map();
            rawData.forEach(item => {
                if (!dataMap.has(item.time)) {
                    dataMap.set(item.time, {
                        timestamp: item.time,
                        ch1: null,
                        ch2: null,
                        ch3: null,
                        ch4: null
                    });
                }
                const entry = dataMap.get(item.time);
                entry[item.field] = item.value;
            });

            // Convert map to array
            const data = Array.from(dataMap.values()).sort((a, b) => a.timestamp - b.timestamp);
            console.log(`✓ Grouped into ${data.length} unique timestamps`);

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, data: data }));
            
            console.log(`✓ Queried ${data.length} records from ${startTime} to ${endTime}`);
            if (data.length > 0) {
                console.log('  Sample data:', data[0]);
            }
        } catch (err) {
            console.error('InfluxDB query error:', err.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
        }
    } else if (req.url === '/api/mock/load') {
        // Load mock data from CSV
        try {
            await loadMockData();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ 
                success: true, 
                totalRows: mockData.length,
                message: 'Mock data loaded successfully'
            }));
        } catch (err) {
            console.error('Error loading mock data:', err.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
        }
    } else if (req.url === '/api/mock/start') {
        // Start mock data playback
        startMockPlayback();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            success: true, 
            isPlaying: true,
            currentIndex: mockPlaybackState.currentIndex,
            speed: mockPlaybackState.speed
        }));
    } else if (req.url === '/api/mock/pause') {
        // Pause mock data playback
        pauseMockPlayback();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            success: true, 
            isPlaying: false,
            currentIndex: mockPlaybackState.currentIndex
        }));
    } else if (req.url === '/api/mock/reset') {
        // Reset mock data playback
        resetMockPlayback();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            success: true, 
            currentIndex: 0
        }));
    } else if (req.url.startsWith('/api/mock/speed')) {
        // Set playback speed
        try {
            const url = new URL(req.url, `http://${req.headers.host}`);
            const speed = parseFloat(url.searchParams.get('speed')) || 1.0;
            setMockSpeed(speed);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ 
                success: true, 
                speed: mockPlaybackState.speed
            }));
        } catch (err) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Invalid speed parameter' }));
        }
    } else if (req.url === '/api/mock/status') {
        // Get mock playback status
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            success: true,
            isPlaying: mockPlaybackState.isPlaying,
            currentIndex: mockPlaybackState.currentIndex,
            totalRows: mockData.length,
            speed: mockPlaybackState.speed,
            progress: mockData.length > 0 ? (mockPlaybackState.currentIndex / mockData.length * 100).toFixed(1) : 0
        }));
    } else if (req.url === '/api/influxdb/stats') {
        // Get InfluxDB write statistics
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            success: true,
            totalPoints: influxWriteStats.totalPoints,
            successCount: influxWriteStats.successCount,
            errorCount: influxWriteStats.errorCount,
            lastError: influxWriteStats.lastError
        }));
    } else {
        res.writeHead(404);
        res.end('Not found');
    }
});

// =============================
// SERVIDOR WEBSOCKET
// =============================
const wss = new WebSocket.Server({ server });

let connectedClients = 0;

wss.on('connection', (ws, req) => {
    connectedClients++;
    const clientIp = req.socket.remoteAddress;
    console.log(`✓ WebSocket connected from ${clientIp} (Total: ${connectedClients})`);

    ws.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to High Speed Datalogger',
        timestamp: Date.now(),
        sendRate: 1000/SEND_INTERVAL_MS
    }));

    ws.on('message', (message) => {
        console.log('Received from client:', message.toString());
    });

    ws.on('close', () => {
        connectedClients--;
        console.log(`✗ Client disconnected (Total: ${connectedClients})`);
    });

    ws.on('error', (err) => {
        console.error('WebSocket client error:', err.message);
    });
});

wss.on('error', (err) => {
    console.error('WebSocket server error:', err);
});

// =============================
// MOCK DATA FUNCTIONS
// =============================

// Load CSV data
async function loadMockData() {
    return new Promise((resolve, reject) => {
        mockData = [];
        const csvPath = path.join(__dirname, 'mock_data.csv');
        
        if (!fs.existsSync(csvPath)) {
            return reject(new Error('mock_data.csv not found'));
        }

        const fileStream = fs.createReadStream(csvPath);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        let isFirstLine = true;
        
        rl.on('line', (line) => {
            if (isFirstLine) {
                isFirstLine = false;
                return; // Skip header
            }
            
            const values = line.split(',');
            if (values.length === 5) {
                mockData.push({
                    timestamp: parseInt(values[0]),
                    ch1: parseFloat(values[1]),
                    ch2: parseFloat(values[2]),
                    ch3: parseFloat(values[3]),
                    ch4: parseFloat(values[4])
                });
            }
        });

        rl.on('close', () => {
            console.log(`✓ Loaded ${mockData.length} mock data samples`);
            resolve();
        });

        rl.on('error', (err) => {
            reject(err);
        });
    });
}

// Start mock playback
function startMockPlayback() {
    if (mockData.length === 0) {
        console.log('✗ Cannot start playback: No mock data loaded');
        return;
    }

    if (mockPlaybackState.isPlaying) {
        return; // Already playing
    }

    mockPlaybackState.isPlaying = true;
    
    const baseInterval = SEND_INTERVAL_MS;
    const interval = baseInterval / mockPlaybackState.speed;
    
    mockPlaybackState.intervalId = setInterval(() => {
        if (mockPlaybackState.currentIndex >= mockData.length) {
            // Loop back to start
            mockPlaybackState.currentIndex = 0;
        }

        const data = mockData[mockPlaybackState.currentIndex];
        
        // Send to all connected WebSocket clients
        const payload = JSON.stringify({
            type: 'update',
            ch1: data.ch1,
            ch2: data.ch2,
            ch3: data.ch3,
            ch4: data.ch4,
            timestamp: Date.now(),
            mockIndex: mockPlaybackState.currentIndex,
            mockTotal: mockData.length
        });

        wss.clients.forEach((client) => {
            if (client.readyState === WebSocket.OPEN) {
                try {
                    client.send(payload);
                } catch (err) {
                    console.error('Error sending mock data to client:', err.message);
                }
            }
        });

        // Write mock data to InfluxDB
        writeToInfluxDB(data.ch1, data.ch2, data.ch3, data.ch4, Date.now());

        mockPlaybackState.currentIndex++;
    }, interval);

    console.log(`✓ Mock playback started at ${mockPlaybackState.speed}x speed`);
}

// Pause mock playback
function pauseMockPlayback() {
    if (mockPlaybackState.intervalId) {
        clearInterval(mockPlaybackState.intervalId);
        mockPlaybackState.intervalId = null;
    }
    mockPlaybackState.isPlaying = false;
    console.log('⏸ Mock playback paused');
}

// Reset mock playback
function resetMockPlayback() {
    pauseMockPlayback();
    mockPlaybackState.currentIndex = 0;
    console.log('↺ Mock playback reset');
}

// Set mock playback speed
function setMockSpeed(speed) {
    const wasPlaying = mockPlaybackState.isPlaying;
    
    if (wasPlaying) {
        pauseMockPlayback();
    }
    
    mockPlaybackState.speed = Math.max(0.1, Math.min(10, speed)); // Clamp between 0.1x and 10x
    
    if (wasPlaying) {
        startMockPlayback();
    }
    
    console.log(`✓ Mock playback speed set to ${mockPlaybackState.speed}x`);
}

// =============================
// INFLUXDB WRITE FUNCTIONS
// =============================

function writeToInfluxDB(ch1, ch2, ch3, ch4, timestamp = Date.now()) {
    try {
        // Debug: Log first few writes
        if (influxWriteStats.totalPoints < 5) {
            console.log(`📝 Writing to InfluxDB: CH1=${ch1}, CH2=${ch2}, CH3=${ch3}, CH4=${ch4}`);
        }
        
        const point = new Point('sensor_data')
            .floatField('ch1', ch1)
            .floatField('ch2', ch2)
            .floatField('ch3', ch3)
            .floatField('ch4', ch4)
            .timestamp(new Date(timestamp));
        
        writeApi.writePoint(point);
        influxWriteStats.totalPoints++;
        influxWriteStats.successCount++;
        
        // Log every 50 points
        if (influxWriteStats.totalPoints % 50 === 0) {
            console.log(`✓ Written ${influxWriteStats.totalPoints} points to InfluxDB (${influxWriteStats.errorCount} errors)`);
        }
    } catch (error) {
        influxWriteStats.errorCount++;
        influxWriteStats.lastError = error.message;
        console.error('✗ Error writing to InfluxDB:', error.message);
        console.error('  Data:', { ch1, ch2, ch3, ch4, timestamp });
    }
}

// Flush data periodically (every 5 seconds)
setInterval(async () => {
    try {
        await writeApi.flush();
        if (influxWriteStats.totalPoints > 0) {
            console.log(`✓ Flushed InfluxDB data (${influxWriteStats.totalPoints} total points)`);
        }
    } catch (error) {
        console.error('✗ Error flushing InfluxDB writes:', error.message);
        influxWriteStats.errorCount++;
        influxWriteStats.lastError = error.message;
    }
}, 5000);

// =============================
// CLIENTE MQTT
// =============================
const mqttClient = mqtt.connect(MQTT_BROKER, {
    reconnectPeriod: 5000,
    connectTimeout: 30000
});

// Buffer con SOLO el último valor recibido de cada canal
const latestValues = {
    ch1: null,
    ch2: null,
    ch3: null,
    ch4: null,
    timestamp: null
};

let mqttConnected = false;

mqttClient.on('connect', () => {
    mqttConnected = true;
    console.log('✓ Connected to MQTT broker');

    const topics = [
        'data/nexus/ch1',
        'data/nexus/ch2',
        'data/nexus/ch3',
        'data/nexus/ch4'
    ];

    topics.forEach(topic => {
        mqttClient.subscribe(topic, (err) => {
            if (!err) {
                console.log('✓ Subscribed to', topic);
            } else {
                console.error('✗ Failed to subscribe to', topic, err.message);
            }
        });
    });
});

mqttClient.on('error', (err) => {
    mqttConnected = false;
    console.error('MQTT error:', err.message);
});

mqttClient.on('reconnect', () => {
    console.log('Reconnecting to MQTT broker...');
});

// Actualiza SOLO el último valor por canal (último valor gana)
let mqttMessageCount = 0;
let lastMqttStatsTime = Date.now();

mqttClient.on('message', (topic, message) => {
    const value = parseFloat(message.toString());
    
    // Actualizar solo el último valor
    if (topic.endsWith('ch1')) latestValues.ch1 = value;
    else if (topic.endsWith('ch2')) latestValues.ch2 = value;
    else if (topic.endsWith('ch3')) latestValues.ch3 = value;
    else if (topic.endsWith('ch4')) latestValues.ch4 = value;
    
    latestValues.timestamp = Date.now();
    
    // Write to InfluxDB if all channels have data
    if (latestValues.ch1 !== null && latestValues.ch2 !== null && 
        latestValues.ch3 !== null && latestValues.ch4 !== null) {
        writeToInfluxDB(
            latestValues.ch1,
            latestValues.ch2,
            latestValues.ch3,
            latestValues.ch4,
            latestValues.timestamp
        );
    }
    
    // Stats cada 5 segundos
    mqttMessageCount++;
    const now = Date.now();
    if (now - lastMqttStatsTime >= 5000) {
        const rate = (mqttMessageCount / 5).toFixed(1);
        console.log(`MQTT Rate: ${rate} msg/s | Latest values: CH1=${latestValues.ch1?.toFixed(6)} CH2=${latestValues.ch2?.toFixed(6)} CH3=${latestValues.ch3?.toFixed(6)} CH4=${latestValues.ch4?.toFixed(6)} | InfluxDB: ${influxWriteStats.totalPoints} points written`);
        mqttMessageCount = 0;
        lastMqttStatsTime = now;
    }
});

// =============================
// ENVÍO PERIÓDICO A WEBSOCKETS
// Envía SOLO el último valor cada SEND_INTERVAL_MS
// =============================
let sendCount = 0;

setInterval(() => {
    if (connectedClients === 0) return;
    
    // Crear payload con solo los últimos valores
    const payload = JSON.stringify({
        type: 'update',
        ch1: latestValues.ch1,
        ch2: latestValues.ch2,
        ch3: latestValues.ch3,
        ch4: latestValues.ch4,
        timestamp: latestValues.timestamp
    });

    let sentCount = 0;
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            try {
                client.send(payload);
                sentCount++;
            } catch (err) {
                console.error('Error sending to client:', err.message);
            }
        }
    });
    
    sendCount++;
    
    // Stats cada 100 envíos (cada 10 segundos a 10 Hz)
    if (sendCount % 100 === 0) {
        console.log(`Sent ${sendCount} updates to ${sentCount} client(s) | MQTT: ${mqttConnected ? 'Connected' : 'Disconnected'}`);
    }
}, SEND_INTERVAL_MS);

// =============================
// INICIO DEL SERVIDOR
// =============================
server.listen(WS_PORT, '0.0.0.0', () => {
    console.log('=================================');
    console.log(`✓ HTTP Server: http://0.0.0.0:${WS_PORT}`);
    console.log(`✓ WebSocket Server ready`);
    console.log(`✓ Send rate: ${1000/SEND_INTERVAL_MS} Hz (every ${SEND_INTERVAL_MS}ms)`);
    console.log('=================================');
    console.log('Waiting for connections...');
});

// =============================
// MANEJO DE ERRORES Y CIERRE LIMPIO
// =============================
server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        console.error(`✗ Port ${WS_PORT} is already in use. Try a different port.`);
    } else {
        console.error('Server error:', err);
    }
    process.exit(1);
});

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

async function shutdown() {
    console.log('\n=================================');
    console.log('Shutting down gracefully...');
    
    // Flush InfluxDB writes
    try {
        await writeApi.flush();
        await writeApi.close();
        console.log(`✓ InfluxDB closed (${influxWriteStats.totalPoints} points written)`);
    } catch (err) {
        console.error('Error closing InfluxDB:', err.message);
    }
    
    wss.clients.forEach((client) => {
        client.close();
    });
    
    wss.close(() => {
        console.log('✓ WebSocket server closed');
    });
    
    mqttClient.end(() => {
        console.log('✓ MQTT client closed');
    });
    
    server.close(() => {
        console.log('✓ HTTP server closed');
        console.log('=================================');
        process.exit(0);
    });
}

// Log info periódico
setInterval(() => {
    if (connectedClients > 0) {
        console.log(`Status: ${connectedClients} client(s) connected | MQTT: ${mqttConnected ? 'OK' : 'ERROR'}`);
    }
}, 30000); // Cada 30 segundos