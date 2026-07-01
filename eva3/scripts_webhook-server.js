const express = require('express');
const { PubSub } = require('@google-cloud/pubsub');
const fs = require('fs');
const path = require('path');

const app = express();

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

const PROJECT_ID = process.env.GCP_PROJECT_ID || 'bigdata-497600';
const TOPIC_NAME = process.env.PUBSUB_TOPIC || 'taxi-trips-stream';
// TOPIC de DEADLETTER para gestionar errores
const DLQ_TOPIC = process.env.PUBSUB_DLQ_TOPIC || 'taxi-trips-stream-dlq';

const pubsub = new PubSub({
    projectId: PROJECT_ID
});

// --- Lookup de zonas en memoria (mismo CSV usado en el batch) ---
// Descarga antes de iniciar: gsutil cp gs://bucket-bigdata-diego/taxi_zone_lookup.csv ~/pipeline/
const ZONE_LOOKUP_PATH = process.env.ZONE_LOOKUP_PATH || path.join(__dirname, 'taxi_zone_lookup.csv');
const zoneLookup = new Map();

function parseCsvLine(line) {
    // Respeta comas dentro de comillas (ej: "Astoria, Queens")
    const fields = [];
    let current = '';
    let inQuotes = false;
    for (const char of line) {
        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            fields.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    fields.push(current.trim());
    return fields;
}

function loadZoneLookup() {
    try {
        const raw = fs.readFileSync(ZONE_LOOKUP_PATH, 'utf8');
        const lines = raw.trim().split('\n');
        for (let i = 1; i < lines.length; i++) {
            const [locationID, borough, zone, serviceZone] = parseCsvLine(lines[i]);
            zoneLookup.set(Number(locationID), {
                borough: borough?.replace(/^"|"$/g, ''),
                zone: zone?.replace(/^"|"$/g, ''),
                serviceZone: serviceZone?.replace(/^"|"$/g, ''),
            });
        }
        console.log(`Zone lookup cargado: ${zoneLookup.size} zonas`);
    } catch (err) {
        console.warn(`No se pudo cargar taxi_zone_lookup.csv (${err.message}). pickup_borough/zone quedarán null.`);
    }
}
loadZoneLookup();

// --- Réplica en JS de los cálculos de la consulta batch (main_trips) ---
// Misma fórmula que el CREATE TABLE main_trips original, para que
// streaming y batch produzcan resultados idénticos.
function computeTripMetrics(event) {
    const pickup = new Date(event.tpep_pickup_datetime);
    const dropoff = new Date(event.tpep_dropoff_datetime);

    const trip_duration_min = Math.round((dropoff - pickup) / 60000);

    const trip_distance = event.trip_distance ?? null;
    const tip_amount = event.tip_amount ?? 0;
    const fare_amount = Number(event.fare_amount);

    const fare_per_mile = trip_distance ? fare_amount / trip_distance : null;
    const tip_pct = fare_amount ? (tip_amount / fare_amount) * 100 : null;

    // BigQuery DAYOFWEEK: 1=Domingo...7=Sábado. JS getDay(): 0=Domingo...6=Sábado.
    const day_of_week = pickup.getDay() + 1;

    const pu = zoneLookup.get(Number(event.PULocationID)) || {};
    const doZone = zoneLookup.get(Number(event.DOLocationID)) || {};

    return {
        trip_duration_min,
        fare_per_mile,
        tip_pct,
        year: pickup.getFullYear(),
        month: pickup.getMonth() + 1,
        day_of_week,
        pickup_hour: pickup.getHours(),
        pickup_borough: pu.borough ?? null,
        pickup_zone: pu.zone ?? null,
        pickup_service_zone: pu.serviceZone ?? null,
        dropoff_borough: doZone.borough ?? null,
        dropoff_zone: doZone.zone ?? null,
        dropoff_service_zone: doZone.serviceZone ?? null,
    };
}

const runLog = [];

function logRun(entry) {
    runLog.push({
        ...entry,
        timestamp: new Date().toISOString()
    });

    console.log("[control_ejecucion]", entry);
}

function validateTripEvent(body) {

    const required = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount"
    ];

    const missing = required.filter(
        f => body[f] === undefined || body[f] === null || body[f] === ""
    );

    if (missing.length) {
        return {
            valid: false,
            reason: "Campos faltantes: " + missing.join(", ")
        };
    }

    const pickup = new Date(body.tpep_pickup_datetime);
    const dropoff = new Date(body.tpep_dropoff_datetime);

    if (dropoff < pickup) {
        return {
            valid: false,
            reason: "dropoff anterior al pickup"
        };
    }

    if (Number(body.fare_amount) < 0) {
        return {
            valid: false,
            reason: "fare_amount negativo"
        };
    }

    return {
        valid: true
    };
}

app.post("/events/trip", async (req, res) => {

    const event = req.body;

    const validation = validateTripEvent(event);

    if (!validation.valid) {

        try {

            await pubsub.topic(DLQ_TOPIC).publishMessage({

                data: Buffer.from(JSON.stringify(event)),

                attributes: {
                    reason: validation.reason
                }

            });

        } catch (err) {

            console.error("Error publicando DLQ:", err.message);

        }

        logRun({

            event_id:
                event.trip_id ||
                `${event.tpep_pickup_datetime}_${event.PULocationID}_${event.DOLocationID}`,

            status: "rejected",

            reason: validation.reason

        });

        return res.status(400).json({
            error: validation.reason
        });

    }

    try {

        event.ingestion_time = new Date().toISOString();

        const metrics = computeTripMetrics(event);
        Object.assign(event, metrics);

        const messageId = await pubsub
            .topic(TOPIC_NAME)
            .publishMessage({

                data: Buffer.from(JSON.stringify(event)),

                attributes: {
                    source: "manual-form"
                }

            });

        logRun({

            event_id:
                event.trip_id ||
                `${event.tpep_pickup_datetime}_${event.PULocationID}_${event.DOLocationID}`,

            status: "published",

            messageId

        });

        res.status(202).json({

            status: "accepted",

            messageId

        });

    } catch (err) {

        console.error(err);

        logRun({

            event_id:
                event.trip_id ||
                `${event.tpep_pickup_datetime}_${event.PULocationID}_${event.DOLocationID}`,

            status: "error",

            reason: err.message

        });

        res.status(500).json({

            error: err.message

        });

    }

});

app.get("/health", (req, res) => {

    res.json({

        status: "ok",

        eventsProcessed: runLog.length

    });

});

app.get("/control-ejecucion", (req, res) => {

    res.json(runLog);

});

const PORT = process.env.PORT || 8080;

app.listen(PORT, "0.0.0.0", () => {

    console.log("================================");
    console.log("Webhook iniciado");
    console.log("Proyecto:", PROJECT_ID);
    console.log("Topic:", TOPIC_NAME);
    console.log("Puerto:", PORT);
    console.log("Formulario:");
    console.log(`http://localhost:${PORT}/form.html`);
    console.log("================================");

});
