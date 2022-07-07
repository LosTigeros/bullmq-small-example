"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const bullmq_1 = require("bullmq");
const ioredis_1 = tslib_1.__importDefault(require("ioredis"));
// Create a redis connection
const redis = new ioredis_1.default({
    maxRetriesPerRequest: null
});
// Create a Worker
const BullWorker = new bullmq_1.Worker("SomeWork", async (job) => {
    console.log("Running Job: ", job.name, job.data);
}, {
    concurrency: 1,
    limiter: {
        max: 2,
        duration: 1000
    },
    connection: redis,
});
// Create a Queue
const BullQueue = new bullmq_1.Queue("SomeWork", {
    defaultJobOptions: {
        attempts: 3,
        backoff: {
            type: "fixed",
            delay: 1000 * 15
        },
    },
    connection: redis,
});
// Log some events
const BullQueueEvents = new bullmq_1.QueueEvents("SomeWork", {
    connection: redis,
});
BullQueueEvents.on("added", ({ jobId, name }) => {
    console.log("Added Job: ", jobId, name);
});
BullQueueEvents.on("delayed", ({ jobId, delay }) => {
    console.log("Delayed Job: ", jobId, delay);
});
BullQueueEvents.on("completed", ({ jobId }) => {
    console.log("Completed Job: ", jobId);
});
// Add jobs to queue every 30 seconds
let iteration = 0;
function addJobs() {
    const promises = [];
    for (let x = 1; x <= 10; x++) {
        promises.push(BullQueue.add(`IT-${iteration}-${x}`, {
            x
        }));
    }
    Promise.all(promises).then(() => {
        setTimeout(addJobs, 1000 * 30);
    });
    // Increase iteration
    iteration++;
}
// Run it once
addJobs();
// Gracefull exit
let exiting = false;
function killProcess(errorCode) {
    if (exiting) {
        return;
    }
    console.log("Shutting down...");
    (async () => {
        // Close the worker
        await BullWorker.close();
        // Drain the queue
        await BullQueue.drain();
    })()
        .then(() => {
        console.error("Safely stopped execution.");
        exiting = true;
        process.exit(errorCode);
    })
        .catch((err) => {
        console.error("Unable to safely shut down the process.", err);
        exiting = true;
        process.exit(errorCode);
    });
}
const signals = ["SIGINT", "SIGUSR2", "SIGTERM"];
signals.forEach((eventType) => {
    process.on(eventType, () => killProcess(0));
});
process.on("exit", () => killProcess(0));
//# sourceMappingURL=index.js.map