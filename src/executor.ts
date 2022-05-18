import os from "os";

export interface ExecutorInit {
    maxParallel?: number
    name?: string
    logTasks?: boolean
    log?: (...args: unknown[]) => void
}

export type PromiseSupplier<T> = () => Promise<T> | T;

export interface TaskInit<T> {
    promiseSupplier: PromiseSupplier<T>
    name?: string
    priority?: number
}

interface Task<T> extends Required<TaskInit<T>> {
    resolve: (value: T) => void
    reject: (reason?: any) => void
}

function randomName(len = 4) {
    const dict = "abcdefghijklmnopqrstuvwxyz";
    let name = "";
    while (name.length < len) {
        name += dict[Math.floor(Math.random() * dict.length)]
    }
    return name;
}

export class Executor {
    private readonly maxParallel: number;
    private readonly name: string;
    private readonly log?: (...args: unknown[]) => void;
    private readonly queue: Array<Task<unknown>> = [];
    private pendingCount: number = 0;
    private nextTaskId = 0;

    constructor(init?: number | ExecutorInit) {
        if (typeof init === "number") {
            this.maxParallel = init;
            this.name = randomName();
        } else {
            this.maxParallel = init?.maxParallel ?? os.cpus().length;
            this.name = init?.name ?? randomName();
            this.log = init?.logTasks
                ? init?.log ?? console.log
                : undefined;
        }
    }

    async enqueue<T>(init: PromiseSupplier<T> | TaskInit<T>): Promise<T> {
        return new Promise((resolve, reject) => {
            this.addToQueue(this.newTask(init, resolve, reject));
            this.processQueueDelayed();
        });
    }

    async enqueueAll<T>(promiseSuppliers: Array<PromiseSupplier<T>>): Promise<Array<T>> {
        const enqueuedPromises: Array<Promise<T>> = promiseSuppliers
            .map(promiseSupplier => this.enqueue(promiseSupplier));
        return Promise.all(enqueuedPromises);
    }

    private newTask<T>(init: PromiseSupplier<T> | TaskInit<T>, resolve: (value: T) => void, reject: (reason?: any) => void): Task<T> {
        if (typeof init === "function") {
            init = {
                promiseSupplier: init
            }
        }

        const task = {
            ...init,
            resolve,
            reject
        }
        task.priority = task.priority ?? 0;
        task.name = task.name ?? `task-${this.nextTaskId++}`;

        return task as Task<T>;
    }

    private addToQueue(task: Task<any>) {
        if (this.queue.length === 0) {
            this.queue.push(task);
            return;
        }

        const lowestQueuedPriority = this.queue[this.queue.length - 1].priority;
        if (lowestQueuedPriority >= task.priority) {
            this.queue.push(task);
            return;
        }

        let targetIdx: number | undefined;
        for (let idx = 0; idx < this.queue.length; idx++) {
            const queuedTask = this.queue[idx];
            if (queuedTask.priority < task.priority) {
                targetIdx = idx;
                break;
            }
        }

        if (targetIdx !== undefined) {
            this.queue.splice(targetIdx, 0, task);
        } else {
            this.queue.push(task);
        }
    }

    private processQueueDelayed() {
        setTimeout(() => this.processQueue(), 0);
    }

    private processQueue(): void {
        this.logQueue();
        if (this.pendingCount >= this.maxParallel) {
            return;
        }
        for (let idx = 0; idx < this.queue.length; idx++) {
            const task = this.queue[idx];
            if (this.pendingCount < this.maxParallel) {
                this.queue.splice(idx, 1);
                this.start(task);
                this.processQueueDelayed();
                return;
            }
        }
    }

    private start(task: Task<unknown>): void {
        this.pendingCount++;
        this.logStarting(task);
        Promise.resolve(task.promiseSupplier())
            .then(task.resolve)
            .catch(task.reject)
            .finally(() => {
                this.pendingCount--;
                this.logDone(task);
                this.processQueueDelayed();
            });
    }

    private logQueue(): void {
        if (this.log) {
            this.log("Queue", this.name, `${this.queue.length} waiting`, `[${this.pendingCount}/${this.maxParallel}]`);
        }
    }

    private logStarting(task: Task<unknown>): void {
        if (this.log) {
            this.log("Starting", this.name, task.name, `[${this.pendingCount}/${this.maxParallel}]`);
        }
    }

    private logDone(task: Task<unknown>): void {
        if (this.log) {
            this.log("Done", this.name, task.name, `[${this.pendingCount}/${this.maxParallel}]`);
        }
    }
}
