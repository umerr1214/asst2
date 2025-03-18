#include "tasksys.h"
#include <vector>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>


using namespace std;

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                        const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {

}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    vector<thread> threads;

    for (int i = 0; i < num_total_tasks; i++) {
        //Using [=] captures i by value, using [&] will capture by refernece and running the loop may cause race condition
        
        //Constructs the object in place directly inside the vector.
        //no copying needed, more efficient
        threads.emplace_back([=]() {
            runnable->runTask(i, num_total_tasks);
        });
    }

    // //Alternative way to create threads, less efficenet
    // for (int i = 0; i < num_total_tasks; i++) {
    //     //adding elements to the vector
    //     //creates the object first then copies it to the vector 
    //     threads.push_back(thread[=](){ 
    //         runnable->runTask(i, num_total_tasks);
    //     });
    // }

    for (auto& thread: threads)
    {
        thread.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
                    : ITaskSystem(num_threads), stop_threads(false), tasks_completed(0) {   //atomic variables should be initialized in the initializer list
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).

    this->num_threads = num_threads;
    this->current_runnable = nullptr;
    
    // Launch worker threads
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerThread, this);
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // shutting down threads and joining them
    stop_threads = true;

    for (auto& worker : workers) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.

    current_runnable = runnable;
    total_tasks = num_total_tasks;
    tasks_completed = 0;

    // Push task onto the queue, so they can get executed dynamically
    {
        lock_guard<mutex> lock(queue_mutex);//automatically locks and unlocks
        for (int i = 0; i < num_total_tasks; i++) { //ensuring one thread modify task_queue
            task_queue.push(i);
        }
    }


    while (tasks_completed.load() < num_total_tasks) {  // Spin-wait until all tasks are completed
        // Do nothing
    }


    // for (int i = 0; i < num_total_tasks; i++) {  //no need for this now
    //     runnable->runTask(i, num_total_tasks);
    // }
}

void TaskSystemParallelThreadPoolSpinning::workerThread() {
    while (!stop_threads) {
        int task_id = -1;

        // Try to get a task, lock is needed to ensure only one thread enters in 
        {
            lock_guard<mutex> lock(queue_mutex);
            if (!task_queue.empty()) {
                task_id = task_queue.front();
                task_queue.pop();
            }
        }

        // If a task was assigned, execute it
        if (task_id != -1 && current_runnable) {
            current_runnable->runTask(task_id, total_tasks);    //running the task 
            tasks_completed++;
        }
    }
}


TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                            const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), 
    num_threads(num_threads), //order of initialization should be same as the order of declaration
    current_runnable(nullptr),
    tasks_completed(0),
    stop_threads(false), 
    total_tasks(0) {    
    // Launch worker threads
    for (int i = 0; i < num_threads; i++) {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        unique_lock<mutex> lock(queue_mutex);
        stop_threads = true;
    }
    condition.notify_all(); // Wake up all threads to exit

    // Join all worker threads
    for (thread& worker : workers) {
        worker.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    current_runnable = runnable;
    total_tasks = num_total_tasks;
    tasks_completed = 0;

    // Push tasks into queue
    {
        unique_lock<mutex> lock(queue_mutex);
        for (int i = 0; i < num_total_tasks; i++) {
            task_queue.push(i);
        }
    }

    condition.notify_all(); // Notify all worker threads that tasks are available

    // Wait until all tasks are completed using condition variable
    unique_lock<mutex> lock(queue_mutex);
    finished_cv.wait(lock, [this]() { return tasks_completed.load() >= total_tasks; });
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        int task_id = -1;

        {
            unique_lock<mutex> lock(queue_mutex);

            // Wait until there is a task in the queue or stop flag is set
            condition.wait(lock, [this]() { return stop_threads || !task_queue.empty(); });

            if (stop_threads) return; // Exit if stop flag is set

            if (!task_queue.empty()) {
                task_id = task_queue.front();
                task_queue.pop();
            }
        }

        // Execute task if valid
        if (task_id != -1 && current_runnable) {
            current_runnable->runTask(task_id, total_tasks);
            tasks_completed.fetch_add(1, memory_order_relaxed);

            // Notify main thread if all tasks are completed
            if (tasks_completed.load() >= total_tasks) {
                finished_cv.notify_one();
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
