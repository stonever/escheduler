# eScheduler 📅

eScheduler is a distributed task scheduling framework designed to handle various load balancing strategies. The framework allows clients to add and remove tasks dynamically by monitoring task statuses.

## Features ✨

- **Distributed Scheduling**: Efficiently schedules tasks across multiple workers.
- **Load Balancing**: Utilizes least-load and sticky strategies for balanced task distribution.
- **Priority Queue**: Executes tasks based on their priority levels.
- **Kubernetes Integration**: Supports rolling updates by ensuring all tasks are running before proceeding.
- **Mutex-Based Worker Registration**: Prevents worker overload by limiting the number of workers registered at any given time.

## How It Works 🛠️

1. **Initial Setup**: Workers gather at a barrier to prevent premature scheduling.
2. **Re-Balancing**: Workers re-balance after an initial wait period.
3. **Task Execution**: Workers start tasks based on priority.
4. **Load Distribution**: Least-load algorithm ensures no worker is overburdened.
5. **Sticky Strategy**: Minimizes changes during reassignments to maintain stability.
6. **Worker Registration**: Uses a mutex to check worker count before registration in etcd.

## Getting Started 🚀

To get started with eScheduler, clone the repository and follow the setup instructions in the [installation guide](https://github.com/stonever/escheduler).

## License 📄

This project is licensed under the Apache-2.0 License - see the [LICENSE](LICENSE) file for details.

## Contributing 🤝

We welcome contributions! Please see our [contributing guidelines](CONTRIBUTING.md) for more details.

## Contact ✉️

For any inquiries or support, please open an issue on the [GitHub repository](https://github.com/stonever/escheduler/issues).

---

Made with ❤️ by the eScheduler team.