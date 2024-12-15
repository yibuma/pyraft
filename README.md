# PyRaft

PyRaft is a simple and elegant implementation of the Raft distributed consensus protocol based on Python and asyncio. Its design goals are efficiency, ease of understanding, learning, and use. It strives to be an excellent reference implementation.

PyRaft 是一个简洁的基于 Python 和 asyncio 实现的 Raft 分布式共识协议。它的设计目标是高效、易于理解、学习和使用。致力于成为一个优秀的参考实现。

- [Overview](#overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Architecture](#architecture)
- [Design Goals](#design-goals)
- [Contributing](#contributing)
- [概述](#概述)
- [主要特性](#主要特性)
- [快速开始](#快速开始)
- [API 参考](#api-参考)
- [架构](#架构)
- [设计目标](#设计目标)
- [贡献](#贡献)

## Overview

PyRaft implements the complete Raft consensus algorithm including:

- Leader election with pre-vote mechanism
- Log replication
- Cluster membership changes
- Log compaction via snapshots
- State machine abstraction

## Key Features

- Pure Python async implementation using `asyncio` and `aiohttp`
- Clear separation of concerns between components:
  - Node: Core Raft logic
  - HTTPGateway: HTTP API handling
  - StateMachine: Application state
  - Snapshot: Log compaction
- Pre-vote mechanism to prevent unnecessary elections
- Graceful handling of cluster membership changes
- Built-in key-value store example

## Quick Start


### Requirements
- Python 3.13+
- aiohttp

```bash
# Start first node as leader
python main.py --host 127.0.0.1 --port 8000

# Join additional nodes to cluster
python main.py --host 127.0.0.1 --port 8001 --join 127.0.0.1:8000
python main.py --host 127.0.0.1 --port 8002 --join 127.0.0.1:8000

# Use HTTP API to interact with cluster
curl -i -X "PUT" http://127.0.0.1:8000/keyx -H "Content-Type: application/json" -d '{"key": "value"}'
curl -i -X "GET" http://127.0.0.1:8000/keyx
```

## API Reference
### Key-Value Operations

- `GET /{key}` - Get value for key
- `PUT /{key}` - Set value for key
- `DELETE /{key}` - Delete key

### Cluster Management
- `PUT /__pyraft/peer/{address}` - Add node to cluster
- `DELETE /__pyraft/peer/{address}` - Remove node from cluster

### Status
- `GET /` - Get node status and cluster information


## Architecture
The implementation follows a modular design:

- `node.py` - Core Raft consensus logic
- `http.py` - HTTP API gateway
- `peer.py` - Inter-node communication
- `state_machine.py` - State machine interface
- `snapshot.py` - Log compaction
- `vo.py` - Data models and constants

## Design Goals

- **Educational Value**: Clear implementation to help understand Raft
- **Correctness**: Proper implementation of the complete Raft protocol
- **Maintainability**: Well-structured and documented code
- **Extensibility**: Easy to modify and enhance

## Contributing
Contributions are welcome! Please feel free to submit issues and pull requests.

## 概述

PyRaft 实现了完整的 Raft 共识算法，包括：

- 带预投票机制的领导者选举
- 日志复制
- 集群成员变更
- 通过快照实现日志压缩
- 状态机抽象

## 主要特性

- 使用 `asyncio` 和 `aiohttp` 的纯 Python 异步实现
- 清晰的组件关注点分离：
  - Node：核心 Raft 逻辑
  - HTTPGateway：HTTP API 处理
  - StateMachine：应用状态
  - Snapshot：日志压缩
- 预投票机制防止不必要的选举
- 优雅处理集群成员变更
- 内置键值存储示例

## 快速开始

### 要求
- Python 3.13+
- aiohttp

```bash
# 安装 aiohttp
pip install aiohttp

# 启动第一个节点作为领导者
python main.py --host 127.0.0.1 --port 8000

# 添加额外节点到集群
python main.py --host 127.0.0.1 --port 8001 --join 127.0.0.1:8000
python main.py --host 127.0.0.1 --port 8002 --join 127.0.0.1:8000

# 使用 HTTP API 与集群交互
curl -i -X "PUT" http://127.0.0.1:8000/keyx -H "Content-Type: application/json" -d '{"key": "value"}'
curl -i -X "GET" http://127.0.0.1:8000/keyx
```

## API 参考
### 键值操作

- `GET /{key}` - 获取键的值
- `PUT /{key}` - 设置键的值
- `DELETE /{key}` - 删除键

### 集群管理
- `PUT /__pyraft/peer/{address}` - 添加节点到集群
- `DELETE /__pyraft/peer/{address}` - 从集群移除节点

### 状态
- `GET /` - 获取节点状态和集群信息

## 架构
该实现遵循模块化设计：

- `node.py` - 核心 Raft 共识逻辑
- `http.py` - HTTP API 网关
- `peer.py` - 节点间通信
- `state_machine.py` - 状态机接口
- `snapshot.py` - 日志压缩
- `vo.py` - 数据模型和常量

## 设计目标

- **教育价值**：清晰的实现有助于理解 Raft
- **正确性**：完整的 Raft 协议实现
- **可维护性**：结构良好且文档完善的代码
- **可扩展性**：易于修改和增强

## 贡献
欢迎贡献！请随时提交问题和拉取请求。


