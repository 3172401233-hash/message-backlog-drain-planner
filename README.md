# message-backlog-drain-planner

规划消息队列积压排空、分区顺序和消费能力恢复。

`message-backlog-drain-planner` 是一个**纯规划库**：给定各分区的当前偏移、生产速率、
消费者处理能力、业务键、租户权重、重试队列、毒消息和可用工作单元，计算出**下一轮**
应当如何分配消费者与批次。它本身不连接任何消息中间件。

## 设计边界（重要）

本库**只生成计划，不负责提交或回滚消费偏移**，也不会：

- 拉取消息、执行消费、提交 offset 或回滚 offset；
- 把毒消息真正写入隔离队列（DLQ）；
- 重试任何消息、执行退避定时器。

调用方负责按 `assignments` 执行处理，并自行在成功后提交 `targetOffsets`、
把 `quarantinedMessages` 写入隔离队列。规划器是无状态的纯函数，新一轮调用把上一轮的
`keyOwners` 通过 `pinnedKeys` 传回，即可在消费者扩容/缩容时保持业务键亲和性。

## 保证的语义

- **键级顺序**：同一分区内同一业务键的消息在任意时刻只会分配给同一个消费者，
  不会并发处理；不同业务键可以并行。
- **分区内 FIFO**：每个分区严格按偏移推进。若队头消息因退避或租户配额不可处理，
  该分区本轮在队头处阻塞，后续消息不会越过它（见 `blockedPartitions`）。
- **重试顺序**：重试消息通过 `partition.retries` 标注，按原分区、原偏移顺序推进，
  可携带 `availableAt` 表达退避；退避未结束时分区阻塞而不是跳过。
- **毒消息隔离**：消息 `attempt >= maxAttempts` 时进入 `quarantinedMessages`
  （目标 `dead_letter_queue`），推进目标偏移但不占用消费能力，不会无限重试。
- **租户公平与配额**：按加权公平队列（已服务量 / 权重）选择分区，
  防止大租户长期饿死小租户；每轮处理量受租户 `quota` 限制，未满足的租户在
  `unsatisfiedTenants` 中给出原因。
- **异构消费者**：每个消费者有独立 `capacity`（工作单元/秒）和可选 `maxBatchSize`，
  按剩余预算分配，并给出每个消费者的 `estimatedFinishMs`。

## 安装与测试

要求 Node.js `>= 24.16.0`（ESM，零运行时依赖）。

```bash
node --test
```

## API

```js
import { planRound } from 'message-backlog-drain-planner';

const plan = planRound(input);
```

### 输入

| 字段 | 说明 |
| --- | --- |
| `round` | 轮次序号，默认 `1`。 |
| `now` | 当前时间戳（ms），用于重试退避判断，默认 `Date.now()`。 |
| `roundDurationMs` | 本轮时长（ms），默认 `1000`。消费者预算 = `capacity × roundDurationMs / 1000`。 |
| `availableWorkUnits` | 本轮全局可用工作单元上限，默认 `Infinity`。 |
| `maxAttempts` | 达到该尝试次数即判定为毒消息，默认 `3`。 |
| `pinnedKeys` | 上一轮 `plan.keyOwners`，用于跨轮保持键亲和性。 |
| `partitions[]` | `{ id, currentOffset, endOffset, produceRate, messages[], retries[] }`。 |
| `consumers[]` | `{ id, capacity, maxBatchSize? }`，`capacity` 为工作单元/秒。 |
| `tenants[]` | `{ id, weight, quota? }`，`quota` 默认 `Infinity`。 |

`messages` 必须为 `[currentOffset, endOffset)` 中每个偏移提供一条
`{ offset, key, tenantId, cost?=1, attempt?=1 }`。
`retries` 为 `{ offset, attempt, availableAt? }`，用于把某条消息标记为重试。
`produceRate` 单位为**消息/秒**，用于估算本轮新增积压。

### 输出

| 字段 | 说明 |
| --- | --- |
| `targetOffsets` | 每分区本轮结束后应提交到的偏移（含已隔离消息）。 |
| `assignments[]` | 每个被分配的消费者：`estimatedFinishMs`、`workUnits` 与 `batches[]`。 |
| `batches[]` | `{ partitionId, fromOffset, toOffset, batchSize, workUnits, retryCount }`。 |
| `keyOwners` | 分区 → 业务键 → 消费者，可直接作为下一轮 `pinnedKeys`。 |
| `remainingBacklog` | 本轮结束后每分区剩余的存量消息数（不含新增生产）。 |
| `projectedRemaining` | 计入本轮预计新增生产后的剩余消息数。 |
| `estimatedDrainTimeMs` | 预计排空时间；消费能力追不上生产时为 `Infinity`，无积压时为 `null`。 |
| `unsatisfiedTenants[]` | 仍有积压的租户及原因（配额、退避、工作单元、消费能力不足）。 |
| `blockedPartitions[]` | 队头阻塞的分区、偏移、租户与原因。 |
| `quarantinedMessages[]` | 应转入 DLQ 的毒消息。 |

## 示例

```js
import { planRound } from 'message-backlog-drain-planner';

const plan = planRound({
  round: 1,
  now: 1_000,
  roundDurationMs: 1000,
  maxAttempts: 3,
  partitions: [
    {
      id: 'orders-0',
      currentOffset: 10,
      endOffset: 13,
      produceRate: 0.5,
      messages: [
        { offset: 10, key: 'user-7', tenantId: 't-a' },
        { offset: 11, key: 'user-7', tenantId: 't-a', attempt: 2 },
        { offset: 12, key: 'user-9', tenantId: 't-b' },
      ],
      retries: [{ offset: 11, attempt: 2, availableAt: 900 }],
    },
  ],
  consumers: [
    { id: 'consumer-1', capacity: 2 },
    { id: 'consumer-2', capacity: 1 },
  ],
  tenants: [
    { id: 't-a', weight: 2, quota: 100 },
    { id: 't-b', weight: 1 },
  ],
});

// 执行 plan.assignments；成功后由调用方提交 plan.targetOffsets，
// 并把 plan.quarantinedMessages 写入隔离队列。
// 下一轮把 plan.keyOwners 作为 pinnedKeys 传入。
```

## 测试覆盖

`node --test` 覆盖：多分区排空、键级顺序与键亲和、消费者扩容、
异构消费能力、重试风暴与重试退避、毒消息隔离、租户配额冲突、
瞬时生产高峰投影、长期无流量分区、全局工作单元上限与输入校验。
