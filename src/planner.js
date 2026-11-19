import { validateInput } from './validate.js';

const REASON = {
  QUOTA: 'tenant_quota_exhausted',
  RETRY_BACKOFF: 'retry_backoff',
  WORK_UNITS: 'insufficient_available_work_units',
  CONSUMER_CAPACITY: 'insufficient_consumer_capacity',
};

function roundUp(value) {
  return Math.max(0, Math.ceil(value - 1e-12));
}

function averageCost(messages) {
  if (messages.length === 0) {
    return 1;
  }
  let total = 0;
  for (const message of messages) {
    total += message.cost;
  }
  return total / messages.length;
}

function appendSegment(consumerState, message, cost) {
  const segments = consumerState.segments;
  const last = segments[segments.length - 1];
  if (last && last.partitionId === message.partitionId && last.toOffset + 1 === message.offset) {
    last.toOffset = message.offset;
    last.messageCount += 1;
    last.workUnits += cost;
    if (message.isRetry) {
      last.retryCount += 1;
    }
  } else {
    segments.push({
      partitionId: message.partitionId,
      fromOffset: message.offset,
      toOffset: message.offset,
      messageCount: 1,
      workUnits: cost,
      retryCount: message.isRetry ? 1 : 0,
    });
  }
}

function chooseConsumer(consumers, consumerStates, keyOwners, partitionId, key, cost) {
  let chosen = -1;
  for (let index = 0; index < consumers.length; index += 1) {
    const state = consumerStates[index];
    if (state.remainingBudget + 1e-12 < cost) {
      continue;
    }
    if (state.batchCount >= consumers[index].maxBatchSize) {
      continue;
    }
    const owner = keyOwners.get(partitionId)?.get(key);
    if (owner !== undefined && owner !== consumers[index].id) {
      continue;
    }
    if (chosen === -1 || state.usedWorkUnits < consumerStates[chosen].usedWorkUnits) {
      chosen = index;
    }
  }
  return chosen;
}

/**
 * Plan a single drain round. Pure function: no I/O, no offset commit/rollback,
 * no message execution. Returns assignments and projected backlog only.
 *
 * @param {object} rawInput partitions, consumers, tenants, retries, quotas and budgets.
 * @returns {object} target offsets, consumer assignments, blocked/quarantined messages
 *   and remaining backlog projections for the round.
 */
export function planRound(rawInput) {
  const input = validateInput(rawInput);
  const {
    round,
    now,
    roundDurationMs,
    availableWorkUnits,
    maxAttempts,
    partitions,
    consumers,
    tenants,
    pinnedKeys,
  } = input;
  const roundSeconds = roundDurationMs / 1000;

  const states = partitions.map((partition) => ({
    partition,
    index: 0,
  }));

  const consumerStates = consumers.map((consumer) => ({
    id: consumer.id,
    remainingBudget: consumer.capacity * roundSeconds,
    usedWorkUnits: 0,
    batchCount: 0,
    segments: [],
  }));

  const keyOwners = new Map(
    partitions.map((partition) => [partition.id, new Map(pinnedKeys.get(partition.id) ?? [])]),
  );
  const tenantServed = new Map(tenants.map((tenant) => [tenant.id, 0]));
  const tenantQuota = new Map(tenants.map((tenant) => [tenant.id, tenant.quota]));
  const tenantWeight = new Map(tenants.map((tenant) => [tenant.id, tenant.weight]));

  const quarantine = [];
  let globalRemaining = availableWorkUnits;
  let globalExhausted = false;

  // Poison messages are quarantined at the head of each partition stream.
  // They advance the planned offset but never consume processing capacity.
  for (const state of states) {
    while (state.index < state.partition.stream.length) {
      const message = state.partition.stream[state.index];
      if (message.attempt < maxAttempts) {
        break;
      }
      quarantine.push({
        partitionId: message.partitionId,
        offset: message.offset,
        key: message.key,
        tenantId: message.tenantId,
        attempt: message.attempt,
        reason: 'max_attempts_exceeded',
        destination: 'dead_letter_queue',
      });
      state.index += 1;
    }
  }

  const blockReasonByPartition = new Map();

  while (true) {
    let candidate = -1;
    let candidateScore = null;

    for (let position = 0; position < states.length; position += 1) {
      const state = states[position];
      if (state.index >= state.partition.stream.length) {
        continue;
      }
      const message = state.partition.stream[state.index];

      if (message.availableAt !== null && message.availableAt > now) {
        blockReasonByPartition.set(message.partitionId, REASON.RETRY_BACKOFF);
        continue;
      }

      if (tenantServed.get(message.tenantId) >= tenantQuota.get(message.tenantId)) {
        blockReasonByPartition.set(message.partitionId, REASON.QUOTA);
        continue;
      }

      if (globalRemaining + 1e-12 < message.cost) {
        globalExhausted = true;
        blockReasonByPartition.set(message.partitionId, REASON.WORK_UNITS);
        continue;
      }

      const consumerIndex = chooseConsumer(
        consumers,
        consumerStates,
        keyOwners,
        message.partitionId,
        message.key,
        message.cost,
      );
      if (consumerIndex === -1) {
        blockReasonByPartition.set(message.partitionId, REASON.CONSUMER_CAPACITY);
        continue;
      }

      blockReasonByPartition.delete(message.partitionId);
      const score = tenantServed.get(message.tenantId) / tenantWeight.get(message.tenantId);
      if (candidateScore === null || score < candidateScore) {
        candidateScore = score;
        candidate = position;
      }
    }

    if (candidate === -1) {
      break;
    }

    const state = states[candidate];
    const message = state.partition.stream[state.index];
    const consumerIndex = chooseConsumer(
      consumers,
      consumerStates,
      keyOwners,
      message.partitionId,
      message.key,
      message.cost,
    );

    const consumerState = consumerStates[consumerIndex];
    consumerState.remainingBudget -= message.cost;
    consumerState.usedWorkUnits += message.cost;
    consumerState.batchCount += 1;
    globalRemaining -= message.cost;
    tenantServed.set(message.tenantId, tenantServed.get(message.tenantId) + 1);
    const partitionOwners = keyOwners.get(message.partitionId);
    if (!partitionOwners.has(message.key)) {
      partitionOwners.set(message.key, consumers[consumerIndex].id);
    }
    appendSegment(consumerState, message, message.cost);
    state.index += 1;

    // Quarantine a poison message as soon as it reaches the head next.
    while (state.index < state.partition.stream.length) {
      const next = state.partition.stream[state.index];
      if (next.attempt < maxAttempts) {
        break;
      }
      quarantine.push({
        partitionId: next.partitionId,
        offset: next.offset,
        key: next.key,
        tenantId: next.tenantId,
        attempt: next.attempt,
        reason: 'max_attempts_exceeded',
        destination: 'dead_letter_queue',
      });
      state.index += 1;
    }
  }

  const blockedPartitions = [];
  for (const state of states) {
    if (state.index >= state.partition.stream.length) {
      continue;
    }
    const message = state.partition.stream[state.index];
    const reason = blockReasonByPartition.get(message.partitionId);
    if (reason === undefined) {
      continue;
    }
    blockedPartitions.push({
      partitionId: message.partitionId,
      offset: message.offset,
      tenantId: message.tenantId,
      availableAt: message.availableAt,
      reason,
    });
  }

  const assignments = [];
  for (let index = 0; index < consumers.length; index += 1) {
    const consumer = consumers[index];
    const state = consumerStates[index];
    if (state.segments.length === 0) {
      continue;
    }
    const elapsedMs =
      consumer.capacity === 0 ? 0 : (state.usedWorkUnits / consumer.capacity) * 1000;
    assignments.push({
      consumerId: consumer.id,
      estimatedFinishMs: now + Math.min(roundDurationMs, elapsedMs),
      workUnits: state.usedWorkUnits,
      batches: state.segments.map((segment) => ({
        partitionId: segment.partitionId,
        fromOffset: segment.fromOffset,
        toOffset: segment.toOffset,
        batchSize: segment.messageCount,
        workUnits: segment.workUnits,
        retryCount: segment.retryCount,
      })),
    });
  }

  const targetOffsets = {};
  const remainingBacklog = {};
  const projectedRemaining = {};
  let totalRemaining = 0;
  let totalProjected = 0;
  let totalInitial = 0;
  let totalProcessed = 0;
  let totalProduceRate = 0;
  const pendingCostSum = new Map();
  const pendingCount = new Map();

  for (const state of states) {
    const { partition } = state;
    targetOffsets[partition.id] = partition.currentOffset + state.index;
    const remaining = partition.stream.length - state.index;
    const incoming = roundUp(partition.produceRate * roundSeconds);
    remainingBacklog[partition.id] = remaining;
    projectedRemaining[partition.id] = remaining + incoming;
    totalRemaining += remaining;
    totalProjected += remaining + incoming;
    totalInitial += partition.stream.length;
    totalProcessed += state.index;
    totalProduceRate += partition.produceRate;
    for (let index = state.index; index < partition.stream.length; index += 1) {
      const message = partition.stream[index];
      pendingCostSum.set(message.tenantId, (pendingCostSum.get(message.tenantId) ?? 0) + message.cost);
      pendingCount.set(message.tenantId, (pendingCount.get(message.tenantId) ?? 0) + 1);
    }
  }

  const totalConsumerCapacity = consumers.reduce((sum, consumer) => sum + consumer.capacity, 0);
  let estimatedDrainTimeMs = null;
  if (totalProjected > 0) {
    const pendingMessages = [];
    for (const state of states) {
      pendingMessages.push(...state.partition.stream.slice(state.index));
    }
    const avgCost = averageCost(pendingMessages);
    const incomingWorkPerSecond = totalProduceRate * avgCost;
    const catchUpRate = totalConsumerCapacity - incomingWorkPerSecond;
    const projectedWork = totalProjected * avgCost;
    estimatedDrainTimeMs =
      catchUpRate <= 0 ? Number.POSITIVE_INFINITY : (projectedWork / catchUpRate) * 1000;
  }

  const headReasonByTenant = new Map();
  for (const blocked of blockedPartitions) {
    const existing = headReasonByTenant.get(blocked.tenantId);
    if (existing === undefined || blocked.reason === REASON.RETRY_BACKOFF) {
      headReasonByTenant.set(blocked.tenantId, blocked.reason);
    }
  }

  const unsatisfiedTenants = [];
  for (const tenant of tenants) {
    const remaining = pendingCount.get(tenant.id) ?? 0;
    if (remaining === 0) {
      continue;
    }
    let reason;
    const headReason = headReasonByTenant.get(tenant.id);
    if (headReason === REASON.RETRY_BACKOFF) {
      reason = REASON.RETRY_BACKOFF;
    } else if (
      headReason === REASON.QUOTA ||
      (Number.isFinite(tenant.quota) && (tenantServed.get(tenant.id) ?? 0) >= tenant.quota)
    ) {
      reason = REASON.QUOTA;
    } else if (headReason === REASON.WORK_UNITS || globalExhausted) {
      reason = REASON.WORK_UNITS;
    } else {
      reason = REASON.CONSUMER_CAPACITY;
    }
    unsatisfiedTenants.push({
      tenantId: tenant.id,
      remainingMessages: remaining,
      remainingWorkUnits: pendingCostSum.get(tenant.id) ?? 0,
      plannedMessages: tenantServed.get(tenant.id) ?? 0,
      reason,
    });
  }

  return {
    round,
    plannedAt: now,
    roundDurationMs,
    targetOffsets,
    assignments,
    keyOwners: Object.fromEntries(
      [...keyOwners].map(([partitionId, owners]) => [partitionId, Object.fromEntries(owners)]),
    ),
    remainingBacklog,
    projectedRemaining,
    totalInitialBacklog: totalInitial,
    totalPlannedMessages: totalProcessed,
    totalRemainingBacklog: totalRemaining,
    totalProjectedRemaining: totalProjected,
    estimatedDrainTimeMs,
    unsatisfiedTenants,
    blockedPartitions,
    quarantinedMessages: quarantine,
  };
}

export const UNSATISFIED_REASONS = Object.freeze({ ...REASON });
