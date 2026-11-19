export function msg(offset, key, tenantId, extra = {}) {
  return { offset, key, tenantId, ...extra };
}

export function partition(id, currentOffset, messages, extra = {}) {
  return {
    id,
    currentOffset,
    endOffset: currentOffset + messages.length,
    produceRate: 0,
    messages,
    ...extra,
  };
}

export function consumer(id, capacity, maxBatchSize) {
  const entry = { id, capacity };
  if (maxBatchSize !== undefined) {
    entry.maxBatchSize = maxBatchSize;
  }
  return entry;
}

export function tenant(id, weight, quota) {
  const entry = { id, weight };
  if (quota !== undefined) {
    entry.quota = quota;
  }
  return entry;
}

export function baseInput(overrides = {}) {
  return {
    round: 1,
    now: 1_000,
    roundDurationMs: 1000,
    maxAttempts: 3,
    ...overrides,
  };
}

export function flattenBatches(plan) {
  return plan.assignments
    .flatMap((assignment) => assignment.batches.map((batch) => ({ ...batch, consumerId: assignment.consumerId })))
    .sort((a, b) => a.partitionId.localeCompare(b.partitionId) || a.fromOffset - b.fromOffset);
}
