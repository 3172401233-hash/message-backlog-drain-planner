const DEFAULT_ROUND_DURATION_MS = 1000;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_COST = 1;

function fail(message) {
  throw new Error(`Invalid planner input: ${message}`);
}

function isObject(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function finiteNumber(value, field) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    fail(`${field} must be a finite number`);
  }
  return value;
}

function positiveInteger(value, field) {
  if (!Number.isInteger(value) || value < 1) {
    fail(`${field} must be a positive integer`);
  }
  return value;
}

function nonNegativeInteger(value, field) {
  if (!Number.isInteger(value) || value < 0) {
    fail(`${field} must be a non-negative integer`);
  }
  return value;
}

function positiveNumberOrInfinity(value, field) {
  if (typeof value !== 'number' || value <= 0 || Number.isNaN(value)) {
    fail(`${field} must be a positive number or Infinity`);
  }
  return value;
}

function checkUniqueIds(items, field) {
  const seen = new Set();
  for (const item of items) {
    if (!isObject(item) || typeof item.id !== 'string' || item.id.length === 0) {
      fail(`every ${field} entry must have a non-empty string id`);
    }
    if (seen.has(item.id)) {
      fail(`duplicate ${field} id: ${item.id}`);
    }
    seen.add(item.id);
  }
  return seen;
}

export function validateInput(input) {
  if (!isObject(input)) {
    fail('input must be an object');
  }

  const round = input.round === undefined ? 1 : positiveInteger(input.round, 'round');
  const now = input.now === undefined ? Date.now() : finiteNumber(input.now, 'now');
  const roundDurationMs =
    input.roundDurationMs === undefined
      ? DEFAULT_ROUND_DURATION_MS
      : finiteNumber(input.roundDurationMs, 'roundDurationMs');
  if (roundDurationMs <= 0) {
    fail('roundDurationMs must be greater than 0');
  }
  const availableWorkUnits =
    input.availableWorkUnits === undefined
      ? Number.POSITIVE_INFINITY
      : positiveNumberOrInfinity(input.availableWorkUnits, 'availableWorkUnits');
  const maxAttempts =
    input.maxAttempts === undefined ? DEFAULT_MAX_ATTEMPTS : positiveInteger(input.maxAttempts, 'maxAttempts');

  if (!Array.isArray(input.partitions) || input.partitions.length === 0) {
    fail('partitions must be a non-empty array');
  }
  if (!Array.isArray(input.consumers) || input.consumers.length === 0) {
    fail('consumers must be a non-empty array');
  }
  if (!Array.isArray(input.tenants) || input.tenants.length === 0) {
    fail('tenants must be a non-empty array');
  }

  checkUniqueIds(input.partitions, 'partition');
  checkUniqueIds(input.consumers, 'consumer');
  const tenantIds = checkUniqueIds(input.tenants, 'tenant');
  const consumerIds = new Set(input.consumers.map((consumer) => consumer.id));

  const pinnedKeys = new Map();
  if (input.pinnedKeys !== undefined) {
    if (!isObject(input.pinnedKeys)) {
      fail('pinnedKeys must be an object mapping partition ids to key/consumer maps');
    }
    for (const [partitionId, owners] of Object.entries(input.pinnedKeys)) {
      if (!isObject(owners)) {
        fail(`pinnedKeys for partition ${partitionId} must be an object mapping keys to consumer ids`);
      }
      const ownerMap = new Map();
      for (const [key, consumerId] of Object.entries(owners)) {
        if (typeof consumerId !== 'string' || !consumerIds.has(consumerId)) {
          fail(`pinnedKeys entry for ${partitionId}/${key} references unknown consumer ${consumerId}`);
        }
        ownerMap.set(key, consumerId);
      }
      pinnedKeys.set(partitionId, ownerMap);
    }
  }

  const tenants = input.tenants.map((tenant) => {
    const weight = finiteNumber(tenant.weight, `tenant ${tenant.id} weight`);
    if (weight <= 0) {
      fail(`tenant ${tenant.id} weight must be greater than 0`);
    }
    const quota = positiveNumberOrInfinity(
      tenant.quota === undefined ? Number.POSITIVE_INFINITY : tenant.quota,
      `tenant ${tenant.id} quota`,
    );
    return { id: tenant.id, weight, quota };
  });

  const consumers = input.consumers.map((consumer) => {
    const capacity = finiteNumber(consumer.capacity, `consumer ${consumer.id} capacity`);
    if (capacity < 0) {
      fail(`consumer ${consumer.id} capacity must be non-negative`);
    }
    const maxBatchSize =
      consumer.maxBatchSize === undefined
        ? Number.POSITIVE_INFINITY
        : positiveInteger(consumer.maxBatchSize, `consumer ${consumer.id} maxBatchSize`);
    return { id: consumer.id, capacity, maxBatchSize };
  });

  const partitions = input.partitions.map((partition) => {
    if (!isObject(partition) || typeof partition.id !== 'string' || partition.id.length === 0) {
      fail('every partition must have a non-empty string id');
    }
    const currentOffset = nonNegativeInteger(
      partition.currentOffset,
      `partition ${partition.id} currentOffset`,
    );
    const endOffset = nonNegativeInteger(partition.endOffset, `partition ${partition.id} endOffset`);
    if (endOffset < currentOffset) {
      fail(`partition ${partition.id} endOffset must be >= currentOffset`);
    }
    const produceRate =
      partition.produceRate === undefined
        ? 0
        : finiteNumber(partition.produceRate, `partition ${partition.id} produceRate`);
    if (produceRate < 0) {
      fail(`partition ${partition.id} produceRate must be non-negative`);
    }

    const depth = endOffset - currentOffset;
    if (!Array.isArray(partition.messages) || partition.messages.length !== depth) {
      fail(
        `partition ${partition.id} messages must be an array with one entry per offset ` +
          `(expected ${depth}, got ${Array.isArray(partition.messages) ? partition.messages.length : 0})`,
      );
    }

    const stream = partition.messages.map((entry, index) => {
      if (!isObject(entry)) {
        fail(`partition ${partition.id} message at index ${index} must be an object`);
      }
      const offset =
        entry.offset === undefined ? currentOffset + index : nonNegativeInteger(entry.offset, 'message offset');
      if (offset !== currentOffset + index) {
        fail(
          `partition ${partition.id} message at index ${index} has offset ${offset}, expected ${currentOffset + index}`,
        );
      }
      if (typeof entry.key !== 'string' || entry.key.length === 0) {
        fail(`partition ${partition.id} message at offset ${offset} must have a non-empty string key`);
      }
      if (typeof entry.tenantId !== 'string' || !tenantIds.has(entry.tenantId)) {
        fail(
          `partition ${partition.id} message at offset ${offset} references unknown tenant ${entry.tenantId}`,
        );
      }
      const cost =
        entry.cost === undefined ? DEFAULT_COST : finiteNumber(entry.cost, `message at offset ${offset} cost`);
      if (cost <= 0) {
        fail(`partition ${partition.id} message at offset ${offset} cost must be greater than 0`);
      }
      return {
        partitionId: partition.id,
        offset,
        key: entry.key,
        tenantId: entry.tenantId,
        cost,
        attempt: positiveInteger(entry.attempt ?? 1, `message at offset ${offset} attempt`),
        isRetry: entry.isRetry === true,
        availableAt: entry.availableAt === undefined ? null : finiteNumber(entry.availableAt, 'availableAt'),
      };
    });

    const retries = Array.isArray(partition.retries) ? partition.retries : [];
    const retryOffsets = new Set();
    for (const retry of retries) {
      if (!isObject(retry)) {
        fail(`partition ${partition.id} retries entries must be objects`);
      }
      const offset = nonNegativeInteger(retry.offset, 'retry offset');
      if (offset < currentOffset || offset >= endOffset) {
        fail(`partition ${partition.id} retry offset ${offset} is outside the available offset range`);
      }
      if (retryOffsets.has(offset)) {
        fail(`partition ${partition.id} has duplicate retry entry for offset ${offset}`);
      }
      retryOffsets.add(offset);
      const attempt = positiveInteger(retry.attempt, `retry at offset ${offset} attempt`);
      const availableAt =
        retry.availableAt === undefined ? null : finiteNumber(retry.availableAt, 'retry availableAt');
      const target = stream[offset - currentOffset];
      target.attempt = attempt;
      target.isRetry = true;
      target.availableAt = availableAt;
    }

    return {
      id: partition.id,
      currentOffset,
      endOffset,
      produceRate,
      stream,
      retryOffsets,
    };
  });

  return {
    round,
    now,
    roundDurationMs,
    availableWorkUnits,
    maxAttempts,
    partitions,
    consumers,
    tenants,
    pinnedKeys,
  };
}

export { DEFAULT_MAX_ATTEMPTS, DEFAULT_ROUND_DURATION_MS };
