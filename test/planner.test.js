import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { planRound } from '../src/index.js';
import { baseInput, consumer, flattenBatches, msg, partition, tenant } from './helpers.js';

describe('backlog drain planner', () => {
  it('drains multiple partitions in one round and reports target offsets', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'k-a', 't1'),
            msg(1, 'k-b', 't2'),
            msg(2, 'k-c', 't1'),
            msg(3, 'k-d', 't2'),
          ]),
          partition('p1', 5, [
            msg(5, 'k-e', 't2'),
            msg(6, 'k-f', 't1'),
            msg(7, 'k-g', 't2'),
            msg(8, 'k-h', 't1'),
          ]),
        ],
        consumers: [consumer('c1', 4), consumer('c2', 4)],
        tenants: [tenant('t1', 1), tenant('t2', 1)],
      }),
    );

    assert.equal(plan.totalPlannedMessages, 8);
    assert.deepEqual(plan.targetOffsets, { p0: 4, p1: 9 });
    assert.deepEqual(plan.remainingBacklog, { p0: 0, p1: 0 });
    assert.equal(plan.totalRemainingBacklog, 0);
    assert.equal(plan.estimatedDrainTimeMs, null);
    assert.equal(plan.quarantinedMessages.length, 0);
    assert.equal(plan.unsatisfiedTenants.length, 0);
  });

  it('never processes the same business key concurrently and keeps partition order', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'same-key', 't1'),
            msg(1, 'same-key', 't1'),
            msg(2, 'same-key', 't1'),
            msg(3, 'same-key', 't1'),
          ]),
        ],
        consumers: [consumer('c1', 2), consumer('c2', 4)],
        tenants: [tenant('t1', 1)],
      }),
    );

    const owners = new Set();
    const offsets = [];
    for (const assignment of plan.assignments) {
      for (const batch of assignment.batches) {
        owners.add(assignment.consumerId);
        for (let offset = batch.fromOffset; offset <= batch.toOffset; offset += 1) {
          offsets.push(offset);
        }
      }
    }
    assert.deepEqual([...owners], ['c1']);
    assert.deepEqual(offsets, [0, 1]);
    assert.equal(plan.targetOffsets.p0, 2);
    assert.equal(plan.remainingBacklog.p0, 2);
    assert.equal(plan.keyOwners.p0['same-key'], 'c1');
  });

  it('runs different keys in parallel across consumers', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'key-a', 't1'),
            msg(1, 'key-a', 't1'),
            msg(2, 'key-b', 't1'),
            msg(3, 'key-b', 't1'),
          ]),
        ],
        consumers: [consumer('c1', 2), consumer('c2', 2)],
        tenants: [tenant('t1', 1)],
      }),
    );

    const byConsumer = Object.fromEntries(plan.assignments.map((entry) => [entry.consumerId, entry.workUnits]));
    assert.deepEqual(byConsumer, { c1: 2, c2: 2 });
    assert.equal(plan.keyOwners.p0['key-a'], 'c1');
    assert.equal(plan.keyOwners.p0['key-b'], 'c2');
  });

  it('keeps key affinity when consumers scale out via pinnedKeys', () => {
    const roundOne = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [msg(0, 'key-k', 't1'), msg(1, 'key-j', 't1')]),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1)],
      }),
    );
    assert.equal(roundOne.keyOwners.p0['key-k'], 'c1');

    const roundTwo = planRound(
      baseInput({
        round: 2,
        partitions: [
          partition('p0', 2, [msg(2, 'key-k', 't1'), msg(3, 'key-l', 't1')]),
        ],
        consumers: [consumer('c1', 4), consumer('c2', 4)],
        tenants: [tenant('t1', 1)],
        pinnedKeys: roundOne.keyOwners,
      }),
    );

    const batches = flattenBatches(roundTwo);
    const keyKBatch = batches.find((batch) => batch.fromOffset === 2);
    const keyLBatch = batches.find((batch) => batch.fromOffset === 3);
    assert.equal(keyKBatch.consumerId, 'c1');
    assert.equal(keyLBatch.consumerId, 'c2');
    assert.equal(roundTwo.keyOwners.p0['key-k'], 'c1');
    assert.equal(roundTwo.keyOwners.p0['key-l'], 'c2');
  });

  it('allocates according to heterogeneous consumer capacity and reports finish time', () => {
    const plan = planRound(
      baseInput({
        now: 0,
        partitions: [
          partition('p0', 0, [msg(0, 'a', 't1'), msg(1, 'a', 't1'), msg(2, 'a', 't1')]),
          partition('p1', 0, [msg(0, 'b', 't1'), msg(1, 'b', 't1')]),
        ],
        consumers: [consumer('c1', 4), consumer('c2', 1)],
        tenants: [tenant('t1', 1)],
      }),
    );

    const work = Object.fromEntries(plan.assignments.map((entry) => [entry.consumerId, entry]));
    assert.equal(work.c1.workUnits, 3);
    assert.equal(work.c2.workUnits, 1);
    assert.equal(work.c1.estimatedFinishMs, 750);
    assert.equal(work.c2.estimatedFinishMs, 1000);
  });

  it('preserves retry order and counts retries inside batches', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            [msg(0, 'a', 't1'), msg(1, 'b', 't1'), msg(2, 'c', 't1'), msg(3, 'd', 't1'), msg(4, 'e', 't1'), msg(5, 'f', 't1')],
            {
              retries: [
                { offset: 0, attempt: 2 },
                { offset: 1, attempt: 2 },
                { offset: 2, attempt: 2 },
                { offset: 3, attempt: 2 },
              ],
            },
          ),
        ],
        consumers: [consumer('c1', 6)],
        tenants: [tenant('t1', 1)],
      }),
    );

    const batch = plan.assignments[0].batches[0];
    assert.equal(batch.batchSize, 6);
    assert.equal(batch.retryCount, 4);
    assert.deepEqual([batch.fromOffset, batch.toOffset], [0, 5]);
    assert.equal(plan.targetOffsets.p0, 6);
  });

  it('blocks a partition at the head while a retry is in backoff', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            [msg(0, 'a', 't1'), msg(1, 'b', 't1'), msg(2, 'c', 't1')],
            { retries: [{ offset: 0, attempt: 2, availableAt: 5000 }] },
          ),
          partition('p1', 0, [msg(0, 'x', 't2')]),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1), tenant('t2', 1)],
      }),
    );

    assert.equal(plan.targetOffsets.p0, 0);
    assert.equal(plan.targetOffsets.p1, 1);
    assert.deepEqual(plan.blockedPartitions, [
      { partitionId: 'p0', offset: 0, tenantId: 't1', availableAt: 5000, reason: 'retry_backoff' },
    ]);
    const unsatisfied = plan.unsatisfiedTenants.find((entry) => entry.tenantId === 't1');
    assert.equal(unsatisfied.reason, 'retry_backoff');
    assert.equal(unsatisfied.remainingMessages, 3);
  });

  it('quarantines poison messages instead of retrying forever', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'a', 't1', { attempt: 3 }),
            msg(1, 'b', 't1'),
          ]),
        ],
        consumers: [consumer('c1', 2)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.deepEqual(plan.quarantinedMessages, [
      {
        partitionId: 'p0',
        offset: 0,
        key: 'a',
        tenantId: 't1',
        attempt: 3,
        reason: 'max_attempts_exceeded',
        destination: 'dead_letter_queue',
      },
    ]);
    assert.equal(plan.targetOffsets.p0, 2);
    assert.equal(plan.assignments[0].batches[0].batchSize, 1);
  });

  it('quarantines poison messages even with no consumer capacity', () => {
    const plan = planRound(
      baseInput({
        partitions: [partition('p0', 0, [msg(0, 'a', 't1', { attempt: 3 }), msg(1, 'b', 't1')])],
        consumers: [consumer('c1', 0)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.quarantinedMessages.length, 1);
    assert.equal(plan.targetOffsets.p0, 1);
    assert.equal(plan.remainingBacklog.p0, 1);
    assert.equal(plan.assignments.length, 0);
  });

  it('quarantines poison messages found mid-stream after the head is processed', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'a', 't1'),
            msg(1, 'b', 't1', { attempt: 3 }),
            msg(2, 'c', 't1'),
          ]),
        ],
        consumers: [consumer('c1', 3)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.quarantinedMessages[0].offset, 1);
    assert.equal(plan.targetOffsets.p0, 3);
    assert.equal(plan.remainingBacklog.p0, 0);
  });

  it('enforces tenant quotas and reports quota conflicts', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [msg(0, 'a', 't1'), msg(1, 'b', 't1'), msg(2, 'c', 't1')]),
          partition('p1', 0, [msg(0, 'd', 't2'), msg(1, 'e', 't2'), msg(2, 'f', 't2')]),
        ],
        consumers: [consumer('c1', 6)],
        tenants: [tenant('t1', 1, 1), tenant('t2', 1)],
      }),
    );

    assert.equal(plan.totalPlannedMessages, 4);
    const t1 = plan.unsatisfiedTenants.find((entry) => entry.tenantId === 't1');
    assert.equal(t1.remainingMessages, 2);
    assert.equal(t1.plannedMessages, 1);
    assert.equal(t1.reason, 'tenant_quota_exhausted');
    assert.equal(plan.unsatisfiedTenants.some((entry) => entry.tenantId === 't2'), false);
    assert.deepEqual(plan.targetOffsets, { p0: 1, p1: 3 });
  });

  it('blocks a whole partition when its head message hits a quota (strict FIFO)', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'a', 't1'),
            msg(1, 'b', 't1'),
            msg(2, 'c', 't2'),
            msg(3, 'd', 't2'),
          ]),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1, 1), tenant('t2', 1)],
      }),
    );

    assert.equal(plan.targetOffsets.p0, 1);
    assert.equal(plan.blockedPartitions[0].reason, 'tenant_quota_exhausted');
    assert.equal(plan.blockedPartitions[0].offset, 1);
    const t2 = plan.unsatisfiedTenants.find((entry) => entry.tenantId === 't2');
    assert.equal(t2.reason, 'insufficient_consumer_capacity');
  });
});
