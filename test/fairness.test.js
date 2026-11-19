import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { planRound } from '../src/index.js';
import { baseInput, consumer, flattenBatches, msg, partition, tenant } from './helpers.js';

describe('fairness, retry storms and projection', () => {
  it('shares capacity across tenants by weight instead of letting one tenant starve', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 10 }, (_, offset) => msg(offset, `a-${offset}`, 't1')),
          ),
          partition(
            'p1',
            0,
            Array.from({ length: 10 }, (_, offset) => msg(offset, `b-${offset}`, 't2')),
          ),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1), tenant('t2', 1)],
      }),
    );

    const t1 = plan.assignments
      .flatMap((entry) => entry.batches)
      .filter((batch) => batch.partitionId === 'p0')
      .reduce((sum, batch) => sum + batch.batchSize, 0);
    const t2 = plan.assignments
      .flatMap((entry) => entry.batches)
      .filter((batch) => batch.partitionId === 'p1')
      .reduce((sum, batch) => sum + batch.batchSize, 0);
    assert.equal(t1, 2);
    assert.equal(t2, 2);
    assert.ok(plan.unsatisfiedTenants.every((entry) => entry.reason === 'insufficient_consumer_capacity'));
  });

  it('serves weighted tenants proportionally', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 12 }, (_, offset) => msg(offset, `a-${offset}`, 't1')),
          ),
          partition(
            'p1',
            0,
            Array.from({ length: 12 }, (_, offset) => msg(offset, `b-${offset}`, 't2')),
          ),
        ],
        consumers: [consumer('c1', 6)],
        tenants: [tenant('t1', 2), tenant('t2', 1)],
      }),
    );

    const counts = {};
    for (const entry of plan.unsatisfiedTenants) {
      counts[entry.tenantId] = entry.plannedMessages;
    }
    assert.equal(counts.t1, 4);
    assert.equal(counts.t2, 2);
  });

  it('handles a retry storm without bypassing fresh messages or breaking order', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            [
              ...Array.from({ length: 6 }, (_, offset) =>
                msg(offset, `retry-${offset}`, 't1', { isRetry: true, attempt: 2 }),
              ),
              ...Array.from({ length: 4 }, (_, offset) => msg(offset + 6, `fresh-${offset}`, 't2')),
            ],
            { retries: Array.from({ length: 6 }, (_, offset) => ({ offset, attempt: 2 })) },
          ),
          partition(
            'p1',
            0,
            Array.from({ length: 10 }, (_, offset) => msg(offset, `other-${offset}`, 't2')),
          ),
        ],
        consumers: [consumer('c1', 5), consumer('c2', 5)],
        tenants: [tenant('t1', 1), tenant('t2', 1)],
      }),
    );

    const batches = flattenBatches(plan);
    const p0 = batches.filter((batch) => batch.partitionId === 'p0');
    assert.equal(p0[0].fromOffset, 0);
    assert.ok(p0.every((batch) => batch.retryCount === batch.batchSize || batch.fromOffset > 5));
    const allP0Offsets = p0.flatMap((batch) =>
      Array.from({ length: batch.toOffset - batch.fromOffset + 1 }, (_, index) => batch.fromOffset + index),
    );
    assert.deepEqual(allP0Offsets, [...allP0Offsets].sort((a, b) => a - b));
    const servedT2 = plan.unsatisfiedTenants.find((entry) => entry.tenantId === 't2');
    assert.ok(servedT2.plannedMessages > 0);
  });

  it('projects remaining backlog including incoming production spikes', () => {
    const plan = planRound(
      baseInput({
        roundDurationMs: 1000,
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 10 }, (_, offset) => msg(offset, `k-${offset}`, 't1')),
            { produceRate: 3.4 },
          ),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.remainingBacklog.p0, 6);
    assert.equal(plan.projectedRemaining.p0, 10);
    assert.equal(plan.totalProjectedRemaining, 10);
    assert.equal(plan.estimatedDrainTimeMs, 16666.666666666664);
  });

  it('estimates a finite drain time when consumers outpace production', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 10 }, (_, offset) => msg(offset, `k-${offset}`, 't1')),
            { produceRate: 1 },
          ),
        ],
        consumers: [consumer('c1', 3)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.estimatedDrainTimeMs, 4000);
  });

  it('drains partitions that have had no traffic for a long time', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [msg(0, 'a', 't1'), msg(1, 'b', 't1')], { produceRate: 0 }),
          partition('p1', 9, [], { produceRate: 0 }),
        ],
        consumers: [consumer('c1', 4)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.deepEqual(plan.targetOffsets, { p0: 2, p1: 9 });
    assert.deepEqual(plan.remainingBacklog, { p0: 0, p1: 0 });
    assert.equal(plan.estimatedDrainTimeMs, null);
  });

  it('respects the global available work units cap and reports the conflict', () => {
    const plan = planRound(
      baseInput({
        availableWorkUnits: 2,
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 5 }, (_, offset) => msg(offset, `k-${offset}`, 't1')),
          ),
        ],
        consumers: [consumer('c1', 10)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.totalPlannedMessages, 2);
    assert.equal(plan.unsatisfiedTenants[0].reason, 'insufficient_available_work_units');
  });

  it('supports weighted message costs when allocating batches', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition('p0', 0, [
            msg(0, 'a', 't1', { cost: 2 }),
            msg(1, 'b', 't1', { cost: 1 }),
            msg(2, 'c', 't1', { cost: 1 }),
          ]),
        ],
        consumers: [consumer('c1', 3)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.assignments[0].workUnits, 3);
    assert.equal(plan.targetOffsets.p0, 2);
    assert.equal(plan.remainingBacklog.p0, 1);
  });

  it('rejects malformed input with explicit errors', () => {
    const valid = baseInput({
      partitions: [partition('p0', 0, [msg(0, 'a', 't1')])],
      consumers: [consumer('c1', 1)],
      tenants: [tenant('t1', 1)],
    });

    assert.throws(() => planRound({ ...valid, partitions: [] }), /partitions/);
    assert.throws(() => planRound({ ...valid, consumers: [] }), /consumers/);
    assert.throws(() => planRound({ ...valid, tenants: [] }), /tenants/);
    assert.throws(
      () =>
        planRound({
          ...valid,
          partitions: [partition('p0', 0, [msg(0, 'a', 'unknown')])],
        }),
      /unknown tenant/,
    );
    assert.throws(
      () =>
        planRound({
          ...valid,
          pinnedKeys: { p0: { a: 'ghost-consumer' } },
        }),
      /unknown consumer/,
    );
  });
});
