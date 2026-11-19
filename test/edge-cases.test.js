import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { planRound } from '../src/index.js';
import { baseInput, consumer, msg, partition, tenant } from './helpers.js';

describe('edge cases', () => {
  it('limits each consumer to its maxBatchSize and splits batches per partition', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 6 }, (_, offset) => msg(offset, `k-${offset}`, 't1')),
          ),
        ],
        consumers: [consumer('c1', 6, 2), consumer('c2', 6, 2)],
        tenants: [tenant('t1', 1)],
      }),
    );

    const sizes = plan.assignments.map((entry) => entry.batches.reduce((sum, batch) => sum + batch.batchSize, 0));
    assert.deepEqual(sizes, [2, 2]);
    assert.equal(plan.targetOffsets.p0, 4);
    assert.equal(plan.remainingBacklog.p0, 2);
  });

  it('returns an empty plan when every consumer has zero capacity', () => {
    const plan = planRound(
      baseInput({
        partitions: [partition('p0', 0, [msg(0, 'a', 't1'), msg(1, 'b', 't1')])],
        consumers: [consumer('c1', 0)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.deepEqual(plan.assignments, []);
    assert.equal(plan.targetOffsets.p0, 0);
    assert.equal(plan.unsatisfiedTenants[0].reason, 'insufficient_consumer_capacity');
  });

  it('processes a retry once its backoff window elapses in a later round', () => {
    const roundOne = planRound(
      baseInput({
        now: 1000,
        partitions: [
          partition(
            'p0',
            0,
            [msg(0, 'a', 't1'), msg(1, 'b', 't1')],
            { retries: [{ offset: 0, attempt: 2, availableAt: 5000 }] },
          ),
        ],
        consumers: [consumer('c1', 2)],
        tenants: [tenant('t1', 1)],
      }),
    );
    assert.equal(roundOne.targetOffsets.p0, 0);

    const roundTwo = planRound(
      baseInput({
        round: 2,
        now: 5000,
        partitions: [
          partition(
            'p0',
            0,
            [msg(0, 'a', 't1'), msg(1, 'b', 't1')],
            { retries: [{ offset: 0, attempt: 2, availableAt: 5000 }] },
          ),
        ],
        consumers: [consumer('c1', 2)],
        tenants: [tenant('t1', 1)],
      }),
    );
    assert.equal(roundTwo.targetOffsets.p0, 2);
    assert.equal(roundTwo.blockedPartitions.length, 0);
    assert.equal(roundTwo.assignments[0].batches[0].retryCount, 1);
  });

  it('keeps retry messages ahead of fresh messages in the same partition', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            [
              msg(0, 'retry-key', 't1', { isRetry: true, attempt: 2 }),
              msg(1, 'fresh-key', 't2'),
              msg(2, 'fresh-key-2', 't2'),
            ],
            { retries: [{ offset: 0, attempt: 2 }] },
          ),
        ],
        consumers: [consumer('c1', 2)],
        tenants: [tenant('t1', 1), tenant('t2', 1)],
      }),
    );

    const plannedOffsets = plan.assignments
      .flatMap((entry) => entry.batches)
      .flatMap((batch) =>
        Array.from({ length: batch.toOffset - batch.fromOffset + 1 }, (_, index) => batch.fromOffset + index),
      );
    assert.deepEqual(plannedOffsets, [0, 1]);
    assert.equal(plan.targetOffsets.p0, 2);
  });

  it('rounds projected incoming messages up per partition', () => {
    const plan = planRound(
      baseInput({
        partitions: [
          partition(
            'p0',
            0,
            Array.from({ length: 3 }, (_, offset) => msg(offset, `k-${offset}`, 't1')),
            { produceRate: 0.2 },
          ),
        ],
        consumers: [consumer('c1', 1)],
        tenants: [tenant('t1', 1)],
      }),
    );

    assert.equal(plan.remainingBacklog.p0, 2);
    assert.equal(plan.projectedRemaining.p0, 3);
  });
});
