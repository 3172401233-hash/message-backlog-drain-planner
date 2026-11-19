export { planRound, UNSATISFIED_REASONS } from './planner.js';
export { validateInput } from './validate.js';

/**
 * Build a contiguous message stream between two offsets.
 * Handy for tests and callers that keep offsets dense.
 */
export function buildMessages(currentOffset, endOffset, fill) {
  const messages = [];
  for (let offset = currentOffset; offset < endOffset; offset += 1) {
    const entry = typeof fill === 'function' ? fill(offset, offset - currentOffset) : { ...fill };
    messages.push({ ...entry, offset });
  }
  return messages;
}
