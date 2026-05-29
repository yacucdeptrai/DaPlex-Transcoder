/**
 * Polls a list of cancelled job ids on an interval. When the given jobId appears
 * in the list, it prunes the list (keeping only ids strictly greater than jobId),
 * invokes onCancel, and keeps the interval running (the caller clears it).
 *
 * Extracted from the previously-duplicated cancel-poll logic in VideoService and
 * videoSourceHelper. The getter/setter preserve the original semantics of
 * reassigning the cancelled-ids array (e.g. `this.CanceledJobIds = ...filter(...)`).
 */
export function createCancelChecker(
  getCanceledJobIds: () => (string | number)[],
  setCanceledJobIds: (ids: (string | number)[]) => void,
  jobId: string | number,
  onCancel: () => void,
  ms: number = 5000
): NodeJS.Timeout {
  return setInterval(() => {
    const ids = getCanceledJobIds();
    if (ids.findIndex((j) => +j === +jobId) === -1) return;
    setCanceledJobIds(ids.filter((id) => +id > +jobId));
    onCancel();
  }, ms);
}
