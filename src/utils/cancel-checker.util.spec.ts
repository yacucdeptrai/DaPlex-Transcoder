import { createCancelChecker } from './cancel-checker.util';

describe('createCancelChecker', () => {
  beforeEach(() => jest.useFakeTimers());
  afterEach(() => jest.clearAllTimers());

  it('does not invoke onCancel while the jobId is not in the cancelled list', () => {
    let ids: (string | number)[] = [1, 2, 3];
    const onCancel = jest.fn();
    const timer = createCancelChecker(() => ids, (v) => (ids = v), 10, onCancel, 1000);

    jest.advanceTimersByTime(5000);

    expect(onCancel).not.toHaveBeenCalled();
    expect(ids).toEqual([1, 2, 3]);
    clearInterval(timer);
  });

  it('invokes onCancel once the jobId appears in the cancelled list', () => {
    let ids: (string | number)[] = [10];
    const onCancel = jest.fn();
    const timer = createCancelChecker(() => ids, (v) => (ids = v), 10, onCancel, 1000);

    jest.advanceTimersByTime(1000);

    expect(onCancel).toHaveBeenCalledTimes(1);
    clearInterval(timer);
  });

  it('prunes the cancelled list to ids strictly greater than the jobId', () => {
    let ids: (string | number)[] = [5, 8, 10, 12];
    const onCancel = jest.fn();
    const timer = createCancelChecker(() => ids, (v) => (ids = v), 10, onCancel, 1000);

    jest.advanceTimersByTime(1000);

    expect(ids).toEqual([12]);
    clearInterval(timer);
  });

  it('matches ids by numeric value across string/number types', () => {
    let ids: (string | number)[] = ['10'];
    const onCancel = jest.fn();
    const timer = createCancelChecker(() => ids, (v) => (ids = v), 10, onCancel, 1000);

    jest.advanceTimersByTime(1000);

    expect(onCancel).toHaveBeenCalledTimes(1);
    clearInterval(timer);
  });

  it('stops triggering after the jobId has been pruned out', () => {
    let ids: (string | number)[] = [10];
    const onCancel = jest.fn();
    const timer = createCancelChecker(() => ids, (v) => (ids = v), 10, onCancel, 1000);

    jest.advanceTimersByTime(3000);

    expect(onCancel).toHaveBeenCalledTimes(1);
    clearInterval(timer);
  });
});
