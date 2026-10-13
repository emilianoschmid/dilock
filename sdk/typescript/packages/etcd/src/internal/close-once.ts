/** @internal Shares one provider shutdown operation among all concurrent callers. */
export function closeOnce(operation: () => Promise<void>): () => Promise<void> {
  let closing: Promise<void> | undefined;
  return () => {
    if (closing) return closing;
    try { closing = operation(); }
    catch (error) { closing = Promise.reject(error); }
    return closing;
  };
}
