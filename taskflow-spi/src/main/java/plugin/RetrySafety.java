package plugin;

/**
 * Declares whether repeating one logical processor task is operationally safe.
 *
 * <p>The declaration is part of the paired server/executor plugin contract.
 * The coordinator trusts the server-side declaration when deciding whether a
 * job may use its configured retry policy; plugin contract tests must keep it
 * equal to the executor-side declaration.</p>
 */
public enum RetrySafety {
    /** Processing has no plugin-owned durable effect outside its returned result. */
    PURE,

    /** Repeating processing is safe because any plugin-owned effect is idempotent. */
    IDEMPOTENT,

    /**
     * Repeating processing is safe only when the plugin supplies a documented
     * TaskFlow execution identity as an idempotency key to its external system.
     */
    REQUIRES_IDEMPOTENCY_KEY,

    /** Processing can produce an external effect that cannot safely be repeated. */
    UNSAFE_TO_RETRY;

    /** Returns whether the coordinator may apply its configured task retry policy. */
    public boolean permitsAutomaticRetry() {
        return this != UNSAFE_TO_RETRY;
    }
}
