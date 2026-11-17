package dev.eventflow;

import dev.eventflow.worker.RetryPolicy;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {
    private final RetryPolicy policy = new RetryPolicy(5, 300);

    @Test void classifiesHttpAndNetworkFailures() {
        assertThat(policy.classify(200)).isEqualTo(RetryPolicy.Outcome.SUCCESS);
        assertThat(policy.classify(204)).isEqualTo(RetryPolicy.Outcome.SUCCESS);
        for (int status : new int[]{0, 408, 429, 500, 503})
            assertThat(policy.classify(status)).isEqualTo(RetryPolicy.Outcome.TRANSIENT_FAILURE);
        assertThat(policy.classify(400)).isEqualTo(RetryPolicy.Outcome.PERMANENT_FAILURE);
        assertThat(policy.classify(404)).isEqualTo(RetryPolicy.Outcome.PERMANENT_FAILURE);
    }

    @Test void delaysGrowAndStopAtCap() {
        assertThat(policy.delay(1).toSeconds()).isEqualTo(5);
        assertThat(policy.delay(2).toSeconds()).isEqualTo(10);
        assertThat(policy.delay(3).toSeconds()).isEqualTo(20);
        assertThat(policy.delay(20).toSeconds()).isEqualTo(300);
        assertThat(policy.shouldRetry(RetryPolicy.Outcome.TRANSIENT_FAILURE, 4, 5)).isTrue();
        assertThat(policy.shouldRetry(RetryPolicy.Outcome.TRANSIENT_FAILURE, 5, 5)).isFalse();
        assertThat(policy.shouldRetry(RetryPolicy.Outcome.PERMANENT_FAILURE, 1, 5)).isFalse();
    }
}
