package com.hcsc.datalake.mqintake.core.mq;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Credential resolution must fail closed.
 *
 * <p>Previously, a configured {@code credential-ref} whose lookup failed
 * logged a warning and then opened an <em>unauthenticated</em> connection.
 * That is a silent security downgrade triggered by exactly the conditions
 * where it is least welcome — a credential-store outage, or a rotation that
 * removed the entry.
 */
class MqCredentialFailClosedTest {

    private static final String CONNECTION_ID = "primary";
    private static final String REF = "vault:mq/primary";
    private static final String SECRET = "s3cr3t-should-never-be-logged";

    @Test
    void credentialRefConfiguredAndAvailableResolvesNormally() throws Exception {
        CredentialProvider provider = ref -> Optional.of(
                new CredentialProvider.Credentials("mq-app", SECRET));

        Optional<CredentialProvider.Credentials> creds =
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, provider);

        assertThat(creds).isPresent();
        assertThat(creds.get().getUsername()).isEqualTo("mq-app");
        assertThat(creds.get().getPassword()).isEqualTo(SECRET);
    }

    @Test
    void credentialRefConfiguredButMissingRefusesToConnect() {
        CredentialProvider provider = ref -> Optional.empty();

        assertThatThrownBy(() ->
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, provider))
                .isInstanceOf(MqConnectionManager.MqCredentialException.class)
                .hasMessageContaining(REF)
                .hasMessageContaining(CONNECTION_ID)
                .hasMessageContaining("Refusing to fall back to an unauthenticated connection");
    }

    @Test
    void credentialProviderFailureRefusesToConnect() {
        CredentialProvider provider = ref -> {
            throw new IllegalStateException("vault unreachable");
        };

        assertThatThrownBy(() ->
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, provider))
                .isInstanceOf(MqConnectionManager.MqCredentialException.class)
                .hasMessageContaining("Refusing to connect without credentials")
                .hasMessageContaining("vault unreachable");
    }

    @Test
    void providerReturningNullIsTreatedAsAFailureNotAsNoCredentials() {
        // A provider that returns null rather than Optional.empty() must not
        // be read as "no credentials required".
        CredentialProvider provider = ref -> null;

        assertThatThrownBy(() ->
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, provider))
                .isInstanceOf(MqConnectionManager.MqCredentialException.class);
    }

    @Test
    void incompleteCredentialIsRejectedRatherThanSentToTheQueueManager() {
        CredentialProvider missingPassword = ref -> Optional.of(
                new CredentialProvider.Credentials("mq-app", ""));
        assertThatThrownBy(() ->
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, missingPassword))
                .isInstanceOf(MqConnectionManager.MqCredentialException.class)
                .hasMessageContaining("password missing");

        CredentialProvider missingUser = ref -> Optional.of(
                new CredentialProvider.Credentials(null, SECRET));
        assertThatThrownBy(() ->
                MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, missingUser))
                .isInstanceOf(MqConnectionManager.MqCredentialException.class)
                .hasMessageContaining("username missing");
    }

    @Test
    void noCredentialRefKeepsTheUnauthenticatedPath() throws Exception {
        CredentialProvider provider = ref -> {
            throw new AssertionError("provider must not be consulted without a credential-ref");
        };

        // Empty means "connect without credentials", which is the intended
        // behaviour when no credential-ref is configured at all.
        assertThatCode(() -> assertThat(
                MqConnectionManager.resolveCredentials(null, CONNECTION_ID, provider)).isEmpty())
                .doesNotThrowAnyException();
        assertThat(MqConnectionManager.resolveCredentials("", CONNECTION_ID, provider)).isEmpty();
        assertThat(MqConnectionManager.resolveCredentials("   ", CONNECTION_ID, provider)).isEmpty();
    }

    @Test
    void secretsNeverAppearInErrorMessages() {
        // Every failure path is checked, because one leaky message is enough
        // to put a live MQ password into a log aggregator.
        CredentialProvider leakyProvider = ref -> {
            throw new IllegalStateException("failed while handling " + SECRET);
        };
        CredentialProvider incomplete = ref -> Optional.of(
                new CredentialProvider.Credentials("mq-app", ""));
        CredentialProvider empty = ref -> Optional.empty();

        assertThat(messageOf(() -> MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, empty)))
                .doesNotContain(SECRET);
        assertThat(messageOf(() -> MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, incomplete)))
                .doesNotContain(SECRET);

        // This one is worth stating plainly: the provider itself put the
        // secret into its own exception message, and we propagate that
        // message. We do not scrub it, because we cannot know a caller's
        // secret format — the contract is that providers must not put secrets
        // in exception text.
        assertThat(messageOf(() -> MqConnectionManager.resolveCredentials(REF, CONNECTION_ID, leakyProvider)))
                .contains(SECRET);
    }

    @Test
    void credentialsToStringDoesNotRevealThePassword() {
        CredentialProvider.Credentials creds =
                new CredentialProvider.Credentials("mq-app", SECRET);

        assertThat(creds.toString()).doesNotContain(SECRET).contains("***");
    }

    private String messageOf(ThrowingCall call) {
        try {
            call.run();
            return "";
        } catch (Exception e) {
            return String.valueOf(e.getMessage());
        }
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
