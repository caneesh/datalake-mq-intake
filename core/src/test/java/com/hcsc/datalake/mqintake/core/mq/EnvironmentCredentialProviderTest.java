package com.hcsc.datalake.mqintake.core.mq;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class EnvironmentCredentialProviderTest {

    private final EnvironmentCredentialProvider provider = new EnvironmentCredentialProvider();

    @Test
    void returnsEmptyForNullRef() {
        Optional<CredentialProvider.Credentials> result = provider.getCredentials(null);

        assertTrue(result.isEmpty());
    }

    @Test
    void returnsEmptyForBlankRef() {
        Optional<CredentialProvider.Credentials> result = provider.getCredentials("   ");

        assertTrue(result.isEmpty());
    }

    @Test
    void returnsEmptyForNonEnvPrefix() {
        Optional<CredentialProvider.Credentials> result = provider.getCredentials("vault:secret/mq");

        assertTrue(result.isEmpty());
    }

    @Test
    void returnsEmptyWhenEnvVarNotSet() {
        Optional<CredentialProvider.Credentials> result = provider.getCredentials("env:NONEXISTENT_VAR_12345");

        assertTrue(result.isEmpty());
    }

    @Test
    void returnsEmptyWhenTwoVarFormatAndUserVarMissing() {
        Optional<CredentialProvider.Credentials> result = provider.getCredentials("env:NONEXISTENT_USER_VAR,NONEXISTENT_PASS_VAR");

        assertTrue(result.isEmpty());
    }

    @Test
    void parsesTwoVarFormat() {
        // Was conditional on USER and HOME being set, so on a container
        // without them the assertions were skipped and the test passed having
        // checked nothing.
        CredentialProvider stubbed = withEnv(java.util.Map.of(
                "MQ_USER", "svc_dmih_rms", "MQ_PASSWORD", "s3cr3t"));

        Optional<CredentialProvider.Credentials> result =
                stubbed.getCredentials("env:MQ_USER,MQ_PASSWORD");

        assertTrue(result.isPresent());
        assertEquals("svc_dmih_rms", result.get().getUsername());
        assertEquals("s3cr3t", result.get().getPassword());
    }

    @Test
    void theSingleVariableFormSplitsOnTheFIRSTColonSoPasswordsMayContainColons() {
        // DEPLOYMENT.md tells operators exactly this. Nothing held it:
        // credentialsWithColonInPassword builds a Credentials object and reads
        // its getters back, which never touches the parse.
        CredentialProvider stubbed = withEnv(java.util.Map.of(
                "MQ_CREDS", "svc_dmih_rms:p4ss:w1th:colons"));

        Optional<CredentialProvider.Credentials> result = stubbed.getCredentials("env:MQ_CREDS");

        assertTrue(result.isPresent());
        assertEquals("svc_dmih_rms", result.get().getUsername());
        assertEquals("p4ss:w1th:colons", result.get().getPassword());
    }

    @Test
    void aSingleVariableWithNoColonResolvesToNothing() {
        CredentialProvider stubbed = withEnv(java.util.Map.of("MQ_CREDS", "just-a-username"));

        assertTrue(stubbed.getCredentials("env:MQ_CREDS").isEmpty());
    }

    @Test
    void aTwoVariableRefWithOnlyThePasswordSetResolvesToNothing() {
        // Refused rather than half-populated; MqConnectionManager turns the
        // empty result into a refusal to connect.
        CredentialProvider stubbed = withEnv(java.util.Map.of("MQ_PASSWORD", "s3cr3t"));

        assertTrue(stubbed.getCredentials("env:MQ_USER,MQ_PASSWORD").isEmpty());
    }

    @Test
    void aShortMalformedRefResolvesToNothingRatherThanThrowing() {
        // The prefix check also guards the substring that follows it. Without
        // it a ref shorter than "env:" throws StringIndexOutOfBoundsException
        // instead of returning empty — the operator still gets a refusal,
        // because resolveCredentials turns a RuntimeException into one, but a
        // stack trace replaces the message that names the reference.
        CredentialProvider stubbed = withEnv(java.util.Map.of());

        assertTrue(stubbed.getCredentials("x").isEmpty());
        assertTrue(stubbed.getCredentials("env").isEmpty());
    }

    /** A provider reading a fixed map instead of the ambient environment. */
    private CredentialProvider withEnv(java.util.Map<String, String> values) {
        return new EnvironmentCredentialProvider(values::get);
    }

    @Test
    void credentialsContainUsernameAndPassword() {
        CredentialProvider.Credentials creds = new CredentialProvider.Credentials("admin", "secret123");

        assertEquals("admin", creds.getUsername());
        assertEquals("secret123", creds.getPassword());
    }

    @Test
    void credentialsWithColonInPassword() {
        CredentialProvider.Credentials creds = new CredentialProvider.Credentials("user", "pass:with:colons");

        assertEquals("user", creds.getUsername());
        assertEquals("pass:with:colons", creds.getPassword());
    }

    @Test
    void credentialProviderInterfaceContract() {
        CredentialProvider stubProvider = ref -> {
            if ("known-ref".equals(ref)) {
                return Optional.of(new CredentialProvider.Credentials("mquser", "mqpass"));
            }
            return Optional.empty();
        };

        Optional<CredentialProvider.Credentials> known = stubProvider.getCredentials("known-ref");
        Optional<CredentialProvider.Credentials> unknown = stubProvider.getCredentials("unknown-ref");

        assertTrue(known.isPresent());
        assertEquals("mquser", known.get().getUsername());
        assertEquals("mqpass", known.get().getPassword());

        assertTrue(unknown.isEmpty());
    }
}
