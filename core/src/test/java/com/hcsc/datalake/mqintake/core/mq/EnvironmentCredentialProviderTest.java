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
        String userEnv = System.getenv("USER");
        String homeEnv = System.getenv("HOME");

        if (userEnv != null && homeEnv != null) {
            Optional<CredentialProvider.Credentials> result = provider.getCredentials("env:USER,HOME");

            assertTrue(result.isPresent());
            assertEquals(userEnv, result.get().getUsername());
            assertEquals(homeEnv, result.get().getPassword());
        }
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
