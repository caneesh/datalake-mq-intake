package com.hcsc.datalake.mqintake.core.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Credential provider that retrieves credentials from environment variables.
 *
 * <p>Credential references are formatted as:
 * <ul>
 *   <li>{@code env:VAR_NAME} - reads from environment variable VAR_NAME</li>
 *   <li>{@code env:USER_VAR,PASS_VAR} - reads username from USER_VAR, password from PASS_VAR</li>
 * </ul>
 *
 * <p>For simple format, expects environment variable to contain "username:password".
 */
@Component
public class EnvironmentCredentialProvider implements CredentialProvider {

    private static final Logger log = LoggerFactory.getLogger(EnvironmentCredentialProvider.class);
    private static final String ENV_PREFIX = "env:";

    @Override
    public Optional<Credentials> getCredentials(String credentialRef) {
        if (credentialRef == null || credentialRef.isBlank()) {
            return Optional.empty();
        }

        if (!credentialRef.startsWith(ENV_PREFIX)) {
            log.debug("Credential ref '{}' is not an environment reference", credentialRef);
            return Optional.empty();
        }

        String envSpec = credentialRef.substring(ENV_PREFIX.length());

        if (envSpec.contains(",")) {
            return getCredentialsFromTwoVars(envSpec);
        } else {
            return getCredentialsFromSingleVar(envSpec);
        }
    }

    private Optional<Credentials> getCredentialsFromTwoVars(String envSpec) {
        String[] parts = envSpec.split(",", 2);
        if (parts.length != 2) {
            log.warn("Invalid env credential spec: {}", envSpec);
            return Optional.empty();
        }

        String userVar = parts[0].trim();
        String passVar = parts[1].trim();

        String username = System.getenv(userVar);
        String password = System.getenv(passVar);

        if (username == null || password == null) {
            log.warn("Environment variables not found: {} or {}", userVar, passVar);
            return Optional.empty();
        }

        log.debug("Retrieved credentials from env vars: {}, {}", userVar, passVar);
        return Optional.of(new Credentials(username, password));
    }

    private Optional<Credentials> getCredentialsFromSingleVar(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            log.warn("Environment variable not found: {}", envVar);
            return Optional.empty();
        }

        int colonIndex = value.indexOf(':');
        if (colonIndex < 0) {
            log.warn("Invalid credential format in {}: expected 'username:password'", envVar);
            return Optional.empty();
        }

        String username = value.substring(0, colonIndex);
        String password = value.substring(colonIndex + 1);

        log.debug("Retrieved credentials from env var: {}", envVar);
        return Optional.of(new Credentials(username, password));
    }
}
