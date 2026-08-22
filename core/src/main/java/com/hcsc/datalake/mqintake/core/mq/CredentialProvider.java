package com.hcsc.datalake.mqintake.core.mq;

import java.util.Optional;

/**
 * Abstraction for providing MQ credentials.
 *
 * <p>Implementations may retrieve credentials from:
 * <ul>
 *   <li>Environment variables</li>
 *   <li>Secret management systems (Vault, AWS Secrets Manager)</li>
 *   <li>Kubernetes secrets</li>
 *   <li>Configuration files (for development only)</li>
 * </ul>
 *
 * <p>This abstraction ensures credentials are never stored in plain text
 * in configuration files.
 */
public interface CredentialProvider {

    /**
     * Retrieves credentials for the given reference.
     *
     * @param credentialRef the credential reference (e.g., "env:MQ_PASSWORD" or "vault:mq/creds")
     * @return credentials if found and valid
     */
    Optional<Credentials> getCredentials(String credentialRef);

    /**
     * Container for username and password.
     */
    class Credentials {
        private final String username;
        private final String password;

        public Credentials(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        @Override
        public String toString() {
            return "Credentials{username='" + username + "', password='***'}";
        }
    }
}
