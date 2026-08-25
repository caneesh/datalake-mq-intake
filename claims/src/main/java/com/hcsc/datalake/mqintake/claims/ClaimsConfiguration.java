package com.hcsc.datalake.mqintake.claims;

import com.hcsc.datalake.mqintake.claims.serializer.ClaimsIdentityExtractor;
import com.hcsc.datalake.mqintake.claims.serializer.ClaimsRecordSerializer;
import com.hcsc.datalake.mqintake.core.config.ProductionMode;
import com.hcsc.datalake.mqintake.core.orchestration.RecordSerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Claims-specific configuration for record serialization.
 *
 * <p>LAND_ONLY mode — no TrackerMessageBuilderFactory needed.
 *
 * <p><strong>Claims identity (open item #17):</strong> The stable claims
 * payload identity is unresolved. It must be configured explicitly via
 * {@code claims.identity-field} (e.g. {@code CLM_XMITSN_ID} or
 * {@code REC_CTL_NBR}) once approved. There is no silent default:
 * <ul>
 *   <li>Production mode + no configured identity → startup FAILS</li>
 *   <li>Non-production + no configured identity → non-production fixture
 *       extractor with a loud warning; reconciliation must not be trusted</li>
 * </ul>
 */
@Configuration
public class ClaimsConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ClaimsConfiguration.class);

    private final String identityField;
    private final boolean productionMode;

    @Autowired
    public ClaimsConfiguration(
            @Value("${claims.identity-field:}") String identityField,
            ProductionMode productionMode) {
        this(identityField, productionMode.isEnabled());
    }

    /**
     * Visible-for-testing constructor with explicit production mode.
     */
    public ClaimsConfiguration(String identityField, boolean productionMode) {
        this.identityField = identityField == null ? "" : identityField.trim();
        this.productionMode = productionMode;
    }

    // Default constructor kept for existing tests that build the config directly.
    public ClaimsConfiguration() {
        this("", false);
    }

    @Bean
    public RecordSerializerFactory recordSerializerFactory() {
        ClaimsIdentityExtractor extractor = resolveIdentityExtractor();
        boolean failOnMissing = isIdentityConfigured();
        return config -> new ClaimsRecordSerializer(extractor, failOnMissing);
    }

    /**
     * Resolves the identity extractor from configuration, enforcing the
     * production gate.
     *
     * @throws IllegalStateException in production mode when no identity field
     *                               is configured
     */
    ClaimsIdentityExtractor resolveIdentityExtractor() {
        if (isIdentityConfigured()) {
            log.info("Claims identity field configured: {}", identityField);
            return ClaimsIdentityExtractor.forTag(identityField);
        }

        if (productionMode) {
            throw new IllegalStateException(
                    "Claims identity field is not configured (claims.identity-field). " +
                    "Production startup is blocked: reconciliation requires an approved " +
                    "stable identity (open item #17). Configure CLM_XMITSN_ID, " +
                    "REC_CTL_NBR, or the approved field once confirmed.");
        }

        log.warn("Claims identity field NOT configured — using NON-PRODUCTION fixture " +
                "extractor (CLM_XMITSN_ID, REC_CTL_NBR). Reconciliation output must " +
                "not be trusted until claims.identity-field is set.");
        return ClaimsIdentityExtractor.nonProductionFixture();
    }

    /**
     * Returns true when an explicit identity field has been configured.
     * Reconciliation readiness for claims requires this to be true.
     */
    public boolean isIdentityConfigured() {
        return !identityField.isEmpty();
    }

    public String getIdentityField() {
        return identityField;
    }
}
