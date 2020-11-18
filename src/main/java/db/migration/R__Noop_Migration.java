package db.migration;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-Op migration just to show you where Java migrations should be placed.
 */
public class R__Noop_Migration extends BaseJavaMigration {
    private static final Logger LOG = LoggerFactory.getLogger(R__Noop_Migration.class);

    @Override
    public void migrate(Context context) throws Exception {
        // Noop
    }
}
