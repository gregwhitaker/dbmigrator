package com.github.gregwhitaker.dbmigrator.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks integration tests for database tables.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseTableTest {

    /**
     * Name of the database table that is associated with this annotated test class.
     *
     * @return database table name
     */
    String tableName();
}
