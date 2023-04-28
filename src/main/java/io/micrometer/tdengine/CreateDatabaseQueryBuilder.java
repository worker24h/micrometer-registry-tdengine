/**
 * Refer to CreateDatabaseQueryBuilder for this class
 */
package io.micrometer.tdengine;

import io.micrometer.core.lang.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;

class CreateDatabaseQueryBuilder {

    private List<KeyValueOptions> options = new ArrayList<>();

    private static final String CREATE_DATABASE_PREFIX = "CREATE DATABASE IF NOT EXISTS %s";
    private static final String PRECISION_CLAUSE_TEMPLATE = " PRECISION '%s'";

    private final String databaseName;
    private String precision = "ms";

    public CreateDatabaseQueryBuilder(String databaseName) {
        if (isEmpty(databaseName)) {
            throw new IllegalArgumentException("The database name cannot be null or empty");
        }
        this.databaseName = databaseName;
    }

    public CreateDatabaseQueryBuilder setOptions(List<KeyValueOptions> options) {
        this.options = options;
        return this;
    }

    public CreateDatabaseQueryBuilder setPrecision(String precision) {
        this.precision = precision;
        return this;
    }


    public String build() {
        StringBuilder queryStringBuilder = new StringBuilder(String.format(CREATE_DATABASE_PREFIX, databaseName));
        if (!options.isEmpty()) {
            options.stream().filter(Objects::nonNull).forEach(o -> {
                queryStringBuilder.append(" ").append(o.getKey()).append(" ").append(o.getValue());
            });
        }
        queryStringBuilder.append(String.format(PRECISION_CLAUSE_TEMPLATE, precision));
        return queryStringBuilder.toString();
    }

    private boolean isEmpty(@Nullable String string) {
        return string == null || string.isEmpty();
    }

}
