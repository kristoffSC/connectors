package io.delta.flink.internal.table;

import java.util.Map.Entry;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.delta.standalone.types.StructType;

public final class CatalogExceptionHelper {

    private CatalogExceptionHelper() {}

    static CatalogException deltaLogAndDdlSchemaMismatchException(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            StructType ddlDeltaSchema,
            StructType deltaSchema) {

        return new CatalogException(
            String.format(
                " Delta table [%s] from filesystem path [%s] has different schema "
                    + "that was defined in CREATE TABLE DDL.\n"
                    + "DDL schema [%s],\n"
                    + "_delta_log schema [%s]",
                catalogTablePath,
                deltaTablePath,
                ddlDeltaSchema.getTreeString(),
                deltaSchema.getTreeString())
        );
    }

    static CatalogException jobSpecificOptionInDdlException(String ddlOption) {
        String message = String.format(
            "DDL contains Job Specific option %s. Job specific options can be used only via Query"
                + " hints.\nJob specific options are:\n%s",
            ddlOption,
            String.join(", ", DeltaFlinkJobSpecificOptions.JOB_OPTIONS)
        );
        return new CatalogException(message);
    }

    static CatalogException invalidOptionInDdl(String ddlOption) {
        String message = String.format(
            "DDL contains invalid option %s. DDL can have delta table properties only or "
                + "arbitrary user options",
            ddlOption);
        return new CatalogException(message);
    }

    static CatalogException ddlAndDeltaLogOptionMismatchException(
            ObjectPath catalogTablePath,
            Entry<String, String> ddlOption,
            String deltaLogPropertyValue) {
        return new CatalogException(
            String.format(
                "DDL option %s for table %s has different value than _delta_log table "
                    + "property.\n"
                    + "Value from DDL: %s, value from _delta_log %s",
                ddlOption.getKey(), catalogTablePath.getFullName(),
                ddlOption.getValue(), deltaLogPropertyValue));
    }
}
