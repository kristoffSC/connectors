package io.delta.flink.internal.table;

import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

// TODO DC - consider extending CatalogException for more concrete types like
//  "DeltaSchemaMismatchException" etc.
public final class CatalogExceptionHelper {

    private CatalogExceptionHelper() {}

    public static CatalogException deltaLogAndDdlSchemaMismatchException(
            ObjectPath catalogTablePath,
            String deltaTablePath,
            Metadata deltaMetadata,
            StructType ddlDeltaSchema,
            List<String> ddlPartitions) {

        String deltaSchemaString = (deltaMetadata.getSchema() == null)
            ? "null"
            : deltaMetadata.getSchema().getTreeString();

        return new CatalogException(
            String.format(
                " Delta table [%s] from filesystem path [%s] has different schema or partition "
                    + "spec that one defined in CREATE TABLE DDL.\n"
                    + "DDL schema:\n[%s],\n_delta_log schema:\n[%s]\n"
                    + "DDL partition spec:\n[%s],\n_delta_log partition spec\n[%s]\n",
                catalogTablePath,
                deltaTablePath,
                ddlDeltaSchema.getTreeString(),
                deltaSchemaString,
                ddlPartitions,
                deltaMetadata.getPartitionColumns())
        );
    }

    public static CatalogException jobSpecificOptionInDdlException(String ddlOption) {
        String message = String.format(
            "DDL contains Job Specific option %s. Job specific options can be used only via Query"
                + " hints.\nJob specific options are:\n%s",
            ddlOption,
            String.join(", ", DeltaFlinkJobSpecificOptions.SOURCE_JOB_OPTIONS)
        );
        return new CatalogException(message);
    }

    public static CatalogException invalidOptionInDdl(Collection<String> invalidOptions) {
        String message = String.format(
            "DDL contains invalid options. DDL can have delta table properties or "
                + "arbitrary user options only.\nInvalid options used:\n%s",
            String.join("\n", invalidOptions));
        return new CatalogException(message);
    }

    public static CatalogException ddlAndDeltaLogOptionMismatchException(
            ObjectPath catalogTablePath,
            List<DDLAndDeltaLogMismatchedOption> invalidOptions) {

        StringJoiner invalidOptionsString = new StringJoiner("\n");
        for (DDLAndDeltaLogMismatchedOption invalidOption : invalidOptions) {
            invalidOptionsString.add(
                String.join(
                    " | ",
                    invalidOption.optionName,
                    invalidOption.ddlOptionValue,
                    invalidOption.deltaLogOptionValue
                )
            );
        }

        return new CatalogException(
            String.format(
                "Invalid DDL options for table [%s]. "
                    + "DDL options for Delta table connector cannot override table properties "
                    + "already defined in _delta_log.\n"
                    + "DDL option name | DDL option value | Delta option value \n%s",
                catalogTablePath.getFullName(),
                invalidOptionsString
            )
        );
    }

    public static class DDLAndDeltaLogMismatchedOption {

        private final String optionName;

        private final String ddlOptionValue;

        private final String deltaLogOptionValue;

        public DDLAndDeltaLogMismatchedOption(
                String optionName,
                String ddlOptionValue,
                String deltaLogOptionValue) {
            this.optionName = optionName;
            this.ddlOptionValue = ddlOptionValue;
            this.deltaLogOptionValue = deltaLogOptionValue;
        }
    }
}
