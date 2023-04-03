"""TranslationType Enum."""
from enum import Enum

from google.cloud import bigquery_migration_v2


class TranslationType(Enum):
    """An Enum representing BQMS translation types."""

    # Uppercase is not used here to match the BQMS API.
    # pylint: disable=invalid-name

    Translation_AzureSynapse2BQ = bigquery_migration_v2.Dialect(
        azure_synapse_dialect=bigquery_migration_v2.AzureSynapseDialect()
    )
    Translation_Bteq2BQ = bigquery_migration_v2.Dialect(
        teradata_dialect=bigquery_migration_v2.TeradataDialect(
            mode=bigquery_migration_v2.TeradataDialect.Mode.BTEQ
        )
    )
    Translation_HiveQL2BQ = bigquery_migration_v2.Dialect(
        hiveql_dialect=bigquery_migration_v2.HiveQLDialect()
    )
    Translation_MySQL2BQ = bigquery_migration_v2.Dialect(
        mysql_dialect=bigquery_migration_v2.MySQLDialect()
    )
    Translation_Netezza2BQ = bigquery_migration_v2.Dialect(
        netezza_dialect=bigquery_migration_v2.NetezzaDialect()
    )
    Translation_Oracle2BQ = bigquery_migration_v2.Dialect(
        oracle_dialect=bigquery_migration_v2.OracleDialect()
    )
    Translation_Presto2BQ = bigquery_migration_v2.Dialect(
        presto_dialect=bigquery_migration_v2.PrestoDialect()
    )
    Translation_Postgresql2BQ = bigquery_migration_v2.Dialect(
        postgresql_dialect=bigquery_migration_v2.PostgresqlDialect()
    )
    Translation_Redshift2BQ = bigquery_migration_v2.Dialect(
        redshift_dialect=bigquery_migration_v2.RedshiftDialect()
    )
    Translation_Snowflake2BQ = bigquery_migration_v2.Dialect(
        snowflake_dialect=bigquery_migration_v2.SnowflakeDialect()
    )
    Translation_SparkSQL2BQ = bigquery_migration_v2.Dialect(
        sparksql_dialect=bigquery_migration_v2.SparkSQLDialect()
    )
    Translation_SQLServer2BQ = bigquery_migration_v2.Dialect(
        sql_server_dialect=bigquery_migration_v2.SQLServerDialect()
    )
    Translation_Teradata2BQ = bigquery_migration_v2.Dialect(
        teradata_dialect=bigquery_migration_v2.TeradataDialect(
            mode=bigquery_migration_v2.TeradataDialect.Mode.SQL
        )
    )
    Translation_Vertica2BQ = bigquery_migration_v2.Dialect(
        vertica_dialect=bigquery_migration_v2.VerticaDialect()
    )

    # pylint: enable=invalid-name

    def __repr__(self) -> str:
        return self.name
