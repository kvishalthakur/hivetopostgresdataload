 enum ApplicationConfig
{
  APP_CONFIG_PG_DB_SOURCE(
    "ami.pipelines.config.pg_db_source"), 

  APP_CONFIG_PG_DB_SINK(
    "ami.pipelines.config.pg_db_sink"), 

  APP_CONFIG_HIGHUSAGEALERT_NUMBER_OF_PAST_MONTHS(
    "ami.pipelines.config.highusagealert.number_of_past_months"), 

  APP_CONFIG_HIGHUSAGEALERT_ZEE_SCORE_SIGMA(
    "ami.pipelines.config.highusagealert.zee_score_sigma"), 

  APP_CONFIG_HIGHUSAGEALERT_MINIMUM_USAGE(
    "ami.pipelines.config.highusagealert.minimum_usage"), 

  APP_CONFIG_HIGHUSAGEALERT_ENDPOINT_URL(
    "ami.pipelines.config.highusagealert.endpoint_url"), 

  APP_CONFIG_LEAKALERT_ENDPOINT_URL(
    "ami.pipelines.config.leakalert.endpoint_url"), 

  APP_CONFIG_LEAKALERT_INTERVALHOURS(
    "ami.pipelines.config.leakalert.intervalHours"), 

  APP_CONFIG_LEAKALERT_MINIMUMCONSUMPTION(
    "ami.pipelines.config.leakalert.minimumConsumption"), 

  APP_CONFIG_USAGE_PREDICTION_NUMBER_OF_PAST_MONTHS(
    "ami.pipelines.config.usage_prediction.number_of_past_months"), 

  APP_CONFIG_USAGE_PREDICTION_ZEE_SCORE_SIGMA(
    "ami.pipelines.config.usage_prediction.zee_score_sigma"), 

  APP_CONFIG_PG_DB_SINK_USERNAME(
    "ami.pipelines.config.pg_db_sink_username"), 

  APP_CONFIG_PG_DB_SINK_PASSWORD(
    "ami.pipelines.config.pg_db_sink_password"), 

  APP_CONFIG_PG_DB_SINK_JDBC_URI(
    "ami.pipelines.config.pg_db_sink_jdbc_uri"), 

  APP_CONFIG_HIVE_DATASOURCENAME(
    "ami.pipelines.config.hive_datasourceName"), 

  APP_CONFIG_HIVE_CLUSTERNAME(
    "ami.pipelines.config.hive_clusterName"), 

  APP_CONFIG_HIVE_JDBC_URI(
    "ami.pipelines.config.hive_jdbc_uri"), 

  APP_CONFIG_HIVE_KERBEROS_PRINCIPAL(
    "ami.pipelines.config.hive_kerberos_principal"), 

  APP_CONFIG_HIVE_KERBEROS_KEYTAB_FILEPATH(
    "ami.pipelines.config.hive_kerberos_keytab_filepath"), 

  APP_CONFIG_COUNTIES_LIST(
    "ami.pipeline.config.counties_list"), 

  APP_CONFIG_TRIGGER_DAILY_DAYS_MINUS(
    "ami.pipeline.config.trigger_daily.days_minus"), 
  APP_CONFIG_TRIGGER_HOURLY_DAYS_MINUS(
    "ami.pipeline.config.trigger_hourly.days_minus"), 

  AMI_SECUREAUTH_CONFIG_OAUTH_URL(
    "ami.secureauth.config.oauth_url"), 

  AMI_SECUREAUTH_CONFIG_GRANT_TYPE(
    "ami.secureauth.config.grant_type"), 

  AMI_SECUREAUTH_CONFIG_USERNAME(
    "ami.secureauth.config.optional.username"), 

  AMI_SECUREAUTH_CONFIG_PASSWORD(
    "ami.secureauth.config.optional.password"), 

  AMI_SECUREAUTH_CONFIG_CLIENT_ID(
    "ami.secureauth.config.client_id"), 

  AMI_SECUREAUTH_CONFIG_CLIENT_SECRET(
    "ami.secureauth.config.client_secret"), 

  AMI_SECUREAUTH_CONFIG_SCOPE(
    "ami.secureauth.config.scope"), 

  AMI_SCHEMA_SOURCE_ACLARA_READINGS(
    "ami.schema.source.aclara_readings");

  private String key;

  private ApplicationConfig(String key)
  {
    this.key = key;
  }

  public String getKey() {
    return this.key;
  }
}