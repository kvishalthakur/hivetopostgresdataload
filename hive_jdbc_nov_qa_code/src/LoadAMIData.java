import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.apporchid.hive.BasicFormatterImpl;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.gson.JsonParser;

public class LoadAMIData
{
  private static String intervalSuffix = "";

  private static final String PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME = System.getProperty("ami.config.file", "application.properties");
  private static final String CONTENT_TYPE_ALERTS_REST = "application/json; charset=utf-8";
  private static final Boolean IS_LOG_QUERY = Boolean.valueOf(Files.exists(Paths.get("/apporchid/ami_jobs/.debug.ami", new String[0]), new LinkOption[] { LinkOption.NOFOLLOW_LINKS }));
  public static Logger logger;
  private static PropertiesConfiguration applicationConfig = null;

  private static HttpClient client = null;

  private static final Integer MINUS_MONTH_THRESHOLD = Integer.valueOf(13);

  static
  {
    Properties properties = new Properties();

    properties.setProperty("log4j.rootLogger", "TRACE,stdout");
    properties.setProperty("log4j.rootCategory", "DEBUG");

    properties.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
    properties.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
    properties.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yyyy/MM/dd HH:mm:ss.SSS} [%5p] %t (%F) - %m%n");

    PropertyConfigurator.configure(properties);

    logger = Logger.getRootLogger();

    showFiglet();

    if (System.getProperty("skipDefaults", "true").matches("true|TRUE|True|1|ON|On|on|yes|Yes|YES")) {
      System.setProperty("HADOOP_HOME", "C:/HADOOP_HOME/");
      System.setProperty("hadoop.home.dir", "C:/HADOOP_HOME/");
      System.setProperty("java.security.krb5.conf", "D:/hive_jdbc_nov_qa_code/hive-jdbc/krb5.ini");
    }

    System.setProperty("sun.security.krb5.debug", "true");
    try {
      initialize();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void initialize()
    throws Exception
  {
    logger.info("+===============================================+");
    logger.info("+ ami.config.file ==> " + (IS_LOG_QUERY.booleanValue() ? Paths.get(PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME, new String[0]).toAbsolutePath().toString() : PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME));
    logger.info("+===============================================+");

    Path path = Paths.get(PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME, new String[0]);
    Charset charset = StandardCharsets.UTF_8;
    try
    {
      String content = new String(Files.readAllBytes(path), charset);
      content = content.replaceAll("\\\\,", ",");
      content = content.replaceAll(",", "\\\\,");
      Files.write(path, content.getBytes(charset), new OpenOption[0]);
    } catch (IOException e1) {
      e1.printStackTrace();
      throw e1;
    }
    try
    {
      applicationConfig = new PropertiesConfiguration(PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME);
      logger.info("This program will try to connect to the source cluster =>" + applicationConfig.getString("ami.pipelines.config.hive_clusterName"));

      applicationConfig.setDelimiterParsingDisabled(true);

      boolean isGrantTypePassword = getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_GRANT_TYPE).equals("password");

      for (ApplicationConfig a : ApplicationConfig.values()) {
        logger.info(" :: app_config => key : " + a.getKey() + " || value : '" + (a.getKey().matches(".*password.*|.*secret.*") ? "xxxxxxxxxxxx" : IS_LOG_QUERY.booleanValue() ? "sha256://" + Hashing.sha256().hashString(getConfigValue(a), StandardCharsets.UTF_8) : getConfigValue(a)) + "'");
        if (a.getKey().contains("optional")) {
          if ((isGrantTypePassword) && (a.getKey().matches(".*username|.*password")))
            Objects.requireNonNull(getConfigValue(a), "Cannot proceed. Incomplete configuration, Secure Auth grant type is password, config expected grant username / password to be available => '" + a.getKey() + "' in " + PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME);
        }
        else
          Objects.requireNonNull(getConfigValue(a), "Cannot proceed. Incomplete configuration, missing value for key => '" + a.getKey() + "' in " + PROGRAM_CONFIGURATION_PROPERTIES_FILE_NAME);
      }
    }
    catch (ConfigurationException e)
    {
      logger.warn("Can't initialize stub application - reason =" + e.getMessage());
      logger.error(e, e.getCause());
      throw e;
    }
    try
    {
      client = HttpClients.custom().setSslcontext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy()
      {
        public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException
        {
          return true;
        }
      }).build()).setDefaultHeaders(Lists.newArrayList(new BasicHeader[] { 
        new BasicHeader("Accept", "application/json") })).build();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public static String getConfigValue(ApplicationConfig c)
  {
    return applicationConfig.getString(c.getKey());
  }

  public static void main2(String[] args)
    throws Exception
  {
	  System.out.println(getTime());
    //logger.info(getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_JDBC_URI));
    //logger.info(Boolean.valueOf("jdbc:hive2://hsynlhdps202.amwaternp.net:2181,hsynlhdps200.amwaternp.net:2181,hsynlhdps201.amwaternp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2".matches(getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_JDBC_URI))));
    //logger.info(Boolean.valueOf("jdbc:hive2://staplhdpsm002.amwater.net:2181,staplhdpsm005.amwater.net:2181,staplhdpsm006.amwater.net:2181,staplhdpsm003.amwater.net:2181,staplhdpsm004.amwater.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2".matches(getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_JDBC_URI))));
  }

  public static void main(String[] args)
    throws Exception
  {
    System.setProperty("java.net.debug", "true");

    boolean loadHourlyData = Boolean.parseBoolean(System.getProperty("ami.isLoadHourlyData", "false"));
    boolean loadHourlyDataWV = Boolean.parseBoolean(System.getProperty("ami.isLoadHourlyDataWV", "false"));
    boolean loadDailyData = Boolean.parseBoolean(System.getProperty("ami.isLoadDailyData", "false"));
    boolean loadDailyDataWV = Boolean.parseBoolean(System.getProperty("ami.isLoadDailyDataWV", "false"));
    boolean runHighUsageAlerts = Boolean.parseBoolean(System.getProperty("ami.isRunHighUsageAlerts", "false"));
    boolean runLeakAlerts = Boolean.parseBoolean(System.getProperty("ami.isRunLeakAlerts", "false"));
    boolean isUpdateMeterNextRead = Boolean.parseBoolean(System.getProperty("ami.isUpdateMeterNextRead", "false"));
    boolean isRunUsagePrediction = Boolean.parseBoolean(System.getProperty("ami.isRunUsagePrediction", "false"));

    Connection pgConn = null;
    Connection hiveConn = null;

    pgConn = getPostgresConnection(getConfigValue(ApplicationConfig.APP_CONFIG_PG_DB_SINK_JDBC_URI), getConfigValue(ApplicationConfig.APP_CONFIG_PG_DB_SINK_USERNAME), getConfigValue(ApplicationConfig.APP_CONFIG_PG_DB_SINK_PASSWORD));

    boolean isZookeeperConnected = false;
    if (loadHourlyData) {
      hiveConn = zookeeperConnect();
      isZookeeperConnected = true;
      updateHourlyData(hiveConn, pgConn);
    }
    
    if (loadHourlyDataWV) {
        if (!isZookeeperConnected) {
          hiveConn = zookeeperConnect();
        }
        loadHourlyDataWV(hiveConn, pgConn);
      }

    if (loadDailyData) {
      if (!isZookeeperConnected) {
        hiveConn = zookeeperConnect();
      }
      updateDailyData(hiveConn, pgConn);
    }
    
    if (loadDailyDataWV) {
        if (!isZookeeperConnected) {
          hiveConn = zookeeperConnect();
        }
        updateDailyDataForWV(hiveConn, pgConn);
      }

    if (isUpdateMeterNextRead) {
      if (!isZookeeperConnected) {
        hiveConn = zookeeperConnect();
      }
      updateMeterNextRead(hiveConn, pgConn);
    }

    if (isRunUsagePrediction) {
      logger.info("Running Usage Prediction Algorithm begin at " + LocalDateTime.now());
      double zScore = Double.parseDouble(getConfigValue(ApplicationConfig.APP_CONFIG_USAGE_PREDICTION_ZEE_SCORE_SIGMA));

      updateUsagePrediction(pgConn, Double.valueOf(zScore));
      logger.info("Running Usage Prediction Algorithm ends at " + LocalDateTime.now());
    }

    if (runHighUsageAlerts) {
      logger.info("High Usage Calculation started at " + new Date());
      double zScore = 
        Double.parseDouble(getConfigValue(ApplicationConfig.APP_CONFIG_HIGHUSAGEALERT_ZEE_SCORE_SIGMA));
      int numOfPastMonths = 
        Integer.parseInt(getConfigValue(ApplicationConfig.APP_CONFIG_HIGHUSAGEALERT_NUMBER_OF_PAST_MONTHS));
      int minimumUsage = 
        Integer.parseInt(getConfigValue(ApplicationConfig.APP_CONFIG_HIGHUSAGEALERT_MINIMUM_USAGE));
      getHighUsage(pgConn, zScore, numOfPastMonths, minimumUsage);
    }

    if (runLeakAlerts) {
      logger.info("Leaks Calculation started at" + new Date());
      int intervalHours = Integer.parseInt(getConfigValue(ApplicationConfig.APP_CONFIG_LEAKALERT_INTERVALHOURS));
      int minimumConsumption = Integer.parseInt(getConfigValue(ApplicationConfig.APP_CONFIG_LEAKALERT_MINIMUMCONSUMPTION));
      getLeaks(pgConn, intervalHours, minimumConsumption);
    }

    if (hiveConn != null) {
      hiveConn.close();
    }

    if (pgConn != null) {
      pgConn.close();
    }
    logger.debug("finished at " + new Date());
  }

  private static void getLeaks(Connection pgConn, int intervalHours, int minimumConsumption) throws Exception {
    String leaksSQL = "select businesspartnernumber,cast (contractaccount as varchar) from app.meter_ami_reads_hourly where    reading_datetime> current_timestamp - interval '" + 
      intervalHours + " hour' " + 
      "group by businesspartnernumber,contractaccount " + 
      "having min(consumption)> " + minimumConsumption;

    debugQuery(leaksSQL);
    ResultSet rs2 = pgConn.createStatement().executeQuery(leaksSQL);

    String token = getSecureAuthToken();
    while (rs2.next()) {
      String businessPartner = rs2.getString(1);
      String contractAccount = rs2.getString(2);
      logger.info(businessPartner + "," + contractAccount);
      String apiUrl = getConfigValue(ApplicationConfig.APP_CONFIG_LEAKALERT_ENDPOINT_URL);
      callLeaksRESTService(pgConn, apiUrl, AccionPayloadAmi.build(businessPartner, contractAccount), token);
    }
  }

  private static String getCountiesListSQLString() {
    StringBuffer result = new StringBuffer("");

    for (String county : getConfigValue(ApplicationConfig.APP_CONFIG_COUNTIES_LIST).split(",")) {
      result.append("'").append(county.trim()).append("',");
    }
    return StringUtils.chop(result.toString());
  }

  private static void getHighUsage(Connection pgConn, double zScore, int numOfPastMonths, int minimumUsage) throws Exception
  {
    String highUsageSQL = "SELECT mar.businesspartnernumber, mar.contractaccount, trim(leading '0' FROM mar.equipmentnumber) , mr2.last_read_time, sum(mar.consumption), ami_calc.high_usage_limit FROM app.meter_ami_reads_daily mar JOIN ( SELECT mv2.meter_id, mv2.business_partner_number, mv2.connection_contract_number, max(meter_reading_time) last_read_time FROM app.meter_readings_v2 mr2 JOIN app.meters_v2 mv2 ON mv2.meter_id = mr2.meter_id AND mv2.installation = mr2.installation AND mv2.register = mr2.register AND mv2.isactive = 'Yes' AND mv2.region = 'WV' AND meter_reading_reason = '01' join app.meter_endpoint_type met on mv2.end_point_type_1 = met.id and met.endpoint_group = 'AMI' GROUP BY mv2.meter_id, mv2.business_partner_number, mv2.connection_contract_number) mr2 ON mr2.meter_id = trim(leading '0' FROM mar.equipmentnumber) AND mr2.business_partner_number = mar.businesspartnernumber AND mr2.connection_contract_number = cast (mar.contractaccount AS varchar) JOIN ( SELECT mv2.meter_id, mv2.register, mv2.business_partner_number, mv2.connection_contract_number, sum(consumption_gl) total_consumption, round(avg(consumption_gl):: numeric,2) avg_consumption, round(stddev_pop(consumption_gl):: numeric,2) std_dev_consumption, round((avg(consumption_gl)+2.5*stddev_pop(consumption_gl))::numeric,2) high_usage_limit, extract( day FROM max(meter_reading_time) - min(meter_reading_time)) total_days, sum(consumption_gl)/extract( day FROM max(meter_reading_time) - min(meter_reading_time)) per_day_consumption, min(meter_reading_time) start_date, max(meter_reading_time) end_date, mnr.next_read_date FROM app.meter_readings_v2 mr2 JOIN app.meters_v2 mv2 ON mv2.meter_id = mr2.meter_id AND mv2.installation = mr2.installation AND mv2.register = mr2.register AND mv2.isactive = 'Yes' AND mv2.region = 'WV' AND meter_reading_reason = '01' AND meter_reading_time > CURRENT_TIMESTAMP - interval '15 month' join app.meter_endpoint_type met on mv2.end_point_type_1 = met.id and met.endpoint_group = 'AMI' JOIN app.meter_next_read mnr ON mnr.meter_reading_unit = mr2.meter_reading_unit GROUP BY mv2.meter_id, mv2.register, mv2.business_partner_number, mv2.connection_contract_number, mnr.next_read_date) ami_calc ON ami_calc.meter_id = trim(leading '0' FROM mar.equipmentnumber) AND ami_calc.business_partner_number = mar.businesspartnernumber AND ami_calc.connection_contract_number = cast (mar.contractaccount AS varchar) WHERE mar.reading_datetime > mr2.last_read_time GROUP BY mar.businesspartnernumber, mar.contractaccount, mar.equipmentnumber, mr2.last_read_time, ami_calc.high_usage_limit HAVING ami_calc.high_usage_limit < sum(mar.consumption) AND sum(mar.consumption) > 3000";

    debugQuery(highUsageSQL);
    ResultSet rs2 = pgConn.createStatement().executeQuery(highUsageSQL);
    logger.info("retrieved data");
    String token = getSecureAuthToken();

    while (rs2.next()) {
      String businessPartner = rs2.getString(1);
      String contractAccount = rs2.getString(2);
      String equipmentNumber = rs2.getString(3);
      Timestamp lastReadTime = rs2.getTimestamp(4);
      Double currentUsage = Double.valueOf(rs2.getDouble(5));
      Double highUsageLimit = Double.valueOf(rs2.getDouble(6));
      logger.info(businessPartner + "," + contractAccount + "," + equipmentNumber + "," + lastReadTime + "," + currentUsage + "," + highUsageLimit);
      String apiUrl = getConfigValue(ApplicationConfig.APP_CONFIG_HIGHUSAGEALERT_ENDPOINT_URL);

      callHighUsageRESTService(pgConn, apiUrl, AccionPayloadAmi.build(businessPartner, contractAccount), token, equipmentNumber, lastReadTime, currentUsage, highUsageLimit);
    }
  }

  private static void updateHourlyData(Connection hiveConn, Connection pgConn) throws SQLException
  {
    String hourlyDaysThreshold = getConfigValue(ApplicationConfig.APP_CONFIG_TRIGGER_HOURLY_DAYS_MINUS);

    logger.info("hourlyDaysThreshold from config => " + hourlyDaysThreshold);

    if (!hourlyDaysThreshold.startsWith("-")) {
      throw new RuntimeException("Can not work with the non negative -> actual" + hourlyDaysThreshold);
    }

    String aclaraSourceExternalSchemaName = getConfigValue(ApplicationConfig.AMI_SCHEMA_SOURCE_ACLARA_READINGS);

    String maxHourlyReadingDatesInPostgresSink = getMaxHourlyReadingDateInSink(pgConn);

    String hourlySQL = "With tmp as  (select distinct meter_id, transponder_id, transponder_port, cast(customer_id AS String) as functionallocation,                       reading_value, unit_of_measure, reading_datetime, timezone, battery_voltage,            round(reading_value - lag(reading_value, 1) OVER (partition by  meter_id ORDER BY reading_datetime),2) consumption,            unix_timestamp(reading_datetime)- unix_timestamp(lag(reading_datetime, 1) OVER (partition by  meter_id ORDER BY reading_datetime)) read_interval,           ingest_watermark from " + 
      aclaraSourceExternalSchemaName + ".aclara_readings " + 
      "        where reading_datetime > '2018-07-19 00:00:00'" + 
      "          ) " + 
      "        Select distinct " + 
      "               tmp.meter_id as headend_meter_id, " + 
      "               tmp.functionallocation, " + 
      "               tmp.reading_datetime, " + 
      "               tmp.timezone, " + 
      "               tmp.reading_value, " + 
      "               tmp.unit_of_measure, " + 
      "               tmp.consumption, " + 
      "               tmp.read_interval, " + 
      "               imd.equipmentnumber, " + 
      "               imd.installation, " + 
      "               imd.register, " + 
      "               imd.logicalregisternumber, " + 
      "               cmd.businesspartnernumber, " + 
      "               cmd.contractaccount, " + 
      "               cmd.utilitiescontract, " + 
      "               cmd.regionalstructuregrouping " + 
      "        from tmp  " + 
      "        Inner Join awinternal.locationmasterdata lmd on tmp.functionallocation = lmd.functionallocation  " + 
      "        inner join awinternal.installedmeterdetails imd on imd.devicelocation = lmd.functionallocation  " + 
      "               and current_date between imd.devicevaliditystartdate and imd.devicevalidityenddate " + 
      "        inner join cloudseer.mv2_temp cmd on  cmd.equipmentnumber =imd.equipmentnumber " + 
      "              and current_date between cmd.devicevaliditystartdate and cmd.devicevalidityenddate " + 
      "              and current_date between cmd.utilitiesmoveindate and cmd.utilitiesmoveoutdate " + 
      "              and imd.register = cmd.register " +  
      "        order by headend_meter_id, reading_datetime , imd.register ";

    debugQuery(hourlySQL);
    ResultSet rs1 = hiveConn.createStatement().executeQuery(hourlySQL);
    updateAMIData("HOURLY" + intervalSuffix, pgConn, rs1, maxHourlyReadingDatesInPostgresSink);
    rs1.close();
  }
  
  private static void loadHourlyDataWV(Connection hiveConn, Connection pgConn) throws SQLException
  {
    String hourlyDaysThreshold = getConfigValue(ApplicationConfig.APP_CONFIG_TRIGGER_HOURLY_DAYS_MINUS);

    logger.info("hourlyDaysThreshold from config => " + hourlyDaysThreshold);

    if (!hourlyDaysThreshold.startsWith("-")) {
      throw new RuntimeException("Can not work with the non negative -> actual" + hourlyDaysThreshold);
    }

    String aclaraSourceExternalSchemaName = getConfigValue(ApplicationConfig.AMI_SCHEMA_SOURCE_ACLARA_READINGS);

    String maxHourlyReadingDatesInPostgresSink = getMaxHourlyReadingDateInSink(pgConn);

    String hourlySQL = "WITH tmp AS (SELECT DISTINCT meter_id, miu_serial_number as transponder_id, '1' as transponder_port, cast(account_id as string) as utilities_premise, reading as reading_value, '' as unit_of_measure, read_date_time as reading_datetime, 'GMT' as timezone, '' as battery_voltage, Round(reading - Lag(reading, 1) over (PARTITION BY meter_id ORDER BY read_date_time), 2) consumption, Unix_timestamp(read_date_time) - Unix_timestamp(Lag(read_date_time, 1) over (PARTITION BY meter_id ORDER BY read_date_time)) read_interval, ingest_watermark FROM awexternal.ami_datamatic_readings WHERE read_date_time > '"+maxHourlyReadingDatesInPostgresSink+"') SELECT DISTINCT tmp.meter_id AS headend_meter_id, tmp.utilities_premise, tmp.reading_datetime, tmp.timezone, tmp.reading_value, tmp.unit_of_measure, tmp.consumption, tmp.read_interval, imd.equipmentnumber, imd.installation, imd.register, imd.logicalregisternumber, cmd.businesspartnernumber, cmd.contractaccount, cmd.utilitiescontract, cmd.regionalstructuregrouping, cmd.devicelocation FROM tmp inner join awinternal.utilitiesinstallation ui on tmp.utilities_premise = ui.utilitiespremise and ui.division <> 'SW' inner join awinternal.installedmeterdetails imd on ui.utilitiesinstallation = imd.installation and current_date BETWEEN imd.devicevaliditystartdate AND imd.devicevalidityenddate and current_date BETWEEN imd.registervaliditystartdate AND imd.registervalidityenddate inner join cloudseer.mv2_temp cmd ON cmd.equipmentnumber = imd.equipmentnumber AND current_date BETWEEN cmd.devicevaliditystartdate AND cmd.devicevalidityenddate AND current_date BETWEEN cmd.utilitiesmoveindate AND cmd.utilitiesmoveoutdate AND imd.register = cmd.register ORDER BY headend_meter_id, reading_datetime, imd.register";

    debugQuery(hourlySQL);
    ResultSet rs1 = hiveConn.createStatement().executeQuery(hourlySQL);
    updateAMIDataWV("HOURLY" + intervalSuffix, pgConn, rs1, maxHourlyReadingDatesInPostgresSink);
    rs1.close();
  }

  private static String getMaxHourlyReadingDateInSink(Connection pgConn) throws SQLException {
    String result = null;
    String readDateSQL = "SELECT TO_CHAR(max(reading_datetime), 'YYYY-MM-DD 00:00:00') FROM app.meter_ami_reads_hourly";
    debugQuery(readDateSQL);
    ResultSet sourceResultSet = pgConn.createStatement().executeQuery(readDateSQL);
    if (sourceResultSet.next()) {
      result = sourceResultSet.getString(1);
    }
    return result;
  }
  
  
  
  private static void updateDailyDataForWV(Connection hiveConn, Connection pgConn) throws SQLException{
	String sql = "WITH tmp AS "
			+ "(SELECT DISTINCT ar.meter_id,"
			+ " ar.miu_serial_number as transponder_id,"
			+ " '1' as transponder_port,"
			+ " cast(account_id as string) as utilities_premise,"
			+ " reading as reading_value,"
			+ " '' as unit_of_measure,"
			+ " ar.read_date_time as reading_datetime,"
			+ " 'GMT' as timezone,"
			+ " '' as battery_voltage,"
			+ " ingest_watermark"
			+ " FROM awexternal.ami_datamatic_readings ar"
			+ " join"
			+ " (SELECT meter_id,"
			+ " Max(read_date_time) reading_datetime,"
			+ " Concat(Year(read_date_time), '-', Month(read_date_time), '-',Day(read_date_time)) meterreadingday"
			+ " FROM awexternal.ami_datamatic_readings"
			+ " WHERE read_date_time BETWEEN Date_add('2019-05-13', -62) AND '2019-05-13'"
			+ " GROUP BY meter_id,"
			+ "Concat(Year(read_date_time), '-', Month(read_date_time), '-', Day(read_date_time)) ) last_row"
			+ " ON last_row.meter_id = ar.meter_id"
			+ " AND last_row.reading_datetime = ar.read_date_time)"
			+ " SELECT DISTINCT tmp.meter_id AS headend_meter_id,"
			+ " tmp.utilities_premise,"
			+ " tmp.reading_datetime,"
			+ " tmp.timezone,"
			+ " tmp.reading_value,"
			+ " tmp.unit_of_measure,"
			+ " Round(reading_value - Lag(reading_value, 1) over (PARTITION BY meter_id ORDER BY reading_datetime), 2) as consumption,"
			+ " Unix_timestamp(reading_datetime) - Unix_timestamp(Lag(reading_datetime, 1) over (PARTITION BY meter_id ORDER BY reading_datetime)) as read_interval,"
			+ " imd.equipmentnumber,"
			+ " imd.installation,"
			+ " imd.register,"
			+ " imd.logicalregisternumber,"
			+ " cmd.businesspartnernumber,"
			+ " cmd.contractaccount,"
			+ " cmd.utilitiescontract,"
			+ " cmd.regionalstructuregrouping,"
			+ " cmd.devicelocation"
			+ " FROM tmp"
			+ " inner join"
			+ " awinternal.utilitiesinstallation ui"
			+ " on tmp.utilities_premise = ui.utilitiespremise"
			+ " and ui.division <> 'SW'"
			+ " inner join"
			+ " awinternal.installedmeterdetails imd"
			+ " on ui.utilitiesinstallation = imd.installation"
			+ " and current_date BETWEEN imd.devicevaliditystartdate"
			+ " AND imd.devicevalidityenddate"
			+ " and current_date BETWEEN imd.registervaliditystartdate"
			+ " AND imd.registervalidityenddate"
			+ " inner join"
			+ " cloudseer.mv2_temp cmd"
			+ " ON cmd.equipmentnumber = imd.equipmentnumber"
			+ " AND current_date BETWEEN cmd.devicevaliditystartdate AND cmd.devicevalidityenddate"
			+ " AND current_date BETWEEN cmd.utilitiesmoveindate AND cmd.utilitiesmoveoutdate"
			+ " AND imd.register = cmd.register"
			+ " ORDER BY"
			+ " headend_meter_id, reading_datetime, imd.register";
	
	ResultSet rs = hiveConn.createStatement().executeQuery(sql);

	updateAMIDataWV("DAILY" + intervalSuffix, pgConn, rs, null);
    rs.close();

  }

  private static void updateDailyData(Connection hiveConn, Connection pgConn) throws SQLException
  {
    String dailyDaysThreshold = getConfigValue(ApplicationConfig.APP_CONFIG_TRIGGER_DAILY_DAYS_MINUS);

    logger.info("dailyDaysThreshold from config => " + dailyDaysThreshold);

    if (!dailyDaysThreshold.startsWith("-")) {
      throw new RuntimeException("Can not work with the non negative -> actual" + dailyDaysThreshold);
    }

    String aclaraSourceExternalSchemaName = getConfigValue(ApplicationConfig.AMI_SCHEMA_SOURCE_ACLARA_READINGS);

    String sql = "With tmp as  (select distinct ar.meter_id, transponder_id, transponder_port, cast(customer_id AS String) as functionallocation,               reading_value, unit_of_measure, ar.reading_datetime, timezone, battery_voltage, ingest_watermark    from " + 
      aclaraSourceExternalSchemaName + ".aclara_readings ar " + 
      "  join ( " + 
      "        select meter_id, max(reading_datetime) reading_datetime, " + 
      "        concat(year(reading_datetime), '-', month(reading_datetime),'-',day(reading_datetime)) meterreadingday " + 
      "        from " + aclaraSourceExternalSchemaName + ".aclara_readings  " + 
      "      where reading_datetime between date_add('2018-07-20'," + dailyDaysThreshold + ") and '2018-07-20' " + 
      "        group by meter_id, concat(year(reading_datetime), '-', month(reading_datetime),'-',day(reading_datetime)) " + 
      "        ) last_row " + 
      "  on last_row.meter_id = ar.meter_id and last_row.reading_datetime = ar.reading_datetime " + 
      ") " + 
      "Select distinct " + 
      "       tmp.meter_id as headend_meter_id, " + 
      "       tmp.functionallocation, " + 
      "       tmp.reading_datetime, " + 
      "       tmp.timezone, " + 
      "       tmp.reading_value, " + 
      "       tmp.unit_of_measure, " + 
      "       round(reading_value - lag(reading_value, 1) OVER (partition by  meter_id ORDER BY reading_datetime),2) consumption, " + 
      "       unix_timestamp(reading_datetime)- unix_timestamp(lag(reading_datetime, 1) OVER (partition by  meter_id ORDER BY reading_datetime))  read_interval, " + 
      "       imd.equipmentnumber, " + 
      "       imd.installation, " + 
      "       imd.register, " + 
      "       imd.logicalregisternumber, " + 
      "       cmd.businesspartnernumber, " + 
      "       cmd.contractaccount, " + 
      "       cmd.utilitiescontract, " + 
      "       cmd.regionalstructuregrouping " + 
      "from tmp " + 
      "Inner Join awinternal.locationmasterdata lmd on tmp.functionallocation = lmd.functionallocation  " + 
      "inner join awinternal.installedmeterdetails imd on imd.devicelocation = lmd.functionallocation  " + 
      "       and current_date between imd.devicevaliditystartdate and imd.devicevalidityenddate " + 
      "inner join cloudseer.mv2_temp cmd on  cmd.equipmentnumber =imd.equipmentnumber " + 
      "      and current_date between cmd.devicevaliditystartdate and cmd.devicevalidityenddate " + 
      "      and current_date between cmd.utilitiesmoveindate and cmd.utilitiesmoveoutdate " + 
      "      and imd.register = cmd.register " + 
      "order by headend_meter_id, reading_datetime , imd.register ";

    debugQuery(sql);

    ResultSet rs = hiveConn.createStatement().executeQuery(sql);

    updateAMIData("DAILY" + intervalSuffix, pgConn, rs, null);
    rs.close();
  }

  private static void debugQuery(String sql) {
    if (IS_LOG_QUERY.booleanValue()) {
      logger.info("QUERY => " + sql);
      logger.warn("FormattedQuery" + BasicFormatterImpl.format(sql));
    }
  }

  private static List<String> getPatchDeleteQueryWhenRunningDeltaLoad()
  {
    String dailyTablesDeleteQuery = null;
    String hourlyTablesDeleteQuery = null;

    LocalDate minimumKeepDate = LocalDate.now().minusMonths(MINUS_MONTH_THRESHOLD.intValue()).with(TemporalAdjusters.firstDayOfMonth());
    DateTimeFormatter sqlDateFormatter = DateTimeFormatter.ISO_LOCAL_DATE;

    dailyTablesDeleteQuery = "DELETE FROM app.meter_ami_reads_daily  where reading_datetime < '" + minimumKeepDate.format(sqlDateFormatter) + "' OR read_interval != '86400'";
    hourlyTablesDeleteQuery = "DELETE FROM app.meter_ami_reads_hourly where reading_datetime < '" + minimumKeepDate.format(sqlDateFormatter) + "' OR read_interval != '3600'";

    return Arrays.asList(new String[] { dailyTablesDeleteQuery, hourlyTablesDeleteQuery });
  }

  private static void updateAMIData(String readIntervalType, Connection pgConn, ResultSet sourceResultSet, String maxHourlyReadingDatesInPostgresSinkToSlice) throws SQLException {
    int count = 0;
    if (sourceResultSet.next())
    {
      String insertSQL = null;

      if ("DAILY".equals(readIntervalType)) {
        insertSQL = "INSERT INTO app.meter_ami_reads_daily (headend_meter_id, functionallocation, reading_datetime, timezone,"
        		+ " reading_value, unit_of_measure, consumption, read_interval, equipmentnumber, installation, register,"
        		+ " logicalregisternumber,              businesspartnernumber, contractaccount, utilitiescontract,"
        		+ " regionalstructuregrouping)     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
      }

      if ("HOURLY".equals(readIntervalType))
      {
        String sliceTodaysReadings = "DELETE from app.meter_ami_reads_hourly where reading_datetime > '" + maxHourlyReadingDatesInPostgresSinkToSlice + "'";
        debugQuery(sliceTodaysReadings);
        pgConn.createStatement().executeUpdate(sliceTodaysReadings);

        insertSQL = "INSERT INTO app.meter_ami_reads_hourly (headend_meter_id, functionallocation, reading_datetime, timezone, "
        		+ "reading_value, unit_of_measure, consumption, read_interval, equipmentnumber, installation, register, "
        		+ "logicalregisternumber,              businesspartnernumber, contractaccount, utilitiescontract, "
        		+ "regionalstructuregrouping)     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
      }

      debugQuery(insertSQL);
      PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);
      do
      {
        pstmt.setString(1, sourceResultSet.getString(1));
        pstmt.setString(2, sourceResultSet.getString(2));
        pstmt.setTimestamp(3, sourceResultSet.getTimestamp(3));
        pstmt.setString(4, sourceResultSet.getString(4));
        pstmt.setDouble(5, sourceResultSet.getDouble(5));
        pstmt.setString(6, sourceResultSet.getString(6));
        pstmt.setDouble(7, sourceResultSet.getDouble(7));
        pstmt.setInt(8, sourceResultSet.getInt(8));
        pstmt.setString(9, sourceResultSet.getString(9));
        pstmt.setLong(10, sourceResultSet.getLong(10));
        pstmt.setString(11, sourceResultSet.getString(11));
        pstmt.setString(12, sourceResultSet.getString(12));
        pstmt.setString(13, sourceResultSet.getString(13));
        pstmt.setLong(14, sourceResultSet.getLong(14));
        pstmt.setLong(15, sourceResultSet.getLong(15));
        pstmt.setString(16, sourceResultSet.getString(16));

        pstmt.addBatch();
        count++;
      }while (sourceResultSet.next());
      pstmt.executeBatch();

      for (String deleteSQL : getPatchDeleteQueryWhenRunningDeltaLoad()) {
        debugQuery(deleteSQL);
        pgConn.createStatement().executeUpdate(deleteSQL);
      }
    } else {
      logger.info("Skipping. ResultSet was empty - at " + new Date() + " \treadIntervalType=" + readIntervalType);
    }
    logger.info("Inserted " + count + " rows");
  }
  
  private static void updateAMIDataWV(String readIntervalType, Connection pgConn, ResultSet sourceResultSet, String maxHourlyReadingDatesInPostgresSinkToSlice) throws SQLException {
	    int count = 0;
	    if (sourceResultSet.next())
	    {
	    	String insertSQL = null;

	        if ("DAILY".equals(readIntervalType)) {
	        	insertSQL = "INSERT INTO app.meter_ami_reads_daily (headend_meter_id, utilities_premise, reading_datetime,timezone,"
	        			+ " reading_value, unit_of_measure, consumption, read_interval, equipmentnumber, installation, register,"
	        			+ " logicalregisternumber,              businesspartnernumber, contractaccount, utilitiescontract,"
	        			+ " regionalstructuregrouping, functionallocation )             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
	        }

	        if ("HOURLY".equals(readIntervalType))
	        {
	          String sliceTodaysReadings = "DELETE from app.meter_ami_reads_hourly where reading_datetime > '" + maxHourlyReadingDatesInPostgresSinkToSlice + "'";
	          debugQuery(sliceTodaysReadings);
	          pgConn.createStatement().executeUpdate(sliceTodaysReadings);

	          insertSQL = "INSERT INTO app.meter_ami_reads_hourly (headend_meter_id, utilities_premise, reading_datetime, timezone, "
	          		+ "reading_value, unit_of_measure, consumption, read_interval, equipmentnumber, installation, register, "
	          		+ "logicalregisternumber,              businesspartnernumber, contractaccount, utilitiescontract, "
	          		+ "regionalstructuregrouping, functionallocation)     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
	        }
	        
	      PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);
	      do
	      {
	        pstmt.setString(1, sourceResultSet.getString(1));
	        pstmt.setString(2, sourceResultSet.getString(2));
	        pstmt.setTimestamp(3, sourceResultSet.getTimestamp(3));
	        pstmt.setString(4, sourceResultSet.getString(4));
	        pstmt.setDouble(5, sourceResultSet.getDouble(5));
	        pstmt.setString(6, sourceResultSet.getString(6));
	        pstmt.setDouble(7, sourceResultSet.getDouble(7));
	        pstmt.setInt(8, sourceResultSet.getInt(8));
	        pstmt.setString(9, sourceResultSet.getString(9));
	        pstmt.setLong(10, sourceResultSet.getLong(10));
	        pstmt.setString(11, sourceResultSet.getString(11));
	        pstmt.setString(12, sourceResultSet.getString(12));
	        pstmt.setString(13, sourceResultSet.getString(13));
	        pstmt.setLong(14, sourceResultSet.getLong(14));
	        pstmt.setLong(15, sourceResultSet.getLong(15));
	        pstmt.setString(16, sourceResultSet.getString(16));
	        pstmt.setString(17, sourceResultSet.getString(17));

	        pstmt.addBatch();
	        count++;
	        
	      }while (sourceResultSet.next());
	      pstmt.executeBatch();

	    } else {
	      logger.info("Skipping. ResultSet was empty - at " + new Date() + " \treadIntervalType=" + readIntervalType);
	    }
	    logger.info("Inserted " + count + " rows. "+getTime());
	  }

  private static void updateUsagePrediction(Connection pgConn, Double zScore) throws SQLException
  {
    String usagePredictionMonthsThreshold = getConfigValue(ApplicationConfig.APP_CONFIG_USAGE_PREDICTION_NUMBER_OF_PAST_MONTHS);

    logger.info("usagePredictionMonthsThreshold from config => " + usagePredictionMonthsThreshold);

    String insertSQL = "INSERT INTO app.meter_ami_projected_consumption(             business_partner_number, premise_id, connection_contract_number,              meter_id, service_period_start, service_period_end, curr_read_consumption,              curr_read_date, per_day_consumption, days_to_next_read, projected_consumption)     VALUES (?, ?, ?,              ?, ?, ?, ?,              ?, ?, ?, ?) ";

    String readDateSQL = "SELECT   mar.businesspartnernumber business_partner_number,           cc.premise_id,           cast (mar.contractaccount as varchar)      connection_contract_number,           trim(leading '0' FROM mar.equipmentnumber) meter_id,           mr2.last_read_time::date                   service_period_start,           ami_calc.next_read_date                    service_period_end,          sum(mar.consumption)                       curr_read_consumption,           max(mar.reading_datetime)::date            curr_read_date,           round(ami_calc.per_day_consumption::numeric,2),            extract('day' FROM ami_calc.next_read_date - max(mar.reading_datetime))                                                                           days_to_next_read,          sum(mar.consumption) + round((extract('day' FROM ami_calc.next_read_date - max(mar.reading_datetime)) * ami_calc.per_day_consumption)::numeric,2) projected_consumption FROM     app.meter_ami_reads_daily mar  JOIN          (                    SELECT   mv2.meter_id,                             mv2.business_partner_number,                             mv2.connection_contract_number,                             max(meter_reading_time) last_read_time                    FROM     app.meter_readings_v2 mr2                    JOIN     app.meters_v2 mv2                    ON       mv2.meter_id = mr2.meter_id                    AND      mv2.isactive = 'Yes'                    AND      district IN (" + 
      getCountiesListSQLString() + ")  " + 
      "                  AND      meter_reading_reason = '01'  " + 
      "                  AND      end_point_type_1 = '20' " + 
      "                  GROUP BY mv2.meter_id,  " + 
      "                           mv2.business_partner_number,  " + 
      "                           mv2.connection_contract_number " + 
      "\t\t) mr2  " + 
      "\t\tON       mr2.meter_id = trim(leading '0' FROM mar.equipmentnumber)  " + 
      "\t\tAND      mr2.business_partner_number = mar.businesspartnernumber  " + 
      "\t\tAND      mr2.connection_contract_number = cast (mar.contractaccount as varchar) " + 
      "JOIN  " + 
      "        (  " + 
      "                  SELECT   mv2.meter_id,  " + 
      "                           mv2.register,  " + 
      "                           mv2.business_partner_number,  " + 
      "                           mv2.connection_contract_number,  " + 
      "                           sum(consumption_gl)                                                                      total_consumption, " + 
      "                           round(avg(consumption_gl)::                                 numeric,2)                   avg_consumption, " + 
      "                           round(stddev_pop(consumption_gl)::                          numeric,2)                   std_dev_consumption, " + 
      "                           round((avg(consumption_gl)+" + zScore + "*stddev_pop(consumption_gl))::numeric,2)                   high_usage_limit, " + 
      "                           extract( day FROM max(meter_reading_time) - min(meter_reading_time))                     total_days, " + 
      "                           sum(consumption_gl)/extract( day FROM max(meter_reading_time) - min(meter_reading_time)) per_day_consumption, " + 
      "                           min(meter_reading_time)                                                                  start_date, " + 
      "                           max(meter_reading_time)                                                                  end_date, " + 
      "                           mnr.next_read_date  " + 
      "                  FROM     app.meter_readings_v2 mr2  " + 
      "                  JOIN     app.meters_v2 mv2  " + 
      "                  ON       mv2.meter_id = mr2.meter_id  " + 
      "                  AND      mv2.isactive = 'Yes'  " + 
      "                  AND      district IN (" + getCountiesListSQLString() + ")  " + 
      "                  AND      meter_reading_reason = '01'  " + 
      "                  AND      end_point_type_1 = '20'  " + 
      "                  AND      meter_reading_time> CURRENT_TIMESTAMP - interval '" + usagePredictionMonthsThreshold + " month'  " + 
      "                  JOIN     app.meter_next_read mnr  " + 
      "                  ON       mnr.meter_reading_unit = mr2.meter_reading_unit  " + 
      "                  GROUP BY mv2.meter_id,  " + 
      "                           mv2.register,  " + 
      "                           mv2.business_partner_number,  " + 
      "                           mv2.connection_contract_number,  " + 
      "                           mnr.next_read_date " + 
      "\t\t) ami_calc  " + 
      "\t\tON       ami_calc.meter_id = trim(leading '0' FROM mar.equipmentnumber)  " + 
      "\t\tAND      ami_calc.business_partner_number = mar.businesspartnernumber  " + 
      "\t\tAND      ami_calc.connection_contract_number = cast (mar.contractaccount as varchar)  " + 
      "JOIN    app.connection_contracts cc  " + 
      "\t\tON       mar.businesspartnernumber = cc.business_partner_number  " + 
      "\t\tAND      cast (mar.contractaccount as varchar) = cc.connection_contract_number " + 
      "WHERE    mar.reading_datetime> mr2.last_read_time  " + 
      "GROUP BY mar.businesspartnernumber,  " + 
      "         cc.premise_id,  " + 
      "         mar.contractaccount,  " + 
      "         mar.equipmentnumber,  " + 
      "         mr2.last_read_time,  " + 
      "         ami_calc.per_day_consumption,  " + 
      "         ami_calc.next_read_date";

    debugQuery(readDateSQL);
    ResultSet sourceResultSet = pgConn.createStatement().executeQuery(readDateSQL);

    String deleteSQL = "delete from app.meter_ami_projected_consumption";

    debugQuery(deleteSQL);
    pgConn.createStatement().executeUpdate(deleteSQL);

    debugQuery(insertSQL);
    PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);

    int count = 0;
    while (sourceResultSet.next()) {
      pstmt.setString(1, sourceResultSet.getString(1));
      pstmt.setString(2, sourceResultSet.getString(2));
      pstmt.setString(3, sourceResultSet.getString(3));
      pstmt.setString(4, sourceResultSet.getString(4));
      pstmt.setDate(5, sourceResultSet.getDate(5));
      pstmt.setDate(6, sourceResultSet.getDate(6));
      pstmt.setBigDecimal(7, sourceResultSet.getBigDecimal(7));
      pstmt.setDate(8, sourceResultSet.getDate(8));
      pstmt.setBigDecimal(9, sourceResultSet.getBigDecimal(9));
      pstmt.setInt(10, sourceResultSet.getInt(10));
      pstmt.setBigDecimal(11, sourceResultSet.getBigDecimal(11));
      pstmt.addBatch();
      count++;
    }
    pstmt.executeBatch();
    logger.info("meter_ami_projected_consumption: Inserted " + count + " rows");

    sourceResultSet.close();
  }

  public static Connection zookeeperConnect() throws Exception
  {
    String url = getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_JDBC_URI);
    String principal = getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_KERBEROS_PRINCIPAL);

    String keyTab = null;

    keyTab = getConfigValue(ApplicationConfig.APP_CONFIG_HIVE_KERBEROS_KEYTAB_FILEPATH);
    try
    {
    	org.apache.hadoop.conf.Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "Kerberos");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principal, keyTab);
      Class.forName("org.apache.hive.jdbc.HiveDriver");

      return DriverManager.getConnection(url);
    }
    catch (SQLException e) {
      e.printStackTrace();
      if (e.getNextException() != null) {
        e.getNextException().printStackTrace();
      }
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static Connection getPostgresConnection(String jdbcUrl, String uid, String pwd) throws SQLException {
    try {
      Class.forName("org.postgresql.Driver");
      Connection connection = null;
      return DriverManager.getConnection(jdbcUrl, uid, pwd);
    }
    catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

/*  private static void updateMeterNextRead(Connection hiveConn, Connection pgConn) throws SQLException
  {
    String insertSQL = "INSERT INTO app.meter_next_read(meter_reading_unit, next_read_date)  VALUES (?, ?)";
    String readDateSQL = "select meterreadingunit,min(scheduledmeterreadingdate) next_read_date from awinternal.meterreadingunitschedulerecord  where scheduledmeterreadingdate >= current_date() group by meterreadingunit ";

    debugQuery(readDateSQL);
    ResultSet sourceResultSet = hiveConn.createStatement().executeQuery(readDateSQL);

    String deleteSQL = "delete from app.meter_next_read";
    debugQuery(deleteSQL);
    pgConn.createStatement().executeUpdate(deleteSQL);

    debugQuery(insertSQL);
    PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);
    int count = 0;
    while (sourceResultSet.next()) {
      pstmt.setString(1, sourceResultSet.getString(1));
      pstmt.setDate(2, sourceResultSet.getDate(2));
      pstmt.addBatch();
      count++;
    }

    pstmt.executeBatch();
    logger.info("updateMeterNextRead: Inserted " + count + " rows");
    sourceResultSet.close();
  }*/
  
  
  private static void updateMeterNextRead(Connection hiveConn, Connection pgConn) throws SQLException
  {
    String insertSQL = "INSERT INTO app.meter_next_read(meter_reading_unit, next_read_date)  VALUES (?, ?)";
    String readDateSQL = "select meterreadingunit,min(scheduledmeterreadingdate) from app.meter_mru_schedule_records where scheduledmeterreadingdate >= current_date group by meterreadingunit";

    debugQuery(readDateSQL);
    ResultSet sourceResultSet = pgConn.createStatement().executeQuery(readDateSQL);

    String deleteSQL = "delete from app.meter_next_read";
    debugQuery(deleteSQL);
    pgConn.createStatement().executeUpdate(deleteSQL);

    debugQuery(insertSQL);
    PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);
    int count = 0;
    while (sourceResultSet.next()) {
      pstmt.setString(1, sourceResultSet.getString(1));
      pstmt.setDate(2, sourceResultSet.getDate(2));
      pstmt.addBatch();
      count++;
    }

    pstmt.executeBatch();
    logger.info("updateMeterNextRead: Inserted " + count + " rows "+ getTime() );
    sourceResultSet.close();
  }

  private static String getSecureAuthToken() throws Exception
  {
    String token = null;
    try
    {
      String secureAuthTokenUrl = getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_OAUTH_URL);
      List paramList = new ArrayList();

      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.grant_type.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_GRANT_TYPE)));
      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.username.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_USERNAME)));
      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.password.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_PASSWORD)));
      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.client_id.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_CLIENT_ID)));
      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.client_secret.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_CLIENT_SECRET)));
      paramList.add(new BasicNameValuePair(LoadAMIData.SecureAuthParam.scope.name(), getConfigValue(ApplicationConfig.AMI_SECUREAUTH_CONFIG_SCOPE)));

      client = HttpClients.custom().setSslcontext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy()
      {
        public boolean isTrusted(X509Certificate[] arg0, String arg1)
          throws CertificateException
        {
          return true;
        }
      }).build()).setDefaultHeaders(Lists.newArrayList(new BasicHeader[] { 
        new BasicHeader("Accept", "application/json") })).build();

      HttpUriRequest request = RequestBuilder.post().setUri(secureAuthTokenUrl)
        .setHeader("Content-Type", "application/x-www-form-urlencoded")
        .setEntity(new UrlEncodedFormEntity(paramList))
        .build();
      HttpResponse response = client.execute(request);

      logger.info("response code = " + response.getStatusLine().getStatusCode());
      logger.info(response);

      String responseJSONStr = EntityUtils.toString(response.getEntity());
      token = new JsonParser().parse(responseJSONStr).getAsJsonObject().get("access_token").getAsString();
      logger.debug(responseJSONStr);
    } catch (Exception e) {
      logger.error(e, e.getCause());
      throw e;
    }
    return token;
  }

  private static void callHighUsageRESTService(Connection pgConn, String urlPath, AccionPayloadAmi payload, String token, String equipmentNumber, Timestamp lastReadTime, Double currentUsage, Double highUsageLimit) throws Exception
  {
    HttpResponse response = callRESTServiceInternal(pgConn, urlPath, payload, token);

    String responseJSON = EntityUtils.toString(response.getEntity());
    logger.info(responseJSON);
    logger.info("---------------");

    AuditAlertEvent.insertHighUsageAuditRecord(pgConn, StringUtils.substringAfterLast(urlPath, "/"), responseJSON, 
      response.toString(), response.getStatusLine().getReasonPhrase(), payload.toJson(), urlPath, equipmentNumber, lastReadTime, currentUsage, highUsageLimit, payload.getBusinessPartnerNumber(), payload.getContractAccountNumber());
  }

  private static void callLeaksRESTService(Connection pgConn, String urlPath, AccionPayloadAmi payload, String token) throws Exception {
    HttpResponse response = callRESTServiceInternal(pgConn, urlPath, payload, token);

    String responseJSON = EntityUtils.toString(response.getEntity());
    logger.info(responseJSON);
    logger.info("^--------------");

    AuditAlertEvent.insertLeaksAuditRecord(pgConn, StringUtils.substringAfterLast(urlPath, "/"), responseJSON, 
      response.toString(), response.getStatusLine().getReasonPhrase(), payload.toJson(), urlPath, payload.getBusinessPartnerNumber(), payload.getContractAccountNumber());
  }

  private static HttpResponse callRESTServiceInternal(Connection pgConn, String urlPath, AccionPayloadAmi payload, String token) throws Exception
  {
    HttpUriRequest request = RequestBuilder.post().setUri(urlPath)
      .setEntity(new StringEntity(payload.toJson()))
      .addHeader("Authorization", token)
      .setHeader("Content-Type", "application/json; charset=utf-8")
      .build();
    logger.info(urlPath);
    logger.info(payload.toJson());
    logger.info(request);
    logger.info(((HttpEntityEnclosingRequest)request).getEntity());

    HttpResponse response = client.execute(request);
    logger.info("response code = " + response.getStatusLine().getStatusCode());
    logger.info(response);
    return response;
  }

  private static void showFiglet()
  {
    logger.info("\t\t\t\t ______                  _____                __              __     \t\t");
    logger.info("\t\t\t\t/\\  _  \\                /\\  __`\\             /\\ \\      __    /\\ \\    \t\t");
    logger.info("\t\t\t\t\\ \\ \\L\\ \\  _____   _____\\ \\ \\/\\ \\  _ __   ___\\ \\ \\___ /\\_\\   \\_\\ \\   \t\t");
    logger.info("\t\t\t\t \\ \\  __ \\/\\ '__`\\/\\ '__`\\ \\ \\ \\ \\/\\`'__\\/'___\\ \\  _ `\\/\\ \\  /'_` \\  \t\t");
    logger.info("\t\t\t\t  \\ \\ \\/\\ \\ \\ \\L\\ \\ \\ \\L\\ \\ \\ \\_\\ \\ \\ \\//\\ \\__/\\ \\ \\ \\ \\ \\ \\/\\ \\L\\ \\ \t\t");
    logger.info("\t\t\t\t   \\ \\_\\ \\_\\ \\ ,__/\\ \\ ,__/\\ \\_____\\ \\_\\\\ \\____\\\\ \\_\\ \\_\\ \\_\\ \\___,_\\\t\t");
    logger.info("\t\t\t\t    \\/_/\\/_/\\ \\ \\/  \\ \\ \\/  \\/_____/\\/_/ \\/____/ \\/_/\\/_/\\/_/\\/__,_ /\t\t");
    logger.info("\t\t\t\t             \\ \\_\\   \\ \\_\\                                           \t\t");
    logger.info("\t\t\t\t              \\/_/    \\/_/                                           \t\t");
    logger.info("Copyright " + LocalDate.now().getYear() + "- AppOrchid Inc");
  }

  private static enum SecureAuthParam
  {
    grant_type, client_id, client_secret, scope, username, password;
  }
  
  private static Date getTime() {
	  TimeZone.setDefault(TimeZone.getTimeZone("CTT"));
	  return new Date();
  }
}