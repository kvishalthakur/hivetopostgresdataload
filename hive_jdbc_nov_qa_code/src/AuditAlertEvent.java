import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Logger;

final class AuditAlertEvent
{
  protected static void insertHighUsageAuditRecord(Connection pgConn, String eventType, String eventResult, String eventResultInfo1, String eventExtraInfo1, String eventExtraInfo2, String eventExtraInfo3, String equipmentNumber, Timestamp lastReadTime, Double currentUsage, Double highUsageLimit, String bpn, String can)
    throws Exception
  {
    insertAuditRecord(pgConn, eventType, eventResult, eventResultInfo1, eventExtraInfo1, eventExtraInfo2, eventExtraInfo3, equipmentNumber, lastReadTime, currentUsage, highUsageLimit, bpn, can);
  }

  protected static void insertLeaksAuditRecord(Connection pgConn, String eventType, String eventResult, String eventResultInfo1, String eventExtraInfo1, String eventExtraInfo2, String eventExtraInfo3, String bpn, String can) throws Exception {
    insertAuditRecord(pgConn, eventType, eventResult, eventResultInfo1, eventExtraInfo1, eventExtraInfo2, eventExtraInfo3, null, null, Double.valueOf((0.0D / 0.0D)), Double.valueOf((0.0D / 0.0D)), bpn, can);
  }

  private static void insertAuditRecord(Connection pgConn, String eventType, String eventResult, String eventResultInfo1, String eventExtraInfo1, String eventExtraInfo2, String eventExtraInfo3, String equipmentNumber, Timestamp lastReadTime, Double currentUsage, Double highUsageLimit, String bpn, String can) throws Exception
  {
    boolean result = false;
    try {
      String insertSQL = "INSERT INTO app.ami_alerts_audit( source_hostname ,event_type , event_result , event_result_info1 , event_extra_info1 , event_extra_info2 , event_extra_info3, businesspartnernumber, contractaccount, equipmentnumber, last_read_time, sum_consumption, current_usage, high_usage_limit) VALUES (?, ?, ?, ? , ? , ? , ?, ?, ? , ? , ? , ? , ? , ?)";

      Double sumConsumption = currentUsage;

      PreparedStatement pstmt = pgConn.prepareStatement(insertSQL);

      pstmt.setString(1, NetUtils.getHostname());
      pstmt.setString(2, eventType);
      pstmt.setString(3, eventResult);
      pstmt.setString(4, eventResultInfo1);
      pstmt.setString(5, eventExtraInfo1);
      pstmt.setString(6, eventExtraInfo2);
      pstmt.setString(7, eventExtraInfo3);

      pstmt.setString(8, bpn);
      pstmt.setString(9, can);
      pstmt.setString(10, equipmentNumber);
      pstmt.setTimestamp(11, lastReadTime);
      pstmt.setDouble(12, sumConsumption.doubleValue());
      pstmt.setDouble(13, currentUsage.doubleValue());
      pstmt.setDouble(14, highUsageLimit.doubleValue());

      result = pstmt.execute();
    }
    catch (Exception e) {
      e.printStackTrace();
      LoadAMIData.logger.error("Error while auditing alerts response - ", e.getCause());
    }
    LoadAMIData.logger.debug(String.format("alerts audit (" + result + ") t=%s | r=%s | ri=%s | ei1=%s | ei2=%s | ei3=%s ", new Object[] { eventType, eventResult, eventResultInfo1, eventExtraInfo1, eventExtraInfo2, eventExtraInfo3 }));
  }
}