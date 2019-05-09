import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.time.ZonedDateTime;
import org.apache.commons.lang.builder.ToStringBuilder;

class AccionPayloadAmi
{
  private String businessPartnerNumber;
  private String contractAccountNumber;
  private ZonedDateTime alertDateTime = ZonedDateTime.now();

  public AccionPayloadAmi(String businessPartnerNumber, String contractAccountNumber)
  {
    this.businessPartnerNumber = businessPartnerNumber;
    this.contractAccountNumber = contractAccountNumber;
  }

  public static AccionPayloadAmi build(String bpn, String can) {
    AccionPayloadAmi a = new AccionPayloadAmi(bpn, can);
    return a;
  }

  public String getBusinessPartnerNumber() {
    return this.businessPartnerNumber;
  }

  public String getContractAccountNumber() {
    return this.contractAccountNumber;
  }

  public ZonedDateTime getAlertDateTime() {
    return this.alertDateTime;
  }

  public String toJson() {
    Gson gson = new GsonBuilder().registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeJsonConverter()).create();
    return gson.toJson(this);
  }

  public String toString()
  {
    return ToStringBuilder.reflectionToString(this);
  }
}