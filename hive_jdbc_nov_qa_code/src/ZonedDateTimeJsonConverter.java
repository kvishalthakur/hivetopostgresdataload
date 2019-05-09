import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

class ZonedDateTimeJsonConverter
  implements JsonSerializer<ZonedDateTime>, JsonDeserializer<ZonedDateTime>
{
  private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  public JsonElement serialize(ZonedDateTime localDateTime, Type type, JsonSerializationContext jsonSerializationContext) {
    return new JsonPrimitive(localDateTime.truncatedTo(ChronoUnit.SECONDS).format(dateTimeFormatter));
  }

  public ZonedDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException
  {
    return ZonedDateTime.parse(json.getAsString(), dateTimeFormatter);
  }
}