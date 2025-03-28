package upic.server.config;
import java.io.IOException;
import java.util.Properties;

public class EnvLoader {
  private static final Properties properties = new Properties();

  static {
    try {
      properties.load(new java.io.FileReader(System.getProperty("user.home") + "/.UPICenv"));
    } catch (IOException e) {
      System.err.println("Could not load .env file from current working directory, using default values.");
    }
  }

  public static String getEnv(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }

  public static int getEnvAsInt(String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value != null) {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException e) {
        System.err.println("Invalid number format for key: " + key + ", using default value.");
      }
    }
    return defaultValue;
  }

  public static boolean getEnvAsBoolean(String key, boolean defaultValue) {
    String value = properties.getProperty(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultValue;
  }
}
