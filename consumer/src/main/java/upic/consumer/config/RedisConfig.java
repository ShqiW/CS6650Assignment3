package upic.consumer.config;


public class RedisConfig {
    private String host;
    private int port;
    private String password;
    private int maxTotal;
    private int maxIdle;
    private int minIdle;
    private long maxWaitMillis;


    public static RedisConfig getDefaultConfig() {
        RedisConfig config = new RedisConfig();
        config.setHost("35.162.22.180");
        config.setPort(6379);
        config.setPassword("redis");
        config.setMaxTotal(128);
        config.setMaxIdle(32);
        config.setMinIdle(8);
        config.setMaxWaitMillis(10000);
        return config;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(long maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }
}
