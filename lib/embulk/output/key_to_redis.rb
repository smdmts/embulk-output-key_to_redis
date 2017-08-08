Embulk::JavaPlugin.register_output(
  "key_to_redis", "org.embulk.output.key_to_redis.KeyToRedisOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
