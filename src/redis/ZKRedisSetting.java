package redis;

import java.nio.charset.Charset;

public interface ZKRedisSetting
{
	final String ROOT = "/redis";
	final String REDIS_MASTER = ROOT + "/master";
	final String REDIS_SLAVE = ROOT + "/slave";
	final Charset CHARSET = Charset.forName("UTF-8");
}
