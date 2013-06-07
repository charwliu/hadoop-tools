package org.streamspf.hadoop.config;

import org.streamspf.hadoop.hbase.annotations.HConnectionConfig;
import java.util.HashMap;
import java.util.Map;

@HConnectionConfig
public class MyConfig {

	@HConnectionConfig("default")
	public Map<String, String> createConfig() {
		Map<String, String> config = new HashMap<String, String>();

		config.put("hbase.zookeeper.quorum", "10.1.253.96, 10.1.253.97, 10.1.253.99"); // 192.168.46.122,192.168.46.123,192.168.46.125");
		config.put("hbase.zookeeper.property.clientPort", "2383");
		config.put("zookeeper.session.timeout", "180000");
		config.put("hhbase.table.references.max", "10");
		config.put("hbase.client.write.buffer", "4194304");

		return config;
	}
}
