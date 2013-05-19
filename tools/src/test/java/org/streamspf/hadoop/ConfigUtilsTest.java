package org.streamspf.hadoop;

import org.streamspf.hadoop.util.ConfigUtils;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author liuwei
 */
public class ConfigUtilsTest {

	public ConfigUtilsTest() {
	}

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}
	// TODO add test methods here.
	// The methods must be annotated with annotation @Test. For example:
	//

	@Test
	public void testGetConfig() {
		Map<String, String> config = ConfigUtils.getConfig("default");
		logger.info("{}", config);
	}

	private final static Logger logger = LoggerFactory.getLogger(ConfigUtilsTest.class);

}
