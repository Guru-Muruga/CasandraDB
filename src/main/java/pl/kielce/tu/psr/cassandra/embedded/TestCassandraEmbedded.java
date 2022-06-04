package pl.kielce.tu.psr.cassandra.embedded;

import java.net.InetSocketAddress;

import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;

import pl.kielce.tu.psr.cassandra.managers.KeyspaceSimpleManager;
import pl.kielce.tu.psr.cassandra.managers.ZooTableSimpleManager;

public class TestCassandraEmbedded {

	public static void main(String[] args) {
		Configurator.setLevel("com.github.nosan.embedded.cassandra.Cassandra", Level.FATAL);
		
		Cassandra cassandra = new CassandraBuilder().build();
		cassandra.start();
		try {
			Settings settings = cassandra.getSettings();
			try (CqlSession session = CqlSession.builder()
					.addContactPoint(new InetSocketAddress(settings.getAddress(), settings.getPort()))
					.withLocalDatacenter("datacenter1").build()) {
				
				KeyspaceSimpleManager keyspaceManager = new KeyspaceSimpleManager(session, "university");
				keyspaceManager.dropKeyspace();
				keyspaceManager.selectKeyspaces();
				keyspaceManager.createKeyspace();
				keyspaceManager.useKeyspace();

				ZooTableSimpleManager tableManager = new ZooTableSimpleManager(session);
				tableManager.createTable();
				tableManager.insertIntoTable();
				tableManager.updateIntoTable();
				tableManager.selectFromTable();
				tableManager.deleteFromTable();
				tableManager.dropTable();
				
			}
		} finally {
			cassandra.stop();
		}

	}

}