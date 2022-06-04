package pl.kielce.tu.psr.cassandra.managers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class ZooTableSimpleManager extends SimpleManager{

	public ZooTableSimpleManager(CqlSession session) {
		super(session);
	}

	public void createTable() {
		executeSimpleStatement( 
				"CREATE TYPE transport (\n" +
				"    transportId int,\n" +
				"    possibilities list<text>,\n" +
				");");
		executeSimpleStatement(
				"CREATE TYPE place (\n" +
				"    placeId int,\n" +
				"    cities list<text>,\n" +
				");");
		executeSimpleStatement( 
				"CREATE TABLE travel (\n" +
				"    travelId int PRIMARY KEY,\n" +
				"    popularity int,\n" +
				"    places frozen<place>,\n" +
				"    transports frozen<transport>,\n" +
				");");
	}
	
	public void insertIntoTable() {
		executeSimpleStatement("INSERT INTO travel (travelId, popularity, places, transports) " +
				" VALUES (7, 9, {placeId : 4,cities : ['Kielce', 'Kraków', 'Poznań']},{transportId : 8,possibilities: ['Samolot', 'Pociąg', 'Taxi']})");
		executeSimpleStatement("INSERT INTO travel (travelId, popularity, places, transports) " +
				" VALUES (8, 7, {placeId : 5,cities : ['Madryt', 'Monako', 'Rzym']},{transportId : 9,possibilities: ['Samolot', 'Statek']})");
		executeSimpleStatement("INSERT INTO travel (travelId, popularity, places, transports) " +
				" VALUES (9, 8, {placeId : 6,cities : ['Góry Świętokrzyskie', 'Mount Blanc', 'Mount Everest']},{transportId : 5,possibilities: ['Rower', 'Bryczka', 'Motor']})");
		executeSimpleStatement("INSERT INTO travel (travelId, popularity, places, transports) " +
				" VALUES (5, 4, {placeId : 7,cities : ['Grecja', 'Polska', 'Norwegia']},{transportId : 2,possibilities: ['Prom', 'Odrzutowiec', 'Rakieta']})");

//		executeSimpleStatement("INSERT INTO place (placeId, cities) " +
//				" VALUES (18,['Góry Świętokrzyskie', 'Mount Blanc', 'Mount Everest'])");

	}

	public void updateIntoTable() {
		executeSimpleStatement("UPDATE travel SET popularity = 10 WHERE travelId = 5;");
	}
	
	public void deleteFromTable() {
		executeSimpleStatement("DELETE FROM travel WHERE travelId = 9;");
	}
	
	public void selectFromTable() {
		String statement = "SELECT * FROM travel;";
		ResultSet resultSet = session.execute(statement);
		for (Row row : resultSet) {
			System.out.print("\ttravel: ");
			System.out.print(row.getInt("travelId") + ", ");
			System.out.print(row.getInt("popularity") + ", ");
			UdtValue place = row.getUdtValue("places");
			System.out.print("{" + place.getInt("placeId") +", " + place.getList("cities",String.class) + "}" +  ", ");
			//UdtValue transport = row.getUdtValue("transport");
			//System.out.print("{" + transport.getInt("transportId") +", " + transport.getList("possibilities",String.class) + "}" +  ", ");
//			//System.out.print("{" + address.getString("street") + ", " + address.getInt("houseNumber")  + ", " + address.getInt("apartmentNumber") + "}" +  ", ");
//			//System.out.print(row.getList("names", String.class) + ", ");


			//llllllllllll              lllllll
			//vvvvvvvvvvvvORIGINAL CODE vvvvvvv
//			UdtValue address = row.getUdtValue("address");
//			System.out.print("{" + address.getString("street") + ", " + address.getInt("houseNumber")  + ", " + address.getInt("apartmentNumber") + "}" +  ", ");


			System.out.println();
		}
		System.out.println("Statement \"" + statement + "\" executed successfully");
	}
	
	public void dropTable() {
		executeSimpleStatement("DROP TABLE travel;");
	}
}
