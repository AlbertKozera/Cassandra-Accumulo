package Cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class ZooTableSimpleManager extends SimpleManager {

    public ZooTableSimpleManager(CqlSession session) {
        super(session);
    }

    public void createTable() {
        executeSimpleStatement(
                "CREATE TYPE adresZamieszkania (\n" +
                        "    kodPocztowy int,\n" +
                        "    ulica text,\n" +
                        "    nrDomu int,\n" +
                        "    nrMieszkania int\n" +
                        ");");
        executeSimpleStatement(
                "CREATE TABLE historiaWizyt (\n" +
                        "    id uuid PRIMARY KEY,\n" +
                        "    imie text,\n" +
                        "    nazwisko text,\n" +
                        "    wiek int,\n" +
                        "    adresZamieszkania frozen<adresZamieszkania>,\n" +
                        "    rodzajBiletu text,\n" +
                        "    dataWejscia text,\n" +
                        "    dataWyjscia text\n" +
                        ");");
    }

    public void insertIntoTable(String imie, String nazwisko, int wiek, int kodPocztowy, String ulica, int nrDomu, int nrMieszkania, String rodzajBiletu, String dataWejscia, String dataWyjscia) {
        executeSimpleStatement("INSERT INTO historiaWizyt (id, imie, nazwisko, wiek, adresZamieszkania, rodzajBiletu, dataWejscia, dataWyjscia) "
                + " VALUES ("
                + UUID.randomUUID() + ", '" + imie + "', '" + nazwisko + "', " + wiek + "," +
                " {kodPocztowy : " + kodPocztowy + ", ulica : '" + ulica + "', nrDomu : " + nrDomu + ", " + "nrMieszkania : "
                + nrMieszkania + "}, '" + rodzajBiletu + "', '" + dataWejscia + "', '" + dataWyjscia + "');");
    }

    public void updateImie(String imie, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET imie = '" + imie + "' WHERE id = "+ id +";");
    }

    public void updateNazwisko(String nazwisko, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET nazwisko = '" + nazwisko + "' WHERE id = "+ id +";");
    }

    public void updateWiek(int wiek, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET wiek = " + wiek + " WHERE id = "+ id +";");
    }

    public void updateRodzajBiletu(String rodzajBiletu, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET rodzajBiletu = '" + rodzajBiletu + "' WHERE id = "+ id +";");
    }

    public void updateDataWejscia(String dataWejscia, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET dataWejscia = '" + dataWejscia + "' WHERE id = "+ id +";");
    }

    public void updateDataWyjscia(String dataWyjscia, UUID id) {
        executeSimpleStatement("UPDATE historiaWizyt SET dataWyjscia = '" + dataWyjscia + "' WHERE id = "+ id +";");
    }

    public void deleteFromTable(UUID id) {
        executeSimpleStatement("DELETE FROM historiaWizyt WHERE id = "+ id +";");
    }

    public void selectFromTable(String statement) {
        ResultSet resultSet = session.execute(statement);
        System.out.println("[historiaWizyt]");
        for (Row row : resultSet) {
            System.out.print("id: " + row.getUuid("id") + ", ");
            System.out.print("imie: " + row.getString("imie") + ", ");
            System.out.print("nazwisko: " + row.getString("nazwisko") + ", ");
            System.out.print("wiek: " + row.getInt("wiek") + ", ");
            UdtValue adresZamieszkania = row.getUdtValue("adresZamieszkania");
            System.out.print("adresZamieszkania{" + "kodPocztowy: " + adresZamieszkania.getInt("kodPocztowy") + ", " + "ulica: " + adresZamieszkania.getString("ulica") +  ", " + "nrDomu: " + adresZamieszkania.getInt("nrDomu") + ", " + "nrMieszkania: " + adresZamieszkania.getInt("nrMieszkania") + "}" + ", ");
            System.out.print("rodzajBiletu: " + row.getString("rodzajBiletu") + ", ");
            System.out.print("dataWejscia: " + row.getString("dataWejscia") + ", ");
            System.out.println("dataWyjscia: " + row.getString("dataWyjscia"));
        }
        System.out.println();
        System.out.println("Statement \"" + statement + "\" executed successfully");
    }

    public void toUpperCase() {
        String statement = "SELECT * FROM historiaWizyt;";
        ResultSet resultSet = session.execute(statement);
        for (Row row : resultSet) {
            executeSimpleStatement("UPDATE historiaWizyt SET imie = '"+ row.getString("imie").toUpperCase() +"' WHERE id = "+ row.getUuid("id") +";");
        }
        System.out.println();
    }

    public void toLowerCase() {
        String statement = "SELECT * FROM historiaWizyt;";
        ResultSet resultSet = session.execute(statement);
        for (Row row : resultSet) {
            executeSimpleStatement("UPDATE historiaWizyt SET imie = '"+ row.getString("imie").toLowerCase() +"' WHERE id = "+ row.getUuid("id") +";");
        }
        System.out.println();
    }

    public void selectByDate(String from_string, String to_string) throws ParseException {
        Date from_date, to_date;
        Date from_date_check, to_date_check;

        from_date_check = new SimpleDateFormat("yyyyy-MM-dd HH:mm:ss").parse(from_string);
        to_date_check = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(to_string);

        String statement = "SELECT * FROM historiaWizyt;";
        ResultSet resultSet = session.execute(statement);
        System.out.println("[historiaWizyt] w przedziale czasowym ---> " + from_string + " --- " + to_string);
        for (Row row : resultSet) {
            from_date =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(row.getString("dataWejscia"));
            to_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(row.getString("dataWyjscia"));
            if(from_date.after(from_date_check) && to_date.before(to_date_check)) {
                System.out.print("id: " + row.getUuid("id") + ", ");
                System.out.print("imie: " + row.getString("imie") + ", ");
                System.out.print("nazwisko: " + row.getString("nazwisko") + ", ");
                System.out.print("wiek: " + row.getInt("wiek") + ", ");
                UdtValue adresZamieszkania = row.getUdtValue("adresZamieszkania");
                System.out.print("adresZamieszkania{" + "kodPocztowy: " + adresZamieszkania.getInt("kodPocztowy") + ", " + "ulica: " + adresZamieszkania.getString("ulica") +  ", " + "nrDomu: " + adresZamieszkania.getInt("nrDomu") + ", " + "nrMieszkania: " + adresZamieszkania.getInt("nrMieszkania") + "}" + ", ");
                System.out.print("rodzajBiletu: " + row.getString("rodzajBiletu") + ", ");
                System.out.print("dataWejscia: " + row.getString("dataWejscia") + ", ");
                System.out.println("dataWyjscia: " + row.getString("dataWyjscia"));
            }
        }
        System.out.println();
    }

    public void dropTable() {
        executeSimpleStatement("DROP TABLE historiaWizyt;");
    }

}
