package Cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

import java.text.ParseException;
import java.util.Scanner;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Executor {

    private static Scanner input = new Scanner(System.in);

    public static void main(String[] args) {
        Logger logger = Logger.getLogger("");
        logger.setLevel(Level.OFF);
        try (CqlSession session = CqlSession.builder().build()) {
            KeyspaceSimpleManager keyspaceManager = new KeyspaceSimpleManager(session, "zoo");
            //keyspaceManager.dropKeyspace();
            //keyspaceManager.selectKeyspaces();
            //keyspaceManager.createKeyspace();
            keyspaceManager.useKeyspace();

            ZooTableSimpleManager tableManager = new ZooTableSimpleManager(session);
            //tableManager.createTable();

            UUID id;
            String from_string, to_string;
            while(true) {
                System.out.println("Z O O");
                System.out.println("1. Dodaj wizyte");
                System.out.println("2. Aktualizuj wizyte");
                System.out.println("3. Usun wizyte");
                System.out.println("4. Wyswietl wizyty");
                System.out.println("5. Przetwarzanie danych [toUpperCase]");
                System.out.println("6. Przetwarzanie danych [toLowerCase]");
                System.out.println("7. Wyswietl wizyte po ID");
                System.out.println("8. Wyswietl wizyty w przedziale czasowym");
                System.out.println("9. Exit");
                int mode = input.nextInt();
                input.nextLine();
                switch (mode) {
                    case 1:
                        insert(tableManager);
                        break;
                    case 2:
                        System.out.println("Podaj id wizyty do edycji");
                        id = UUID.fromString(input.nextLine());
                        while(update(tableManager, id));
                        break;
                    case 3:
                        System.out.println("Podaj id wizyty do usuniecia");
                        id = UUID.fromString(input.nextLine());
                        delete(tableManager, id);
                        break;
                    case 4:
                        tableManager.selectFromTable("SELECT * FROM historiaWizyt;");
                        break;
                    case 5:
                        tableManager.toUpperCase();
                        break;
                    case 6:
                        tableManager.toLowerCase();
                        break;
                    case 7:
                        System.out.println("Podaj id wizyty do wyswietlenia");
                        id = UUID.fromString(input.nextLine());
                        tableManager.selectFromTable("SELECT * FROM historiaWizyt WHERE id = "+ id +";");
                        break;
                    case 8:
                        System.out.println("Podaj dolny przedzial [rrrr-mm-dd gg:mm:ss]"); // 2000-12-20 23:20:01
                        from_string = input.nextLine();
                        System.out.println("Podaj gorny przedzial [rrrr-mm-dd gg:mm:ss]"); // 2000-12-20 23:20:01
                        to_string = input.nextLine();
                        tableManager.selectByDate(from_string, to_string);
                        break;
                    case 9:
                        System.exit(0);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private static void insert(ZooTableSimpleManager tableManager) {
        String imie;
        String nazwisko;
        int wiek;
        int kodPocztowy;
        String ulica;
        int nrDomu;
        int nrMieszkania;
        String rodzajBiletu;
        String dataWejscia;
        String dataWyjscia;

        System.out.println("Podaj imie");
        imie = input.nextLine();
        System.out.println("Podaj nazwisko");
        nazwisko = input.nextLine();
        System.out.println("Podaj wiek");
        wiek = input.nextInt();
        input.nextLine();
        System.out.println("Podaj adres zamieszkania");
        System.out.println("- kod pocztowy");
        kodPocztowy = input.nextInt();
        input.nextLine();
        System.out.println("- ulica");
        ulica = input.nextLine();
        System.out.println("- numer domu");
        nrDomu = input.nextInt();
        input.nextLine();
        System.out.println("- numer mieszkania");
        nrMieszkania = input.nextInt();
        input.nextLine();
        System.out.println("Podaj rodzaj biletu");
        rodzajBiletu = input.nextLine();
        System.out.println("Podaj date wejscia [rrrr-mm-dd gg:mm:ss]"); // 2000-12-20 23:20:01
        dataWejscia = input.nextLine();
        System.out.println("Podaj date wyjscia [rrrr-mm-dd gg:mm:ss]"); // 2000-12-20 23:20:01
        dataWyjscia = input.nextLine();

        tableManager.insertIntoTable(imie, nazwisko, wiek, kodPocztowy, ulica, nrDomu, nrMieszkania, rodzajBiletu, dataWejscia, dataWyjscia);
    }

    private static boolean update(ZooTableSimpleManager tableManager, UUID id) {
        String imie;
        String nazwisko;
        int wiek;
        String rodzajBiletu;
        String dataWejscia;
        String dataWyjscia;

        System.out.println("Wybierz pole do aktualizacji");
        System.out.println("1. imie");
        System.out.println("2. nazwisko");
        System.out.println("3. wiek");
        System.out.println("4. rodzaj biletu");
        System.out.println("5. data wejscia");
        System.out.println("6. data wyjscia");
        System.out.println("7. zakoncz edycje");
        int mode = input.nextInt();
        input.nextLine();
        switch (mode) {
            case 1:
                System.out.println("Podaj imie");
                imie = input.nextLine();
                tableManager.updateImie(imie, id);
                return true;
            case 2:
                System.out.println("Podaj nazwisko");
                nazwisko = input.nextLine();
                tableManager.updateNazwisko(nazwisko, id);
                return true;
            case 3:
                System.out.println("Podaj wiek");
                wiek = input.nextInt();
                input.nextLine();
                tableManager.updateWiek(wiek, id);
                return true;
            case 4:
                System.out.println("Podaj rodzaj biletu");
                rodzajBiletu = input.nextLine();
                tableManager.updateRodzajBiletu(rodzajBiletu, id);
                return true;
            case 5:
                System.out.println("Podaj date wejscia");
                dataWejscia = input.nextLine();
                tableManager.updateDataWejscia(dataWejscia, id);
                return true;
            case 6:
                System.out.println("Podaj date wyjscia");
                dataWyjscia = input.nextLine();
                tableManager.updateDataWyjscia(dataWyjscia, id);
                return true;
            default:
                return false;
        }



    }

    private static void delete(ZooTableSimpleManager tableManager, UUID id) {
        tableManager.deleteFromTable(id);
    }

}